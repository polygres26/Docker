package com.sayonora.warp.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

/** Pure-Java tests of the MongoDB semantics engine (no Postgres): ordering, matching, projection, update, aggregation. */
class MongoEngineTest {

    private static BsonDocument d(String json) {
        return BsonDocument.parse(json);
    }

    private static boolean matches(String filter, String doc) {
        return MongoMatcher.compile(d(filter)).test(d(doc));
    }

    @Test
    void bsonTypeBracketsAndNumericEquality() {
        assertTrue(BsonCmp.compare(new BsonInt32(1), d("{v: 1.0}").get("v")) == 0);
        assertTrue(BsonCmp.compare(new BsonInt64(2), new BsonInt32(1)) > 0);
        assertTrue(BsonCmp.compare(d("{a: null}").get("a"), d("{a: 0}").get("a")) < 0, "null sorts before numbers");
        assertTrue(BsonCmp.compare(d("{a: 'x'}").get("a"), d("{a: {}}").get("a")) < 0, "strings before objects");
        assertEquals(BsonCmp.key(new BsonInt32(1)), BsonCmp.key(d("{v: 1.0}").get("v")));
        assertTrue(PostgresDocumentStore.idKey(new BsonInt64(7)).equals(PostgresDocumentStore.idKey(new BsonInt32(7))));
    }

    @Test
    void queryOperatorsFollowMongoSemantics() {
        assertTrue(matches("{a: 1}", "{a: [1, 2]}"), "scalar matches array element");
        assertFalse(matches("{'a.0': 1}", "{a: [[1, 2]]}"), "no expansion below a numeric index");
        assertTrue(matches("{a: null}", "{b: 1}"), "null matches a missing field");
        assertFalse(matches("{a: {$gt: 1}}", "{a: 'z'}"), "comparison is bracketed by type");
        assertTrue(matches("{a: {$elemMatch: {$gt: 2, $lt: 5}}}", "{a: [1, 3]}"));
        assertFalse(matches("{a: {$elemMatch: {$gt: 2}}}", "{a: [[3]]}"), "elemMatch does not descend into nested arrays");
        assertTrue(matches("{'d.f.g': 2}", "{d: {f: [{g: 1}, {g: 2}]}}"));
        assertTrue(matches("{$expr: {$gt: ['$a', '$b']}}", "{a: 3, b: 2}"));
        assertTrue(matches("{a: {$type: 'number'}}", "{a: NumberLong(5)}"));
        assertFalse(matches("{a: {$in: [1]}}", "{a: 2}"));
        assertThrows(MongoCmdException.class, () -> MongoMatcher.compile(d("{a: {$bogus: 1}}")));
    }

    @Test
    void sortKeysUseArrayMinAndMax() {
        var asc = MongoSort.comparator(d("{a: 1}"), null);
        var desc = MongoSort.comparator(d("{a: -1}"), null);
        BsonDocument x = d("{a: [5, 1]}");
        BsonDocument y = d("{a: 3}");
        assertTrue(asc.compare(x, y) < 0, "ascending uses the smallest element (1 < 3)");
        assertTrue(desc.compare(x, y) < 0, "descending uses the largest element (5 > 3)");
    }

    @Test
    void projectionInclusionExclusionAndSlice() {
        BsonDocument doc = d("{_id: 1, a: 1, b: {c: 2, e: 3}, arr: [1, 2, 3, 4]}");
        assertEquals(d("{_id: 1, b: {c: 2}}"), MongoProjection.compile(d("{'b.c': 1}"), false, null, null).apply(doc));
        assertEquals(d("{a: 1, arr: [1, 2, 3, 4]}"), MongoProjection.compile(d("{_id: 0, b: 0}"), false, null, null).apply(doc));
        assertEquals(d("{_id: 1, a: 1, b: {c: 2, e: 3}, arr: [3, 4]}"),
                MongoProjection.compile(d("{arr: {$slice: -2}}"), false, null, null).apply(doc));
        assertEquals(31254, assertThrows(MongoCmdException.class, () -> MongoProjection.compile(d("{a: 1, b: 0}"), false, null, null)).code);
    }

    private static BsonDocument update(String doc, String upd) {
        return MongoUpdate.apply(d(doc), d(upd), new MongoUpdate.Ctx());
    }

    @Test
    void updateOperatorsKeepBsonTypes() {
        assertEquals(d("{_id: 1, n: {$numberLong: '6'}}"), update("{_id: 1, n: {$numberLong: '5'}}", "{$inc: {n: 1}}"));
        assertEquals(new BsonInt64(2147483648L), update("{_id: 1, n: 2147483647}", "{$inc: {n: 1}}").get("n"), "int32 overflow widens to int64");
        assertEquals(d("{_id: 1, arr: [0, 1, 2, 3]}"), update("{_id: 1, arr: [1, 2, 3]}", "{$push: {arr: {$each: [0], $position: 0}}}"));
        assertEquals(d("{_id: 1, arr: [1, 2]}"), update("{_id: 1, arr: [1, 2, 3]}", "{$pop: {arr: 1}}"));
        assertEquals(d("{_id: 1, arr: [1, 2, 3]}"), update("{_id: 1, arr: [1, 2]}", "{$addToSet: {arr: {$each: [2, 3]}}}"));
        assertEquals(40, assertThrows(MongoCmdException.class, () -> MongoUpdate.validate(d("{$set: {a: 1}, $inc: {'a.b': 1}}"), null)).code);
        assertEquals(66, assertThrows(MongoCmdException.class, () -> update("{_id: 1}", "{$set: {_id: 2}}")).code);
    }

    @Test
    void positionalAndArrayFilters() {
        MongoUpdate.Ctx ctx = new MongoUpdate.Ctx();
        ctx.arrayFilters = MongoUpdate.parseArrayFilters(BsonArray.parse("[{e: {$gte: 2}}]"));
        assertEquals(d("{_id: 1, g: [1, 0, 0]}"), MongoUpdate.apply(d("{_id: 1, g: [1, 2, 3]}"), d("{$set: {'g.$[e]': 0}}"), ctx));
        MongoUpdate.Ctx pos = new MongoUpdate.Ctx();
        pos.filter = d("{g: 2}");
        assertEquals(d("{_id: 1, g: [1, 9, 3]}"), MongoUpdate.apply(d("{_id: 1, g: [1, 2, 3]}"), d("{$set: {'g.$': 9}}"), pos));
    }

    private static List<BsonDocument> run(String pipeline, String... docs) {
        MongoAgg.Ctx ctx = new MongoAgg.Ctx();
        ctx.db = "t";
        ctx.coll = "c";
        return MongoAgg.run(BsonArray.parse(pipeline), Stream.of(docs).map(MongoEngineTest::d), ctx).collect(Collectors.toList());
    }

    @Test
    void aggregationGroupUnwindAndExpressions() {
        List<BsonDocument> out = run("[{$group: {_id: '$k', s: {$sum: '$v'}, a: {$avg: '$v'}, n: {$push: '$v'}}}, {$sort: {_id: 1}}]",
                "{k: 'a', v: 1}", "{k: 'b', v: 5}", "{k: 'a', v: 2}");
        assertEquals(d("{_id: 'a', s: 3, a: 1.5, n: [1, 2]}"), out.get(0));
        assertEquals(2, out.size());
        assertEquals(2, run("[{$unwind: '$t'}]", "{t: [1, 2]}").size());
        assertEquals(d("{r: 'HELLO', l: 5, m: [2, 4], c: 'x-y'}"),
                run("[{$project: {_id: 0, r: {$toUpper: '$s'}, l: {$strLenCP: '$s'}, m: {$map: {input: '$a', as: 'x', in: {$multiply: ['$$x', 2]}}}, c: {$concat: ['x', '-', 'y']}}}]",
                        "{s: 'hello', a: [1, 2]}").get(0));
        assertEquals(d("{c: 2}"), run("[{$match: {v: {$gt: 1}}}, {$count: 'c'}]", "{v: 1}", "{v: 2}", "{v: 3}").get(0));
        assertEquals(d("{_id: 0, count: 2}"), run("[{$bucket: {groupBy: '$v', boundaries: [0, 10], output: {count: {$sum: 1}}}}]", "{v: 1}", "{v: 2}").get(0));
    }

    @Test
    void expressionTypeConversionAndDates() {
        BsonValue v = run("[{$project: {_id: 0, i: {$toInt: '42'}, s: {$toString: 5.0}, d: {$dateToString: {date: {$toDate: {$numberLong: '0'}}, format: '%Y-%m-%d'}}, t: {$type: '$missing'}}}]", "{}")
                .get(0).get("d");
        assertEquals("1970-01-01", v.asString().getValue());
        assertEquals(d("{i: 42, s: '5', t: 'missing'}"), run("[{$project: {_id: 0, i: {$toInt: '42'}, s: {$toString: 5.0}, t: {$type: '$missing'}}}]", "{}").get(0));
        assertEquals(241, assertThrows(MongoCmdException.class, () -> run("[{$project: {x: {$toInt: 'abc'}}}]", "{}")).code);
    }

    @Test
    void jsonSchemaValidator() {
        var schema = MongoJsonSchema.compile(d("{bsonType: 'object', required: ['a'], properties: {a: {bsonType: 'int', minimum: 0}}}"));
        assertTrue(schema.test(d("{a: 1}")));
        assertFalse(schema.test(d("{a: -1}")));
        assertFalse(schema.test(d("{b: 1}")));
    }

    @Test
    void cursorBatchesAndPagination() {
        List<BsonDocument> docs = Stream.iterate(0, i -> i + 1).limit(250).map(i -> new BsonDocument("_id", new BsonInt32(i))).collect(Collectors.toList());
        BsonDocument first = MongoCursors.reply("t.c", docs.iterator(), null, null, -1, false, true);
        BsonDocument cursor = first.getDocument("cursor");
        assertEquals(101, cursor.getArray("firstBatch").size());
        long id = cursor.getInt64("id").getValue();
        assertTrue(id != 0);
        BsonDocument more = MongoCursors.getMore(id, "c", "t", 200L);
        assertEquals(149, more.getDocument("cursor").getArray("nextBatch").size());
        assertEquals(0, more.getDocument("cursor").getInt64("id").getValue());
    }
}
