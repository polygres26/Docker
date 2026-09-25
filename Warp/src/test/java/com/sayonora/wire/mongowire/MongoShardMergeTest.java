package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/** Exact merging of per-backend {@code $group} partials, and the pipelines refused on several backends. */
class MongoShardMergeTest {

    private static MongoAggregationTranslator.ShardMerge plan(String pipelineJson) {
        BsonArray pipeline = BsonArray.parse(pipelineJson);
        return MongoAggregationTranslator.translateForShards("\"db\".\"c\"", pipeline).merge();
    }

    @Test
    void sumCountAndAvgAreRebuiltFromPartials() {
        var merge = plan("[{\"$group\": {\"_id\": \"$g\", \"c\": {\"$sum\": 1}, \"t\": {\"$sum\": \"$n\"}, "
                + "\"a\": {\"$avg\": \"$n\"}}}, {\"$sort\": {\"_id\": 1}}]");
        List<Document> shard1 = List.of(
                Document.parse("{\"_id\": 1, \"c\": 2, \"t\": 10, \"a__sum\": 10, \"a__cnt\": 2}"),
                Document.parse("{\"_id\": 2, \"c\": 1, \"t\": 5, \"a__sum\": 5, \"a__cnt\": 1}"));
        List<Document> shard2 = List.of(
                Document.parse("{\"_id\": 1, \"c\": 4, \"t\": 20, \"a__sum\": 20, \"a__cnt\": 4}"));
        List<Document> out = MongoShardMerge.merge(merge, List.of(shard1, shard2));
        assertEquals(2, out.size());
        assertEquals(1, out.get(0).get("_id"));
        assertEquals(6, ((Number) out.get(0).get("c")).intValue());
        assertEquals(30, ((Number) out.get(0).get("t")).intValue());
        assertEquals(5.0, ((Number) out.get(0).get("a")).doubleValue(), 1e-9);
        assertEquals(2, out.get(1).get("_id"));
    }

    @Test
    void sortDescendingAndLimitApplyAfterTheMerge() {
        var merge = plan("[{\"$group\": {\"_id\": \"$g\", \"c\": {\"$sum\": 1}}}, {\"$sort\": {\"c\": -1}}, {\"$limit\": 1}]");
        List<Document> out = MongoShardMerge.merge(merge, List.of(
                List.of(Document.parse("{\"_id\": \"a\", \"c\": 3}"), Document.parse("{\"_id\": \"b\", \"c\": 1}")),
                List.of(Document.parse("{\"_id\": \"b\", \"c\": 5}"))));
        assertEquals(1, out.size());
        assertEquals("b", out.get(0).get("_id"));
        assertEquals(6, ((Number) out.get(0).get("c")).intValue());
    }

    @Test
    void minAndMaxKeepTheExtreme() {
        var merge = plan("[{\"$group\": {\"_id\": null, \"lo\": {\"$min\": \"$n\"}, \"hi\": {\"$max\": \"$n\"}}}]");
        List<Document> out = MongoShardMerge.merge(merge, List.of(
                List.of(Document.parse("{\"_id\": null, \"lo\": \"3\", \"hi\": \"9\"}")),
                List.of(Document.parse("{\"_id\": null, \"lo\": \"1\", \"hi\": \"7\"}"))));
        assertEquals(1, out.size());
        assertEquals("1", out.get(0).get("lo"));
        assertEquals("9", out.get(0).get("hi"));
    }

    @Test
    void sortWithoutGroupIsRefusedOnSeveralBackends() {
        BsonArray pipeline = BsonArray.parse("[{\"$sort\": {\"n\": -1}}, {\"$limit\": 3}]");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> MongoAggregationTranslator.translateForShards("\"db\".\"c\"", pipeline));
        assertEquals(true, e.getMessage().contains("several backends"));
        // plain $match/$project concatenates fine
        assertEquals(null, MongoAggregationTranslator.translateForShards("\"db\".\"c\"",
                BsonArray.parse("[{\"$match\": {\"a\": 1}}]")).merge());
        // an unsupported accumulator is refused too
        assertThrows(IllegalArgumentException.class, () -> MongoAggregationTranslator.translateForShards("\"db\".\"c\"",
                BsonArray.parse("[{\"$group\": {\"_id\": \"$g\", \"x\": {\"$first\": \"$n\"}}}]")));
        BsonDocument unused = new BsonDocument();
        assertEquals(0, unused.size());
    }
}
