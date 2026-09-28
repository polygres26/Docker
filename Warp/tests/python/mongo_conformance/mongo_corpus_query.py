"""Corpus part 1: find / query operators / projection / sort / count / distinct."""
import datetime

from bson import Binary, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.code import Code

from mongo_corpus import case

D = datetime.datetime

# A dataset with mixed types, nested documents, arrays of documents, nulls and missing fields.
Q_DATA = [
    {"_id": 1, "a": 1, "b": "x", "c": [1, 2, 3], "d": {"e": 1, "f": [{"g": 1}, {"g": 2}]}, "n": None, "t": True},
    {"_id": 2, "a": 2.5, "b": "y", "c": [], "d": {"e": 2}, "t": False},
    {"_id": 3, "a": -3, "b": "xy", "c": [3, 4], "d": None, "n": None},
    {"_id": 4, "a": "str", "b": None, "c": [[1, 2], [3]], "d": [1, 2, {"e": 5}]},
    {"_id": 5, "a": [1, 2], "b": "X", "c": "notarray", "d": {"e": [1, 2, 3]}},
    {"_id": 6, "a": Int64(5), "b": "", "c": [None], "d": {}, "t": 1},
    {"_id": 7, "a": Decimal128("2.5"), "b": "abc\ndef", "c": [1, 1, 2], "d": {"f": [{"g": 3, "h": 1}, {"g": 4}]}},
    {"_id": 8, "a": D(2020, 1, 1), "b": "a.b", "c": [10, 20, 30], "d": {"e": None}},
    {"_id": 9, "a": True, "b": "ABC", "c": [{"k": 1}, {"k": 2}, {"k": 3}], "d": {"e": {"z": 1}}},
    {"_id": 10, "a": None, "b": "1", "c": [5, [6, 7]], "d": {"e": [{"z": 1}, {"z": 2}]}},
    {"_id": 11, "a": 0, "b": "xyz", "c": [0.5, 1.5], "d": {"f": []}},
    {"_id": 12, "a": {"x": 1, "y": 2}, "b": "x", "c": [3, 3], "d": {"e": 1, "f": [{"g": 1}]}},
    {"_id": 13, "a": {"y": 2, "x": 1}, "b": "Y", "c": [2, 4, 6, 8]},
    {"_id": 14, "a": ObjectId("507f1f77bcf86cd799439011"), "b": "z", "c": [7]},
    {"_id": 15, "a": Binary(b"\x01\x02", 0), "b": "é", "c": [-1, 0, 1]},
    {"_id": 16, "a": Timestamp(1000, 1), "b": "10", "c": [9, 8, 7]},
    {"_id": 17, "a": MinKey(), "b": "b", "c": [1, 2]},
    {"_id": 18, "a": MaxKey(), "b": "c", "c": [2, 3]},
    {"_id": 19, "a": float("nan"), "b": "nan", "c": [float("nan")]},
    {"_id": 20, "a": float("inf"), "b": "inf", "c": [float("-inf")]},
    {"_id": 21, "a": 7, "b": "seven", "c": [7, 7, 7], "d": {"e": 7}, "t": False, "arr2": [{"x": 1, "y": 1}, {"x": 2, "y": 2}]},
    {"_id": 22, "a": 8, "b": "eight", "c": [8], "d": {"e": 8}, "arr2": [{"x": 1, "y": 2}, {"x": 2, "y": 1}]},
]

FILTERS = [
    # equality / null / missing
    {}, {"a": 1}, {"a": 1.0}, {"a": Int64(1)}, {"a": Decimal128("1")}, {"a": 2.5}, {"a": "str"}, {"a": None}, {"n": None}, {"zz": None},
    {"a": {"$eq": 1}}, {"a": {"$eq": None}}, {"a": {"$ne": 1}}, {"a": {"$ne": None}}, {"n": {"$ne": None}}, {"b": "x"}, {"b": {"$ne": "x"}},
    {"a": [1, 2]}, {"a": {"x": 1, "y": 2}}, {"a": {"y": 2, "x": 1}}, {"c": 1}, {"c": [1, 2, 3]}, {"c": [3, 4]}, {"c": []}, {"c": [[1, 2], [3]]},
    {"c": [1, 2]}, {"c": {"$eq": [1, 2]}}, {"t": True}, {"t": 1}, {"t": False}, {"t": {"$ne": True}}, {"a": True},
    {"a": D(2020, 1, 1)}, {"a": ObjectId("507f1f77bcf86cd799439011")}, {"a": Binary(b"\x01\x02", 0)}, {"a": Timestamp(1000, 1)},
    {"a": float("nan")}, {"a": float("inf")}, {"a": MinKey()}, {"a": MaxKey()},
    # comparison and type bracketing
    {"a": {"$gt": 1}}, {"a": {"$gte": 1}}, {"a": {"$lt": 1}}, {"a": {"$lte": 1}}, {"a": {"$gt": 1, "$lt": 6}}, {"a": {"$gte": 2.5, "$lte": 5}},
    {"a": {"$gt": "a"}}, {"a": {"$lt": "z"}}, {"a": {"$gt": None}}, {"a": {"$gte": None}}, {"a": {"$lt": None}}, {"a": {"$lte": None}},
    {"a": {"$gt": True}}, {"a": {"$lt": D(2021, 1, 1)}}, {"a": {"$gt": {"x": 0}}}, {"a": {"$lt": {"z": 0}}}, {"a": {"$gt": [1]}},
    {"a": {"$gt": float("nan")}}, {"a": {"$gte": float("nan")}}, {"a": {"$lte": float("inf")}}, {"a": {"$gt": float("-inf")}},
    {"a": {"$gt": MinKey()}}, {"a": {"$lt": MaxKey()}}, {"a": {"$gte": MaxKey()}}, {"c": {"$gt": 2}}, {"c": {"$lt": 1}}, {"c": {"$gte": 3, "$lte": 3}},
    {"c": {"$gt": 1, "$lt": 3}}, {"c": {"$gt": [1]}}, {"c": {"$lt": [2]}}, {"b": {"$gt": "x"}}, {"b": {"$lt": "b"}}, {"b": {"$gte": "X", "$lte": "x"}},
    # in / nin
    {"a": {"$in": [1, 2.5]}}, {"a": {"$in": []}}, {"a": {"$in": [None]}}, {"a": {"$in": [None, 1]}}, {"a": {"$nin": [1, 2.5]}}, {"a": {"$nin": []}},
    {"c": {"$in": [1]}}, {"c": {"$in": [[1, 2, 3]]}}, {"c": {"$in": [[3, 4], 7]}}, {"c": {"$nin": [1, 2]}}, {"b": {"$in": ["x", "y"]}},
    {"b": {"$in": [{"$regex": "^x"}]}}, {"b": {"$in": [Regex("^x")]}}, {"b": {"$in": [Regex("^X", "i"), "z"]}}, {"n": {"$in": [None]}}, {"zz": {"$in": [None]}},
    {"zz": {"$nin": [None]}}, {"a": {"$in": 1}}, {"a": {"$nin": "x"}}, {"a": {"$in": [{"$gt": 1}]}},
    # logical
    {"$and": [{"a": {"$gt": 0}}, {"b": "x"}]}, {"$or": [{"a": 1}, {"b": "y"}]}, {"$nor": [{"a": 1}, {"b": "y"}]}, {"$and": []}, {"$or": []}, {"$nor": []},
    {"$and": [{"a": 1}]}, {"$or": [{"a": 1}, {"a": 1}]}, {"$and": [{"$or": [{"a": 1}, {"a": 2.5}]}, {"b": {"$ne": "x"}}]}, {"$or": [{"a": {"$exists": False}}, {"a": None}]},
    {"$not": {"a": 1}}, {"a": {"$not": {"$gt": 1}}}, {"a": {"$not": {"$eq": 1}}}, {"a": {"$not": Regex("^s")}}, {"a": {"$not": {"$in": [1, 2]}}}, {"a": {"$not": 5}},
    {"c": {"$not": {"$size": 2}}}, {"c": {"$not": {"$elemMatch": {"$gt": 2}}}}, {"a": {"$not": {"$not": {"$gt": 1}}}},
    # exists
    {"a": {"$exists": True}}, {"a": {"$exists": False}}, {"n": {"$exists": True}}, {"n": {"$exists": False}}, {"zz": {"$exists": True}}, {"zz": {"$exists": False}},
    {"d.e": {"$exists": True}}, {"d.e": {"$exists": False}}, {"d.f.g": {"$exists": True}}, {"c.k": {"$exists": True}}, {"a": {"$exists": 1}}, {"a": {"$exists": 0}},
    {"a": {"$exists": "x"}}, {"a": {"$exists": None}}, {"a": {"$exists": True, "$ne": None}},
    # type
    {"a": {"$type": "int"}}, {"a": {"$type": "long"}}, {"a": {"$type": "double"}}, {"a": {"$type": "decimal"}}, {"a": {"$type": "number"}}, {"a": {"$type": "string"}},
    {"a": {"$type": "object"}}, {"a": {"$type": "array"}}, {"a": {"$type": "bool"}}, {"a": {"$type": "date"}}, {"a": {"$type": "null"}}, {"a": {"$type": "objectId"}},
    {"a": {"$type": "binData"}}, {"a": {"$type": "timestamp"}}, {"a": {"$type": "minKey"}}, {"a": {"$type": "maxKey"}}, {"a": {"$type": 16}}, {"a": {"$type": 18}},
    {"a": {"$type": [16, 1]}}, {"a": {"$type": ["string", "null"]}}, {"c": {"$type": "array"}}, {"c": {"$type": "int"}}, {"c": {"$type": "null"}}, {"a": {"$type": "regex"}},
    {"a": {"$type": "bogus"}}, {"a": {"$type": 99}}, {"a": {"$type": 0}}, {"a": {"$type": -1}}, {"a": {"$type": 127}}, {"a": {"$type": {}}}, {"d.e": {"$type": "array"}},
    # regex
    {"b": Regex("^x")}, {"b": {"$regex": "^x"}}, {"b": {"$regex": "^X", "$options": "i"}}, {"b": {"$regex": "x$"}}, {"b": {"$regex": "^abc.def$"}},
    {"b": {"$regex": "^abc.def$", "$options": "s"}}, {"b": {"$regex": "^def", "$options": "m"}}, {"b": {"$regex": "a b", "$options": "x"}}, {"b": {"$regex": "a\\.b"}},
    {"b": {"$regex": "["}}, {"b": {"$regex": "x", "$options": "z"}}, {"b": {"$regex": 5}}, {"a": {"$regex": "^s"}}, {"c": {"$regex": "^n"}}, {"c": Regex("1")},
    {"b": {"$not": Regex("^x")}}, {"b": Regex("é")}, {"b": Regex("^.$")}, {"b": {"$regex": ""}}, {"b": {"$options": "i"}}, {"b": Regex("X", "i")},
    {"b": {"$regex": Regex("^x")}}, {"b": {"$regex": Regex("^x", "i"), "$options": "i"}},
    # mod
    {"a": {"$mod": [2, 1]}}, {"a": {"$mod": [3, 0]}}, {"a": {"$mod": [2]}}, {"a": {"$mod": [0, 1]}}, {"a": {"$mod": "x"}}, {"a": {"$mod": [2, 1, 0]}}, {"a": {"$mod": [2.5, 1]}},
    {"a": {"$mod": [-3, -1]}}, {"c": {"$mod": [2, 0]}}, {"a": {"$mod": ["a", 1]}}, {"a": {"$mod": [1, "b"]}},
    # size / all / elemMatch
    {"c": {"$size": 0}}, {"c": {"$size": 1}}, {"c": {"$size": 2}}, {"c": {"$size": 3}}, {"c": {"$size": -1}}, {"c": {"$size": "a"}}, {"c": {"$size": 2.0}},
    {"c": {"$size": 2.5}}, {"a": {"$size": 2}}, {"d.f": {"$size": 2}}, {"d.f": {"$size": 0}},
    {"c": {"$all": [1, 2]}}, {"c": {"$all": [1]}}, {"c": {"$all": []}}, {"c": {"$all": [[1, 2], [3]]}}, {"c": {"$all": [[1, 2, 3]]}}, {"c": {"$all": [3, 3]}},
    {"c": {"$all": [{"$elemMatch": {"$gt": 1}}]}}, {"a": {"$all": [1]}}, {"a": {"$all": [1, 2]}}, {"a": {"$all": 1}}, {"b": {"$all": ["x"]}}, {"c": {"$all": [None]}},
    {"c": {"$all": [{"k": 1}]}}, {"c": {"$all": [Regex("^1")]}}, {"c": {"$all": [{"$gt": 1}]}},
    {"c": {"$elemMatch": {"$gt": 2}}}, {"c": {"$elemMatch": {"$gt": 1, "$lt": 3}}}, {"c": {"$elemMatch": {"k": 2}}}, {"c": {"$elemMatch": {"k": {"$gt": 2}}}},
    {"d.f": {"$elemMatch": {"g": 1}}}, {"d.f": {"$elemMatch": {"g": 3, "h": 1}}}, {"d.f": {"$elemMatch": {"g": {"$in": [2, 4]}}}}, {"c": {"$elemMatch": {"$in": [1, 5]}}},
    {"c": {"$elemMatch": {"$eq": 1}}}, {"c": {"$elemMatch": {}}}, {"c": {"$elemMatch": 1}}, {"a": {"$elemMatch": {"$gt": 1}}}, {"c": {"$elemMatch": {"$and": [{"k": 1}]}}},
    {"c": {"$elemMatch": {"$or": [{"k": 1}, {"k": 3}]}}}, {"c": {"$elemMatch": {"$exists": True}}}, {"arr2": {"$elemMatch": {"x": 1, "y": 2}}},
    {"arr2": {"$elemMatch": {"x": 1, "y": 1}}}, {"arr2.x": 1, "arr2.y": 2}, {"arr2": {"$elemMatch": {"x": {"$gt": 1}, "y": {"$lt": 2}}}}, {"c": {"$elemMatch": {"$not": {"$gt": 2}}}},
    {"c": {"$elemMatch": {"$type": "array"}}}, {"c": {"$elemMatch": {"$size": 2}}}, {"c": {"$elemMatch": {"$elemMatch": {"$gt": 5}}}},
    # dotted paths / arrays traversal
    {"d.e": 1}, {"d.e": {"$gt": 1}}, {"d.e": None}, {"d.e": [1, 2, 3]}, {"d.e": 2}, {"d.e": 5}, {"d.f.g": 1}, {"d.f.g": {"$gt": 2}}, {"d.f.0.g": 1}, {"d.f.1.g": 2},
    {"d.f.g": [1, 2]}, {"d.0": 1}, {"d.2.e": 5}, {"d.e.z": 1}, {"d.e.0.z": 1}, {"d.e.1.z": 2}, {"d.e.1": 2}, {"c.0": 1}, {"c.1": 2}, {"c.5": 1}, {"c.0.0": 1}, {"c.1.1": 7},
    {"c.k": 1}, {"c.k": {"$gt": 1}}, {"c.0.k": 1}, {"a.x": 1}, {"a.y": 2}, {"a.z": None}, {"a.0": 1}, {"a.1": 2}, {"a.0": {"$gt": 1}}, {"d.f.h": 1}, {"d.f.h": None},
    {"d..e": 1}, {".d": 1}, {"d.": 1}, {"d.e.f.g.h": None}, {"zz.yy": None}, {"zz.yy": {"$exists": False}}, {"": 1}, {"arr2.x": {"$all": [1, 2]}}, {"arr2.0.x": 1},
    {"arr2.x": {"$size": 1}},
    # expr
    {"$expr": {"$eq": ["$a", 1]}}, {"$expr": {"$gt": ["$a", 1]}}, {"$expr": {"$eq": ["$a", "$b"]}}, {"$expr": {"$and": [{"$gt": ["$a", 0]}, {"$lt": ["$a", 5]}]}},
    {"$expr": {"$in": ["$a", [1, 2.5]]}}, {"$expr": {"$isArray": "$a"}}, {"$expr": {"$eq": [{"$type": "$a"}, "string"]}}, {"$expr": True}, {"$expr": False}, {"$expr": 1}, {"$expr": 0},
    {"$expr": "$a"}, {"$expr": {"$gt": [{"$size": {"$ifNull": ["$c", []]}}, 2]}}, {"$expr": {"$eq": ["$d.e", 1]}}, {"$expr": {"$eq": ["$zz", None]}}, {"$expr": {"$lte": ["$zz", None]}},
    {"$expr": {"$cmp": ["$a", 1]}}, {"$expr": {"$foo": 1}}, {"$expr": {"$eq": ["$a"]}}, {"a": 1, "$expr": {"$gt": ["$_id", 0]}}, {"$or": [{"$expr": {"$eq": ["$_id", 1]}}, {"a": 2.5}]},
    {"$expr": {"$eq": ["$$ROOT._id", 3]}}, {"$expr": {"$eq": ["$$CURRENT.b", "x"]}}, {"$expr": {"$eq": ["$$nope", 1]}},
    # bit operators
    {"a": {"$bitsAllSet": [0]}}, {"a": {"$bitsAnySet": [1]}}, {"a": {"$bitsAllClear": [0]}}, {"a": {"$bitsAnyClear": [0, 1]}}, {"a": {"$bitsAllSet": 5}}, {"a": {"$bitsAllSet": Binary(b"\x05")}},
    {"a": {"$bitsAllSet": [-1]}}, {"a": {"$bitsAllSet": "x"}}, {"a": {"$bitsAnySet": []}},
    # jsonSchema / misc / invalid
    {"$jsonSchema": {"bsonType": "object", "required": ["a"], "properties": {"a": {"bsonType": "number"}}}}, {"$jsonSchema": {"properties": {"b": {"maxLength": 1}}}},
    {"$jsonSchema": {"properties": {"c": {"bsonType": "array", "minItems": 2}}}}, {"$jsonSchema": {"bogus": 1}}, {"$jsonSchema": 1}, {"a": {"$jsonSchema": {}}},
    {"$comment": "hello", "a": 1}, {"a": {"$comment": "x"}}, {"$where": "this.a == 1"}, {"$text": {"$search": "x"}}, {"$foo": 1}, {"a": {"$foo": 1}}, {"a": {"$gt": 1, "$foo": 1}},
    {"a": {"$eq": {"$gt": 1}}}, {"a": {"$ne": Regex("x")}}, {"a": {"$in": [{"$in": [1]}]}}, {"a": {"$near": [0, 0]}}, {"a": {"$geoWithin": {"$box": [[0, 0], [1, 1]]}}},
    {"$and": 1}, {"$or": [1]}, {"$and": [[]]}, {"$nor": {}}, {"a": {"$not": 1}}, {"a": {"$not": {}}}, {"a": {}}, {"a": {"$exists": True, "$gt": None}},
    {"a": {"$lt": 5, "$gt": 1, "$ne": 2.5}}, {"_id": {"$in": [1, 2, 3]}}, {"_id": {"$gt": 20}}, {"_id": 5}, {"_id": {"$eq": 5}}, {"_id": {"$in": []}}, {"_id": {"$in": [5, 5.0, Int64(5)]}},
    {"_id": 5.0}, {"_id": Int64(5)}, {"_id": Decimal128("5")}, {"_id": {"$ne": 5}}, {"_id": {"$not": {"$gt": 3}}}, {"_id": "5"},
    {"_id": {"$in": [1, 2], "$nin": [2]}}, {"_id": {"$exists": True}}, {"_id": {"$type": "int"}}, {"_id": {"$mod": [2, 0]}}, {"$and": [{"_id": 1}, {"_id": 2}]}, {"$or": [{"_id": 1}, {"_id": 2}]},
]


def _chunks(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


for _i, _chunk in enumerate(_chunks(FILTERS, 25)):
    def _make(chunk, i):
        @case("find_filters_%02d" % i)
        def _(e):
            c = e.c
            c.insert_many(Q_DATA)
            for j, flt in enumerate(chunk):
                e.find("f%02d" % j, c, flt, sort=[("_id", 1)])
        return _
    _make(_chunk, _i)


@case("find_natural_order_and_defaults")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "v": i % 4} for i in range(30, 0, -1)])
    e.find("all", c, {})
    e.find("filter", c, {"v": 2})
    e.find("none_filter", c, None)
    e.find("limit", c, {}, limit=5)
    e.find("skip", c, {}, skip=25)
    e.find("skip_limit", c, {}, skip=3, limit=2)
    e.find("neg_limit", c, {}, limit=-3)
    e.find("limit_over", c, {}, limit=100)
    e.find("skip_over", c, {}, skip=100)
    e.cmd("neg_skip", {"find": "c", "skip": -1})
    e.cmd("neg_limit_cmd", {"find": "c", "limit": -1})
    e.cmd("neg_batch", {"find": "c", "batchSize": -1})
    e.cmd("bad_filter_type", {"find": "c", "filter": "x"})
    e.cmd("bad_sort_type", {"find": "c", "sort": 5})
    e.cmd("bad_projection_type", {"find": "c", "projection": []})
    e.cmd("unknown_field", {"find": "c", "bogusField": 1})
    e.cmd("bad_coll_type", {"find": 1})
    e.cmd("empty_coll", {"find": ""})
    e.cmd("dollar_coll", {"find": "a$b"})
    e.find("find_one_missing", e.coll("nonexistent"), {"a": 1})
    e.op("count_after", lambda: c.count_documents({"v": 1}))


@case("find_sort")
def _(e):
    c = e.c
    docs = []
    vals = [1, 2.5, -1, "b", "a", "B", None, True, False, D(2020, 1, 1), ObjectId("507f1f77bcf86cd799439011"), {"x": 1}, {"x": 2}, {"y": 0},
            [1, 2], [0, 5], [], [[1]], Int64(3), Decimal128("2"), Binary(b"a", 0), Binary(b"b", 0), Timestamp(5, 1), Regex("a"), MinKey(), MaxKey(),
            float("nan"), float("-inf"), float("inf"), "", "é", "\U0001F600", {"x": [1]}]
    for i, v in enumerate(vals):
        docs.append({"_id": i, "v": v})
    docs.append({"_id": 900})  # missing v
    c.insert_many(docs)
    e.find("asc", c, {}, sort=[("v", 1), ("_id", 1)])
    e.find("desc", c, {}, sort=[("v", -1), ("_id", 1)])
    e.find("proj", c, {}, projection={"v": 1, "_id": 0}, sort=[("v", 1), ("_id", 1)], limit=50)
    c2 = e.coll("c2")
    c2.insert_many([{"_id": 1, "a": 1, "b": 2}, {"_id": 2, "a": 1, "b": 1}, {"_id": 3, "a": 2, "b": 2}, {"_id": 4, "a": 2, "b": 1}, {"_id": 5, "a": None}, {"_id": 6}])
    e.find("compound", c2, {}, sort=[("a", -1), ("b", 1), ("_id", 1)])
    e.find("compound2", c2, {}, sort=[("a", 1), ("b", -1), ("_id", 1)])
    e.find("sort_skip_limit", c2, {}, sort=[("a", 1), ("_id", 1)], skip=1, limit=3)
    c3 = e.coll("c3")
    c3.insert_many([{"_id": 1, "a": [5, 1, 3]}, {"_id": 2, "a": [2, 4]}, {"_id": 3, "a": []}, {"_id": 4, "a": 3}, {"_id": 5}, {"_id": 6, "a": [[7], 1]}, {"_id": 7, "a": [{"x": 2}, {"x": 1}]}])
    e.find("array_asc", c3, {}, sort=[("a", 1), ("_id", 1)])
    e.find("array_desc", c3, {}, sort=[("a", -1), ("_id", 1)])
    e.find("array_dotted", c3, {}, sort=[("a.x", 1), ("_id", 1)])
    e.find("array_dotted_desc", c3, {}, sort=[("a.x", -1), ("_id", 1)])
    e.find("array_index", c3, {}, sort=[("a.0", 1), ("_id", 1)])
    e.cmd("sort_bad_dir", {"find": "c3", "sort": {"a": 2}})
    e.cmd("sort_zero", {"find": "c3", "sort": {"a": 0}})
    e.cmd("sort_string_dir", {"find": "c3", "sort": {"a": "asc"}})
    e.cmd("sort_meta", {"find": "c3", "sort": {"a": {"$meta": "textScore"}}})
    e.cmd("sort_dollar", {"find": "c3", "sort": {"$a": 1}})
    e.cmd("sort_empty", {"find": "c3", "sort": {}})
    e.cmd("sort_parallel", {"find": "c3", "sort": {"a": 1, "_id": 1}, "limit": 2})
    c4 = e.coll("c4")
    c4.insert_many([{"_id": 1, "s": "a"}, {"_id": 2, "s": "B"}, {"_id": 3, "s": "b"}, {"_id": 4, "s": "A"}, {"_id": 5, "s": "é"}, {"_id": 6, "s": "e"}, {"_id": 7, "s": "10"}, {"_id": 8, "s": "9"}])
    e.find("collation_default", c4, {}, sort=[("s", 1)])
    e.find("collation_en", c4, {}, sort=[("s", 1), ("_id", 1)], collation={"locale": "en"})
    e.find("collation_en_s2", c4, {}, sort=[("s", 1), ("_id", 1)], collation={"locale": "en", "strength": 2})
    e.find("collation_en_s1", c4, {}, sort=[("s", 1), ("_id", 1)], collation={"locale": "en", "strength": 1})
    e.find("collation_numeric", c4, {}, sort=[("s", 1), ("_id", 1)], collation={"locale": "en", "numericOrdering": True})
    e.find("collation_eq", c4, {"s": "a"}, sort=[("_id", 1)], collation={"locale": "en", "strength": 2})
    e.find("collation_simple", c4, {"s": "a"}, sort=[("_id", 1)], collation={"locale": "simple"})
    e.find("collation_bad_locale", c4, {}, collation={"locale": "zz_bogus"})
    e.find("collation_bad_strength", c4, {}, collation={"locale": "en", "strength": 9})
    e.find("collation_missing_locale", c4, {}, collation={"strength": 1})


@case("find_projection")
def _(e):
    c = e.c
    c.insert_many([
        {"_id": 1, "a": 1, "b": {"c": 1, "d": 2, "e": {"f": 3}}, "arr": [1, 2, 3, 4, 5], "docs": [{"x": 1, "y": 2}, {"x": 3, "y": 4}], "s": "str"},
        {"_id": 2, "a": 2, "b": {"c": 5}, "arr": [], "docs": [], "s": None},
        {"_id": 3, "a": 3, "b": 5, "arr": "no", "docs": [1, {"x": 9, "y": 8}, [{"x": 7}]]},
        {"_id": 4, "b": None, "docs": [{"x": 5}, {"x": 6}]},
        {"_id": 5, "arr": [[1, 2], [3, 4]], "n": {"m": {"o": 1, "p": 2}}},
    ])
    projs = [
        {"a": 1}, {"a": 1, "_id": 0}, {"_id": 0}, {"_id": 1}, {"a": 0}, {"a": 0, "b": 0}, {"a": 0, "_id": 0}, {"b.c": 1}, {"b.c": 0}, {"b.e.f": 1}, {"b.e.f": 0}, {"b.c": 1, "b.d": 1},
        {"b": 1, "b.c": 1}, {"b.c": 1, "b": 1}, {"b": 0, "b.c": 0}, {"docs.x": 1}, {"docs.x": 0}, {"docs.x": 1, "docs.y": 1}, {"docs.0": 1}, {"arr.0": 1}, {"n.m.o": 1}, {"n.m": 0},
        {"a": True}, {"a": False}, {"a": 2}, {"a": -1}, {"a": 0.5}, {"a": "x"}, {"a": None}, {"a": [1]}, {"a": {"$literal": 1}}, {"a": "$b"}, {"z": "$a"}, {"a": {"$add": ["$a", 1]}},
        {"a": 1, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "_id": 0, "b": 1}, {"_id": 0, "a": 0}, {},
        {"arr": {"$slice": 2}}, {"arr": {"$slice": -2}}, {"arr": {"$slice": [1, 2]}}, {"arr": {"$slice": [-3, 2]}}, {"arr": {"$slice": 0}}, {"arr": {"$slice": 10}}, {"arr": {"$slice": [10, 2]}},
        {"arr": {"$slice": [1, 0]}}, {"arr": {"$slice": [1, -1]}}, {"arr": {"$slice": "x"}}, {"arr": {"$slice": [1]}}, {"a": 1, "arr": {"$slice": 2}}, {"a": 0, "arr": {"$slice": 2}},
        {"docs": {"$slice": 1}}, {"docs.x": {"$slice": 1}},
        {"docs": {"$elemMatch": {"x": 3}}}, {"docs": {"$elemMatch": {"x": {"$gt": 0}}}}, {"docs": {"$elemMatch": {"x": 99}}}, {"a": 1, "docs": {"$elemMatch": {"x": 3}}}, {"docs": {"$elemMatch": 1}},
        {"arr": {"$elemMatch": {"$gt": 2}}}, {"a": 0, "docs": {"$elemMatch": {"x": 3}}},
        {"docs.$": 1}, {"arr.$": 1}, {"a.$": 1},
        {"$a": 1}, {"a.": 1}, {".a": 1}, {"a..b": 1}, {"a": {"$bogus": 1}}, {"a": {"$meta": "textScore"}},
    ]
    for i, p in enumerate(projs):
        e.find("p%02d" % i, c, {}, projection=p, sort=[("_id", 1)])
    e.find("positional_arr", c, {"arr": {"$gt": 2}}, projection={"arr.$": 1}, sort=[("_id", 1)])
    e.find("positional_docs", c, {"docs.x": 3}, projection={"docs.$": 1}, sort=[("_id", 1)])
    e.find("positional_elemmatch", c, {"docs": {"$elemMatch": {"x": 3}}}, projection={"docs.$": 1}, sort=[("_id", 1)])
    e.find("positional_nomatch", c, {"a": 1}, projection={"arr.$": 1}, sort=[("_id", 1)])
    e.find("projection_list", c, {}, projection=["a", "s"], sort=[("_id", 1)])
    e.find("projection_expr_concat", c, {}, projection={"r": {"$concat": ["$s", "!"]}}, sort=[("_id", 1)])


@case("count_distinct_estimated")
def _(e):
    c = e.c
    e.op("count_missing_coll", lambda: e.coll("nope").count_documents({}))
    e.op("estimated_missing_coll", lambda: e.coll("nope").estimated_document_count())
    e.cmd("count_missing_cmd", {"count": "nope"})
    e.cmd("distinct_missing", {"distinct": "nope", "key": "a"})
    c.insert_many(Q_DATA)
    e.op("estimated", lambda: c.estimated_document_count())
    e.op("count_all", lambda: c.count_documents({}))
    for i, flt in enumerate([{"a": 1}, {"c": 1}, {"a": {"$gt": 1}}, {"b": {"$regex": "^x"}}, {"$or": [{"a": 1}, {"b": "y"}]}, {"zz": None}, {"_id": {"$in": [1, 2, 99]}}, {"$expr": {"$gt": ["$_id", 10]}}]):
        e.op("count_%d" % i, lambda flt=flt: c.count_documents(flt))
    e.op("count_skip", lambda: c.count_documents({}, skip=5))
    e.op("count_limit", lambda: c.count_documents({}, limit=5))
    e.op("count_skip_limit", lambda: c.count_documents({}, skip=20, limit=5))
    e.op("count_limit_zero", lambda: c.count_documents({}, limit=0))
    e.op("count_bad_filter", lambda: c.count_documents({"$foo": 1}))
    e.cmd("count_cmd", {"count": "c"})
    e.cmd("count_cmd_query", {"count": "c", "query": {"a": 1}})
    e.cmd("count_cmd_limit", {"count": "c", "limit": 3})
    e.cmd("count_cmd_neg_limit", {"count": "c", "limit": -3})
    e.cmd("count_cmd_skip", {"count": "c", "skip": 20})
    e.cmd("count_cmd_neg_skip", {"count": "c", "skip": -1})
    e.cmd("count_cmd_bad_query", {"count": "c", "query": 5})
    e.cmd("count_cmd_unknown", {"count": "c", "bogus": 1})
    e.cmd("count_cmd_bad_coll", {"count": 1})
    for key in ["a", "b", "c", "d", "d.e", "d.f.g", "n", "t", "zz", "c.k", "arr2.x", "_id", "a.x", "c.0", "d.0"]:
        e.op("distinct_" + key.replace(".", "_"), lambda key=key: c.distinct(key))
    e.op("distinct_query", lambda: c.distinct("b", {"a": {"$gt": 0}}))
    e.op("distinct_dupes", lambda: c.distinct("c", {"_id": 7}))
    e.op("distinct_collation", lambda: c.distinct("b", collation={"locale": "en", "strength": 2}))
    e.cmd("distinct_bad_key", {"distinct": "c", "key": 1})
    e.cmd("distinct_empty_key", {"distinct": "c", "key": ""})
    e.cmd("distinct_no_key", {"distinct": "c"})
    e.cmd("distinct_bad_query", {"distinct": "c", "key": "a", "query": 5})
    e.cmd("distinct_unknown", {"distinct": "c", "key": "a", "bogus": 1})
    e.cmd("distinct_raw", {"distinct": "c", "key": "b"})


@case("find_misc_types_roundtrip")
def _(e):
    c = e.c
    docs = [
        {"_id": 1, "i32": 5, "i64": Int64(5), "dbl": 5.0, "dec": Decimal128("5.000"), "s": "x", "bin": Binary(b"\x00\x01", 0), "bin4": Binary(b"0123456789abcdef", 4),
         "bin128": Binary(b"custom", 128), "oid": ObjectId("507f1f77bcf86cd799439011"), "dt": D(1970, 1, 1, 0, 0, 1, 500000), "re": Regex("ab+", "im"), "ts": Timestamp(1, 2),
         "min": MinKey(), "max": MaxKey(), "code": Code("f()"), "codews": Code("g(x)", {"x": 1}), "null": None, "arr": [1, [2, [3]]], "nested": {"a": {"b": {"c": []}}}, "bool": False,
         "neg0": -0.0, "big": Int64(2**62), "small": Int64(-2**62), "nan": float("nan"), "inf": float("inf"), "empty": {}, "emptys": "", "uni": "héllo 世界 \U0001F600"},
    ]
    c.insert_many(docs)
    e.find("roundtrip", c, {})
    e.find("proj_types", c, {}, projection={"i32": 1, "i64": 1, "dbl": 1, "dec": 1})
    e.find("query_int_as_long", c, {"i32": Int64(5)})
    e.find("query_dec", c, {"dbl": Decimal128("5")})
    e.find("query_neg0", c, {"neg0": 0})
    e.find("query_bin4", c, {"bin4": Binary(b"0123456789abcdef", 4)})
    e.find("query_bin_sub", c, {"bin": Binary(b"\x00\x01", 5)})
    e.find("query_date_ms", c, {"dt": D(1970, 1, 1, 0, 0, 1, 500000)})
    e.find("query_re_value", c, {"re": Regex("ab+", "im")})
    e.find("query_code", c, {"code": Code("f()")})
    e.find("query_uni", c, {"uni": "héllo 世界 \U0001F600"})


@case("find_large_and_batches")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "pad": "x" * 100, "v": i % 7} for i in range(1, 501)])
    e.op("all_count", lambda: len(list(c.find({}))))
    e.op("batch_size", lambda: len(list(c.find({}, batch_size=7))))
    e.find("limit_batch", c, {"v": 3}, sort=[("_id", 1)], limit=10, batch_size=3)
    e.cmd("first_batch_default", {"find": "c", "sort": {"_id": 1}}, mask=())
    e.cmd("first_batch_10", {"find": "c", "sort": {"_id": 1}, "batchSize": 10, "projection": {"pad": 0}})
    e.cmd("first_batch_0", {"find": "c", "batchSize": 0})
    e.cmd("first_batch_exact", {"find": "c", "batchSize": 500, "projection": {"pad": 0}})
    e.cmd("single_batch", {"find": "c", "batchSize": 5, "singleBatch": True, "projection": {"pad": 0}})
    e.cmd("limit_eq_batch", {"find": "c", "batchSize": 5, "limit": 5, "projection": {"pad": 0}})
    e.cmd("limit_lt_batch", {"find": "c", "batchSize": 5, "limit": 3, "projection": {"pad": 0}})
    e.cmd("limit_gt_batch", {"find": "c", "batchSize": 5, "limit": 7, "projection": {"pad": 0}})
    e.cmd("empty_result", {"find": "c", "filter": {"v": 99}, "batchSize": 5})
    e.cmd("no_such_coll", {"find": "nosuch", "batchSize": 5})
    e.cmd("exact_batch_boundary", {"find": "c", "filter": {"_id": {"$lte": 4}}, "batchSize": 4, "projection": {"pad": 0}, "sort": {"_id": 1}})
    e.cmd("exact_batch_boundary_unsorted", {"find": "c", "filter": {"_id": {"$lte": 4}}, "batchSize": 4, "projection": {"pad": 0}})
    e.cmd("exact_default_101", {"find": "c", "filter": {"_id": {"$lte": 101}}, "projection": {"pad": 0}})
    e.cmd("default_102", {"find": "c", "filter": {"_id": {"$lte": 102}}, "projection": {"pad": 0}})
