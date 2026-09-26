"""Corpus part 2: insert / update / delete / findAndModify / bulkWrite / upsert semantics."""
import datetime

from bson import Binary, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne, ReturnDocument

from mongo_corpus import case

D = datetime.datetime


@case("insert_basic")
def _(e):
    c = e.c
    e.op("one", lambda: c.insert_one({"_id": 1, "a": 1}))
    e.op("one_no_id", lambda: c.insert_one({"a": 2}))
    e.op("one_dup", lambda: c.insert_one({"_id": 1, "a": 3}))
    e.op("many", lambda: c.insert_many([{"_id": 10}, {"_id": 11}, {"_id": 12}]))
    e.op("many_ordered_dup", lambda: c.insert_many([{"_id": 20}, {"_id": 10}, {"_id": 21}]))
    e.op("many_unordered_dup", lambda: c.insert_many([{"_id": 30}, {"_id": 11}, {"_id": 31}, {"_id": 12}], ordered=False))
    e.op("many_no_ids", lambda: c.insert_many([{"a": 1}, {"a": 2}]) and 1)
    e.state("state")
    e.op("id_numeric_dup_double", lambda: c.insert_one({"_id": 1.0}))
    e.op("id_numeric_dup_long", lambda: c.insert_one({"_id": Int64(1)}))
    e.op("id_numeric_dup_decimal", lambda: c.insert_one({"_id": Decimal128("1")}))
    e.op("id_string_1", lambda: c.insert_one({"_id": "1"}))
    e.op("id_doc", lambda: c.insert_one({"_id": {"a": 1, "b": 2}}))
    e.op("id_doc_reordered", lambda: c.insert_one({"_id": {"b": 2, "a": 1}}))
    e.op("id_doc_dup", lambda: c.insert_one({"_id": {"a": 1, "b": 2}}))
    e.op("id_array", lambda: c.insert_one({"_id": [1, 2]}))
    e.op("id_null", lambda: c.insert_one({"_id": None}))
    e.op("id_null_dup", lambda: c.insert_one({"_id": None}))
    e.op("id_regex", lambda: c.insert_one({"_id": Regex("a")}))
    e.op("id_date", lambda: c.insert_one({"_id": D(2020, 1, 1)}))
    e.op("id_bin", lambda: c.insert_one({"_id": Binary(b"abc", 0)}))
    e.op("id_bool", lambda: c.insert_one({"_id": True}))
    e.op("id_minkey", lambda: c.insert_one({"_id": MinKey()}))
    e.op("id_objectid", lambda: c.insert_one({"_id": ObjectId("507f1f77bcf86cd799439011")}))
    e.op("id_objectid_dup", lambda: c.insert_one({"_id": ObjectId("507f1f77bcf86cd799439011")}))
    e.state("state2")
    e.cmd("empty_docs", {"insert": "c2", "documents": []})
    e.cmd("no_docs", {"insert": "c2"})
    e.cmd("docs_not_array", {"insert": "c2", "documents": {"a": 1}})
    e.cmd("doc_not_object", {"insert": "c2", "documents": [1]})
    e.cmd("unknown_field", {"insert": "c2", "documents": [{"a": 1}], "bogus": 1})
    e.cmd("ordered_false", {"insert": "c2", "documents": [{"_id": 1}, {"_id": 1}, {"_id": 2}], "ordered": False})
    e.cmd("ordered_true", {"insert": "c2", "documents": [{"_id": 5}, {"_id": 5}, {"_id": 6}], "ordered": True})
    e.cmd("bad_ordered", {"insert": "c2", "documents": [{"_id": 8}], "ordered": "x"})
    e.cmd("dollar_field", {"insert": "c2", "documents": [{"$a": 1}]})
    e.cmd("dotted_field", {"insert": "c2", "documents": [{"a.b": 1, "_id": 100}]})
    e.cmd("nested_dollar", {"insert": "c2", "documents": [{"_id": 101, "a": {"$b": 1}}]})
    e.cmd("nested_dotted", {"insert": "c2", "documents": [{"_id": 102, "a": {"b.c": 1}}]})
    e.find("c2_state", e.coll("c2"), {}, sort=[("_id", 1)])
    e.cmd("coll_with_dot", {"insert": "a.b", "documents": [{"_id": 1}]})
    e.cmd("coll_with_dollar", {"insert": "a$b", "documents": [{"_id": 1}]})
    e.cmd("coll_empty", {"insert": "", "documents": [{"_id": 1}]})
    e.cmd("coll_system", {"insert": "system.foo", "documents": [{"_id": 1}]})
    e.cmd("coll_number", {"insert": 5, "documents": [{"_id": 1}]})
    e.cmd("coll_long_name", {"insert": "x" * 300, "documents": [{"_id": 1}]})
    e.cmd("coll_unicode", {"insert": "colección_日本", "documents": [{"_id": 1}]})
    e.cmd("coll_dash_space", {"insert": "my-coll 2", "documents": [{"_id": 1}]})
    e.op("names", lambda: sorted(e.db.list_collection_names()))


@case("insert_write_concern")
def _(e):
    c = e.c
    e.cmd("w1", {"insert": "c", "documents": [{"_id": 1}], "writeConcern": {"w": 1}})
    e.cmd("w0", {"insert": "c", "documents": [{"_id": 2}], "writeConcern": {"w": 0}})
    e.cmd("wmajority", {"insert": "c", "documents": [{"_id": 3}], "writeConcern": {"w": "majority"}})
    e.cmd("w2", {"insert": "c", "documents": [{"_id": 4}], "writeConcern": {"w": 2}})
    e.cmd("wbogus", {"insert": "c", "documents": [{"_id": 5}], "writeConcern": {"w": "bogus"}})
    e.cmd("wj", {"insert": "c", "documents": [{"_id": 6}], "writeConcern": {"w": 1, "j": True}})
    e.cmd("wtimeout", {"insert": "c", "documents": [{"_id": 7}], "writeConcern": {"w": 1, "wtimeout": 100}})
    e.cmd("w_neg", {"insert": "c", "documents": [{"_id": 8}], "writeConcern": {"w": -1}})
    e.cmd("update_w2", {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"x": 1}}}], "writeConcern": {"w": 2}})
    e.cmd("delete_w2", {"delete": "c", "deletes": [{"q": {"_id": 1}, "limit": 1}], "writeConcern": {"w": 2}})
    e.state("state")


@case("insert_bson_limits")
def _(e):
    c = e.c
    e.op("just_under_16mb", lambda: c.insert_one({"_id": 1, "s": "x" * (16 * 1024 * 1024 - 100)}))
    e.op("over_16mb", lambda: c.insert_one({"_id": 2, "s": "x" * (16 * 1024 * 1024)}))
    e.op("count", lambda: c.count_documents({}))
    e.op("deep_100", lambda: c.insert_one({"_id": 3, "a": _nest(90)}))
    e.op("deep_150", lambda: c.insert_one({"_id": 4, "a": _nest(150)}))
    e.op("many_fields", lambda: c.insert_one({"_id": 5, **{"f%d" % i: i for i in range(2000)}}))
    e.op("long_field_name", lambda: c.insert_one({"_id": 6, "f" * 5000: 1}))
    e.op("empty_key", lambda: c.insert_one({"_id": 7, "": 1}))
    e.op("nul_in_string", lambda: c.insert_one({"_id": 8, "s": "a\x00b"}))
    e.op("nul_in_key", lambda: c.insert_one({"_id": 9, "a\x00b": 1}))
    e.find("empty_and_nul", c, {"_id": {"$in": [7, 8]}}, sort=[("_id", 1)])
    e.op("big_array", lambda: c.insert_one({"_id": 10, "a": list(range(50000))}))
    e.op("big_array_len", lambda: len(e.rawc(c).find_one({"_id": 10})["a"]))
    e.op("many_docs", lambda: len(c.insert_many([{"_id": 1000 + i, "v": i} for i in range(3000)]).inserted_ids))
    e.op("many_docs_count", lambda: c.count_documents({"_id": {"$gte": 1000}}))


def _nest(n):
    d = {"x": 1}
    for _ in range(n):
        d = {"n": d}
    return d


UPD_BASE = {"_id": 1, "a": 1, "s": "str", "arr": [1, 2, 3], "d": {"x": 1, "y": {"z": 2}}, "docs": [{"k": 1, "v": 10}, {"k": 2, "v": 20}, {"k": 3, "v": 30}],
            "n": None, "f": 1.5, "l": Int64(5), "big": 2147483647, "dec": Decimal128("1.5"), "t": True, "nested": [[1, 2], [3, 4]]}

UPDATES = [
    # $set / $unset
    {"$set": {"a": 2}}, {"$set": {"a": "x"}}, {"$set": {"new": 1}}, {"$set": {"d.x": 5}}, {"$set": {"d.y.z": 5}}, {"$set": {"d.q.r": 1}}, {"$set": {"d": 1}}, {"$set": {"arr.1": 9}},
    {"$set": {"arr.5": 9}}, {"$set": {"arr.a": 9}}, {"$set": {"a.b": 1}}, {"$set": {"s.b": 1}}, {"$set": {"docs.0.k": 100}}, {"$set": {"docs.1": 5}}, {"$set": {"n.x": 1}},
    {"$set": {"a": 1}}, {"$set": {"_id": 1}}, {"$set": {"_id": 2}}, {"$set": {"_id": 1.0}}, {"$set": {"": 1}}, {"$set": {"a..b": 1}}, {"$set": {"$x": 1}}, {"$set": {"x.$": 1}},
    {"$set": {}}, {"$set": 1}, {"$set": {"a": 1}, "$unset": {"a": ""}}, {"$set": {"a": 1, "a.b": 1}}, {"$set": {"d": {"x": 1}, "d.x": 2}}, {"$set": {"d.x": 1, "d.y": 2}},
    {"$unset": {"a": ""}}, {"$unset": {"a": 1}}, {"$unset": {"zz": ""}}, {"$unset": {"d.x": ""}}, {"$unset": {"d.y.z": ""}}, {"$unset": {"arr.1": ""}}, {"$unset": {"arr": ""}},
    {"$unset": {"docs.0.k": ""}}, {"$unset": {"_id": ""}}, {"$unset": {"a": "", "s": ""}}, {"$unset": {"a.b.c": ""}}, {"$unset": {}}, {"$unset": "a"},
    # $inc / $mul
    {"$inc": {"a": 1}}, {"$inc": {"a": -1}}, {"$inc": {"a": 1.5}}, {"$inc": {"a": Int64(2)}}, {"$inc": {"a": Decimal128("0.5")}}, {"$inc": {"new": 5}}, {"$inc": {"s": 1}},
    {"$inc": {"a": "x"}}, {"$inc": {"a": None}}, {"$inc": {"big": 1}}, {"$inc": {"big": Int64(1)}}, {"$inc": {"l": 1}}, {"$inc": {"l": 1.5}}, {"$inc": {"f": 1}}, {"$inc": {"dec": 1}},
    {"$inc": {"d.x": 1}}, {"$inc": {"d.new": 1}}, {"$inc": {"arr.0": 10}}, {"$inc": {"docs.0.v": 5}}, {"$inc": {"a": 1, "big": 1}}, {"$inc": {"n": 1}}, {"$inc": {"t": 1}},
    {"$inc": {"a": 2 ** 31 - 1}}, {"$inc": {"a": float("nan")}}, {"$inc": {"a": float("inf")}}, {"$inc": {"a": 0}}, {"$inc": {"_id": 1}},
    {"$mul": {"a": 3}}, {"$mul": {"a": 0.5}}, {"$mul": {"a": Int64(3)}}, {"$mul": {"new": 3}}, {"$mul": {"new": 3.5}}, {"$mul": {"new": Int64(3)}}, {"$mul": {"s": 2}}, {"$mul": {"a": "x"}},
    {"$mul": {"big": 2}}, {"$mul": {"big": Int64(2)}}, {"$mul": {"l": 2 ** 40}}, {"$mul": {"dec": 2}}, {"$mul": {"a": Decimal128("2")}}, {"$mul": {"new": Decimal128("2")}},
    # $min / $max / $rename / $currentDate
    {"$min": {"a": 0}}, {"$min": {"a": 5}}, {"$min": {"a": "x"}}, {"$min": {"a": None}}, {"$min": {"new": 5}}, {"$min": {"s": "a"}}, {"$min": {"s": 1}}, {"$min": {"a": 1.0}}, {"$min": {"a": Int64(1)}},
    {"$min": {"d": {"x": 0}}}, {"$min": {"arr": [0]}}, {"$min": {"f": 1}}, {"$min": {"a": float("nan")}},
    {"$max": {"a": 5}}, {"$max": {"a": 0}}, {"$max": {"a": "x"}}, {"$max": {"a": None}}, {"$max": {"new": 5}}, {"$max": {"s": "z"}}, {"$max": {"a": 1.0}}, {"$max": {"a": Int64(1)}}, {"$max": {"a": 2.5}},
    {"$max": {"d": {"x": 9}}}, {"$max": {"a": D(2020, 1, 1)}}, {"$max": {"f": 2}},
    {"$rename": {"a": "b"}}, {"$rename": {"a": "s"}}, {"$rename": {"zz": "b"}}, {"$rename": {"d.x": "d.w"}}, {"$rename": {"d.x": "top"}}, {"$rename": {"a": "d.new"}}, {"$rename": {"a": "a"}},
    {"$rename": {"a": 1}}, {"$rename": {"a": "b", "b": "c"}}, {"$rename": {"a": "b", "s": "b"}}, {"$rename": {"arr.0": "x"}}, {"$rename": {"docs.k": "x"}}, {"$rename": {"a": "arr.0"}}, {"$rename": {"_id": "x"}},
    {"$rename": {"a": "_id"}}, {"$rename": {"d": "d2"}}, {"$rename": {"a": ""}}, {"$rename": {"": "a"}},
    {"$currentDate": {"cd": True}}, {"$currentDate": {"cd": {"$type": "date"}}}, {"$currentDate": {"cd": {"$type": "timestamp"}}}, {"$currentDate": {"cd": False}}, {"$currentDate": {"cd": 1}},
    {"$currentDate": {"cd": {"$type": "bogus"}}}, {"$currentDate": {"cd": {}}}, {"$currentDate": {"d.cd": True}},
    {"$setOnInsert": {"a": 5}}, {"$setOnInsert": {"new": 5}},
    # $push
    {"$push": {"arr": 4}}, {"$push": {"arr": [4, 5]}}, {"$push": {"arr": {"$each": [4, 5]}}}, {"$push": {"arr": {"$each": []}}}, {"$push": {"arr": {"$each": [4, 5], "$position": 1}}},
    {"$push": {"arr": {"$each": [4, 5], "$position": -1}}}, {"$push": {"arr": {"$each": [4, 5], "$position": 100}}}, {"$push": {"arr": {"$each": [4, 5], "$position": -100}}}, {"$push": {"arr": {"$each": [0], "$position": 0}}},
    {"$push": {"arr": {"$each": [4, 5], "$slice": 4}}}, {"$push": {"arr": {"$each": [4, 5], "$slice": -4}}}, {"$push": {"arr": {"$each": [4, 5], "$slice": 0}}}, {"$push": {"arr": {"$each": [4], "$slice": 10}}},
    {"$push": {"arr": {"$each": [0, 5, 4], "$sort": 1}}}, {"$push": {"arr": {"$each": [0, 5, 4], "$sort": -1}}}, {"$push": {"arr": {"$each": [], "$sort": -1}}}, {"$push": {"arr": {"$each": [0, 5], "$sort": 1, "$slice": 3}}},
    {"$push": {"arr": {"$each": [0, 5], "$sort": -1, "$slice": -2}}}, {"$push": {"arr": {"$each": [0], "$position": 1, "$sort": 1, "$slice": 2}}},
    {"$push": {"docs": {"$each": [{"k": 0, "v": 5}], "$sort": {"k": 1}}}}, {"$push": {"docs": {"$each": [{"k": 0, "v": 5}], "$sort": {"v": -1}}}}, {"$push": {"docs": {"$each": [{"k": 0}], "$sort": {"k": 1, "v": 1}}}},
    {"$push": {"arr": {"$each": [1], "$sort": {"a": 1}}}}, {"$push": {"arr": {"$each": [1], "$sort": 2}}}, {"$push": {"arr": {"$each": [1], "$sort": "x"}}}, {"$push": {"arr": {"$each": [1], "$slice": "x"}}},
    {"$push": {"arr": {"$each": [1], "$slice": 1.5}}}, {"$push": {"arr": {"$each": [1], "$position": "x"}}}, {"$push": {"arr": {"$each": 1}}}, {"$push": {"arr": {"$each": [1], "$bogus": 1}}},
    {"$push": {"arr": {"$slice": 1}}}, {"$push": {"arr": {"$position": 1}}}, {"$push": {"new": 1}}, {"$push": {"new": {"$each": [1, 2]}}}, {"$push": {"a": 1}}, {"$push": {"s": 1}}, {"$push": {"d.new": 1}},
    {"$push": {"nested": [5]}}, {"$push": {"nested.0": 9}}, {"$push": {"docs.0.list": 1}}, {"$push": {"arr": 1, "docs": 2}}, {"$push": {"arr": {"$each": [{"$x": 1}]}}}, {"$push": {"n": 1}},
    # $pull / $pullAll / $addToSet / $pop
    {"$pull": {"arr": 2}}, {"$pull": {"arr": {"$gt": 1}}}, {"$pull": {"arr": {"$in": [1, 3]}}}, {"$pull": {"arr": 99}}, {"$pull": {"arr": [1, 2]}}, {"$pull": {"docs": {"k": 2}}}, {"$pull": {"docs": {"k": {"$gt": 1}}}},
    {"$pull": {"docs": {"k": 2, "v": 20}}}, {"$pull": {"docs": {"k": 2, "v": 99}}}, {"$pull": {"docs": {"$or": [{"k": 1}, {"k": 3}]}}}, {"$pull": {"docs": {}}}, {"$pull": {"nested": [1, 2]}}, {"$pull": {"nested": {"$elemMatch": {"$gt": 3}}}},
    {"$pull": {"nested": {"$gt": 1}}}, {"$pull": {"arr": {"$exists": True}}}, {"$pull": {"arr": {"$not": {"$gt": 1}}}}, {"$pull": {"zz": 1}}, {"$pull": {"a": 1}}, {"$pull": {"s": 1}}, {"$pull": {"arr": Regex("1")}},
    {"$pull": {"docs": {"k": {"$in": [1, 2]}}}}, {"$pull": {"arr": None}}, {"$pull": {"arr": {"$bogus": 1}}}, {"$pull": {"docs.k": 1}}, {"$pull": {"arr": 1, "docs": {"k": 1}}}, {"$pull": {"n": 1}},
    {"$pullAll": {"arr": [1, 3]}}, {"$pullAll": {"arr": []}}, {"$pullAll": {"arr": 1}}, {"$pullAll": {"arr": [[1]]}}, {"$pullAll": {"nested": [[1, 2]]}}, {"$pullAll": {"docs": [{"k": 1, "v": 10}]}}, {"$pullAll": {"zz": [1]}}, {"$pullAll": {"a": [1]}},
    {"$pullAll": {"arr": [1.0]}}, {"$pullAll": {"arr": [Int64(2)]}},
    {"$addToSet": {"arr": 4}}, {"$addToSet": {"arr": 3}}, {"$addToSet": {"arr": 3.0}}, {"$addToSet": {"arr": Int64(3)}}, {"$addToSet": {"arr": [3]}}, {"$addToSet": {"arr": {"$each": [3, 4, 4, 5]}}}, {"$addToSet": {"arr": {"$each": []}}},
    {"$addToSet": {"docs": {"k": 1, "v": 10}}}, {"$addToSet": {"docs": {"v": 10, "k": 1}}}, {"$addToSet": {"docs": {"k": 9}}}, {"$addToSet": {"new": 1}}, {"$addToSet": {"new": {"$each": [1, 1, 2]}}}, {"$addToSet": {"a": 1}}, {"$addToSet": {"s": 1}},
    {"$addToSet": {"arr": {"$each": 1}}}, {"$addToSet": {"arr": {"$each": [1], "$position": 1}}}, {"$addToSet": {"nested": [1, 2]}}, {"$addToSet": {"arr": None}}, {"$addToSet": {"n": 1}}, {"$addToSet": {"d.list": 1}},
    {"$pop": {"arr": 1}}, {"$pop": {"arr": -1}}, {"$pop": {"arr": 0}}, {"$pop": {"arr": 2}}, {"$pop": {"arr": "x"}}, {"$pop": {"arr": 1.0}}, {"$pop": {"zz": 1}}, {"$pop": {"a": 1}}, {"$pop": {"docs": 1}}, {"$pop": {"nested.0": 1}},
    {"$pop": {"arr": True}}, {"$pop": {"n": 1}},
    # bit
    {"$bit": {"a": {"and": 3}}}, {"$bit": {"a": {"or": 6}}}, {"$bit": {"a": {"xor": 5}}}, {"$bit": {"l": {"or": Int64(2 ** 40)}}}, {"$bit": {"a": {"and": 1, "or": 2}}}, {"$bit": {"a": {"nand": 1}}}, {"$bit": {"a": {"and": 1.5}}},
    {"$bit": {"s": {"and": 1}}}, {"$bit": {"new": {"or": 8}}}, {"$bit": {"a": 1}}, {"$bit": {"f": {"and": 1}}},
    # replacement / multiple ops / errors
    {"a": 5}, {"a": 5, "b": 6}, {"_id": 1, "a": 9}, {"_id": 2, "a": 9}, {}, {"d": {"x": 1}}, {"$set": {"a": 1}, "b": 2}, {"a": 1, "$set": {"b": 2}}, {"$bogus": {"a": 1}}, {"$set": {"a": 1}, "$bogus": {"b": 1}},
    {"$inc": {"a": 1}, "$set": {"s": "y"}, "$push": {"arr": 9}, "$unset": {"n": ""}}, {"$inc": {"a": 1}, "$set": {"a": 2}}, {"$set": {"d": 1}, "$inc": {"d.x": 1}}, {"$set": {"arr.0": 1}, "$push": {"arr": 5}},
]

PIPELINE_UPDATES = [
    [{"$set": {"a": {"$add": ["$a", 10]}}}], [{"$addFields": {"x": "$a"}}], [{"$project": {"a": 1}}], [{"$unset": "a"}], [{"$unset": ["a", "s"]}], [{"$replaceRoot": {"newRoot": {"_id": "$_id", "q": 1}}}],
    [{"$replaceWith": {"_id": "$_id", "q": {"$size": "$arr"}}}], [{"$set": {"a": 1}}, {"$set": {"b": {"$add": ["$a", 1]}}}], [{"$set": {"a": "$$REMOVE"}}], [{"$set": {"arr": {"$concatArrays": ["$arr", [4]]}}}],
    [{"$set": {"_id": 5}}], [{"$match": {"a": 1}}], [{"$limit": 1}], [{"$group": {"_id": 1}}], [{"$set": {"d.x": 100}}], [], [{"$replaceRoot": {"newRoot": "$a"}}],
    [{"$set": {"s": {"$toUpper": "$s"}}}], [{"$set": {"arr": {"$map": {"input": "$arr", "as": "x", "in": {"$multiply": ["$$x", 2]}}}}}], [{"$set": {"docs": {"$filter": {"input": "$docs", "cond": {"$gt": ["$$this.k", 1]}}}}}],
    [{"$set": {"t": {"$cond": ["$t", "yes", "no"]}}}], [{"$set": "x"}], [{"$bogus": {}}], [{"$set": {"a": {"$bogus": 1}}}],
]


def _chunks(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


for _i, _chunk in enumerate(_chunks(UPDATES, 30)):
    def _make(chunk, i):
        @case("update_ops_%02d" % i)
        def _(e):
            c = e.c
            for j, u in enumerate(chunk):
                c.delete_many({})
                c.insert_one(UPD_BASE)
                e.op("u%02d" % j, lambda u=u: c.update_one({"_id": 1}, u))
                e.state("s%02d" % j, mask=("$date", "$ts"))
        return _
    _make(_chunk, _i)


@case("update_pipeline")
def _(e):
    c = e.c
    for j, u in enumerate(PIPELINE_UPDATES):
        c.delete_many({})
        c.insert_one(UPD_BASE)
        e.op("u%02d" % j, lambda u=u: c.update_one({"_id": 1}, u))
        e.state("s%02d" % j)
    c.delete_many({})
    c.insert_many([{"_id": i, "v": i} for i in range(1, 6)])
    e.op("many_pipeline", lambda: c.update_many({}, [{"$set": {"w": {"$multiply": ["$v", 2]}}}]))
    e.state("many_state")
    e.op("upsert_pipeline", lambda: c.update_one({"_id": 99}, [{"$set": {"v": 1}}], upsert=True))
    e.state("upsert_state")


@case("update_positional_arrayfilters")
def _(e):
    c = e.c
    base = {"_id": 1, "grades": [80, 85, 90, 70], "docs": [{"a": 1, "b": [1, 2, 3]}, {"a": 2, "b": [4, 5]}, {"a": 1, "b": [6]}], "m": [[1, 2], [3, 4]], "x": 5}
    ops = [
        ("pos_set", {"grades": 85}, {"$set": {"grades.$": 100}}, {}),
        ("pos_inc", {"grades": {"$gte": 85}}, {"$inc": {"grades.$": 1}}, {}),
        ("pos_nomatch_query", {"x": 5}, {"$set": {"grades.$": 1}}, {}),
        ("pos_docs", {"docs.a": 2}, {"$set": {"docs.$.z": 1}}, {}),
        ("pos_elemmatch", {"docs": {"$elemMatch": {"a": 1, "b": 6}}}, {"$set": {"docs.$.z": 1}}, {}),
        ("pos_nested", {"docs.b": 5}, {"$set": {"docs.$.b.0": 0}}, {}),
        ("pos_unset", {"grades": 90}, {"$unset": {"grades.$": ""}}, {}),
        ("pos_push_error", {"docs.a": 1}, {"$push": {"docs.$.b": 9}}, {}),
        ("pos_pull", {"docs.a": 2}, {"$pull": {"docs.$.b": 4}}, {}),
        ("pos_two", {"grades": 85, "docs.a": 2}, {"$set": {"grades.$": 1, "docs.$.a": 3}}, {}),
        ("pos_non_array", {"x": 5}, {"$set": {"x.$": 1}}, {}),
        ("all_set", {}, {"$set": {"grades.$[]": 0}}, {}),
        ("all_inc", {}, {"$inc": {"grades.$[]": 1}}, {}),
        ("all_docs", {}, {"$set": {"docs.$[].z": 1}}, {}),
        ("all_nested", {}, {"$inc": {"docs.$[].b.$[]": 10}}, {}),
        ("all_matrix", {}, {"$set": {"m.$[].$[]": 0}}, {}),
        ("all_non_array", {}, {"$set": {"x.$[]": 1}}, {}),
        ("all_missing", {}, {"$set": {"zz.$[]": 1}}, {}),
        ("all_unset", {}, {"$unset": {"grades.$[]": ""}}, {}),
        ("all_push", {}, {"$push": {"docs.$[].b": 0}}, {}),
        ("af_ge", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"e": {"$gte": 85}}]}),
        ("af_none_match", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"e": {"$gte": 1000}}]}),
        ("af_docs", {}, {"$set": {"docs.$[d].z": 1}}, {"array_filters": [{"d.a": 1}]}),
        ("af_docs_nested", {}, {"$inc": {"docs.$[d].b.$[n]": 100}}, {"array_filters": [{"d.a": 1}, {"n": {"$gt": 2}}]}),
        ("af_two", {}, {"$set": {"grades.$[lo]": 0, "docs.$[d].z": 1}}, {"array_filters": [{"lo": {"$lt": 80}}, {"d.a": 2}]}),
        ("af_missing_filter", {}, {"$set": {"grades.$[e]": 100}}, {}),
        ("af_unused", {}, {"$set": {"grades.$[]": 100}}, {"array_filters": [{"e": {"$gt": 1}}]}),
        ("af_bad_id", {}, {"$set": {"grades.$[E]": 100}}, {"array_filters": [{"E": {"$gt": 1}}]}),
        ("af_dup_id", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"e": {"$gt": 1}}, {"e": {"$lt": 1}}]}),
        ("af_two_ids_in_one", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"e": {"$gt": 1}, "f": 1}]}),
        ("af_and", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"$and": [{"e": {"$gt": 70}}, {"e": {"$lt": 90}}]}]}),
        ("af_or", {}, {"$set": {"grades.$[e]": 100}}, {"array_filters": [{"$or": [{"e": 70}, {"e": 90}]}]}),
        ("af_on_replacement", {}, {"x": 1}, {"array_filters": [{"e": 1}]}),
        ("af_matrix", {}, {"$set": {"m.$[r].$[c]": 0}}, {"array_filters": [{"r.0": 1}, {"c": {"$gt": 1}}]}),
        ("af_not_array", {}, {"$set": {"x.$[e]": 1}}, {"array_filters": [{"e": 1}]}),
        ("af_upsert", {"_id": 77}, {"$set": {"g.$[e]": 1}}, {"array_filters": [{"e": 1}], "upsert": True}),
        ("af_empty_array_filters", {}, {"$set": {"grades.$[]": 1}}, {"array_filters": []}),
    ]
    for name, flt, upd, kw in ops:
        c.delete_many({})
        c.insert_one(base)
        e.op(name, lambda flt=flt, upd=upd, kw=kw: c.update_one(flt or {"_id": 1}, upd, **kw))
        e.state(name + "_state")
    c.delete_many({})
    c.insert_many([{"_id": 1, "g": [1, 2, 3]}, {"_id": 2, "g": [3, 4]}, {"_id": 3, "g": []}, {"_id": 4}])
    e.op("many_all", lambda: c.update_many({}, {"$inc": {"g.$[]": 1}}))
    e.state("many_all_state")
    e.op("many_af", lambda: c.update_many({}, {"$set": {"g.$[x]": 0}}, array_filters=[{"x": {"$gt": 3}}]))
    e.state("many_af_state")


@case("update_semantics")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "g": i % 2, "v": i} for i in range(1, 9)])
    e.op("one_first_match", lambda: c.update_one({"g": 1}, {"$set": {"hit": 1}}))
    e.state("s1")
    e.op("many", lambda: c.update_many({"g": 0}, {"$inc": {"v": 100}}))
    e.state("s2")
    e.op("many_no_match", lambda: c.update_many({"g": 5}, {"$set": {"x": 1}}))
    e.op("one_noop_same_value", lambda: c.update_one({"_id": 1}, {"$set": {"hit": 1}}))
    e.op("many_noop", lambda: c.update_many({"g": 1}, {"$set": {"g": 1}}))
    e.op("noop_type_change", lambda: c.update_one({"_id": 2}, {"$set": {"g": 0.0}}))
    e.op("noop_type_change_long", lambda: c.update_one({"_id": 2}, {"$set": {"g": Int64(0)}}))
    e.state("s3")
    e.op("replace", lambda: c.replace_one({"_id": 3}, {"replaced": True}))
    e.op("replace_same", lambda: c.replace_one({"_id": 3}, {"replaced": True}))
    e.op("replace_with_id", lambda: c.replace_one({"_id": 4}, {"_id": 4, "r": 1}))
    e.op("replace_change_id", lambda: c.replace_one({"_id": 5}, {"_id": 55, "r": 1}))
    e.op("replace_no_match", lambda: c.replace_one({"_id": 500}, {"r": 1}))
    e.op("replace_upsert", lambda: c.replace_one({"_id": 500}, {"r": 1}, upsert=True))
    e.op("replace_upsert_new_id", lambda: c.replace_one({"k": "x"}, {"r": 2}, upsert=True) and 1)
    e.op("replace_upsert_conflict_id", lambda: c.replace_one({"_id": 600}, {"_id": 601, "r": 2}, upsert=True))
    e.state("s4", mask=())
    e.cmd("multi_replacement", {"update": "c", "updates": [{"q": {}, "u": {"a": 1}, "multi": True}]})
    e.cmd("empty_updates", {"update": "c", "updates": []})
    e.cmd("no_q", {"update": "c", "updates": [{"u": {"$set": {"a": 1}}}]})
    e.cmd("no_u", {"update": "c", "updates": [{"q": {}}]})
    e.cmd("bad_u_type", {"update": "c", "updates": [{"q": {}, "u": 1}]})
    e.cmd("bad_q_type", {"update": "c", "updates": [{"q": 1, "u": {"$set": {"a": 1}}}]})
    e.cmd("unknown_spec_field", {"update": "c", "updates": [{"q": {}, "u": {"$set": {"a": 1}}, "bogus": 1}]})
    e.cmd("unknown_field", {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"a": 1}}}], "bogus": 1})
    e.cmd("ordered_error", {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"o1": 1}}}, {"q": {"_id": 2}, "u": {"$badop": 1}}, {"q": {"_id": 3}, "u": {"$set": {"o3": 1}}}]})
    e.cmd("unordered_error", {"update": "c", "ordered": False, "updates": [{"q": {"_id": 1}, "u": {"$set": {"u1": 1}}}, {"q": {"_id": 2}, "u": {"$badop": 1}}, {"q": {"_id": 3}, "u": {"$set": {"u3": 1}}}]})
    e.cmd("multi_specs", {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"m": 1}}}, {"q": {"_id": 1}, "u": {"$set": {"m": 1}}}, {"q": {"_id": 700}, "u": {"$set": {"m": 1}}, "upsert": True}, {"q": {"g": 0}, "u": {"$set": {"m": 2}}, "multi": True}]})
    e.state("s5")
    e.cmd("upsert_multi_ids", {"update": "c", "updates": [{"q": {"_id": 800}, "u": {"$set": {"a": 1}}, "upsert": True}, {"q": {"_id": 801}, "u": {"$set": {"a": 1}}, "upsert": True}]})
    e.op("update_missing_coll", lambda: e.coll("nope").update_one({"_id": 1}, {"$set": {"a": 1}}))
    e.op("update_missing_coll_created", lambda: "nope" in e.db.list_collection_names())
    e.op("update_missing_coll_upsert", lambda: e.coll("nope2").update_one({"_id": 1}, {"$set": {"a": 1}}, upsert=True))
    e.op("update_missing_coll_upsert_created", lambda: "nope2" in e.db.list_collection_names())
    e.op("hint_bad", lambda: c.update_one({"_id": 1}, {"$set": {"a": 1}}, hint="nope_1"))
    e.op("hint_id", lambda: c.update_one({"_id": 1}, {"$set": {"hint": 1}}, hint="_id_"))
    e.op("collation_update", lambda: c.update_many({"g": {"$in": [0]}}, {"$set": {"col": 1}}, collation={"locale": "en"}))


@case("upsert_semantics")
def _(e):
    c = e.c
    cases = [
        ("eq", {"a": 1}, {"$set": {"b": 2}}),
        ("eq_dotted", {"a.b": 1}, {"$set": {"c": 2}}),
        ("eq_multi", {"a": 1, "b": "x", "c": {"d": 1}}, {"$set": {"e": 1}}),
        ("eq_and", {"$and": [{"a": 1}, {"b": 2}]}, {"$set": {"e": 1}}),
        ("gt_only", {"a": {"$gt": 1}}, {"$set": {"e": 1}}),
        ("in_only", {"a": {"$in": [1, 2]}}, {"$set": {"e": 1}}),
        ("eq_op", {"a": {"$eq": 5}}, {"$set": {"e": 1}}),
        ("or", {"$or": [{"a": 1}, {"b": 2}]}, {"$set": {"e": 1}}),
        ("id_eq", {"_id": 77}, {"$set": {"e": 1}}),
        ("id_gt", {"_id": {"$gt": 1}}, {"$set": {"e": 1}}),
        ("id_in", {"_id": {"$in": [5]}}, {"$set": {"e": 1}}),
        ("setoninsert", {"a": 9}, {"$setOnInsert": {"s": 1}, "$set": {"b": 1}}),
        ("inc", {"a": 10}, {"$inc": {"n": 5}}),
        ("push", {"a": 11}, {"$push": {"arr": 1}}),
        ("addtoset", {"a": 12}, {"$addToSet": {"arr": {"$each": [1, 1, 2]}}}),
        ("mul", {"a": 13}, {"$mul": {"n": 5}}),
        ("min_max", {"a": 14}, {"$min": {"lo": 5}, "$max": {"hi": 5}}),
        ("unset", {"a": 15}, {"$unset": {"zz": ""}}),
        ("rename", {"a": 16}, {"$rename": {"a": "b"}}),
        ("conflict_eq_set", {"a": 17}, {"$set": {"a": 18}}),
        ("conflict_dotted", {"a.b": 19}, {"$set": {"a": 1}}),
        ("conflict_dotted2", {"a": 20}, {"$set": {"a.b": 1}}),
        ("pos_error", {"a": 21}, {"$set": {"x.$": 1}}),
        ("all_error", {"a": 22}, {"$set": {"x.$[]": 1}}),
        ("replacement", {"a": 23}, {"b": 1}),
        ("replacement_id", {"a": 24}, {"_id": 24, "b": 1}),
        ("replacement_id_conflict", {"_id": 25}, {"_id": 26, "b": 1}),
        ("regex_query", {"a": {"$regex": "x"}}, {"$set": {"e": 1}}),
        ("empty_query", {}, {"$set": {"e": 1}}),
        ("array_eq", {"a": [1, 2]}, {"$set": {"e": 1}}),
        ("null_eq", {"a": None}, {"$set": {"e": 1}}),
        ("doc_eq", {"a": {"x": 1}}, {"$set": {"e": 1}}),
        ("expr_query", {"$expr": {"$eq": ["$a", 1]}}, {"$set": {"e": 1}}),
        ("dollar_field", {"a": 27}, {"$set": {"$x": 1}}),
        ("currentdate", {"a": 28}, {"$currentDate": {"now": True}}),
    ]
    for name, flt, upd in cases:
        c.delete_many({})
        e.op(name, lambda flt=flt, upd=upd: c.update_one(flt, upd, upsert=True), mask=())
        e.find(name + "_state", c, {}, mask=("$date",), ordered=False)
    c.delete_many({})
    c.insert_one({"_id": 1, "a": 1})
    e.op("upsert_existing", lambda: c.update_one({"a": 1}, {"$set": {"b": 1}}, upsert=True))
    e.op("upsert_many_none", lambda: c.update_many({"a": 99}, {"$set": {"b": 1}}, upsert=True))
    e.op("upsert_many_existing", lambda: c.update_many({"a": 1}, {"$set": {"b": 2}}, upsert=True))
    e.op("upsert_returns_id", lambda: c.update_one({"_id": "custom"}, {"$set": {"b": 1}}, upsert=True).upserted_id)
    e.op("upsert_dup_race", lambda: c.update_one({"_id": "custom"}, {"$set": {"b": 2}}, upsert=True).upserted_id)
    c.create_index("u", unique=True)
    e.op("upsert_unique_violation", lambda: c.update_one({"q": 1}, {"$set": {"u": 1}}, upsert=True))
    e.op("upsert_unique_ok", lambda: c.update_one({"q": 1}, {"$set": {"u": 5}}, upsert=True))
    e.op("upsert_unique_hit", lambda: c.update_one({"u": 5}, {"$set": {"z": 5}}, upsert=True))


@case("delete_semantics")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "g": i % 3, "v": i} for i in range(1, 13)])
    e.op("one", lambda: c.delete_one({"g": 1}))
    e.op("one_no_match", lambda: c.delete_one({"g": 9}))
    e.op("many", lambda: c.delete_many({"g": 2}))
    e.op("many_none", lambda: c.delete_many({"g": 9}))
    e.state("s1")
    e.op("by_id", lambda: c.delete_one({"_id": 3}))
    e.op("by_id_missing", lambda: c.delete_one({"_id": 3}))
    e.op("by_id_numeric", lambda: c.delete_one({"_id": 6.0}))
    e.op("by_operator", lambda: c.delete_many({"v": {"$gt": 9}}))
    e.state("s2")
    e.op("all", lambda: c.delete_many({}))
    e.state("s3")
    e.op("missing_coll", lambda: e.coll("nope").delete_many({}))
    e.cmd("no_deletes", {"delete": "c"})
    e.cmd("empty_deletes", {"delete": "c", "deletes": []})
    e.cmd("no_q", {"delete": "c", "deletes": [{"limit": 1}]})
    e.cmd("no_limit", {"delete": "c", "deletes": [{"q": {}}]})
    e.cmd("limit_2", {"delete": "c", "deletes": [{"q": {}, "limit": 2}]})
    e.cmd("limit_bool", {"delete": "c", "deletes": [{"q": {}, "limit": True}]})
    e.cmd("limit_str", {"delete": "c", "deletes": [{"q": {}, "limit": "1"}]})
    e.cmd("bad_q", {"delete": "c", "deletes": [{"q": {"$foo": 1}, "limit": 0}]})
    e.cmd("bad_q_type", {"delete": "c", "deletes": [{"q": 1, "limit": 0}]})
    e.cmd("unknown_field", {"delete": "c", "deletes": [{"q": {}, "limit": 0, "bogus": 1}]})
    e.cmd("unknown_cmd_field", {"delete": "c", "deletes": [{"q": {}, "limit": 0}], "bogus": 1})
    c.insert_many([{"_id": i} for i in range(1, 6)])
    e.cmd("ordered_error", {"delete": "c", "deletes": [{"q": {"_id": 1}, "limit": 1}, {"q": {"$foo": 1}, "limit": 1}, {"q": {"_id": 2}, "limit": 1}]})
    e.cmd("unordered_error", {"delete": "c", "ordered": False, "deletes": [{"q": {"_id": 3}, "limit": 1}, {"q": {"$foo": 1}, "limit": 1}, {"q": {"_id": 4}, "limit": 1}]})
    e.state("s4")
    e.op("collation", lambda: (c.insert_one({"_id": 50, "s": "Abc"}), c.delete_many({"s": "abc"}, collation={"locale": "en", "strength": 2}).deleted_count)[1])
    e.op("hint_bad", lambda: c.delete_one({"_id": 1}, hint="nope_1"))


@case("find_and_modify")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "g": i % 2, "v": i * 10, "arr": [i]} for i in range(1, 7)])
    e.op("fou_default", lambda: e.rawc(c).find_one_and_update({"g": 1}, {"$inc": {"v": 1}}))
    e.op("fou_after", lambda: e.rawc(c).find_one_and_update({"g": 1}, {"$inc": {"v": 1}}, return_document=ReturnDocument.AFTER))
    e.op("fou_sort_desc", lambda: e.rawc(c).find_one_and_update({"g": 1}, {"$set": {"s": 1}}, sort=[("_id", -1)], return_document=ReturnDocument.AFTER))
    e.op("fou_projection", lambda: e.rawc(c).find_one_and_update({"g": 0}, {"$set": {"p": 1}}, projection={"v": 1, "_id": 0}, return_document=ReturnDocument.AFTER))
    e.op("fou_projection_excl", lambda: e.rawc(c).find_one_and_update({"g": 0}, {"$set": {"p": 2}}, projection={"arr": 0}))
    e.op("fou_no_match", lambda: e.rawc(c).find_one_and_update({"g": 9}, {"$set": {"x": 1}}))
    e.op("fou_upsert_after", lambda: e.rawc(c).find_one_and_update({"_id": 50}, {"$set": {"x": 1}}, upsert=True, return_document=ReturnDocument.AFTER))
    e.op("fou_upsert_before", lambda: e.rawc(c).find_one_and_update({"_id": 51}, {"$set": {"x": 1}}, upsert=True))
    e.op("fou_pipeline", lambda: e.rawc(c).find_one_and_update({"_id": 1}, [{"$set": {"pp": {"$add": ["$v", 1]}}}], return_document=ReturnDocument.AFTER))
    e.op("fou_arrayfilters", lambda: e.rawc(c).find_one_and_update({"_id": 2}, {"$set": {"arr.$[e]": 5}}, array_filters=[{"e": 2}], return_document=ReturnDocument.AFTER))
    e.op("fou_positional", lambda: e.rawc(c).find_one_and_update({"_id": 3, "arr": 3}, {"$set": {"arr.$": 33}}, return_document=ReturnDocument.AFTER))
    e.op("fou_error", lambda: e.rawc(c).find_one_and_update({"_id": 1}, {"$inc": {"arr": 1}}))
    e.op("fou_bad_op", lambda: e.rawc(c).find_one_and_update({"_id": 1}, {"$bogus": {"arr": 1}}))
    e.op("fou_id_change", lambda: e.rawc(c).find_one_and_update({"_id": 1}, {"$set": {"_id": 9}}))
    e.op("fou_collation", lambda: e.rawc(c).find_one_and_update({"_id": 1}, {"$set": {"col": 1}}, collation={"locale": "en"}))
    e.op("fod", lambda: e.rawc(c).find_one_and_delete({"g": 1}))
    e.op("fod_sort", lambda: e.rawc(c).find_one_and_delete({"g": 0}, sort=[("_id", -1)]))
    e.op("fod_projection", lambda: e.rawc(c).find_one_and_delete({"g": 0}, projection={"v": 1}))
    e.op("fod_no_match", lambda: e.rawc(c).find_one_and_delete({"g": 9}))
    e.op("for", lambda: e.rawc(c).find_one_and_replace({"_id": 2}, {"r": 1}))
    e.op("for_after", lambda: e.rawc(c).find_one_and_replace({"_id": 4}, {"r": 2}, return_document=ReturnDocument.AFTER))
    e.op("for_upsert", lambda: e.rawc(c).find_one_and_replace({"_id": 60}, {"r": 3}, upsert=True, return_document=ReturnDocument.AFTER))
    e.op("for_bad_id", lambda: e.rawc(c).find_one_and_replace({"_id": 4}, {"_id": 99}))
    e.state("state")
    e.cmd("neither", {"findAndModify": "c", "query": {"_id": 1}})
    e.cmd("both", {"findAndModify": "c", "query": {"_id": 1}, "update": {"$set": {"a": 1}}, "remove": True})
    e.cmd("remove_new", {"findAndModify": "c", "query": {"_id": 1}, "remove": True, "new": True})
    e.cmd("remove_upsert", {"findAndModify": "c", "query": {"_id": 1}, "remove": True, "upsert": True})
    e.cmd("bad_update_type", {"findAndModify": "c", "query": {"_id": 1}, "update": 1})
    e.cmd("bad_query_type", {"findAndModify": "c", "query": 1, "update": {"$set": {"a": 1}}})
    e.cmd("bad_sort", {"findAndModify": "c", "query": {}, "sort": {"a": 7}, "update": {"$set": {"a": 1}}})
    e.cmd("unknown_field", {"findAndModify": "c", "query": {}, "update": {"$set": {"a": 1}}, "bogus": 1})
    e.cmd("lowercase_name", {"findandmodify": "c", "query": {"_id": 1}, "update": {"$set": {"lc": 1}}, "new": True})
    e.cmd("fields_error", {"findAndModify": "c", "query": {"_id": 1}, "update": {"$set": {"a": 1}}, "fields": {"a": 1, "b": 0}})
    e.cmd("missing_coll", {"findAndModify": "nope", "query": {"_id": 1}, "update": {"$set": {"a": 1}}})
    e.cmd("missing_coll_upsert", {"findAndModify": "nope2", "query": {"_id": 1}, "update": {"$set": {"a": 1}}, "upsert": True, "new": True})
    e.cmd("remove_missing_coll", {"findAndModify": "nope3", "query": {"_id": 1}, "remove": True})
    e.state("state2")


@case("bulk_write")
def _(e):
    c = e.c
    c.insert_many([{"_id": i, "v": i} for i in range(1, 6)])
    e.op("mixed_ordered", lambda: c.bulk_write([InsertOne({"_id": 10, "v": 10}), UpdateOne({"_id": 1}, {"$set": {"u": 1}}), DeleteOne({"_id": 2}),
                                               ReplaceOne({"_id": 3}, {"r": 3}), UpdateMany({"v": {"$gt": 3}}, {"$inc": {"v": 100}}), DeleteMany({"_id": {"$gt": 1000}})]))
    e.state("s1")
    e.op("mixed_unordered", lambda: c.bulk_write([InsertOne({"_id": 20}), UpdateOne({"_id": 999}, {"$set": {"x": 1}}, upsert=True), DeleteOne({"_id": 4})], ordered=False))
    e.state("s2")
    e.op("ordered_error_stops", lambda: c.bulk_write([InsertOne({"_id": 30}), InsertOne({"_id": 1}), InsertOne({"_id": 31})]))
    e.op("unordered_error_continues", lambda: c.bulk_write([InsertOne({"_id": 32}), InsertOne({"_id": 1}), InsertOne({"_id": 33}), InsertOne({"_id": 1})], ordered=False))
    e.state("s3")
    e.op("multiple_errors_types", lambda: c.bulk_write([InsertOne({"_id": 40}), UpdateOne({"_id": 40}, {"$bogus": 1}), DeleteOne({"_id": 40})], ordered=False))
    e.op("upserts", lambda: c.bulk_write([UpdateOne({"_id": 100 + i}, {"$set": {"u": i}}, upsert=True) for i in range(3)]))
    e.op("upserts_replace", lambda: c.bulk_write([ReplaceOne({"_id": 200 + i}, {"u": i}, upsert=True) for i in range(3)]))
    e.op("empty_bulk", lambda: c.bulk_write([]))
    e.op("bulk_all_deletes", lambda: c.bulk_write([DeleteMany({"_id": {"$gte": 100}}), DeleteMany({"_id": {"$gte": 20}})]))
    e.state("s4")
    e.op("large_batch", lambda: c.bulk_write([InsertOne({"_id": 1000 + i}) for i in range(5000)]).inserted_count)
    e.op("large_batch_count", lambda: c.count_documents({"_id": {"$gte": 1000}}))
    e.op("write_error_details_dup", lambda: c.insert_many([{"_id": 1000}], ordered=True))
    e.op("bulk_with_collation", lambda: c.bulk_write([UpdateMany({"_id": {"$lt": 3}}, {"$set": {"col": 1}}, collation={"locale": "en"})]))
    e.op("bulk_array_filters", lambda: c.bulk_write([UpdateOne({"_id": 1}, {"$set": {"a.$[x]": 1}}, array_filters=[{"x": 1}])]))
    e.cmd("raw_batch", {"insert": "c", "documents": [{"_id": 90000 + i} for i in range(1200)], "ordered": True})
    e.cmd("raw_batch_over_limit", {"insert": "c", "documents": [{"_id": 95000 + i} for i in range(100001)]})
