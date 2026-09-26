"""Corpus part 3: aggregation stages and expression operators."""
import datetime

from bson import Binary, Decimal128, Int64, ObjectId, Regex, Timestamp

from mongo_corpus import case

D = datetime.datetime

A_DATA = [
    {"_id": 1, "cat": "a", "n": 10, "s": "apple", "tags": ["x", "y"], "d": D(2020, 1, 15, 10, 30, 45, 123000), "sub": {"p": 1, "q": [1, 2]}, "items": [{"k": "u", "v": 1}, {"k": "w", "v": 2}]},
    {"_id": 2, "cat": "a", "n": 20, "s": "Banana", "tags": ["y"], "d": D(2020, 2, 29, 23, 59, 59, 999000), "sub": {"p": 2}, "items": []},
    {"_id": 3, "cat": "b", "n": 30, "s": "cherry", "tags": [], "d": D(2021, 12, 31), "sub": None, "items": [{"k": "u", "v": 5}]},
    {"_id": 4, "cat": "b", "n": 5.5, "s": "date", "tags": ["x", "z", "x"], "d": D(1999, 7, 4, 12), "items": [{"k": "z", "v": 9}, {"k": "u", "v": 3}]},
    {"_id": 5, "cat": "c", "n": None, "s": None, "tags": None, "d": None},
    {"_id": 6, "cat": "c", "n": Int64(7), "s": "", "tags": "notarray", "sub": {"p": 3, "q": []}},
    {"_id": 7, "cat": None, "n": Decimal128("2.5"), "s": "Éclair", "tags": ["y", "x"]},
    {"_id": 8, "n": -4, "s": "  padded  ", "tags": [1, 2, 3]},
    {"_id": 9, "cat": "a", "n": 0, "s": "a,b,,c", "tags": [[1], [2, 3]]},
    {"_id": 10, "cat": "b", "n": 100, "s": "apple pie", "tags": ["x"], "d": D(2022, 6, 15, 8), "sub": {"p": 10}},
]

PIPES = [
    [], [{"$match": {"cat": "a"}}], [{"$match": {"n": {"$gt": 5}}}, {"$count": "c"}], [{"$count": "c"}], [{"$match": {"_id": 99}}, {"$count": "c"}], [{"$limit": 3}], [{"$skip": 8}], [{"$skip": 2}, {"$limit": 2}],
    [{"$sort": {"n": -1}}, {"$limit": 3}], [{"$sort": {"cat": 1, "n": -1}}], [{"$sort": {"_id": -1}}, {"$skip": 1}], [{"$limit": 0}], [{"$limit": -1}], [{"$skip": -1}], [{"$limit": "x"}], [{"$limit": 2.5}], [{"$sort": {}}],
    [{"$sort": {"a": 2}}], [{"$count": ""}], [{"$count": "a.b"}], [{"$count": "$a"}], [{"$count": 1}], [{}], [{"$match": 1}], [{"$bogus": {}}], [{"$match": {}, "$limit": 1}], ["x"], [{"$match": {"$foo": 1}}],
    [{"$project": {"cat": 1, "n": 1}}], [{"$project": {"cat": 1, "_id": 0}}], [{"$project": {"n": 0, "tags": 0}}], [{"$project": {"x": "$n", "y": {"$add": ["$n", 1]}}}], [{"$project": {"sub.p": 1}}], [{"$project": {"r": {"$literal": "$n"}}}],
    [{"$project": {}}], [{"$project": {"a": 1, "b": 0}}], [{"$project": {"cat": 1, "x": {"$concat": ["$cat", "!"]}}}], [{"$project": {"_id": 0, "z": {"$ifNull": ["$s", "none"]}}}], [{"$project": {"sub": {"p": "$sub.p", "c": "$cat"}}}],
    [{"$project": {"a.b": {"$literal": 1}}}], [{"$project": {"x": 1, "x.y": 1}}], [{"$project": {"_id": 1}}], [{"$project": {"n": True}}], [{"$project": {"n": "$$REMOVE", "cat": 1}}], [{"$project": {"arr": ["$n", "$cat"]}}],
    [{"$addFields": {"x": 1}}], [{"$addFields": {"x": "$n", "sub.z": 5}}], [{"$set": {"n": {"$multiply": ["$n", 2]}}}], [{"$addFields": {"sub": {"p": 99}}}], [{"$addFields": {"sub.new": 1}}], [{"$addFields": {"items.k": "K"}}], [{"$addFields": {"tags.0": 1}}],
    [{"$addFields": {"a": {"b": {"c": "$n"}}}}], [{"$set": {"n": "$$REMOVE"}}], [{"$addFields": {}}], [{"$addFields": 1}], [{"$unset": "n"}], [{"$unset": ["n", "tags"]}], [{"$unset": "sub.p"}], [{"$unset": "items.k"}], [{"$unset": []}], [{"$unset": 1}],
    [{"$replaceRoot": {"newRoot": "$sub"}}], [{"$replaceRoot": {"newRoot": {"a": "$n"}}}], [{"$replaceWith": "$sub"}], [{"$replaceWith": {"$mergeObjects": ["$sub", {"c": "$cat"}]}}], [{"$replaceRoot": {}}], [{"$replaceRoot": {"newRoot": "$n"}}],
    [{"$unwind": "$tags"}], [{"$unwind": "tags"}], [{"$unwind": {"path": "$tags"}}], [{"$unwind": {"path": "$tags", "includeArrayIndex": "i"}}], [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}],
    [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True, "includeArrayIndex": "i"}}], [{"$unwind": "$items"}], [{"$unwind": "$items.k"}], [{"$unwind": "$sub.q"}], [{"$unwind": "$nothere"}], [{"$unwind": {"path": "$tags", "bogus": 1}}],
    [{"$unwind": {}}], [{"$unwind": 1}], [{"$unwind": {"path": "$tags", "includeArrayIndex": "$i"}}], [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": "x"}}], [{"$unwind": "$tags"}, {"$unwind": "$tags"}],
    [{"$group": {"_id": "$cat"}}], [{"$group": {"_id": "$cat", "c": {"$sum": 1}}}], [{"$group": {"_id": None, "c": {"$sum": 1}, "t": {"$sum": "$n"}, "a": {"$avg": "$n"}, "mn": {"$min": "$n"}, "mx": {"$max": "$n"}}}],
    [{"$group": {"_id": "$cat", "f": {"$first": "$n"}, "l": {"$last": "$n"}}}], [{"$group": {"_id": "$cat", "p": {"$push": "$n"}}}], [{"$group": {"_id": "$cat", "s": {"$addToSet": "$cat"}}}], [{"$group": {"_id": "$cat", "n": {"$count": {}}}}],
    [{"$group": {"_id": "$cat", "sd": {"$stdDevPop": "$n"}, "ss": {"$stdDevSamp": "$n"}}}], [{"$group": {"_id": "$cat", "m": {"$mergeObjects": "$sub"}}}], [{"$group": {"_id": {"c": "$cat", "big": {"$gt": ["$n", 8]}}, "c": {"$sum": 1}}}],
    [{"$group": {"_id": "$tags", "c": {"$sum": 1}}}], [{"$group": {"_id": "$s"}}], [{"$group": {"_id": {"$toLower": "$s"}, "c": {"$sum": 1}}}], [{"$group": {"_id": "$n", "c": {"$sum": 1}}}], [{"$group": {"_id": "$sub"}}],
    [{"$group": {"_id": "$cat", "t": {"$sum": {"$multiply": ["$n", 2]}}}}], [{"$group": {"_id": "$cat", "t": {"$sum": 5}}}], [{"$group": {"_id": "$cat", "t": {"$sum": "$s"}}}], [{"$group": {"_id": "$cat", "t": {"$avg": "$s"}}}],
    [{"$group": {"_id": "$cat", "top": {"$top": {"sortBy": {"n": -1}, "output": "$_id"}}}}], [{"$group": {"_id": "$cat", "bot": {"$bottom": {"sortBy": {"n": -1}, "output": "$_id"}}}}], [{"$group": {"_id": "$cat", "t2": {"$topN": {"n": 2, "sortBy": {"_id": 1}, "output": "$_id"}}}}],
    [{"$group": {"_id": "$cat", "f2": {"$firstN": {"input": "$_id", "n": 2}}}}], [{"$group": {"_id": "$cat", "mx2": {"$maxN": {"input": "$n", "n": 2}}}}], [{"$group": {"_id": "$cat", "l2": {"$lastN": {"input": "$_id", "n": 2}}}}],
    [{"$group": {}}], [{"$group": {"c": {"$sum": 1}}}], [{"$group": {"_id": 1, "x": 1}}], [{"$group": {"_id": 1, "x": {"$bogus": 1}}}], [{"$group": {"_id": 1, "x": {"$sum": 1, "$avg": 1}}}], [{"$group": {"_id": 1, "a.b": {"$sum": 1}}}], [{"$group": {"_id": 1, "$x": {"$sum": 1}}}],
    [{"$group": {"_id": "$cat", "c": {"$sum": 1}}}, {"$sort": {"c": -1, "_id": 1}}], [{"$group": {"_id": "$cat", "c": {"$sum": 1}}}, {"$match": {"c": {"$gt": 1}}}], [{"$sortByCount": "$cat"}], [{"$sortByCount": "$tags"}], [{"$sortByCount": {"$toUpper": "$cat"}}], [{"$sortByCount": 1}],
    [{"$bucket": {"groupBy": "$n", "boundaries": [0, 10, 50], "default": "other"}}], [{"$bucket": {"groupBy": "$n", "boundaries": [0, 10, 50, 1000], "default": "other", "output": {"c": {"$sum": 1}, "ids": {"$push": "$_id"}}}}], [{"$bucket": {"groupBy": "$n", "boundaries": [0, 10]}}],
    [{"$bucket": {"groupBy": "$n", "boundaries": [10, 0]}}], [{"$bucket": {"groupBy": "$n", "boundaries": [0]}}], [{"$bucket": {"groupBy": "$n", "boundaries": [0, "a"]}}], [{"$bucket": {"boundaries": [0, 1]}}], [{"$bucket": {"groupBy": "$n", "boundaries": [0, 10], "default": 5}}],
    [{"$bucketAuto": {"groupBy": "$_id", "buckets": 3}}], [{"$bucketAuto": {"groupBy": "$_id", "buckets": 4, "output": {"c": {"$sum": 1}}}}], [{"$bucketAuto": {"groupBy": "$cat", "buckets": 2}}], [{"$bucketAuto": {"groupBy": "$_id", "buckets": 0}}], [{"$bucketAuto": {"groupBy": "$_id", "buckets": 20}}],
    [{"$sample": {"size": 100}}, {"$count": "c"}], [{"$sample": {"size": 3}}, {"$count": "c"}], [{"$sample": {"size": -1}}], [{"$sample": {}}], [{"$sample": {"size": "x"}}],
    [{"$facet": {"a": [{"$match": {"cat": "a"}}, {"$count": "c"}], "b": [{"$limit": 2}, {"$project": {"_id": 1}}]}}], [{"$facet": {}}], [{"$facet": {"a": 1}}], [{"$facet": {"a": [{"$out": "x"}]}}], [{"$facet": {"a": [{"$facet": {"b": []}}]}}], [{"$facet": {"a": []}}],
    [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k", "as": "o"}}], [{"$lookup": {"from": "other", "localField": "n", "foreignField": "k", "as": "o"}}], [{"$lookup": {"from": "other", "localField": "tags", "foreignField": "k", "as": "o"}}],
    [{"$lookup": {"from": "other", "localField": "nothere", "foreignField": "k", "as": "o"}}], [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "nothere", "as": "o"}}], [{"$lookup": {"from": "nosuch", "localField": "cat", "foreignField": "k", "as": "o"}}],
    [{"$lookup": {"from": "other", "let": {"c": "$cat"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$k", "$$c"]}}}, {"$project": {"_id": 0, "w": 1}}], "as": "o"}}], [{"$lookup": {"from": "other", "pipeline": [{"$match": {"w": {"$gt": 1}}}], "as": "o"}}],
    [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k", "as": "sub.o"}}], [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k", "as": "cat"}}], [{"$lookup": {"from": "other", "localField": "cat", "as": "o"}}],
    [{"$lookup": {"from": "other", "as": "o"}}], [{"$lookup": {"localField": "cat", "foreignField": "k", "as": "o"}}], [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k"}}], [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k", "as": "o", "bogus": 1}}],
    [{"$lookup": {"from": "other", "localField": "cat", "foreignField": "k", "as": "o"}}, {"$unwind": "$o"}, {"$project": {"cat": 1, "o.w": 1}}], [{"$lookup": {"from": "other", "let": {"n": "$n"}, "pipeline": [{"$match": {"$expr": {"$lt": ["$w", "$$n"]}}}, {"$count": "c"}], "as": "o"}}],
    [{"$unionWith": "other"}, {"$count": "c"}], [{"$unionWith": {"coll": "other", "pipeline": [{"$match": {"w": 1}}]}}, {"$count": "c"}], [{"$unionWith": "nosuch"}, {"$count": "c"}], [{"$unionWith": 1}],
    [{"$redact": {"$cond": [{"$gt": ["$n", 8]}, "$$KEEP", "$$PRUNE"]}}], [{"$redact": "$$DESCEND"}], [{"$redact": "x"}], [{"$graphLookup": {"from": "tree", "startWith": "$n", "connectFromField": "parent", "connectToField": "_id", "as": "path"}}, {"$project": {"_id": 1, "path._id": 1}}],
    [{"$match": {"_id": {"$lte": 3}}}, {"$graphLookup": {"from": "tree", "startWith": 1, "connectFromField": "parent", "connectToField": "_id", "as": "path", "maxDepth": 1, "depthField": "depth"}}, {"$project": {"path": 1}}],
    [{"$collStats": {"count": {}}}], [{"$indexStats": {}}], [{"$currentOp": {}}], [{"$changeStream": {}}], [{"$geoNear": {"near": [0, 0], "distanceField": "d"}}], [{"$documents": [{"a": 1}]}], [{"$search": {}}],
    [{"$setWindowFields": {"partitionBy": "$cat", "sortBy": {"_id": 1}, "output": {"total": {"$sum": "$n", "window": {"documents": ["unbounded", "current"]}}}}}, {"$project": {"cat": 1, "total": 1}}],
    [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"r": {"$rank": {}}, "dr": {"$denseRank": {}}, "dn": {"$documentNumber": {}}}}}, {"$project": {"r": 1, "dr": 1, "dn": 1}}],
    [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"prev": {"$shift": {"output": "$_id", "by": -1, "default": 0}}}}}, {"$project": {"prev": 1}}],
    [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"avg3": {"$avg": "$n", "window": {"documents": [-1, 1]}}}}}, {"$project": {"avg3": 1}}],
    [{"$densify": {"field": "n", "range": {"step": 1, "bounds": "full"}}}], [{"$fill": {"output": {"n": {"value": 0}}}}],
    [{"$match": {"cat": "a"}}, {"$group": {"_id": None, "ids": {"$push": "$_id"}}}, {"$project": {"_id": 0, "ids": 1}}], [{"$match": {"$expr": {"$gt": ["$n", 9]}}}, {"$project": {"_id": 1}}],
    [{"$addFields": {"r": {"$round": ["$n", 0]}}}, {"$sort": {"r": 1, "_id": 1}}, {"$project": {"r": 1}}], [{"$sort": {"tags": 1, "_id": 1}}, {"$project": {"tags": 1}}], [{"$sort": {"sub.p": -1, "_id": 1}}, {"$project": {"sub.p": 1}}],
    [{"$sort": {"n": 1}}, {"$skip": 3}, {"$limit": 3}, {"$project": {"n": 1}}], [{"$limit": 5}, {"$sort": {"_id": -1}}, {"$project": {"_id": 1}}], [{"$sort": {"s": 1}}, {"$project": {"s": 1}}],
]


@case("agg_data_setup")
def _(e):
    c = e.c
    c.insert_many(A_DATA)
    e.coll("other").insert_many([{"_id": 1, "k": "a", "w": 1}, {"_id": 2, "k": "a", "w": 2}, {"_id": 3, "k": "b", "w": 3}, {"_id": 4, "k": None, "w": 4}, {"_id": 5, "w": 5}, {"_id": 6, "k": ["x", "b"], "w": 6}, {"_id": 7, "k": 10, "w": 7}])
    e.coll("tree").insert_many([{"_id": 1, "parent": None}, {"_id": 2, "parent": 1}, {"_id": 3, "parent": 2}, {"_id": 4, "parent": 2}, {"_id": 10, "parent": 3}, {"_id": 5, "parent": 4}])
    e.agg("all", c, [])


def _chunks(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


for _i, _chunk in enumerate(_chunks(PIPES, 25)):
    def _make(chunk, i):
        @case("agg_pipes_%02d" % i)
        def _(e):
            c = e.c
            c.insert_many(A_DATA)
            e.coll("other").insert_many([{"_id": 1, "k": "a", "w": 1}, {"_id": 2, "k": "a", "w": 2}, {"_id": 3, "k": "b", "w": 3}, {"_id": 4, "k": None, "w": 4}, {"_id": 5, "w": 5}, {"_id": 6, "k": ["x", "b"], "w": 6}, {"_id": 7, "k": 10, "w": 7}])
            e.coll("tree").insert_many([{"_id": 1, "parent": None}, {"_id": 2, "parent": 1}, {"_id": 3, "parent": 2}, {"_id": 4, "parent": 2}, {"_id": 10, "parent": 3}, {"_id": 5, "parent": 4}])
            for j, p in enumerate(chunk):
                has_sort = any(isinstance(s, dict) and "$sort" in s for s in p)
                e.agg("p%02d" % j, c, p, ordered=has_sort)
        return _
    _make(_chunk, _i)


@case("agg_cursor_and_options")
def _(e):
    c = e.c
    c.insert_many(A_DATA)
    e.cmd("no_cursor", {"aggregate": "c", "pipeline": []})
    e.cmd("cursor_not_doc", {"aggregate": "c", "pipeline": [], "cursor": 1})
    e.cmd("no_pipeline", {"aggregate": "c", "cursor": {}})
    e.cmd("pipeline_not_array", {"aggregate": "c", "pipeline": {}, "cursor": {}})
    e.cmd("bad_batch", {"aggregate": "c", "pipeline": [], "cursor": {"batchSize": -1}})
    e.cmd("batch_2", {"aggregate": "c", "pipeline": [{"$sort": {"_id": 1}}], "cursor": {"batchSize": 2}})
    e.cmd("batch_0", {"aggregate": "c", "pipeline": [], "cursor": {"batchSize": 0}})
    e.cmd("unknown_field", {"aggregate": "c", "pipeline": [], "cursor": {}, "bogus": 1})
    e.cmd("collationless_documents", {"aggregate": 1, "pipeline": [{"$documents": [{"a": 1}, {"a": 2}]}], "cursor": {}})
    e.cmd("collationless_no_stage", {"aggregate": 1, "pipeline": [{"$match": {}}], "cursor": {}})
    e.cmd("missing_coll", {"aggregate": "nosuch", "pipeline": [{"$match": {}}], "cursor": {}})
    e.cmd("missing_coll_count", {"aggregate": "nosuch", "pipeline": [{"$count": "c"}], "cursor": {}})
    e.cmd("collation_agg", {"aggregate": "c", "pipeline": [{"$match": {"s": "APPLE"}}, {"$project": {"_id": 1}}], "cursor": {}, "collation": {"locale": "en", "strength": 2}})
    e.cmd("hint_bad", {"aggregate": "c", "pipeline": [], "cursor": {}, "hint": "nope_1"})
    e.cmd("let_var", {"aggregate": "c", "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$x"]}}}, {"$project": {"_id": 1}}], "cursor": {}, "let": {"x": 3}})
    e.cmd("allow_disk", {"aggregate": "c", "pipeline": [{"$count": "c"}], "cursor": {}, "allowDiskUse": True})
    e.cmd("explain_shape", {"aggregate": "c", "pipeline": [{"$match": {"_id": 1}}], "explain": True}, mask=("$date",))
    e.cmd("out_stage_not_last", {"aggregate": "c", "pipeline": [{"$out": "o"}, {"$match": {}}], "cursor": {}})
    e.op("large_group", lambda: len(list(c.aggregate([{"$group": {"_id": None, "c": {"$sum": 1}}}]))))
    e.op("many_stages_cursor", lambda: [d["_id"] for d in c.aggregate([{"$sort": {"_id": 1}}], batchSize=3)])
    e.op("agg_kill_cursor", lambda: (lambda cur: (next(cur), cur.close(), 1)[2])(c.aggregate([{"$sort": {"_id": 1}}], batchSize=2)))


@case("agg_out_merge")
def _(e):
    c = e.c
    c.insert_many(A_DATA)
    e.agg("out", c, [{"$match": {"cat": "a"}}, {"$project": {"n": 1}}, {"$out": "outc"}])
    e.state("out_state", e.coll("outc"))
    e.agg("out_again", c, [{"$match": {"cat": "b"}}, {"$project": {"n": 1}}, {"$out": "outc"}])
    e.state("out_state2", e.coll("outc"))
    e.agg("out_db", c, [{"$match": {"_id": 1}}, {"$project": {"n": 1}}, {"$out": {"db": e.dbname + "x", "coll": "o2"}}])
    e.op("out_db_data", lambda: list(e.client[e.dbname + "x"]["o2"].find({}, {"_id": 1})))
    e.client.drop_database(e.dbname + "x")
    tgt = e.coll("tgt")
    tgt.insert_many([{"_id": 1, "n": "old", "keep": 1}, {"_id": 2, "n": "old2"}])
    e.agg("merge_default", c, [{"$match": {"_id": {"$lte": 3}}}, {"$project": {"n": 1}}, {"$merge": "tgt"}])
    e.state("merge_state", tgt)
    e.agg("merge_replace", c, [{"$match": {"_id": 1}}, {"$project": {"z": 1}}, {"$merge": {"into": "tgt", "whenMatched": "replace"}}])
    e.agg("merge_keep", c, [{"$match": {"_id": {"$in": [2, 3]}}}, {"$project": {"q": 1}}, {"$merge": {"into": "tgt", "whenMatched": "keepExisting"}}])
    e.agg("merge_fail", c, [{"$match": {"_id": 2}}, {"$project": {"q": 1}}, {"$merge": {"into": "tgt", "whenMatched": "fail"}}])
    e.agg("merge_discard", c, [{"$match": {"_id": {"$in": [50, 3]}}}, {"$project": {"q": 1}}, {"$merge": {"into": "tgt", "whenNotMatched": "discard"}}])
    e.agg("merge_pipeline", c, [{"$match": {"_id": 3}}, {"$project": {"n": 1}}, {"$merge": {"into": "tgt", "whenMatched": [{"$set": {"n": {"$add": ["$n", "$$new.n"]}}}]}}])
    e.state("merge_state2", tgt)
    e.agg("merge_on_field", c, [{"$match": {"_id": 4}}, {"$project": {"_id": 0, "s": 1}}, {"$merge": {"into": "tgt", "on": "s"}}])
    e.agg("merge_notmatched_fail", c, [{"$match": {"_id": 99}}, {"$merge": {"into": "tgt"}}])
    e.agg("merge_bad_option", c, [{"$merge": {"into": "tgt", "whenMatched": "bogus"}}])
