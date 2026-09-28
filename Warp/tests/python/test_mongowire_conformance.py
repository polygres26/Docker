"""Warp-side regression tests for mongowire's MongoDB conformance (real Warp + real Postgres, no oracle needed).

mongo_conformance/golden/oracle_7.0.json.gz holds the answers a real mongod 7.0 gave to every step of
mongo_conformance/mongo_corpus*.py (recorded with `mongo_harness.py --record`: each case twice, non-deterministic steps
dropped). Every case is replayed here against Warp on one Postgres backend and on two sharded backends (one backend
set, the `mongodb` store sharded by _id) and must produce the same normalised answer, except for the divergences
documented in mongo_conformance/mongo_known.py. On top: pymongo end-to-end checks of features added with the engine
(exact BSON types, operators, sharded merging, cross-shard unique indexes) and a check that documents really land on
both Postgres hosts.

Needs WARP_TEST_PG_LOCAL=1 (or Docker) like the other Warp tests; see mongo_conformance/README.md.
"""
import gzip
import json
import os
import sys

import pytest
from bson import Int64
from pymongo import MongoClient, errors as pe

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "mongo_conformance"))
import mongo_corpus as corpus  # noqa: E402
import mongo_launch_warp  # noqa: E402

with gzip.open(os.path.join(HERE, "mongo_conformance", "golden", "oracle_7.0.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)["cases"]


@pytest.fixture(scope="module")
def one():
    s = mongo_launch_warp.Stack(1)
    yield s
    s.close()


@pytest.fixture(scope="module")
def two():
    s = mongo_launch_warp.Stack(2)
    yield s
    s.close()


def client(stack):
    return MongoClient(stack.uri, serverSelectionTimeoutMS=15000, retryWrites=False)


def _replay(stack, name, sharded):
    c = client(stack)
    try:
        idx = list(GOLDEN).index(name)
        got = corpus.run_case(name, corpus.CASES[name], c, "wc%d_x" % idx)
    finally:
        c.close()
    res = corpus.compare_steps(GOLDEN[name], got, sharded=sharded, case=name)
    bad = [(l, d) for l, v, d in res if v == "diff"]
    assert not bad, "\n".join("%s: %s" % b for b in bad[:12])


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_golden_single_backend(one, name):
    _replay(one, name, sharded=False)


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_golden_two_sharded_backends(two, name):
    _replay(two, name, sharded=True)


# ------------------------------------------------------------------------------------------------ end to end

def test_bson_types_round_trip_exactly(one):
    c = client(one)["e2e_types"]["t"]
    c.drop()
    import datetime
    from bson import Binary, Decimal128, ObjectId, Regex, Timestamp
    doc = {"_id": 1, "i": 5, "l": Int64(5), "d": 5.0, "dec": Decimal128("5.000"), "b": Binary(b"\x00\x01", 5),
           "dt": datetime.datetime(2020, 1, 2, 3, 4, 5, 6000), "ts": Timestamp(1, 2), "re": Regex("a+", "i"), "n": {"z": 1, "a": 2}}
    c.insert_one(doc)
    raw = c.with_options(codec_options=__import__("bson").CodecOptions(document_class=__import__("bson.raw_bson", fromlist=["x"]).RawBSONDocument)).find_one({"_id": 1})
    assert bytes(raw.raw) == __import__("bson").encode(doc)  # byte-identical, field order included


def test_update_operators_and_positional(one):
    c = client(one)["e2e_upd"]["t"]
    c.drop()
    c.insert_one({"_id": 1, "g": [1, 2, 3], "docs": [{"a": 1}, {"a": 2}]})
    c.update_one({"_id": 1}, {"$inc": {"g.$[e]": 10}}, array_filters=[{"e": {"$gte": 2}}])
    c.update_one({"_id": 1, "docs.a": 2}, {"$set": {"docs.$.b": 1}})
    c.update_one({"_id": 1}, {"$push": {"g": {"$each": [0, 9], "$sort": 1, "$slice": 4}}})
    assert c.find_one({"_id": 1}) == {"_id": 1, "g": [0, 1, 9, 12], "docs": [{"a": 1}, {"a": 2, "b": 1}]}


def test_aggregation_pipeline_lookup_facet(one):
    db = client(one)["e2e_agg"]
    db.t.drop()
    db.o.drop()
    db.t.insert_many([{"_id": i, "k": i % 3, "v": i} for i in range(12)])
    db.o.insert_many([{"_id": 0, "name": "zero"}, {"_id": 1, "name": "one"}])
    out = list(db.t.aggregate([{"$group": {"_id": "$k", "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}, {"$sort": {"_id": 1}},
                               {"$lookup": {"from": "o", "localField": "_id", "foreignField": "_id", "as": "o"}}]))
    assert [(d["_id"], d["s"]) for d in out] == [(0, 18), (1, 22), (2, 26)]
    assert out[0]["o"] == [{"_id": 0, "name": "zero"}] and out[2]["o"] == []
    f = list(db.t.aggregate([{"$facet": {"c": [{"$count": "n"}], "top": [{"$sort": {"v": -1}}, {"$limit": 2}]}}]))[0]
    assert f["c"] == [{"n": 12}] and [d["v"] for d in f["top"]] == [11, 10]


def test_transactions_and_change_streams_are_refused_like_a_standalone_mongod(one):
    c = client(one)
    with pytest.raises(pe.OperationFailure) as e:
        with c.start_session() as s:
            s.start_transaction()
            c["e2e_tx"]["t"].insert_one({"a": 1}, session=s)
    assert e.value.code == 20
    with pytest.raises(pe.OperationFailure) as e:
        list(c["e2e_tx"]["t"].watch())
    assert e.value.code == 40573


def test_zlib_compression_and_handshake(one):
    c = MongoClient(one.uri, compressors="zlib", serverSelectionTimeoutMS=10000)
    assert c["e2e_z"]["t"].count_documents({}) == 0
    h = c.admin.command("hello")
    assert h["maxWireVersion"] >= 21 and h["logicalSessionTimeoutMinutes"] == 30 and h["isWritablePrimary"] is True


def test_documents_land_on_both_postgres_hosts_and_reads_merge(two):
    import psycopg2
    c = client(two)["shopdb"]["people"]
    c.drop()
    c.insert_many([{"_id": i, "name": "p%03d" % i, "age": i % 40, "tags": ["t%d" % (i % 5)]} for i in range(400)])
    counts = []
    for pg in two.pgs:
        conn = psycopg2.connect(host="127.0.0.1", port=pg.port, user="postgres", password="postgres", dbname="postgres")
        with conn, conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM "shopdb"."people"')
            counts.append(cur.fetchone()[0])
        conn.close()
    assert sum(counts) == 400 and all(n > 100 for n in counts), counts
    assert [d["_id"] for d in c.find({}, {"_id": 1}).sort("_id", 1).skip(10).limit(5)] == [10, 11, 12, 13, 14]
    assert c.count_documents({"age": {"$gte": 30}}) == sum(1 for i in range(400) if i % 40 >= 30)
    assert sorted(c.distinct("age")) == list(range(40))
    g = {d["_id"]: d["n"] for d in c.aggregate([{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}}}])}
    assert g == {"t%d" % i: 80 for i in range(5)}
    assert c.update_many({"age": 0}, {"$set": {"flag": True}}).modified_count == 10
    assert c.delete_many({"age": {"$lt": 5}}).deleted_count == 50


def test_unique_index_is_enforced_across_shards(two):
    c = client(two)["uniqdb"]["u"]
    c.drop()
    c.create_index("email", unique=True)
    c.insert_many([{"_id": i, "email": "e%d@x" % i} for i in range(60)])
    dups = 0
    for i in range(60):
        try:
            c.insert_one({"_id": 1000 + i, "email": "e%d@x" % i})
        except pe.DuplicateKeyError as ex:
            dups += 1
            assert ex.details["keyPattern"] == {"email": 1}
    assert dups == 60
    with pytest.raises(pe.DuplicateKeyError):
        c.update_one({"_id": 3}, {"$set": {"email": "e4@x"}})
    c.delete_one({"_id": 4})
    c.update_one({"_id": 3}, {"$set": {"email": "e4@x"}})
    assert c.count_documents({}) == 59


def test_cursors_batch_across_shards(two):
    c = client(two)["curdb"]["c"]
    c.drop()
    c.insert_many([{"_id": i} for i in range(1000)])
    assert sorted(d["_id"] for d in c.find({}, batch_size=37)) == list(range(1000))
