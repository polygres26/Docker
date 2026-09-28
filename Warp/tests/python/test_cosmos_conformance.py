"""Warp-side tests of cosmoswire (the Azure Cosmos DB for NoSQL REST API on Postgres).

NO real Cosmos DB service or emulator was available (the official Linux emulator image crashes on arm64), so NOTHING here is a comparison
with real Cosmos behaviour. The evidence is:
* the official azure-cosmos Python SDK against Warp (discovery, CRUD, queries incl. cross-partition, patch, batch, change feed, ETags, paging, TTL);
* hand-written expected values from the published REST API and query-language documentation (cited per test; from documentation as
  recalled, not re-fetched);
* a randomized differential test of the SQL semantics against an independent Python implementation (cosmos_diff_engine.py);
* sharding over two real Postgres servers, MCP tools, concurrency.
See cosmos_conformance/cosmos_known.md for the divergences and the unverified behaviours.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests, WARP_TEST_JAR, and `pip install azure-cosmos`.
"""
import concurrent.futures
import json
import os
import random
import sys
import time
import uuid

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "cosmos_conformance"))
import cosmos_diff_engine as DE  # noqa: E402
import cosmos_launch_warp as L  # noqa: E402
import cosmos_raw_client as RC  # noqa: E402

from azure import core  # noqa: E402
from azure.cosmos import CosmosClient, PartitionKey, exceptions  # noqa: E402
from azure.cosmos.partition_key import NonePartitionKeyValue as NonePK  # noqa: E402
from mcp_support import call, call_json, create_endpoint, tool_names  # noqa: E402
from warp_test_support import free_port  # noqa: E402


def uniq(prefix):
    return "%s%s" % (prefix, uuid.uuid4().hex[:8])


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def one():
    w = L.CosmosWarp(1)
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.CosmosWarp(2)
    yield w
    w.close()


@pytest.fixture
def handshake(one):
    # shares the Postgres server (and the enabled store) of `one`: a local machine has few SysV shared-memory segments for Postgres
    w = L.CosmosWarp(pgs=one.pgs, default_store=False, extra_env={"WARP_COSMOSWIRE_QUERY_PLAN_HANDSHAKE": "true"})
    yield w
    w.proc.close()


def client(w, key=RC.EMULATOR_KEY):
    return CosmosClient(w.endpoint, credential=key)


def mk(w, pk="/pk", **kw):
    c = client(w)
    db = c.create_database(uniq("db"))
    return c, db, db.create_container(uniq("c"), partition_key=PartitionKey(path=pk), **kw)


def coll_path(db, co):
    return "/dbs/%s/colls/%s" % (db.id, co.id)


FAMILIES = [
    {"id": "AndersenFamily", "pk": "a", "lastName": "Andersen", "parents": [{"firstName": "Thomas"}, {"firstName": "Mary Kay"}],
     "children": [{"firstName": "Henriette Thaulow", "gender": "female", "grade": 5, "pets": [{"givenName": "Fluffy"}]}],
     "address": {"state": "WA", "county": "King", "city": "Seattle"}, "creationDate": 1431620472, "isRegistered": True},
    {"id": "WakefieldFamily", "pk": "w", "parents": [{"familyName": "Wakefield", "givenName": "Robin"}, {"familyName": "Miller", "givenName": "Ben"}],
     "children": [{"familyName": "Merriam", "givenName": "Jesse", "gender": "female", "grade": 1, "pets": [{"givenName": "Goofy"}, {"givenName": "Shadow"}]},
                  {"familyName": "Miller", "givenName": "Lisa", "gender": "female", "grade": 8}],
     "address": {"state": "NY", "county": "Manhattan", "city": "NY"}, "creationDate": 1431620462, "isRegistered": False},
]


# ---------------------------------------------------------------------------------------------
# protocol level (raw signed HTTP): REST API documentation
# ---------------------------------------------------------------------------------------------

def test_store_appears_in_the_backend_set_api(one):
    r = requests.get("http://localhost:%d/api/backend-sets" % one.proc.metrics_port, headers={"Authorization": "Bearer warp-test-admin-token"}, timeout=10)
    assert r.status_code == 200 and "cosmos" in r.text


def test_account_document_points_sdk_discovery_at_warp(one):
    """REST "Get Database Account": writableLocations/readableLocations with databaseAccountEndpoint."""
    r = RC.Raw(one.port).req("GET", "/")
    assert r.status_code == 200
    d = r.json()
    assert d["writableLocations"][0]["databaseAccountEndpoint"] == "http://localhost:%d/" % one.port
    assert d["readableLocations"] and d["userConsistencyPolicy"]["defaultConsistencyLevel"] == "Session"
    for h in ("x-ms-request-charge", "x-ms-activity-id", "x-ms-session-token", "x-ms-serviceversion"):
        assert h in r.headers, h


def test_master_key_authorization(one):
    """Access control in the Cosmos DB SQL REST API: HMAC-SHA256 over verb/type/link/date; wrong key, stale date, missing header -> 401."""
    ok = RC.Raw(one.port)
    assert ok.req("GET", "/dbs").status_code == 200
    wrong = RC.Raw(one.port, key=RC.base64.b64encode(b"x" * 64).decode())
    r = wrong.req("GET", "/dbs")
    assert r.status_code == 401 and r.json()["code"] == "Unauthorized"
    r = requests.get(ok.base + "/dbs", timeout=10)
    assert r.status_code == 401
    r = ok.req("GET", "/dbs", headers={"x-ms-date": "Thu, 01 Jan 2015 00:00:00 GMT"})
    assert r.status_code == 401  # signed for another date than the one sent


def test_custom_keys_and_auth_disabled():
    key = RC.base64.b64encode(os.urandom(64)).decode()
    w = L.CosmosWarp(1, extra_env={"WARP_COSMOSWIRE_KEYS": key})
    try:
        assert RC.Raw(w.port, key).req("GET", "/dbs").status_code == 200
        assert RC.Raw(w.port).req("GET", "/dbs").status_code == 401  # the emulator key no longer works
    finally:
        w.close()


def test_database_and_container_lifecycle_and_errors(one):
    raw = RC.Raw(one.port)
    db = uniq("db")
    r = raw.req("POST", "/dbs", {"id": db})
    assert r.status_code == 201 and r.json()["id"] == db and r.json()["_rid"] and r.json()["_self"].startswith("dbs/")
    assert raw.req("POST", "/dbs", {"id": db}).status_code == 409
    assert raw.req("POST", "/dbs", {}).status_code == 400
    assert raw.req("GET", "/dbs/nope").status_code == 404
    pkdef = {"paths": ["/pk"], "kind": "Hash", "version": 2}
    r = raw.req("POST", "/dbs/%s/colls" % db, {"id": "c", "partitionKey": pkdef, "defaultTtl": 100,
                                               "uniqueKeyPolicy": {"uniqueKeys": [{"paths": ["/email"]}]}})
    assert r.status_code == 201
    c = r.json()
    assert c["partitionKey"]["paths"] == ["/pk"] and c["defaultTtl"] == 100 and c["indexingPolicy"]["indexingMode"] == "consistent"
    assert c["uniqueKeyPolicy"]["uniqueKeys"][0]["paths"] == ["/email"] and c["_docs"] == "docs/"
    assert raw.req("POST", "/dbs/%s/colls" % db, {"id": "c", "partitionKey": pkdef}).status_code == 409
    assert raw.req("POST", "/dbs/%s/colls" % db, {"id": "nopk"}).status_code == 400
    # replace: indexing policy/TTL may change, the partition key may not
    c2 = dict(c, defaultTtl=-1)
    assert raw.req("PUT", "/dbs/%s/colls/c" % db, c2).json()["defaultTtl"] == -1
    assert raw.req("PUT", "/dbs/%s/colls/c" % db, dict(c, partitionKey={"paths": ["/other"], "kind": "Hash"})).status_code == 400
    # addressing by _rid works like by id
    dbrid = raw.req("GET", "/dbs/%s" % db).json()["_rid"]
    assert raw.req("GET", "/dbs/%s/colls/%s" % (dbrid, c["_rid"])).json()["id"] == "c"
    assert raw.req("DELETE", "/dbs/%s/colls/c" % db).status_code == 204
    assert raw.req("GET", "/dbs/%s/colls/c" % db).status_code == 404
    assert raw.req("DELETE", "/dbs/%s" % db).status_code == 204


def test_document_crud_status_codes_headers_and_etags(one):
    """REST "Create/Replace/Delete/Get a document": 201/200/204/200, 409 duplicate id, 404, If-Match -> 412, If-None-Match -> 304."""
    c, db, co = mk(one)
    raw, p = RC.Raw(one.port), coll_path(db, co)
    pk = {"x-ms-documentdb-partitionkey": '["a"]'}
    r = raw.req("POST", p + "/docs", {"id": "1", "pk": "a", "v": 1}, pk)
    assert r.status_code == 201
    d = r.json()
    for sysprop in ("_rid", "_self", "_etag", "_attachments", "_ts"):
        assert sysprop in d, sysprop
    assert d["_etag"] == r.headers["etag"] and float(r.headers["x-ms-request-charge"]) > 0 and r.headers["x-ms-session-token"]
    assert list(d)[:3] == ["id", "pk", "v"]  # user properties keep their order, system properties follow
    assert raw.req("POST", p + "/docs", {"id": "1", "pk": "a"}, pk).status_code == 409
    assert raw.req("POST", p + "/docs", {"id": "1", "pk": "b"}, {"x-ms-documentdb-partitionkey": '["a"]'}).status_code == 400  # header/body mismatch
    assert raw.req("POST", p + "/docs", {"pk": "a"}, pk).status_code == 400  # id missing
    for bad in ("a/b", "a?b", "a#b", "a\\b"):
        assert raw.req("POST", p + "/docs", {"id": bad, "pk": "a"}, pk).status_code == 400
    g = raw.req("GET", p + "/docs/1", headers=pk)
    assert g.status_code == 200 and g.json()["v"] == 1
    assert raw.req("GET", p + "/docs/1", headers={**pk, "If-None-Match": d["_etag"]}).status_code == 304
    assert raw.req("GET", p + "/docs/1", headers={"x-ms-documentdb-partitionkey": '["zzz"]'}).status_code == 404
    assert raw.req("GET", p + "/docs/nope", headers=pk).status_code == 404
    rp = raw.req("PUT", p + "/docs/1", {"id": "1", "pk": "a", "v": 2}, {**pk, "If-Match": '"bogus"'})
    assert rp.status_code == 412
    rp = raw.req("PUT", p + "/docs/1", {"id": "1", "pk": "a", "v": 2}, {**pk, "If-Match": d["_etag"]})
    assert rp.status_code == 200 and rp.json()["v"] == 2 and rp.json()["_etag"] != d["_etag"] and rp.json()["_rid"] == d["_rid"]
    assert raw.req("PUT", p + "/docs/2", {"id": "2", "pk": "a"}, pk).status_code == 404
    assert raw.req("DELETE", p + "/docs/1", headers={**pk, "If-Match": d["_etag"]}).status_code == 412  # stale etag
    assert raw.req("DELETE", p + "/docs/1", headers=pk).status_code == 204
    assert raw.req("DELETE", p + "/docs/1", headers=pk).status_code == 404
    # upsert: 201 when created, 200 when replaced
    up = {"x-ms-documentdb-is-upsert": "True", **pk}
    assert raw.req("POST", p + "/docs", {"id": "u", "pk": "a"}, up).status_code == 201
    assert raw.req("POST", p + "/docs", {"id": "u", "pk": "a", "n": 1}, up).status_code == 200
    # 2 MB document limit
    assert raw.req("POST", p + "/docs", {"id": "big", "pk": "a", "x": "y" * (2 * 1024 * 1024 + 10)}, pk).status_code == 413
    # request charge override / activity id echo
    assert raw.req("GET", p + "/docs/u", headers={**pk, "x-ms-activity-id": "11111111-2222-3333-4444-555555555555"}).headers["x-ms-activity-id"] == \
        "11111111-2222-3333-4444-555555555555"


def test_error_body_shape(one):
    c, db, co = mk(one)
    r = RC.Raw(one.port).req("GET", coll_path(db, co) + "/docs/x", headers={"x-ms-documentdb-partitionkey": '["a"]'})
    j = r.json()
    assert j["code"] == "NotFound" and j["message"].startswith("Message: {") and "ActivityId:" in j["message"]


def test_pkranges_and_query_plan_endpoints(one):
    """pkranges: one range covering ["", "FF"); query plan (x-ms-cosmos-is-query-plan-request): a single-range plan (see cosmos_known.md)."""
    c, db, co = mk(one)
    raw, p = RC.Raw(one.port), coll_path(db, co)
    r = raw.req("GET", p + "/pkranges")
    rng = r.json()["PartitionKeyRanges"]
    assert len(rng) == 1 and rng[0]["id"] == "0" and rng[0]["minInclusive"] == "" and rng[0]["maxExclusive"] == "FF"
    r = raw.req("POST", p + "/docs", {"query": "SELECT VALUE COUNT(1) FROM c", "parameters": []},
                {"x-ms-cosmos-is-query-plan-request": "True", "x-ms-documentdb-isquery": "True", "Content-Type": "application/query+json",
                 "x-ms-cosmos-supported-query-features": "Aggregate,Distinct,MultipleOrderBy,OffsetAndLimit,OrderBy,Top,GroupBy"})
    plan = r.json()
    assert r.status_code == 200 and plan["partitionedQueryExecutionInfoVersion"] == 2
    assert plan["queryInfo"]["hasSelectValue"] is True and plan["queryRanges"] == [{"min": "", "max": "FF", "isMinInclusive": True, "isMaxInclusive": False}]


def test_sdk_query_plan_handshake_path(handshake):
    """With WARP_COSMOSWIRE_QUERY_PLAN_HANDSHAKE=true a cross-partition query without a partition key gets the real gateway's first-chance
    400/substatus 1004 carrying the plan: the Python SDK then requests pkranges and runs the query per range (x-ms-documentdb-partitionkeyrangeid)."""
    c, db, co = mk(handshake)
    for i in range(30):
        co.upsert_item({"id": "i%02d" % i, "pk": "p%d" % (i % 5), "n": i})
    raw = RC.Raw(handshake.port)
    r = raw.req("POST", coll_path(db, co) + "/docs", {"query": "SELECT * FROM c ORDER BY c.n", "parameters": []},
                {"x-ms-documentdb-isquery": "True", "Content-Type": "application/query+json", "x-ms-documentdb-query-enablecrosspartition": "True"})
    assert r.status_code == 400 and r.headers["x-ms-substatus"] == "1004"
    assert json.loads(r.json()["additionalErrorInfo"])["queryRanges"]
    assert [d["n"] for d in co.query_items("SELECT * FROM c ORDER BY c.n DESC", enable_cross_partition_query=True)] == list(range(29, -1, -1))
    assert list(co.query_items("SELECT VALUE COUNT(1) FROM c", enable_cross_partition_query=True)) == [30]
    assert [d["n"] for d in co.query_items("SELECT TOP 3 * FROM c ORDER BY c.n", enable_cross_partition_query=True)] == [0, 1, 2]
    assert len(list(co.query_items("SELECT * FROM c", partition_key="p1"))) == 6  # single partition: served directly


# ---------------------------------------------------------------------------------------------
# the official Python SDK
# ---------------------------------------------------------------------------------------------

def test_sdk_crud_etag_conflicts_and_errors(one):
    c, db, co = mk(one)
    d = co.create_item({"id": "a", "pk": "x", "v": 1})
    assert co.read_item("a", "x")["v"] == 1
    with pytest.raises(exceptions.CosmosResourceExistsError):
        co.create_item({"id": "a", "pk": "x"})
    with pytest.raises(exceptions.CosmosResourceNotFoundError):
        co.read_item("nope", "x")
    with pytest.raises(exceptions.CosmosAccessConditionFailedError):
        co.replace_item("a", {"id": "a", "pk": "x", "v": 2}, etag='"bad"', match_condition=core.MatchConditions.IfNotModified)
    n = co.replace_item("a", {"id": "a", "pk": "x", "v": 2}, etag=d["_etag"], match_condition=core.MatchConditions.IfNotModified)
    assert n["v"] == 2 and n["_etag"] != d["_etag"]
    with pytest.raises(exceptions.CosmosAccessConditionFailedError):
        co.delete_item("a", "x", etag=d["_etag"], match_condition=core.MatchConditions.IfNotModified)
    co.delete_item("a", "x")
    with pytest.raises(exceptions.CosmosResourceNotFoundError):
        co.read_item("a", "x")
    # a partition key of every scalar type, and a missing one
    for i, v in enumerate(["s", 5, 5.5, True, None]):
        co.create_item({"id": "t%d" % i, "pk": v})
        assert co.read_item("t%d" % i, v)["id"] == "t%d" % i
    co.create_item({"id": "nopk"})
    assert co.read_item("nopk", NonePK)["id"] == "nopk"


def test_sdk_patch(one):
    """Partial document update docs: add/set/replace/remove/incr/move, conditional patch, at most 10 operations."""
    c, db, co = mk(one)
    co.create_item({"id": "p", "pk": "x", "n": 1, "tags": ["a"], "o": {"k": 1}})
    r = co.patch_item("p", "x", [{"op": "add", "path": "/tags/-", "value": "b"}, {"op": "incr", "path": "/n", "value": 4},
                                 {"op": "set", "path": "/o/z", "value": True}, {"op": "remove", "path": "/o/k"},
                                 {"op": "add", "path": "/tags/0", "value": "first"}, {"op": "replace", "path": "/n", "value": 9},
                                 {"op": "incr", "path": "/m", "value": 3}])
    assert (r["tags"], r["n"], r["o"], r["m"]) == (["first", "a", "b"], 9, {"z": True}, 3)
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        co.patch_item("p", "x", [{"op": "replace", "path": "/missing", "value": 1}])
    assert e.value.status_code == 400
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        co.patch_item("p", "x", [{"op": "set", "path": "/id", "value": "q"}])
    assert e.value.status_code == 400
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        co.patch_item("p", "x", [{"op": "set", "path": "/pk", "value": "q"}])
    assert e.value.status_code == 400
    with pytest.raises(exceptions.CosmosAccessConditionFailedError):
        co.patch_item("p", "x", [{"op": "set", "path": "/n", "value": 0}], filter_predicate="from c where c.n = 12345")
    assert co.patch_item("p", "x", [{"op": "set", "path": "/n", "value": 0}], filter_predicate="from c where c.n = 9")["n"] == 0
    with pytest.raises(exceptions.CosmosHttpResponseError):
        co.patch_item("p", "x", [{"op": "set", "path": "/a%d" % i, "value": i} for i in range(11)])


def test_sdk_transactional_batch(one):
    """Transactional batch: atomic within one partition key; 207 with 424 for the others when one operation fails."""
    c, db, co = mk(one)
    co.create_item({"id": "e", "pk": "b", "v": 0})
    res = co.execute_item_batch([("create", ({"id": "n1", "pk": "b"},)), ("upsert", ({"id": "n2", "pk": "b", "v": 2},)),
                                 ("replace", ("e", {"id": "e", "pk": "b", "v": 1})), ("read", ("n1",)),
                                 ("patch", ("e", [{"op": "incr", "path": "/v", "value": 10}])), ("delete", ("n2",))], partition_key="b")
    assert [r["statusCode"] for r in res] == [201, 201, 200, 200, 200, 204]
    assert co.read_item("e", "b")["v"] == 11
    with pytest.raises(exceptions.CosmosBatchOperationError) as ex:
        co.execute_item_batch([("create", ({"id": "n3", "pk": "b"},)), ("create", ({"id": "n1", "pk": "b"},)), ("delete", ("e",))], partition_key="b")
    assert ex.value.status_code == 409
    for gone in ("n3",):  # rolled back
        with pytest.raises(exceptions.CosmosResourceNotFoundError):
            co.read_item(gone, "b")
    assert co.read_item("e", "b")["v"] == 11
    # raw: the response carries per-operation statuses with 424 for the untouched ones
    raw = RC.Raw(one.port)
    r = raw.req("POST", coll_path(db, co) + "/docs",
                [{"operationType": "Create", "resourceBody": {"id": "z1", "pk": "b"}}, {"operationType": "Create", "resourceBody": {"id": "n1", "pk": "b"}},
                 {"operationType": "Read", "id": "e"}],
                {"x-ms-cosmos-is-batch-request": "True", "x-ms-cosmos-batch-atomic": "True", "x-ms-documentdb-partitionkey": '["b"]'})
    assert r.status_code == 207 and [o["statusCode"] for o in r.json()] == [424, 409, 424]
    # 101 operations are refused
    r = raw.req("POST", coll_path(db, co) + "/docs", [{"operationType": "Read", "id": "e"}] * 101,
                {"x-ms-cosmos-is-batch-request": "True", "x-ms-documentdb-partitionkey": '["b"]'})
    assert r.status_code == 400


def test_sdk_change_feed(two):
    """Change feed (A-IM: Incremental feed): latest version of each changed document in _lsn order per partition key, etag continuation, 304."""
    c, db, co = mk(two)
    for i in range(12):
        co.upsert_item({"id": "d%02d" % i, "pk": "p%d" % (i % 4), "v": 0})
    feed = list(co.query_items_change_feed(start_time="Beginning"))
    assert sorted(d["id"] for d in feed) == ["d%02d" % i for i in range(12)]
    by_pk = {}
    for d in feed:
        by_pk.setdefault(d["pk"], []).append(d["_lsn"])
    assert all(v == sorted(v) for v in by_pk.values())  # ordered by _lsn inside a partition key
    # continuation via the raw etag protocol: nothing new -> 304; a change shows up as the latest version only
    raw, p = RC.Raw(two.port), coll_path(db, co)
    h = {"A-IM": "Incremental feed", "x-ms-max-item-count": "5"}
    r = raw.req("GET", p + "/docs", headers=h)
    assert r.status_code == 200 and len(r.json()["Documents"]) == 5
    seen, etag = list(r.json()["Documents"]), r.headers["etag"]
    while True:
        r = raw.req("GET", p + "/docs", headers={**h, "If-None-Match": etag})
        if r.status_code == 304:
            etag = r.headers["etag"]
            break
        seen += r.json()["Documents"]
        etag = r.headers["etag"]
    assert sorted(d["id"] for d in seen) == ["d%02d" % i for i in range(12)]
    co.upsert_item({"id": "d03", "pk": "p3", "v": 1})
    co.upsert_item({"id": "d03", "pk": "p3", "v": 2})
    co.delete_item("d05", "p1")
    r = raw.req("GET", p + "/docs", headers={"A-IM": "Incremental feed", "If-None-Match": etag})
    assert r.status_code == 200 and [(d["id"], d["v"]) for d in r.json()["Documents"]] == [("d03", 2)]  # deletes are not in the incremental feed
    assert raw.req("GET", p + "/docs", headers={"A-IM": "Incremental feed", "If-None-Match": r.headers["etag"]}).status_code == 304
    # "If-None-Match: *" starts from now
    r = raw.req("GET", p + "/docs", headers={"A-IM": "Incremental feed", "If-None-Match": "*"})
    assert r.status_code == 304
    co.upsert_item({"id": "later", "pk": "p0"})
    r2 = raw.req("GET", p + "/docs", headers={"A-IM": "Incremental feed", "If-None-Match": r.headers["etag"]})
    assert [d["id"] for d in r2.json()["Documents"]] == ["later"]


def test_sdk_ttl_and_sweeper(one):
    """TTL: container defaultTtl, per-item ttl overrides (-1 never), items expire after _ts + ttl and are physically swept."""
    c, db, co = mk(one, default_ttl=6)
    co.create_item({"id": "default", "pk": "t"})
    co.create_item({"id": "never", "pk": "t", "ttl": -1})
    co.create_item({"id": "short", "pk": "t", "ttl": 1})
    co.create_item({"id": "long", "pk": "t", "ttl": 3600})
    time.sleep(2.5)
    with pytest.raises(exceptions.CosmosResourceNotFoundError):
        co.read_item("short", "t")
    assert co.read_item("default", "t")
    time.sleep(4.5)
    ids = sorted(d["id"] for d in co.query_items("SELECT * FROM c", enable_cross_partition_query=True))
    assert ids == ["long", "never"]
    time.sleep(2)  # the sweeper (1 s in tests) physically deleted the expired rows
    assert sql(one.pgs[0], "SELECT count(*) FROM warp_cosmos_docs WHERE coll = %s", (co.id,))[0][0] == 2
    # TTL disabled on a container: the item ttl is ignored
    c2, db2, co2 = mk(one)
    co2.create_item({"id": "x", "pk": "t", "ttl": 1})
    time.sleep(2.2)
    assert co2.read_item("x", "t")


def test_sdk_unique_keys_and_hierarchical_partition_keys(one):
    c, db, co = mk(one, unique_key_policy={"uniqueKeys": [{"paths": ["/email"]}, {"paths": ["/a", "/b"]}]})
    co.create_item({"id": "1", "pk": "p", "email": "x@y", "a": 1, "b": 2})
    with pytest.raises(exceptions.CosmosResourceExistsError):
        co.create_item({"id": "2", "pk": "p", "email": "x@y", "a": 9, "b": 9})
    with pytest.raises(exceptions.CosmosResourceExistsError):
        co.create_item({"id": "3", "pk": "p", "email": "z@y", "a": 1, "b": 2})
    co.create_item({"id": "4", "pk": "other", "email": "x@y", "a": 1, "b": 2})  # unique within a logical partition only
    co.replace_item("1", {"id": "1", "pk": "p", "email": "new@y", "a": 1, "b": 2})
    co.create_item({"id": "5", "pk": "p", "email": "x@y", "a": 7, "b": 7})  # the old value was released
    co.delete_item("5", "p")
    co.create_item({"id": "6", "pk": "p", "email": "x@y", "a": 8, "b": 8})
    # hierarchical (MultiHash) partition keys: full keys and prefixes
    h = db.create_container(uniq("h"), partition_key=PartitionKey(path=["/tenant", "/user"], kind="MultiHash"))
    for t in ("t1", "t2"):
        for u in ("u1", "u2", "u3"):
            h.create_item({"id": t + u, "tenant": t, "user": u})
    assert h.read_item("t1u2", partition_key=["t1", "u2"])["id"] == "t1u2"
    assert [d["id"] for d in h.query_items("SELECT * FROM c", partition_key=["t2", "u3"])] == ["t2u3"]
    # prefix queries: by the partition key header on the wire (the SDKs send EPK ranges instead, which cosmoswire refuses: cosmos_known.md)
    rows, _, _ = RC.Raw(one.port).query("/dbs/%s/colls/%s" % (db.id, h.id), "SELECT c.id FROM c", pk=["t2"])
    assert sorted(x["id"] for x in rows) == ["t2u1", "t2u2", "t2u3"]
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        list(h.query_items("SELECT * FROM c", partition_key=["t2"]))
    assert e.value.status_code == 501
    with pytest.raises(exceptions.CosmosHttpResponseError):
        h.read_item("t1u2", partition_key=["t1"])


def test_sdk_paging_and_continuation(two):
    c, db, co = mk(two)
    for i in range(53):
        co.upsert_item({"id": "i%03d" % i, "pk": "p%d" % (i % 7), "n": i})
    for q, expect_n in (("SELECT * FROM c", 53), ("SELECT * FROM c ORDER BY c.n", 53), ("SELECT DISTINCT VALUE c.pk FROM c", 7),
                        ("SELECT c.id FROM c JOIN t IN [1,2]", 106)):
        it = co.query_items(q, enable_cross_partition_query=True, max_item_count=10)
        pages = [list(p) for p in it.by_page()]
        assert sum(map(len, pages)) == expect_n and all(len(p) <= 10 for p in pages), q
        assert len(pages) >= expect_n // 10, q
    ordered = [d["n"] for d in co.query_items("SELECT * FROM c ORDER BY c.n DESC", enable_cross_partition_query=True, max_item_count=8)]
    assert ordered == list(range(52, -1, -1))
    # a token resumes exactly where the page ended, also across the two hosts
    it = co.query_items("SELECT VALUE c.id FROM c", enable_cross_partition_query=True, max_item_count=20)
    pager = it.by_page()
    first = list(next(pager))
    token = pager.continuation_token
    rest = [x for p in co.query_items("SELECT VALUE c.id FROM c", enable_cross_partition_query=True, max_item_count=20).by_page(token) for x in p]
    assert len(first) == 20 and len(set(first) | set(rest)) == 53 and not set(first) & set(rest)
    assert len(list(co.read_all_items())) == 53
    assert [d["id"] for d in co.query_items("SELECT * FROM c ORDER BY c.n OFFSET 50 LIMIT 100", enable_cross_partition_query=True)] == ["i050", "i051", "i052"]


def test_sdk_catalog_operations_and_throughput(one):
    c = client(one)
    name = uniq("db")
    db = c.create_database(name)
    assert name in [d["id"] for d in c.list_databases()]
    assert [d["id"] for d in c.query_databases("SELECT * FROM root r WHERE r.id = '%s'" % name)] == [name]
    co = db.create_container("k", partition_key=PartitionKey(path="/pk"), offer_throughput=1000)
    assert co.get_throughput().offer_throughput == 1000
    co.replace_throughput(2000)
    assert co.get_throughput().offer_throughput == 2000
    assert [d["id"] for d in db.list_containers()] == ["k"]
    assert [d["id"] for d in db.query_containers("SELECT * FROM c WHERE c.id = 'k'")] == ["k"]
    props = co.read()
    assert props["partitionKey"]["paths"] == ["/pk"]
    with pytest.raises(exceptions.CosmosResourceExistsError):
        c.create_database(name)
    db.delete_container("k")
    with pytest.raises(exceptions.CosmosResourceNotFoundError):
        co.read()
    c.delete_database(name)
    with pytest.raises(exceptions.CosmosResourceNotFoundError):
        c.get_database_client(name).read()


def test_scripts_are_stored_but_never_executed(one):
    c, db, co = mk(one)
    sp = co.scripts.create_stored_procedure({"id": "sp", "body": "function(){ getContext().getResponse().setBody(1); }"})
    assert sp["id"] == "sp" and co.scripts.get_stored_procedure("sp")["body"].startswith("function")
    assert [s["id"] for s in co.scripts.list_stored_procedures()] == ["sp"]
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        co.scripts.execute_stored_procedure("sp", partition_key="x")
    assert e.value.status_code == 501 and "not supported" in str(e.value)
    co.scripts.create_trigger({"id": "tr", "body": "function(){}", "triggerType": "Pre", "triggerOperation": "All"})
    co.scripts.create_user_defined_function({"id": "f", "body": "function(x){return x;}"})
    assert [s["id"] for s in co.scripts.list_triggers()] == ["tr"] and [s["id"] for s in co.scripts.list_user_defined_functions()] == ["f"]
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        co.create_item({"id": "q", "pk": "x"}, pre_trigger_include="tr")
    assert e.value.status_code == 501
    with pytest.raises(exceptions.CosmosHttpResponseError) as e:
        list(co.query_items("SELECT udf.f(c.id) FROM c", enable_cross_partition_query=True))
    assert e.value.status_code == 400 and "not supported" in str(e.value)
    co.scripts.delete_stored_procedure("sp")
    assert list(co.scripts.list_stored_procedures()) == []


# ---------------------------------------------------------------------------------------------
# SQL semantics from the documentation (hand-written expected values)
# ---------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fam(two):
    c, db, co = mk(two)
    for f in FAMILIES:
        co.create_item(f)
    return two, db, co


def q(fam, sql_, params=None, **kw):
    _, _, co = fam
    return list(co.query_items(sql_, parameters=params, enable_cross_partition_query=True, **kw))


def test_doc_select_from_where(fam):
    """Query language docs: SELECT/FROM/WHERE examples over the Families sample data."""
    r = q(fam, "SELECT * FROM Families f WHERE f.id = 'AndersenFamily'")
    assert len(r) == 1 and r[0]["address"]["city"] == "Seattle" and "_etag" in r[0]
    assert q(fam, "SELECT f.address.city, f.address.state FROM Families f WHERE f.id = 'AndersenFamily'") == [{"city": "Seattle", "state": "WA"}]
    assert q(fam, 'SELECT {"Name":f.id, "City":f.address.city} AS Family FROM Families f WHERE f.address.city = f.address.state') == \
        [{"Family": {"Name": "WakefieldFamily", "City": "NY"}}]
    assert q(fam, "SELECT VALUE f.address.city FROM Families f ORDER BY f.address.city") == ["NY", "Seattle"]
    assert sorted(q(fam, "SELECT VALUE f.id FROM Families f WHERE f.address.state IN ('NY', 'WA')")) == ["AndersenFamily", "WakefieldFamily"]
    assert q(fam, "SELECT VALUE f.id FROM Families f WHERE f.creationDate BETWEEN 1431620400 AND 1431620470") == ["WakefieldFamily"]
    assert q(fam, "SELECT VALUE f.id FROM Families f WHERE f.lastName = @n", [{"name": "@n", "value": "Andersen"}]) == ["AndersenFamily"]


def test_doc_join_and_subqueries(fam):
    assert sorted(q(fam, "SELECT VALUE c.givenName FROM Families f JOIN c IN f.children")) == ["Jesse", "Lisa"]  # Andersen's child has no givenName
    assert sorted(q(fam, "SELECT VALUE p.givenName FROM Families f JOIN c IN f.children JOIN p IN c.pets")) == ["Fluffy", "Goofy", "Shadow"]
    assert q(fam, "SELECT VALUE f.id FROM Families f WHERE EXISTS(SELECT VALUE p FROM p IN f.parents WHERE p.firstName = 'Thomas')") == ["AndersenFamily"]
    assert sorted(q(fam, "SELECT VALUE ARRAY_LENGTH(f.children) FROM Families f")) == [1, 2]
    assert sorted(q(fam, "SELECT VALUE (SELECT VALUE COUNT(1) FROM c IN f.children) FROM Families f")) == [1, 2]
    assert sorted(q(fam, "SELECT VALUE ARRAY(SELECT VALUE p.givenName FROM c IN f.children JOIN p IN c.pets) FROM Families f")) == [["Fluffy"], ["Goofy", "Shadow"]]


def test_doc_aggregates_group_by_order_by(fam):
    assert q(fam, "SELECT VALUE COUNT(1) FROM Families f") == [2]
    assert q(fam, "SELECT VALUE COUNT(1) FROM Families f WHERE f.id = 'none'") == [0]
    assert q(fam, "SELECT VALUE MAX(f.creationDate) FROM Families f") == [1431620472]
    assert q(fam, "SELECT VALUE SUM(f.creationDate) FROM Families f") == [2863240934]
    assert q(fam, "SELECT VALUE AVG(f.creationDate) FROM Families f") == [1431620467]
    assert q(fam, "SELECT COUNT(1) AS n, MIN(f.creationDate) AS lo FROM Families f") == [{"n": 2, "lo": 1431620462}]
    assert sorted(q(fam, "SELECT f.isRegistered, COUNT(1) AS n FROM Families f GROUP BY f.isRegistered"), key=lambda x: x["isRegistered"]) == \
        [{"isRegistered": False, "n": 1}, {"isRegistered": True, "n": 1}]
    assert q(fam, "SELECT VALUE f.id FROM Families f ORDER BY f.creationDate DESC") == ["AndersenFamily", "WakefieldFamily"]
    assert q(fam, "SELECT TOP 1 VALUE f.id FROM Families f ORDER BY f.creationDate") == ["WakefieldFamily"]


def test_doc_system_functions(fam):
    r = q(fam, "SELECT ABS(-1), CEILING(123.45), FLOOR(-123.45), ROUND(2.5), TRUNC(-2.6), POWER(2, 3), SQRT(16), SIGN(-2) FROM Families f WHERE f.id = 'AndersenFamily'")[0]
    assert list(r.values()) == [1, 124, -124, 3, -2, 8, 4, -1]
    r = q(fam, "SELECT UPPER('abc'), LOWER('ABC'), LEFT('abc', 2), RIGHT('abc', 2), SUBSTRING('abc', 1, 1), INDEX_OF('abc', 'c'), LENGTH('abc'), "
               "CONCAT('a', 'b', 'c'), REPLACE('aXa', 'X', '-'), REVERSE('Abc'), REPLICATE('a', 3), TRIM('  a  '), LTRIM(' a'), RTRIM('a ') "
               "FROM Families f WHERE f.id = 'AndersenFamily'")[0]
    assert list(r.values()) == ["ABC", "abc", "ab", "bc", "b", 2, 3, "abc", "a-a", "cbA", "aaa", "a", "a", "a"]
    r = q(fam, "SELECT IS_DEFINED(f.lastName), IS_DEFINED(f.zzz), IS_ARRAY(f.parents), IS_BOOL(f.isRegistered), IS_NUMBER(f.creationDate), IS_OBJECT(f.address), "
               "IS_STRING(f.id), IS_NULL(f.lastName), IS_PRIMITIVE(f.address) FROM Families f WHERE f.id = 'AndersenFamily'")[0]
    assert list(r.values()) == [True, False, True, True, True, True, True, False, False]
    r = q(fam, "SELECT ARRAY_CONTAINS(f.children, {'gender':'female'}, true) AS a, ARRAY_CONTAINS(f.children, {'gender':'female'}) AS b, "
               "ARRAY_LENGTH(f.parents) AS l FROM Families f WHERE f.id = 'AndersenFamily'")[0]
    assert r == {"a": True, "b": False, "l": 2}  # partial match = object subset
    assert q(fam, "SELECT VALUE f.id FROM Families f WHERE STARTSWITH(f.id, 'wake', true) AND CONTAINS(f.id, 'FIELD', true) AND REGEXMATCH(f.id, '^W.*y$')") == ["WakefieldFamily"]
    assert q(fam, "SELECT VALUE ST_DISTANCE({'type':'Point','coordinates':[0,0]}, {'type':'Point','coordinates':[0,1]}) FROM Families f WHERE f.id = 'AndersenFamily'")[0] == \
        pytest.approx(111319.49, rel=1e-3)
    assert q(fam, "SELECT VALUE ST_WITHIN({'type':'Point','coordinates':[1,1]}, {'type':'Polygon','coordinates':[[[0,0],[0,2],[2,2],[2,0],[0,0]]]}) "
                  "FROM Families f WHERE f.id = 'AndersenFamily'") == [True]


def test_doc_undefined_null_and_type_order(two):
    """Undefined vs null, cross-type comparison and the ORDER BY type order undefined < null < boolean < number < string < array < object."""
    c, db, co = mk(two)
    vals = {"o": {"a": 1}, "s": "s", "n5": 5, "b": True, "nul": None, "arr": [1]}
    for k, v in vals.items():
        co.create_item({"id": k, "pk": k, "v": v})
    co.create_item({"id": "undef", "pk": "undef"})
    order = [d["id"] for d in co.query_items("SELECT c.id FROM c ORDER BY c.v", enable_cross_partition_query=True)]
    assert order == ["undef", "nul", "b", "n5", "s", "arr", "o"]
    assert [d["id"] for d in co.query_items("SELECT c.id FROM c ORDER BY c.v DESC", enable_cross_partition_query=True)] == order[::-1]
    ids = lambda w: sorted(x for x in co.query_items("SELECT VALUE c.id FROM c WHERE " + w, enable_cross_partition_query=True))  # noqa: E731
    assert ids("c.v = null") == ["nul"]  # null equals only null; undefined never equals anything
    assert ids("NOT IS_DEFINED(c.v)") == ["undef"]
    assert ids("IS_NULL(c.v)") == ["nul"]
    assert ids("c.v != 5") == sorted(["o", "s", "b", "nul", "arr"])  # cross-type inequality is true, undefined is excluded
    assert ids("c.v < 6") == ["n5"]  # ordering comparison across types is undefined
    assert ids("c.v = 5 OR c.v = 'zz'") == ["n5"]


# ---------------------------------------------------------------------------------------------
# differential test against the independent Python implementation
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("seed", [1, 2, 3])
def test_randomized_sql_matches_independent_implementation(two, seed):
    rnd = random.Random(seed)
    c, db, co = mk(two)
    docs = [DE.rand_doc(rnd, i) for i in range(90)]
    for d in docs:
        co.create_item(d)
    raw, p = RC.Raw(two.port), coll_path(db, co)
    checked = 0
    for _ in range(60):
        pred = DE.rand_pred(rnd)
        shape = rnd.choice(DE.SHAPES)
        tail, want, ordered = DE.expected(docs, pred, shape, rnd)
        rows, pages, _ = raw.query(p, "SELECT " + tail, max_items=rnd.choice([None, 7, 25]))
        got = DE.norm(rows)
        want = DE.norm(want)
        if not ordered:
            key = lambda x: json.dumps(x, sort_keys=True)  # noqa: E731
            got, want = sorted(got, key=key), sorted(want, key=key)
        assert got == want, "SELECT " + tail
        checked += 1
    assert checked == 60


# ---------------------------------------------------------------------------------------------
# sharding, concurrency, MCP
# ---------------------------------------------------------------------------------------------

def test_documents_are_sharded_over_two_postgres_hosts(two):
    c, db, co = mk(two)
    for i in range(200):
        co.create_item({"id": "s%03d" % i, "pk": "k%03d" % i, "n": i})
    counts = [sql(pg, "SELECT count(*) FROM warp_cosmos_docs WHERE coll = %s", (co.id,))[0][0] for pg in two.pgs]
    assert sum(counts) == 200 and min(counts) > 40, counts
    cat = [sql(pg, "SELECT count(*) FROM warp_cosmos_colls WHERE id = %s", (co.id,))[0][0] for pg in two.pgs]
    assert cat == [1, 0]  # the catalog lives on the first host only
    # a partition key value lives wholly on one host
    for k in ("k007", "k100"):
        assert sum(sql(pg, "SELECT count(*) FROM warp_cosmos_docs WHERE coll = %s AND pkh = %s", (co.id, '["%s"]' % k))[0][0] for pg in two.pgs) == 1
    # cross-partition results merged across hosts: order, top, offset, aggregates, distinct
    assert [d["n"] for d in co.query_items("SELECT TOP 5 * FROM c ORDER BY c.n DESC", enable_cross_partition_query=True)] == [199, 198, 197, 196, 195]
    assert [d["n"] for d in co.query_items("SELECT * FROM c ORDER BY c.n OFFSET 100 LIMIT 3", enable_cross_partition_query=True)] == [100, 101, 102]
    assert list(co.query_items("SELECT VALUE SUM(c.n) FROM c", enable_cross_partition_query=True)) == [sum(range(200))]
    assert list(co.query_items("SELECT VALUE AVG(c.n) FROM c", enable_cross_partition_query=True)) == [99.5]
    assert list(co.query_items("SELECT VALUE MAX(c.n) FROM c WHERE c.n < 150", enable_cross_partition_query=True)) == [149]
    assert len(list(co.query_items("SELECT DISTINCT VALUE c.pk FROM c", enable_cross_partition_query=True))) == 200
    # a partition key filter goes to one host only (the other holds no row of it)
    assert [d["n"] for d in co.query_items("SELECT * FROM c WHERE c.pk = 'k042'", enable_cross_partition_query=True)] == [42]
    # dropping the container removes the data on every host
    db.delete_container(co)
    assert [sql(pg, "SELECT count(*) FROM warp_cosmos_docs WHERE coll = %s", (co.id,))[0][0] for pg in two.pgs] == [0, 0]


def test_concurrent_writers_keep_change_feed_and_counts_consistent(two):
    c, db, co = mk(two)

    def work(t):
        cl = client(two).get_database_client(db.id).get_container_client(co.id)
        for i in range(25):
            cl.upsert_item({"id": "t%d-%d" % (t, i), "pk": "k%d" % ((t * 25 + i) % 10), "n": i})
            if i % 5 == 0:
                cl.upsert_item({"id": "t%d-%d" % (t, i), "pk": "k%d" % ((t * 25 + i) % 10), "n": -i})

    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        list(ex.map(work, range(8)))
    assert list(co.query_items("SELECT VALUE COUNT(1) FROM c", enable_cross_partition_query=True)) == [200]
    feed = list(co.query_items_change_feed(start_time="Beginning"))
    assert len(feed) == 200 and len({d["id"] for d in feed}) == 200


def test_mcp_tools_share_the_wire_data_and_read_only_hides_writes():
    mcp = free_port()
    w = L.CosmosWarp(1, extra_env={"WARP_MCP_PORT": str(mcp)})
    ro = None
    try:
        ep = create_endpoint(w, "cosmos-ep", "db:default", wait_live=False)
        path, token = ep["path"], ep["token"]
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                tool_names(mcp, path=path, token=token)
                break
            except Exception:  # noqa: BLE001 -- MCP listener still starting
                time.sleep(1)

        def cj(name, args=None):
            return call_json(mcp, name, args, path=path, token=token)

        names = tool_names(mcp, path=path, token=token)
        for t in ["cosmos_list_databases", "cosmos_list_containers", "cosmos_query", "cosmos_get_item", "cosmos_create_database",
                  "cosmos_create_container", "cosmos_upsert_item", "cosmos_delete_item"]:
            assert t in names, t
        # created over the wire with the SDK, visible to the tools
        cli = client(w)
        db = cli.create_database("mcpdb")
        co = db.create_container("mc", partition_key=PartitionKey(path="/pk"))
        co.create_item({"id": "w1", "pk": "a", "n": 1})
        assert "mcpdb" in [d["id"] for d in cj("cosmos_list_databases")["databases"]]
        assert cj("cosmos_list_containers", {"database": "mcpdb"})["containers"][0]["items"] == 1
        assert cj("cosmos_query", {"database": "mcpdb", "container": "mc", "query": "SELECT VALUE c.n FROM c WHERE c.id = @i",
                                    "parameters": [{"name": "@i", "value": "w1"}]})["items"] == [1]
        # created by the tools, visible on the wire
        cj("cosmos_upsert_item", {"database": "mcpdb", "container": "mc", "item": {"id": "m1", "pk": "b", "n": 2}, "partitionKey": "b"})
        assert co.read_item("m1", "b")["n"] == 2
        assert cj("cosmos_get_item", {"database": "mcpdb", "container": "mc", "id": "w1", "partitionKey": "a"})["n"] == 1
        cj("cosmos_delete_item", {"database": "mcpdb", "container": "mc", "id": "m1", "partitionKey": "b"})
        with pytest.raises(exceptions.CosmosResourceNotFoundError):
            co.read_item("m1", "b")
        assert "NotFound" in call(mcp, "cosmos_get_item", {"database": "mcpdb", "container": "mc", "id": "m1", "partitionKey": "b"},
                                  path=path, token=token, expect_error=True)[0]
        cj("cosmos_create_database", {"database": "mcpdb2"})
        cj("cosmos_create_container", {"database": "mcpdb2", "container": "x", "partitionKeyPath": "/t"})
        assert client(w).get_database_client("mcpdb2").get_container_client("x").read()["partitionKey"]["paths"] == ["/t"]
        r = requests.get("http://localhost:%d/api/metrics/summary" % w.proc.metrics_port, headers={"Authorization": "Bearer warp-test-admin-token"}, timeout=10)
        assert "cosmoswire" in r.text and "mcp-cosmosstore" in r.text, r.text[:400]
        mcp2 = free_port()
        ro = L.CosmosWarp(pgs=w.pgs, default_store=False, extra_env={"WARP_MCP_PORT": str(mcp2), "WARP_MCP_READ_ONLY": "true"})
        deadline = time.time() + 30
        rnames = set()
        while time.time() < deadline:
            try:
                rnames = tool_names(mcp2, path=path, token=token)
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
        assert "cosmos_query" in rnames and "cosmos_get_item" in rnames
        for hidden in ("cosmos_upsert_item", "cosmos_delete_item", "cosmos_create_database", "cosmos_create_container"):
            assert hidden not in rnames
    finally:
        if ro is not None:
            ro.proc.close()
        w.close()
