"""DynamoDB conformance of dynamowire, against a real Warp on real Postgres.

Every test runs twice: on ONE Postgres backend and on TWO sharded backends (both in the same backend
set with the DynamoDB store enabled, items hashed over them by partition key). What is covered:
secondary indexes (GSI/LSI query/scan, projections, sparse indexes, pagination across shards, adding and
dropping a GSI on a live table), parallel scan, TTL expiry, atomic conditional writes / transactions under
concurrency (also across shards), PartiQL, the legacy parameters, validation errors, ConsumedCapacity,
ItemCollectionMetrics, tags and continuous backups.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_PG_BIN=/opt/homebrew/opt/postgresql@17/bin WARP_TEST_JAR=<jar> \
      python3 -m pytest test_dynamowire_conformance.py -q
"""
import concurrent.futures as cf
import os
import time
import uuid

import boto3
import psycopg2
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def pg_name(table):
    return "dynamo_item_" + table.lower().replace("-", "_").replace(".", "_")


class Env:
    def __init__(self, kind, pgs, warp):
        self.kind, self.pgs, self.warp = kind, pgs, warp
        self.client = boto3.client("dynamodb", endpoint_url=f"http://localhost:{warp.frontend_port}",
                                   region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t",
                                   config=Config(retries={"max_attempts": 0}, max_pool_connections=64))
        # no client-side parameter validation: lets a test send values the SDK itself would refuse
        self.raw = boto3.client("dynamodb", endpoint_url=f"http://localhost:{warp.frontend_port}",
                                region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t",
                                config=Config(retries={"max_attempts": 0}, parameter_validation=False))

    @property
    def sharded(self):
        return self.kind == "sharded"

    def count(self, table):
        return sum(sql(pg, f"SELECT count(*) FROM {pg_name(table)}")[0][0] for pg in self.pgs)


@pytest.fixture(scope="module", params=["single", "sharded"])
def env(request):
    n = 2 if request.param == "sharded" else 1
    pgs = [RealPostgres() for _ in range(n)]
    ports = isolated_ports("WARP_DYNAMOWIRE_PORT")
    warp = WarpProcess(pgs[0], "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env={
        **ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_DYNAMOWIRE_TTL_SWEEP_MS": "500"})
    if n == 2:
        def api(method, path, body=None, expect=200):
            r = requests.request(method, f"http://localhost:{warp.metrics_port}{path}", json=body, timeout=60,
                                 headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
            assert r.status_code == expect, (r.status_code, r.text)
            return r.json()

        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["dynamodb"]})
        api("POST", "/api/backend-sets/default/backends", {
            "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
            "enabledStores": ["dynamodb"]}, expect=201)
    yield Env(request.param, pgs, warp)
    warp.close()
    for pg in pgs:
        pg.close()


def S(v):
    return {"S": v}


def N(v):
    return {"N": str(v)}


def uname(prefix="t"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def err(fn, **kw):
    with pytest.raises(ClientError) as e:
        fn(**kw)
    return e.value.response["Error"]["Code"], e.value.response["Error"]["Message"], e.value.response


def make_table(c, name, hash_=("pk", "S"), range_=None, gsis=(), lsis=(), extra_attrs=(), **kw):
    keys = [{"AttributeName": hash_[0], "KeyType": "HASH"}]
    attrs = {hash_[0]: hash_[1]}
    if range_:
        keys.append({"AttributeName": range_[0], "KeyType": "RANGE"})
        attrs[range_[0]] = range_[1]
    extra = dict(extra_attrs)
    for g in list(gsis) + list(lsis):
        for k in g["KeySchema"]:
            attrs.setdefault(k["AttributeName"], extra.get(k["AttributeName"], "S"))
    args = dict(TableName=name, KeySchema=keys,
                AttributeDefinitions=[{"AttributeName": a, "AttributeType": t} for a, t in attrs.items()])
    if gsis:
        args["GlobalSecondaryIndexes"] = list(gsis)
    if lsis:
        args["LocalSecondaryIndexes"] = list(lsis)
    args.setdefault("BillingMode", "PAY_PER_REQUEST")
    args.update(kw)
    c.create_table(**args)
    return name


def gsi(name, hash_, range_=None, proj="ALL", include=None):
    ks = [{"AttributeName": hash_, "KeyType": "HASH"}]
    if range_:
        ks.append({"AttributeName": range_, "KeyType": "RANGE"})
    p = {"ProjectionType": proj}
    if include:
        p["NonKeyAttributes"] = include
    return {"IndexName": name, "KeySchema": ks, "Projection": p}


def lsi(name, range_, proj="ALL", include=None):
    d = gsi(name, "pk", range_, proj, include)
    return d


# ------------------------------------------------------------------------------------ indexes

def test_gsi_query_scatter_gathers_orders_and_paginates(env):
    c = env.client
    t = make_table(c, uname("gsi"), range_=("sk", "S"), extra_attrs=[("status", "S"), ("score", "N")],
                   gsis=[gsi("by-status", "status", "score")])
    for i in range(120):
        c.put_item(TableName=t, Item={"pk": S(f"p{i % 40}"), "sk": S(f"s{i}"), "status": S("open" if i % 3 else "done"),
                                      "score": N(i), "note": S("x")})
    # items without the index key are simply not in the index (sparse)
    c.put_item(TableName=t, Item={"pk": S("nostatus"), "sk": S("s"), "score": N(5)})
    if env.sharded:
        assert env.count(t) == 121 and all(sql(pg, f"SELECT count(*) FROM {pg_name(t)}")[0][0] > 0 for pg in env.pgs)

    def q(**kw):
        args = dict(TableName=t, IndexName="by-status", KeyConditionExpression="#s = :s",
                    ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":s": S("open")})
        args.update(kw)
        return c.query(**args)

    r = q()
    scores = [int(i["score"]["N"]) for i in r["Items"]]
    assert len(scores) == 80 and scores == sorted(scores), "one ordered stream merged from every shard"
    assert [int(i["score"]["N"]) for i in q(ScanIndexForward=False)["Items"]] == sorted(scores, reverse=True)
    # numeric sort-key range on the index
    r2 = c.query(TableName=t, IndexName="by-status", KeyConditionExpression="#s = :s AND score BETWEEN :a AND :b",
                 ExpressionAttributeNames={"#s": "status"},
                 ExpressionAttributeValues={":s": S("open"), ":a": N(10), ":b": N(20)})
    assert [int(i["score"]["N"]) for i in r2["Items"]] == [10, 11, 13, 14, 16, 17, 19, 20]
    # pagination with Limit walks the merged order exactly once
    seen, start, pages = [], None, 0
    while True:
        kw = {"Limit": 7}
        if start:
            kw["ExclusiveStartKey"] = start
        page = q(**kw)
        seen += [int(i["score"]["N"]) for i in page["Items"]]
        pages += 1
        start = page.get("LastEvaluatedKey")
        assert start is None or set(start) == {"pk", "sk", "status", "score"}, "index LastEvaluatedKey carries index + table keys"
        if not start:
            break
    assert seen == scores and pages >= 11
    # descending pagination
    seen, start = [], None
    while True:
        kw = {"Limit": 9, "ScanIndexForward": False}
        if start:
            kw["ExclusiveStartKey"] = start
        page = q(**kw)
        seen += [int(i["score"]["N"]) for i in page["Items"]]
        start = page.get("LastEvaluatedKey")
        if not start:
            break
    assert seen == sorted(scores, reverse=True)
    # Limit counts evaluated items, the filter applies afterwards (DynamoDB semantics)
    page = q(Limit=10, FilterExpression="note = :m", ExpressionAttributeValues={":s": S("open"), ":m": S("nomatch")})
    assert page["Count"] == 0 and page["ScannedCount"] == 10 and "LastEvaluatedKey" in page
    # scan of the whole index
    seen = []
    start = None
    while True:
        kw = {"TableName": t, "IndexName": "by-status", "Limit": 25}
        if start:
            kw["ExclusiveStartKey"] = start
        page = c.scan(**kw)
        seen += [(i["pk"]["S"], i["sk"]["S"]) for i in page["Items"]]
        start = page.get("LastEvaluatedKey")
        if not start:
            break
    assert len(seen) == 120 and len(set(seen)) == 120 and ("nostatus", "s") not in seen
    # DescribeTable reports the index with its own item count
    d = c.describe_table(TableName=t)["Table"]
    g = d["GlobalSecondaryIndexes"][0]
    assert g["IndexName"] == "by-status" and g["IndexStatus"] == "ACTIVE" and g["ItemCount"] == 120
    assert d["ItemCount"] == 121 and g["IndexArn"].endswith(f"table/{t}/index/by-status")
    # GSIs are not strongly consistent
    assert err(c.query, TableName=t, IndexName="by-status", ConsistentRead=True, KeyConditionExpression="#s = :s",
               ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":s": S("open")})[0] == "ValidationException"


def test_gsi_projections_and_key_schema_validation(env):
    c = env.client
    t = make_table(c, uname("proj"), extra_attrs=[("g", "S"), ("h", "S")],
                   gsis=[gsi("keys", "g", proj="KEYS_ONLY"), gsi("inc", "g", "h", proj="INCLUDE", include=["a"]),
                         gsi("all", "g")])
    c.put_item(TableName=t, Item={"pk": S("1"), "g": S("x"), "h": S("y"), "a": S("A"), "b": S("B")})
    for name, expect in (("keys", {"pk", "g"}), ("inc", {"pk", "g", "h", "a"}), ("all", {"pk", "g", "h", "a", "b"})):
        items = c.query(TableName=t, IndexName=name, KeyConditionExpression="g = :g",
                        ExpressionAttributeValues={":g": S("x")})["Items"]
        assert set(items[0]) == expect, name
    code, msg, _ = err(c.query, TableName=t, IndexName="keys", Select="ALL_ATTRIBUTES", KeyConditionExpression="g = :g",
                       ExpressionAttributeValues={":g": S("x")})
    assert code == "ValidationException" and "not supported for global secondary index" in msg
    code, msg, _ = err(c.query, TableName=t, IndexName="inc", ProjectionExpression="b", KeyConditionExpression="g = :g",
                       ExpressionAttributeValues={":g": S("x")})
    assert code == "ValidationException" and "does not project" in msg
    # key schema of the queried index: partition key is required, types must match
    assert err(c.query, TableName=t, IndexName="inc", KeyConditionExpression="h = :h",
               ExpressionAttributeValues={":h": S("y")})[0] == "ValidationException"
    assert err(c.query, TableName=t, IndexName="inc", KeyConditionExpression="g = :g",
               ExpressionAttributeValues={":g": N(1)})[0] == "ValidationException"
    assert err(c.query, TableName=t, IndexName="nope", KeyConditionExpression="g = :g",
               ExpressionAttributeValues={":g": S("x")})[0] == "ValidationException"
    # writes of an item whose index key has the wrong type / an empty string are rejected
    code, msg, _ = err(c.put_item, TableName=t, Item={"pk": S("2"), "g": N(5)})
    assert code == "ValidationException" and "Type mismatch for Index Key" in msg
    assert err(c.put_item, TableName=t, Item={"pk": S("3"), "g": S("")})[0] == "ValidationException"


def test_lsi_query_and_item_collection_metrics(env):
    c = env.client
    t = make_table(c, uname("lsi"), range_=("sk", "S"), extra_attrs=[("alt", "N")], lsis=[lsi("by-alt", "alt", "KEYS_ONLY")])
    for i in range(30):
        c.put_item(TableName=t, Item={"pk": S("only"), "sk": S(f"s{i:02d}"), "alt": N(100 - i), "extra": S("e")})
    c.put_item(TableName=t, Item={"pk": S("only"), "sk": S("no-alt")})
    r = c.query(TableName=t, IndexName="by-alt", KeyConditionExpression="pk = :p AND alt < :a",
                ExpressionAttributeValues={":p": S("only"), ":a": N(80)})
    assert [int(i["alt"]["N"]) for i in r["Items"]] == list(range(71, 80)) and set(r["Items"][0]) == {"pk", "sk", "alt"}
    # an LSI reads the full item on demand
    r = c.query(TableName=t, IndexName="by-alt", KeyConditionExpression="pk = :p", Select="ALL_ATTRIBUTES", Limit=2,
                ExpressionAttributeValues={":p": S("only")})
    assert r["Items"][0]["extra"]["S"] == "e" and r["Items"][0]["alt"]["N"] == "71"
    # consistent reads are allowed on an LSI, the table's sort key is not part of it
    c.query(TableName=t, IndexName="by-alt", ConsistentRead=True, KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": S("only")})
    assert err(c.query, TableName=t, IndexName="by-alt", KeyConditionExpression="pk = :p AND sk = :s",
               ExpressionAttributeValues={":p": S("only"), ":s": S("s01")})[0] == "ValidationException"
    m = c.put_item(TableName=t, Item={"pk": S("only"), "sk": S("m")}, ReturnItemCollectionMetrics="SIZE")
    assert m["ItemCollectionMetrics"]["ItemCollectionKey"] == {"pk": S("only")}
    assert len(m["ItemCollectionMetrics"]["SizeEstimateRangeGB"]) == 2
    plain = make_table(c, uname("nolsi"))
    assert "ItemCollectionMetrics" not in c.put_item(TableName=plain, Item={"pk": S("a")}, ReturnItemCollectionMetrics="SIZE")


def test_update_table_adds_and_drops_a_gsi_on_a_live_table(env):
    c = env.client
    t = make_table(c, uname("upd"), extra_attrs=[("color", "S")])
    for i in range(60):
        c.put_item(TableName=t, Item={"pk": S(f"k{i}"), "color": S("red" if i % 2 else "blue")})
    c.update_table(TableName=t, AttributeDefinitions=[{"AttributeName": "color", "AttributeType": "S"}],
                   GlobalSecondaryIndexUpdates=[{"Create": {"IndexName": "by-color",
                                                            "KeySchema": [{"AttributeName": "color", "KeyType": "HASH"}],
                                                            "Projection": {"ProjectionType": "ALL"}}}])
    r = c.query(TableName=t, IndexName="by-color", KeyConditionExpression="color = :c",
                ExpressionAttributeValues={":c": S("red")})
    assert r["Count"] == 30, "existing items are visible through the new index"
    d = c.describe_table(TableName=t)["Table"]
    assert d["GlobalSecondaryIndexes"][0]["ItemCount"] == 60
    if env.sharded:  # the index exists on every shard
        for pg in env.pgs:
            assert sql(pg, "SELECT count(*) FROM pg_indexes WHERE tablename = %s AND indexname LIKE 'dxi_%%'",
                       (pg_name(t),))[0][0] == 1
    c.update_table(TableName=t, GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "by-color"}}])
    assert "GlobalSecondaryIndexes" not in c.describe_table(TableName=t)["Table"]
    assert err(c.query, TableName=t, IndexName="by-color", KeyConditionExpression="color = :c",
               ExpressionAttributeValues={":c": S("red")})[0] == "ValidationException"
    assert err(c.update_table, TableName=t)[0] == "ValidationException"
    # billing mode / deletion protection
    c.update_table(TableName=t, BillingMode="PROVISIONED", ProvisionedThroughput={"ReadCapacityUnits": 7, "WriteCapacityUnits": 9})
    d = c.describe_table(TableName=t)["Table"]
    assert d["ProvisionedThroughput"]["ReadCapacityUnits"] == 7 and "BillingModeSummary" not in d
    c.update_table(TableName=t, DeletionProtectionEnabled=True)
    assert err(c.delete_table, TableName=t)[0] == "ValidationException"
    c.update_table(TableName=t, DeletionProtectionEnabled=False)
    c.delete_table(TableName=t)


# ------------------------------------------------------------------------------------ scan

def test_parallel_scan_partitions_the_table(env):
    c = env.client
    t = make_table(c, uname("par"), range_=("sk", "N"))
    for i in range(150):
        c.put_item(TableName=t, Item={"pk": S(f"k{i % 30}"), "sk": N(i)})
    keys = []
    for seg in range(4):
        start = None
        while True:
            kw = {"TableName": t, "Segment": seg, "TotalSegments": 4, "Limit": 20}
            if start:
                kw["ExclusiveStartKey"] = start
            page = c.scan(**kw)
            keys += [(seg, i["pk"]["S"], i["sk"]["N"]) for i in page["Items"]]
            start = page.get("LastEvaluatedKey")
            if not start:
                break
    assert len(keys) == 150 and len({k[1:] for k in keys}) == 150, "every item exactly once"
    assert len({k[0] for k in keys}) == 4
    assert err(c.scan, TableName=t, Segment=0)[0] == "ValidationException"
    assert err(c.scan, TableName=t, Segment=4, TotalSegments=4)[0] == "ValidationException"
    # a numeric sort key scans in (partition, numeric sort key) order
    assert c.scan(TableName=t, Select="COUNT")["Count"] == 150 and "Items" not in c.scan(TableName=t, Select="COUNT")


# ------------------------------------------------------------------------------------ TTL

def test_ttl_expires_items_on_every_shard(env):
    c = env.client
    t = make_table(c, uname("ttl"))
    assert c.describe_time_to_live(TableName=t)["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
    now = int(time.time())
    for i in range(40):
        c.put_item(TableName=t, Item={"pk": S(f"old{i}"), "ttl": N(now - 100), "v": S("x")})
    for i in range(10):
        c.put_item(TableName=t, Item={"pk": S(f"future{i}"), "ttl": N(now + 3600)})
    c.put_item(TableName=t, Item={"pk": S("nottl")})
    c.put_item(TableName=t, Item={"pk": S("wrongtype"), "ttl": S("soon")})
    r = c.update_time_to_live(TableName=t, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"})
    assert r["TimeToLiveSpecification"] == {"Enabled": True, "AttributeName": "ttl"}
    d = c.describe_time_to_live(TableName=t)["TimeToLiveDescription"]
    assert d == {"TimeToLiveStatus": "ENABLED", "AttributeName": "ttl"}
    assert err(c.update_time_to_live, TableName=t, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"})[0] == "ValidationException"
    deadline = time.time() + 20
    while time.time() < deadline and c.scan(TableName=t, Select="COUNT")["Count"] > 12:
        time.sleep(0.5)
    left = sorted(i["pk"]["S"] for i in c.scan(TableName=t)["Items"])
    assert left == sorted([f"future{i}" for i in range(10)] + ["nottl", "wrongtype"]), left
    # a write that is already expired disappears too, and the cache does not resurrect it
    c.put_item(TableName=t, Item={"pk": S("late"), "ttl": N(int(time.time()) - 1)})
    deadline = time.time() + 20
    while time.time() < deadline and "Item" in c.get_item(TableName=t, Key={"pk": S("late")}):
        time.sleep(0.5)
    assert "Item" not in c.get_item(TableName=t, Key={"pk": S("late")})
    c.update_time_to_live(TableName=t, TimeToLiveSpecification={"Enabled": False, "AttributeName": "ttl"})
    assert c.describe_time_to_live(TableName=t)["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"


# ------------------------------------------------------------------------------------ concurrency

def test_conditional_puts_and_updates_are_atomic_under_concurrency(env):
    c = env.client
    t = make_table(c, uname("conc"), range_=("sk", "S"))
    key = {"pk": S("shared"), "sk": S("k")}

    def try_put(_):
        try:
            c.put_item(TableName=t, Item={**key, "who": S(uuid.uuid4().hex)}, ConditionExpression="attribute_not_exists(pk)")
            return 1
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ConditionalCheckFailedException"
            return 0

    with cf.ThreadPoolExecutor(20) as ex:
        assert sum(ex.map(try_put, range(20))) == 1

    def incr(_):
        r = c.update_item(TableName=t, Key={"pk": S("ctr"), "sk": S("k")}, UpdateExpression="SET cnt = if_not_exists(cnt, :z) + :one",
                          ExpressionAttributeValues={":z": N(0), ":one": N(1)}, ReturnValues="ALL_NEW")
        return int(r["Attributes"]["cnt"]["N"])

    with cf.ThreadPoolExecutor(20) as ex:
        seen = sorted(ex.map(incr, range(40)))
    assert seen == list(range(1, 41)), "every increment observed a distinct value"

    # optimistic-locking counter: at most one writer wins each version
    c.put_item(TableName=t, Item={"pk": S("ver"), "sk": S("k"), "v": N(0)})

    def cas(_):
        cur = int(c.get_item(TableName=t, Key={"pk": S("ver"), "sk": S("k")}, ConsistentRead=True)["Item"]["v"]["N"])
        try:
            c.update_item(TableName=t, Key={"pk": S("ver"), "sk": S("k")}, UpdateExpression="SET v = :n",
                          ConditionExpression="v = :c", ExpressionAttributeValues={":n": N(cur + 1), ":c": N(cur)})
            return 1
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ConditionalCheckFailedException"
            return 0

    with cf.ThreadPoolExecutor(20) as ex:
        wins = sum(ex.map(cas, range(20)))
    assert wins == int(c.get_item(TableName=t, Key={"pk": S("ver"), "sk": S("k")})["Item"]["v"]["N"]) and wins >= 1


def _two_partition_keys_on_different_shards(env, table):
    """Find two partition keys stored on different backends (sharded env) by writing until both hosts hold one."""
    where = {}
    for i in range(60):
        k = f"probe{i}"
        env.client.put_item(TableName=table, Item={"pk": S(k), "v": N(0)})
        shard = 0 if sql(env.pgs[0], f"SELECT 1 FROM {pg_name(table)} WHERE pk_value = %s", (k,)) else 1
        where.setdefault(shard, k)
        if len(where) == 2:
            return where[0], where[1]
    raise AssertionError("keys did not spread over both shards")


def test_transactions_are_atomic_and_idempotent_including_across_shards(env):
    c = env.client
    t = make_table(c, uname("tx"))
    a, b = _two_partition_keys_on_different_shards(env, t) if env.sharded else ("probe0", "probe1")
    if not env.sharded:
        for k in (a, b):
            c.put_item(TableName=t, Item={"pk": S(k), "v": N(0)})

    def bump(k, frm, to):
        return {"Update": {"TableName": t, "Key": {"pk": S(k)}, "UpdateExpression": "SET v = :n",
                           "ConditionExpression": "v = :o", "ExpressionAttributeValues": {":n": N(to), ":o": N(frm)}}}

    c.transact_write_items(TransactItems=[bump(a, 0, 1), bump(b, 0, 1)])
    assert c.get_item(TableName=t, Key={"pk": S(a)})["Item"]["v"]["N"] == "1"
    assert c.get_item(TableName=t, Key={"pk": S(b)})["Item"]["v"]["N"] == "1"
    # one failing condition cancels everything, with a per-item reason list and the failed item on request
    with pytest.raises(ClientError) as e:
        c.transact_write_items(TransactItems=[
            bump(a, 1, 2),
            {"Update": {"TableName": t, "Key": {"pk": S(b)}, "UpdateExpression": "SET v = :n", "ConditionExpression": "v = :o",
                        "ExpressionAttributeValues": {":n": N(9), ":o": N(77)}, "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}}])
    resp = e.value.response
    assert resp["Error"]["Code"] == "TransactionCanceledException"
    reasons = resp["CancellationReasons"]
    assert [r["Code"] for r in reasons] == ["None", "ConditionalCheckFailed"] and "Message" not in reasons[0]
    assert reasons[1]["Item"]["v"]["N"] == "1"
    assert c.get_item(TableName=t, Key={"pk": S(a)})["Item"]["v"]["N"] == "1", "the passing item was not applied"
    # idempotency token: same payload replays, a different payload is rejected
    tok = uuid.uuid4().hex
    items = [bump(a, 1, 2), bump(b, 1, 2)]
    c.transact_write_items(TransactItems=items, ClientRequestToken=tok)
    c.transact_write_items(TransactItems=items, ClientRequestToken=tok)  # not applied a second time (would fail its condition)
    assert err(c.transact_write_items, TransactItems=[bump(a, 2, 3)], ClientRequestToken=tok)[0] == "IdempotentParameterMismatchException"
    # duplicate targets and item-count limits
    assert err(c.transact_write_items, TransactItems=[bump(a, 2, 3), bump(a, 2, 4)])[0] == "ValidationException"
    # snapshot read of both
    g = c.transact_get_items(TransactItems=[{"Get": {"TableName": t, "Key": {"pk": S(a)}}}, {"Get": {"TableName": t, "Key": {"pk": S(b)}}}])
    assert [r["Item"]["v"]["N"] for r in g["Responses"]] == ["2", "2"]

    # concurrent overlapping transactions: one winner per version, both items always agree
    def race(_):
        cur = int(c.get_item(TableName=t, Key={"pk": S(a)}, ConsistentRead=True)["Item"]["v"]["N"])
        try:
            c.transact_write_items(TransactItems=[bump(a, cur, cur + 1), bump(b, cur, cur + 1)])
            return 1
        except ClientError as ex:
            assert ex.response["Error"]["Code"] == "TransactionCanceledException"
            return 0

    with cf.ThreadPoolExecutor(20) as ex:
        wins = sum(ex.map(race, range(20)))
    va = int(c.get_item(TableName=t, Key={"pk": S(a)})["Item"]["v"]["N"])
    vb = int(c.get_item(TableName=t, Key={"pk": S(b)})["Item"]["v"]["N"])
    assert va == vb == 2 + wins and wins >= 1


# ------------------------------------------------------------------------------------ expressions & legacy

def test_expressions_types_and_numbers(env):
    c = env.client
    t = make_table(c, uname("expr"))
    c.put_item(TableName=t, Item={"pk": S("a"), "n": N("1.50"), "big": N("12345678901234567890123456789012345678"),
                                  "flag": {"BOOL": True}, "l": {"L": [N(1), S("x")]}, "m": {"M": {"x": {"M": {"y": N(1)}}}},
                                  "ss": {"SS": ["a", "b"]}, "bin": {"B": b"\x00\x01"}})
    it = c.get_item(TableName=t, Key={"pk": S("a")})["Item"]
    assert it["n"] == N("1.5") and it["big"]["N"] == "12345678901234567890123456789012345678"
    assert err(c.put_item, TableName=t, Item={"pk": S("b"), "x": N("123456789012345678901234567890123456789")})[0] == "ValidationException"
    assert err(c.put_item, TableName=t, Item={"pk": S("b"), "x": N("1e126")})[0] == "ValidationException"
    assert err(c.put_item, TableName=t, Item={"pk": S("b"), "x": {"SS": []}})[0] == "ValidationException"
    # BOOL / nested / list index / size / attribute_type in conditions
    def ok(cond, **vals):
        c.update_item(TableName=t, Key={"pk": S("a")}, UpdateExpression="SET touched = :t", ConditionExpression=cond,
                      ExpressionAttributeValues={":t": S("y"), **vals})

    ok("flag = :f AND m.x.y = :one AND l[0] = :one AND size(ss) = :two AND attribute_type(bin, :b)",
       **{":f": {"BOOL": True}, ":one": N(1), ":two": N(2), ":b": S("B")})
    assert err(c.update_item, TableName=t, Key={"pk": S("a")}, UpdateExpression="SET z = :t", ConditionExpression="flag <> :f",
               ExpressionAttributeValues={":t": S("y"), ":f": {"BOOL": True}})[0] == "ConditionalCheckFailedException"
    # UpdateExpression: nested SET / REMOVE / ADD / DELETE, list ops, arithmetic
    r = c.update_item(TableName=t, Key={"pk": S("a")}, ReturnValues="ALL_NEW",
                      UpdateExpression="SET m.x.y = m.x.y + :one, l = list_append(l, :el), n = n - :d ADD ctr :one, ss :sset REMOVE m.x.gone",
                      ExpressionAttributeValues={":one": N(1), ":el": {"L": [S("z")]}, ":d": N("0.25"), ":sset": {"SS": ["c"]}})["Attributes"]
    assert r["m"]["M"]["x"]["M"]["y"] == N(2) and r["l"]["L"] == [N(1), S("x"), S("z")] and r["n"] == N("1.25")
    assert r["ctr"] == N(1) and sorted(r["ss"]["SS"]) == ["a", "b", "c"]
    # UPDATED_OLD / UPDATED_NEW return whole top-level attributes
    r = c.update_item(TableName=t, Key={"pk": S("a")}, ReturnValues="UPDATED_OLD", UpdateExpression="SET m.x.y = :v",
                      ExpressionAttributeValues={":v": N(9)})
    assert list(r["Attributes"]) == ["m"] and r["Attributes"]["m"]["M"]["x"]["M"]["y"] == N(2)
    # validation: unused names/values, reserved words, missing / undefined placeholders
    for kw in (dict(UpdateExpression="SET q = :v", ExpressionAttributeValues={":v": S("a"), ":w": S("b")}),
               dict(UpdateExpression="SET q = :v", ExpressionAttributeValues={":v": S("a")}, ExpressionAttributeNames={"#x": "y"}),
               dict(UpdateExpression="SET name = :v", ExpressionAttributeValues={":v": S("a")}),
               dict(UpdateExpression="SET #n = :v", ExpressionAttributeValues={":v": S("a")}),
               dict(UpdateExpression="SET q = :nope"),
               dict(UpdateExpression="SET q = q + :v", ExpressionAttributeValues={":v": S("a")}),
               dict(UpdateExpression="SET pk = :v", ExpressionAttributeValues={":v": S("a")}),
               dict(UpdateExpression="SET a.b = :v", ExpressionAttributeValues={":v": S("a")}),
               dict(UpdateExpression="SET q = :v AND", ExpressionAttributeValues={":v": S("a")})):
        assert err(c.update_item, TableName=t, Key={"pk": S("a")}, **kw)[0] == "ValidationException", kw
    # key validation
    assert err(c.get_item, TableName=t, Key={"pk": N(1)})[0] == "ValidationException"
    assert err(c.put_item, TableName=t, Item={"pk": S("")})[0] == "ValidationException"
    assert err(c.put_item, TableName=t, Item={"pk": S("k"), "blob": S("x" * (400 * 1024))})[0] == "ValidationException"
    assert err(c.get_item, TableName="ab", Key={"pk": S("a")})[0] == "ValidationException"
    assert err(c.put_item, TableName="does-not-exist-xyz", Item={"pk": S("a")}, ReturnValues="UPDATED_NEW")[0] == "ValidationException"


def test_legacy_parameters(env):
    c = env.client
    t = make_table(c, uname("legacy"), range_=("sk", "S"))
    for i in range(6):
        c.put_item(TableName=t, Item={"pk": S("p"), "sk": S(f"s{i}"), "n": N(i), "name": S(f"name-{i}")})
    r = c.get_item(TableName=t, Key={"pk": S("p"), "sk": S("s1")}, AttributesToGet=["name"])
    assert r["Item"] == {"name": S("name-1")}
    q = c.query(TableName=t, KeyConditions={"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [S("p")]},
                                            "sk": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [S("s")]}},
                QueryFilter={"n": {"ComparisonOperator": "GE", "AttributeValueList": [N(4)]}})
    assert [i["n"]["N"] for i in q["Items"]] == ["4", "5"] and q["ScannedCount"] == 6
    sc = c.scan(TableName=t, ScanFilter={"n": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [N(1), N(3)]},
                                          "name": {"ComparisonOperator": "NOT_NULL"}}, AttributesToGet=["n"])
    assert sorted(i["n"]["N"] for i in sc["Items"]) == ["1", "2", "3"]
    c.update_item(TableName=t, Key={"pk": S("p"), "sk": S("s0")},
                  AttributeUpdates={"n": {"Value": N(100), "Action": "ADD"}, "name": {"Action": "DELETE"},
                                    "fresh": {"Value": S("f"), "Action": "PUT"}},
                  Expected={"n": {"Value": N(0)}})
    it = c.get_item(TableName=t, Key={"pk": S("p"), "sk": S("s0")})["Item"]
    assert it["n"] == N(100) and "name" not in it and it["fresh"] == S("f")
    assert err(c.update_item, TableName=t, Key={"pk": S("p"), "sk": S("s0")}, AttributeUpdates={"n": {"Value": N(1), "Action": "PUT"}},
               Expected={"n": {"Value": N(0)}})[0] == "ConditionalCheckFailedException"
    assert err(c.put_item, TableName=t, Item={"pk": S("p"), "sk": S("s0")}, Expected={"n": {"Exists": False}},
               ConditionalOperator="OR")[0] == "ConditionalCheckFailedException"
    assert err(c.query, TableName=t, KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": S("p")},
               QueryFilter={"n": {"ComparisonOperator": "EQ", "AttributeValueList": [N(1)]}})[0] == "ValidationException"


# ------------------------------------------------------------------------------------ batches, paging, capacity

def test_batches_pagination_and_consumed_capacity(env):
    c = env.client
    t = make_table(c, uname("batch"), range_=("sk", "N"))
    writes = [{"PutRequest": {"Item": {"pk": S(f"k{i % 7}"), "sk": N(i), "d": S("x" * 100)}}} for i in range(25)]
    r = c.batch_write_item(RequestItems={t: writes}, ReturnConsumedCapacity="TOTAL")
    assert r["UnprocessedItems"] == {} and r["ConsumedCapacity"][0]["TableName"] == t
    assert err(c.batch_write_item, RequestItems={t: writes + writes[:1]})[0] == "ValidationException"
    assert err(c.batch_get_item, RequestItems={t: {"Keys": [{"pk": S("k"), "sk": N(i)} for i in range(101)]}})[0] == "ValidationException"
    assert err(c.batch_write_item, RequestItems={t: [writes[0], writes[0]]})[0] == "ValidationException"
    got = c.batch_get_item(RequestItems={t: {"Keys": [{"pk": S(f"k{i % 7}"), "sk": N(i)} for i in range(25)] + [{"pk": S("no"), "sk": N(1)}],
                                             "ProjectionExpression": "sk"}})
    assert len(got["Responses"][t]) == 25 and got["UnprocessedKeys"] == {}
    # 1 MB page limit: 15 items of ~100 KB come back in several pages, none lost or repeated
    big = make_table(c, uname("big"))
    for i in range(15):
        c.put_item(TableName=big, Item={"pk": S(f"b{i:02d}"), "blob": S("z" * 100_000)})
    seen, start, pages = [], None, 0
    while True:
        page = c.scan(TableName=big, **({"ExclusiveStartKey": start} if start else {}))
        seen += [i["pk"]["S"] for i in page["Items"]]
        pages += 1
        start = page.get("LastEvaluatedKey")
        if not start:
            break
    assert pages >= 2 and len(seen) == 15 and len(set(seen)) == 15
    # consumed capacity: reads 0.5 per 4 KB (eventually consistent), writes 1 per KB
    cc = c.get_item(TableName=big, Key={"pk": S("b00")}, ReturnConsumedCapacity="TOTAL")["ConsumedCapacity"]
    assert cc["TableName"] == big and cc["CapacityUnits"] == 12.5
    cc = c.get_item(TableName=big, Key={"pk": S("b00")}, ConsistentRead=True, ReturnConsumedCapacity="INDEXES")["ConsumedCapacity"]
    assert cc["CapacityUnits"] == 25.0 and cc["Table"]["CapacityUnits"] == 25.0
    cc = c.put_item(TableName=big, Item={"pk": S("small")}, ReturnConsumedCapacity="TOTAL")["ConsumedCapacity"]
    assert cc["CapacityUnits"] == 1.0
    assert "ConsumedCapacity" not in c.put_item(TableName=big, Item={"pk": S("small")}, ReturnConsumedCapacity="NONE")
    # ListTables paging
    names = [make_table(c, uname("lt")) for _ in range(3)]
    first = c.list_tables(Limit=2)
    assert len(first["TableNames"]) == 2 and first["LastEvaluatedTableName"] == first["TableNames"][-1]
    allnames, start = [], None
    while True:
        p = c.list_tables(Limit=3, **({"ExclusiveStartTableName": start} if start else {}))
        allnames += p["TableNames"]
        start = p.get("LastEvaluatedTableName")
        if not start:
            break
    assert allnames == sorted(allnames) and set(names) <= set(allnames)
    assert err(env.raw.list_tables, Limit=0)[0] == "ValidationException" and err(env.raw.list_tables, Limit=101)[0] == "ValidationException"


# ------------------------------------------------------------------------------------ PartiQL

def test_partiql_statements_and_paging(env):
    c = env.client
    t = make_table(c, uname("pq"), range_=("sk", "S"), extra_attrs=[("g", "S")], gsis=[gsi("by-g", "g")])
    for i in range(12):
        c.execute_statement(Statement=f"INSERT INTO \"{t}\" VALUE {{'pk': 'p{i % 4}', 'sk': 's{i}', 'g': 'g{i % 2}', 'n': {i}, 'tags': <<'a', 'b'>>}}")
    assert err(c.execute_statement, Statement=f"INSERT INTO \"{t}\" VALUE {{'pk': 'p0', 'sk': 's0'}}")[0] == "DuplicateItemException"
    r = c.execute_statement(Statement=f"SELECT * FROM \"{t}\" WHERE n > ? AND g = ?", Parameters=[N(5), S("g1")])
    assert sorted(int(i["n"]["N"]) for i in r["Items"]) == [7, 9, 11]
    r = c.execute_statement(Statement=f"SELECT sk, n FROM \"{t}\" WHERE pk = 'p1' AND begins_with(sk, 's') ORDER BY sk DESC")
    assert [i["sk"]["S"] for i in r["Items"]] == ["s9", "s5", "s1"] and set(r["Items"][0]) == {"sk", "n"}
    r = c.execute_statement(Statement=f"SELECT * FROM \"{t}\".\"by-g\" WHERE g = 'g0'")
    assert r["Count"] if "Count" in r else len(r["Items"]) == 6
    seen, tok = [], None
    while True:
        p = c.execute_statement(Statement=f"SELECT * FROM \"{t}\"", Limit=5, **({"NextToken": tok} if tok else {}))
        seen += [i["sk"]["S"] for i in p["Items"]]
        tok = p.get("NextToken")
        if not tok:
            break
    assert len(seen) == 12 and len(set(seen)) == 12
    c.execute_statement(Statement=f"UPDATE \"{t}\" SET n = n + 100 SET fresh = 'y' REMOVE tags WHERE pk = 'p0' AND sk = 's0'")
    it = c.get_item(TableName=t, Key={"pk": S("p0"), "sk": S("s0")})["Item"]
    assert it["n"] == N(100) and it["fresh"] == S("y") and "tags" not in it
    assert err(c.execute_statement, Statement=f"UPDATE \"{t}\" SET n = 1 WHERE pk = 'p0' AND sk = 'nope'")[0] == "ConditionalCheckFailedException"
    assert err(c.execute_statement, Statement=f"UPDATE \"{t}\" SET n = 1 WHERE n = 2")[0] == "ValidationException"
    r = c.execute_statement(Statement=f"DELETE FROM \"{t}\" WHERE pk = 'p0' AND sk = 's0' RETURNING ALL OLD *")
    assert r["Items"][0]["fresh"] == S("y")
    c.execute_statement(Statement=f"INSERT INTO \"{t}\" VALUE ?", Parameters=[{"M": {"pk": S("param"), "sk": S("m"), "amount": N("42.50")}}])
    got = c.execute_statement(Statement=f"SELECT * FROM \"{t}\" WHERE pk = ? AND sk = ?", Parameters=[S("param"), S("m")])["Items"][0]
    assert got["amount"] == N("42.5"), "numbers are normalised like in DynamoDB"
    c.execute_statement(Statement=f"UPDATE \"{t}\" SET \"status\" = ? WHERE pk = ? AND sk = ?", Parameters=[S("shipped"), S("param"), S("m")])
    assert c.get_item(TableName=t, Key={"pk": S("param"), "sk": S("m")})["Item"]["status"] == S("shipped")
    b = c.batch_execute_statement(Statements=[
        {"Statement": f"SELECT * FROM \"{t}\" WHERE pk = 'p1' AND sk = 's1'"},
        {"Statement": f"SELECT * FROM \"{t}\" WHERE pk = 'p1'"}])
    assert b["Responses"][0]["Item"]["sk"] == S("s1") and b["Responses"][1]["Error"]["Code"] == "ValidationError"
    tx = c.execute_transaction(TransactStatements=[
        {"Statement": f"INSERT INTO \"{t}\" VALUE {{'pk': 'tx', 'sk': 'a'}}"}, {"Statement": f"INSERT INTO \"{t}\" VALUE {{'pk': 'tx2', 'sk': 'b'}}"}])
    assert tx.get("Responses", []) == []
    code, _, resp = err(c.execute_transaction, TransactStatements=[
        {"Statement": f"INSERT INTO \"{t}\" VALUE {{'pk': 'tx3', 'sk': 'x'}}"}, {"Statement": f"INSERT INTO \"{t}\" VALUE {{'pk': 'tx', 'sk': 'a'}}"}])
    assert code == "TransactionCanceledException" and resp["CancellationReasons"][1]["Code"] == "DuplicateItem"
    assert "Item" not in c.get_item(TableName=t, Key={"pk": S("tx3"), "sk": S("x")})


# ------------------------------------------------------------------------------------ control plane

def test_describe_table_tags_backups_and_limits(env):
    c = env.client
    t = uname("meta")
    desc = c.create_table(TableName=t, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                          AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                          ProvisionedThroughput={"ReadCapacityUnits": 3, "WriteCapacityUnits": 4},
                          Tags=[{"Key": "env", "Value": "test"}])["TableDescription"]
    arn = desc["TableArn"]
    assert arn.endswith(f":table/{t}") and desc["TableStatus"] == "ACTIVE" and desc["ProvisionedThroughput"]["ReadCapacityUnits"] == 3
    d = c.describe_table(TableName=t)["Table"]
    assert d["ItemCount"] == 0 and d["DeletionProtectionEnabled"] is False and d["TableId"] and d["TableClassSummary"]["TableClass"] == "STANDARD"
    assert err(c.create_table, TableName=t, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
               AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")[0] == "ResourceInUseException"
    c.tag_resource(ResourceArn=arn, Tags=[{"Key": "team", "Value": "x"}, {"Key": "env", "Value": "prod"}])
    assert {x["Key"]: x["Value"] for x in c.list_tags_of_resource(ResourceArn=arn)["Tags"]} == {"env": "prod", "team": "x"}
    c.untag_resource(ResourceArn=arn, TagKeys=["team"])
    assert [x["Key"] for x in c.list_tags_of_resource(ResourceArn=arn)["Tags"]] == ["env"]
    assert err(c.tag_resource, ResourceArn="not-an-arn", Tags=[{"Key": "a", "Value": "b"}])[0] == "ValidationException"
    assert err(c.list_tags_of_resource, ResourceArn="arn:aws:dynamodb:us-east-1:000000000000:table/nope-nope")[0] == "AccessDeniedException"
    cb = c.describe_continuous_backups(TableName=t)["ContinuousBackupsDescription"]
    assert cb["PointInTimeRecoveryDescription"] == {"PointInTimeRecoveryStatus": "DISABLED"}
    cb = c.update_continuous_backups(TableName=t, PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True})["ContinuousBackupsDescription"]
    assert cb["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"] == "ENABLED" and cb["PointInTimeRecoveryDescription"]["RecoveryPeriodInDays"] == 35
    assert c.describe_limits()["TableMaxReadCapacityUnits"] > 0
    # unsupported features say so clearly instead of pretending
    assert err(c.create_table, TableName=uname("stream"), KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
               AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST",
               StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"})[0] == "ValidationException"
    c.delete_table(TableName=t)
    assert err(c.describe_table, TableName=t)[0] == "ResourceNotFoundException"


def _raw(env, target, body, prefix="DynamoDB_20120810."):
    r = requests.post(f"http://localhost:{env.warp.frontend_port}/", data=body, timeout=30,
                      headers={"X-Amz-Target": prefix + target, "Content-Type": "application/x-amz-json-1.0"})
    return r.status_code, (r.json() if r.text else {})


def test_unsupported_features_and_malformed_requests_fail_clearly(env):
    for op in ("CreateBackup", "CreateGlobalTable", "RestoreTableToPointInTime", "ExportTableToPointInTime"):
        status, body = _raw(env, op, "{}")
        assert status == 400 and body["__type"].endswith("#UnsupportedOperationException"), (op, body)
    status, body = _raw(env, "ListStreams", "{}", prefix="DynamoDBStreams_20120810.")
    assert status == 400 and body["__type"].endswith("#UnsupportedOperationException") and "Streams" in body["message"]
    status, body = _raw(env, "NoSuchOperation", "{}")
    assert status == 400 and body["__type"].endswith("#UnknownOperationException")
    # a request of the wrong shape is a 400, never a 500
    for target, payload in (("GetItem", '{"TableName": 5, "Key": {}}'), ("PutItem", '{"TableName": "abc", "Item": "x"}'),
                            ("Query", '{"TableName": "abc", "Limit": "many"}'), ("GetItem", "[]"), ("GetItem", "not json")):
        status, body = _raw(env, target, payload)
        assert status == 400, (target, payload, status, body)
    status, body = _raw(env, "GetItem", "{}")
    assert status == 400 and body["__type"].endswith("#ValidationException")


def test_reserved_words_billing_mode_and_table_names_are_enforced_like_dynamodb(env):
    c = env.client
    assert err(env.raw.create_table, TableName="ab", KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
               AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")[0] == "ValidationException"
    assert err(env.raw.create_table, TableName=uname("nb"), KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
               AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}])[0] == "ValidationException"
    assert err(env.raw.create_table, TableName=uname("bad"), KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
               AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "unused", "AttributeType": "S"}],
               BillingMode="PAY_PER_REQUEST")[0] == "ValidationException"
    t = make_table(c, uname("rw"))
    code, msg, _ = err(c.put_item, TableName=t, Item={"pk": S("a")}, ConditionExpression="attribute_not_exists(status)")
    assert code == "ValidationException" and "reserved keyword: status" in msg
    c.put_item(TableName=t, Item={"pk": S("a")}, ConditionExpression="attribute_not_exists(#s)", ExpressionAttributeNames={"#s": "status"})
    a, b = "Case-Sensitive", "case_sensitive"
    make_table(c, a)
    code, msg, _ = err(c.create_table, TableName=b, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                       AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    assert code == "ValidationException" and "collides" in msg


def test_binary_and_number_keys_order_and_range(env):
    c = env.client
    t = make_table(c, uname("keys"), hash_=("h", "N"), range_=("r", "B"))
    for h in (1, 2, 10):
        for r in (b"\x00", b"\x01\x02", b"\xff", b"\x01"):
            c.put_item(TableName=t, Item={"h": N(h), "r": {"B": r}})
    r = c.query(TableName=t, KeyConditionExpression="h = :h AND r >= :r", ExpressionAttributeValues={":h": N("1.0"), ":r": {"B": b"\x01"}})
    assert [i["r"]["B"] for i in r["Items"]] == [b"\x01", b"\x01\x02", b"\xff"]
    r = c.query(TableName=t, KeyConditionExpression="h = :h AND begins_with(r, :p)", ExpressionAttributeValues={":h": N(10), ":p": {"B": b"\x01"}})
    assert len(r["Items"]) == 2
    assert c.scan(TableName=t, Select="COUNT")["Count"] == 12
    # 10 and 1.0 are numbers, not strings: 1.0 finds partition 1
    assert c.get_item(TableName=t, Key={"h": N("1.0"), "r": {"B": b"\xff"}})["Item"]["h"] == N(1)


# ------------------------------------------------------------------------------------ shard group changes

def test_a_backend_added_after_table_creation_gets_the_table_and_its_indexes_on_first_use():
    pgs = [RealPostgres(), RealPostgres()]
    ports = isolated_ports("WARP_DYNAMOWIRE_PORT")
    warp = WarpProcess(pgs[0], "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env={
        **ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})
    try:
        def api(method, path, body=None, expect=200):
            r = requests.request(method, f"http://localhost:{warp.metrics_port}{path}", json=body, timeout=60,
                                 headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
            assert r.status_code == expect, (r.status_code, r.text)
            return r.json()

        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["dynamodb"]})
        c = Env("single", pgs[:1], warp).client
        t = make_table(c, uname("late"), extra_attrs=[("g", "S")], gsis=[gsi("by-g", "g")])
        c.put_item(TableName=t, Item={"pk": S("before"), "g": S("x")})
        assert not sql(pgs[1], "SELECT 1 FROM information_schema.tables WHERE table_name = %s", (pg_name(t),))
        api("POST", "/api/backend-sets/default/backends", {
            "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
            "enabledStores": ["dynamodb"]}, expect=201)
        keys = [f"new{i}" for i in range(40)]
        for k in keys:
            c.put_item(TableName=t, Item={"pk": S(k), "g": S("x")})
        assert sql(pgs[1], "SELECT count(*) FROM information_schema.tables WHERE table_name = %s", (pg_name(t),))[0][0] == 1
        assert sql(pgs[1], "SELECT count(*) FROM pg_indexes WHERE tablename = %s AND indexname LIKE 'dxi_%%'", (pg_name(t),))[0][0] == 1
        on_pg2 = sql(pgs[1], f"SELECT count(*) FROM {pg_name(t)}")[0][0]
        assert 0 < on_pg2 < 40, "new keys spread over both hosts"
        # reads for the new keys find them wherever they live, and the GSI query and scan merge both hosts
        assert all("Item" in c.get_item(TableName=t, Key={"pk": S(k)}) for k in keys)
        q = c.query(TableName=t, IndexName="by-g", KeyConditionExpression="g = :g", ExpressionAttributeValues={":g": S("x")})
        assert {i["pk"]["S"] for i in q["Items"]} >= set(keys)
        assert c.scan(TableName=t, Select="COUNT")["Count"] >= 40
    finally:
        warp.close()
        for pg in pgs:
            pg.close()
