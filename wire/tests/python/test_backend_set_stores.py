"""Protocol frontends hosted on the Postgres backends of a backend set, sharded across them.

One real Warp; two real Postgres containers (`default` and `pg2`) in the SAME backend set with the
stores enabled on both through the admin API. Real clients (boto3, pymongo, HTTP, the neo4j driver)
talk to the frontends; psycopg2 then looks straight into each Postgres to prove where rows landed:
keys spread over both hosts, point operations hit one, reads scatter-gather and lose nothing.
Neo4j is enabled on ONE backend only (graph traversals cannot be sharded).
"""
import json
import os
import time
import uuid

import boto3
import psycopg2
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from neo4j import GraphDatabase
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

SHARDED = ["influxdb", "mongodb", "sqs", "opensearch", "dynamodb"]


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def count(pg, table):
    return sql(pg, f'SELECT count(*) FROM {table}')[0][0]


def has_table(pg, name):
    return bool(sql(pg, "SELECT 1 FROM information_schema.tables WHERE table_name = %s", (name,)))


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def ports():
    return isolated_ports("WARP_DYNAMOWIRE_PORT")


@pytest.fixture(scope="module")
def warp(pgs, ports):
    proc = WarpProcess(pgs[0], "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env={
        **ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})
    # both Postgres backends live in the one `default` set, which every frontend serves by default
    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": SHARDED + ["neo4j"]})
    added = api("POST", "/api/backend-sets/default/backends", {
        "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
        "enabledStores": SHARDED}, expect=201)
    # adding a host is flagged: existing data is NOT rebalanced
    assert {r["store"] for r in added["rebalanceRequired"]} >= set(SHARDED)
    proc.api = api
    yield proc
    proc.close()


# ---------------------------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------------------------

def dynamo(warp, ports):
    return boto3.client("dynamodb", endpoint_url=f"http://localhost:{warp.frontend_port}", region_name="us-east-1",
                        aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))


def test_dynamodb_items_shard_across_both_backends_and_reads_scatter_gather(warp, pgs, ports):
    c = dynamo(warp, ports)
    c.create_table(TableName="orders", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                   AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    # DDL ran on BOTH hosts (an item can land on either)
    assert has_table(pgs[0], "dynamo_item_orders") and has_table(pgs[1], "dynamo_item_orders")
    n = 200
    for i in range(n):
        c.put_item(TableName="orders", Item={"id": {"S": f"order-{i}"}, "total": {"N": str(i)}})
    on0, on1 = count(pgs[0], "dynamo_item_orders"), count(pgs[1], "dynamo_item_orders")
    assert on0 + on1 == n and on0 > 40 and on1 > 40, (on0, on1)
    # point reads route to the shard that holds the key
    for i in (0, 57, 199):
        assert c.get_item(TableName="orders", Key={"id": {"S": f"order-{i}"}})["Item"]["total"]["N"] == str(i)
    # a key lives on exactly one shard
    assert sql(pgs[0], "SELECT count(*) FROM dynamo_item_orders WHERE pk_value='order-57'")[0][0] + \
        sql(pgs[1], "SELECT count(*) FROM dynamo_item_orders WHERE pk_value='order-57'")[0][0] == 1
    # scan scatter-gathers: every item exactly once, also when paginated with Limit across shards
    seen, start, pages = [], None, 0
    while True:
        kw = {"TableName": "orders", "Limit": 30}
        if start:
            kw["ExclusiveStartKey"] = start
        page = c.scan(**kw)
        seen += [i["id"]["S"] for i in page["Items"]]
        pages += 1
        start = page.get("LastEvaluatedKey")
        if not start:
            break
    assert pages >= 7 and len(seen) == n and len(set(seen)) == n
    assert c.scan(TableName="orders", Select="COUNT")["Count"] == n
    c.delete_item(TableName="orders", Key={"id": {"S": "order-57"}})
    assert "Item" not in c.get_item(TableName="orders", Key={"id": {"S": "order-57"}})
    # the catalog lives on the first host only
    assert has_table(pgs[0], "_dynamo_tables")


def test_dynamodb_query_with_sort_key_hits_one_shard(warp, pgs, ports):
    c = dynamo(warp, ports)
    c.create_table(TableName="events", KeySchema=[{"AttributeName": "user", "KeyType": "HASH"},
                                                  {"AttributeName": "ts", "KeyType": "RANGE"}],
                   AttributeDefinitions=[{"AttributeName": "user", "AttributeType": "S"},
                                         {"AttributeName": "ts", "AttributeType": "N"}], BillingMode="PAY_PER_REQUEST")
    for u in range(20):
        for ts in range(5):
            c.put_item(TableName="events", Item={"user": {"S": f"u{u}"}, "ts": {"N": str(ts)}})
    assert count(pgs[0], "dynamo_item_events") + count(pgs[1], "dynamo_item_events") == 100
    assert count(pgs[0], "dynamo_item_events") > 0 and count(pgs[1], "dynamo_item_events") > 0
    r = c.query(TableName="events", KeyConditionExpression="#u = :u AND ts >= :t",
                ExpressionAttributeNames={"#u": "user"},
                ExpressionAttributeValues={":u": {"S": "u3"}, ":t": {"N": "2"}})
    assert [i["ts"]["N"] for i in r["Items"]] == ["2", "3", "4"]
    # one user's whole partition lives on one shard
    assert sorted(sql(pgs[0], "SELECT count(*) FROM dynamo_item_events WHERE pk_value='u3'")[0] +
                  sql(pgs[1], "SELECT count(*) FROM dynamo_item_events WHERE pk_value='u3'")[0]) == [0, 5]


def test_dynamodb_batch_write_spans_shards_and_cross_shard_transactions_are_refused(warp, pgs, ports):
    c = dynamo(warp, ports)
    c.batch_write_item(RequestItems={"orders": [
        {"PutRequest": {"Item": {"id": {"S": f"b{i}"}, "total": {"N": "1"}}}} for i in range(20)]})
    got = [c.get_item(TableName="orders", Key={"id": {"S": f"b{i}"}}).get("Item") for i in range(20)]
    assert all(got)
    # two keys on different shards
    on = {}
    for i in range(20):
        shard = 0 if sql(pgs[0], "SELECT 1 FROM dynamo_item_orders WHERE pk_value=%s", (f"b{i}",)) else 1
        on.setdefault(shard, f"b{i}")
    assert set(on) == {0, 1}
    with pytest.raises(ClientError) as e:
        c.transact_write_items(TransactItems=[
            {"Put": {"TableName": "orders", "Item": {"id": {"S": on[0]}, "total": {"N": "9"}}}},
            {"Put": {"TableName": "orders", "Item": {"id": {"S": on[1]}, "total": {"N": "9"}}}}])
    assert "spans 2 storage shards" in str(e.value)
    assert c.get_item(TableName="orders", Key={"id": {"S": on[0]}})["Item"]["total"]["N"] == "1", "nothing may be written"
    assert c.get_item(TableName="orders", Key={"id": {"S": on[1]}})["Item"]["total"]["N"] == "1"
    # a transaction whose items share one partition key is one shard and works
    c.transact_write_items(TransactItems=[
        {"Put": {"TableName": "orders", "Item": {"id": {"S": on[0]}, "total": {"N": "5"}}}},
        {"Update": {"TableName": "orders", "Key": {"id": {"S": on[0]}}, "UpdateExpression": "SET total = :v",
                    "ExpressionAttributeValues": {":v": {"N": "6"}}}}])
    assert c.get_item(TableName="orders", Key={"id": {"S": on[0]}})["Item"]["total"]["N"] == "6"


# ---------------------------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------------------------

def sqs(warp, ports):
    return boto3.client("sqs", endpoint_url=f"http://localhost:{ports['WARP_SQSWIRE_PORT']}", region_name="us-east-1",
                        aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))


def test_sqs_each_queue_lives_wholly_on_one_shard_and_queues_distribute(warp, pgs, ports):
    c = sqs(warp, ports)
    urls = {}
    for i in range(24):
        urls[f"q{i}"] = c.create_queue(QueueName=f"q{i}")["QueueUrl"]
    homes = {}
    for name in urls:
        table = f"sqs_queue_{name}"
        where = [k for k, pg in enumerate(pgs) if has_table(pg, table)]
        assert len(where) == 1, (name, where)
        homes[name] = where[0]
    assert set(homes.values()) == {0, 1}, "queues must spread over both backends"
    for name, url in urls.items():
        c.send_message(QueueUrl=url, MessageBody=f"hello {name}")
    for name, url in urls.items():
        msgs = c.receive_message(QueueUrl=url, MaxNumberOfMessages=1).get("Messages", [])
        assert [m["Body"] for m in msgs] == [f"hello {name}"], name
    # the catalog is on the first host, and ListQueues sees every queue whichever shard holds it
    assert has_table(pgs[0], "sqs_queues_catalog")
    listed = set(c.list_queues(MaxResults=1000).get("QueueUrls", []))
    assert set(urls.values()) <= listed


def test_sqs_fifo_ordering_is_preserved_on_a_sharded_set(warp, pgs, ports):
    c = sqs(warp, ports)
    url = c.create_queue(QueueName="ordered.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    for i in range(10):
        c.send_message(QueueUrl=url, MessageBody=f"m{i}", MessageGroupId="g", MessageDeduplicationId=f"d{i}")
    got = []
    for _ in range(10):
        msgs = c.receive_message(QueueUrl=url, MaxNumberOfMessages=1).get("Messages", [])
        assert len(msgs) == 1
        got.append(msgs[0]["Body"])
        c.delete_message(QueueUrl=url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    assert got == [f"m{i}" for i in range(10)]
    assert sum(has_table(pg, "sqs_queue_ordered_fifo") for pg in pgs) == 1


# ---------------------------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------------------------

def mongo(warp, ports):
    return MongoClient(host="localhost", port=int(ports["WARP_MONGOWIRE_PORT"]), serverSelectionTimeoutMS=8000)


def test_mongodb_documents_shard_by_id_and_finds_scatter_gather(warp, pgs, ports):
    coll = mongo(warp, ports)["shopdb"]["people"]
    coll.insert_many([{"_id": f"p{i}", "n": i, "grp": i % 4} for i in range(120)])
    on0, on1 = count(pgs[0], 'shopdb.people'), count(pgs[1], 'shopdb.people')
    assert on0 + on1 == 120 and on0 > 30 and on1 > 30, (on0, on1)
    assert coll.count_documents({}) == 120
    assert len(list(coll.find({}))) == 120
    assert coll.count_documents({"grp": 1}) == 30
    assert coll.find_one({"_id": "p77"})["n"] == 77
    # updateOne with a non-_id filter must touch exactly ONE document even though both shards match
    r = coll.update_one({"grp": 2}, {"$set": {"touched": True}})
    assert r.modified_count == 1 and coll.count_documents({"touched": True}) == 1
    assert coll.delete_one({"grp": 3}).deleted_count == 1 and coll.count_documents({}) == 119
    assert coll.update_many({"grp": 1}, {"$set": {"x": 1}}).modified_count == 30
    assert sorted(coll.distinct("grp")) == [0, 1, 2, 3]
    # $group merges exactly across the shards ($sum/$min/$max; $avg via its sum and count)
    rows = list(coll.aggregate([{"$group": {"_id": "$grp", "c": {"$sum": 1}, "total": {"$sum": "$n"},
                                            "avg": {"$avg": "$n"}}}, {"$sort": {"_id": 1}}]))
    docs = list(coll.find({}))
    assert [r["_id"] for r in rows] == [0, 1, 2, 3]
    for r in rows:
        mine = [d["n"] for d in docs if d["grp"] == r["_id"]]
        assert r["c"] == len(mine) and r["total"] == sum(mine) and abs(r["avg"] - sum(mine) / len(mine)) < 1e-9, r
    top = list(coll.aggregate([{"$group": {"_id": "$grp", "c": {"$sum": 1}}}, {"$sort": {"c": -1, "_id": 1}},
                               {"$limit": 2}]))
    assert len(top) == 2 and top[0]["c"] >= top[1]["c"]
    # what cannot be merged exactly is refused, not silently wrong
    with pytest.raises(OperationFailure) as e:
        list(coll.aggregate([{"$sort": {"n": -1}}, {"$limit": 3}]))
    assert "several backends" in str(e.value)


# ---------------------------------------------------------------------------------------------
# InfluxDB
# ---------------------------------------------------------------------------------------------

def influx_query(ports, q):
    r = requests.get(f"http://localhost:{ports['WARP_INFLUXWIRE_PORT']}/query", params={"db": "db0", "q": q}, timeout=20)
    r.raise_for_status()
    return r.json()["results"][0].get("series", [])


def test_influx_series_shard_and_queries_merge_exactly(warp, pgs, ports):
    lines = "\n".join(f"cpu,host=h{i % 40} value={i} {1700000000000000000 + i * 1_000_000_000}" for i in range(200))
    requests.post(f"http://localhost:{ports['WARP_INFLUXWIRE_PORT']}/write", params={"db": "db0"}, data=lines,
                  timeout=20).raise_for_status()
    on0, on1 = count(pgs[0], "warp_influx_cpu"), count(pgs[1], "warp_influx_cpu")
    assert on0 + on1 == 200 and on0 > 40 and on1 > 40, (on0, on1)
    # one series (host) lives on exactly one shard
    assert sorted([sql(pgs[0], "SELECT count(*) FROM warp_influx_cpu WHERE tags->>'host'='h7'")[0][0],
                   sql(pgs[1], "SELECT count(*) FROM warp_influx_cpu WHERE tags->>'host'='h7'")[0][0]]) == [0, 5]
    s = influx_query(ports, "SELECT count(value), sum(value), min(value), max(value), mean(value) FROM cpu")
    row = s[0]["values"][0]
    assert row[-5:] == [200, sum(range(200)), 0, 199, sum(range(200)) / 200], row
    # newest-first plain read with a limit merges across shards
    s = influx_query(ports, "SELECT value FROM cpu LIMIT 5")
    assert [r[-1] for r in s[0]["values"]] == [199, 198, 197, 196, 195]
    s = influx_query(ports, "SELECT count(value) FROM cpu WHERE host = 'h7'")
    assert s[0]["values"][0][-1] == 5
    s = influx_query(ports, "SELECT mean(value) FROM cpu GROUP BY time(50s)")
    assert [r[-1] for r in s[0]["values"]] == [24.5, 74.5, 124.5, 174.5], s
    names = [v[0] for v in influx_query(ports, "SHOW MEASUREMENTS")[0]["values"]]
    assert "cpu" in names


# ---------------------------------------------------------------------------------------------
# OpenSearch
# ---------------------------------------------------------------------------------------------

def os_url(ports, path):
    return f"http://localhost:{ports['WARP_OSWIRE_PORT']}{path}"


def test_opensearch_documents_shard_and_search_merges(warp, pgs, ports):
    for i in range(100):
        requests.put(os_url(ports, f"/books/_doc/b{i}"), json={"title": f"book {i}", "rank": i, "genre": "g%d" % (i % 3)},
                     timeout=20).raise_for_status()
    on0, on1 = count(pgs[0], "warp_search_books"), count(pgs[1], "warp_search_books")
    assert on0 + on1 == 100 and on0 > 25 and on1 > 25, (on0, on1)
    assert requests.get(os_url(ports, "/books/_doc/b42"), timeout=20).json()["_source"]["rank"] == 42
    r = requests.post(os_url(ports, "/books/_search"), timeout=20,
                      json={"query": {"match_all": {}}, "size": 100, "sort": [{"rank": "asc"}]}).json()
    ranks = [h["_source"]["rank"] for h in r["hits"]["hits"]]
    assert ranks == list(range(100)) and r["hits"]["total"]["value"] == 100
    r = requests.post(os_url(ports, "/books/_search"), timeout=20,
                      json={"query": {"term": {"genre": "g1"}}, "size": 5, "from": 10, "sort": [{"rank": "asc"}]}).json()
    assert [h["_source"]["rank"] for h in r["hits"]["hits"]] == [31, 34, 37, 40, 43]
    assert requests.delete(os_url(ports, "/books/_doc/b42"), timeout=20).status_code == 200
    assert requests.get(os_url(ports, "/books/_doc/b42"), timeout=20).json()["found"] is False
    assert count(pgs[0], "warp_search_books") + count(pgs[1], "warp_search_books") == 99


# ---------------------------------------------------------------------------------------------
# Neo4j: one host per set
# ---------------------------------------------------------------------------------------------

def test_neo4j_graph_lives_on_the_single_enabled_backend(warp, pgs, ports):
    drv = GraphDatabase.driver(f"bolt://localhost:{ports['WARP_BOLTWIRE_PORT']}", auth=("postgres", "postgres"))
    with drv.session() as s:
        s.run("CREATE (n:Person {name: 'Ada'})").consume()
        rows = list(s.run("MATCH (n:Person) RETURN n.name"))
    drv.close()
    assert [r[0] for r in rows] == ["Ada"]
    assert count(pgs[0], "warp_graph_nodes") == 1
    assert not has_table(pgs[1], "warp_graph_nodes"), "the graph is not spread over a second backend"
    # and asking for a second host is refused with the reason
    r = requests.patch(f"http://localhost:{warp.metrics_port}/api/backend-sets/default/backends/pg2", timeout=20,
                       headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, json={"enabledStores": SHARDED + ["neo4j"]})
    assert r.status_code == 400 and "ONE backend per backend set" in r.json()["error"]
