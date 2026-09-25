"""MCP view of stores enabled through backend sets: no WARP_MCP_EMULATED_STORES env involved.

Two real Postgres containers in one backend set with dynamodb/mongodb/influxdb/sqs/opensearch
enabled on both (Neo4j on `default` only). list_backends / describe_backend must show the
enabledStores, list each hosted store as a typed store `<backend>.<kind>` and the tools behind it
must be the sharded frontends' logic (rows really land on both Postgres). Real clients again read
the same data back through the wire protocols.
"""
import json
import os

import boto3
import psycopg2
import pytest
import requests
from botocore.config import Config
from pymongo import MongoClient

from mcp_support import ADMIN_TOKEN, call, call_json
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

SHARDED = ["influxdb", "mongodb", "sqs", "opensearch", "dynamodb"]


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def count(pg, table):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def ports():
    return isolated_ports("WARP_MCP_PORT")


@pytest.fixture(scope="module")
def warp(pgs, ports):
    env = {**ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost"}
    env.pop("WARP_MCP_EMULATED_STORES", None)
    proc = WarpProcess(pgs[0], "WARP_MCP_PORT", frontend_name="mcp", extra_env=env)

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": SHARDED + ["neo4j"],
                                                                 "description": "gateway postgres"})
    api("POST", "/api/backend-sets/default/backends", {"name": "pg2", "url": pg_url(pgs[1]), "user": "postgres",
                                                        "password": "postgres", "enabledStores": SHARDED}, expect=201)
    yield proc
    proc.close()


def test_list_backends_shows_enabled_stores_as_typed_stores_of_their_hosts(warp):
    out = call_json(warp.frontend_port, "list_backends")
    by = {b["name"]: b for b in out["backends"]}
    assert {"default", "pg2"} <= set(by)
    assert by["default"]["enabledStores"] == SHARDED + ["neo4j"] and by["pg2"]["enabledStores"] == SHARDED
    assert by["pg2"]["backendSet"] == "default" and by["default"]["description"] == "gateway postgres"
    for host in ("default", "pg2"):
        for kind in ("dynamodb", "mongodb", "influx", "sqs", "opensearch"):
            b = by[f"{host}.{kind}"]
            assert b["engine"] == "warp-emulated" and b["host"] == host and b["type"] == kind
            assert b["sharded"] is True and b["hostedOn"] == ["default", "pg2"]
    assert by["default.neo4j"]["sharded"] is False and "pg2.neo4j" not in by
    assert "put_item" in by["pg2.dynamodb"]["tools"] and "find" in by["pg2.mongodb"]["tools"]
    assert by["default.sqs"]["tools"] == []      # described only: no MCP data tools for SQS yet


def test_dynamodb_tools_route_to_the_sharded_store(warp, pgs, ports):
    p = warp.frontend_port
    call(p, "create_table", {"tableName": "mcp_items", "partitionKey": "id", "partitionKeyType": "S",
                             "backend": "pg2.dynamodb"})
    for i in range(60):
        call(p, "put_item", {"tableName": "mcp_items", "item": {"id": f"k{i}", "n": i}, "backend": "pg2.dynamodb"})
    a, b = count(pgs[0], "dynamo_item_mcp_items"), count(pgs[1], "dynamo_item_mcp_items")
    assert a + b == 60 and a > 10 and b > 10, (a, b)
    # the same table read back with a real boto3 client through the dynamowire frontend
    ddb = boto3.client("dynamodb", endpoint_url=f"http://localhost:{ports['WARP_DYNAMOWIRE_PORT']}",
                       region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t",
                       config=Config(retries={"max_attempts": 0}))
    assert ddb.scan(TableName="mcp_items", Select="COUNT")["Count"] == 60
    d = call_json(p, "describe_backend", {"backend": "default.dynamodb"})
    assert d["sharded"] is True and d["hostedOn"] == ["default", "pg2"]
    t = {x["name"]: x for x in d["contents"]["tables"]}["mcp_items"]
    assert t["name"] == "mcp_items"
    scanned = call_json(p, "scan_table", {"tableName": "mcp_items", "backend": "default.dynamodb"})
    assert scanned


def test_mongodb_and_influx_tools_route_to_the_sharded_stores(warp, pgs, ports):
    p = warp.frontend_port
    docs = [{"_id": f"d{i}", "grp": i % 3} for i in range(60)]
    call(p, "insert-many", {"database": "mcpdb", "collection": "things", "documents": docs, "backend": "pg2.mongodb"})
    a, b = count(pgs[0], "mcpdb.things"), count(pgs[1], "mcpdb.things")
    assert a + b == 60 and a > 10 and b > 10, (a, b)
    mc = MongoClient(host="localhost", port=int(ports["WARP_MONGOWIRE_PORT"]))
    assert mc["mcpdb"]["things"].count_documents({}) == 60
    lines = "\n".join(f"mcpm,host=h{i % 20} v={i} {1700000000000000000 + i * 1_000_000_000}" for i in range(100))
    call(p, "write_line_protocol", {"db": "x", "data": lines, "backend": "default.influx"})
    a, b = count(pgs[0], "warp_influx_mcpm"), count(pgs[1], "warp_influx_mcpm")
    assert a + b == 100 and a > 10 and b > 10, (a, b)
    res = call_json(p, "query_influxql", {"db": "x", "q": "SELECT count(v), sum(v) FROM mcpm", "backend": "pg2.influx"})
    row = res["results"][0]["series"][0]["values"][0]
    assert row[-2:] == [100, sum(range(100))]
    # SQL tools act on the addressed host's own shard
    r = call_json(p, "query_sql", {"db": "x", "q": "SELECT count(*) AS n FROM warp_influx_mcpm", "backend": "pg2.influx"})
    assert r["rows"][0]["n"] == count(pgs[1], "warp_influx_mcpm")


def test_describe_backend_lists_sqs_opensearch_and_neo4j_contents(warp, pgs, ports):
    p = warp.frontend_port
    sqs = boto3.client("sqs", endpoint_url=f"http://localhost:{ports['WARP_SQSWIRE_PORT']}", region_name="us-east-1",
                       aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))
    for i in range(30):
        sqs.create_queue(QueueName=f"mq{i}")
    for i in range(20):
        requests.put(f"http://localhost:{ports['WARP_OSWIRE_PORT']}/mcpidx/_doc/{i}", json={"n": i}, timeout=20)
    d = call_json(p, "describe_backend", {"backend": "pg2.sqs"})
    names = {q["name"]: q["shard"] for q in d["contents"]["queues"]}
    assert set(names) == {f"mq{i}" for i in range(30)} and set(names.values()) == {"default", "pg2"}
    d = call_json(p, "describe_backend", {"backend": "default.opensearch"})
    assert {i["index"]: i["documents"] for i in d["contents"]["indexes"]}["mcpidx"] == 20
    d = call_json(p, "describe_backend", {"backend": "default.neo4j"})
    assert d["contents"]["graphHost"] == "default" and d["contents"]["nodeCount"] == 0
    real = call_json(p, "describe_backend", {"backend": "pg2"})
    assert real["enabledStores"] == SHARDED


def test_the_env_fallback_still_lists_default_stores_when_nothing_is_enabled(pgs):
    # a Warp with NO enabled stores and WARP_MCP_EMULATED_STORES=all: same `default.*` entries as before
    cfg = RealPostgres()
    try:
        env = {**isolated_ports("WARP_MCP_PORT"), "WARP_MCP_EMULATED_STORES": "all"}
        proc = WarpProcess(cfg, "WARP_MCP_PORT", frontend_name="mcp-env", extra_env=env)
        try:
            out = call_json(proc.frontend_port, "list_backends")
            names = {b["name"] for b in out["backends"]}
            assert {"default", "default.dynamodb", "default.mongodb", "default.influx"} <= names
            assert not any(n.endswith(".sqs") for n in names)
            assert next(b for b in out["backends"] if b["name"] == "default")["enabledStores"] == []
        finally:
            proc.close()
    finally:
        cfg.close()
