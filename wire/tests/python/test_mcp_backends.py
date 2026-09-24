"""End-to-end proof that Warp's MCP endpoint speaks each backend KIND's own tool vocabulary
(DynamoDB / InfluxDB / MongoDB) over the Warp-emulated stores, while the relational tool list is
unchanged. Real Warp subprocess (the shaded jar), real Postgres container, real clients (boto3,
InfluxDB line protocol over HTTP, pymongo) and a plain JSON-RPC-over-HTTP MCP client -- no mocks.

One Warp process serves all four kinds: WARP_MCP_KIND=relational,dynamodb,influx,mongodb (the
multi-kind form: non-relational tools are "<kind>_"-prefixed) and each kind's bare, official
vocabulary is reached through the "/kinds/<kind>" request path on the same listener.
"""
import json
import os

import boto3
import pytest
import requests
from botocore.config import Config
from pymongo import MongoClient

from warp_test_support import WarpProcess, RealPostgres, free_port

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

RELATIONAL_TOOLS = {"execute_sql", "list_tables", "describe_table", "run_sql", "inspect_schema",
                    "column_stats", "compare_groups", "correlation", "sample_rows", "find_outliers",
                    "find_join_path", "explain_sql"}


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def ports():
    return {"dynamo": free_port(), "influx": free_port(), "mongo": free_port()}


@pytest.fixture(scope="module")
def warp(postgres, ports):
    proc = WarpProcess(postgres, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_MCP_KIND": "relational,dynamodb,influx,mongodb",
        "WARP_DYNAMOWIRE_PORT": ports["dynamo"],
        "WARP_INFLUXWIRE_PORT": ports["influx"],
        "WARP_MONGOWIRE_PORT": ports["mongo"],
    })
    yield proc
    proc.close()


_ids = iter(range(1, 10_000))


def rpc(warp, method, params=None, path="/"):
    resp = requests.post(f"http://localhost:{warp.frontend_port}{path}", timeout=20, json={
        "jsonrpc": "2.0", "id": next(_ids), "method": method, "params": params or {}})
    resp.raise_for_status()
    return resp.json()


def tool_names(warp, path="/"):
    return {t["name"] for t in rpc(warp, "tools/list", path=path)["result"]["tools"]}


def call(warp, name, args=None, path="/", expect_error=False):
    body = rpc(warp, "tools/call", {"name": name, "arguments": args or {}}, path=path)
    assert "error" not in body, body
    result = body["result"]
    assert result["isError"] is expect_error, result
    return [c["text"] for c in result["content"]]


def call_json(warp, name, args=None, path="/"):
    return json.loads(call(warp, name, args, path)[-1])


def dynamo_client(ports):
    return boto3.client("dynamodb", endpoint_url=f"http://localhost:{ports['dynamo']}",
                        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(retries={"max_attempts": 0}))


# ---------------------------------------------------------------------------
# tools/list per kind
# ---------------------------------------------------------------------------

def test_relational_tool_list_unchanged(warp):
    names = tool_names(warp, "/kinds/relational")
    assert RELATIONAL_TOOLS <= names
    assert {"query_federated", "query_natural_language", "explain_query", "document_schema"} <= names
    assert not any(n.startswith(("dynamodb_", "influx_", "mongodb_")) for n in names)
    assert "query_table" not in names and "find" not in names


def test_multi_kind_default_list_is_prefixed(warp):
    names = tool_names(warp)
    assert RELATIONAL_TOOLS <= names                       # relational keeps bare names
    assert {"dynamodb_list_tables", "dynamodb_query_table", "influx_query_influxql",
            "mongodb_find", "mongodb_insert-many"} <= names
    assert "list_tables" in names and "find" not in names  # bare list_tables is still the SQL one


def test_single_kind_views_use_official_vocabulary(warp):
    dyn = tool_names(warp, "/kinds/dynamodb")
    assert dyn == {"list_tables", "describe_table", "create_table", "put_item", "get_item",
                   "update_item", "delete_item", "query_table", "scan_table"}
    influx = tool_names(warp, "/kinds/influx")
    assert influx == {"health_check", "list_databases", "get_measurements", "list_tables",
                      "get_measurement_schema", "describe_table", "query_sql", "query_influxql",
                      "write_line_protocol"}
    mongo = tool_names(warp, "/kinds/mongodb")
    assert mongo == {"list-databases", "list-collections", "find", "aggregate", "count",
                     "collection-schema", "insert-many", "update-many", "delete-many"}
    # a kind that is not enabled has no view
    body = rpc(warp, "tools/list", path="/kinds/nosuch")
    assert "error" in body


def test_kind_view_refuses_other_kinds_tools(warp):
    body = rpc(warp, "tools/call", {"name": "execute_sql", "arguments": {"sql": "select 1"}},
               path="/kinds/dynamodb")
    assert "error" in body and "Unknown tool" in body["error"]["message"]


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

def test_dynamodb_round_trip(warp, ports):
    ddb = dynamo_client(ports)
    ddb.create_table(TableName="mcp_orders",
                     AttributeDefinitions=[{"AttributeName": "customer", "AttributeType": "S"},
                                           {"AttributeName": "order_no", "AttributeType": "N"}],
                     KeySchema=[{"AttributeName": "customer", "KeyType": "HASH"},
                                {"AttributeName": "order_no", "KeyType": "RANGE"}],
                     BillingMode="PAY_PER_REQUEST")
    ddb.put_item(TableName="mcp_orders", Item={"customer": {"S": "acme"}, "order_no": {"N": "1"},
                                              "total": {"N": "10"}})
    ddb.put_item(TableName="mcp_orders", Item={"customer": {"S": "acme"}, "order_no": {"N": "2"},
                                              "total": {"N": "25"}})
    p = "/kinds/dynamodb"

    assert "mcp_orders" in call_json(warp, "list_tables", path=p)["TableNames"]
    desc = call_json(warp, "describe_table", {"tableName": "mcp_orders"}, path=p)["Table"]
    assert {(k["AttributeName"], k["KeyType"]) for k in desc["KeySchema"]} == {
        ("customer", "HASH"), ("order_no", "RANGE")}

    got = call_json(warp, "get_item", {"tableName": "mcp_orders",
                                       "key": {"customer": {"S": "acme"}, "order_no": {"N": "2"}}}, path=p)
    assert got["Item"]["total"] == {"N": "25"}
    q = call_json(warp, "query_table", {
        "tableName": "mcp_orders", "keyConditionExpression": "customer = :c AND order_no > :n",
        "expressionAttributeValues": {":c": {"S": "acme"}, ":n": {"N": "1"}}}, path=p)
    assert [i["order_no"]["N"] for i in q["Items"]] == ["2"]
    scan = call_json(warp, "scan_table", {"tableName": "mcp_orders", "filterExpression": "total >= :t",
                                          "expressionAttributeValues": {":t": {"N": "10"}}}, path=p)
    assert len(scan["Items"]) == 2

    # write through MCP (plain JSON accepted), read back through boto3
    call(warp, "put_item", {"tableName": "mcp_orders",
                            "item": {"customer": "beta", "order_no": 7, "total": 99, "vip": True}}, path=p)
    item = ddb.get_item(TableName="mcp_orders", Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}})["Item"]
    assert item["total"] == {"N": "99"} and item["vip"] == {"BOOL": True}
    call(warp, "update_item", {"tableName": "mcp_orders",
                               "key": {"customer": "beta", "order_no": 7},
                               "updateExpression": "SET total = :t",
                               "expressionAttributeValues": {":t": 100}}, path=p)
    assert ddb.get_item(TableName="mcp_orders", Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}}
                        )["Item"]["total"] == {"N": "100"}
    call(warp, "delete_item", {"tableName": "mcp_orders", "key": {"customer": "beta", "order_no": 7}}, path=p)
    assert "Item" not in ddb.get_item(TableName="mcp_orders",
                                      Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}})

    # create a table through MCP, see it through boto3; honest errors for the unsupported ones
    call(warp, "create_table", {"tableName": "mcp_made", "partitionKey": "id", "partitionKeyType": "S"}, path=p)
    assert "mcp_made" in ddb.list_tables()["TableNames"]
    err = call(warp, "get_item", {"tableName": "no_such_table", "key": {"id": "x"}}, path=p, expect_error=True)
    assert "ResourceNotFoundException" in err[0] or "not found" in err[0].lower()
    err = call(warp, "create_gsi", {"tableName": "mcp_orders"}, path=p, expect_error=True)
    assert "UnsupportedOperation" in err[0]


def test_dynamodb_prefixed_in_multi_kind_endpoint(warp, ports):
    assert "mcp_orders" in call_json(warp, "dynamodb_list_tables")["TableNames"]


# ---------------------------------------------------------------------------
# InfluxDB
# ---------------------------------------------------------------------------

def test_influx_round_trip(warp, ports):
    lp = ("mcp_temp,host=alpha,room=lab value=21.5,ok=true 1700000000000000000\n"
          "mcp_temp,host=beta,room=lab value=19.0,ok=false 1700000060000000000\n")
    requests.post(f"http://localhost:{ports['influx']}/write", params={"db": "d"}, data=lp,
                  timeout=10).raise_for_status()
    p = "/kinds/influx"
    assert call_json(warp, "list_databases", path=p)["databases"]
    assert "mcp_temp" in call_json(warp, "get_measurements", {"db": "d"}, path=p)["measurements"]
    assert "mcp_temp" in call_json(warp, "list_tables", {"db": "d"}, path=p)["tables"]

    schema = call_json(warp, "get_measurement_schema", {"db": "d", "measurement": "mcp_temp"}, path=p)
    cols = {(c["name"], c["category"]) for c in schema["columns"]}
    assert {("time", "time"), ("host", "tag"), ("room", "tag"), ("value", "field"), ("ok", "field")} <= cols
    assert call_json(warp, "describe_table", {"db": "d", "table": "mcp_temp"}, path=p)["columns"]

    ql = call_json(warp, "query_influxql", {"db": "d", "q": "SELECT value FROM mcp_temp WHERE host = 'alpha'"},
                   path=p)
    assert ql["results"][0]["series"][0]["values"][0][1] == 21.5

    sql = call_json(warp, "query_sql", {
        "db": "d", "q": "SELECT tags->>'host' AS host, (fields->>'value')::float AS value "
                        "FROM warp_influx_mcp_temp ORDER BY 2"}, path=p)
    assert [(r["host"], r["value"]) for r in sql["rows"]] == [("beta", 19.0), ("alpha", 21.5)]
    assert sql["truncated"] is False

    # write through MCP, read back via the native InfluxQL endpoint
    out = call_json(warp, "write_line_protocol", {"db": "d", "data": "mcp_temp,host=gamma,room=lab value=30 "
                                                                      "1700000120000000000"}, path=p)
    assert out["points_written"] == 1
    native = requests.get(f"http://localhost:{ports['influx']}/query",
                          params={"db": "d", "q": "SELECT value FROM mcp_temp WHERE host = 'gamma'"},
                          timeout=10).json()
    assert native["results"][0]["series"][0]["values"][0][1] == 30

    # honesty: writes and foreign tables are refused
    call(warp, "query_sql", {"db": "d", "q": "DELETE FROM warp_influx_mcp_temp"}, path=p, expect_error=True)
    call(warp, "query_sql", {"db": "d", "q": "SELECT * FROM pg_class"}, path=p, expect_error=True)
    call(warp, "get_measurement_schema", {"db": "d", "measurement": "nope"}, path=p, expect_error=True)


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def test_mongodb_round_trip(warp, ports):
    mongo = MongoClient(host="localhost", port=ports["mongo"], serverSelectionTimeoutMS=5000)
    coll = mongo["mcpdb"]["people"]
    coll.insert_many([{"_id": "a", "name": "Ada", "age": 36, "addr": {"city": "London"}},
                      {"_id": "b", "name": "Bob", "age": 25},
                      {"_id": "c", "name": "Cy", "age": 41}])
    p = "/kinds/mongodb"

    assert "mcpdb" in [d["name"] for d in call_json(warp, "list-databases", path=p)]
    assert "people" in [c["name"] for c in call_json(warp, "list-collections", {"database": "mcpdb"}, path=p)]

    found = call_json(warp, "find", {"database": "mcpdb", "collection": "people",
                                     "filter": {"age": {"$gt": 30}}, "sort": {"age": -1}}, path=p)
    assert [d["name"] for d in found] == ["Cy", "Ada"]
    proj = call_json(warp, "find", {"database": "mcpdb", "collection": "people", "filter": {"_id": "a"},
                                    "projection": {"name": 1, "_id": 0}}, path=p)
    assert proj == [{"name": "Ada"}]
    limited = call_json(warp, "find", {"database": "mcpdb", "collection": "people", "limit": 1}, path=p)
    assert len(limited) == 1
    assert "Found 3" in call(warp, "count", {"database": "mcpdb", "collection": "people"}, path=p)[0]
    agg = call_json(warp, "aggregate", {"database": "mcpdb", "collection": "people",
                                        "pipeline": [{"$match": {"age": {"$gt": 30}}}, {"$sort": {"age": 1}}]},
                    path=p)
    assert [d["name"] for d in agg] == ["Ada", "Cy"]
    schema = json.loads(call(warp, "collection-schema", {"database": "mcpdb", "collection": "people"}, path=p)[-1])
    assert schema["fields"]["age"]["types"] == ["number"]
    assert "addr.city" in schema["fields"]

    # write through MCP, read back through pymongo
    call(warp, "insert-many", {"database": "mcpdb", "collection": "people",
                               "documents": [{"_id": "d", "name": "Di", "age": 30}]}, path=p)
    assert coll.find_one({"_id": "d"})["name"] == "Di"
    call(warp, "update-many", {"database": "mcpdb", "collection": "people", "filter": {"_id": "d"},
                               "update": {"$set": {"age": 31}}}, path=p)
    assert coll.find_one({"_id": "d"})["age"] == 31
    call(warp, "delete-many", {"database": "mcpdb", "collection": "people", "filter": {"_id": "d"}}, path=p)
    assert coll.find_one({"_id": "d"}) is None

    err = call(warp, "drop-collection", {"database": "mcpdb", "collection": "people"}, path=p, expect_error=True)
    assert "UnsupportedOperation" in err[0]
    assert coll.count_documents({}) == 3


def test_mongodb_prefixed_in_multi_kind_endpoint(warp):
    assert "mcpdb" in [d["name"] for d in call_json(warp, "mongodb_list-databases")]
