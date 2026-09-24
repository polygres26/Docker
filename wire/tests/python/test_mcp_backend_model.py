"""End-to-end proof of Warp's MCP backend model: every backend has a TYPE derived from its own
definition, tools are associated automatically from the types in an endpoint's scope, a `backend`
argument routes a call, and list_backends / describe_backend describe what is behind the endpoint.

One real Warp subprocess (the shaded jar) with a backend set holding: the gateway's default
Postgres, a second real Postgres (`pg2`), plus the Warp-emulated dynamowire/mongowire/influxwire
stores as logical backends of the default. Real clients (psycopg2, boto3, pymongo, HTTP) and a plain
JSON-RPC MCP client; no mocks. Different scopes are exercised through user-created endpoints
(/e/<id>) on the one listener, each narrowing the listener's scope.
"""
import json
import os

import subprocess

import boto3
import psycopg2
import pytest
from botocore.config import Config

from pymongo import MongoClient

from polywire_support import WarpProcess, RealPostgres, RealMongo, free_port
from mcp_support import (ADMIN_TOKEN, admin, call, call_json, create_endpoint, rpc, tool_defs,
                         tool_names)

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

RELATIONAL_TOOLS = {"execute_sql", "list_tables", "describe_table", "run_sql", "inspect_schema",
                    "column_stats", "compare_groups", "correlation", "sample_rows", "find_outliers",
                    "find_join_path", "explain_sql", "query_federated", "query_natural_language",
                    "explain_query", "document_schema"}
DISCOVERY = {"list_backends", "describe_backend"}


@pytest.fixture(scope="module")
def infra():
    default_pg = RealPostgres()
    pg2 = RealPostgres()
    with psycopg2.connect(host="localhost", port=pg2.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        c.autocommit = True
        cur = c.cursor()
        cur.execute("CREATE TABLE pg2_orders(id int primary key, total numeric)")
        cur.execute("INSERT INTO pg2_orders VALUES (1, 10.5), (2, 20)")
    with psycopg2.connect(host="localhost", port=default_pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        c.autocommit = True
        c.cursor().execute("CREATE TABLE default_people(id int primary key, name text)")
    real_mongo = RealMongo()
    MongoClient(host="localhost", port=real_mongo.port)["appdb"]["people"].insert_many(
        [{"_id": 1, "name": "Ada", "age": 36, "addr": {"city": "London"}}, {"_id": 2, "name": "Bob", "age": 25}])
    yield {"default_pg": default_pg, "pg2": pg2, "mongo": real_mongo}
    real_mongo.close()
    pg2.close()
    default_pg.close()


@pytest.fixture(scope="module")
def ports():
    return {"dynamo": free_port(), "influx": free_port(), "mongo": free_port()}


@pytest.fixture(scope="module")
def warp(infra, ports):
    pg2 = infra["pg2"]
    proc = WarpProcess(infra["default_pg"], "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": (f"default=jdbc:postgresql://localhost:{infra['default_pg'].port}/postgres|postgres|postgres;"
                          f"pg2=jdbc:postgresql://localhost:{pg2.port}/postgres|postgres|postgres;"
                          f"mongo1=mongodb://localhost:{infra['mongo'].port}/appdb||"),
        "WARP_BACKEND_GROUPS": "sales:plain=pg2|mixed:plain=mongo1",
        "WARP_BACKEND_DESCRIPTIONS": json.dumps({
            "default": "Gateway Postgres: people directory",
            "pg2": "Orders database (finance)",
            "mongo1": "Customer profiles (real MongoDB)",
            "default.dynamodb": "Session store (emulated DynamoDB)"}),
        "WARP_BACKEND_GROUP_DESCRIPTIONS": json.dumps({"sales": "Everything the sales team may query",
                                                                "mixed": "Profiles set"}),
        "WARP_MCP_EMULATED_STORES": "all",
        "WARP_DYNAMOWIRE_PORT": ports["dynamo"], "WARP_INFLUXWIRE_PORT": ports["influx"],
        "WARP_MONGOWIRE_PORT": ports["mongo"],
    })
    yield proc
    proc.close()


def dynamo_client(ports):
    return boto3.client("dynamodb", endpoint_url=f"http://localhost:{ports['dynamo']}", region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(retries={"max_attempts": 0}))


# ---------------------------------------------------------------------------
# type derivation, list_backends, descriptions
# ---------------------------------------------------------------------------

def test_list_backends_reports_type_description_and_sets(warp):
    out = call_json(warp.frontend_port, "list_backends")
    by_name = {b["name"]: b for b in out["backends"]}
    assert set(by_name) == {"default", "pg2", "mongo1", "default.dynamodb", "default.mongodb", "default.influx"}
    assert by_name["mongo1"]["type"] == "mongodb" and by_name["mongo1"]["engine"] == "real"
    assert by_name["mongo1"]["description"] == "Customer profiles (real MongoDB)"
    assert by_name["mongo1"]["groups"] == ["mixed"] and "find" in by_name["mongo1"]["tools"]
    assert by_name["default"]["type"] == "postgres" and by_name["default"]["engine"] == "real"
    assert by_name["default"]["description"] == "Gateway Postgres: people directory"
    assert by_name["pg2"]["type"] == "postgres" and by_name["pg2"]["groups"] == ["sales"]
    assert by_name["pg2"]["description"] == "Orders database (finance)"
    assert by_name["pg2"]["status"] == "ACTIVE"
    assert "execute_sql" in by_name["pg2"]["tools"]
    dyn = by_name["default.dynamodb"]
    assert dyn["type"] == "dynamodb" and dyn["engine"] == "warp-emulated" and dyn["host"] == "default"
    assert dyn["description"] == "Session store (emulated DynamoDB)" and "get_item" in dyn["tools"]
    assert by_name["default.mongodb"]["description"] is None
    sets = {s["name"]: s for s in out["backendSets"]}
    assert sets["sales"]["description"] == "Everything the sales team may query"
    assert sets["sales"]["members"] == ["pg2"] and sets["mixed"]["members"] == ["mongo1"]


def test_admin_backends_json_carries_type_and_description(warp):
    rows = {b["name"]: b for b in admin(warp, "GET", "/api/backends", expect=200).json()}
    assert rows["pg2"]["type"] == "postgres" and rows["pg2"]["description"] == "Orders database (finance)"
    assert rows["pg2"]["group"] == "sales"


# ---------------------------------------------------------------------------
# tools/list = union by type; relational unchanged; collisions resolved by backend
# ---------------------------------------------------------------------------

def test_tools_list_is_union_of_types_in_scope(warp):
    defs = tool_defs(warp.frontend_port)
    names = set(defs)
    assert RELATIONAL_TOOLS <= names and DISCOVERY <= names
    assert {"get_item", "query_table", "find", "aggregate", "query_influxql", "write_line_protocol"} <= names
    # one bare `list_tables` entry serves relational + dynamodb + influx, keyed by `backend`
    lt = defs["list_tables"]
    assert "backend" in lt["inputSchema"]["properties"]
    assert "pg2 (postgres)" in lt["description"] and "default.dynamodb (dynamodb)" in lt["description"]
    assert "backend" not in lt["inputSchema"].get("required", [])          # relational default stays optional
    # a tool offered by one backend only needs no routing argument; `find` has two (real + emulated)
    assert "backend" not in defs["get_item"]["inputSchema"]["properties"]
    assert "backend" in defs["find"]["inputSchema"]["properties"] and "backend" in defs["find"]["inputSchema"]["required"]
    assert "mongo1 (mongodb)" in defs["find"]["description"] and "default.mongodb (mongodb)" in defs["find"]["description"]


def test_relational_tools_unchanged_and_backend_pins_a_relational_backend(warp):
    p = warp.frontend_port
    # no backend: the gateway's own routing, exactly as before (default postgres)
    rows = json.loads(call(p, "execute_sql", {"sql": "SELECT count(*) AS n FROM default_people"})[0])
    assert rows == [{"n": 0}]
    # backend=pg2 pins the statement to that backend
    rows = json.loads(call(p, "execute_sql", {"sql": "SELECT id, total FROM pg2_orders ORDER BY id",
                                              "backend": "pg2"})[0])
    assert [r["id"] for r in rows] == [1, 2]
    tables = json.loads(call(p, "list_tables", {"backend": "pg2"})[0])
    assert any(t["tablename"] == "pg2_orders" for t in tables)
    # ...and pg2's table is not on the default backend
    call(p, "execute_sql", {"sql": "SELECT * FROM pg2_orders", "backend": "default"}, expect_error=True)


def test_backend_routing_errors_are_clear(warp):
    p = warp.frontend_port
    err = call(p, "execute_sql", {"sql": "select 1", "backend": "nope"}, expect_error=True)[0]
    assert "42501" in err and "Valid backends" in err and "pg2" in err
    # tool that does not apply to that backend's type
    err = call(p, "get_item", {"tableName": "t", "key": {"id": "1"}, "backend": "pg2"}, expect_error=True)[0]
    assert "does not apply" in err and "default.dynamodb" in err
    # list_tables on a dynamodb backend routes by type
    assert "TableNames" in call_json(p, "list_tables", {"backend": "default.dynamodb"})


def test_ambiguous_backend_is_an_error_naming_the_choices(warp):
    p = warp.frontend_port
    # influx `get_measurements` is the only influx backend -> optional; describe_backend with several -> required
    err = call(p, "describe_backend", {}, expect_error=True)[0]
    assert "required" in err and "pg2" in err and "default.mongodb" in err and "mongo1" in err


# ---------------------------------------------------------------------------
# describe_backend: live contents per type
# ---------------------------------------------------------------------------

def test_describe_backend_lists_live_contents(warp, ports):
    p = warp.frontend_port
    d = call_json(p, "describe_backend", {"backend": "pg2"})
    assert d["type"] == "postgres" and d["description"] == "Orders database (finance)"
    tables = {t["table"]: t for t in d["contents"]["tables"]}
    cols = {c["name"] for c in tables["public.pg2_orders"]["columns"]}
    assert cols == {"id", "total"}
    dflt = call_json(p, "describe_backend", {"backend": "default"})
    assert any(t["table"] == "public.default_people" for t in dflt["contents"]["tables"])

    ddb = dynamo_client(ports)
    ddb.create_table(TableName="model_sessions", BillingMode="PAY_PER_REQUEST",
                     AttributeDefinitions=[{"AttributeName": "sid", "AttributeType": "S"}],
                     KeySchema=[{"AttributeName": "sid", "KeyType": "HASH"}])
    d = call_json(p, "describe_backend", {"backend": "default.dynamodb"})
    t = {x["name"]: x for x in d["contents"]["tables"]}["model_sessions"]
    assert t["keySchema"] == [{"AttributeName": "sid", "KeyType": "HASH"}]
    assert d["description"] == "Session store (emulated DynamoDB)"


# ---------------------------------------------------------------------------
# scoped endpoints on the one listener
# ---------------------------------------------------------------------------

def test_single_backend_endpoint_advertises_only_its_types_tools(warp):
    ep = create_endpoint(warp, "pg2-only", "db:pg2", description="just the orders db")
    path, token, port = ep["path"], ep["token"], warp.frontend_port
    names = tool_names(port, path, token)
    assert RELATIONAL_TOOLS <= names and DISCOVERY <= names
    assert not ({"get_item", "find", "query_influxql"} & names)
    lb = call_json(port, "list_backends", path=path, token=token)
    assert [b["name"] for b in lb["backends"]] == ["pg2"] and lb["scope"] == {"type": "db", "name": "pg2"}
    assert lb["endpoint"]["description"] == "just the orders db"
    # never needs a backend argument, describe_backend defaults to the only backend
    assert call_json(port, "describe_backend", path=path, token=token)["name"] == "pg2"
    rows = json.loads(call(port, "execute_sql", {"sql": "SELECT count(*) AS n FROM pg2_orders"},
                           path=path, token=token)[0])
    assert rows == [{"n": 2}]
    # cannot reach another backend: by argument, by table, or by a tool of another type
    err = call(port, "execute_sql", {"sql": "select 1", "backend": "default"}, path=path, token=token,
               expect_error=True)[0]
    assert "42501" in err
    call(port, "execute_sql", {"sql": "SELECT * FROM default_people"}, path=path, token=token, expect_error=True)
    err = call(port, "get_item", {"tableName": "x", "key": {"a": "b"}}, path=path, token=token, expect_error=True)[0]
    assert "Unknown tool" in err


def test_emulated_store_endpoint_and_scope_refusal(warp, ports):
    ep = create_endpoint(warp, "dyn-only", "db:default")
    port = warp.frontend_port
    # db:default holds the emulated stores (logical backends of default)
    names = tool_names(port, ep["path"], ep["token"])
    assert {"get_item", "find", "query_influxql"} <= names
    out = call_json(port, "list_backends", path=ep["path"], token=ep["token"])
    assert {b["name"] for b in out["backends"]} == {"default", "default.dynamodb", "default.mongodb", "default.influx"}
    # an endpoint scoped to pg2 cannot reach the emulated stores (they live on `default`)
    ep2 = create_endpoint(warp, "pg2-only-2", "db:pg2")
    err = call(port, "list_tables", {"backend": "default.dynamodb"}, path=ep2["path"], token=ep2["token"],
               expect_error=True)[0]
    assert "42501" in err
    err = call(port, "put_item", {"tableName": "t", "item": {"id": "1"}}, path=ep2["path"], token=ep2["token"],
               expect_error=True)[0]
    assert "Unknown tool" in err


def test_group_endpoint_lists_its_set_and_members(warp):
    ep = create_endpoint(warp, "sales-set", "group:sales", description="sales agents")
    port = warp.frontend_port
    out = call_json(port, "list_backends", path=ep["path"], token=ep["token"])
    assert out["scope"] == {"type": "group", "name": "sales", "description": "Everything the sales team may query"}
    assert [b["name"] for b in out["backends"]] == ["pg2"]
    rows = json.loads(call(port, "inspect_schema", {}, path=ep["path"], token=ep["token"])[0])
    assert rows and rows[0]["backend"] == "pg2" and rows[0]["backend_type"] == "postgres"
    assert rows[0]["backend_description"] == "Orders database (finance)"


def test_inspect_schema_multi_backend_includes_type_and_description(warp):
    rows = json.loads(call(warp.frontend_port, "inspect_schema", {"scope": "all"})[0])
    r = next(x for x in rows if x["backend"] == "pg2" and x["table"] == "pg2_orders")
    assert r["backend_type"] == "postgres" and r["backend_description"] == "Orders database (finance)"


def test_description_hot_reload_via_admin_config(warp):
    body = {"backendDescriptions": json.dumps({
        "default": "Gateway Postgres: people directory", "pg2": "Orders database v2",
        "mongo1": "Customer profiles (real MongoDB)", "default.dynamodb": "Session store (emulated DynamoDB)"})}
    admin(warp, "PUT", "/api/config", body, expect=200)
    import time
    deadline = time.time() + 15
    while time.time() < deadline:
        b = {x["name"]: x for x in call_json(warp.frontend_port, "list_backends")["backends"]}
        if b["pg2"]["description"] == "Orders database v2":
            return
        time.sleep(0.3)
    raise AssertionError("description did not hot-reload")


# ---------------------------------------------------------------------------
# real external MongoDB: same tools, routed by `backend`, next to the emulated store
# ---------------------------------------------------------------------------

def test_mongo_tools_route_to_real_or_emulated_by_backend(warp, infra, ports):
    p = warp.frontend_port
    emu = MongoClient(host="localhost", port=ports["mongo"], serverSelectionTimeoutMS=5000)["mcpdb"]["people"]
    emu.insert_one({"_id": "e1", "name": "Emu", "age": 1})
    # two mongodb backends in scope -> `backend` is required, and the error lists both
    err = call(p, "find", {"database": "appdb", "collection": "people"}, expect_error=True)[0]
    assert "required" in err and "mongo1" in err and "default.mongodb" in err
    real = call_json(p, "find", {"backend": "mongo1", "database": "appdb", "collection": "people",
                                 "filter": {"age": {"$gt": 30}}})
    assert [d["name"] for d in real] == ["Ada"]
    emulated = call_json(p, "find", {"backend": "default.mongodb", "database": "mcpdb", "collection": "people"})
    assert [d["name"] for d in emulated] == ["Emu"]
    # the emulated store does not see the real backend's data and vice versa
    assert call_json(p, "find", {"backend": "default.mongodb", "database": "appdb", "collection": "people"}) == []
    # projection / sort / limit / count / aggregate / schema against the REAL server
    srt = call_json(p, "find", {"backend": "mongo1", "database": "appdb", "collection": "people",
                                "sort": {"age": 1}, "projection": {"name": 1, "_id": 0}, "limit": 5})
    assert srt == [{"name": "Bob"}, {"name": "Ada"}]
    assert "Found 2" in call(p, "count", {"backend": "mongo1", "database": "appdb", "collection": "people"})[0]
    agg = call_json(p, "aggregate", {"backend": "mongo1", "database": "appdb", "collection": "people",
                                     "pipeline": [{"$match": {"age": {"$gt": 30}}}]})
    assert [d["name"] for d in agg] == ["Ada"]
    schema = json.loads(call(p, "collection-schema", {"backend": "mongo1", "database": "appdb",
                                                       "collection": "people"})[-1])
    assert schema["fields"]["age"]["types"] == ["number"] and "addr.city" in schema["fields"]
    assert "appdb" in [d["name"] for d in call_json(p, "list-databases", {"backend": "mongo1"})]
    assert "people" in [c["name"] for c in call_json(p, "list-collections", {"backend": "mongo1", "database": "appdb"})]
    # a database outside the backend's configured one is refused
    err = call(p, "find", {"backend": "mongo1", "database": "other", "collection": "x"}, expect_error=True)[0]
    assert "outside this backend" in err
    # writes land in the real server, verified with a separate pymongo client
    real_coll = MongoClient(host="localhost", port=infra["mongo"].port)["appdb"]["people"]
    call(p, "insert-many", {"backend": "mongo1", "database": "appdb", "collection": "people",
                            "documents": [{"_id": 3, "name": "Cy", "age": 41}]})
    assert real_coll.find_one({"_id": 3})["name"] == "Cy"
    call(p, "update-many", {"backend": "mongo1", "database": "appdb", "collection": "people",
                            "filter": {"_id": 3}, "update": {"$set": {"age": 42}}})
    assert real_coll.find_one({"_id": 3})["age"] == 42
    call(p, "delete-many", {"backend": "mongo1", "database": "appdb", "collection": "people", "filter": {"_id": 3}})
    assert real_coll.find_one({"_id": 3}) is None
    assert emu.count_documents({}) == 1                                  # the emulated store was untouched
    err = call(p, "drop-collection", {"backend": "mongo1", "database": "appdb", "collection": "people"},
               expect_error=True)[0]
    assert "Unknown tool" in err or "UnsupportedOperation" in err


def test_describe_real_mongodb_backend(warp):
    d = call_json(warp.frontend_port, "describe_backend", {"backend": "mongo1"})
    assert d["type"] == "mongodb" and d["description"] == "Customer profiles (real MongoDB)"
    dbs = {x["name"]: x for x in d["contents"]["databases"]}
    assert dbs["appdb"]["collections"] == ["people"]


def test_single_backend_endpoint_for_a_non_relational_type(warp):
    ep = create_endpoint(warp, "mongo-only", "db:mongo1")
    port = warp.frontend_port
    names = tool_names(port, ep["path"], ep["token"])
    assert {"find", "aggregate", "insert-many", "list_backends", "describe_backend"} <= names
    assert not ({"execute_sql", "get_item", "query_influxql", "list_tables"} & names)
    defs = tool_defs(port, ep["path"], ep["token"])
    assert "backend" not in defs["find"]["inputSchema"]["properties"]           # single backend: never needed
    assert [d["name"] for d in call_json(port, "find", {"database": "appdb", "collection": "people",
                                                        "filter": {"_id": 1}}, path=ep["path"], token=ep["token"])] == ["Ada"]
    assert "Unknown tool" in call(port, "execute_sql", {"sql": "select 1"}, path=ep["path"], token=ep["token"],
                                  expect_error=True)[0]
    err = call(port, "find", {"backend": "default.mongodb", "database": "mcpdb", "collection": "people"},
               path=ep["path"], token=ep["token"], expect_error=True)[0]
    assert "42501" in err
    # a group endpoint mixing types lists both and advertises the union of their tools
    mixed = create_endpoint(warp, "mixed", "group:mixed")
    assert [b["name"] for b in call_json(port, "list_backends", path=mixed["path"], token=mixed["token"])["backends"]] == ["mongo1"]


def test_unreachable_backend_degrades_gracefully(warp, infra):
    subprocess.run(["docker", "stop", "-t", "1", infra["mongo"].name], capture_output=True, check=True)
    d = call_json(warp.frontend_port, "describe_backend", {"backend": "mongo1"})
    assert d["contents"] is None and "could not be listed" in d["contentsNote"]
    assert d["type"] == "mongodb" and d["description"] == "Customer profiles (real MongoDB)"
    # the other backends are unaffected
    assert call_json(warp.frontend_port, "describe_backend", {"backend": "pg2"})["contents"]["tables"]
