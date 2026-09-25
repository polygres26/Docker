"""MCP tools against REAL external DynamoDB and S3 backends registered in WARP_BACKENDS: a real
DynamoDB Local container and a real MinIO bucket, next to the gateway's default Postgres and the
emulated dynamowire store (so `get_item` needs `backend` to pick real vs emulated). Real Warp
subprocess, real boto3 clients to verify effects independently; no mocks.

(Developer edition caps a WARP_BACKENDS set at three backends -- default + DynamoDB + S3 here; the
Mongo/Postgres mix lives in test_mcp_backend_model.py.)
"""
import json
import os

import boto3
import pytest
from botocore.config import Config

from warp_test_support import WarpProcess, RealPostgres, RealDynamoDb, RealMinio, free_port
from mcp_support import ADMIN_TOKEN, call, call_json, create_endpoint, tool_defs, tool_names

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def infra():
    pg, ddb, minio = RealPostgres(), RealDynamoDb(), RealMinio()
    s3 = boto3.client("s3", endpoint_url=minio.endpoint, aws_access_key_id=minio.ACCESS_KEY,
                      aws_secret_access_key=minio.SECRET_KEY, region_name="us-east-1",
                      config=Config(s3={"addressing_style": "path"}))
    s3.create_bucket(Bucket="lake")
    for key, body in [("raw/2026/a.csv", b"id,v\n1,x\n"), ("raw/2026/b.csv", b"id,v\n2,y\n"),
                      ("curated/c.json", b'{"k": 1}'), ("readme.txt", b"hello lake")]:
        s3.put_object(Bucket="lake", Key=key, Body=body)
    ddb_client = boto3.client("dynamodb", endpoint_url=ddb.endpoint, region_name=ddb.REGION,
                              aws_access_key_id=ddb.ACCESS_KEY, aws_secret_access_key=ddb.SECRET_KEY)
    ddb_client.create_table(TableName="real_orders", BillingMode="PAY_PER_REQUEST",
                            AttributeDefinitions=[{"AttributeName": "customer", "AttributeType": "S"},
                                                  {"AttributeName": "order_no", "AttributeType": "N"}],
                            KeySchema=[{"AttributeName": "customer", "KeyType": "HASH"},
                                       {"AttributeName": "order_no", "KeyType": "RANGE"}])
    for n, total in [(1, 10), (2, 25)]:
        ddb_client.put_item(TableName="real_orders", Item={"customer": {"S": "acme"}, "order_no": {"N": str(n)},
                                                           "amount": {"N": str(total)}})
    yield {"pg": pg, "ddb": ddb, "minio": minio, "s3": s3, "ddb_client": ddb_client}
    minio.close()
    ddb.close()
    pg.close()


@pytest.fixture(scope="module")
def ports():
    return {"dynamo": free_port(), "influx": free_port(), "mongo": free_port()}


@pytest.fixture(scope="module")
def warp(infra, ports):
    ddb, minio = infra["ddb"], infra["minio"]
    proc = WarpProcess(infra["pg"], "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": (f"default=jdbc:postgresql://localhost:{infra['pg'].port}/postgres|postgres|postgres;"
                          f"dyn1=dynamodb://us-east-1?endpoint={ddb.endpoint}|{ddb.ACCESS_KEY}|{ddb.SECRET_KEY};"
                          f"lake=s3://lake?endpoint={minio.endpoint}&pathStyleAccess=true|{minio.ACCESS_KEY}|{minio.SECRET_KEY}"),
        "WARP_BACKEND_DESCRIPTIONS": json.dumps({"dyn1": "Order history (real DynamoDB)",
                                                 "lake": "Data lake bucket (real S3/MinIO)"}),
        "WARP_MCP_EMULATED_STORES": "dynamodb",
        "WARP_DYNAMOWIRE_PORT": ports["dynamo"], "WARP_INFLUXWIRE_PORT": ports["influx"],
        "WARP_MONGOWIRE_PORT": ports["mongo"],
    })
    yield proc
    proc.close()


def test_list_backends_shows_real_dynamodb_and_s3_types(warp):
    out = call_json(warp.frontend_port, "list_backends")
    by = {b["name"]: b for b in out["backends"]}
    assert set(by) == {"default", "dyn1", "lake", "default.dynamodb"}
    assert by["dyn1"]["type"] == "dynamodb" and by["dyn1"]["engine"] == "real"
    assert by["dyn1"]["description"] == "Order history (real DynamoDB)"
    assert by["lake"]["type"] == "s3" and by["lake"]["description"] == "Data lake bucket (real S3/MinIO)"
    assert "list_objects" in by["lake"]["tools"] and "get_item" in by["dyn1"]["tools"]
    assert "execute_sql" not in by["lake"]["tools"]
    names = set(tool_names(warp.frontend_port))
    assert {"execute_sql", "get_item", "list_objects", "get_object", "list_backends"} <= names
    defs = tool_defs(warp.frontend_port)
    assert "backend" in defs["get_item"]["inputSchema"]["required"]         # real + emulated dynamodb
    assert "backend" not in defs["list_objects"]["inputSchema"]["properties"]  # only one S3 backend


def test_dynamodb_routes_to_real_backend(warp, infra):
    p = warp.frontend_port
    err = call(p, "get_item", {"tableName": "real_orders", "key": {"customer": "acme", "order_no": 1}},
               expect_error=True)[0]
    assert "required" in err and "dyn1" in err and "default.dynamodb" in err
    got = call_json(p, "get_item", {"backend": "dyn1", "tableName": "real_orders",
                                    "key": {"customer": {"S": "acme"}, "order_no": {"N": "2"}}})
    assert got["Item"]["amount"] == {"N": "25"}
    q = call_json(p, "query_table", {"backend": "dyn1", "tableName": "real_orders",
                                     "keyConditionExpression": "customer = :c AND order_no > :n",
                                     "expressionAttributeValues": {":c": {"S": "acme"}, ":n": {"N": "1"}}})
    assert [i["order_no"]["N"] for i in q["Items"]] == ["2"]
    scan = call_json(p, "scan_table", {"backend": "dyn1", "tableName": "real_orders",
                                       "filterExpression": "amount >= :t", "expressionAttributeValues": {":t": 10}})
    assert len(scan["Items"]) == 2
    assert "real_orders" in call_json(p, "list_tables", {"backend": "dyn1"})["TableNames"]
    desc = call_json(p, "describe_table", {"backend": "dyn1", "tableName": "real_orders"})["Table"]
    assert {(k["AttributeName"], k["KeyType"]) for k in desc["KeySchema"]} == {("customer", "HASH"), ("order_no", "RANGE")}
    # the emulated store is a different system: it does not have the real table
    assert "real_orders" not in call_json(p, "list_tables", {"backend": "default.dynamodb"})["TableNames"]
    err = call(p, "get_item", {"backend": "default.dynamodb", "tableName": "real_orders", "key": {"customer": "acme", "order_no": 1}},
               expect_error=True)[0]
    assert "not found" in err.lower() or "ResourceNotFound" in err


def test_dynamodb_writes_land_in_the_real_backend(warp, infra):
    p, c = warp.frontend_port, infra["ddb_client"]
    call(p, "put_item", {"backend": "dyn1", "tableName": "real_orders",
                         "item": {"customer": "beta", "order_no": 7, "amount": 99, "vip": True}})
    item = c.get_item(TableName="real_orders", Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}})["Item"]
    assert item["amount"] == {"N": "99"} and item["vip"] == {"BOOL": True}
    call(p, "update_item", {"backend": "dyn1", "tableName": "real_orders", "key": {"customer": "beta", "order_no": 7},
                            "updateExpression": "SET amount = :t", "expressionAttributeValues": {":t": 100}})
    assert c.get_item(TableName="real_orders", Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}}
                      )["Item"]["amount"] == {"N": "100"}
    call(p, "delete_item", {"backend": "dyn1", "tableName": "real_orders", "key": {"customer": "beta", "order_no": 7}})
    assert "Item" not in c.get_item(TableName="real_orders", Key={"customer": {"S": "beta"}, "order_no": {"N": "7"}})
    call(p, "create_table", {"backend": "dyn1", "tableName": "made_by_mcp", "partitionKey": "id", "partitionKeyType": "S"})
    assert "made_by_mcp" in c.list_tables()["TableNames"]
    err = call(p, "get_item", {"backend": "dyn1", "tableName": "no_such_table", "key": {"id": "x"}}, expect_error=True)[0]
    assert "ResourceNotFoundException" in err


def test_describe_real_dynamodb_and_s3(warp):
    p = warp.frontend_port
    d = call_json(p, "describe_backend", {"backend": "dyn1"})
    t = {x["name"]: x for x in d["contents"]["tables"]}["real_orders"]
    assert {(k["AttributeName"], k["KeyType"]) for k in t["keySchema"]} == {("customer", "HASH"), ("order_no", "RANGE")}
    s = call_json(p, "describe_backend", {"backend": "lake"})
    assert s["type"] == "s3" and s["contents"]["bucket"] == "lake"
    assert sorted(s["contents"]["prefixes"]) == ["curated/", "raw/"]
    assert [o["key"] for o in s["contents"]["rootObjects"]] == ["readme.txt"]


def test_s3_tools_on_real_minio(warp, infra):
    p, s3 = warp.frontend_port, infra["s3"]
    assert call_json(p, "list_buckets")["buckets"] == [{"name": "lake"}]
    lst = call_json(p, "list_objects", {"prefix": "raw/", "delimiter": "/"})
    assert lst["commonPrefixes"] == ["raw/2026/"] and lst["objects"] == []
    lst = call_json(p, "list_objects", {"prefix": "raw/2026/"})
    assert [o["key"] for o in lst["objects"]] == ["raw/2026/a.csv", "raw/2026/b.csv"]
    assert call_json(p, "list_objects", {"maxKeys": 1})["isTruncated"] is True
    head = call_json(p, "head_object", {"key": "readme.txt"})
    assert head["contentLength"] == 10
    got = call_json(p, "get_object", {"key": "raw/2026/a.csv"})
    assert got["body"] == "id,v\n1,x\n" and got["encoding"] == "utf-8" and got["truncated"] is False
    assert call_json(p, "get_object", {"key": "readme.txt", "maxBytes": 5})["body"] == "hello"
    call(p, "put_object", {"key": "written/by_mcp.txt", "content": "from mcp", "contentType": "text/plain"})
    assert s3.get_object(Bucket="lake", Key="written/by_mcp.txt")["Body"].read() == b"from mcp"
    call(p, "delete_object", {"key": "written/by_mcp.txt"})
    with pytest.raises(Exception):
        s3.head_object(Bucket="lake", Key="written/by_mcp.txt")
    assert "NoSuchKey" in call(p, "get_object", {"key": "nope"}, expect_error=True)[0]
    # bound to its bucket
    assert "42501" in call(p, "list_objects", {"bucket": "other"}, expect_error=True)[0]
    # routed by backend even though only one s3 backend exists
    assert call_json(p, "list_buckets", {"backend": "lake"})["buckets"]
    assert "does not apply" in call(p, "list_objects", {"backend": "dyn1"}, expect_error=True)[0]


def test_endpoint_scoped_to_s3_backend_only_serves_s3(warp):
    ep = create_endpoint(warp, "lake-only", "db:lake")
    port = warp.frontend_port
    names = tool_names(port, ep["path"], ep["token"])
    assert {"list_objects", "get_object", "list_backends", "describe_backend"} <= names
    assert not ({"execute_sql", "get_item", "list_tables"} & names)
    assert call_json(port, "list_objects", {"delimiter": "/"}, path=ep["path"], token=ep["token"])["commonPrefixes"]
    assert "42501" in call(port, "list_objects", {"backend": "dyn1"}, path=ep["path"], token=ep["token"], expect_error=True)[0]
    assert "Unknown tool" in call(port, "get_item", {"tableName": "real_orders", "key": {"customer": "acme", "order_no": 1}},
                                  path=ep["path"], token=ep["token"], expect_error=True)[0]
