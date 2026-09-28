"""MCP data tools of the Warp-hosted stores (redis, azblob, azqueue, aztable, gcs, sns, kinesis, awsparams, pubsub, firestore,
datastore, bigtable), against real Warp subprocesses on real Postgres servers. Covered: per-backend-type tool sets on scoped
endpoints (single backend and whole backend set), reads and writes through tools/call, data visible both ways between the tools
and the native wire frontends, read-only enforcement (WARP_MCP_READ_ONLY hides and refuses write tools), the sensitive-data
rules (secrets / decrypted parameters / KMS decrypt), bounded results, endpoint expiry and the metrics the calls produce.

Needs WARP_TEST_JAR=<jar> (and WARP_TEST_PG_LOCAL=1 for local Postgres servers). No mocks.
"""
import base64
import json
import os
import socket
import time
import uuid

import boto3
import psycopg2
import pytest
import requests
from botocore.config import Config

from mcp_support import ADMIN_TOKEN, admin, call, call_json, create_endpoint, rpc, tool_defs, tool_names
from redis_resp_client import Resp
from warp_test_support import RealPostgres, WarpProcess, free_port, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

STORES = ["redis", "azblob", "azqueue", "aztable", "gcs", "sns", "kinesis", "awsparams", "pubsub", "firestore", "datastore",
          "bigtable"]
PG2_STORES = ["redis", "gcs", "pubsub"]
AZ_TOKEN = "mst-azure-bearer"

# tools each store must offer (read tools and write tools), by the kind of the backend they belong to
READ_TOOLS = {
    "redis": ["redis_get", "redis_type", "redis_ttl", "redis_scan_keys", "redis_scan_all_keys", "redis_hget", "redis_hgetall",
              "redis_hexists", "redis_lrange", "redis_llen", "redis_smembers", "redis_zrange", "redis_xrange", "redis_dbsize",
              "redis_info"],
    "azblob": ["azblob_list_containers", "azblob_list_blobs", "azblob_get_blob", "azblob_get_blob_properties"],
    "azqueue": ["azqueue_list_queues", "azqueue_peek_messages", "azqueue_get_queue_metadata"],
    "aztable": ["aztable_list_tables", "aztable_get_entity", "aztable_query_entities"],
    "gcs": ["gcs_list_buckets", "gcs_get_bucket_metadata", "gcs_list_objects", "gcs_get_object", "gcs_get_object_metadata"],
    "sns": ["sns_list_topics", "sns_get_topic_attributes", "sns_list_subscriptions"],
    "kinesis": ["kinesis_list_streams", "kinesis_describe_stream_summary", "kinesis_list_shards", "kinesis_get_records"],
    "awsparams": ["secrets_list_secrets", "secrets_describe_secret", "secrets_get_secret_value", "ssm_get_parameter",
                  "ssm_get_parameters_by_path", "ssm_describe_parameters", "kms_list_keys", "kms_describe_key", "kms_list_aliases",
                  "kms_encrypt", "kms_decrypt", "sts_get_caller_identity"],
    "pubsub": ["pubsub_list_topics", "pubsub_get_topic", "pubsub_list_subscriptions", "pubsub_get_subscription",
               "pubsub_list_topic_subscriptions"],
    "firestore": ["firestore_list_collections", "firestore_list_documents", "firestore_get_documents", "firestore_query_collection",
                  "firestore_run_query", "firestore_count"],
    "datastore": ["datastore_lookup", "datastore_run_query", "datastore_count"],
    "bigtable": ["bigtable_list_tables", "bigtable_get_table", "bigtable_read_rows", "bigtable_read_row"],
}
WRITE_TOOLS = {
    "redis": ["redis_set", "redis_delete", "redis_expire", "redis_rename", "redis_incr", "redis_hset", "redis_hdel", "redis_lpush",
              "redis_rpush", "redis_lpop", "redis_rpop", "redis_sadd", "redis_srem", "redis_zadd", "redis_zrem", "redis_xadd",
              "redis_xdel", "redis_publish"],
    "azblob": ["azblob_create_container", "azblob_delete_container", "azblob_upload_blob", "azblob_delete_blob"],
    "azqueue": ["azqueue_create_queue", "azqueue_delete_queue", "azqueue_send_message", "azqueue_receive_messages",
                "azqueue_delete_message", "azqueue_clear_messages"],
    "aztable": ["aztable_create_table", "aztable_delete_table", "aztable_insert_entity", "aztable_upsert_entity",
                "aztable_delete_entity"],
    "gcs": ["gcs_create_bucket", "gcs_delete_bucket", "gcs_put_object", "gcs_copy_object", "gcs_delete_object"],
    "sns": ["sns_create_topic", "sns_delete_topic", "sns_set_topic_attributes", "sns_subscribe", "sns_unsubscribe", "sns_publish"],
    "kinesis": ["kinesis_create_stream", "kinesis_delete_stream", "kinesis_put_record", "kinesis_put_records"],
    "awsparams": ["secrets_create_secret", "secrets_put_secret_value", "secrets_delete_secret", "ssm_put_parameter",
                  "ssm_delete_parameter", "kms_create_key"],
    "pubsub": ["pubsub_create_topic", "pubsub_delete_topic", "pubsub_publish", "pubsub_create_subscription",
               "pubsub_delete_subscription", "pubsub_pull", "pubsub_ack", "pubsub_modify_ack_deadline"],
    "firestore": ["firestore_add_document", "firestore_set_document", "firestore_update_document", "firestore_delete_document"],
    "datastore": ["datastore_upsert_entity", "datastore_insert_entity", "datastore_delete_entity"],
    "bigtable": ["bigtable_create_table", "bigtable_delete_table", "bigtable_mutate_row", "bigtable_mutate_rows",
                 "bigtable_delete_row", "bigtable_increment", "bigtable_drop_row_range"],
}


def mst_first_free_ignite_port():
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port in 47500..47599")


def mst_pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def mst_uniq(prefix):
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def mst_start(pg, pg2, read_only=False, allow_secret_reads=False, wire=True):
    """A Warp whose MCP listener is the frontend. With wire=True the native frontends the tests cross-check run too."""
    ports = isolated_ports("WARP_MCP_PORT")
    env = {**ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_ADMIN_TOKEN": ADMIN_TOKEN,
           "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
           "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{mst_first_free_ignite_port()}",
           "WARP_KMS_INSECURE_DEV_KEY": "true", "WARP_AZURE_DEV_ACCOUNT": "true", "WARP_AZURE_BEARER_TOKEN": AZ_TOKEN,
           "WARP_GCSWIRE_ALLOW_ANONYMOUS": "true", "WARP_MCP_GCP_PROJECT": "mst-project",
           "WARP_BACKENDS": f"default={mst_pg_url(pg)}|postgres|postgres;pg2={mst_pg_url(pg2)}|postgres|postgres",
           "WARP_BACKEND_GROUPS": "hosting:plain=default,pg2"}
    if wire:
        env.update({"WARP_REDISWIRE_ENABLED": "true", "WARP_GCSWIRE_ENABLED": "true", "WARP_PUBSUBWIRE_ENABLED": "true",
                    "WARP_FIRESTOREWIRE_ENABLED": "true", "WARP_DATASTOREWIRE_ENABLED": "true",
                    "WARP_AZBLOBWIRE_ENABLED": "true", "WARP_AWSWIRE_PORT": str(free_port())})
    if read_only:
        env["WARP_MCP_READ_ONLY"] = "true"
    if allow_secret_reads:
        env["WARP_MCP_ALLOW_SECRET_READS"] = "true"
    proc = WarpProcess(pg, "WARP_MCP_PORT", frontend_name="mcp-store-tools", extra_env=env)
    proc.ports = ports
    proc.aws_port = env.get("WARP_AWSWIRE_PORT")
    return proc


def mst_api(proc, method, path, body=None, expect=200):
    r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                         headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
    assert r.status_code == expect, (r.status_code, r.text)
    return r.json()


def mst_default_ep(proc):
    e = create_endpoint(proc, "default-host-" + uuid.uuid4().hex[:6], "db:default")
    return Ep(proc.frontend_port, e["path"], e["token"])


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def warp(pgs):
    proc = mst_start(pgs[0], pgs[1])
    mst_api(proc, "PATCH", "/api/backend-sets/hosting/backends/default", {"enabledStores": STORES})
    mst_api(proc, "PATCH", "/api/backend-sets/hosting/backends/pg2", {"enabledStores": PG2_STORES})
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def port(warp):
    return warp.frontend_port


@pytest.fixture(scope="module")
def p(warp):
    """The endpoint of the default backend (it hosts every store), so tool calls need no `backend` argument."""
    e = create_endpoint(warp, "default-host", "db:default")
    return Ep(warp.frontend_port, e["path"], e["token"])


@pytest.fixture(scope="module")
def ro(pgs, warp):
    # a second Warp on the same config database (stores, backends and endpoints come from it), read-only
    proc = mst_start(pgs[0], pgs[1], read_only=True, wire=False)
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def ro_allow(pgs, warp):
    proc = mst_start(pgs[0], pgs[1], read_only=True, allow_secret_reads=True, wire=False)
    yield proc
    proc.close()


def aws(warp, service):
    return boto3.client(service, endpoint_url=f"http://localhost:{warp.aws_port}", region_name="us-east-1",
                        aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))


def rest(warp, var, path, method="GET", **kw):
    return requests.request(method, f"http://localhost:{warp.ports[var]}{path}", timeout=30, **kw)


def resp(warp):
    return Resp("127.0.0.1", int(warp.ports["WARP_REDISWIRE_PORT"]))


class Ep:
    """One MCP endpoint (port + path + bearer token); a bare port means the listener root (whole gateway scope)."""

    def __init__(self, port, path="/", token=None):
        self.port, self.path, self.token = port, path, token


def _split(ep, path, token):
    if isinstance(ep, Ep):
        return ep.port, path or ep.path, token or ep.token
    return ep, path or "/", token


def cj(ep, name, args=None, path=None, token=None):
    port, path, token = _split(ep, path, token)
    return call_json(port, name, args, path=path, token=token)


def err(ep, name, args=None, path=None, token=None):
    port, path, token = _split(ep, path, token)
    return call(port, name, args, path=path, token=token, expect_error=True)[0]


def tnames(ep, path=None, token=None):
    port, path, token = _split(ep, path, token)
    return tool_names(port, path, token)


def tdefs(ep, path=None, token=None):
    port, path, token = _split(ep, path, token)
    return tool_defs(port, path, token)


# ----------------------------------------------------------------------------------------------------------- tool sets

def test_backend_set_endpoint_lists_the_tool_set_of_every_backend_type(warp, port):
    ep = create_endpoint(warp, "whole-set", "group:hosting")
    names = tool_names(port, ep["path"], ep["token"])
    for store in STORES:
        for t in READ_TOOLS[store] + WRITE_TOOLS[store]:
            assert t in names, (store, t)
    assert "execute_sql" in names            # the relational tools are still there for the SQL backends of the set
    defs = tool_defs(port, ep["path"], ep["token"])
    for t in READ_TOOLS["redis"] + WRITE_TOOLS["gcs"]:
        assert defs[t]["inputSchema"]["type"] == "object" and defs[t]["description"]


def test_single_backend_endpoint_lists_only_the_tools_of_the_stores_it_reaches(warp, port):
    ep = create_endpoint(warp, "pg2-only", "db:pg2")
    names = tool_names(port, ep["path"], ep["token"])
    for store in PG2_STORES:
        assert set(READ_TOOLS[store] + WRITE_TOOLS[store]) <= names, store
    for store in set(STORES) - set(PG2_STORES):
        assert not (set(READ_TOOLS[store] + WRITE_TOOLS[store]) & names), store
    # a tool of a store outside the endpoint is not routable either
    msg = err(port, "sns_list_topics", path=ep["path"], token=ep["token"])
    assert "Unknown tool" in msg


def test_list_backends_names_the_tools_of_each_typed_store(port):
    out = call_json(port, "list_backends")
    by = {b["name"]: b for b in out["backends"]}
    for store in STORES:
        b = by[f"default.{store}"]
        assert b["engine"] == "warp-emulated" and b["type"] == store
        assert set(READ_TOOLS[store] + WRITE_TOOLS[store]) <= set(b["tools"]), store
    assert by["pg2.redis"]["sharded"] is True


def test_every_tool_has_a_description_and_object_schema(port):
    defs = tool_defs(port)
    for store in STORES:
        for t in READ_TOOLS[store] + WRITE_TOOLS[store]:
            d = defs[t]
            assert d["description"].strip() and d["inputSchema"]["type"] == "object", t
            for r in d["inputSchema"].get("required", []):
                assert r in d["inputSchema"]["properties"], (t, r)


# ----------------------------------------------------------------------------------------------------------- redis

def test_redis_strings_expiry_counters_and_scan(p, warp):
    tag = uuid.uuid4().hex[:8]      # a hash tag keeps the keys of one command on one slot (the store is sharded over two hosts)
    k = f"mst:s:{{{tag}}}"
    assert cj(p, "redis_set", {"key": k, "value": "héllo"})["ok"] is True
    got = cj(p, "redis_get", {"key": k})
    assert got == {"key": k, "exists": True, "value": "héllo", "truncated": False}
    assert cj(p, "redis_type", {"key": k})["type"] == "string"
    assert cj(p, "redis_set", {"key": k, "value": "other", "nx": True})["ok"] is False
    assert cj(p, "redis_set", {"key": k, "value": "x", "ex": 100})["ok"] is True
    assert 90 <= cj(p, "redis_ttl", {"key": k})["ttl"] <= 100
    n = f"mst:n:{{{tag}}}"
    assert cj(p, "redis_incr", {"key": n})["value"] == 1
    assert cj(p, "redis_incr", {"key": n, "by": 41})["value"] == 42
    scanned = cj(p, "redis_scan_all_keys", {"pattern": "mst:n:*"})
    assert n in scanned["keys"] and scanned["truncated"] is False
    page = cj(p, "redis_scan_keys", {"pattern": "mst:n:*", "count": 100})
    assert "cursor" in page and isinstance(page["keys"], list)
    assert cj(p, "redis_rename", {"key": n, "newKey": n + "b"})["ok"] is True
    assert cj(p, "redis_delete", {"keys": [n + "b", k]})["deleted"] == 2
    assert cj(p, "redis_get", {"key": k})["exists"] is False


def test_redis_hash_list_set_zset_stream(p):
    h = mst_uniq("mst:h:")
    assert cj(p, "redis_hset", {"key": h, "mapping": {"a": "1", "b": "2"}})["added"] == 2
    assert cj(p, "redis_hset", {"key": h, "field": "c", "value": "3"})["added"] == 1
    assert cj(p, "redis_hget", {"key": h, "field": "b"})["value"] == "2"
    assert cj(p, "redis_hgetall", {"key": h})["fields"] == {"a": "1", "b": "2", "c": "3"}
    assert cj(p, "redis_hexists", {"key": h, "field": "zz"})["exists"] is False
    assert cj(p, "redis_hdel", {"key": h, "fields": ["a"]})["deleted"] == 1
    lst = mst_uniq("mst:l:")
    assert cj(p, "redis_rpush", {"key": lst, "values": ["1", "2", "3"]})["length"] == 3
    assert cj(p, "redis_lpush", {"key": lst, "values": ["0"]})["length"] == 4
    assert cj(p, "redis_lrange", {"key": lst, "start": 0, "stop": -1})["values"] == ["0", "1", "2", "3"]
    assert cj(p, "redis_lpop", {"key": lst})["value"] == "0"
    assert cj(p, "redis_rpop", {"key": lst, "count": 2})["values"] == ["3", "2"]
    assert cj(p, "redis_llen", {"key": lst})["length"] == 1
    s = mst_uniq("mst:set:")
    assert cj(p, "redis_sadd", {"key": s, "members": ["x", "y", "z"]})["added"] == 3
    assert sorted(cj(p, "redis_smembers", {"key": s})["members"]) == ["x", "y", "z"]
    assert cj(p, "redis_srem", {"key": s, "members": ["x"]})["removed"] == 1
    z = mst_uniq("mst:z:")
    assert cj(p, "redis_zadd", {"key": z, "members": {"a": 1, "b": 2.5, "c": 3}})["added"] == 3
    assert cj(p, "redis_zrange", {"key": z, "start": 0, "stop": -1})["members"] == ["a", "b", "c"]
    ws = cj(p, "redis_zrange", {"key": z, "start": 0, "stop": 0, "withScores": True, "reverse": True})
    assert ws["members"] == [{"member": "c", "score": 3.0}]
    assert cj(p, "redis_zrem", {"key": z, "members": ["a"]})["removed"] == 1
    x = mst_uniq("mst:x:")
    i1 = cj(p, "redis_xadd", {"key": x, "fields": {"f": "v1", "g": "w"}})["id"]
    cj(p, "redis_xadd", {"key": x, "fields": {"f": "v2"}})
    entries = cj(p, "redis_xrange", {"key": x})["entries"]
    assert [e["fields"]["f"] for e in entries] == ["v1", "v2"] and entries[0]["id"] == i1
    assert cj(p, "redis_xdel", {"key": x, "ids": [i1]})["deleted"] == 1
    assert cj(p, "redis_publish", {"channel": "mst", "message": "m"})["receivers"] == 0
    assert cj(p, "redis_dbsize")["keys"] >= 4
    assert "redis_version" in cj(p, "redis_info", {"section": "server"})["info"]


def test_redis_errors_and_bounds(p):
    k = mst_uniq("mst:e:")
    cj(p, "redis_set", {"key": k, "value": "v"})
    assert "WRONGTYPE" in err(p, "redis_hget", {"key": k, "field": "f"})
    assert "invalid arguments" in err(p, "redis_hset", {"key": k})
    assert "db must be between" in err(p, "redis_get", {"key": k, "db": 99})
    big = mst_uniq("mst:big:")
    cj(p, "redis_rpush", {"key": big, "values": [str(i) for i in range(1500)]})
    out = cj(p, "redis_lrange", {"key": big, "start": 0, "stop": 1499})
    assert out["count"] == 1000 and out["truncated"] is True and out["values"][0] == "0" and out["values"][-1] == "999"
    rest_of_it = cj(p, "redis_lrange", {"key": big, "start": 1000, "stop": 1499})
    assert rest_of_it["count"] == 500 and rest_of_it["truncated"] is False
    v = mst_uniq("mst:txt:")
    cj(p, "redis_set", {"key": v, "value": "a" * 300000})
    t = cj(p, "redis_get", {"key": v})
    assert t["truncated"] is True and len(t["value"]) == 262144 and t["length"] == 300000


def test_redis_data_is_shared_with_the_resp_frontend(p, warp):
    r = resp(warp)
    k = mst_uniq("mst:wire:")
    cj(p, "redis_set", {"key": k, "value": "from-mcp"})
    assert r("GET", k) == b"from-mcp"
    r("SET", k + "2", "from-resp")
    assert cj(p, "redis_get", {"key": k + "2"})["value"] == "from-resp"
    r("HSET", k + "h", "f", "v")
    assert cj(p, "redis_hgetall", {"key": k + "h"})["fields"] == {"f": "v"}
    cj(p, "redis_zadd", {"key": k + "z", "members": {"m": 7}})
    assert r("ZSCORE", k + "z", "m") in (b"7", 7.0)
    r.close()


def test_redis_db_argument_selects_the_logical_database(p, warp):
    k = mst_uniq("mst:db:")
    cj(p, "redis_set", {"key": k, "value": "in3", "db": 3})
    assert cj(p, "redis_get", {"key": k})["exists"] is False
    assert cj(p, "redis_get", {"key": k, "db": 3})["value"] == "in3"
    r = resp(warp)
    r("SELECT", 3)
    assert r("GET", k) == b"in3"
    r.close()


# ----------------------------------------------------------------------------------------------------------- gcs

def test_gcs_buckets_and_objects_round_trip_and_wire_visibility(p, warp):
    b = mst_uniq("mst-bkt-")
    assert cj(p, "gcs_create_bucket", {"bucket": b, "location": "EU"})["name"] == b
    assert b in [x["name"] for x in cj(p, "gcs_list_buckets", {"prefix": "mst-bkt-"})["buckets"]]
    assert cj(p, "gcs_get_bucket_metadata", {"bucket": b})["location"] == "EU"
    o = cj(p, "gcs_put_object", {"bucket": b, "object": "dir/a b.txt", "content": "héllo wörld", "contentType": "text/plain"})
    assert o["name"] == "dir/a b.txt" and o["contentType"] == "text/plain" and o["size"] == "13"
    got = cj(p, "gcs_get_object", {"bucket": b, "object": "dir/a b.txt"})
    assert got["encoding"] == "utf-8" and got["body"] == "héllo wörld" and got["size"] == 13 and got["truncated"] is False
    cj(p, "gcs_put_object", {"bucket": b, "object": "bin", "contentBase64": base64.b64encode(b"\xff" * 256).decode()})
    binary = cj(p, "gcs_get_object", {"bucket": b, "object": "bin"})
    assert binary["encoding"] == "base64" and base64.b64decode(binary["body"]) == b"\xff" * 256
    part = cj(p, "gcs_get_object", {"bucket": b, "object": "bin", "maxBytes": 10})
    assert part["truncated"] is True and part["size"] == 256 and len(base64.b64decode(part["body"])) == 10
    lst = cj(p, "gcs_list_objects", {"bucket": b, "delimiter": "/"})
    assert lst["prefixes"] == ["dir/"] and [x["name"] for x in lst["objects"]] == ["bin"]
    assert [x["name"] for x in cj(p, "gcs_list_objects", {"bucket": b, "prefix": "dir/"})["objects"]] == ["dir/a b.txt"]
    assert cj(p, "gcs_get_object_metadata", {"bucket": b, "object": "bin"})["size"] == "256"
    # the native JSON API sees what the tools wrote, and the tools see what it writes
    listing = rest(warp, "WARP_GCSWIRE_PORT", f"/storage/v1/b/{b}/o").json()
    assert {i["name"] for i in listing["items"]} == {"dir/a b.txt", "bin"}
    rest(warp, "WARP_GCSWIRE_PORT", f"/upload/storage/v1/b/{b}/o?uploadType=media&name=wire.txt", "POST", data=b"via wire",
         headers={"Content-Type": "text/plain"}).raise_for_status()
    assert cj(p, "gcs_get_object", {"bucket": b, "object": "wire.txt"})["body"] == "via wire"
    cp = cj(p, "gcs_copy_object", {"sourceBucket": b, "sourceObject": "wire.txt", "destinationBucket": b,
                                          "destinationObject": "copy.txt"})
    assert cp["name"] == "copy.txt"
    assert cj(p, "gcs_delete_object", {"bucket": b, "object": "copy.txt"})["ok"] is True
    assert "No such object" in err(p, "gcs_get_object", {"bucket": b, "object": "copy.txt"}) or "notFound" in err(
        p, "gcs_get_object", {"bucket": b, "object": "copy.txt"})
    assert "Conflict" in err(p, "gcs_create_bucket", {"bucket": b}) or "conflict" in err(p, "gcs_create_bucket", {"bucket": b}).lower()
    for name in ("dir/a b.txt", "bin", "wire.txt"):
        cj(p, "gcs_delete_object", {"bucket": b, "object": name})
    assert cj(p, "gcs_delete_bucket", {"bucket": b})["ok"] is True


# ----------------------------------------------------------------------------------------------------------- azure

def test_azure_blob_tools_and_wire_visibility(p, warp):
    c = mst_uniq("mstc")
    assert cj(p, "azblob_create_container", {"container": c})["ok"] is True
    assert c in [x["Name"] for x in cj(p, "azblob_list_containers", {"prefix": "mstc"})["containers"]]
    up = cj(p, "azblob_upload_blob", {"container": c, "blob": "d/one.txt", "content": "blob ☃", "contentType": "text/plain"})
    assert up["size"] == len("blob ☃".encode()) and up["etag"]
    cj(p, "azblob_upload_blob", {"container": c, "blob": "two.bin", "contentBase64": base64.b64encode(b"\xff\x00" * 100).decode()})
    props = cj(p, "azblob_get_blob_properties", {"container": c, "blob": "d/one.txt"})["properties"]
    assert props["content-type"] == "text/plain" and props["content-length"] == str(len("blob ☃".encode()))
    got = cj(p, "azblob_get_blob", {"container": c, "blob": "d/one.txt"})
    assert got["body"] == "blob ☃" and got["truncated"] is False
    part = cj(p, "azblob_get_blob", {"container": c, "blob": "two.bin", "maxBytes": 16})
    assert part["encoding"] == "base64" and part["truncated"] is True and part["size"] == 200
    flat = cj(p, "azblob_list_blobs", {"container": c})
    assert {b["Name"] for b in flat["blobs"]} == {"d/one.txt", "two.bin"}
    tree = cj(p, "azblob_list_blobs", {"container": c, "delimiter": "/"})
    assert [b["Name"] for b in tree["prefixes"]] == ["d/"] and [b["Name"] for b in tree["blobs"]] == ["two.bin"]
    # the Blob REST frontend (static bearer token) sees the same container, and can write into it
    base = f"/devstoreaccount1/{c}"
    r = rest(warp, "WARP_AZBLOBWIRE_PORT", f"{base}/d/one.txt", headers={"Authorization": f"Bearer {AZ_TOKEN}", "x-ms-version": "2021-08-06"})
    assert r.status_code == 200 and r.content == "blob ☃".encode()
    r = rest(warp, "WARP_AZBLOBWIRE_PORT", f"{base}/wire.txt", "PUT", data=b"wired",
             headers={"Authorization": f"Bearer {AZ_TOKEN}", "x-ms-version": "2021-08-06", "x-ms-blob-type": "BlockBlob"})
    assert r.status_code == 201
    assert cj(p, "azblob_get_blob", {"container": c, "blob": "wire.txt"})["body"] == "wired"
    assert "BlobNotFound" in err(p, "azblob_get_blob", {"container": c, "blob": "nope"})
    assert "InvalidResourceName" in err(p, "azblob_create_container", {"container": "A"}) or "OutOfRange" in err(
        p, "azblob_create_container", {"container": "A"})
    assert cj(p, "azblob_delete_blob", {"container": c, "blob": "wire.txt"})["ok"] is True
    assert cj(p, "azblob_delete_container", {"container": c})["ok"] is True
    assert "account" in err(p, "azblob_list_blobs", {"container": c, "account": "nosuchacct"})


def test_azure_queue_tools(p):
    q = mst_uniq("mstq")
    assert cj(p, "azqueue_create_queue", {"queue": q})["ok"] is True
    assert q in [x["Name"] for x in cj(p, "azqueue_list_queues", {"prefix": "mstq"})["queues"]]
    for i in range(3):
        sent = cj(p, "azqueue_send_message", {"queue": q, "message": f"m<{i}>&"})
        assert sent["messages"][0]["MessageId"]
    assert cj(p, "azqueue_get_queue_metadata", {"queue": q})["approximateMessagesCount"] == "3"
    peek = cj(p, "azqueue_peek_messages", {"queue": q, "maxMessages": 2})["messages"]
    assert [m["MessageText"] for m in peek] == ["m<0>&", "m<1>&"]
    got = cj(p, "azqueue_receive_messages", {"queue": q, "maxMessages": 1, "visibilityTimeout": 60})["messages"]
    assert got[0]["MessageText"] == "m<0>&" and got[0]["PopReceipt"] and got[0]["DequeueCount"] == "1"
    assert cj(p, "azqueue_delete_message", {"queue": q, "messageId": got[0]["MessageId"], "popReceipt": got[0]["PopReceipt"]})["ok"]
    assert "MessageNotFound" in err(p, "azqueue_delete_message", {"queue": q, "messageId": got[0]["MessageId"],
                                                                    "popReceipt": got[0]["PopReceipt"]}) or "PopReceipt" in err(
        p, "azqueue_delete_message", {"queue": q, "messageId": got[0]["MessageId"], "popReceipt": got[0]["PopReceipt"]})
    assert cj(p, "azqueue_clear_messages", {"queue": q})["ok"] is True
    assert cj(p, "azqueue_peek_messages", {"queue": q})["messages"] == []
    assert cj(p, "azqueue_delete_queue", {"queue": q})["ok"] is True
    assert "QueueNotFound" in err(p, "azqueue_send_message", {"queue": q, "message": "x"})


def test_azure_table_tools(p):
    t = mst_uniq("mstt")
    assert cj(p, "aztable_create_table", {"table": t})["ok"] is True
    assert {"TableName": t} in cj(p, "aztable_list_tables")["tables"]
    for i in range(5):
        cj(p, "aztable_insert_entity", {"table": t, "partitionKey": "pk", "rowKey": f"r{i}", "entity": {"n": i, "name": f"x{i}", "ok": i % 2 == 0}})
    assert "EntityAlreadyExists" in err(p, "aztable_insert_entity", {"table": t, "partitionKey": "pk", "rowKey": "r0", "entity": {}})
    e = cj(p, "aztable_get_entity", {"table": t, "partitionKey": "pk", "rowKey": "r3"})["entity"]
    assert e["n"] == 3 and e["name"] == "x3" and e["ok"] is False
    q = cj(p, "aztable_query_entities", {"table": t, "filter": "n ge 2 and ok eq true", "select": "RowKey,n"})
    assert sorted(x["RowKey"] for x in q["entities"]) == ["r2", "r4"] and "name" not in q["entities"][0]
    page1 = cj(p, "aztable_query_entities", {"table": t, "top": 2})
    assert page1["count"] == 2 and page1["continuation"]
    page2 = cj(p, "aztable_query_entities", {"table": t, "top": 10, **{"nextPartitionKey": page1["continuation"]["nextPartitionKey"],
                                                                                "nextRowKey": page1["continuation"]["nextRowKey"]}})
    assert page2["count"] == 3
    cj(p, "aztable_upsert_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1", "entity": {"extra": "y"}, "mode": "merge"})
    e = cj(p, "aztable_get_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1"})["entity"]
    assert e["extra"] == "y" and e["n"] == 1
    cj(p, "aztable_upsert_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1", "entity": {"only": 1}})
    e = cj(p, "aztable_get_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1"})["entity"]
    assert e["only"] == 1 and "n" not in e
    assert cj(p, "aztable_delete_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1"})["ok"] is True
    assert "ResourceNotFound" in err(p, "aztable_get_entity", {"table": t, "partitionKey": "pk", "rowKey": "r1"})
    assert cj(p, "aztable_delete_table", {"table": t})["ok"] is True
    assert "TableNotFound" in err(p, "aztable_query_entities", {"table": t}) or "ResourceNotFound" in err(p, "aztable_query_entities", {"table": t})


# ----------------------------------------------------------------------------------------------------------- aws

def test_sns_topics_subscriptions_and_publish(p, warp):
    name = mst_uniq("mst-topic-")
    arn = cj(p, "sns_create_topic", {"name": name})["TopicArn"]
    assert arn.endswith(":" + name)
    assert arn in [t["TopicArn"] for t in cj(p, "sns_list_topics")["Topics"]]
    cj(p, "sns_set_topic_attributes", {"topicArn": arn, "attributeName": "DisplayName", "attributeValue": "MST"})
    assert cj(p, "sns_get_topic_attributes", {"topicArn": arn})["Attributes"]["DisplayName"] == "MST"
    sub = cj(p, "sns_subscribe", {"topicArn": arn, "protocol": "email", "endpoint": "a@example.com"})["SubscriptionArn"]
    assert sub.startswith(arn)
    assert sub in [s["SubscriptionArn"] for s in cj(p, "sns_list_subscriptions", {"topicArn": arn})["Subscriptions"]]
    assert cj(p, "sns_publish", {"topicArn": arn, "message": "hello", "subject": "s", "messageAttributes": {"k": "v", "n": 3}})["MessageId"]
    assert "NotFound" in err(p, "sns_publish", {"topicArn": arn + "x", "message": "m"})
    # the SNS SDK sees the topic created by the tool
    assert arn in [t["TopicArn"] for t in aws(warp, "sns").list_topics()["Topics"]]
    assert cj(p, "sns_unsubscribe", {"subscriptionArn": sub}) is not None
    cj(p, "sns_delete_topic", {"topicArn": arn})
    assert arn not in [t["TopicArn"] for t in cj(p, "sns_list_topics")["Topics"]]


def test_kinesis_streams_and_records(p, warp):
    s = mst_uniq("mst-stream-")
    cj(p, "kinesis_create_stream", {"streamName": s, "shardCount": 1})
    assert s in cj(p, "kinesis_list_streams")["StreamNames"]
    deadline = time.time() + 20
    while cj(p, "kinesis_describe_stream_summary", {"streamName": s})["StreamDescriptionSummary"]["StreamStatus"] != "ACTIVE":
        assert time.time() < deadline
        time.sleep(0.3)
    shard = cj(p, "kinesis_list_shards", {"streamName": s})["Shards"][0]["ShardId"]
    put = cj(p, "kinesis_put_record", {"streamName": s, "partitionKey": "pk", "data": "rec-1 ☃"})
    assert put["ShardId"] == shard and put["SequenceNumber"]
    cj(p, "kinesis_put_records", {"streamName": s, "records": [{"partitionKey": "a", "data": "rec-2"},
                                                                        {"partitionKey": "b", "dataBase64": base64.b64encode(b"\xff\xfe").decode()}]})
    got = cj(p, "kinesis_get_records", {"streamName": s, "shardId": shard})["records"]
    assert [r.get("body") for r in got][:2] == ["rec-1 ☃", "rec-2"] and got[2]["encoding"] == "base64"
    one = cj(p, "kinesis_get_records", {"streamName": s, "shardId": shard, "limit": 1})
    assert len(one["records"]) == 1 and one["nextShardIterator"]
    after = cj(p, "kinesis_get_records", {"streamName": s, "shardId": shard, "iteratorType": "AFTER_SEQUENCE_NUMBER",
                                                 "startingSequenceNumber": put["SequenceNumber"]})
    assert [r["body"] for r in after["records"]][0] == "rec-2"
    # the Kinesis SDK reads what the tool wrote
    k = aws(warp, "kinesis")
    it = k.get_shard_iterator(StreamName=s, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    assert k.get_records(ShardIterator=it)["Records"][0]["Data"] == "rec-1 ☃".encode()
    cj(p, "kinesis_delete_stream", {"streamName": s})
    assert "ResourceNotFound" in err(p, "kinesis_describe_stream_summary", {"streamName": s})


def test_secrets_ssm_kms_sts_and_key_material_never_returned(p, warp):
    name = mst_uniq("mst/secret-")
    created = cj(p, "secrets_create_secret", {"name": name, "secretString": "s3cr3t", "description": "d"})
    assert created["Name"] == name
    d = cj(p, "secrets_describe_secret", {"secretId": name})
    assert d["Description"] == "d" and "SecretString" not in json.dumps(d)
    assert name in [s["Name"] for s in cj(p, "secrets_list_secrets")["SecretList"]]
    assert cj(p, "secrets_get_secret_value", {"secretId": name})["SecretString"] == "s3cr3t"
    cj(p, "secrets_put_secret_value", {"secretId": name, "secretString": "v2"})
    assert cj(p, "secrets_get_secret_value", {"secretId": name})["SecretString"] == "v2"
    assert aws(warp, "secretsmanager").get_secret_value(SecretId=name)["SecretString"] == "v2"     # the SDK sees the tool's write
    assert cj(p, "secrets_delete_secret", {"secretId": name, "forceDelete": True})["Name"] == name
    param = mst_uniq("/mst/app/")
    cj(p, "ssm_put_parameter", {"name": param + "/plain", "value": "pv"})
    cj(p, "ssm_put_parameter", {"name": param + "/secure", "value": "sv", "type": "SecureString"})
    assert cj(p, "ssm_get_parameter", {"name": param + "/plain"})["Parameter"]["Value"] == "pv"
    assert cj(p, "ssm_get_parameter", {"name": param + "/secure", "withDecryption": True})["Parameter"]["Value"] == "sv"
    assert cj(p, "ssm_get_parameter", {"name": param + "/secure"})["Parameter"]["Value"] != "sv"
    by_path = cj(p, "ssm_get_parameters_by_path", {"path": param, "withDecryption": True})["Parameters"]
    assert {x["Name"]: x["Value"] for x in by_path} == {param + "/plain": "pv", param + "/secure": "sv"}
    assert {x["Name"] for x in cj(p, "ssm_describe_parameters")["Parameters"]} >= {param + "/plain"}
    assert aws(warp, "ssm").get_parameter(Name=param + "/plain")["Parameter"]["Value"] == "pv"
    cj(p, "ssm_delete_parameter", {"name": param + "/plain"})
    assert "ParameterNotFound" in err(p, "ssm_get_parameter", {"name": param + "/plain"})
    # KMS: metadata only, ciphertext round trip, no key material anywhere
    key = cj(p, "kms_create_key", {"description": "mst key"})
    assert set(key) == {"KeyMetadata"}
    key_id = key["KeyMetadata"]["KeyId"]
    text = json.dumps([key, cj(p, "kms_describe_key", {"keyId": key_id}), cj(p, "kms_list_keys"),
                       cj(p, "kms_list_aliases")]).lower()
    for forbidden in ("plaintext", "keymaterial", "privatekey", "publickey", "ciphertextblob"):
        assert forbidden not in text
    enc = cj(p, "kms_encrypt", {"keyId": key_id, "plaintext": "top secret ☃"})
    assert enc["ciphertextBase64"] and "top secret" not in json.dumps(enc)
    dec = cj(p, "kms_decrypt", {"ciphertextBase64": enc["ciphertextBase64"]})
    assert dec["body"] == "top secret ☃"
    assert aws(warp, "kms").decrypt(CiphertextBlob=base64.b64decode(enc["ciphertextBase64"]))["Plaintext"] == "top secret ☃".encode()
    ident = cj(p, "sts_get_caller_identity")
    assert ident["Account"] and ident["Arn"].startswith("arn:aws:")
    names = tnames(p)
    assert not any(n.startswith("kms_") and ("data_key" in n or "export" in n or "get_public" in n or "import" in n) for n in names)


# ----------------------------------------------------------------------------------------------------------- google

def test_pubsub_topics_publish_pull_ack_and_rest_visibility(p, warp):
    t, s = mst_uniq("mst-topic-"), mst_uniq("mst-sub-")
    assert cj(p, "pubsub_create_topic", {"topic": t})["name"] == f"projects/mst-project/topics/{t}"
    assert f"projects/mst-project/topics/{t}" in [x["name"] for x in cj(p, "pubsub_list_topics")["topics"]]
    sub = cj(p, "pubsub_create_subscription", {"subscription": s, "topic": t, "ackDeadlineSeconds": 30})
    assert sub["topic"].endswith(t) and sub["ackDeadlineSeconds"] == 30
    assert cj(p, "pubsub_list_topic_subscriptions", {"topic": t})["subscriptions"] == [f"projects/mst-project/subscriptions/{s}"]
    ids = cj(p, "pubsub_publish", {"topic": t, "data": "hi ☃", "attributes": {"k": "v"}})["messageIds"]
    assert len(ids) == 1
    cj(p, "pubsub_publish", {"topic": t, "messages": [{"data": "b"}, {"dataBase64": base64.b64encode(b"\xff").decode()}]})
    pulled = cj(p, "pubsub_pull", {"subscription": s, "maxMessages": 10})
    assert pulled["count"] == 3
    first = pulled["messages"][0]
    assert first["body"] == "hi ☃" and first["attributes"] == {"k": "v"} and first["messageId"] == ids[0] and first["ackId"]
    assert pulled["messages"][2]["encoding"] == "base64"
    assert cj(p, "pubsub_pull", {"subscription": s})["count"] == 0          # leased, nothing more to pull
    cj(p, "pubsub_modify_ack_deadline", {"subscription": s, "ackIds": [first["ackId"]], "ackDeadlineSeconds": 60})
    cj(p, "pubsub_ack", {"subscription": s, "ackIds": [m["ackId"] for m in pulled["messages"]]})
    # the REST frontend publishes into the tool's subscription and reads its topic
    r = rest(warp, "WARP_PUBSUBWIRE_REST_PORT", f"/v1/projects/mst-project/topics/{t}:publish", "POST",
             json={"messages": [{"data": base64.b64encode(b"via rest").decode()}]})
    assert r.status_code == 200
    assert cj(p, "pubsub_pull", {"subscription": s})["messages"][0]["body"] == "via rest"
    assert rest(warp, "WARP_PUBSUBWIRE_REST_PORT", f"/v1/projects/mst-project/topics/{t}").json()["name"].endswith(t)
    assert "ALREADY_EXISTS" in err(p, "pubsub_create_topic", {"topic": t})
    cj(p, "pubsub_delete_subscription", {"subscription": s})
    cj(p, "pubsub_delete_topic", {"topic": t})
    assert "NOT_FOUND" in err(p, "pubsub_get_topic", {"topic": t})


def test_firestore_documents_queries_and_rest_visibility(p, warp):
    c = mst_uniq("mstcol")
    added = cj(p, "firestore_add_document", {"collection": c, "documentId": "alice", "data": {
        "name": "Alice", "age": 30, "score": 1.5, "vip": True, "tags": ["a", "b"], "addr": {"city": "Paris"}, "none": None}})
    assert added["path"] == f"{c}/alice" and added["data"]["addr"] == {"city": "Paris"} and added["data"]["age"] == 30
    for name, age in (("bob", 25), ("carol", 41), ("dave", 30)):
        cj(p, "firestore_add_document", {"collection": c, "documentId": name, "data": {"name": name, "age": age}})
    got = cj(p, "firestore_get_documents", {"paths": [f"{c}/alice", f"{c}/nobody"]})
    assert [d["id"] for d in got["documents"]] == ["alice"] and got["missing"] == [f"{c}/nobody"]
    assert got["documents"][0]["data"]["tags"] == ["a", "b"] and got["documents"][0]["data"]["none"] is None
    q = cj(p, "firestore_query_collection", {"collection": c, "where": [{"field": "age", "op": ">=", "value": 30}],
                                                    "orderBy": [{"field": "age", "direction": "DESCENDING"}], "limit": 2})
    assert len(q["documents"]) == 2 and q["documents"][0]["id"] == "carol" and q["documents"][1]["data"]["age"] == 30
    assert cj(p, "firestore_count", {"collection": c, "where": [{"field": "age", "op": "==", "value": 30}]})["count"] == 2
    assert cj(p, "firestore_count", {"collection": c})["count"] == 4
    cj(p, "firestore_update_document", {"path": f"{c}/bob", "data": {"age": 26}})
    assert cj(p, "firestore_get_documents", {"paths": [f"{c}/bob"]})["documents"][0]["data"] == {"name": "bob", "age": 26}
    cj(p, "firestore_set_document", {"path": f"{c}/bob", "data": {"city": "Rome"}, "merge": True})
    assert cj(p, "firestore_get_documents", {"paths": [f"{c}/bob"]})["documents"][0]["data"] == {"name": "bob", "age": 26, "city": "Rome"}
    cj(p, "firestore_set_document", {"path": f"{c}/bob", "data": {"only": 1}})
    assert cj(p, "firestore_get_documents", {"paths": [f"{c}/bob"]})["documents"][0]["data"] == {"only": 1}
    assert "NOT_FOUND" in err(p, "firestore_update_document", {"path": f"{c}/ghost", "data": {"a": 1}})
    cj(p, "firestore_add_document", {"collection": f"{c}/alice/orders", "data": {"item": "book"}})
    assert cj(p, "firestore_list_collections", {"documentPath": f"{c}/alice"})["collectionIds"] == ["orders"]
    assert c in cj(p, "firestore_list_collections")["collectionIds"]
    listed = cj(p, "firestore_list_documents", {"collection": c, "pageSize": 2})
    assert len(listed["documents"]) == 2 and listed["nextPageToken"]
    raw = cj(p, "firestore_run_query", {"structuredQuery": {"from": [{"collectionId": c}], "limit": 1}})
    assert raw["count"] == 1
    # REST frontend: sees the documents the tools wrote, and its writes show up in the tools
    doc = rest(warp, "WARP_FIRESTOREWIRE_PORT", f"/v1/projects/mst-project/databases/(default)/documents/{c}/alice").json()
    assert doc["fields"]["name"]["stringValue"] == "Alice" and doc["fields"]["age"]["integerValue"] == "30"
    rest(warp, "WARP_FIRESTOREWIRE_PORT", f"/v1/projects/mst-project/databases/(default)/documents/{c}/erin", "PATCH",
         json={"fields": {"name": {"stringValue": "Erin"}}}).raise_for_status()
    assert cj(p, "firestore_get_documents", {"paths": [f"{c}/erin"]})["documents"][0]["data"] == {"name": "Erin"}
    assert cj(p, "firestore_delete_document", {"path": f"{c}/erin"})["ok"] is True
    assert cj(p, "firestore_get_documents", {"paths": [f"{c}/erin"]})["documents"] == []
    assert "operator" in err(p, "firestore_query_collection", {"collection": c, "where": [{"field": "a", "op": "~", "value": 1}]})


def test_datastore_entities_queries_gql_and_rest_visibility(p, warp):
    kind = mst_uniq("MstKind")
    put = cj(p, "datastore_upsert_entity", {"kind": kind, "name": "t1", "properties": {
        "title": "one", "done": False, "n": 1, "tags": ["x"], "nested": {"a": 1}}})
    assert put["key"] == {"kind": kind, "name": "t1"}
    cj(p, "datastore_upsert_entity", {"kind": kind, "name": "t2", "properties": {"title": "two", "done": True, "n": 2}})
    alloc = cj(p, "datastore_upsert_entity", {"kind": kind, "properties": {"title": "auto", "done": False, "n": 3}})
    assert alloc["key"]["id"] and "name" not in alloc["key"]
    child = cj(p, "datastore_insert_entity", {"kind": "Child", "name": "c1", "ancestors": [{"kind": kind, "name": "t1"}],
                                                     "properties": {"v": 1}})
    assert child["key"]["ancestors"] == [{"kind": kind, "name": "t1"}]
    found = cj(p, "datastore_lookup", {"kind": kind, "name": "t1"})
    assert found["entities"][0]["properties"] == {"title": "one", "done": False, "n": 1, "tags": ["x"], "nested": {"a": 1}}
    many = cj(p, "datastore_lookup", {"keys": [{"kind": kind, "name": "t2"}, {"kind": kind, "name": "missing"}]})
    assert [e["key"]["name"] for e in many["entities"]] == ["t2"] and many["missing"][0]["name"] == "missing"
    q = cj(p, "datastore_run_query", {"kind": kind, "where": [{"property": "done", "op": "==", "value": False}],
                                             "orderBy": [{"property": "n", "direction": "DESCENDING"}]})
    assert [e["properties"]["title"] for e in q["entities"]] == ["auto", "one"]
    assert cj(p, "datastore_count", {"kind": kind})["count"] == 3
    assert cj(p, "datastore_count", {"kind": kind, "where": [{"property": "n", "op": ">=", "value": 2}]})["count"] == 2
    keys = cj(p, "datastore_run_query", {"kind": kind, "keysOnly": True})["entities"]
    assert len(keys) == 3 and all(e["properties"] == {} for e in keys)
    gql = cj(p, "datastore_run_query", {"gql": f"SELECT * FROM {kind} WHERE n > 1 ORDER BY n"})
    assert [e["properties"]["n"] for e in gql["entities"]] == [2, 3]
    anc = cj(p, "datastore_run_query", {"kind": "Child", "ancestor": [{"kind": kind, "name": "t1"}]})
    assert [e["key"]["name"] for e in anc["entities"]] == ["c1"]
    assert "ALREADY_EXISTS" in err(p, "datastore_insert_entity", {"kind": kind, "name": "t1", "properties": {"a": 1}})
    # the Datastore REST frontend sees the same entity, and writes there are visible to the tools
    r = rest(warp, "WARP_DATASTOREWIRE_PORT", "/v1/projects/mst-project:lookup", "POST",
             json={"keys": [{"path": [{"kind": kind, "name": "t2"}]}]}).json()
    assert r["found"][0]["entity"]["properties"]["title"]["stringValue"] == "two"
    rest(warp, "WARP_DATASTOREWIRE_PORT", "/v1/projects/mst-project:commit", "POST", json={"mode": "NON_TRANSACTIONAL", "mutations": [
        {"upsert": {"key": {"path": [{"kind": kind, "name": "wired"}]}, "properties": {"title": {"stringValue": "w"}}}}]}).raise_for_status()
    assert cj(p, "datastore_lookup", {"kind": kind, "name": "wired"})["entities"][0]["properties"] == {"title": "w"}
    assert cj(p, "datastore_delete_entity", {"kind": kind, "name": "wired"})["ok"] is True
    assert cj(p, "datastore_lookup", {"kind": kind, "name": "wired"})["entities"] == []


def test_bigtable_tables_rows_filters_counters(p):
    t = mst_uniq("mstbt")
    cj(p, "bigtable_create_table", {"table": t, "families": {"cf": {"maxVersions": 2}, "meta": {}}})
    assert any(x["name"].endswith("/tables/" + t) for x in cj(p, "bigtable_list_tables")["tables"])
    fam = cj(p, "bigtable_get_table", {"table": t})["columnFamilies"]
    assert set(fam) == {"cf", "meta"} and fam["cf"]["gcRule"]["maxNumVersions"] == 2
    cj(p, "bigtable_mutate_row", {"table": t, "rowKey": "user#1", "set": [
        {"family": "cf", "qualifier": "name", "value": "Ann ☃"}, {"family": "cf", "qualifier": "city", "value": "Oslo"},
        {"family": "meta", "qualifier": "src", "value": "mcp"}]})
    r = cj(p, "bigtable_mutate_rows", {"table": t, "entries": [
        {"rowKey": f"user#{i}", "set": [{"family": "cf", "qualifier": "name", "value": f"U{i}"}]} for i in range(2, 6)]})
    assert r == {"entries": 4, "failed": 0}
    row = cj(p, "bigtable_read_row", {"table": t, "rowKey": "user#1"})
    assert row["found"] is True and {(c["family"], c["qualifier"]): c["value"] for c in row["row"]["cells"]} == {
        ("cf", "name"): "Ann ☃", ("cf", "city"): "Oslo", ("meta", "src"): "mcp"}
    assert cj(p, "bigtable_read_row", {"table": t, "rowKey": "nope"}) == {"found": False, "row": None}
    pre = cj(p, "bigtable_read_rows", {"table": t, "prefix": "user#"})
    assert [x["rowKey"] for x in pre["rows"]] == [f"user#{i}" for i in range(1, 6)] and pre["truncated"] is False
    lim = cj(p, "bigtable_read_rows", {"table": t, "prefix": "user#", "limit": 2})
    assert lim["count"] == 2 and lim["truncated"] is True
    rng = cj(p, "bigtable_read_rows", {"table": t, "startKey": "user#2", "endKey": "user#4", "reversed": True})
    assert [x["rowKey"] for x in rng["rows"]] == ["user#3", "user#2"]
    keys = cj(p, "bigtable_read_rows", {"table": t, "rowKeys": ["user#5", "user#1"], "family": "meta"})
    assert [x["rowKey"] for x in keys["rows"]] == ["user#1"]
    latest = cj(p, "bigtable_read_rows", {"table": t, "prefix": "user#1", "qualifier": "city", "latestOnly": True})
    assert [c["value"] for c in latest["rows"][0]["cells"]] == ["Oslo"]
    cj(p, "bigtable_mutate_row", {"table": t, "rowKey": "user#1", "deleteColumns": [{"family": "cf", "qualifier": "city"}]})
    cells = cj(p, "bigtable_read_row", {"table": t, "rowKey": "user#1"})["row"]["cells"]
    assert "city" not in [c["qualifier"] for c in cells]
    assert cj(p, "bigtable_increment", {"table": t, "rowKey": "ctr", "family": "cf", "qualifier": "n", "by": 5})["value"] == 5
    assert cj(p, "bigtable_increment", {"table": t, "rowKey": "ctr", "family": "cf", "qualifier": "n"})["value"] == 6
    cj(p, "bigtable_delete_row", {"table": t, "rowKey": "user#5"})
    assert cj(p, "bigtable_read_row", {"table": t, "rowKey": "user#5"})["found"] is False
    cj(p, "bigtable_drop_row_range", {"table": t, "prefix": "user#"})
    assert [x["rowKey"] for x in cj(p, "bigtable_read_rows", {"table": t})["rows"]] == ["ctr"]
    assert "NOT_FOUND" in err(p, "bigtable_get_table", {"table": t + "x"})
    cj(p, "bigtable_delete_table", {"table": t})
    assert "NOT_FOUND" in err(p, "bigtable_read_rows", {"table": t})


# ----------------------------------------------------------------------------------------------------------- read only

def test_read_only_endpoint_hides_and_refuses_every_write_tool(ro, warp, p):
    q = mst_default_ep(ro)
    names = tnames(q)
    for store in STORES:
        for t in READ_TOOLS[store]:
            assert t in names, t
        for t in WRITE_TOOLS[store]:
            assert t not in names, t
    # write tools are refused when called anyway; nothing is written
    key = mst_uniq("mst:ro:")
    for tool, args in (("redis_set", {"key": key, "value": "v"}), ("gcs_create_bucket", {"bucket": "mst-ro"}),
                       ("pubsub_publish", {"topic": "t", "data": "d"}), ("firestore_add_document", {"collection": "c", "data": {"a": 1}}),
                       ("datastore_upsert_entity", {"kind": "K", "name": "n", "properties": {}}),
                       ("bigtable_delete_table", {"table": "t"}), ("secrets_put_secret_value", {"secretId": "s", "secretString": "x"}),
                       ("sns_publish", {"topicArn": "a", "message": "m"}), ("kinesis_put_record", {"streamName": "s", "partitionKey": "k", "data": "d"}),
                       ("azblob_upload_blob", {"container": "c", "blob": "b", "content": "x"}), ("azqueue_send_message", {"queue": "q", "message": "m"}),
                       ("aztable_insert_entity", {"table": "t", "partitionKey": "p", "rowKey": "r", "entity": {}}),
                       ("pubsub_pull", {"subscription": "s"})):
        assert "Unknown tool" in err(q, tool, args), tool
    assert cj(p, "redis_get", {"key": key})["exists"] is False


def test_read_only_endpoint_still_reads_what_read_write_wrote(ro, warp, p):
    q = mst_default_ep(ro)
    k = mst_uniq("mst:ro:r:")
    cj(p, "redis_set", {"key": k, "value": "visible"})
    assert cj(q, "redis_get", {"key": k})["value"] == "visible"
    b = mst_uniq("mst-ro-b-")
    cj(p, "gcs_create_bucket", {"bucket": b})
    cj(p, "gcs_put_object", {"bucket": b, "object": "o", "content": "ro read"})
    assert cj(q, "gcs_get_object", {"bucket": b, "object": "o"})["body"] == "ro read"
    assert b in [x["name"] for x in cj(q, "gcs_list_buckets")["buckets"]]
    t = mst_uniq("mst-ro-t-")
    cj(p, "pubsub_create_topic", {"topic": t})
    assert cj(q, "pubsub_get_topic", {"topic": t})["name"].endswith(t)
    d = cj(q, "describe_backend", {"backend": "default.redis"})
    assert d["type"] == "redis"


def test_secret_values_are_refused_on_read_only_endpoints_but_metadata_is_not(ro, warp, p):
    q = mst_default_ep(ro)
    name = mst_uniq("mst/ro-secret-")
    cj(p, "secrets_create_secret", {"name": name, "secretString": "hunter2"})
    param = mst_uniq("/mst/ro/")
    cj(p, "ssm_put_parameter", {"name": param, "value": "sv", "type": "SecureString"})
    cj(p, "ssm_put_parameter", {"name": param + "p", "value": "plain"})
    key_id = cj(p, "kms_create_key", {})["KeyMetadata"]["KeyId"]
    blob = cj(p, "kms_encrypt", {"keyId": key_id, "plaintext": "classified"})["ciphertextBase64"]
    for tool, args in (("secrets_get_secret_value", {"secretId": name}),
                       ("ssm_get_parameter", {"name": param, "withDecryption": True}),
                       ("ssm_get_parameters_by_path", {"path": param, "withDecryption": True}),
                       ("kms_decrypt", {"ciphertextBase64": blob})):
        msg = err(q, tool, args)
        assert "42501" in msg and "read-only" in msg, (tool, msg)
        assert "hunter2" not in msg and "classified" not in msg
    assert cj(q, "secrets_describe_secret", {"secretId": name})["Name"] == name
    assert name in [s["Name"] for s in cj(q, "secrets_list_secrets")["SecretList"]]
    assert cj(q, "ssm_get_parameter", {"name": param + "p"})["Parameter"]["Value"] == "plain"
    assert cj(q, "kms_describe_key", {"keyId": key_id})["KeyMetadata"]["KeyId"] == key_id
    assert cj(q, "kms_encrypt", {"keyId": key_id, "plaintext": "x"})["ciphertextBase64"]   # encrypt never reveals anything
    assert cj(q, "sts_get_caller_identity")["Account"]


def test_operator_can_allow_secret_reads_while_writes_stay_blocked(ro_allow, warp, p):
    q = mst_default_ep(ro_allow)
    name = mst_uniq("mst/allow-secret-")
    cj(p, "secrets_create_secret", {"name": name, "secretString": "allowed"})
    assert cj(q, "secrets_get_secret_value", {"secretId": name})["SecretString"] == "allowed"
    key_id = cj(p, "kms_create_key", {})["KeyMetadata"]["KeyId"]
    blob = cj(p, "kms_encrypt", {"keyId": key_id, "plaintext": "ok"})["ciphertextBase64"]
    assert cj(q, "kms_decrypt", {"ciphertextBase64": blob})["body"] == "ok"
    assert "Unknown tool" in err(q, "secrets_put_secret_value", {"secretId": name, "secretString": "nope"})


def test_read_only_backend_set_endpoint_lists_the_read_tool_sets_per_type(ro):
    ep = create_endpoint(ro, "ro-set", "group:hosting")
    names = tool_names(ro.frontend_port, ep["path"], ep["token"])
    for store in STORES:
        assert set(READ_TOOLS[store]) <= names and not set(WRITE_TOOLS[store]) & names, store
    ep2 = create_endpoint(ro, "ro-pg2", "db:pg2")
    names2 = tool_names(ro.frontend_port, ep2["path"], ep2["token"])
    assert "redis_get" in names2 and "redis_set" not in names2 and "sns_list_topics" not in names2 and "gcs_get_object" in names2


# ----------------------------------------------------------------------------------------------------------- endpoints

def test_endpoint_expiry_applies_to_store_tools(warp, port):
    ep = create_endpoint(warp, "short", "db:default", ttlSeconds=6)
    k = mst_uniq("mst:exp:")
    assert cj(port, "redis_set", {"key": k, "value": "v"}, path=ep["path"], token=ep["token"])["ok"] is True
    assert cj(port, "redis_get", {"key": k}, path=ep["path"], token=ep["token"])["value"] == "v"
    deadline = time.time() + 20
    while time.time() < deadline:
        r = requests.post(f"http://localhost:{port}{ep['path']}", timeout=10, headers={"Authorization": f"Bearer {ep['token']}"},
                          json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "redis_get", "arguments": {"key": k}}})
        if r.status_code == 401:
            break
        time.sleep(0.3)
    else:
        pytest.fail("endpoint kept serving store tools after its expiry")
    assert "expired" not in r.text.lower() or "not valid" in r.text     # the generic, non-leaky message
    # extending the endpoint brings it back
    mst_api_patch = admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"ttlSeconds": 3600})
    assert mst_api_patch.status_code == 200
    deadline = time.time() + 15
    while time.time() < deadline:
        r = requests.post(f"http://localhost:{port}{ep['path']}", timeout=10, headers={"Authorization": f"Bearer {ep['token']}"},
                          json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        if r.status_code == 200:
            break
        time.sleep(0.3)
    assert r.status_code == 200


def test_backend_argument_routes_between_hosts_of_the_same_store_type(warp, port):
    # redis is hosted on default and pg2: a tool call may name either host's typed store and reaches the whole store
    k = mst_uniq("mst:route:")
    ep = create_endpoint(warp, "set-both", "group:hosting")
    cj(port, "redis_set", {"key": k, "value": "v", "backend": "pg2.redis"}, path=ep["path"], token=ep["token"])
    assert cj(port, "redis_get", {"key": k, "backend": "default.redis"}, path=ep["path"], token=ep["token"])["value"] == "v"
    msg = err(port, "redis_get", {"key": k}, path=ep["path"], token=ep["token"])
    assert "backend" in msg and "default.redis" in msg and "pg2.redis" in msg      # two candidates: the name is required
    msg = err(port, "redis_get", {"key": k, "backend": "default.gcs"}, path=ep["path"], token=ep["token"])
    assert "does not apply" in msg
    only_pg2 = create_endpoint(warp, "pg2-redis", "db:pg2")
    assert cj(port, "redis_get", {"key": k}, path=only_pg2["path"], token=only_pg2["token"])["value"] == "v"


def test_store_tool_calls_are_counted_in_the_mcp_and_operation_metrics(warp, p):
    k = mst_uniq("mst:metrics:")
    cj(p, "redis_set", {"key": k, "value": "v"})
    cj(p, "redis_get", {"key": k})
    text = warp.metrics_text()
    assert 'tool="redis_set"' in text and 'tool="redis_get"' in text
    assert "mcp-redis" in text            # the per-operation metrics of the shared collector carry the MCP store protocol


def test_data_written_through_tools_lands_in_the_postgres_hosts(warp, p, pgs):
    k = mst_uniq("mst:pg:")
    cj(p, "redis_set", {"key": k, "value": "in postgres"})
    found = 0
    for pg in pgs:
        with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
            cur = c.cursor()
            cur.execute("SELECT count(*) FROM warp_redis_keys WHERE k = %s", (k.encode(),))
            found += cur.fetchone()[0]
    assert found == 1
