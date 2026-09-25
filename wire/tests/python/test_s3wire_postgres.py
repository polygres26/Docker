"""s3wire in POSTGRES mode: objects are stored IN the Postgres backends of a backend set (the `s3` store),
sharded by hash(bucket + "/" + key), with no MinIO / external S3 involved.

One real Warp subprocess (JVM heap capped at 384 MiB so any whole-object buffering would fail loudly and
the Postgres pool limited to 4 connections so any pooled-connection pinning would starve), two real Postgres
servers (`default` and `pg2`) in the same backend set with the s3 store enabled on both through the admin
API, a real boto3 client, and psycopg2 looking straight into each Postgres to prove where rows and chunks
landed. Set WARP_TEST_PG_LOCAL=1 to use native Postgres instead of Docker.

How to start s3wire in Postgres mode with no MinIO (what other suites need): enable the `s3` store on a
Postgres backend (PATCH /api/backend-sets/<set>/backends/<name> {"enabledStores": ["s3"]} or
WARP_BACKEND_STORES="default=s3") and set WARP_S3WIRE_CREDENTIALS=key=secret. Because the store is enabled
AFTER startup here, WARP_S3WIRE_ENABLED=true starts the listener up front.
"""
import base64
import hashlib
import json
import os
import random
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import boto3
import psycopg2
import pytest
import requests
from boto3.s3.transfer import TransferConfig
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.client import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from warp_test_support import RealPostgres, WarpProcess, isolated_ports

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

KEY = "s3pg-client-key"
SECRET = "s3pg-client-secret-value"
MIB = 1024 * 1024
CHUNK = 4 * MIB


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def warp(pgs):
    ports = isolated_ports("WARP_S3WIRE_PORT")
    mcp_port = ports["WARP_MCP_PORT"]
    proc = WarpProcess(pgs[0], "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env={
        **ports,
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_S3WIRE_ENABLED": "true",
        "WARP_S3WIRE_CREDENTIALS": f"{KEY}={SECRET}",
        "WARP_POOL_MAX_SIZE": "4",
        "JAVA_TOOL_OPTIONS": "-Xmx384m",
        "WARP_S3WIRE_GC_INTERVAL_SECONDS": "1",
        "WARP_S3WIRE_GC_GRACE_SECONDS": "3",
        "WARP_S3WIRE_GC_UPLOADING_AGE_SECONDS": "3600",
        "WARP_S3WIRE_BUCKET_CACHE_MILLIS": "0",
    })

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["s3"]})
    added = api("POST", "/api/backend-sets/default/backends", {
        "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
        "enabledStores": ["s3"]}, expect=201)
    assert {r["store"] for r in added["rebalanceRequired"]} == {"s3"}
    proc.api = api
    proc.mcp_port = mcp_port
    yield proc
    proc.close()


def make_client(warp, key=KEY, secret=SECRET, retries=0):
    return boto3.client("s3", endpoint_url=f"http://localhost:{warp.frontend_port}", region_name="us-east-1",
                        aws_access_key_id=key, aws_secret_access_key=secret,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      retries={"max_attempts": retries}, max_pool_connections=64))


@pytest.fixture(scope="module")
def s3(warp):
    return make_client(warp)


_counter = [0]


def new_bucket(s3):
    _counter[0] += 1
    name = f"pgb-{_counter[0]}-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=name)
    return name


@pytest.fixture()
def bucket(s3):
    return new_bucket(s3)


def where(pgs, bucket, key):
    """Indexes of the Postgres servers that hold an object row for (bucket, key)."""
    out = []
    for i, pg in enumerate(pgs):
        if sql(pg, "SELECT 1 FROM warp_s3_objects WHERE bucket=%s AND key=%s", (bucket, key)):
            out.append(i)
    return out


def chunk_count(pg, bucket, key):
    return sql(pg, "SELECT count(*) FROM warp_s3_chunks c WHERE c.object_id IN "
                   "(SELECT object_id FROM warp_s3_objects WHERE bucket=%s AND key=%s UNION "
                   " SELECT (s->>'id')::uuid FROM warp_s3_objects o, jsonb_array_elements(o.segments) s "
                   " WHERE o.bucket=%s AND o.key=%s)", (bucket, key, bucket, key))[0][0]


def md5(b):
    return hashlib.md5(b).hexdigest()


def err(e):
    return e.value.response["Error"]["Code"], e.value.response["ResponseMetadata"]["HTTPStatusCode"]


# ---------------------------------------------------------------------------------------------
# schema / enablement
# ---------------------------------------------------------------------------------------------

def test_store_is_offered_and_schema_created_on_both_hosts(warp, pgs):
    stores = warp.api("GET", "/api/backend-stores")["stores"]
    s3 = next(s for s in stores if s["id"] == "s3")
    assert s3["label"] == "S3" and s3["shardable"] is True and s3["setEnvVar"] == "WARP_S3WIRE_SET"
    for pg in pgs:
        tables = {r[0] for r in sql(pg, "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'warp_s3_%'")}
        assert tables == {"warp_s3_buckets", "warp_s3_objects", "warp_s3_blobs", "warp_s3_chunks",
                          "warp_s3_multipart_uploads", "warp_s3_parts",
                          "warp_s3_versions", "warp_s3_bucket_config", "warp_s3_annotations"}
        assert sql(pg, "SELECT 1 FROM warp_enabled_stores WHERE store='s3'")
    # COLLATE "C" keys: btree order == S3 UTF-8 byte order
    coll = sql(pgs[0], "SELECT collation_name FROM information_schema.columns WHERE table_name='warp_s3_objects' "
                       "AND column_name IN ('bucket','key')")
    assert [c[0] for c in coll] == ["C", "C"]


def test_bad_signature_rejected(warp, bucket):
    bad = make_client(warp, secret="wrong-secret")
    with pytest.raises(ClientError) as e:
        bad.list_buckets()
    assert err(e)[0] == "SignatureDoesNotMatch" and err(e)[1] == 403
    unknown = make_client(warp, key="nobody")
    with pytest.raises(ClientError) as e:
        unknown.put_object(Bucket=bucket, Key="x", Body=b"1")
    assert err(e)[1] == 403


# ---------------------------------------------------------------------------------------------
# buckets
# ---------------------------------------------------------------------------------------------

def test_bucket_lifecycle_and_errors(s3, pgs):
    b = new_bucket(s3)
    s3.head_bucket(Bucket=b)
    listed = {x["Name"]: x["CreationDate"] for x in s3.list_buckets()["Buckets"]}
    assert b in listed
    assert abs((datetime.now(timezone.utc) - listed[b]).total_seconds()) < 120
    with pytest.raises(ClientError) as e:
        s3.create_bucket(Bucket=b)
    assert err(e) == ("BucketAlreadyOwnedByYou", 409)
    with pytest.raises(ClientError) as e:
        s3.head_bucket(Bucket="does-not-exist-bucket")
    assert err(e)[1] == 404
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket="does-not-exist-bucket", Key="k", Body=b"x")
    assert err(e) == ("NoSuchBucket", 404)
    with pytest.raises(ClientError) as e:
        s3.list_objects_v2(Bucket="does-not-exist-bucket")
    assert err(e) == ("NoSuchBucket", 404)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket="does-not-exist-bucket", Key="k")
    assert err(e) == ("NoSuchBucket", 404)
    with pytest.raises(ClientError) as e:
        s3.create_bucket(Bucket="Bad_Bucket")
    assert err(e)[0] == "InvalidBucketName"
    assert s3.get_bucket_location(Bucket=b)["ResponseMetadata"]["HTTPStatusCode"] == 200
    # the catalog is only written on the FIRST host
    assert sql(pgs[0], "SELECT 1 FROM warp_s3_buckets WHERE name=%s", (b,))
    assert not sql(pgs[1], "SELECT 1 FROM warp_s3_buckets WHERE name=%s", (b,))
    s3.delete_bucket(Bucket=b)
    assert b not in [x["Name"] for x in s3.list_buckets()["Buckets"]]
    with pytest.raises(ClientError) as e:
        s3.delete_bucket(Bucket=b)
    assert err(e) == ("NoSuchBucket", 404)


def test_delete_bucket_requires_empty_on_all_shards(s3, pgs):
    b = new_bucket(s3)
    keys = [f"k{i}" for i in range(30)]
    for k in keys:
        s3.put_object(Bucket=b, Key=k, Body=b"x")
    shards = {where(pgs, b, k)[0] for k in keys}
    assert shards == {0, 1}
    for k in keys[:-1]:
        s3.delete_object(Bucket=b, Key=k)
    # exactly one object remains, on whichever shard owns it: still BucketNotEmpty
    with pytest.raises(ClientError) as e:
        s3.delete_bucket(Bucket=b)
    assert err(e) == ("BucketNotEmpty", 409)
    s3.delete_object(Bucket=b, Key=keys[-1])
    s3.delete_bucket(Bucket=b)


# ---------------------------------------------------------------------------------------------
# put / get / head / delete
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("size", [0, 1, MIB, 10 * MIB, 50 * MIB])
def test_put_get_byte_exact_and_chunks_on_owning_shard(s3, pgs, bucket, size):
    payload = os.urandom(size)
    key = f"size/{size}.bin"
    put = s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/x-test")
    assert put["ETag"] == f'"{md5(payload)}"'
    got = s3.get_object(Bucket=bucket, Key=key)
    body = got["Body"].read()
    assert len(body) == size and body == payload
    assert got["ContentLength"] == size and got["ETag"] == put["ETag"] and got["ContentType"] == "application/x-test"
    h = s3.head_object(Bucket=bucket, Key=key)
    assert h["ContentLength"] == size and h["ETag"] == put["ETag"]
    owners = where(pgs, bucket, key)
    assert len(owners) == 1, owners  # on exactly one shard
    other = 1 - owners[0]
    expected_chunks = (size + CHUNK - 1) // CHUNK
    assert chunk_count(pgs[owners[0]], bucket, key) == expected_chunks
    assert chunk_count(pgs[other], bucket, key) == 0
    if size:
        (stored,) = sql(pgs[owners[0]], "SELECT sum(length(data)) FROM warp_s3_chunks WHERE object_id="
                        "(SELECT object_id FROM warp_s3_objects WHERE bucket=%s AND key=%s)", (bucket, key))[0]
        assert stored == size


def test_objects_spread_across_both_backends(s3, pgs, bucket):
    n = 120
    for i in range(n):
        s3.put_object(Bucket=bucket, Key=f"spread/{i:04d}", Body=f"v{i}".encode())
    counts = [sql(pg, "SELECT count(*) FROM warp_s3_objects WHERE bucket=%s", (bucket,))[0][0] for pg in pgs]
    assert sum(counts) == n and min(counts) > 30, counts
    for i in (0, 33, 77, 119):
        assert len(where(pgs, bucket, f"spread/{i:04d}")) == 1
        assert s3.get_object(Bucket=bucket, Key=f"spread/{i:04d}")["Body"].read() == f"v{i}".encode()


def test_metadata_headers_and_special_keys_roundtrip(s3, bucket):
    body = b"metadata body"
    s3.put_object(Bucket=bucket, Key="m.txt", Body=body, ContentType="text/plain", CacheControl="max-age=60",
                  ContentDisposition="attachment; filename=m.txt", ContentEncoding="identity",
                  ContentLanguage="en", Expires=datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                  Metadata={"origin": "boto3", "Mixed-Case": "value with spaces"})
    for r in (s3.get_object(Bucket=bucket, Key="m.txt"), s3.head_object(Bucket=bucket, Key="m.txt")):
        assert r["ContentType"] == "text/plain" and r["CacheControl"] == "max-age=60"
        assert r["ContentDisposition"] == "attachment; filename=m.txt" and r["ContentLanguage"] == "en"
        assert r["Metadata"] == {"origin": "boto3", "mixed-case": "value with spaces"}
        assert r["Expires"].year == 2030
    # default content type like S3
    s3.put_object(Bucket=bucket, Key="plain", Body=b"p")
    assert s3.head_object(Bucket=bucket, Key="plain")["ContentType"] == "binary/octet-stream"
    weird = ["sp ace.txt", "plus+sign", "percent%20literal", "uni/ü/日本語.txt", "emoji-\U0001F600", "a//b", "trail/",
             "dots/../x", "q?mark", "hash#tag", "amp&eq=", "tilde~", "x" * 1000]
    for k in weird:
        s3.put_object(Bucket=bucket, Key=k, Body=k.encode())
    for k in weird:
        assert s3.get_object(Bucket=bucket, Key=k)["Body"].read() == k.encode(), k
    # a key ending in '/' is an ordinary empty-able object
    assert s3.head_object(Bucket=bucket, Key="trail/")["ContentLength"] == len(b"trail/")
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=bucket, Key="k" * 1025, Body=b"x")
    assert err(e) == ("KeyTooLongError", 400)
    s3.put_object(Bucket=bucket, Key="k" * 1024, Body=b"ok")
    s3.put_object(Bucket=bucket, Key="ü" * 512, Body=b"ok")  # 1024 bytes
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=bucket, Key="ü" * 513, Body=b"x")  # 1026 bytes
    assert err(e)[0] == "KeyTooLongError"


def test_no_such_key_and_delete_semantics(s3, bucket):
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="missing")
    assert err(e) == ("NoSuchKey", 404)
    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=bucket, Key="missing")
    assert err(e)[1] == 404
    s3.delete_object(Bucket=bucket, Key="missing")  # 204 like S3
    s3.put_object(Bucket=bucket, Key="d", Body=b"1")
    s3.delete_object(Bucket=bucket, Key="d")
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="d")
    assert err(e)[0] == "NoSuchKey"


def test_range_gets_span_chunk_boundaries(s3, bucket):
    size = 10 * MIB + 12345
    payload = os.urandom(size)
    s3.put_object(Bucket=bucket, Key="r.bin", Body=payload)

    def rng(spec):
        r = s3.get_object(Bucket=bucket, Key="r.bin", Range=spec)
        return r["Body"].read(), r["ResponseMetadata"]["HTTPStatusCode"], r["ContentRange"]

    body, status, cr = rng("bytes=0-9")
    assert (body, status, cr) == (payload[0:10], 206, f"bytes 0-9/{size}")
    body, status, cr = rng(f"bytes={CHUNK - 5}-{CHUNK + 4}")  # straddles chunk 0/1
    assert body == payload[CHUNK - 5:CHUNK + 5] and cr == f"bytes {CHUNK - 5}-{CHUNK + 4}/{size}"
    body, _, _ = rng(f"bytes={CHUNK - 1}-{2 * CHUNK}")  # three chunks touched
    assert body == payload[CHUNK - 1:2 * CHUNK + 1]
    body, _, _ = rng(f"bytes={3 * MIB}-{9 * MIB}")  # middle
    assert body == payload[3 * MIB:9 * MIB + 1]
    body, _, cr = rng("bytes=-1000")  # suffix
    assert body == payload[-1000:] and cr == f"bytes {size - 1000}-{size - 1}/{size}"
    body, _, _ = rng(f"bytes={size - 3}-")  # open ended
    assert body == payload[-3:]
    body, _, _ = rng(f"bytes={size - 10}-{size + 999}")  # end clipped
    assert body == payload[-10:]
    body, _, _ = rng(f"bytes={2 * CHUNK}-{2 * CHUNK}")  # single byte exactly on a boundary
    assert body == payload[2 * CHUNK:2 * CHUNK + 1]
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="r.bin", Range=f"bytes={size}-")
    assert err(e) == ("InvalidRange", 416)
    # malformed / multi ranges are ignored like S3 (whole object)
    r = s3.get_object(Bucket=bucket, Key="r.bin", Range="bytes=0-1,5-6")
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 200 and len(r["Body"].read()) == size


def test_conditional_get_and_head(s3, bucket):
    s3.put_object(Bucket=bucket, Key="c", Body=b"conditional")
    h = s3.head_object(Bucket=bucket, Key="c")
    etag, lm = h["ETag"], h["LastModified"]
    assert s3.get_object(Bucket=bucket, Key="c", IfMatch=etag)["Body"].read() == b"conditional"
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="c", IfMatch='"deadbeef"')
    assert err(e)[1] == 412
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="c", IfNoneMatch=etag)
    assert err(e)[1] == 304
    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=bucket, Key="c", IfNoneMatch=etag)
    assert err(e)[1] == 304
    assert s3.get_object(Bucket=bucket, Key="c", IfNoneMatch='"other"')["Body"].read() == b"conditional"
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="c", IfModifiedSince=lm + timedelta(seconds=5))
    assert err(e)[1] == 304
    assert s3.get_object(Bucket=bucket, Key="c", IfModifiedSince=lm - timedelta(hours=1))["Body"].read()
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="c", IfUnmodifiedSince=lm - timedelta(hours=1))
    assert err(e)[1] == 412
    assert s3.get_object(Bucket=bucket, Key="c", IfUnmodifiedSince=lm + timedelta(hours=1))["Body"].read()
    # If-Match wins over If-Unmodified-Since (RFC 7232): a matching ETag ignores a failing date
    assert s3.get_object(Bucket=bucket, Key="c", IfMatch=etag,
                         IfUnmodifiedSince=lm - timedelta(hours=1))["Body"].read()


def test_content_md5_validation(s3, bucket, warp):
    body = b"md5 protected"
    good = base64.b64encode(hashlib.md5(body).digest()).decode()
    s3.put_object(Bucket=bucket, Key="ok", Body=body, ContentMD5=good)
    bad = base64.b64encode(hashlib.md5(b"something else").digest()).decode()
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=bucket, Key="bad", Body=body, ContentMD5=bad)
    assert err(e) == ("BadDigest", 400)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="bad")
    assert err(e)[0] == "NoSuchKey"  # the rejected object never became visible
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=bucket, Key="bad2", Body=body, ContentMD5="not-base64!!")
    assert err(e)[0] in ("InvalidDigest", "BadDigest")
    # a bad digest must not replace an existing object
    s3.put_object(Bucket=bucket, Key="keep", Body=b"original")
    with pytest.raises(ClientError):
        s3.put_object(Bucket=bucket, Key="keep", Body=body, ContentMD5=bad)
    assert s3.get_object(Bucket=bucket, Key="keep")["Body"].read() == b"original"


def test_overwrite_is_atomic_and_old_chunks_collected_after_grace(s3, pgs, bucket):
    key = "over"
    first, second = os.urandom(9 * MIB), os.urandom(5 * MIB)
    s3.put_object(Bucket=bucket, Key=key, Body=first, Metadata={"v": "1"})
    owner = where(pgs, bucket, key)[0]
    old_id = sql(pgs[owner], "SELECT object_id FROM warp_s3_objects WHERE bucket=%s AND key=%s", (bucket, key))[0][0]
    s3.put_object(Bucket=bucket, Key=key, Body=second, Metadata={"v": "2"})
    got = s3.get_object(Bucket=bucket, Key=key)
    assert got["Body"].read() == second and got["Metadata"] == {"v": "2"}
    assert sql(pgs[owner], "SELECT count(*) FROM warp_s3_objects WHERE bucket=%s AND key=%s", (bucket, key))[0][0] == 1
    # right after the overwrite the old chunks still exist (grace: readers of the old version finish);
    # the collector removes them after WARP_S3WIRE_GC_GRACE_SECONDS (3s here)
    state = sql(pgs[owner], "SELECT state FROM warp_s3_blobs WHERE object_id=%s", (old_id,))
    assert not state or state[0][0] in ("garbage",)
    deadline = time.time() + 30
    while time.time() < deadline:
        if not sql(pgs[owner], "SELECT 1 FROM warp_s3_chunks WHERE object_id=%s LIMIT 1", (old_id,)) and \
                not sql(pgs[owner], "SELECT 1 FROM warp_s3_blobs WHERE object_id=%s", (old_id,)):
            break
        time.sleep(0.5)
    else:
        pytest.fail("old chunks were not collected after the grace period")
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == second


def test_reader_of_old_version_is_not_torn_by_concurrent_overwrite(s3, warp, bucket):
    """A GET resolves the object row once; overwriting mid-download must not corrupt it (old chunks live on)."""
    key = "torn"
    old = os.urandom(24 * MIB)
    s3.put_object(Bucket=bucket, Key=key, Body=old)
    sock = raw_get(warp, bucket, key)
    head = recv_headers(sock)
    assert b" 200 " in head[0].split(b"\r\n")[0]
    first = sock.recv(64 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(24 * MIB))  # overwrite while the download is in flight
    data = bytearray(head[1]) + bytearray(first)
    while len(data) < len(old):
        chunk = sock.recv(1 << 20)
        assert chunk, "connection closed early"
        data += chunk
    sock.close()
    assert bytes(data) == old


# ---------------------------------------------------------------------------------------------
# delete many / copy
# ---------------------------------------------------------------------------------------------

def test_delete_objects_across_shards(s3, pgs, bucket):
    keys = [f"del/{i}" for i in range(40)]
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b"z")
    assert {where(pgs, bucket, k)[0] for k in keys} == {0, 1}
    res = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[:30]] + [{"Key": "never-existed"}]})
    assert {d["Key"] for d in res["Deleted"]} == set(keys[:30]) | {"never-existed"}
    assert "Errors" not in res
    remaining = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket)["Contents"]]
    assert remaining == sorted(keys[30:])
    quiet = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[30:]], "Quiet": True})
    assert "Deleted" not in quiet
    assert s3.list_objects_v2(Bucket=bucket)["KeyCount"] == 0


def test_copy_within_and_across_shards_byte_exact(s3, pgs, bucket):
    payload = os.urandom(9 * MIB + 17)
    s3.put_object(Bucket=bucket, Key="src", Body=payload, ContentType="application/x-src", Metadata={"m": "1"})
    src_owner = where(pgs, bucket, "src")[0]
    same = cross = None
    for i in range(200):
        k = f"dst{i}"
        s3.copy_object(Bucket=bucket, Key=k, CopySource={"Bucket": bucket, "Key": "src"})
        owner = where(pgs, bucket, k)[0]
        if owner == src_owner and same is None:
            same = k
        if owner != src_owner and cross is None:
            cross = k
        if same and cross:
            break
    assert same and cross, "keys did not hash to both shards"
    for k in (same, cross):
        got = s3.get_object(Bucket=bucket, Key=k)
        assert got["Body"].read() == payload and got["ContentType"] == "application/x-src" and got["Metadata"] == {"m": "1"}
    # independent copies: overwriting the source leaves them intact
    s3.put_object(Bucket=bucket, Key="src", Body=b"changed")
    assert s3.get_object(Bucket=bucket, Key=same)["Body"].read() == payload
    # REPLACE metadata directive
    s3.copy_object(Bucket=bucket, Key="replaced", CopySource={"Bucket": bucket, "Key": same},
                   MetadataDirective="REPLACE", ContentType="text/replaced", Metadata={"new": "yes"})
    h = s3.head_object(Bucket=bucket, Key="replaced")
    assert h["ContentType"] == "text/replaced" and h["Metadata"] == {"new": "yes"}
    # copy onto itself: illegal without REPLACE, allowed as a metadata update with it
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=bucket, Key="replaced", CopySource={"Bucket": bucket, "Key": "replaced"})
    assert err(e)[0] == "InvalidRequest"
    s3.copy_object(Bucket=bucket, Key="replaced", CopySource={"Bucket": bucket, "Key": "replaced"},
                   MetadataDirective="REPLACE", Metadata={"again": "1"})
    assert s3.head_object(Bucket=bucket, Key="replaced")["Metadata"] == {"again": "1"}
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=bucket, Key="x", CopySource={"Bucket": bucket, "Key": "absent"})
    assert err(e)[0] == "NoSuchKey"
    # across buckets
    other = new_bucket(s3)
    s3.copy_object(Bucket=other, Key="fromother", CopySource={"Bucket": bucket, "Key": same})
    assert s3.get_object(Bucket=other, Key="fromother")["Body"].read() == payload
    s3.put_object(Bucket=bucket, Key="empty", Body=b"")
    s3.copy_object(Bucket=bucket, Key="empty-copy", CopySource={"Bucket": bucket, "Key": "empty"})
    assert s3.head_object(Bucket=bucket, Key="empty-copy")["ContentLength"] == 0


# ---------------------------------------------------------------------------------------------
# listing (compared with an in-memory model of S3's listing)
# ---------------------------------------------------------------------------------------------

def utf8_key(k):
    return k.encode("utf-8")


def model_entries(keys, prefix, delimiter, after):
    """S3's listing: keys in UTF-8 byte order, rolled up at `delimiter`, strictly after the marker `after`."""
    entries, seen = [], set()
    for k in sorted(keys, key=utf8_key):
        if not k.startswith(prefix) or after is not None and utf8_key(k) <= utf8_key(after):
            continue
        rest = k[len(prefix):]
        if delimiter and delimiter in rest:
            cp = prefix + rest[:rest.index(delimiter) + len(delimiter)]
            if cp not in seen:
                seen.add(cp)
                entries.append(("P", cp))
        else:
            entries.append(("K", k))
    return entries


TRICKY = ["a/b", "a-b", "a.b", "A", "a b", "a+b", "a%20b", "a_b", "a~b", "ü", "ü/x", "日本語/文書.txt", "日本語", "b",
          "\U0001F600/smile", "￮", "\U00010000", "\U0001F600", "z/y/x", "z/y", "z/", "z", "dir/", "dir/sub/", "dir/sub/file",
          "dir/sub2/file", "dir/file", "dir2", "dir-", "dir.", "dir0", "diré/x", "0", "00", "1/2/3/4"]


def random_keys(rng, n):
    alphabet = ["a", "b", "c", "/", "-", ".", "A", "ü", "日", "\U0001F600", " ", "+", "0"]
    out = set(TRICKY)
    while len(out) < n:
        k = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 7)))
        out.add(k)
    return sorted(out)


def paginate_v2(s3, bucket, max_keys, **kw):
    pages, token = [], None
    while True:
        args = dict(Bucket=bucket, MaxKeys=max_keys, **kw)
        if token:
            args["ContinuationToken"] = token
        r = s3.list_objects_v2(**args)
        entries = [("K", o["Key"]) for o in r.get("Contents", [])] + [("P", p["Prefix"]) for p in r.get("CommonPrefixes", [])]
        entries.sort(key=lambda e: utf8_key(e[1]))
        assert r["KeyCount"] == len(entries) and len(entries) <= max_keys
        pages.append(entries)
        if not r["IsTruncated"]:
            assert "NextContinuationToken" not in r
            return pages
        assert len(entries) == max_keys, "a truncated page must be full"
        token = r["NextContinuationToken"]


def test_listing_matches_s3_model_across_shards(s3, pgs, bucket):
    rng = random.Random(20260925)
    keys = random_keys(rng, 160)
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda k: s3.put_object(Bucket=bucket, Key=k, Body=b"x" * (len(k) % 7)), keys))
    spread = [sql(pg, "SELECT count(*) FROM warp_s3_objects WHERE bucket=%s", (bucket,))[0][0] for pg in pgs]
    assert sum(spread) == len(keys) and min(spread) > 40, spread
    # the plain full listing is in strict UTF-8 byte order and reports sizes
    full = [o for p in paginate_v2(s3, bucket, 1000) for o in p]
    assert [k for _, k in full] == sorted(keys, key=utf8_key)
    for prefix, delimiter, max_keys in [("", None, 1000), ("", None, 7), ("", "/", 5), ("", "/", 1000), ("a", None, 4),
                                        ("a", "/", 3), ("dir/", "/", 2), ("dir/", "/", 1000), ("z/", "/", 1),
                                        ("日本", None, 2), ("dir", "/", 2), ("zzz", "/", 10), ("", "-", 6), ("a/", "/", 1)]:
        kw = {}
        if prefix:
            kw["Prefix"] = prefix
        if delimiter:
            kw["Delimiter"] = delimiter
        expected = model_entries(keys, prefix, delimiter, None)
        pages = paginate_v2(s3, bucket, max_keys, **kw)
        flat = [e for p in pages for e in p]
        assert flat == expected, (prefix, delimiter, max_keys, flat[:5], expected[:5])
        # page boundaries are exactly max_keys entries
        assert [len(p) for p in pages[:-1]] == [max_keys] * (len(pages) - 1)
        if max_keys > 1 and expected:
            assert len(pages) == (len(expected) + max_keys - 1) // max_keys


def test_list_v1_marker_and_start_after(s3, bucket):
    rng = random.Random(7)
    keys = random_keys(rng, 90)
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda k: s3.put_object(Bucket=bucket, Key=k, Body=b"1"), keys))
    # v1 with marker, following NextMarker (which may be a common prefix)
    for prefix, delimiter, mk in [("", "/", 4), ("", None, 9), ("dir/", "/", 2)]:
        got, marker = [], None
        while True:
            kw = dict(Bucket=bucket, MaxKeys=mk)
            if prefix:
                kw["Prefix"] = prefix
            if delimiter:
                kw["Delimiter"] = delimiter
            if marker:
                kw["Marker"] = marker
            r = s3.list_objects(**kw)
            page = [("K", o["Key"]) for o in r.get("Contents", [])] + [("P", p["Prefix"]) for p in r.get("CommonPrefixes", [])]
            page.sort(key=lambda e: utf8_key(e[1]))
            got += page
            if not r["IsTruncated"]:
                break
            marker = r.get("NextMarker") or page[-1][1]
        assert got == model_entries(keys, prefix, delimiter, None), (prefix, delimiter, mk)
    sa = sorted(keys, key=utf8_key)[20]
    r = s3.list_objects_v2(Bucket=bucket, StartAfter=sa, MaxKeys=1000)
    assert [o["Key"] for o in r["Contents"]] == [k for k in sorted(keys, key=utf8_key) if utf8_key(k) > utf8_key(sa)]
    assert r["StartAfter"] == sa
    r = s3.list_objects_v2(Bucket=bucket, MaxKeys=0)
    assert r["KeyCount"] == 0 and not r["IsTruncated"]
    # URL-encoded listing
    r = s3.list_objects_v2(Bucket=bucket, Prefix="a b", EncodingType="url")
    from urllib.parse import unquote
    # an explicit EncodingType is not auto-decoded by botocore
    assert [unquote(o["Key"]) for o in r.get("Contents", [])] == [k for k in sorted(keys, key=utf8_key) if k.startswith("a b")]
    assert r["EncodingType"] == "url"


def test_common_prefix_seen_on_several_shards_is_returned_once(s3, pgs, bucket):
    keys = [f"top/{i:03d}/leaf" for i in range(60)]
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b"1")
    assert {where(pgs, bucket, k)[0] for k in keys} == {0, 1}
    r = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
    assert [p["Prefix"] for p in r["CommonPrefixes"]] == ["top/"] and "Contents" not in r
    r = s3.list_objects_v2(Bucket=bucket, Prefix="top/", Delimiter="/", MaxKeys=25)
    assert len(r["CommonPrefixes"]) == 25 and r["IsTruncated"]
    assert [p["Prefix"] for p in r["CommonPrefixes"]] == [f"top/{i:03d}/" for i in range(25)]


def test_list_buckets_merges_catalog(s3):
    names = [new_bucket(s3) for _ in range(3)]
    listed = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    for n in names:
        assert n in listed
    assert listed == sorted(listed)


# ---------------------------------------------------------------------------------------------
# multipart
# ---------------------------------------------------------------------------------------------

def multipart_etag(parts):
    return '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest() + f'-{len(parts)}"'


def test_upload_file_uses_multipart_and_assembles_by_reference(s3, pgs, bucket, tmp_path):
    payload = os.urandom(20 * MIB + 321)
    path = tmp_path / "big.bin"
    path.write_bytes(payload)
    s3.upload_file(str(path), bucket, "mp/upload.bin",
                   Config=TransferConfig(multipart_threshold=8 * MIB, multipart_chunksize=8 * MIB, max_concurrency=4))
    parts = [payload[i:i + 8 * MIB] for i in range(0, len(payload), 8 * MIB)]
    assert len(parts) == 3
    h = s3.head_object(Bucket=bucket, Key="mp/upload.bin")
    assert h["ETag"] == multipart_etag(parts)
    assert h["ETag"].endswith('-3"') and h["ContentLength"] == len(payload)
    assert s3.get_object(Bucket=bucket, Key="mp/upload.bin")["Body"].read() == payload
    owner = where(pgs, bucket, "mp/upload.bin")[0]
    # assembled BY REFERENCE: the object row lists the part blobs; no byte was copied into a new chunk set
    (segments,) = sql(pgs[owner], "SELECT jsonb_array_length(segments) FROM warp_s3_objects WHERE bucket=%s AND key=%s",
                      (bucket, "mp/upload.bin"))[0]
    assert segments == 3
    total = sql(pgs[owner], "SELECT sum(length(data)) FROM warp_s3_chunks c JOIN warp_s3_blobs b USING (object_id) "
                            "WHERE b.state='committed' AND c.object_id IN (SELECT (s->>'id')::uuid FROM warp_s3_objects o, "
                            "jsonb_array_elements(o.segments) s WHERE o.bucket=%s AND o.key=%s)", (bucket, "mp/upload.bin"))[0][0]
    assert total == len(payload)  # stored exactly once
    # ranges across part boundaries (parts are 8 MiB, chunks 4 MiB: boundaries do not need to align)
    for a, b in [(8 * MIB - 3, 8 * MIB + 3), (16 * MIB - 1, 16 * MIB + 1), (0, len(payload) - 1), (5 * MIB, 19 * MIB)]:
        got = s3.get_object(Bucket=bucket, Key="mp/upload.bin", Range=f"bytes={a}-{b}")["Body"].read()
        assert got == payload[a:b + 1]
    # the multipart object can be copied and overwritten
    s3.copy_object(Bucket=bucket, Key="mp/copy.bin", CopySource={"Bucket": bucket, "Key": "mp/upload.bin"})
    assert s3.get_object(Bucket=bucket, Key="mp/copy.bin")["Body"].read() == payload
    s3.delete_object(Bucket=bucket, Key="mp/upload.bin")
    assert s3.get_object(Bucket=bucket, Key="mp/copy.bin")["Body"].read() == payload


def test_manual_multipart_out_of_order_parts_reupload_abort_and_errors(s3, bucket, pgs):
    key = "manual/mp.bin"
    up = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/x-mp", Metadata={"k": "v"})
    uid = up["UploadId"]
    p1, p2, p3 = os.urandom(5 * MIB), os.urandom(5 * MIB + 7), os.urandom(1234)
    e3 = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=3, Body=p3)["ETag"]
    e1_old = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=os.urandom(5 * MIB))["ETag"]
    e2 = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=2, Body=p2)["ETag"]
    e1 = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=p1)["ETag"]  # re-upload replaces
    assert e1 != e1_old
    # errors: order, wrong etag, too small non-last part, unknown upload
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 2, "ETag": e2}, {"PartNumber": 1, "ETag": e1}]})
    assert err(e) == ("InvalidPartOrder", 400)
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": e1_old}, {"PartNumber": 2, "ETag": e2}]})
    assert err(e) == ("InvalidPart", 400)
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": e1}, {"PartNumber": 4, "ETag": e2}]})
    assert err(e) == ("InvalidPart", 400)
    with pytest.raises(ClientError) as e:  # duplicate part number
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 3, "ETag": e3}, {"PartNumber": 3, "ETag": e3}]})
    assert err(e)[0] == "InvalidPartOrder"
    with pytest.raises(ClientError) as e:
        s3.upload_part(Bucket=bucket, Key=key, UploadId="bogus", PartNumber=1, Body=b"x")
    assert err(e) == ("NoSuchUpload", 404)
    with pytest.raises(ClientError) as e:
        s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=10001, Body=b"x")
    assert err(e)[0] == "InvalidArgument"
    # good completion in order; parts 1..3 (part 3 last and small)
    done = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
        {"PartNumber": 1, "ETag": e1}, {"PartNumber": 2, "ETag": e2}, {"PartNumber": 3, "ETag": e3}]})
    assert done["ETag"] == multipart_etag([p1, p2, p3])
    got = s3.get_object(Bucket=bucket, Key=key)
    assert got["Body"].read() == p1 + p2 + p3 and got["ContentType"] == "application/x-mp" and got["Metadata"] == {"k": "v"}
    # the upload is gone
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": e1}]})
    assert err(e)[0] == "NoSuchUpload"

    # EntityTooSmall: a non-last part below 5 MiB
    up2 = s3.create_multipart_upload(Bucket=bucket, Key="small-parts")["UploadId"]
    a = s3.upload_part(Bucket=bucket, Key="small-parts", UploadId=up2, PartNumber=1, Body=b"tiny")["ETag"]
    b = s3.upload_part(Bucket=bucket, Key="small-parts", UploadId=up2, PartNumber=2, Body=b"tiny2")["ETag"]
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=bucket, Key="small-parts", UploadId=up2, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": a}, {"PartNumber": 2, "ETag": b}]})
    assert err(e) == ("EntityTooSmall", 400)
    # a single small part is fine (it is the last part)
    s3.complete_multipart_upload(Bucket=bucket, Key="small-parts", UploadId=up2, MultipartUpload={"Parts": [
        {"PartNumber": 1, "ETag": a}]})
    assert s3.get_object(Bucket=bucket, Key="small-parts")["Body"].read() == b"tiny"

    # abort: nothing visible, part chunks become garbage
    up3 = s3.create_multipart_upload(Bucket=bucket, Key="aborted")["UploadId"]
    s3.upload_part(Bucket=bucket, Key="aborted", UploadId=up3, PartNumber=1, Body=os.urandom(6 * MIB))
    s3.abort_multipart_upload(Bucket=bucket, Key="aborted", UploadId=up3)
    with pytest.raises(ClientError) as e:
        s3.abort_multipart_upload(Bucket=bucket, Key="aborted", UploadId=up3)
    assert err(e) == ("NoSuchUpload", 404)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="aborted")
    assert err(e)[0] == "NoSuchKey"
    with pytest.raises(ClientError) as e:  # upload id of another key is rejected
        other = s3.create_multipart_upload(Bucket=bucket, Key="other-key")["UploadId"]
        s3.upload_part(Bucket=bucket, Key="not-the-key", UploadId=other, PartNumber=1, Body=b"x")
    assert err(e)[0] == "NoSuchUpload"


def test_multipart_upload_lives_on_the_owning_shard_and_id_encodes_it(s3, pgs, bucket):
    seen = set()
    for i in range(24):
        key = f"mpshard/{i}"
        uid = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        host_b64, hexid = uid.rsplit(".", 1)
        padded = host_b64 + "=" * (-len(host_b64) % 4)
        host = base64.urlsafe_b64decode(padded).decode()
        assert host in ("default", "pg2") and len(hexid) == 32
        idx = 0 if host == "default" else 1
        seen.add(idx)
        s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=b"p" * 100)
        assert sql(pgs[idx], "SELECT 1 FROM warp_s3_multipart_uploads WHERE bucket=%s AND key=%s", (bucket, key))
        assert not sql(pgs[1 - idx], "SELECT 1 FROM warp_s3_multipart_uploads WHERE bucket=%s AND key=%s", (bucket, key))
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": md5_quoted(b"p" * 100)}]})
        assert where(pgs, bucket, key) == [idx]  # the completed object is on the same shard as its upload
    assert seen == {0, 1}


def md5_quoted(b):
    return f'"{md5(b)}"'


def test_stale_multipart_upload_is_garbage_collected(s3, pgs, bucket):
    key = "stale/mp"
    uid = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=os.urandom(6 * MIB))
    owner = where_upload(pgs, bucket, key)
    blob = sql(pgs[owner], "SELECT p.object_id FROM warp_s3_parts p JOIN warp_s3_multipart_uploads u USING (upload_id) "
                           "WHERE u.bucket=%s AND u.key=%s", (bucket, key))[0][0]
    # age the upload past WARP_S3WIRE_GC_MULTIPART_AGE_SECONDS instead of waiting a week
    sql(pgs[owner], "UPDATE warp_s3_multipart_uploads SET initiated = now() - interval '30 days' WHERE bucket=%s AND key=%s",
        (bucket, key))
    deadline = time.time() + 40
    while time.time() < deadline:
        if not sql(pgs[owner], "SELECT 1 FROM warp_s3_multipart_uploads WHERE bucket=%s AND key=%s", (bucket, key)) and \
                not sql(pgs[owner], "SELECT 1 FROM warp_s3_chunks WHERE object_id=%s LIMIT 1", (blob,)):
            break
        time.sleep(0.5)
    else:
        pytest.fail("stale multipart upload and its chunks were not collected")
    with pytest.raises(ClientError) as e:
        s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=2, Body=b"late")
    assert err(e)[0] == "NoSuchUpload"


def where_upload(pgs, bucket, key):
    for i, pg in enumerate(pgs):
        if sql(pg, "SELECT 1 FROM warp_s3_multipart_uploads WHERE bucket=%s AND key=%s", (bucket, key)):
            return i
    raise AssertionError("upload not found")


# ---------------------------------------------------------------------------------------------
# half-written uploads are invisible and collected
# ---------------------------------------------------------------------------------------------

def sign_headers(warp, method, path, headers, query=""):
    url = f"http://localhost:{warp.frontend_port}{path}"
    req = AWSRequest(method=method, url=url + (f"?{query}" if query else ""), headers=headers)
    SigV4Auth(Credentials(KEY, SECRET), "s3", "us-east-1").add_auth(req)
    return dict(req.headers.items())


def raw_put_start(warp, bucket, key, total):
    path = f"/{bucket}/{key}"
    hdrs = sign_headers(warp, "PUT", path, {"Host": f"localhost:{warp.frontend_port}", "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
                                             "Content-Length": str(total)})
    sock = socket.create_connection(("localhost", warp.frontend_port))
    head = f"PUT {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items()) + "\r\n"
    sock.sendall(head.encode())
    return sock


def raw_get(warp, bucket, key, rcvbuf=None):
    path = f"/{bucket}/{key}"
    hdrs = sign_headers(warp, "GET", path, {"Host": f"localhost:{warp.frontend_port}", "x-amz-content-sha256": "UNSIGNED-PAYLOAD"})
    sock = socket.socket()
    if rcvbuf:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.connect(("localhost", warp.frontend_port))
    sock.sendall((f"GET {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items()) + "\r\n").encode())
    return sock


def recv_headers(sock):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        assert chunk, "closed before headers"
        buf += chunk
    head, rest = buf.split(b"\r\n\r\n", 1)
    return head + b"\r\n", rest


def read_status(sock):
    data = b""
    sock.settimeout(30)
    while b"\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return int(data.split(b" ")[1]) if data else 0


def test_half_uploaded_object_is_invisible_and_collected(s3, warp, pgs, bucket):
    key = "half"
    s3.put_object(Bucket=bucket, Key=key, Body=b"committed-old")
    total = 30 * MIB
    sock = raw_put_start(warp, bucket, key, total)
    sock.sendall(os.urandom(9 * MIB))  # two full chunks stored, the client then stalls
    owner = where(pgs, bucket, key)[0]
    deadline = time.time() + 15
    uploading = []
    while time.time() < deadline and not uploading:
        uploading = sql(pgs[owner], "SELECT object_id FROM warp_s3_blobs WHERE state='uploading'")
        time.sleep(0.2)
    assert uploading, "the in-flight upload should be an 'uploading' blob"
    blob = uploading[0][0]
    assert sql(pgs[owner], "SELECT count(*) FROM warp_s3_chunks WHERE object_id=%s", (blob,))[0][0] >= 1
    # invisible: readers still see the previous committed object, listing shows the old size
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"committed-old"
    assert s3.list_objects_v2(Bucket=bucket)["Contents"][0]["Size"] == len(b"committed-old")
    # abandoned: make it look idle past WARP_S3WIRE_GC_UPLOADING_AGE_SECONDS and wait for the collector
    sql(pgs[owner], "UPDATE warp_s3_blobs SET state_at = now() - interval '2 days' WHERE object_id=%s", (blob,))
    deadline = time.time() + 30
    while time.time() < deadline:
        if not sql(pgs[owner], "SELECT 1 FROM warp_s3_chunks WHERE object_id=%s LIMIT 1", (blob,)) and \
                not sql(pgs[owner], "SELECT 1 FROM warp_s3_blobs WHERE object_id=%s", (blob,)):
            break
        time.sleep(0.5)
    else:
        pytest.fail("abandoned upload was not collected")
    # the stalled client resumes: the store refuses to resurrect the collected upload and nothing is committed
    try:
        sock.sendall(os.urandom(21 * MIB))
    except OSError:
        pass
    status = read_status(sock)
    assert status in (0, 400, 500, 503), status
    sock.close()
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"committed-old"


def test_client_disconnect_mid_upload_leaves_nothing_visible(s3, warp, pgs, bucket):
    sock = raw_put_start(warp, bucket, "dropped", 20 * MIB)
    sock.sendall(os.urandom(6 * MIB))
    time.sleep(0.5)
    sock.close()
    time.sleep(1)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="dropped")
    assert err(e)[0] == "NoSuchKey"
    assert s3.list_objects_v2(Bucket=bucket)["KeyCount"] == 0
    # the aborted blob is marked garbage or already collected: never 'committed'
    for pg in pgs:
        assert not sql(pg, "SELECT 1 FROM warp_s3_blobs b WHERE b.state='committed' AND NOT EXISTS "
                           "(SELECT 1 FROM warp_s3_objects o WHERE o.object_id=b.object_id) AND NOT EXISTS "
                           "(SELECT 1 FROM warp_s3_parts p WHERE p.object_id=b.object_id) AND NOT EXISTS "
                           "(SELECT 1 FROM warp_s3_objects o, jsonb_array_elements(o.segments) s WHERE (s->>'id')::uuid=b.object_id)"
                           " AND b.size > %s", (5 * MIB,))


# ---------------------------------------------------------------------------------------------
# concurrency and pool discipline (WARP_POOL_MAX_SIZE=4)
# ---------------------------------------------------------------------------------------------

def test_mixed_concurrent_operations_with_a_tiny_pool(warp, bucket):
    clients = [make_client(warp) for _ in range(4)]
    errors, lock = [], threading.Lock()
    store = {}

    def worker(n):
        c = clients[n % 4]
        rng = random.Random(n)
        try:
            for i in range(20):
                k = f"conc/{n}/{i}"
                body = os.urandom(rng.choice([0, 10, 5000, 300_000, 5 * MIB]))
                c.put_object(Bucket=bucket, Key=k, Body=body)
                with lock:
                    store[k] = body
                assert c.get_object(Bucket=bucket, Key=k)["Body"].read() == body
                if i % 3 == 0:
                    c.list_objects_v2(Bucket=bucket, Prefix=f"conc/{n}/", MaxKeys=5)
                if i % 5 == 0:
                    c.head_object(Bucket=bucket, Key=k)
                    c.copy_object(Bucket=bucket, Key=k + ".copy", CopySource={"Bucket": bucket, "Key": k})
                if i % 7 == 0:
                    c.delete_object(Bucket=bucket, Key=k)
                    with lock:
                        store.pop(k)
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append((n, repr(e)))

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(16)]
    started = time.time()
    [t.start() for t in threads]
    [t.join(300) for t in threads]
    assert not errors, errors[:3]
    assert time.time() - started < 250
    c = clients[0]
    for k in random.Random(1).sample(sorted(store), 20):
        assert c.get_object(Bucket=bucket, Key=k)["Body"].read() == store[k]


def test_slow_clients_do_not_pin_pooled_connections(warp, bucket, s3):
    """8 uploaders trickle 12 MiB bodies for ~6s and 2 downloaders read slowly, while the Postgres pool has
    only 4 connections: if any of them held a pooled connection across the client wait, the fast requests
    made meanwhile would time out or fail."""
    stop = threading.Event()
    results = {"slow_ok": 0}
    body_total = 12 * MIB
    payloads = {}

    def slow_upload(n):
        key = f"slow/up{n}"
        payload = os.urandom(body_total)
        payloads[key] = payload
        sock = raw_put_start(warp, bucket, key, body_total)
        step = 600 * 1024
        for off in range(0, body_total, step):
            sock.sendall(payload[off:off + step])
            time.sleep(0.3)
        assert read_status(sock) == 200
        sock.close()
        results["slow_ok"] += 1

    big = os.urandom(40 * MIB)
    s3.put_object(Bucket=bucket, Key="slow/download", Body=big)

    def slow_download(n):
        sock = raw_get(warp, bucket, "slow/download", rcvbuf=64 * 1024)
        head, rest = recv_headers(sock)
        got = bytearray(rest)
        while len(got) < len(big) and time.time() < deadline:
            chunk = sock.recv(64 * 1024)
            if not chunk:
                break
            got += chunk
            time.sleep(0.02)
        sock.close()
        if len(got) == len(big):
            assert bytes(got) == big
            results["slow_ok"] += 1

    deadline = time.time() + 120
    threads = [threading.Thread(target=slow_upload, args=(n,)) for n in range(8)]
    threads += [threading.Thread(target=slow_download, args=(n,)) for n in range(2)]
    [t.start() for t in threads]
    time.sleep(1.5)
    fast = make_client(warp)
    latencies = []
    n_fast = 0
    while any(t.is_alive() for t in threads[:8]) and time.time() < deadline and n_fast < 400:
        t0 = time.time()
        k = f"fast/{n_fast}"
        fast.put_object(Bucket=bucket, Key=k, Body=b"fast")
        assert fast.get_object(Bucket=bucket, Key=k)["Body"].read() == b"fast"
        fast.list_objects_v2(Bucket=bucket, Prefix="fast/", MaxKeys=3)
        latencies.append(time.time() - t0)
        n_fast += 1
    [t.join(180) for t in threads]
    assert n_fast >= 20, n_fast
    latencies.sort()
    assert latencies[int(len(latencies) * 0.95)] < 2.0, latencies[-5:]
    assert results["slow_ok"] >= 8, results
    for key, payload in payloads.items():
        assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == payload


def test_large_object_memory_is_bounded_by_chunk_size_not_object_size(warp, s3, bucket):
    """A 200 MiB object through a JVM capped at -Xmx384m: whole-object buffering (request or response) would
    need > 200 MiB of live heap per copy and fail. RSS growth is sampled as a second signal."""
    import subprocess

    def rss_mib():
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(warp.process.pid)], capture_output=True, text=True).stdout
        return int(out.strip()) / 1024

    payload = os.urandom(200 * MIB)
    digest = hashlib.md5(payload).hexdigest()
    before = rss_mib()
    import io
    s3.upload_fileobj(io.BytesIO(payload), bucket, "huge", Config=TransferConfig(multipart_threshold=1 << 40))
    peak = rss_mib()
    got = hashlib.md5()
    body = s3.get_object(Bucket=bucket, Key="huge")["Body"]
    n = 0
    for chunk in iter(lambda: body.read(1 << 20), b""):
        got.update(chunk)
        n += len(chunk)
        peak = max(peak, rss_mib())
    assert n == len(payload) and got.hexdigest() == digest
    assert s3.head_object(Bucket=bucket, Key="huge")["ETag"] == f'"{digest}"'
    print(f"RSS before {before:.0f} MiB, peak {peak:.0f} MiB for a 200 MiB object")
    assert peak - before < 250, (before, peak)


def test_metrics_report_operations_as_s3wire(warp, s3, bucket):
    s3.put_object(Bucket=bucket, Key="m1", Body=b"1")
    s3.get_object(Bucket=bucket, Key="m1")
    time.sleep(1)
    r = requests.get(f"http://localhost:{warp.metrics_port}/api/metrics/summary",
                     headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5)
    r.raise_for_status()
    text = r.text
    assert "s3wire" in text and "PutObject" in text and "GetObject" in text and "s3pg:" in text


def test_mcp_lists_and_describes_the_s3_store(warp, s3, bucket, pgs):
    from mcp_support import call_json
    keys = [f"mcp/{i}" for i in range(20)]
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b"12345")
    out = call_json(warp.mcp_port, "list_backends")
    by = {b["name"]: b for b in out["backends"]}
    assert by["default"]["enabledStores"] == ["s3"] and by["pg2"]["enabledStores"] == ["s3"]
    for host in ("default", "pg2"):
        b = by[f"{host}.s3store"]
        assert b["engine"] == "warp-emulated" and b["host"] == host and b["sharded"] is True
        assert b["hostedOn"] == ["default", "pg2"]
    d = call_json(warp.mcp_port, "describe_backend", {"backend": "pg2.s3store"})
    assert d["sharded"] is True and d["hostedOn"] == ["default", "pg2"]
    mine = {x["name"]: x for x in d["contents"]["buckets"]}[bucket]
    assert mine["objectCount"] == 20 and mine["totalBytes"] == 100
    shards = {sh["host"]: sh for sh in d["contents"]["shards"]}
    assert set(shards) == {"default", "pg2"}
    assert sum(sh["objectCount"] for sh in shards.values()) >= 20 and all(sh["objectCount"] > 0 for sh in shards.values())


# ---------------------------------------------------------------------------------------------
# mode selection: Postgres mode wins over proxy mode, and follows hot reload
# ---------------------------------------------------------------------------------------------

def test_postgres_mode_wins_over_proxy_config_and_follows_hot_reload():
    from warp_test_support import RealMinio
    pg = RealPostgres()
    minio = RealMinio()
    proc = None
    try:
        direct = boto3.client("s3", endpoint_url=minio.endpoint, region_name="us-east-1",
                              aws_access_key_id=minio.ACCESS_KEY, aws_secret_access_key=minio.SECRET_KEY,
                              config=Config(s3={"addressing_style": "path"}))
        direct.create_bucket(Bucket="warp-backend")
        proc = WarpProcess(pg, "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env={
            **isolated_ports("WARP_S3WIRE_PORT"),
            "WARP_S3WIRE_BACKEND_ENDPOINT": minio.endpoint, "WARP_S3WIRE_BACKEND_BUCKET": "warp-backend",
            "WARP_S3WIRE_BACKEND_ACCESS_KEY": minio.ACCESS_KEY, "WARP_S3WIRE_BACKEND_SECRET_KEY": minio.SECRET_KEY,
            "WARP_S3WIRE_CREDENTIALS": f"{KEY}={SECRET}",
            "WARP_BACKEND_STORES": "default=s3", "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})
        c = make_client(proc)

        def api(method, path, body=None):
            r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                                 headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
            assert r.status_code == 200, (r.status_code, r.text)

        log = "".join(proc._output_lines)
        assert "Postgres mode wins" in log and "proxy configuration is ignored" in log
        c.create_bucket(Bucket="modeb")
        c.put_object(Bucket="modeb", Key="in-pg", Body=b"pg")
        assert sql(pg, "SELECT count(*) FROM warp_s3_objects WHERE bucket='modeb'")[0][0] == 1
        assert not direct.list_objects_v2(Bucket="warp-backend").get("Contents")   # nothing reached MinIO
        # remove the store from the set: the SAME listener falls back to the proxy on the next request
        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": []})
        time.sleep(1)
        c.create_bucket(Bucket="proxyb")
        c.put_object(Bucket="proxyb", Key="in-minio", Body=b"minio")
        assert direct.get_object(Bucket="warp-backend", Key="proxyb/in-minio")["Body"].read() == b"minio"
        assert "modeb" not in [b["Name"] for b in c.list_buckets()["Buckets"]]
        # enable again: back to Postgres, the earlier Postgres data is still there
        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["s3"]})
        time.sleep(1)
        assert c.get_object(Bucket="modeb", Key="in-pg")["Body"].read() == b"pg"
        assert "proxyb" not in [b["Name"] for b in c.list_buckets()["Buckets"]]
    finally:
        if proc:
            proc.close()
        minio.close()
        pg.close()


# ---------------------------------------------------------------------------------------------
# topology change: data is not rebalanced; optional probe-other-shards fallback
# ---------------------------------------------------------------------------------------------

def test_object_on_the_wrong_shard_is_not_found_unless_probing_is_enabled(warp, s3, pgs, bucket):
    """Adding a host does not move existing objects, so a key can sit on a shard it no longer hashes to. By
    default GET says NoSuchKey (documented); WARP_S3WIRE_PROBE_OTHER_SHARDS=true finds it. The second Warp
    reads the same config store, so it sees the same two-host set."""
    s3.put_object(Bucket=bucket, Key="misplaced", Body=b"moved between shards", ContentType="text/x-moved")
    src = where(pgs, bucket, "misplaced")[0]
    dst = 1 - src
    cols = "bucket, key, object_id, size, etag, chunk_size, segments, content_type, cache_control, content_disposition, " \
           "content_encoding, content_language, expires, user_metadata, last_modified"
    row = sql(pgs[src], f"SELECT {cols} FROM warp_s3_objects WHERE bucket=%s AND key=%s", (bucket, "misplaced"))[0]
    obj_id = row[2]
    chunks = sql(pgs[src], "SELECT object_id, seq, data FROM warp_s3_chunks WHERE object_id=%s", (obj_id,))
    blobs = sql(pgs[src], "SELECT object_id, state, size FROM warp_s3_blobs WHERE object_id=%s", (obj_id,))
    import json as _json
    with psycopg2.connect(host="localhost", port=pgs[dst].port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute("INSERT INTO warp_s3_blobs (object_id, state, size) VALUES (%s, %s, %s)", blobs[0])
        for oid, seq, data in chunks:
            cur.execute("INSERT INTO warp_s3_chunks (object_id, seq, data) VALUES (%s, %s, %s)", (oid, seq, data))
        r = list(row)
        r[6] = _json.dumps(r[6]) if r[6] is not None else None
        r[13] = _json.dumps(r[13]) if r[13] is not None else None
        cur.execute(f"INSERT INTO warp_s3_objects ({cols}) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)", r)
    sql(pgs[src], "DELETE FROM warp_s3_objects WHERE bucket=%s AND key=%s", (bucket, "misplaced"))
    assert where(pgs, bucket, "misplaced") == [dst]
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="misplaced")
    assert err(e) == ("NoSuchKey", 404)
    probe = WarpProcess(pgs[0], "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env={
        **isolated_ports("WARP_S3WIRE_PORT"), "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_S3WIRE_ENABLED": "true",
        "WARP_S3WIRE_CREDENTIALS": f"{KEY}={SECRET}", "WARP_S3WIRE_PROBE_OTHER_SHARDS": "true"})
    try:
        pc = make_client(probe)
        got = pc.get_object(Bucket=bucket, Key="misplaced")
        assert got["Body"].read() == b"moved between shards" and got["ContentType"] == "text/x-moved"
        assert pc.head_object(Bucket=bucket, Key="misplaced")["ContentLength"] == len(b"moved between shards")
    finally:
        probe.close()
