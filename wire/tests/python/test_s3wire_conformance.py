"""s3wire (Postgres mode) conformance: the S3 features beyond plain put/get -- versioning, tagging, ACLs,
public access block, CORS, checksums, multipart extras (ListParts / ListMultipartUploads / UploadPartCopy),
conditional requests, presigned URLs and POST policy uploads, virtual-hosted addressing, SelectObjectContent,
annotations and error parity -- with a real boto3 client against a real Warp process and native Postgres.

Every test runs twice: against ONE Postgres backend and against TWO (the s3 store sharded by key across both), so
everything -- versions, tags, multipart, listings -- is proven across shards. Set WARP_TEST_PG_LOCAL=1 for native Postgres.
"""
import base64
import gzip
import hashlib
import io
import json
import os
import time
import uuid
import zlib
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
import pytest
import requests
from botocore.auth import S3SigV4Auth, SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.client import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from warp_test_support import RealPostgres, WarpProcess, isolated_ports

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN
KEY = "s3conf-key"
SECRET = "s3conf-secret-value"
MIB = 1024 * 1024


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def cluster_env():
    """Static single-node cluster discovery. Ignite binds its discovery SPI to the first free port from 47500 up, so the seed
    node must be exactly that port (the same rule launch_warp.py applies with lsof); wait briefly for a just-closed Warp
    to release its port so two Warps in one session agree."""
    import socket
    time.sleep(1.5)
    for p in range(47500, 47600):
        with socket.socket() as sk:
            try:
                sk.bind(("127.0.0.1", p))
            except OSError:
                continue
        return {"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
                "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{p}"}
    return {}


class Env:
    def __init__(self, n):
        self.pgs = [RealPostgres() for _ in range(n)]
        ports = {**isolated_ports("WARP_S3WIRE_PORT"), **cluster_env()}
        self.warp = WarpProcess(self.pgs[0], "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env={
            **ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_S3WIRE_ENABLED": "true",
            "WARP_S3WIRE_CREDENTIALS": f"{KEY}={SECRET}", "WARP_S3WIRE_BUCKET_CACHE_MILLIS": "0",
            "WARP_S3WIRE_GC_INTERVAL_SECONDS": "0", "WARP_S3WIRE_VHOST_DOMAIN": "s3.vhost.test",
        })
        self.port = self.warp.frontend_port
        self.api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["s3"]})
        for i, pg in enumerate(self.pgs[1:], start=2):
            self.api("POST", "/api/backend-sets/default/backends", {
                "name": f"pg{i}", "url": f"jdbc:postgresql://localhost:{pg.port}/postgres", "user": "postgres",
                "password": "postgres", "enabledStores": ["s3"]}, expect=201)
        self.s3 = self.client()

    def api(self, method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{self.warp.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    @property
    def endpoint(self):
        return f"http://localhost:{self.port}"

    def client(self, region="us-east-1", **kw):
        cfg = dict(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 0})
        cfg.update(kw)
        return boto3.client("s3", endpoint_url=self.endpoint, region_name=region, aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET, config=Config(**cfg))

    def close(self):
        self.warp.close()
        for pg in self.pgs:
            pg.close()


@pytest.fixture(scope="module", params=[1, 2], ids=["one-backend", "two-backends"])
def env(request):
    e = Env(request.param)
    yield e
    e.close()


@pytest.fixture()
def s3(env):
    return env.s3


_n = [0]


def new_bucket(s3, **kw):
    _n[0] += 1
    name = f"cf-{_n[0]}-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=name, **kw)
    return name


@pytest.fixture()
def bucket(s3):
    return new_bucket(s3)


@pytest.fixture()
def vbucket(s3):
    b = new_bucket(s3)
    s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    return b


def code(e):
    return e.value.response["Error"]["Code"], e.value.response["ResponseMetadata"]["HTTPStatusCode"]


def body_of(s3, b, k, **kw):
    return s3.get_object(Bucket=b, Key=k, **kw)["Body"].read()


def b64(raw):
    return base64.b64encode(raw).decode()


def crc32(data):
    return b64(zlib.crc32(data).to_bytes(4, "big"))


def sha256(data):
    return b64(hashlib.sha256(data).digest())


def crc64nvme(data):
    poly = 0x9a6c9329ac4bc9b5
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        table.append(c)
    c = 0xFFFFFFFFFFFFFFFF
    for b in data:
        c = table[(c ^ b) & 0xff] ^ (c >> 8)
    return b64((c ^ 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big"))


# =============================================================================================
# versioning
# =============================================================================================

def test_versioning_config_roundtrip(s3, bucket):
    assert "Status" not in s3.get_bucket_versioning(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    assert s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    assert s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Suspended"
    with pytest.raises(ClientError) as e:
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Bogus"})
    assert code(e)[0] == "MalformedXML"


def test_versions_get_head_delete_markers_and_promotion(s3, vbucket):
    b = vbucket
    v1 = s3.put_object(Bucket=b, Key="k", Body=b"one")["VersionId"]
    v2 = s3.put_object(Bucket=b, Key="k", Body=b"two")["VersionId"]
    assert v1 != v2 and len(v1) >= 16
    assert body_of(s3, b, "k") == b"two"
    assert body_of(s3, b, "k", VersionId=v1) == b"one"
    assert s3.head_object(Bucket=b, Key="k", VersionId=v1)["VersionId"] == v1
    lv = s3.list_object_versions(Bucket=b)
    assert [(v["VersionId"], v["IsLatest"]) for v in lv["Versions"]] == [(v2, True), (v1, False)]
    # delete without a version id creates a delete marker
    d = s3.delete_object(Bucket=b, Key="k")
    assert d["DeleteMarker"] is True and d["VersionId"]
    marker = d["VersionId"]
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="k")
    assert code(e) == ("NoSuchKey", 404)
    assert e.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amz-delete-marker") == "true"
    assert s3.list_objects_v2(Bucket=b).get("KeyCount") == 0
    lv = s3.list_object_versions(Bucket=b)
    assert [m["VersionId"] for m in lv["DeleteMarkers"]] == [marker] and lv["DeleteMarkers"][0]["IsLatest"] is True
    assert all(not v["IsLatest"] for v in lv["Versions"])
    # GET of the marker by id is MethodNotAllowed
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="k", VersionId=marker)
    assert code(e)[1] == 405
    # removing the marker brings the newest version back (promotion)
    r = s3.delete_object(Bucket=b, Key="k", VersionId=marker)
    assert r["DeleteMarker"] is True
    assert body_of(s3, b, "k") == b"two"
    assert s3.list_objects_v2(Bucket=b)["KeyCount"] == 1
    # deleting the current version by id promotes the older one
    s3.delete_object(Bucket=b, Key="k", VersionId=v2)
    assert body_of(s3, b, "k") == b"one"
    assert s3.head_object(Bucket=b, Key="k")["VersionId"] == v1
    s3.delete_object(Bucket=b, Key="k", VersionId=v1)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="k")
    assert code(e) == ("NoSuchKey", 404)
    assert "Versions" not in s3.list_object_versions(Bucket=b)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="k", VersionId=v1)
    assert code(e) == ("NoSuchVersion", 404)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="k", VersionId="not a version id")
    assert code(e) == ("InvalidArgument", 400)
    # now the bucket is empty of versions and can be deleted
    s3.delete_bucket(Bucket=b)


def test_null_version_semantics_and_suspension(s3, bucket):
    b = bucket
    r = s3.put_object(Bucket=b, Key="k", Body=b"pre")
    assert "VersionId" not in r  # never-versioned bucket: no version header
    lv = s3.list_object_versions(Bucket=b)
    assert [(v["VersionId"], v["IsLatest"]) for v in lv["Versions"]] == [("null", True)]
    s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=b, Key="k", Body=b"v1")["VersionId"]
    assert body_of(s3, b, "k", VersionId="null") == b"pre"
    s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Suspended"})
    r = s3.put_object(Bucket=b, Key="k", Body=b"suspended")
    assert r["VersionId"] == "null"
    r2 = s3.put_object(Bucket=b, Key="k", Body=b"suspended2")  # replaces the null version
    assert r2["VersionId"] == "null"
    ids = [v["VersionId"] for v in s3.list_object_versions(Bucket=b)["Versions"]]
    assert ids == ["null", v1] and len(ids) == 2  # old null version was replaced, v1 survives
    assert body_of(s3, b, "k") == b"suspended2" and body_of(s3, b, "k", VersionId=v1) == b"v1"
    d = s3.delete_object(Bucket=b, Key="k")
    assert d["DeleteMarker"] is True and d["VersionId"] == "null"
    assert [v["VersionId"] for v in s3.list_object_versions(Bucket=b)["Versions"]] == [v1]


def test_list_object_versions_paging_prefix_delimiter(s3, env, vbucket):
    b = vbucket
    keys = [f"d{i % 3}/k{i:02d}" for i in range(12)] + ["top"]
    expected = []
    for k in keys:
        for n in range(3):
            vid = s3.put_object(Bucket=b, Key=k, Body=f"{k}-{n}".encode())["VersionId"]
            expected.append((k, vid))
    s3.delete_object(Bucket=b, Key="d0/k00")
    got, km, vm = [], None, None
    while True:
        kw = {"Bucket": b, "MaxKeys": 7}
        if km:
            kw.update(KeyMarker=km, VersionIdMarker=vm)
        page = s3.list_object_versions(**kw)
        vkeys = [v["Key"] for v in page.get("Versions", [])]
        assert vkeys == sorted(vkeys)  # key order (S3 lists Versions and DeleteMarkers as separate ordered lists)
        assert len(vkeys) + len(page.get("DeleteMarkers", [])) <= 7
        got += [(v["Key"], v["VersionId"]) for v in page.get("Versions", [])]
        got += [(m["Key"], m["VersionId"]) for m in page.get("DeleteMarkers", [])]
        if not page["IsTruncated"]:
            break
        km, vm = page["NextKeyMarker"], page["NextVersionIdMarker"]
    assert len(got) == len(set(got)) == len(expected) + 1
    by_key = {}
    for k, v in got:
        by_key.setdefault(k, []).append(v)
    for k in keys:  # versions of one key are newest first
        exp = [v for kk, v in expected if kk == k][::-1]
        have = [v for v in by_key[k] if v in exp]
        assert have == exp, k
    page = s3.list_object_versions(Bucket=b, Delimiter="/")
    assert [p["Prefix"] for p in page["CommonPrefixes"]] == ["d0/", "d1/", "d2/"]
    assert [v["Key"] for v in page["Versions"]] == ["top"] * 3
    page = s3.list_object_versions(Bucket=b, Prefix="d1/")
    assert {v["Key"] for v in page["Versions"]} == {f"d1/k{i:02d}" for i in range(12) if i % 3 == 1}


def test_versioned_copy_and_copy_source_version(s3, vbucket):
    b = vbucket
    v1 = s3.put_object(Bucket=b, Key="src", Body=b"first")["VersionId"]
    s3.put_object(Bucket=b, Key="src", Body=b"second")
    r = s3.copy_object(Bucket=b, Key="dst", CopySource={"Bucket": b, "Key": "src", "VersionId": v1})
    assert r["CopySourceVersionId"] == v1 and r["VersionId"]
    assert body_of(s3, b, "dst") == b"first"
    r = s3.copy_object(Bucket=b, Key="dst", CopySource={"Bucket": b, "Key": "src"})
    assert body_of(s3, b, "dst") == b"second"
    assert len(s3.list_object_versions(Bucket=b, Prefix="dst")["Versions"]) == 2
    # self copy in a versioned bucket creates a new version
    s3.copy_object(Bucket=b, Key="dst", CopySource={"Bucket": b, "Key": "dst"}, MetadataDirective="REPLACE",
                   Metadata={"m": "1"})
    assert len(s3.list_object_versions(Bucket=b, Prefix="dst")["Versions"]) == 3
    assert s3.head_object(Bucket=b, Key="dst")["Metadata"] == {"m": "1"}
    d = s3.delete_object(Bucket=b, Key="src")
    with pytest.raises(ClientError) as e:  # copying a delete marker
        s3.copy_object(Bucket=b, Key="x", CopySource={"Bucket": b, "Key": "src"})
    assert code(e) == ("NoSuchKey", 404)
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=b, Key="x", CopySource={"Bucket": b, "Key": "src", "VersionId": d["VersionId"]})
    assert code(e) == ("InvalidRequest", 400)


def test_delete_objects_with_versions_and_markers(s3, vbucket):
    b = vbucket
    v = [s3.put_object(Bucket=b, Key=f"k{i}", Body=b"x")["VersionId"] for i in range(3)]
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": "k0"}, {"Key": "k1", "VersionId": v[1]}, {"Key": "k2"}]})
    dm = {d["Key"]: d for d in r["Deleted"]}
    assert dm["k0"]["DeleteMarker"] is True and dm["k0"]["DeleteMarkerVersionId"]
    assert dm["k1"]["VersionId"] == v[1] and "DeleteMarker" not in dm["k1"]
    lv = s3.list_object_versions(Bucket=b)
    assert {m["Key"] for m in lv["DeleteMarkers"]} == {"k0", "k2"}
    assert {x["Key"] for x in lv["Versions"]} == {"k0", "k2"}  # k1's only version is gone


def test_versions_live_on_the_owning_shard(env, s3, vbucket):
    if len(env.pgs) < 2:
        pytest.skip("two-backend only")
    b = vbucket
    for i in range(40):
        s3.put_object(Bucket=b, Key=f"s{i}", Body=b"1")
        s3.put_object(Bucket=b, Key=f"s{i}", Body=b"22")
    for i in range(40):
        counts = [sql(pg, "SELECT (SELECT count(*) FROM warp_s3_objects WHERE bucket=%s AND key=%s)"
                          " + (SELECT count(*) FROM warp_s3_versions WHERE bucket=%s AND key=%s)",
                      (b, f"s{i}", b, f"s{i}"))[0][0] for pg in env.pgs]
        assert sorted(counts) == [0, 2], (i, counts)  # both versions on one shard only
    assert all(sql(pg, "SELECT count(*) FROM warp_s3_objects WHERE bucket=%s", (b,))[0][0] > 5 for pg in env.pgs)


# =============================================================================================
# tagging
# =============================================================================================

def test_object_tagging_roundtrip_and_header(s3, bucket):
    b = bucket
    s3.put_object(Bucket=b, Key="t", Body=b"x", Tagging="env=prod&team=core%20dev")
    tags = {t["Key"]: t["Value"] for t in s3.get_object_tagging(Bucket=b, Key="t")["TagSet"]}
    assert tags == {"env": "prod", "team": "core dev"}
    assert s3.get_object(Bucket=b, Key="t")["TagCount"] == 2
    assert s3.head_object(Bucket=b, Key="t")["ResponseMetadata"]["HTTPHeaders"]["x-amz-tagging-count"] == "2"
    s3.put_object_tagging(Bucket=b, Key="t", Tagging={"TagSet": [{"Key": "a", "Value": "1"}]})
    assert s3.get_object_tagging(Bucket=b, Key="t")["TagSet"] == [{"Key": "a", "Value": "1"}]
    s3.delete_object_tagging(Bucket=b, Key="t")
    assert s3.get_object_tagging(Bucket=b, Key="t")["TagSet"] == []
    assert "TagCount" not in s3.get_object(Bucket=b, Key="t")
    with pytest.raises(ClientError) as e:
        s3.get_object_tagging(Bucket=b, Key="missing")
    assert code(e) == ("NoSuchKey", 404)
    with pytest.raises(ClientError) as e:
        s3.put_object_tagging(Bucket=b, Key="t", Tagging={"TagSet": [{"Key": f"k{i}", "Value": "v"} for i in range(11)]})
    assert code(e) == ("BadRequest", 400)
    with pytest.raises(ClientError) as e:
        s3.put_object_tagging(Bucket=b, Key="t", Tagging={"TagSet": [{"Key": "aws:x", "Value": "v"}]})
    assert code(e)[0] == "InvalidTag"
    with pytest.raises(ClientError) as e:
        s3.put_object_tagging(Bucket=b, Key="t", Tagging={"TagSet": [{"Key": "k", "Value": "v" * 257}]})
    assert code(e)[0] == "InvalidTag"


def test_tagging_is_per_version_and_copy_directives(s3, vbucket):
    b = vbucket
    v1 = s3.put_object(Bucket=b, Key="k", Body=b"1", Tagging="gen=1")["VersionId"]
    v2 = s3.put_object(Bucket=b, Key="k", Body=b"2", Tagging="gen=2")["VersionId"]
    assert s3.get_object_tagging(Bucket=b, Key="k", VersionId=v1)["TagSet"] == [{"Key": "gen", "Value": "1"}]
    assert s3.get_object_tagging(Bucket=b, Key="k")["VersionId"] == v2
    s3.put_object_tagging(Bucket=b, Key="k", VersionId=v1, Tagging={"TagSet": [{"Key": "gen", "Value": "one"}]})
    assert s3.get_object_tagging(Bucket=b, Key="k")["TagSet"] == [{"Key": "gen", "Value": "2"}]
    s3.copy_object(Bucket=b, Key="c1", CopySource={"Bucket": b, "Key": "k"})
    assert s3.get_object_tagging(Bucket=b, Key="c1")["TagSet"] == [{"Key": "gen", "Value": "2"}]
    s3.copy_object(Bucket=b, Key="c2", CopySource={"Bucket": b, "Key": "k"}, TaggingDirective="REPLACE", Tagging="new=yes")
    assert s3.get_object_tagging(Bucket=b, Key="c2")["TagSet"] == [{"Key": "new", "Value": "yes"}]


def test_bucket_tagging_and_no_such_tag_set(s3, bucket):
    with pytest.raises(ClientError) as e:
        s3.get_bucket_tagging(Bucket=bucket)
    assert code(e) == ("NoSuchTagSet", 404)
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": [{"Key": "team", "Value": "x"}]})
    assert s3.get_bucket_tagging(Bucket=bucket)["TagSet"] == [{"Key": "team", "Value": "x"}]
    s3.delete_bucket_tagging(Bucket=bucket)
    with pytest.raises(ClientError) as e:
        s3.get_bucket_tagging(Bucket=bucket)
    assert code(e)[0] == "NoSuchTagSet"


# =============================================================================================
# ACLs, public access block, ownership, policy, bucket configuration documents
# =============================================================================================

def test_bucket_and_object_acls(s3, bucket):
    b = bucket
    acl = s3.get_bucket_acl(Bucket=b)
    assert acl["Owner"]["ID"] and [g["Permission"] for g in acl["Grants"]] == ["FULL_CONTROL"]
    s3.put_bucket_acl(Bucket=b, ACL="public-read")
    grants = s3.get_bucket_acl(Bucket=b)["Grants"]
    assert {(g["Grantee"].get("URI"), g["Permission"]) for g in grants} >= {
        ("http://acs.amazonaws.com/groups/global/AllUsers", "READ")}
    s3.put_object(Bucket=b, Key="o", Body=b"x", ACL="authenticated-read")
    og = s3.get_object_acl(Bucket=b, Key="o")["Grants"]
    assert any(g["Grantee"].get("URI", "").endswith("AuthenticatedUsers") for g in og)
    s3.put_object_acl(Bucket=b, Key="o", GrantRead='uri="http://acs.amazonaws.com/groups/global/AllUsers"')
    og = s3.get_object_acl(Bucket=b, Key="o")["Grants"]
    assert {(g["Grantee"].get("URI"), g["Permission"]) for g in og} >= {
        ("http://acs.amazonaws.com/groups/global/AllUsers", "READ")}
    with pytest.raises(ClientError) as e:
        s3.put_object_acl(Bucket=b, Key="o", ACL="not-a-canned-acl")
    assert code(e)[1] == 400
    with pytest.raises(ClientError) as e:
        s3.get_object_acl(Bucket=b, Key="nope")
    assert code(e)[0] == "NoSuchKey"


def test_public_access_block_ownership_policy_and_enforcement(s3, bucket):
    b = bucket
    with pytest.raises(ClientError) as e:
        s3.get_public_access_block(Bucket=b)
    assert code(e) == ("NoSuchPublicAccessBlockConfiguration", 404)
    s3.put_public_access_block(Bucket=b, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": False, "BlockPublicPolicy": True, "RestrictPublicBuckets": False})
    cfg = s3.get_public_access_block(Bucket=b)["PublicAccessBlockConfiguration"]
    assert cfg == {"BlockPublicAcls": True, "IgnorePublicAcls": False, "BlockPublicPolicy": True,
                   "RestrictPublicBuckets": False}
    with pytest.raises(ClientError) as e:
        s3.put_bucket_acl(Bucket=b, ACL="public-read")
    assert code(e) == ("AccessDenied", 403)
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="p", Body=b"x", ACL="public-read")
    assert code(e)[1] == 403
    s3.put_object(Bucket=b, Key="p", Body=b"x")
    public = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
                                                     "Resource": f"arn:aws:s3:::{b}/*"}]}
    with pytest.raises(ClientError) as e:
        s3.put_bucket_policy(Bucket=b, Policy=json.dumps(public))
    assert code(e)[1] == 403
    private = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::111122223333:root"},
                                                      "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{b}/*"}]}
    s3.put_bucket_policy(Bucket=b, Policy=json.dumps(private))
    assert json.loads(s3.get_bucket_policy(Bucket=b)["Policy"]) == private
    assert s3.get_bucket_policy_status(Bucket=b)["PolicyStatus"]["IsPublic"] is False
    s3.delete_bucket_policy(Bucket=b)
    with pytest.raises(ClientError) as e:
        s3.get_bucket_policy(Bucket=b)
    assert code(e) == ("NoSuchBucketPolicy", 404)
    with pytest.raises(ClientError) as e:
        s3.put_bucket_policy(Bucket=b, Policy="{not json")
    assert code(e)[0] == "MalformedPolicy"
    s3.delete_public_access_block(Bucket=b)
    s3.put_bucket_acl(Bucket=b, ACL="public-read")  # allowed once the block is gone
    s3.put_bucket_ownership_controls(Bucket=b, OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]})
    assert s3.get_bucket_ownership_controls(Bucket=b)["OwnershipControls"]["Rules"] == [
        {"ObjectOwnership": "BucketOwnerEnforced"}]
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="q", Body=b"x", ACL="public-read")
    assert code(e)[0] == "AccessControlListNotSupported"
    s3.delete_bucket_ownership_controls(Bucket=b)
    with pytest.raises(ClientError) as e:
        s3.get_bucket_ownership_controls(Bucket=b)
    assert code(e)[0] == "OwnershipControlsNotFoundError"


def test_bucket_configuration_documents_roundtrip(s3, bucket):
    b = bucket
    for getter, expected in [(s3.get_bucket_cors, "NoSuchCORSConfiguration"),
                             (s3.get_bucket_lifecycle_configuration, "NoSuchLifecycleConfiguration"),
                             (s3.get_bucket_website, "NoSuchWebsiteConfiguration"),
                             (s3.get_bucket_encryption, "ServerSideEncryptionConfigurationNotFoundError"),
                             (s3.get_bucket_replication, "ReplicationConfigurationNotFoundError")]:
        with pytest.raises(ClientError) as e:
            getter(Bucket=b)
        assert code(e) == (expected, 404), expected
    lc = {"Rules": [{"ID": "expire", "Status": "Enabled", "Filter": {"Prefix": "logs/"}, "Expiration": {"Days": 7}}]}
    s3.put_bucket_lifecycle_configuration(Bucket=b, LifecycleConfiguration=lc)
    got = s3.get_bucket_lifecycle_configuration(Bucket=b)
    assert got["Rules"][0]["ID"] == "expire" and got["Rules"][0]["Expiration"] == {"Days": 7}
    assert got["ResponseMetadata"]["HTTPHeaders"]["x-amz-transition-default-minimum-object-size"] == "all_storage_classes_128K"
    s3.delete_bucket_lifecycle(Bucket=b)
    s3.put_bucket_website(Bucket=b, WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}})
    assert s3.get_bucket_website(Bucket=b)["IndexDocument"] == {"Suffix": "index.html"}
    enc = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}
    s3.put_bucket_encryption(Bucket=b, ServerSideEncryptionConfiguration=enc)
    assert s3.get_bucket_encryption(Bucket=b)["ServerSideEncryptionConfiguration"]["Rules"][0][
        "ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    s3.put_bucket_logging(Bucket=b, BucketLoggingStatus={})
    assert "LoggingEnabled" not in s3.get_bucket_logging(Bucket=b)
    s3.put_bucket_notification_configuration(Bucket=b, NotificationConfiguration={})
    assert s3.get_bucket_notification_configuration(Bucket=b)["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_bucket_request_payment(Bucket=b)["Payer"] == "BucketOwner"
    s3.put_bucket_accelerate_configuration(Bucket=b, AccelerateConfiguration={"Status": "Enabled"})
    assert s3.get_bucket_accelerate_configuration(Bucket=b)["Status"] == "Enabled"
    with pytest.raises(ClientError) as e:  # replication needs versioning
        s3.put_bucket_replication(Bucket=b, ReplicationConfiguration={
            "Role": "arn:aws:iam::123456789012:role/r", "Rules": [{"Status": "Enabled", "Priority": 1,
                                                                    "Filter": {}, "Destination": {"Bucket": "arn:aws:s3:::d"},
                                                                    "DeleteMarkerReplication": {"Status": "Disabled"}}]})
    assert code(e)[1] == 400
    ac = {"Id": "a1", "StorageClassAnalysis": {}}
    s3.put_bucket_analytics_configuration(Bucket=b, Id="a1", AnalyticsConfiguration=ac)
    assert s3.get_bucket_analytics_configuration(Bucket=b, Id="a1")["AnalyticsConfiguration"]["Id"] == "a1"
    assert [c["Id"] for c in s3.list_bucket_analytics_configurations(Bucket=b)["AnalyticsConfigurationList"]] == ["a1"]
    s3.delete_bucket_analytics_configuration(Bucket=b, Id="a1")
    with pytest.raises(ClientError) as e:
        s3.get_bucket_analytics_configuration(Bucket=b, Id="a1")
    assert code(e)[0] == "NoSuchConfiguration"
    it = {"Id": "tier", "Status": "Enabled", "Tierings": [{"Days": 90, "AccessTier": "ARCHIVE_ACCESS"}]}
    s3.put_bucket_intelligent_tiering_configuration(Bucket=b, Id="tier", IntelligentTieringConfiguration=it)
    assert s3.get_bucket_intelligent_tiering_configuration(Bucket=b, Id="tier")["IntelligentTieringConfiguration"]["Id"] == "tier"


def test_default_encryption_and_sse_headers(s3, bucket):
    b = bucket
    assert s3.put_object(Bucket=b, Key="a", Body=b"x")["ServerSideEncryption"] == "AES256"
    assert s3.head_object(Bucket=b, Key="a")["ServerSideEncryption"] == "AES256"
    r = s3.put_object(Bucket=b, Key="k", Body=b"x", ServerSideEncryption="aws:kms", SSEKMSKeyId="arn:aws:kms:us-east-1:1:key/abc")
    assert r["ServerSideEncryption"] == "aws:kms" and r["SSEKMSKeyId"].endswith("key/abc")
    assert s3.head_object(Bucket=b, Key="k")["SSEKMSKeyId"].endswith("key/abc")
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="c", Body=b"x", SSECustomerAlgorithm="AES256", SSECustomerKey="a" * 32)
    assert code(e)[1] in (400, 501)  # SSE-C needs TLS on S3
    s3.put_object(Bucket=b, Key="sc", Body=b"x", StorageClass="STANDARD_IA")
    assert s3.head_object(Bucket=b, Key="sc")["StorageClass"] == "STANDARD_IA"
    assert s3.list_objects_v2(Bucket=b, Prefix="sc")["Contents"][0]["StorageClass"] == "STANDARD_IA"
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="bad", Body=b"x", StorageClass="NOPE")
    assert code(e) == ("InvalidStorageClass", 400)


def test_object_lock_and_retention(s3):
    b = new_bucket(s3, ObjectLockEnabledForBucket=True)
    assert s3.get_bucket_versioning(Bucket=b)["Status"] == "Enabled"
    assert s3.get_object_lock_configuration(Bucket=b)["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"
    until = datetime.now(timezone.utc) + timedelta(days=1)
    r = s3.put_object(Bucket=b, Key="k", Body=b"x", ObjectLockMode="GOVERNANCE", ObjectLockRetainUntilDate=until)
    vid = r["VersionId"]
    ret = s3.get_object_retention(Bucket=b, Key="k")["Retention"]
    assert ret["Mode"] == "GOVERNANCE"
    with pytest.raises(ClientError) as e:
        s3.delete_object(Bucket=b, Key="k", VersionId=vid)
    assert code(e) == ("AccessDenied", 403)
    s3.delete_object(Bucket=b, Key="k", VersionId=vid, BypassGovernanceRetention=True)
    s3.put_object(Bucket=b, Key="h", Body=b"x")
    s3.put_object_legal_hold(Bucket=b, Key="h", LegalHold={"Status": "ON"})
    assert s3.get_object_legal_hold(Bucket=b, Key="h")["LegalHold"]["Status"] == "ON"
    hv = s3.head_object(Bucket=b, Key="h")["VersionId"]
    with pytest.raises(ClientError) as e:
        s3.delete_object(Bucket=b, Key="h", VersionId=hv)
    assert code(e)[0] == "AccessDenied"
    s3.put_object_legal_hold(Bucket=b, Key="h", LegalHold={"Status": "OFF"})
    s3.delete_object(Bucket=b, Key="h", VersionId=hv)
    plain = new_bucket(s3)
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=plain, Key="k", Body=b"x", ObjectLockMode="GOVERNANCE", ObjectLockRetainUntilDate=until)
    assert code(e)[0] == "InvalidRequest"


# =============================================================================================
# CORS
# =============================================================================================

def raw(env, method, path, headers=None, data=None):
    return requests.request(method, env.endpoint + path, headers=headers or {}, data=data, timeout=30)


def test_cors_preflight_and_actual_requests(env, s3, bucket):
    b = bucket
    assert raw(env, "OPTIONS", f"/{b}/o", {"Origin": "http://a.example", "Access-Control-Request-Method": "GET"}).status_code == 403
    s3.put_bucket_cors(Bucket=b, CORSConfiguration={"CORSRules": [
        {"AllowedOrigins": ["http://*.example.com"], "AllowedMethods": ["GET", "PUT"], "AllowedHeaders": ["x-amz-*", "content-type"],
         "ExposeHeaders": ["ETag"], "MaxAgeSeconds": 600},
        {"AllowedOrigins": ["*"], "AllowedMethods": ["GET"]}]})
    assert s3.get_bucket_cors(Bucket=b)["CORSRules"][0]["MaxAgeSeconds"] == 600
    r = raw(env, "OPTIONS", f"/{b}/o", {"Origin": "http://app.example.com", "Access-Control-Request-Method": "PUT",
                                        "Access-Control-Request-Headers": "x-amz-meta-a, content-type"})
    assert r.status_code == 200
    assert r.headers["Access-Control-Allow-Origin"] == "http://app.example.com"
    assert r.headers["Access-Control-Max-Age"] == "600" and "PUT" in r.headers["Access-Control-Allow-Methods"]
    assert "Origin" in r.headers["Vary"]
    r = raw(env, "OPTIONS", f"/{b}/o", {"Origin": "http://app.example.com", "Access-Control-Request-Method": "DELETE"})
    assert r.status_code == 403 and "<Code>AccessForbidden</Code>" in r.text
    r = raw(env, "OPTIONS", f"/{b}/o", {"Origin": "http://elsewhere.io", "Access-Control-Request-Method": "GET"})
    assert r.status_code == 200 and r.headers["Access-Control-Allow-Origin"] == "*"
    r = raw(env, "OPTIONS", f"/{b}/o", {"Access-Control-Request-Method": "GET"})
    assert r.status_code == 400 and "Access-Control-Allow-Origin" not in r.headers
    # actual request: CORS headers ride on the response, even an authorization failure
    r = raw(env, "GET", f"/{b}/o", {"Origin": "http://app.example.com"})
    assert r.status_code == 403 and r.headers["Access-Control-Allow-Origin"] == "http://app.example.com"
    assert r.headers["Access-Control-Expose-Headers"] == "ETag"
    s3.put_object(Bucket=b, Key="o", Body=b"x")
    url = s3.generate_presigned_url("get_object", Params={"Bucket": b, "Key": "o"}, ExpiresIn=60)
    r = requests.get(url, headers={"Origin": "http://app.example.com"})
    assert r.status_code == 200 and r.headers["Access-Control-Allow-Origin"] == "http://app.example.com"
    assert "Access-Control-Allow-Origin" not in requests.get(url).headers
    s3.delete_bucket_cors(Bucket=b)
    assert raw(env, "OPTIONS", f"/{b}/o", {"Origin": "http://a.example", "Access-Control-Request-Method": "GET"}).status_code == 403
    with pytest.raises(ClientError) as e:
        s3.put_bucket_cors(Bucket=b, CORSConfiguration={"CORSRules": [{"AllowedOrigins": ["*"], "AllowedMethods": ["PATCH"]}]})
    assert code(e)[0] == "InvalidRequest"


# =============================================================================================
# checksums
# =============================================================================================

try:
    import awscrt  # noqa: F401
    HAVE_CRT = True
except ImportError:
    HAVE_CRT = False
needs_crt = pytest.mark.skipif(not HAVE_CRT, reason="botocore needs awscrt to send CRC64NVME")


@pytest.mark.parametrize("algo,fn", [("CRC32", crc32), ("SHA256", sha256),
                                     pytest.param("CRC64NVME", crc64nvme, marks=needs_crt)])
def test_put_with_checksum_algorithm_roundtrip(s3, bucket, algo, fn):
    data = os.urandom(300_000)
    r = s3.put_object(Bucket=bucket, Key="c", Body=data, ChecksumAlgorithm=algo)
    assert r["Checksum" + algo] == fn(data)
    h = s3.head_object(Bucket=bucket, Key="c", ChecksumMode="ENABLED")
    assert h["Checksum" + algo] == fn(data) and h["ChecksumType"] == "FULL_OBJECT"
    assert "ChecksumCRC32" not in s3.head_object(Bucket=bucket, Key="c") or algo == "CRC32"
    g = s3.get_object(Bucket=bucket, Key="c", ChecksumMode="ENABLED")
    assert g["Body"].read() == data and g["Checksum" + algo] == fn(data)
    attrs = s3.get_object_attributes(Bucket=bucket, Key="c", ObjectAttributes=["ETag", "Checksum", "ObjectSize", "StorageClass"])
    assert attrs["Checksum"]["Checksum" + algo] == fn(data) and attrs["ObjectSize"] == len(data)
    assert attrs["ETag"] == hashlib.md5(data).hexdigest() and attrs["StorageClass"] == "STANDARD"


def test_default_checksum_is_crc64nvme_and_mismatch_is_bad_digest(s3, bucket):
    data = b"hello checksums"
    plain = boto3.client("s3", endpoint_url=s3.meta.endpoint_url, region_name="us-east-1", aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         config=Config(s3={"addressing_style": "path"}, request_checksum_calculation="when_required",
                                       response_checksum_validation="when_required"))
    r = plain.put_object(Bucket=bucket, Key="d", Body=data)
    assert r["ChecksumCRC64NVME"] == crc64nvme(data)
    assert plain.head_object(Bucket=bucket, Key="d", ChecksumMode="ENABLED")["ChecksumCRC64NVME"] == crc64nvme(data)
    with pytest.raises(ClientError) as e:
        plain.put_object(Bucket=bucket, Key="bad", Body=data, ChecksumSHA256=sha256(b"other"))
    assert code(e) == ("BadDigest", 400)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bucket, Key="bad")
    assert code(e)[0] == "NoSuchKey"  # the rejected write left nothing behind
    with pytest.raises(ClientError) as e:
        plain.put_object(Bucket=bucket, Key="bad", Body=data, ChecksumCRC32="not-base64!!")
    assert code(e)[1] == 400
    with pytest.raises(ClientError) as e:
        plain.put_object(Bucket=bucket, Key="md5", Body=data, ContentMD5=b64(hashlib.md5(b"x").digest()))
    assert code(e) == ("BadDigest", 400)


def test_streaming_trailer_checksums_are_verified(env, s3, bucket):
    # boto3's default is an aws-chunked upload with a trailing CRC32 (STREAMING-UNSIGNED-PAYLOAD-TRAILER over HTTP)
    data = os.urandom(3 * MIB + 17)
    r = s3.put_object(Bucket=bucket, Key="tr", Body=io.BytesIO(data))
    assert r["ChecksumCRC32"] == crc32(data)
    assert body_of(s3, bucket, "tr") == data
    # a wrong trailing checksum must be rejected: craft the framing by hand and sign it as the SDK would
    url = f"{env.endpoint}/{bucket}/bad-trailer"
    payload = f"{len(data):x}\r\n".encode() + data + b"\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"
    headers = {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "x-amz-trailer": "x-amz-checksum-crc32",
               "x-amz-decoded-content-length": str(len(data)), "Content-Encoding": "aws-chunked",
               "Content-Length": str(len(payload))}
    req = AWSRequest(method="PUT", url=url, data=b"", headers=headers)
    SigV4Auth(Credentials(KEY, SECRET), "s3", "us-east-1").add_auth(req)
    r = requests.put(url, data=payload, headers=dict(req.headers))
    assert r.status_code == 400 and "<Code>BadDigest</Code>" in r.text, r.text
    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=bucket, Key="bad-trailer")
    assert code(e)[1] == 404


def _signed_stream(env, bucket, key, chunks, trailer_crc=None, tamper=False, amz_trailer=True):
    """PUT with STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]: every chunk (and the trailer) carries a valid SigV4 chunk signature."""
    import hmac
    import datetime as dt
    data = b"".join(chunks)
    variant = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" if trailer_crc else "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    url = f"{env.endpoint}/{bucket}/{key}"
    headers = {"x-amz-content-sha256": variant, "x-amz-decoded-content-length": str(len(data)),
               "Content-Encoding": "aws-chunked"}
    if trailer_crc:
        headers["x-amz-trailer"] = "x-amz-checksum-crc32"
    req = AWSRequest(method="PUT", url=url, data=b"", headers=headers)
    SigV4Auth(Credentials(KEY, SECRET), "s3", "us-east-1").add_auth(req)
    auth = req.headers["Authorization"]
    seed = auth.rsplit("Signature=", 1)[1]
    amz_date = req.headers["X-Amz-Date"]
    day = amz_date[:8]
    scope = f"{day}/us-east-1/s3/aws4_request"
    k = hmac.new(("AWS4" + SECRET).encode(), day.encode(), hashlib.sha256).digest()
    for part in ("us-east-1", "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    empty = hashlib.sha256(b"").hexdigest()
    prev = seed
    body = b""
    for c in chunks + [b""]:
        sts = "\n".join(["AWS4-HMAC-SHA256-PAYLOAD", amz_date, scope, prev, empty, hashlib.sha256(c).hexdigest()])
        prev = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        body += f"{len(c):x};chunk-signature={prev}\r\n".encode() + c + b"\r\n"
    if trailer_crc:
        text = f"x-amz-checksum-crc32:{trailer_crc}\n"
        sts = "\n".join(["AWS4-HMAC-SHA256-TRAILER", amz_date, scope, prev, hashlib.sha256(text.encode()).hexdigest()])
        tsig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        body = body[:-2] + f"x-amz-checksum-crc32:{trailer_crc}\r\nx-amz-trailer-signature:{tsig}\r\n\r\n".encode()
    if tamper:  # flip a payload byte after signing
        i = body.index(chunks[0])
        body = body[:i] + bytes([body[i] ^ 1]) + body[i + 1:]
    hdrs = dict(req.headers)
    hdrs["Content-Length"] = str(len(body))
    return requests.put(url, data=body, headers=hdrs)


def test_signed_streaming_payload_chunk_signatures_are_verified(env, s3, bucket):
    parts = [os.urandom(70_000), os.urandom(5000)]
    r = _signed_stream(env, bucket, "signed", parts)
    assert r.status_code == 200, r.text
    assert body_of(s3, bucket, "signed") == b"".join(parts)
    r = _signed_stream(env, bucket, "signed-tamper", parts, tamper=True)
    assert r.status_code == 403 and "SignatureDoesNotMatch" in r.text, r.text
    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=bucket, Key="signed-tamper")
    assert code(e)[1] == 404
    r = _signed_stream(env, bucket, "signed-trailer", parts, trailer_crc=crc32(b"".join(parts)))
    assert r.status_code == 200, r.text
    assert s3.head_object(Bucket=bucket, Key="signed-trailer", ChecksumMode="ENABLED")["ChecksumCRC32"] == crc32(b"".join(parts))
    r = _signed_stream(env, bucket, "signed-trailer-bad", parts, trailer_crc="AAAAAA==")
    assert r.status_code == 400 and "BadDigest" in r.text, r.text


# =============================================================================================
# multipart
# =============================================================================================

def _upload(s3, b, k, parts, **create):
    uid = s3.create_multipart_upload(Bucket=b, Key=k, **create)["UploadId"]
    done = []
    for i, data in enumerate(parts, 1):
        r = s3.upload_part(Bucket=b, Key=k, UploadId=uid, PartNumber=i, Body=data, **(
            {"ChecksumAlgorithm": create["ChecksumAlgorithm"]} if "ChecksumAlgorithm" in create else {}))
        p = {"PartNumber": i, "ETag": r["ETag"]}
        for f in ("ChecksumCRC32", "ChecksumSHA256", "ChecksumCRC32C", "ChecksumSHA1"):
            if f in r:
                p[f] = r[f]
        done.append(p)
    return uid, done


def test_list_parts_and_list_multipart_uploads(s3, bucket):
    b = bucket
    uid, done = _upload(s3, b, "a/multi", [b"a" * 100, b"b" * 200, b"c" * 300], Metadata={"m": "1"})
    uid2 = s3.create_multipart_upload(Bucket=b, Key="a/other")["UploadId"]
    uid3 = s3.create_multipart_upload(Bucket=b, Key="b/third")["UploadId"]
    lp = s3.list_parts(Bucket=b, Key="a/multi", UploadId=uid)
    assert [(p["PartNumber"], p["Size"]) for p in lp["Parts"]] == [(1, 100), (2, 200), (3, 300)]
    assert lp["Parts"][0]["ETag"] == done[0]["ETag"] and lp["StorageClass"] == "STANDARD" and lp["Owner"]["ID"]
    page = s3.list_parts(Bucket=b, Key="a/multi", UploadId=uid, MaxParts=2)
    assert page["IsTruncated"] and page["NextPartNumberMarker"] == 2 and len(page["Parts"]) == 2
    page = s3.list_parts(Bucket=b, Key="a/multi", UploadId=uid, PartNumberMarker=2)
    assert [p["PartNumber"] for p in page["Parts"]] == [3] and not page["IsTruncated"]
    with pytest.raises(ClientError) as e:
        s3.list_parts(Bucket=b, Key="a/multi", UploadId="nope")
    assert code(e) == ("NoSuchUpload", 404)
    lu = s3.list_multipart_uploads(Bucket=b)
    assert {(u["Key"], u["UploadId"]) for u in lu["Uploads"]} == {("a/multi", uid), ("a/other", uid2), ("b/third", uid3)}
    assert [u["Key"] for u in lu["Uploads"]] == sorted(u["Key"] for u in lu["Uploads"])
    lu = s3.list_multipart_uploads(Bucket=b, Delimiter="/")
    assert [c["Prefix"] for c in lu["CommonPrefixes"]] == ["a/", "b/"] and not lu.get("Uploads")
    lu = s3.list_multipart_uploads(Bucket=b, Prefix="a/")
    assert {u["Key"] for u in lu["Uploads"]} == {"a/multi", "a/other"}
    first = s3.list_multipart_uploads(Bucket=b, MaxUploads=1)
    assert first["IsTruncated"] and len(first["Uploads"]) == 1
    second = s3.list_multipart_uploads(Bucket=b, MaxUploads=5, KeyMarker=first["NextKeyMarker"],
                                       UploadIdMarker=first["NextUploadIdMarker"])
    assert len(second["Uploads"]) == 2 and first["Uploads"][0]["UploadId"] not in [u["UploadId"] for u in second["Uploads"]]
    s3.abort_multipart_upload(Bucket=b, Key="a/other", UploadId=uid2)
    s3.abort_multipart_upload(Bucket=b, Key="b/third", UploadId=uid3)
    s3.abort_multipart_upload(Bucket=b, Key="a/multi", UploadId=uid)
    assert "Uploads" not in s3.list_multipart_uploads(Bucket=b)


def test_upload_part_copy_with_range_and_get_part_number(s3, bucket):
    b = bucket
    src = os.urandom(12 * MIB)
    s3.put_object(Bucket=b, Key="src", Body=src)
    uid = s3.create_multipart_upload(Bucket=b, Key="dst")["UploadId"]
    p1 = s3.upload_part_copy(Bucket=b, Key="dst", UploadId=uid, PartNumber=1, CopySource={"Bucket": b, "Key": "src"},
                             CopySourceRange=f"bytes=0-{6 * MIB - 1}")
    p2 = s3.upload_part_copy(Bucket=b, Key="dst", UploadId=uid, PartNumber=2, CopySource={"Bucket": b, "Key": "src"},
                             CopySourceRange=f"bytes={6 * MIB}-{12 * MIB - 1}")
    assert p1["CopyPartResult"]["ETag"] == f'"{hashlib.md5(src[:6 * MIB]).hexdigest()}"'
    done = s3.complete_multipart_upload(Bucket=b, Key="dst", UploadId=uid, MultipartUpload={"Parts": [
        {"PartNumber": 1, "ETag": p1["CopyPartResult"]["ETag"]}, {"PartNumber": 2, "ETag": p2["CopyPartResult"]["ETag"]}]})
    assert done["ETag"].endswith('-2"')
    assert body_of(s3, b, "dst") == src
    g = s3.get_object(Bucket=b, Key="dst", PartNumber=2)
    assert g["Body"].read() == src[6 * MIB:] and g["PartsCount"] == 2 and g["ContentRange"].startswith(f"bytes {6 * MIB}-")
    assert s3.head_object(Bucket=b, Key="dst", PartNumber=1)["ContentLength"] == 6 * MIB
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="dst", PartNumber=3)
    assert code(e)[1] == 416
    uid = s3.create_multipart_upload(Bucket=b, Key="dst3")["UploadId"]
    with pytest.raises(ClientError) as e:
        s3.upload_part_copy(Bucket=b, Key="dst3", UploadId=uid, PartNumber=1, CopySource={"Bucket": b, "Key": "src"},
                            CopySourceRange=f"bytes={20 * MIB}-{21 * MIB}")
    assert code(e)[1] in (400, 416)
    with pytest.raises(ClientError) as e:
        s3.upload_part_copy(Bucket=b, Key="dst3", UploadId=uid, PartNumber=1, CopySource={"Bucket": b, "Key": "nokey"})
    assert code(e) == ("NoSuchKey", 404)
    s3.abort_multipart_upload(Bucket=b, Key="dst3", UploadId=uid)
    # copy-source conditions on a part copy
    uid = s3.create_multipart_upload(Bucket=b, Key="dst2")["UploadId"]
    with pytest.raises(ClientError) as e:
        s3.upload_part_copy(Bucket=b, Key="dst2", UploadId=uid, PartNumber=1, CopySource={"Bucket": b, "Key": "src"},
                            CopySourceIfMatch='"nope"')
    assert code(e) == ("PreconditionFailed", 412)
    s3.abort_multipart_upload(Bucket=b, Key="dst2", UploadId=uid)


def test_multipart_composite_and_full_object_checksums(s3, bucket):
    b = bucket
    p1, p2 = os.urandom(5 * MIB), os.urandom(MIB)
    uid, done = _upload(s3, b, "sha", [p1, p2], ChecksumAlgorithm="SHA256")
    assert [d["ChecksumSHA256"] for d in done] == [sha256(p1), sha256(p2)]
    composite = b64(hashlib.sha256(hashlib.sha256(p1).digest() + hashlib.sha256(p2).digest()).digest()) + "-2"
    r = s3.complete_multipart_upload(Bucket=b, Key="sha", UploadId=uid, MultipartUpload={"Parts": done})
    assert r["ChecksumSHA256"] == composite and r["ChecksumType"] == "COMPOSITE"
    h = s3.head_object(Bucket=b, Key="sha", ChecksumMode="ENABLED")
    assert h["ChecksumSHA256"] == composite and h["ETag"].endswith('-2"')
    at = s3.get_object_attributes(Bucket=b, Key="sha", ObjectAttributes=["Checksum", "ObjectParts", "ETag"])
    assert at["Checksum"]["ChecksumSHA256"] == composite[:-2] and at["ObjectParts"]["TotalPartsCount"] == 2
    assert [p["ChecksumSHA256"] for p in at["ObjectParts"]["Parts"]] == [sha256(p1), sha256(p2)]
    # a composite upload's Complete must carry each part checksum
    uid, done = _upload(s3, b, "strict", [p2], ChecksumAlgorithm="SHA256")
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=b, Key="strict", UploadId=uid,
                                     MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": done[0]["ETag"]}]})
    assert code(e)[0] == "InvalidRequest" and "must include the checksum for each part" in e.value.response["Error"]["Message"]
    s3.abort_multipart_upload(Bucket=b, Key="strict", UploadId=uid)
    # FULL_OBJECT CRC32: combined from the parts, equals the checksum of the whole
    uid, done = _upload(s3, b, "full", [p1, p2], ChecksumAlgorithm="CRC32", ChecksumType="FULL_OBJECT")
    r = s3.complete_multipart_upload(Bucket=b, Key="full", UploadId=uid, ChecksumType="FULL_OBJECT",
                                     MultipartUpload={"Parts": done})
    assert r["ChecksumCRC32"] == crc32(p1 + p2) and r["ChecksumType"] == "FULL_OBJECT"
    with pytest.raises(ClientError) as e:
        uid, done = _upload(s3, b, "wrongfull", [p1, p2], ChecksumAlgorithm="CRC32", ChecksumType="FULL_OBJECT")
        s3.complete_multipart_upload(Bucket=b, Key="wrongfull", UploadId=uid, ChecksumCRC32="AAAAAA==",
                                     MultipartUpload={"Parts": done})
    assert code(e) == ("BadDigest", 400)
    if HAVE_CRT:  # CRC64NVME (always FULL_OBJECT) combines too
        uid, done = _upload(s3, b, "crc64", [p1, p2], ChecksumAlgorithm="CRC64NVME")
        r = s3.complete_multipart_upload(Bucket=b, Key="crc64", UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": d["PartNumber"], "ETag": d["ETag"]} for d in done]})
        assert r["ChecksumCRC64NVME"] == crc64nvme(p1 + p2)
    # copying the multipart object writes a single object: plain MD5 ETag, FULL_OBJECT checksum
    c = s3.copy_object(Bucket=b, Key="sha-copy", CopySource={"Bucket": b, "Key": "sha"})
    assert c["CopyObjectResult"]["ETag"] == f'"{hashlib.md5(p1 + p2).hexdigest()}"'
    assert c["CopyObjectResult"]["ChecksumSHA256"] == sha256(p1 + p2) and c["CopyObjectResult"]["ChecksumType"] == "FULL_OBJECT"


def test_multipart_errors(s3, bucket):
    b = bucket
    uid, done = _upload(s3, b, "e", [b"tiny", b"tiny2"])
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=b, Key="e", UploadId=uid, MultipartUpload={"Parts": done})
    assert code(e) == ("EntityTooSmall", 400)
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=b, Key="e", UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 2, "ETag": done[1]["ETag"]}, {"PartNumber": 1, "ETag": done[0]["ETag"]}]})
    assert code(e) == ("InvalidPartOrder", 400)
    with pytest.raises(ClientError) as e:
        s3.complete_multipart_upload(Bucket=b, Key="e", UploadId=uid, MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": '"deadbeef"'}]})
    assert code(e) == ("InvalidPart", 400)
    with pytest.raises(ClientError) as e:
        s3.upload_part(Bucket=b, Key="e", UploadId=uid, PartNumber=10001, Body=b"x")
    assert code(e)[0] == "InvalidArgument"
    with pytest.raises(ClientError) as e:
        s3.upload_part(Bucket=b, Key="e", UploadId="bogus", PartNumber=1, Body=b"x")
    assert code(e) == ("NoSuchUpload", 404)
    with pytest.raises(ClientError) as e:
        s3.create_multipart_upload(Bucket=b, Key="e2", ChecksumAlgorithm="CRC32", ChecksumType="COMPOSITE") and \
            s3.create_multipart_upload(Bucket=b, Key="e3", ChecksumAlgorithm="SHA256", ChecksumType="FULL_OBJECT")
    assert code(e)[0] == "InvalidRequest"
    s3.abort_multipart_upload(Bucket=b, Key="e", UploadId=uid)
    with pytest.raises(ClientError) as e:
        s3.abort_multipart_upload(Bucket=b, Key="e", UploadId=uid)
    assert code(e) == ("NoSuchUpload", 404)


# =============================================================================================
# conditional requests, copy, response overrides, ranges
# =============================================================================================

def test_conditional_put_and_get(s3, bucket):
    b = bucket
    r = s3.put_object(Bucket=b, Key="c", Body=b"1", IfNoneMatch="*")
    etag = r["ETag"]
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="c", Body=b"2", IfNoneMatch="*")
    assert code(e) == ("PreconditionFailed", 412)
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="c", Body=b"2", IfMatch='"wrong"')
    assert code(e) == ("PreconditionFailed", 412)
    s3.put_object(Bucket=b, Key="c", Body=b"3", IfMatch=etag)
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=b, Key="missing", Body=b"3", IfMatch=etag)
    assert code(e) == ("NoSuchKey", 404)
    assert body_of(s3, b, "c") == b"3"
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="c", IfNoneMatch=hashlib.md5(b"3").hexdigest())
    assert code(e)[1] == 304
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="c", IfMatch='"nope"')
    assert code(e) == ("PreconditionFailed", 412)
    future = datetime.now(timezone.utc) + timedelta(days=1)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="c", IfModifiedSince=future)
    assert code(e)[1] == 304
    past = datetime(2001, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=b, Key="c", IfUnmodifiedSince=past)
    assert code(e)[1] == 412


def test_copy_object_conditions_and_directives(s3, bucket):
    b = bucket
    etag = s3.put_object(Bucket=b, Key="s", Body=b"src", ContentType="text/plain", Metadata={"a": "1"})["ETag"]
    s3.copy_object(Bucket=b, Key="d1", CopySource={"Bucket": b, "Key": "s"}, CopySourceIfMatch=etag)
    h = s3.head_object(Bucket=b, Key="d1")
    assert h["ContentType"] == "text/plain" and h["Metadata"] == {"a": "1"}  # COPY directive
    s3.copy_object(Bucket=b, Key="d2", CopySource={"Bucket": b, "Key": "s"}, MetadataDirective="REPLACE",
                   ContentType="application/json", Metadata={"b": "2"})
    h = s3.head_object(Bucket=b, Key="d2")
    assert h["ContentType"] == "application/json" and h["Metadata"] == {"b": "2"}
    for kw in ({"CopySourceIfMatch": '"x"'}, {"CopySourceIfNoneMatch": etag},
               {"CopySourceIfModifiedSince": datetime.now(timezone.utc) + timedelta(days=1)},
               {"CopySourceIfUnmodifiedSince": datetime(2001, 1, 1, tzinfo=timezone.utc)}):
        with pytest.raises(ClientError) as e:
            s3.copy_object(Bucket=b, Key="d3", CopySource={"Bucket": b, "Key": "s"}, **kw)
        assert code(e) == ("PreconditionFailed", 412), kw
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=b, Key="s", CopySource={"Bucket": b, "Key": "s"})
    assert code(e) == ("InvalidRequest", 400)
    s3.copy_object(Bucket=b, Key="s", CopySource={"Bucket": b, "Key": "s"}, MetadataDirective="REPLACE", Metadata={"z": "9"})
    assert s3.head_object(Bucket=b, Key="s")["Metadata"] == {"z": "9"}
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=b, Key="d4", CopySource={"Bucket": b, "Key": "nokey"})
    assert code(e) == ("NoSuchKey", 404)
    with pytest.raises(ClientError) as e:
        s3.copy_object(Bucket=b, Key="d4", CopySource={"Bucket": "nobucket-xyz", "Key": "k"})
    assert code(e) == ("NoSuchBucket", 404)


def test_response_header_overrides_and_range(s3, bucket):
    b = bucket
    s3.put_object(Bucket=b, Key="r", Body=b"0123456789", ContentType="text/plain")
    g = s3.get_object(Bucket=b, Key="r", ResponseContentType="application/x-custom", ResponseContentDisposition="attachment; filename=z",
                      ResponseCacheControl="no-store", ResponseContentLanguage="de", ResponseContentEncoding="identity",
                      ResponseExpires=datetime(2031, 1, 1, tzinfo=timezone.utc))
    assert g["ContentType"] == "application/x-custom" and g["ContentDisposition"] == "attachment; filename=z"
    assert g["CacheControl"] == "no-store" and g["ContentLanguage"] == "de"
    assert s3.get_object(Bucket=b, Key="r", Range="bytes=2-4")["Body"].read() == b"234"
    assert s3.get_object(Bucket=b, Key="r", Range="bytes=-3")["Body"].read() == b"789"
    assert s3.get_object(Bucket=b, Key="r", Range="bytes=8-")["Body"].read() == b"89"
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=b, Key="r", Range="bytes=100-200")
    assert code(e) == ("InvalidRange", 416)


def test_list_objects_v2_options_and_errors(s3, bucket):
    b = bucket
    for k in ["a b", "a/b", "c+d", "é"]:
        s3.put_object(Bucket=b, Key=k, Body=b"x")
    from urllib.parse import unquote
    r = s3.list_objects_v2(Bucket=b, EncodingType="url", FetchOwner=True)  # explicit url encoding: the client does not decode
    assert {unquote(c["Key"]) for c in r["Contents"]} == {"a b", "a/b", "c+d", "é"} and r["Contents"][0]["Owner"]["ID"]
    assert {c["Key"] for c in s3.list_objects_v2(Bucket=b)["Contents"]} == {"a b", "a/b", "c+d", "é"}
    assert "Owner" not in s3.list_objects_v2(Bucket=b)["Contents"][0]
    assert s3.list_objects(Bucket=b)["Contents"][0]["Owner"]["ID"]
    assert s3.list_objects_v2(Bucket=b, MaxKeys=0)["KeyCount"] == 0
    with pytest.raises(ClientError) as e:
        s3.list_objects_v2(Bucket=b, MaxKeys=-1)
    assert code(e)[0] == "InvalidArgument"
    with pytest.raises(ClientError) as e:
        s3.list_objects_v2(Bucket=b, EncodingType="bogus")
    assert code(e)[0] == "InvalidArgument"
    with pytest.raises(ClientError) as e:
        s3.list_objects_v2(Bucket=b, ContinuationToken="!!!not-a-token")
    assert code(e)[0] == "InvalidArgument"


def test_list_buckets_pagination_prefix_and_region(s3, env):
    p = f"pg{uuid.uuid4().hex[:6]}"
    names = [f"{p}-{i}" for i in range(5)]
    for n in names:
        s3.create_bucket(Bucket=n)
    r = s3.list_buckets(Prefix=p)
    assert [b["Name"] for b in r["Buckets"]] == names
    page = s3.list_buckets(Prefix=p, MaxBuckets=2)
    assert [b["Name"] for b in page["Buckets"]] == names[:2] and page["ContinuationToken"]
    rest = s3.list_buckets(Prefix=p, MaxBuckets=10, ContinuationToken=page["ContinuationToken"])
    assert [b["Name"] for b in rest["Buckets"]] == names[2:]


def test_location_constraint_and_head_bucket_region(env, s3):
    eu = env.client(region="eu-west-1")
    b = f"eu-{uuid.uuid4().hex[:8]}"
    eu.create_bucket(Bucket=b, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    assert s3.get_bucket_location(Bucket=b)["LocationConstraint"] == "eu-west-1"
    assert s3.head_bucket(Bucket=b)["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"] == "eu-west-1"
    us = f"us-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=us)
    assert s3.get_bucket_location(Bucket=us).get("LocationConstraint") in (None, "")
    with pytest.raises(ClientError) as e:  # explicit us-east-1 is invalid
        s3.create_bucket(Bucket=f"x-{uuid.uuid4().hex[:8]}", CreateBucketConfiguration={"LocationConstraint": "us-east-1"})
    assert code(e)[0] == "InvalidLocationConstraint"
    with pytest.raises(ClientError) as e:  # a regional endpoint rejects a different constraint
        eu.create_bucket(Bucket=f"y-{uuid.uuid4().hex[:8]}", CreateBucketConfiguration={"LocationConstraint": "eu-central-1"})
    assert code(e)[0] == "IllegalLocationConstraintException"


# =============================================================================================
# presigned URLs, POST policy, virtual-hosted addressing
# =============================================================================================

def test_presigned_get_put_expiry_and_signature(env, s3, bucket):
    b = bucket
    put = s3.generate_presigned_url("put_object", Params={"Bucket": b, "Key": "p", "ContentType": "text/plain"}, ExpiresIn=60)
    assert requests.put(put, data=b"presigned body", headers={"Content-Type": "text/plain"}).status_code == 200
    get = s3.generate_presigned_url("get_object", Params={"Bucket": b, "Key": "p"}, ExpiresIn=60)
    r = requests.get(get)
    assert r.status_code == 200 and r.content == b"presigned body"
    tampered = get.replace("/p?", "/other?")
    assert requests.get(tampered).status_code == 403
    expired = boto3.client("s3", endpoint_url=env.endpoint, region_name="us-east-1", aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET, config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    url = expired.generate_presigned_url("get_object", Params={"Bucket": b, "Key": "p"}, ExpiresIn=1)
    time.sleep(2.2)
    r = requests.get(url)
    assert r.status_code == 403 and "<Code>AccessDenied</Code>" in r.text and "expired" in r.text
    # an x-amz-* header that was not signed is rejected on a presigned URL
    r = requests.put(s3.generate_presigned_url("put_object", Params={"Bucket": b, "Key": "u"}, ExpiresIn=60),
                     data=b"x", headers={"x-amz-meta-sneaky": "1"})
    assert r.status_code == 403 and "<HeadersNotSigned>x-amz-meta-sneaky</HeadersNotSigned>" in r.text
    # response-* overrides are part of the signed query
    url = s3.generate_presigned_url("get_object", Params={"Bucket": b, "Key": "p", "ResponseContentType": "image/png"}, ExpiresIn=60)
    assert requests.get(url).headers["Content-Type"] == "image/png"


def test_post_policy_browser_upload(env, s3, bucket):
    b = bucket
    post = s3.generate_presigned_post(b, "uploads/${filename}", Fields={"Content-Type": "text/plain", "x-amz-meta-src": "form",
                                                                        "success_action_status": "201"},
                                      Conditions=[["starts-with", "$Content-Type", "text/"], {"x-amz-meta-src": "form"},
                                                  {"success_action_status": "201"}, ["content-length-range", 1, 1000]],
                                      ExpiresIn=60)
    r = requests.post(post["url"], data=post["fields"], files={"file": ("hello.txt", b"form content")})
    assert r.status_code == 201, r.text
    assert "<Key>uploads/hello.txt</Key>" in r.text
    h = s3.head_object(Bucket=b, Key="uploads/hello.txt")
    assert h["ContentType"] == "text/plain" and h["Metadata"] == {"src": "form"}
    # policy violations
    r = requests.post(post["url"], data=post["fields"], files={"file": ("big.txt", b"x" * 2000)})
    assert r.status_code == 400 and "EntityTooLarge" in r.text
    bad = dict(post["fields"], **{"Content-Type": "image/png"})
    r = requests.post(post["url"], data=bad, files={"file": ("a.txt", b"x")})
    assert r.status_code == 403 and "AccessDenied" in r.text
    tampered = dict(post["fields"], **{"x-amz-signature": "0" * 64})
    r = requests.post(post["url"], data=tampered, files={"file": ("a.txt", b"x")})
    assert r.status_code == 403 and "SignatureDoesNotMatch" in r.text
    extra = dict(post["fields"], **{"x-amz-meta-extra": "nope"})
    r = requests.post(post["url"], data=extra, files={"file": ("a.txt", b"x")})
    assert r.status_code == 403 and "Extra input fields" in r.text
    r = requests.post(post["url"], data={"key": "k"}, files={"file": ("a.txt", b"x")})
    assert r.status_code == 403
    exp = s3.generate_presigned_post(b, "k", ExpiresIn=1)
    time.sleep(2.2)
    r = requests.post(exp["url"], data=exp["fields"], files={"file": ("a.txt", b"x")})
    assert r.status_code == 403 and "expired" in r.text


def _signed(env, method, host, path, body=b"", extra=None, query=""):
    # S3SigV4Auth: single-encoded paths, like real S3 clients
    url = f"http://127.0.0.1:{env.port}{path}{query}"
    headers = {"Host": f"{host}:{env.port}", "x-amz-content-sha256": hashlib.sha256(body).hexdigest()}
    headers.update(extra or {})
    req = AWSRequest(method=method, url=url, data=body, headers=headers)
    S3SigV4Auth(Credentials(KEY, SECRET), "s3", "us-east-1").add_auth(req)
    return requests.request(method, url, data=body, headers=dict(req.headers), timeout=30)


@pytest.mark.parametrize("domain", ["s3.vhost.test", "localhost", "s3.localhost", "s3.amazonaws.com", "s3.eu-west-1.amazonaws.com"])
def test_virtual_hosted_style_addressing(env, s3, domain):
    b = f"vh-{uuid.uuid4().hex[:8]}"
    r = _signed(env, "PUT", f"{b}.{domain}", "/")
    assert r.status_code == 200, r.text
    assert b in [x["Name"] for x in s3.list_buckets()["Buckets"]]
    assert _signed(env, "PUT", f"{b}.{domain}", "/dir/key%20one.txt", b"vhost data").status_code == 200
    assert body_of(s3, b, "dir/key one.txt") == b"vhost data"
    r = _signed(env, "GET", f"{b}.{domain}", "/dir/key%20one.txt")
    assert r.status_code == 200 and r.content == b"vhost data"
    r = _signed(env, "GET", f"{b}.{domain}", "/", query="?list-type=2&prefix=dir%2F")
    assert r.status_code == 200 and "<Key>dir/key one.txt</Key>" in r.text and f"<Name>{b}</Name>" in r.text
    r = _signed(env, "HEAD", f"{b}.{domain}", "/")
    assert r.status_code == 200
    r = _signed(env, "GET", f"{b}.{domain}", "/nope")
    assert r.status_code == 404 and "<Code>NoSuchKey</Code>" in r.text
    r = _signed(env, "DELETE", f"{b}.{domain}", "/dir/key%20one.txt")
    assert r.status_code == 204
    assert _signed(env, "DELETE", f"{b}.{domain}", "/").status_code == 204


def test_bucket_named_like_service_host_stays_path_style(env, s3):
    # the bare domain is the service endpoint, not a bucket
    r = _signed(env, "GET", "s3.vhost.test", "/")
    assert r.status_code == 200 and "<ListAllMyBucketsResult" in r.text
    r = _signed(env, "GET", "localhost", "/")
    assert r.status_code == 200 and "<ListAllMyBucketsResult" in r.text


# =============================================================================================
# SelectObjectContent and annotations
# =============================================================================================

def _select(s3, b, k, expr, inp, out):
    r = s3.select_object_content(Bucket=b, Key=k, Expression=expr, ExpressionType="SQL", InputSerialization=inp,
                                 OutputSerialization=out)
    data = b""
    for ev in r["Payload"]:
        if "Records" in ev:
            data += ev["Records"]["Payload"]
    return data.decode()


def test_select_object_content_csv_and_json(s3, bucket):
    b = bucket
    csv = "name,age,city\nAlice,30,NYC\nBob,25,\nCharlie,41,LA\n\"Smith, John\",50,SF\n"
    s3.put_object(Bucket=b, Key="p.csv", Body=csv.encode())
    inp = {"CSV": {"FileHeaderInfo": "USE"}}
    out = {"CSV": {}}
    assert _select(s3, b, "p.csv", "SELECT name FROM S3Object WHERE CAST(age AS INT) > 28", inp, out) == \
        'Alice\nCharlie\n"Smith, John"\n'
    assert _select(s3, b, "p.csv", "SELECT COUNT(*), SUM(age), MAX(age) FROM S3Object", inp, out) == "4,146,50\n"
    assert _select(s3, b, "p.csv", "SELECT upper(name) AS n, age * 2 FROM S3Object s WHERE s.name LIKE 'A%' OR s.age BETWEEN 40 AND 45 LIMIT 5",
                   inp, out) == "ALICE,60\nCHARLIE,82\n"
    assert _select(s3, b, "p.csv", "SELECT _1 FROM S3Object LIMIT 2", {"CSV": {"FileHeaderInfo": "NONE"}}, out) == "name\nAlice\n"
    j = _select(s3, b, "p.csv", "SELECT name, age FROM S3Object WHERE city IN ('LA', 'SF')", inp, {"JSON": {}})
    assert [json.loads(x) for x in j.strip().split("\n")] == [{"name": "Charlie", "age": "41"}, {"name": "Smith, John", "age": "50"}]
    lines = "\n".join(json.dumps(x) for x in [{"id": 1, "tags": {"a": "x"}, "v": None}, {"id": 2, "tags": {"a": "y"}}])
    s3.put_object(Bucket=b, Key="p.json", Body=lines.encode())
    j = _select(s3, b, "p.json", "SELECT s.id, s.tags.a FROM S3Object s WHERE s.id >= 1 AND s.v IS NULL", {"JSON": {"Type": "LINES"}}, {"JSON": {}})
    assert [json.loads(x) for x in j.strip().split("\n")] == [{"id": 1, "a": "x"}, {"id": 2, "a": "y"}]
    s3.put_object(Bucket=b, Key="p.csv.gz", Body=gzip.compress(csv.encode()))
    assert _select(s3, b, "p.csv.gz", "SELECT COUNT(*) FROM S3Object", {"CSV": {"FileHeaderInfo": "USE"}, "CompressionType": "GZIP"}, out) == "4\n"
    with pytest.raises(ClientError) as e:
        _select(s3, b, "p.csv", "SELEKT nonsense", inp, out)
    assert code(e)[1] == 400


def test_object_annotations_roundtrip_versions_and_copy(env, s3):
    b = new_bucket(s3)
    s3.put_object(Bucket=b, Key="o", Body=b"body")

    def ann(method, key, name=None, body=b"", extra=None, version=None):
        q = "?annotation" + (f"&annotationName={name}" if name else "") + (f"&versionId={version}" if version else "")
        return _signed(env, method, "localhost", f"/{b}/{key}", body, extra=extra, query=q)

    r = ann("PUT", "o", "note", b'{"a": 1}')
    assert r.status_code == 200 and "<Key>o</Key>" in r.text and "<AnnotationName>note</AnnotationName>" in r.text
    assert r.headers["ETag"]
    r = ann("GET", "o", "note")
    assert r.status_code == 200 and r.content == b'{"a": 1}'
    assert ann("PUT", "o", "zeta", b"z").status_code == 200
    r = ann("GET", "o")
    assert r.status_code == 200 and r.text.index("<AnnotationName>note") < r.text.index("<AnnotationName>zeta")
    assert "<AnnotationCount>2</AnnotationCount>" in r.text
    assert ann("DELETE", "o", "zeta").status_code == 204
    assert ann("DELETE", "o", "zeta").status_code == 204  # idempotent
    r = ann("GET", "o", "zeta")
    assert r.status_code == 404 and "NoSuchAnnotation" in r.text
    assert ann("PUT", "o", "bad%20name", b"x").status_code == 400
    assert ann("PUT", "o", "empty", b"").status_code == 400
    s3.copy_object(Bucket=b, Key="copy", CopySource={"Bucket": b, "Key": "o"})
    r = _signed(env, "GET", "localhost", f"/{b}/copy", query="?annotation&annotationName=note")
    assert r.status_code == 200 and r.content == b'{"a": 1}'
    s3.copy_object(Bucket=b, Key="copy2", CopySource={"Bucket": b, "Key": "o"}, MetadataDirective="COPY")
    _signed(env, "PUT", "localhost", f"/{b}/copy3", b"", extra={"x-amz-copy-source": f"/{b}/o", "x-amz-annotation-directive": "EXCLUDE"})
    r = _signed(env, "GET", "localhost", f"/{b}/copy3", query="?annotation")
    assert r.status_code == 200 and "<AnnotationCount>0</AnnotationCount>" in r.text
    s3.put_object(Bucket=b, Key="o", Body=b"new body")  # overwriting an unversioned object drops its annotations
    assert "<AnnotationCount>0</AnnotationCount>" in ann("GET", "o").text
    # versioned: annotations attach to one version
    vb = new_bucket(s3)
    s3.put_bucket_versioning(Bucket=vb, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=vb, Key="k", Body=b"1")["VersionId"]
    r = _signed(env, "PUT", "localhost", f"/{vb}/k", b"on v1", query=f"?annotation&annotationName=n&versionId={v1}")
    assert r.status_code == 200 and r.headers["x-amz-object-version-id"] == v1
    s3.put_object(Bucket=vb, Key="k", Body=b"2")
    r = _signed(env, "GET", "localhost", f"/{vb}/k", query="?annotation&annotationName=n")
    assert r.status_code == 404
    r = _signed(env, "GET", "localhost", f"/{vb}/k", query=f"?annotation&annotationName=n&versionId={v1}")
    assert r.status_code == 200 and r.content == b"on v1"


# =============================================================================================
# error parity
# =============================================================================================

def test_error_bodies_carry_s3_fields(env, s3, bucket):
    b = bucket
    r = _signed(env, "GET", "localhost", f"/{b}/missing-key")
    assert r.status_code == 404 and r.headers["x-amz-request-id"] and r.headers["x-amz-id-2"]
    assert "<Code>NoSuchKey</Code>" in r.text and "<Key>missing-key</Key>" in r.text and "<RequestId>" in r.text
    assert "<Resource>" in r.text and "<HostId>" in r.text
    r = _signed(env, "GET", "localhost", "/nosuchbucket-xyz/k")
    assert "<Code>NoSuchBucket</Code>" in r.text and "<BucketName>nosuchbucket-xyz</BucketName>" in r.text
    r = _signed(env, "PUT", "localhost", f"/{b}/x", b"1", extra={"x-amz-storage-class": "BOGUS"})
    assert r.status_code == 400 and "InvalidStorageClass" in r.text
    r = requests.get(f"{env.endpoint}/{b}/x")  # unsigned
    assert r.status_code == 403 and "<Code>AccessDenied</Code>" in r.text
    r = _signed(env, "POST", "localhost", f"/{b}", b"<bad", query="?delete")
    assert r.status_code == 400 and "MalformedXML" in r.text
    r = _signed(env, "PATCH", "localhost", f"/{b}/x")
    assert r.status_code == 405
    for name in ["ab", "A_B", "a" * 64, "192.168.1.1", "-abc", "a..b", "abc-", "xn--abc"]:
        r = _signed(env, "PUT", "localhost", f"/{name}")
        assert r.status_code == 400 and "<Code>InvalidBucketName</Code>" in r.text, (name, r.text)


def test_expect_continue_and_zero_byte_and_content_length_errors(env, s3, bucket):
    b = bucket
    s3.put_object(Bucket=b, Key="zero", Body=b"")
    assert s3.head_object(Bucket=b, Key="zero")["ContentLength"] == 0
    assert body_of(s3, b, "zero") == b""
    r = _signed(env, "PUT", "localhost", f"/{b}/exp", b"data", extra={"Expect": "100-continue"})
    assert r.status_code == 200
    assert body_of(s3, b, "exp") == b"data"


def test_copy_between_shards_keeps_content_checksums_and_tags(env, s3, bucket):
    b = bucket
    data = {f"src{i}": os.urandom(1000 + i) for i in range(24)}
    for k, v in data.items():
        s3.put_object(Bucket=b, Key=k, Body=v, ChecksumAlgorithm="SHA256", Tagging="n=1", Metadata={"k": k})
    for k, v in data.items():
        r = s3.copy_object(Bucket=b, Key="dst-" + k, CopySource={"Bucket": b, "Key": k}, ChecksumAlgorithm="SHA256")
        assert r["CopyObjectResult"]["ChecksumSHA256"] == sha256(v)
        assert body_of(s3, b, "dst-" + k) == v
        h = s3.head_object(Bucket=b, Key="dst-" + k, ChecksumMode="ENABLED")
        assert h["ChecksumSHA256"] == sha256(v) and h["Metadata"] == {"k": k} and h["TagCount"] == 1
    if len(env.pgs) == 2:  # copies must be spread over both shards, some crossing shards
        cross = 0
        for k in data:
            src = [i for i, pg in enumerate(env.pgs) if sql(pg, "SELECT 1 FROM warp_s3_objects WHERE bucket=%s AND key=%s", (b, k))]
            dst = [i for i, pg in enumerate(env.pgs) if sql(pg, "SELECT 1 FROM warp_s3_objects WHERE bucket=%s AND key=%s", (b, "dst-" + k))]
            assert len(src) == len(dst) == 1
            cross += src != dst
        assert 0 < cross < len(data), cross


def test_deleted_versions_release_their_blobs(env, s3, vbucket):
    b = vbucket

    def blobs(state):
        return sum(sql(pg, "SELECT count(*) FROM warp_s3_blobs WHERE state=%s", (state,))[0][0] for pg in env.pgs)

    before = blobs("garbage")
    vs = [s3.put_object(Bucket=b, Key="k", Body=os.urandom(100))["VersionId"] for _ in range(3)]
    committed = blobs("committed")
    assert blobs("garbage") == before  # history keeps its content: nothing is garbage yet
    for v in vs[:2]:
        s3.delete_object(Bucket=b, Key="k", VersionId=v)
    assert blobs("garbage") == before + 2 and blobs("committed") == committed - 2
    s3.delete_object(Bucket=b, Key="k", VersionId=vs[2])
    assert blobs("garbage") == before + 3
    rows = sum(sql(pg, "SELECT (SELECT count(*) FROM warp_s3_objects WHERE bucket=%s) + "
                       "(SELECT count(*) FROM warp_s3_versions WHERE bucket=%s)", (b, b))[0][0] for pg in env.pgs)
    assert rows == 0


def test_multi_object_delete_across_shards_and_quiet(s3, bucket):
    b = bucket
    keys = [f"k{i}" for i in range(50)]
    for k in keys:
        s3.put_object(Bucket=b, Key=k, Body=b"x")
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in keys[:25]] + [{"Key": "absent"}], "Quiet": True})
    assert "Deleted" not in r and "Errors" not in r
    left = [o["Key"] for o in s3.list_objects_v2(Bucket=b)["Contents"]]
    assert sorted(left) == sorted(keys[25:])
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in keys[25:30]]})
    assert sorted(d["Key"] for d in r["Deleted"]) == sorted(keys[25:30])
