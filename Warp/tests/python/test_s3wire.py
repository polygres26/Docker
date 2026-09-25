"""End-to-end proof that a real S3 client (boto3, SigV4-signed, path-style) can read and write
objects through s3wire into a REAL MinIO backend bucket -- real Warp subprocess, real MinIO
container, real Postgres for Warp's own config store, no mocks.

Bucket model (see S3WireServer's javadoc): client-visible buckets are key prefixes inside ONE backend
bucket, so an object `k` in Warp bucket `b` is stored in MinIO as `<backend-bucket>/b/k`; the
tests verify that directly with a separate boto3 client that bypasses Warp.
"""
import hashlib
import os
import time

import boto3
import pytest
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from warp_test_support import WarpProcess, RealPostgres, RealMinio

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

BACKEND_BUCKET = "warp-backend"
CLIENT_KEY = "s3wire-client-key"
CLIENT_SECRET = "s3wire-client-secret-value"


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def minio():
    m = RealMinio()
    direct = boto3.client("s3", endpoint_url=m.endpoint, region_name="us-east-1",
                          aws_access_key_id=m.ACCESS_KEY, aws_secret_access_key=m.SECRET_KEY,
                          config=Config(s3={"addressing_style": "path"}))
    direct.create_bucket(Bucket=BACKEND_BUCKET)
    yield m
    m.close()


@pytest.fixture(scope="module")
def warp(postgres, minio):
    proc = WarpProcess(postgres, "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env={
        "WARP_S3WIRE_BACKEND_ENDPOINT": minio.endpoint,
        "WARP_S3WIRE_BACKEND_BUCKET": BACKEND_BUCKET,
        "WARP_S3WIRE_BACKEND_ACCESS_KEY": minio.ACCESS_KEY,
        "WARP_S3WIRE_BACKEND_SECRET_KEY": minio.SECRET_KEY,
        "WARP_S3WIRE_CREDENTIALS": f"{CLIENT_KEY}={CLIENT_SECRET}",
    })
    yield proc
    proc.close()


def _s3(endpoint, key, secret, retries=0):
    return boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id=key, aws_secret_access_key=secret,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      retries={"max_attempts": retries}))


def client(warp):
    return _s3(f"http://localhost:{warp.frontend_port}", CLIENT_KEY, CLIENT_SECRET)


def direct_client(minio):
    return _s3(minio.endpoint, minio.ACCESS_KEY, minio.SECRET_KEY)


def metrics_summary(warp):
    resp = requests.get(f"http://localhost:{warp.metrics_port}/api/metrics/summary",
                        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5)
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="module")
def bucket(warp):
    c = client(warp)
    c.create_bucket(Bucket="it-bucket")
    return "it-bucket"


def test_bucket_lifecycle(warp, bucket):
    c = client(warp)
    c.head_bucket(Bucket=bucket)
    assert bucket in [b["Name"] for b in c.list_buckets()["Buckets"]]
    with pytest.raises(ClientError) as e:
        c.create_bucket(Bucket=bucket)
    assert e.value.response["Error"]["Code"] == "BucketAlreadyOwnedByYou"
    with pytest.raises(ClientError) as e:
        c.head_bucket(Bucket="no-such-bucket")
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    # empty bucket can be deleted; non-empty cannot
    c.create_bucket(Bucket="tmp-bucket")
    c.put_object(Bucket="tmp-bucket", Key="x", Body=b"1")
    with pytest.raises(ClientError) as e:
        c.delete_bucket(Bucket="tmp-bucket")
    assert e.value.response["Error"]["Code"] == "BucketNotEmpty"
    c.delete_object(Bucket="tmp-bucket", Key="x")
    c.delete_bucket(Bucket="tmp-bucket")
    assert "tmp-bucket" not in [b["Name"] for b in c.list_buckets()["Buckets"]]


def test_put_get_roundtrip_byte_exact_and_lands_in_minio(warp, minio, bucket):
    c = client(warp)
    payload = os.urandom(300_000) + b"\x00\xff unicode \xe2\x9c\x93"
    put = c.put_object(Bucket=bucket, Key="dir/roundtrip.bin", Body=payload, ContentType="application/x-test",
                       Metadata={"origin": "boto3"})
    got = c.get_object(Bucket=bucket, Key="dir/roundtrip.bin")
    assert got["Body"].read() == payload
    assert got["ContentType"] == "application/x-test"
    assert got["Metadata"]["origin"] == "boto3"
    assert got["ETag"] == put["ETag"]
    # Verify it is REALLY in MinIO, reading directly and bypassing Warp
    d = direct_client(minio)
    raw = d.get_object(Bucket=BACKEND_BUCKET, Key="it-bucket/dir/roundtrip.bin")["Body"].read()
    assert raw == payload


def test_head_object(warp, bucket):
    c = client(warp)
    c.put_object(Bucket=bucket, Key="head.txt", Body=b"hello head")
    h = c.head_object(Bucket=bucket, Key="head.txt")
    assert h["ContentLength"] == 10
    assert h["ETag"] == '"' + hashlib.md5(b"hello head").hexdigest() + '"'


def test_range_get(warp, bucket):
    c = client(warp)
    c.put_object(Bucket=bucket, Key="range.txt", Body=b"0123456789")
    r = c.get_object(Bucket=bucket, Key="range.txt", Range="bytes=2-5")
    assert r["Body"].read() == b"2345"
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert r["ContentRange"] == "bytes 2-5/10"
    r = c.get_object(Bucket=bucket, Key="range.txt", Range="bytes=-3")
    assert r["Body"].read() == b"789"
    with pytest.raises(ClientError) as e:
        c.get_object(Bucket=bucket, Key="range.txt", Range="bytes=50-60")
    assert e.value.response["Error"]["Code"] == "InvalidRange"


def test_missing_key_error_shape(warp, bucket):
    c = client(warp)
    with pytest.raises(ClientError) as e:
        c.get_object(Bucket=bucket, Key="does/not/exist")
    err = e.value.response["Error"]
    assert err["Code"] == "NoSuchKey"
    assert err["Key"] == "does/not/exist"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    with pytest.raises(ClientError) as e:
        c.get_object(Bucket="no-such-bucket", Key="k")
    assert e.value.response["Error"]["Code"] == "NoSuchBucket"
    with pytest.raises(ClientError) as e:
        c.head_object(Bucket=bucket, Key="does/not/exist")
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_list_prefix_delimiter_pagination(warp, bucket):
    c = client(warp)
    for k in ["l/a.txt", "l/b.txt", "l/sub/c.txt", "l/sub/d.txt", "l/z e+r.txt", "other.txt"]:
        c.put_object(Bucket=bucket, Key=k, Body=k.encode())
    r = c.list_objects_v2(Bucket=bucket, Prefix="l/", Delimiter="/")
    assert sorted(o["Key"] for o in r["Contents"]) == ["l/a.txt", "l/b.txt", "l/z e+r.txt"]
    assert [p["Prefix"] for p in r["CommonPrefixes"]] == ["l/sub/"]
    assert r["KeyCount"] == 4
    r = c.list_objects_v2(Bucket=bucket, Prefix="l/")
    assert len(r["Contents"]) == 5
    assert ".s3wire-bucket" not in [o["Key"] for o in r["Contents"]]
    # pagination with max-keys / continuation-token
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": "l/", "MaxKeys": 2}
        if token:
            kw["ContinuationToken"] = token
        r = c.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", [])]
        if not r["IsTruncated"]:
            break
        token = r["NextContinuationToken"]
    assert keys == sorted(["l/a.txt", "l/b.txt", "l/sub/c.txt", "l/sub/d.txt", "l/z e+r.txt"])
    # v1 listing
    r = c.list_objects(Bucket=bucket, Prefix="l/", Delimiter="/")
    assert len(r["Contents"]) == 3 and r["CommonPrefixes"][0]["Prefix"] == "l/sub/"


def test_delete_and_delete_objects(warp, bucket):
    c = client(warp)
    c.put_object(Bucket=bucket, Key="del/one", Body=b"1")
    c.delete_object(Bucket=bucket, Key="del/one")
    with pytest.raises(ClientError) as e:
        c.head_object(Bucket=bucket, Key="del/one")
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    for i in range(5):
        c.put_object(Bucket=bucket, Key=f"del/m{i}", Body=b"x")
    r = c.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": f"del/m{i}"} for i in range(5)]})
    assert sorted(d["Key"] for d in r["Deleted"]) == [f"del/m{i}" for i in range(5)]
    assert "Contents" not in c.list_objects_v2(Bucket=bucket, Prefix="del/")


def test_copy_object(warp, minio, bucket):
    c = client(warp)
    c.create_bucket(Bucket="copy-dest")
    c.put_object(Bucket=bucket, Key="copy/src key.txt", Body=b"copy me", Metadata={"m": "v"})
    c.copy_object(Bucket="copy-dest", Key="copied.txt", CopySource={"Bucket": bucket, "Key": "copy/src key.txt"})
    got = c.get_object(Bucket="copy-dest", Key="copied.txt")
    assert got["Body"].read() == b"copy me"
    assert got["Metadata"]["m"] == "v"
    raw = direct_client(minio).get_object(Bucket=BACKEND_BUCKET, Key="copy-dest/copied.txt")["Body"].read()
    assert raw == b"copy me"
    with pytest.raises(ClientError) as e:
        c.copy_object(Bucket="copy-dest", Key="x", CopySource={"Bucket": bucket, "Key": "nope"})
    assert e.value.response["Error"]["Code"] == "NoSuchKey"


def test_bad_signature_rejected(warp, bucket):
    bad = _s3(f"http://localhost:{warp.frontend_port}", CLIENT_KEY, "wrong-secret")
    with pytest.raises(ClientError) as e:
        bad.list_objects_v2(Bucket=bucket)
    assert e.value.response["Error"]["Code"] == "SignatureDoesNotMatch"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    unknown = _s3(f"http://localhost:{warp.frontend_port}", "nobody", "x")
    with pytest.raises(ClientError) as e:
        unknown.list_buckets()
    assert e.value.response["Error"]["Code"] == "InvalidAccessKeyId"
    # unsigned request
    r = requests.get(f"http://localhost:{warp.frontend_port}/{bucket}?list-type=2", timeout=5)
    assert r.status_code == 403 and "<Code>AccessDenied</Code>" in r.text
    # a rejected PUT must not have written anything
    with pytest.raises(ClientError):
        bad.put_object(Bucket=bucket, Key="bad/sig", Body=b"nope")
    with pytest.raises(ClientError) as e:
        client(warp).head_object(Bucket=bucket, Key="bad/sig")
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_presigned_url(warp, bucket):
    c = client(warp)
    c.put_object(Bucket=bucket, Key="presign.txt", Body=b"presigned body")
    url = c.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "presign.txt"}, ExpiresIn=60)
    resp = requests.get(url, timeout=5)
    assert resp.content == b"presigned body", (url, resp.text)
    tampered = url.replace("presign.txt", "other.txt")
    assert requests.get(tampered, timeout=5).status_code == 403


def test_large_upload_file_multipart(warp, minio, bucket, tmp_path):
    c = client(warp)
    size = 20 * 1024 * 1024 + 123
    src = tmp_path / "big.bin"
    data = os.urandom(size)
    src.write_bytes(data)
    cfg = TransferConfig(multipart_threshold=8 * 1024 * 1024, multipart_chunksize=8 * 1024 * 1024)
    c.upload_file(str(src), bucket, "big/big.bin", Config=cfg)
    assert c.head_object(Bucket=bucket, Key="big/big.bin")["ContentLength"] == size
    dst = tmp_path / "big.out"
    c.download_file(bucket, "big/big.bin", str(dst), Config=cfg)
    assert hashlib.sha256(dst.read_bytes()).hexdigest() == hashlib.sha256(data).hexdigest()
    # really in MinIO, and multipart etag shape ("<md5>-<parts>") proves the parts path ran
    head = direct_client(minio).head_object(Bucket=BACKEND_BUCKET, Key="it-bucket/big/big.bin")
    assert head["ContentLength"] == size
    assert head["ETag"].strip('"').endswith("-3")


def test_multipart_abort_and_unsupported(warp, bucket):
    c = client(warp)
    up = c.create_multipart_upload(Bucket=bucket, Key="abort/me")
    c.upload_part(Bucket=bucket, Key="abort/me", UploadId=up["UploadId"], PartNumber=1, Body=b"x" * 1024)
    c.abort_multipart_upload(Bucket=bucket, Key="abort/me", UploadId=up["UploadId"])
    with pytest.raises(ClientError) as e:
        c.get_object(Bucket=bucket, Key="abort/me")
    assert e.value.response["Error"]["Code"] == "NoSuchKey"
    with pytest.raises(ClientError) as e:
        c.get_bucket_versioning(Bucket=bucket)
    assert e.value.response["Error"]["Code"] == "NotImplemented"


def test_metrics_report_s3wire(warp, bucket):
    c = client(warp)
    c.put_object(Bucket=bucket, Key="metrics.txt", Body=b"m")
    c.get_object(Bucket=bucket, Key="metrics.txt")["Body"].read()
    summary = metrics_summary(warp)
    ops = {s["sql"]: s for s in summary.get("topSql", [])}
    assert "PutObject" in ops and "GetObject" in ops
    assert ops["GetObject"]["avgRttMs"] is not None
    assert "s3wire" in str(summary)


def _p50(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2]


def test_write_rtt_baseline(warp, minio, bucket):
    c = client(warp)
    d = direct_client(minio)
    body = b"x" * 256

    c.put_object(Bucket=bucket, Key="rtt/warm", Body=body)
    d.put_object(Bucket=BACKEND_BUCKET, Key="rtt/warm", Body=body)

    n = 40
    via_warp, direct = [], []
    for i in range(n):
        t0 = time.perf_counter()
        c.put_object(Bucket=bucket, Key=f"rtt/w{i}", Body=body)
        via_warp.append((time.perf_counter() - t0) * 1000.0)
        t0 = time.perf_counter()
        d.put_object(Bucket=BACKEND_BUCKET, Key=f"rtt/d{i}", Body=body)
        direct.append((time.perf_counter() - t0) * 1000.0)

    summary = metrics_summary(warp)
    entry = next((s for s in summary.get("topSql", []) if s.get("sql") == "PutObject"), None)
    assert entry is not None and entry["avgRttMs"] is not None, f"topSql={summary.get('topSql')}"

    vw, dr = _p50(via_warp), _p50(direct)
    print(f"\n[s3wire write RTT, 256B PutObject x{n}] via Warp: client p50={vw:.3f}ms "
          f"min={min(via_warp):.3f}ms | server avgRttMs={entry['avgRttMs']} | "
          f"direct-to-MinIO client p50={dr:.3f}ms min={min(direct):.3f}ms | "
          f"gateway overhead p50={vw - dr:.3f}ms")
    # Sanity only: gateway adds one hop plus SigV4 verify; it must not be pathologically slow.
    assert vw < dr + 100
