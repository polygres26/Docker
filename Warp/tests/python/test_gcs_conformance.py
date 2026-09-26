"""Warp-side tests of gcswire (Google Cloud Storage JSON + XML API on Postgres).

* the golden corpus: gcs_conformance/golden.json.gz holds fake-gcs-server's answers to every step of gcs_conformance/gcs_corpus.py
  (recorded twice per case with `gcs_harness.py --record`); each case is replayed OFFLINE (no Docker) against a real Warp on one
  Postgres backend and on two sharded backends and must produce the same normalised answer, or a documented `msg`/`known`
  divergence (gcs_known.py: fake-gcs-server is an emulator with bugs and gaps; where it deviates from real GCS, Warp implements GCS);
* tests that assert the documented Google behaviour directly, because the emulator cannot be the oracle for it: the resumable upload
  protocol (308 / Range / 256 KiB alignment / status query / cancel 499), XML API, HMAC keys, V4/V2 signed URLs, auth;
* sharding: objects and their generations land on BOTH Postgres hosts, merged listings, cross-shard copy and compose;
* a 100 MiB resumable upload with bounded memory (Warp runs with -Xmx300m), concurrency (16 writers, one winner of a create race)
  and WARP_POOL_MAX_SIZE=4 with slow uploaders (other requests stay responsive).

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests.
"""
import base64
import concurrent.futures
import gzip
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time
import urllib.parse

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "gcs_conformance"))
import gcs_client as C  # noqa: E402
import gcs_corpus  # noqa: E402
import gcs_harness as H  # noqa: E402
import gcs_launch_warp as L  # noqa: E402
import gcs_signer as S  # noqa: E402

with gzip.open(os.path.join(HERE, "gcs_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)

J = "/storage/v1/b"
U = "/upload/storage/v1/b"
P = [("project", "warp-test-project")]
CHUNK = 256 * 1024


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def one():
    w = L.GcsWarp(1, extra_env={"JAVA_TOOL_OPTIONS": "-Xmx300m"})
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.GcsWarp(2)
    yield w
    w.close()


def _replay(w, case):
    bad = H.replay_golden(w.url, case, GOLDEN[case])
    assert not bad, "\n".join(f"{st}\n  expected {json.dumps(exp)[:400]}\n  got      {json.dumps(got)[:400]}" for st, exp, got in bad[:5])


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_single_backend(one, case):
    _replay(one, case)


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_two_sharded_backends(two, case):
    _replay(two, case)


# ------------------------------------------------------------------------------------------------ helpers

_n = [0]


def bucket(w, **body):
    _n[0] += 1
    name = "t%s%d" % (hex(int(time.time() * 1000))[2:], _n[0])
    r = C.do(w.url, "POST", J, P, {"Content-Type": "application/json"}, json.dumps({"name": name, **body}))
    assert r.status_code == 200, r.text
    return name


def put(w, b, name, data=b"x", ctype="application/octet-stream", q=None, h=None):
    r = C.do(w.url, "POST", f"{U}/{b}/o", [("uploadType", "media"), ("name", name)] + (q or []), {"Content-Type": ctype, **(h or {})}, data)
    assert r.status_code == 200, r.text
    return r.json()


def blob(n, seed=1):
    return (hashlib.sha256(str(seed).encode()).digest() * (n // 32 + 1))[:n]


def start_resumable(w, b, name, meta=None, q=None, h=None):
    r = C.do(w.url, "POST", f"{U}/{b}/o", [("uploadType", "resumable"), ("name", name)] + (q or []),
             {"Content-Type": "application/json", **(h or {})}, json.dumps(meta or {}))
    return r


def chunk(w, loc, data, first, total="*", last=None):
    end = first + len(data) - 1 if last is None else last
    return requests.put(loc, data=data, headers={"Content-Range": f"bytes {first}-{end}/{total}"}, timeout=120)


# ------------------------------------------------------------------------------------------------ the resumable protocol (real GCS)

def test_resumable_protocol_follows_the_documented_behaviour(one):
    b = bucket(one)
    r = start_resumable(one, b, "doc.bin", {"contentType": "application/x-doc", "metadata": {"k": "v"}}, h={"X-Upload-Content-Length": str(2 * CHUNK + 10)})
    assert r.status_code == 200 and r.content == b"" and r.headers["Content-Length"] == "0"
    loc = r.headers["Location"]
    assert "upload_id=" in loc and r.headers["X-GUploader-UploadID"] in loc
    data = blob(2 * CHUNK + 10, 3)
    # status query before any byte: 308 without a Range header
    r = requests.put(loc, headers={"Content-Range": "bytes */*"})
    assert r.status_code == 308 and "Range" not in r.headers and r.content == b""
    # a full aligned chunk
    r = chunk(one, loc, data[:CHUNK], 0)
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{CHUNK - 1}" and r.content == b""
    # a non-final chunk that is not a multiple of 256 KiB: only the aligned prefix is persisted (here nothing)
    r = chunk(one, loc, data[CHUNK:CHUNK + 1000], CHUNK)
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{CHUNK - 1}"
    # ... 300000 bytes: exactly one aligned 256 KiB block is kept, the client resumes from the reported offset
    r = chunk(one, loc, data[CHUNK:CHUNK + 300000], CHUNK)
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{2 * CHUNK - 1}"
    # status query reports the same
    r = requests.put(loc, headers={"Content-Range": "bytes */*"})
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{2 * CHUNK - 1}"
    # an offset beyond what was persisted is a client error
    r = chunk(one, loc, b"zz", 3 * CHUNK)
    assert r.status_code == 400
    # overlapping resend (offset below the persisted size): the persisted prefix is skipped
    r = chunk(one, loc, data[CHUNK:], CHUNK, total=len(data))
    assert r.status_code == 200, r.text
    o = r.json()
    assert o["size"] == str(len(data)) and o["contentType"] == "application/x-doc" and o["metadata"] == {"k": "v"}
    assert o["md5Hash"] == base64.b64encode(hashlib.md5(data).digest()).decode()
    got = C.do(one.url, "GET", f"{J}/{b}/o/doc.bin", [("alt", "media")])
    assert got.content == data and got.headers["x-goog-generation"] == o["generation"]
    # the final chunk can be retried: same object
    r2 = chunk(one, loc, data[CHUNK:], CHUNK, total=len(data))
    assert r2.status_code == 200 and r2.json()["generation"] == o["generation"]
    r3 = requests.put(loc, headers={"Content-Range": "bytes */*"})
    assert r3.status_code == 200 and r3.json()["generation"] == o["generation"]


def test_resumable_finalize_by_empty_request_and_size_mismatch(one):
    b = bucket(one)
    loc = start_resumable(one, b, "fin.bin").headers["Location"]
    assert chunk(one, loc, blob(CHUNK, 5), 0).status_code == 308
    # total says more than persisted: still incomplete
    r = requests.put(loc, headers={"Content-Range": f"bytes */{CHUNK + 5}"})
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{CHUNK - 1}"
    # total equals persisted: an empty request finalizes
    r = requests.put(loc, headers={"Content-Range": f"bytes */{CHUNK}"})
    assert r.status_code == 200 and r.json()["size"] == str(CHUNK)
    # declared X-Upload-Content-Length that the upload does not reach
    loc = start_resumable(one, b, "fin2.bin", h={"X-Upload-Content-Length": "100"}).headers["Location"]
    assert chunk(one, loc, b"a" * 50, 0, total=50).status_code == 400
    assert C.do(one.url, "GET", f"{J}/{b}/o/fin2.bin").status_code == 404


def test_resumable_cancel_and_unknown_sessions(one):
    b = bucket(one)
    loc = start_resumable(one, b, "cx.bin").headers["Location"]
    assert chunk(one, loc, blob(CHUNK, 6), 0).status_code == 308
    r = requests.delete(loc)
    assert r.status_code == 499 and r.content == b""
    assert requests.put(loc, headers={"Content-Range": "bytes */*"}).status_code == 404
    assert C.do(one.url, "GET", f"{J}/{b}/o/cx.bin").status_code == 404
    assert requests.put(f"{one.url}{U}/{b}/o", params={"uploadType": "resumable", "upload_id": "nope"}, headers={"Content-Range": "bytes */*"}).status_code == 404
    # no data of the cancelled session remains
    left = sql(one.pgs[0], "SELECT count(*) FROM warp_gcs_data_owner WHERE bucket=%s AND kind IN ('S','T')", (b,))[0][0]
    assert left == 0


def test_resumable_hash_mismatch_and_preconditions_at_finalize(one):
    b = bucket(one)
    loc = start_resumable(one, b, "h.bin", {"crc32c": "AAAAAA=="}).headers["Location"]
    r = requests.put(loc, data=b"hello")
    assert r.status_code == 400 and "CRC32C" in r.text
    assert C.do(one.url, "GET", f"{J}/{b}/o/h.bin").status_code == 404
    loc = start_resumable(one, b, "h2.bin", {"crc32c": "mnG7TA==", "md5Hash": "XUFAKrxLKna5cZ2REBfFkg=="}).headers["Location"]
    assert requests.put(loc, data=b"hello").status_code == 200
    # ifGenerationMatch=0 is checked at initiation and again at finalize
    assert start_resumable(one, b, "h2.bin", q=[("ifGenerationMatch", "0")]).status_code == 412
    loc = start_resumable(one, b, "race.bin", q=[("ifGenerationMatch", "0")]).headers["Location"]
    put(one, b, "race.bin", b"someone else")
    assert requests.put(loc, data=b"late").status_code == 412
    assert C.do(one.url, "GET", f"{J}/{b}/o/race.bin", [("alt", "media")]).content == b"someone else"


def test_100mib_resumable_upload_with_bounded_memory(one):
    b = bucket(one)
    total = 100 * 1024 * 1024
    step = 8 * 1024 * 1024
    loc = start_resumable(one, b, "big.bin", h={"X-Upload-Content-Length": str(total)}).headers["Location"]
    md5 = hashlib.md5()
    piece = os.urandom(1024 * 1024)
    for off in range(0, total, step):
        body = (piece * (step // len(piece)))[:min(step, total - off)]
        md5.update(body)
        last = off + step >= total
        r = requests.put(loc, data=body, headers={"Content-Range": f"bytes {off}-{off + len(body) - 1}/{total if last else '*'}"}, timeout=300)
        assert r.status_code == (200 if last else 308), (off, r.status_code, r.text[:200])
    o = r.json()
    assert o["size"] == str(total) and o["md5Hash"] == base64.b64encode(md5.digest()).decode()
    g = C.do(one.url, "GET", f"{J}/{b}/o/big.bin", [("alt", "media")], stream=True)
    h2, n = hashlib.md5(), 0
    for c in g.iter_content(1 << 20):
        h2.update(c)
        n += len(c)
    assert n == total and h2.hexdigest() == md5.hexdigest()
    # a ranged read from the middle
    r = C.do(one.url, "GET", f"{J}/{b}/o/big.bin", [("alt", "media")], {"Range": f"bytes={50 * 1024 * 1024}-{50 * 1024 * 1024 + 9}"})
    assert r.status_code == 206 and r.content == piece[:10]
    # Warp ran with -Xmx300m and chunks are rows: 12 blobs of 8 MiB (two 4 MiB chunk rows each) and one of 4 MiB
    rows = sql(one.pgs[0], "SELECT count(*) FROM warp_gcs_data d JOIN warp_gcs_data_owner o USING (data_id) WHERE o.bucket=%s", (b,))[0][0]
    assert rows == 25


# ------------------------------------------------------------------------------------------------ sharding

def test_objects_and_generations_land_on_both_hosts(two):
    b = bucket(two, versioning={"enabled": True})
    for i in range(60):
        put(two, b, f"obj-{i:02d}", f"data-{i}".encode())
    for i in range(0, 60, 2):
        put(two, b, f"obj-{i:02d}", f"again-{i}".encode())
    counts = [sql(pg, "SELECT count(*) FROM warp_gcs_objects WHERE bucket=%s", (b,))[0][0] for pg in two.pgs]
    assert sum(counts) == 90 and min(counts) > 15, counts
    # the bucket catalog lives on the first host only
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_gcs_buckets WHERE name=%s", (b,))[0][0] == 1
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_gcs_buckets WHERE name=%s", (b,))[0][0] == 0
    # merged listing in order with paging; every generation appears with versions=true
    seen, token = [], None
    while True:
        q = [("maxResults", "7")] + ([("pageToken", token)] if token else [])
        j = C.do(two.url, "GET", f"{J}/{b}/o", q).json()
        seen += [x["name"] for x in j.get("items", [])]
        token = j.get("nextPageToken")
        if not token:
            break
    assert seen == [f"obj-{i:02d}" for i in range(60)]
    seen, token = [], None
    while True:
        j = C.do(two.url, "GET", f"{J}/{b}/o", [("versions", "true"), ("maxResults", "11")] + ([("pageToken", token)] if token else [])).json()
        seen += [(x["name"], x["generation"]) for x in j.get("items", [])]
        token = j.get("nextPageToken")
        if not token:
            break
    assert len(seen) == 90 and seen == sorted(seen, key=lambda t: (t[0], int(t[1])))
    # delimiter roll-up merges the identical prefixes of both hosts
    for i in range(20):
        put(two, b, f"dir{i % 3}/f{i}")
    j = C.do(two.url, "GET", f"{J}/{b}/o", [("delimiter", "/"), ("prefix", "dir"), ("maxResults", "2")]).json()
    assert j["prefixes"] == ["dir0/", "dir1/"] and j["nextPageToken"]
    j = C.do(two.url, "GET", f"{J}/{b}/o", [("delimiter", "/"), ("prefix", "dir"), ("pageToken", j["nextPageToken"])]).json()
    assert j["prefixes"] == ["dir2/"] and "nextPageToken" not in j


def test_cross_shard_copy_and_compose(two):
    b = bucket(two)
    names = [f"c{i}" for i in range(12)]
    for n in names:
        put(two, b, n, n.encode() * 100)
    hosts = {n: [i for i, pg in enumerate(two.pgs) if sql(pg, "SELECT count(*) FROM warp_gcs_objects WHERE bucket=%s AND name=%s", (b, n))[0][0]][0] for n in names}
    assert len(set(hosts.values())) == 2
    enc = urllib.parse.quote
    for n in names:
        r = C.do(two.url, "POST", f"{J}/{b}/o/{enc(n, safe='')}/copyTo/b/{b}/o/{enc('copy-' + n, safe='')}")
        assert r.status_code == 200, r.text
        assert C.do(two.url, "GET", f"{J}/{b}/o/{enc('copy-' + n, safe='')}", [("alt", "media")]).content == n.encode() * 100
    # compose of sources living on both hosts
    r = C.do(two.url, "POST", f"{J}/{b}/o/all/compose", [], {"Content-Type": "application/json"},
             json.dumps({"sourceObjects": [{"name": n} for n in names], "destination": {"contentType": "text/plain"}}))
    assert r.status_code == 200, r.text
    assert r.json()["componentCount"] == 12 and "md5Hash" not in r.json()
    body = C.do(two.url, "GET", f"{J}/{b}/o/all", [("alt", "media")]).content
    assert body == b"".join(n.encode() * 100 for n in names)
    same = put(two, b, "same-bytes", body)
    assert r.json()["crc32c"] == same["crc32c"]


# ------------------------------------------------------------------------------------------------ JSON API extras

def test_bucket_defaults_acl_iam_notifications_and_service_account(one):
    b = bucket(one)
    j = C.do(one.url, "GET", f"{J}/{b}", [("projection", "full")]).json()
    assert j["kind"] == "storage#bucket" and j["locationType"] and any(a["entity"].startswith("project-owners-") for a in j["acl"])
    assert j["iamConfiguration"]["uniformBucketLevelAccess"] == {"enabled": False}
    # bucket ACLs, predefined
    r = C.do(one.url, "PATCH", f"{J}/{b}", [("predefinedAcl", "publicRead")], {"Content-Type": "application/json"}, "{}")
    assert r.status_code == 200
    acl = C.do(one.url, "GET", f"{J}/{b}/acl").json()["items"]
    assert {"entity": "allUsers", "role": "READER"} in [{"entity": a["entity"], "role": a["role"]} for a in acl]
    # IAM
    pol = C.do(one.url, "GET", f"{J}/{b}/iam").json()
    assert pol["kind"] == "storage#policy" and pol["bindings"] == []
    r = C.do(one.url, "PUT", f"{J}/{b}/iam", [], {"Content-Type": "application/json"}, json.dumps({"bindings": [{"role": "roles/storage.objectViewer", "members": ["allUsers"]}]}))
    assert r.json()["bindings"][0]["members"] == ["allUsers"]
    assert C.do(one.url, "GET", f"{J}/{b}/iam").json()["bindings"][0]["role"] == "roles/storage.objectViewer"
    r = C.do(one.url, "GET", f"{J}/{b}/iam/testPermissions", [("permissions", "storage.objects.get"), ("permissions", "storage.objects.list")])
    assert r.json()["permissions"] == ["storage.objects.get", "storage.objects.list"]
    # notifications
    r = C.do(one.url, "POST", f"{J}/{b}/notificationConfigs", [], {"Content-Type": "application/json"},
             json.dumps({"topic": "//pubsub.googleapis.com/projects/p/topics/t", "payload_format": "JSON_API_V1", "event_types": ["OBJECT_FINALIZE"]}))
    assert r.status_code == 200 and r.json()["kind"] == "storage#notification"
    nid = r.json()["id"]
    assert [n["id"] for n in C.do(one.url, "GET", f"{J}/{b}/notificationConfigs").json()["items"]] == [nid]
    assert C.do(one.url, "GET", f"{J}/{b}/notificationConfigs/{nid}").json()["topic"].endswith("topics/t")
    assert C.do(one.url, "DELETE", f"{J}/{b}/notificationConfigs/{nid}").status_code == 204
    assert C.do(one.url, "GET", f"{J}/{b}/notificationConfigs/{nid}").status_code == 404
    # service account
    r = C.do(one.url, "GET", "/storage/v1/projects/warp-test-project/serviceAccount").json()
    assert r["kind"] == "storage#serviceAccount" and r["email_address"].endswith("@gs-project-accounts.iam.gserviceaccount.com")
    # retention policy blocks deletes and overwrites
    rb = bucket(one, retentionPolicy={"retentionPeriod": "3600"})
    put(one, rb, "held.txt")
    r = C.do(one.url, "DELETE", f"{J}/{rb}/o/held.txt")
    assert r.status_code == 403 and "retention policy" in r.text
    r = C.do(one.url, "POST", f"{U}/{rb}/o", [("uploadType", "media"), ("name", "held.txt")], {}, b"y")
    assert r.status_code == 403
    # temporary hold
    hb = bucket(one)
    put(one, hb, "hold.txt")
    C.do(one.url, "PATCH", f"{J}/{hb}/o/hold.txt", [], {"Content-Type": "application/json"}, json.dumps({"temporaryHold": True}))
    assert C.do(one.url, "DELETE", f"{J}/{hb}/o/hold.txt").status_code == 403
    C.do(one.url, "PATCH", f"{J}/{hb}/o/hold.txt", [], {"Content-Type": "application/json"}, json.dumps({"temporaryHold": False}))
    assert C.do(one.url, "DELETE", f"{J}/{hb}/o/hold.txt").status_code == 204


def test_hmac_keys_lifecycle(one):
    hk = "/storage/v1/projects/warp-test-project/hmacKeys"
    r = C.do(one.url, "POST", hk)
    assert r.status_code == 400
    r = C.do(one.url, "POST", hk, [("serviceAccountEmail", "sa@warp-test-project.iam.gserviceaccount.com")])
    assert r.status_code == 200
    k = r.json()
    assert k["kind"] == "storage#hmacKey" and len(k["secret"]) >= 40 and k["metadata"]["state"] == "ACTIVE"
    aid = k["metadata"]["accessId"]
    assert aid in [x["accessId"] for x in C.do(one.url, "GET", hk).json()["items"]]
    assert C.do(one.url, "DELETE", f"{hk}/{aid}").status_code == 400  # must be INACTIVE first
    r = C.do(one.url, "PUT", f"{hk}/{aid}", [], {"Content-Type": "application/json"}, json.dumps({"state": "INACTIVE"}))
    assert r.json()["state"] == "INACTIVE"
    assert C.do(one.url, "DELETE", f"{hk}/{aid}").status_code == 204
    assert C.do(one.url, "GET", f"{hk}/{aid}").status_code == 404


def test_batch_requests(one):
    b = bucket(one)
    put(one, b, "b1.txt", b"batch")
    body = ("--bnd\r\nContent-Type: application/http\r\nContent-ID: <item1>\r\n\r\n"
            f"GET {J}/{b}/o/b1.txt HTTP/1.1\r\n\r\n"
            "--bnd\r\nContent-Type: application/http\r\nContent-ID: <item2>\r\n\r\n"
            f"GET {J}/{b}/o/missing HTTP/1.1\r\n\r\n"
            "--bnd\r\nContent-Type: application/http\r\nContent-ID: <item3>\r\n\r\n"
            f"PATCH {J}/{b}/o/b1.txt HTTP/1.1\r\nContent-Type: application/json\r\n\r\n{json.dumps({'contentType': 'text/x-b'})}\r\n"
            "--bnd\r\nContent-Type: application/http\r\nContent-ID: <item4>\r\n\r\n"
            f"DELETE {J}/{b}/o/b1.txt HTTP/1.1\r\n\r\n"
            "--bnd--")
    r = C.do(one.url, "POST", "/batch/storage/v1", [], {"Content-Type": "multipart/mixed; boundary=bnd"}, body)
    assert r.status_code == 200 and r.headers["Content-Type"].startswith("multipart/mixed")
    codes = re.findall(r"Content-ID: <response-(item\d)>.*?HTTP/1\.1 (\d{3})", r.text, re.S)
    assert codes == [("item1", "200"), ("item2", "404"), ("item3", "200"), ("item4", "204")], r.text
    assert C.do(one.url, "GET", f"{J}/{b}/o/b1.txt").status_code == 404


def test_content_encoding_decompressive_transcoding(one):
    b = bucket(one)
    raw = b"compress me " * 200
    z = gzip.compress(raw)
    put(one, b, "z.txt", z, "text/plain", q=[("contentEncoding", "gzip")])
    j = C.do(one.url, "GET", f"{J}/{b}/o/z.txt").json()
    assert j["contentEncoding"] == "gzip" and j["size"] == str(len(z))
    r = requests.get(f"{one.url}{J}/{b}/o/z.txt", params={"alt": "media"}, headers={"Accept-Encoding": "identity"})
    assert r.content == raw and "Content-Encoding" not in r.headers and r.headers["x-goog-stored-content-encoding"] == "gzip"
    assert r.headers["x-goog-stored-content-length"] == str(len(z))
    r = requests.get(f"{one.url}{J}/{b}/o/z.txt", params={"alt": "media"}, headers={"Accept-Encoding": "gzip"}, stream=True)
    assert r.headers["Content-Encoding"] == "gzip" and r.raw.read() == z


def test_conditional_media_requests_and_ranges(one):
    b = bucket(one)
    o = put(one, b, "c.txt", b"0123456789")
    u = f"{J}/{b}/o/c.txt"
    r = C.do(one.url, "GET", u, [("alt", "media")])
    etag = r.headers["ETag"]
    assert C.do(one.url, "GET", u, [("alt", "media")], {"If-None-Match": etag}).status_code == 304
    assert C.do(one.url, "GET", u, [("alt", "media")], {"If-Match": '"nope"'}).status_code == 412
    assert C.do(one.url, "GET", u, [("alt", "media")], {"If-Match": etag}).status_code == 200
    assert C.do(one.url, "GET", u, [("alt", "media")], {"If-Modified-Since": r.headers["Last-Modified"]}).status_code == 304
    assert C.do(one.url, "GET", u, [("alt", "media")], {"If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"}).status_code == 412
    r = C.do(one.url, "GET", u, [("alt", "media")], {"Range": "bytes=8-100"})
    assert r.status_code == 206 and r.content == b"89" and r.headers["Content-Range"] == "bytes 8-9/10"
    r = C.do(one.url, "GET", u, [("alt", "media")], {"Range": "bytes=20-"})
    assert r.status_code == 416 and r.headers["Content-Range"] == "bytes */10"
    assert C.do(one.url, "HEAD", u, [("alt", "media")]).headers["x-goog-hash"] == f"crc32c={o['crc32c']},md5={o['md5Hash']}"
    assert C.do(one.url, "GET", u, [("ifGenerationMatch", o["generation"])]).status_code == 200
    assert C.do(one.url, "GET", u, [("ifGenerationNotMatch", o["generation"])]).status_code == 304


def test_cors_preflight_json_and_xml(one):
    b = bucket(one, cors=[{"origin": ["https://app.example"], "method": ["GET", "PUT"], "responseHeader": ["content-type"], "maxAgeSeconds": 600}])
    r = requests.options(f"{one.url}{J}/{b}", headers={"Origin": "https://x", "Access-Control-Request-Method": "GET"})
    assert r.status_code == 200 and r.headers["Access-Control-Allow-Origin"] == "*"
    r = requests.options(f"{one.url}/{b}/obj", headers={"Origin": "https://app.example", "Access-Control-Request-Method": "PUT", "Access-Control-Request-Headers": "content-type"})
    assert r.status_code == 200 and r.headers["Access-Control-Allow-Origin"] == "https://app.example" and r.headers["Access-Control-Max-Age"] == "600"
    r = requests.options(f"{one.url}/{b}/obj", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "PUT"})
    assert r.status_code == 403
    put(one, b, "obj", b"x")
    r = requests.get(f"{one.url}/{b}/obj", headers={"Origin": "https://app.example"})
    assert r.status_code == 200 and r.headers["Access-Control-Allow-Origin"] == "https://app.example"


# ------------------------------------------------------------------------------------------------ XML API (anonymous access)

def test_xml_api_buckets_objects_and_listing(one):
    u = one.url
    b = "x" + hex(int(time.time() * 1000))[2:]
    assert requests.put(f"{u}/{b}", headers={"x-goog-project-id": "warp-test-project"}).status_code == 200
    assert requests.put(f"{u}/{b}", headers={"x-goog-project-id": "warp-test-project"}).status_code == 409
    assert requests.put(f"{u}/{b}2").status_code == 400
    assert b in requests.get(f"{u}/", headers={"x-goog-project-id": "warp-test-project"}).text
    r = requests.put(f"{u}/{b}/dir/a.txt", data=b"xml-put", headers={"Content-Type": "text/plain", "x-goog-meta-color": "red", "Cache-Control": "no-cache"})
    assert r.status_code == 200 and r.headers["ETag"] == '"' + hashlib.md5(b"xml-put").hexdigest() + '"'
    assert r.headers["x-goog-generation"] and r.headers["x-goog-hash"].startswith("crc32c=")
    g = requests.get(f"{u}/{b}/dir/a.txt")
    assert g.content == b"xml-put" and g.headers["x-goog-meta-color"] == "red" and g.headers["Content-Type"] == "text/plain"
    h = requests.head(f"{u}/{b}/dir/a.txt")
    assert h.status_code == 200 and h.headers["Content-Length"] == "7" and h.content == b""
    assert requests.get(f"{u}/{b}/dir/a.txt", headers={"Range": "bytes=1-3"}).content == b"ml-"
    e = requests.get(f"{u}/{b}/missing")
    assert e.status_code == 404 and "<Code>NoSuchKey</Code>" in e.text and e.headers["Content-Type"].startswith("application/xml")
    assert "<Code>NoSuchBucket</Code>" in requests.get(f"{u}/nosuchbucketx/o").text
    # conditional put
    assert requests.put(f"{u}/{b}/dir/a.txt", data=b"z", headers={"x-goog-if-generation-match": "0"}).status_code == 412
    # copy with metadata directive
    r = requests.put(f"{u}/{b}/dir/copy.txt", headers={"x-goog-copy-source": f"/{b}/dir/a.txt"})
    assert r.status_code == 200 and "<CopyObjectResult>" in r.text
    assert requests.get(f"{u}/{b}/dir/copy.txt").headers["x-goog-meta-color"] == "red"
    r = requests.put(f"{u}/{b}/dir/copy2.txt", headers={"x-goog-copy-source": f"/{b}/dir/a.txt", "x-goog-metadata-directive": "REPLACE", "x-goog-meta-color": "blue", "Content-Type": "text/x"})
    assert requests.get(f"{u}/{b}/dir/copy2.txt").headers["x-goog-meta-color"] == "blue"
    for i in range(5):
        requests.put(f"{u}/{b}/k{i}", data=b"k")
    # list v1 / v2, delimiter, paging
    t = requests.get(f"{u}/{b}", params={"delimiter": "/"}).text
    assert re.findall(r"<Key>(.*?)</Key>", t) == ["k0", "k1", "k2", "k3", "k4"] and "<Prefix>dir/</Prefix></CommonPrefixes>" in t
    seen, tok = [], None
    while True:
        t = requests.get(f"{u}/{b}", params={"list-type": "2", "max-keys": "3", **({"continuation-token": tok} if tok else {})}).text
        seen += re.findall(r"<Key>(.*?)</Key>", t)
        m = re.search(r"<NextContinuationToken>(.*?)</NextContinuationToken>", t)
        if not m:
            break
        tok = m.group(1)
    assert seen == ["dir/a.txt", "dir/copy.txt", "dir/copy2.txt", "k0", "k1", "k2", "k3", "k4"]
    seen, marker = [], None
    while True:
        t = requests.get(f"{u}/{b}", params={"max-keys": "3", **({"marker": marker} if marker else {})}).text
        seen += re.findall(r"<Key>(.*?)</Key>", t)
        m = re.search(r"<NextMarker>(.*?)</NextMarker>", t)
        if not m:
            break
        marker = m.group(1)
    assert len(seen) == 8 and seen == sorted(seen)
    # versioning through the XML API, delete, bucket delete
    assert requests.put(f"{u}/{b}?versioning", data="<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>").status_code == 200
    assert "<Status>Enabled</Status>" in requests.get(f"{u}/{b}?versioning").text
    assert requests.get(f"{u}/{b}", params={"location": ""}).text.count("LocationConstraint") == 2
    assert requests.delete(f"{u}/{b}").status_code == 409
    for k in re.findall(r"<Key>(.*?)</Key>", requests.get(f"{u}/{b}", params={"versions": "true"}).text):
        pass
    j = C.do(u, "GET", f"{J}/{b}/o", [("versions", "true")]).json()
    for it in j["items"]:
        assert requests.delete(f"{u}/{b}/{urllib.parse.quote(it['name'])}", params={"generation": it["generation"]}).status_code == 204
    assert requests.delete(f"{u}/{b}").status_code == 204


def test_xml_multipart_upload(one):
    u = one.url
    b = bucket(one)
    r = requests.post(f"{u}/{b}/mp/big.bin?uploads", headers={"Content-Type": "application/x-mp", "x-goog-meta-k": "v"})
    assert r.status_code == 200
    uid = re.search(r"<UploadId>(.*?)</UploadId>", r.text).group(1)
    assert uid in requests.get(f"{u}/{b}?uploads").text
    parts = [os.urandom(700 * 1024), os.urandom(300 * 1024), b"tail"]
    etags = {}
    for n in (2, 1, 3):
        r = requests.put(f"{u}/{b}/mp/big.bin", params={"partNumber": n, "uploadId": uid}, data=parts[n - 1])
        assert r.status_code == 200
        etags[n] = r.headers["ETag"]
        assert etags[n] == '"' + hashlib.md5(parts[n - 1]).hexdigest() + '"'
    # re-upload of part 2 replaces it
    r = requests.put(f"{u}/{b}/mp/big.bin", params={"partNumber": 2, "uploadId": uid}, data=parts[1])
    assert r.status_code == 200
    lp = requests.get(f"{u}/{b}/mp/big.bin", params={"uploadId": uid}).text
    assert re.findall(r"<PartNumber>(\d)</PartNumber>", lp) == ["1", "2", "3"]
    bad = "<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>" % (etags[2], etags[1])
    assert "InvalidPartOrder" in requests.post(f"{u}/{b}/mp/big.bin", params={"uploadId": uid}, data=bad).text
    wrong = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"deadbeef\"</ETag></Part></CompleteMultipartUpload>"
    assert "InvalidPart" in requests.post(f"{u}/{b}/mp/big.bin", params={"uploadId": uid}, data=wrong).text
    body = "<CompleteMultipartUpload>" + "".join(f"<Part><PartNumber>{n}</PartNumber><ETag>{etags[n]}</ETag></Part>" for n in (1, 2, 3)) + "</CompleteMultipartUpload>"
    r = requests.post(f"{u}/{b}/mp/big.bin", params={"uploadId": uid}, data=body)
    assert r.status_code == 200 and "<CompleteMultipartUploadResult" in r.text
    assert requests.get(f"{u}/{b}/mp/big.bin").content == b"".join(parts)
    assert requests.get(f"{u}/{b}/mp/big.bin").headers["x-goog-meta-k"] == "v"
    j = C.do(u, "GET", f"{J}/{b}/o/{urllib.parse.quote('mp/big.bin', safe='')}").json()
    assert j["componentCount"] == 3 and j["size"] == str(sum(map(len, parts)))
    # the upload is gone
    assert requests.put(f"{u}/{b}/mp/big.bin", params={"partNumber": 1, "uploadId": uid}, data=b"x").status_code == 404
    # abort
    uid2 = re.search(r"<UploadId>(.*?)</UploadId>", requests.post(f"{u}/{b}/mp/abort.bin?uploads").text).group(1)
    requests.put(f"{u}/{b}/mp/abort.bin", params={"partNumber": 1, "uploadId": uid2}, data=b"zzz")
    assert requests.delete(f"{u}/{b}/mp/abort.bin", params={"uploadId": uid2}).status_code == 204
    assert requests.delete(f"{u}/{b}/mp/abort.bin", params={"uploadId": uid2}).status_code == 404
    assert requests.get(f"{u}/{b}/mp/abort.bin").status_code == 404


def test_xml_resumable_upload(one):
    u = one.url
    b = bucket(one)
    r = requests.post(f"{u}/{b}/xr.bin", headers={"x-goog-resumable": "start", "Content-Type": "application/x-xr", "x-goog-meta-a": "b"})
    assert r.status_code == 201 and "upload_id=" in r.headers["Location"]
    loc = r.headers["Location"]
    data = blob(CHUNK + 500, 9)
    r = requests.put(loc, data=data[:CHUNK], headers={"Content-Range": f"bytes 0-{CHUNK - 1}/*"})
    assert r.status_code == 308 and r.headers["Range"] == f"bytes=0-{CHUNK - 1}"
    r = requests.put(loc, data=data[CHUNK:], headers={"Content-Range": f"bytes {CHUNK}-{len(data) - 1}/{len(data)}"})
    assert r.status_code == 200 and r.content == b"" and r.headers["x-goog-generation"]
    g = requests.get(f"{u}/{b}/xr.bin")
    assert g.content == data and g.headers["Content-Type"] == "application/x-xr" and g.headers["x-goog-meta-a"] == "b"


# ------------------------------------------------------------------------------------------------ auth, HMAC, signed URLs

@pytest.fixture(scope="module")
def strict():
    key, priv, pub = S.new_rsa()
    d = tempfile.mkdtemp(prefix="gcs-keys-")
    pk = os.path.join(d, "sa-public.pem")
    with open(pk, "wb") as f:
        f.write(pub)
    email = "signer@warp-test-project.iam.gserviceaccount.com"
    w = L.GcsWarp(1, anonymous=False, extra_env={"WARP_GCSWIRE_TOKENS": "tok-a,tok-b", "WARP_GCSWIRE_SIGNING_KEYS": f"{email}={pk}",
                                                   "WARP_POOL_MAX_SIZE": "4"})
    w.rsa, w.email = key, email
    yield w
    w.close()


def test_bearer_tokens_and_anonymous_refusal(strict):
    u = strict.url
    r = requests.get(f"{u}{J}", params=P)
    assert r.status_code == 401 and r.json()["error"]["errors"][0]["reason"] == "required" and "Bearer" in r.headers["WWW-Authenticate"]
    r = requests.get(f"{u}{J}", params=P, headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401 and r.json()["error"]["message"] == "Invalid Credentials"
    assert requests.get(f"{u}{J}", params=P, headers={"Authorization": "Bearer tok-a"}).status_code == 200
    assert requests.get(f"{u}{J}", params={**dict(P), "access_token": "tok-b"}).status_code == 200
    assert requests.get(f"{u}{J}", params=P, headers={"Authorization": "Basic abc"}).status_code == 401
    assert "<Code>" in requests.get(f"{u}/b/o").text  # XML API errors are XML


def _mkbucket(strict, name):
    r = requests.post(f"{strict.url}{J}", params=P, json={"name": name}, headers={"Authorization": "Bearer tok-a"})
    assert r.status_code == 200, r.text


def _hmac_key(strict):
    r = requests.post(f"{strict.url}/storage/v1/projects/warp-test-project/hmacKeys", params={"serviceAccountEmail": "x@warp-test-project.iam.gserviceaccount.com"},
                      headers={"Authorization": "Bearer tok-a"})
    k = r.json()
    return k["metadata"]["accessId"], k["secret"]


@pytest.mark.parametrize("algo", ["AWS4-HMAC-SHA256", "GOOG4-HMAC-SHA256"])
def test_xml_api_with_hmac_signatures(strict, algo):
    u = strict.url
    host = u.split("//")[1]
    b = "hm" + hex(int(time.time() * 1000))[2:] + algo[:3].lower()
    _mkbucket(strict, b)
    access, secret = _hmac_key(strict)

    def call(method, path, query=(), body=b"", headers=None, sec=secret, acc=access):
        ph = hashlib.sha256(body).hexdigest()
        hdr = S.sigv4(method, host, path, list(query), headers or {}, ph, acc, sec, algo)
        return requests.request(method, u + path, params=list(query), data=body, headers=hdr)

    assert call("PUT", f"/{b}/signed/obj.txt", body=b"hello hmac", headers={"content-type": "text/plain"}).status_code == 200
    r = call("GET", f"/{b}/signed/obj.txt")
    assert r.status_code == 200 and r.content == b"hello hmac"
    assert call("GET", f"/{b}", [("list-type", "2"), ("prefix", "signed/")]).text.count("<Key>") == 1
    assert call("HEAD", f"/{b}/signed/obj.txt").status_code == 200
    bad = call("GET", f"/{b}/signed/obj.txt", sec="wrong-secret")
    assert bad.status_code == 403 and "SignatureDoesNotMatch" in bad.text
    assert "InvalidAccessKeyId" in call("GET", f"/{b}/signed/obj.txt", acc="GOOG1EDOESNOTEXIST").text
    # an INACTIVE key is refused
    requests.put(f"{u}/storage/v1/projects/warp-test-project/hmacKeys/{access}", json={"state": "INACTIVE"}, headers={"Authorization": "Bearer tok-a"})
    r = call("GET", f"/{b}/signed/obj.txt")
    assert r.status_code == 403
    requests.put(f"{u}/storage/v1/projects/warp-test-project/hmacKeys/{access}", json={"state": "ACTIVE"}, headers={"Authorization": "Bearer tok-a"})
    assert call("DELETE", f"/{b}/signed/obj.txt").status_code == 204


def test_v4_and_v2_signed_urls(strict):
    u = strict.url
    host = u.split("//")[1]
    b = "su" + hex(int(time.time() * 1000))[2:]
    _mkbucket(strict, b)
    r = requests.post(f"{u}{U}/{b}/o", params={"uploadType": "media", "name": "sec/doc.txt"}, data=b"signed content", headers={"Authorization": "Bearer tok-a", "Content-Type": "text/plain"})
    assert r.status_code == 200
    path = f"/{b}/sec/doc.txt"
    # V4
    url = S.signed_url_v4(strict.rsa, strict.email, "GET", host, path, 300)
    r = requests.get(u + url)
    assert r.status_code == 200 and r.content == b"signed content"
    tampered = url.replace("doc.txt", "other.txt")
    assert requests.get(u + tampered).status_code == 403
    assert requests.put(u + url, data=b"x").status_code == 403  # the signature covers the method
    expired = S.signed_url_v4(strict.rsa, strict.email, "GET", host, path, 5, now=__import__("datetime").datetime.now(__import__("datetime").timezone.utc) - __import__("datetime").timedelta(seconds=60))
    r = requests.get(u + expired)
    assert r.status_code == 400 and "ExpiredToken" in r.text
    other_key, _, _ = S.new_rsa()
    assert requests.get(u + S.signed_url_v4(other_key, strict.email, "GET", host, path, 300)).status_code == 403
    assert requests.get(u + S.signed_url_v4(other_key, "unknown@x.iam", "GET", host, path, 300)).status_code == 403
    up = S.signed_url_v4(strict.rsa, strict.email, "PUT", host, f"/{b}/sec/uploaded.txt", 300, headers={"content-type": "text/plain"})
    assert requests.put(u + up, data=b"via signed put", headers={"Content-Type": "text/plain"}).status_code == 200
    assert requests.get(f"{u}/{b}/sec/uploaded.txt", headers={"Authorization": "Bearer tok-b"}).content == b"via signed put"
    # V2
    v2 = S.signed_url_v2(strict.rsa, strict.email, "GET", path, int(time.time()) + 300)
    r = requests.get(u + v2)
    assert r.status_code == 200 and r.content == b"signed content"
    assert requests.get(u + S.signed_url_v2(strict.rsa, strict.email, "GET", path, int(time.time()) - 10)).status_code == 400
    assert requests.get(u + v2.replace("doc.txt", "x.txt")).status_code == 403
    # the JSON API path form works with a signed URL too
    jpath = f"/storage/v1/b/{b}/o/{urllib.parse.quote('sec/doc.txt', safe='')}"
    ju = S.signed_url_v4(strict.rsa, strict.email, "GET", host, jpath, 300, query=[("alt", "media")])
    assert requests.get(u + ju).content == b"signed content"


def test_pool_of_4_connections_and_slow_uploaders_do_not_starve_other_requests(strict):
    u = strict.url
    hdr = {"Authorization": "Bearer tok-a"}
    b = "sl" + hex(int(time.time() * 1000))[2:]
    _mkbucket(strict, b)

    def slow_upload(i):
        def body():
            for _ in range(12):
                time.sleep(0.2)
                yield b"x" * 1024
        return requests.post(f"{u}{U}/{b}/o", params={"uploadType": "media", "name": f"s{i}"}, data=body(), headers=hdr, timeout=60).status_code

    with concurrent.futures.ThreadPoolExecutor(12) as ex:
        futs = [ex.submit(slow_upload, i) for i in range(12)]
        time.sleep(0.5)
        worst = 0.0
        for i in range(20):
            t = time.time()
            assert requests.get(f"{u}{J}/{b}", headers=hdr).status_code == 200
            assert requests.get(f"{u}{J}/{b}/o", headers=hdr).status_code == 200
            worst = max(worst, time.time() - t)
        assert worst < 3.0, worst
        assert all(f.result() == 200 for f in futs)
    assert len(requests.get(f"{u}{J}/{b}/o", headers=hdr).json()["items"]) == 12


# ------------------------------------------------------------------------------------------------ concurrency

def test_concurrent_writers_and_create_race(two):
    b = bucket(two)

    def writer(i):
        for k in range(8):
            put(two, b, f"w{i:02d}/o{k}", f"{i}-{k}".encode())
        return i

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        list(ex.map(writer, range(16)))
    names = []
    token = None
    while True:
        j = C.do(two.url, "GET", f"{J}/{b}/o", [("maxResults", "50")] + ([("pageToken", token)] if token else [])).json()
        names += [x["name"] for x in j.get("items", [])]
        token = j.get("nextPageToken")
        if not token:
            break
    assert len(names) == len(set(names)) == 128
    # 16 clients race to create the same name with ifGenerationMatch=0: exactly one wins, the rest get 412
    def create(i):
        r = C.do(two.url, "POST", f"{U}/{b}/o", [("uploadType", "media"), ("name", "race"), ("ifGenerationMatch", "0")], {}, str(i).encode())
        return r.status_code

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        codes = list(ex.map(create, range(16)))
    assert sorted(codes) == [200] + [412] * 15, codes


def test_gc_of_deleted_data_and_operations_metrics(one):
    b = bucket(one)
    put(one, b, "gone.bin", os.urandom(100000))
    put(one, b, "gone.bin", os.urandom(100000))  # overwrite: the old data becomes garbage
    C.do(one.url, "DELETE", f"{J}/{b}/o/gone.bin")
    rows = sql(one.pgs[0], "SELECT kind, count(*) FROM warp_gcs_data_owner WHERE bucket=%s GROUP BY kind", (b,))
    assert dict(rows).get("C", 0) == 0 and dict(rows).get("G", 0) == 2
    m = one.proc.metrics_text()
    assert "gcswire" in m


def test_more_than_one_cursor_batch_lists_in_order_with_delimiters(two):
    b = bucket(two)
    names = [f"d{d:02d}/f{i:03d}" for d in range(11) for i in range(100)]
    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda n: put(two, b, n, b"."), names))
    j = C.do(two.url, "GET", f"{J}/{b}/o", [("delimiter", "/")]).json()
    assert j["prefixes"] == [f"d{d:02d}/" for d in range(11)] and "items" not in j
    seen, token = [], None
    while True:
        j = C.do(two.url, "GET", f"{J}/{b}/o", [("maxResults", "300")] + ([("pageToken", token)] if token else [])).json()
        seen += [x["name"] for x in j["items"]]
        token = j.get("nextPageToken")
        if not token:
            break
    assert seen == sorted(names) and len(seen) == 1100
    # startOffset / endOffset ranges and a prefix that ends inside a directory
    j = C.do(two.url, "GET", f"{J}/{b}/o", [("startOffset", "d03/f098"), ("endOffset", "d04/f002")]).json()
    assert [x["name"] for x in j["items"]] == ["d03/f098", "d03/f099", "d04/f000", "d04/f001"]


def test_mcp_describe_backend_lists_buckets_and_object_counts(one):
    from mcp_support import call_json
    b = bucket(one)
    put(one, b, "m1", b"12345")
    put(one, b, "m2", b"1234567")
    port = int(one.ports["WARP_MCP_PORT"])
    d = call_json(port, "describe_backend", {"backend": "default.gcs"})
    objects = {x["name"]: x for x in d["contents"]["objects"]}
    assert objects[b]["count"] == 2 and objects[b]["bytes"] == 12
    assert b in d["contents"]["buckets"]
    lb = call_json(port, "list_backends", {})
    assert "default.gcs" in json.dumps(lb) and "gcs" in json.dumps(lb)
