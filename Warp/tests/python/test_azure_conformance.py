"""Warp-side tests of azurewire (Azure Blob / Queue / Table on Postgres).

* the golden corpus: az_conformance/golden.json.gz holds Azurite's answers to every step of az_conformance/az_corpus.py
  (recorded twice per case with `az_harness.py --record`); each case is replayed OFFLINE (no Docker) against a real Warp on one
  Postgres backend and on two sharded backends, and must produce the same normalised answer (or a documented `msg`/`known`
  divergence, see az_known.py);
* end-to-end checks that blobs / queues / entities really land on BOTH Postgres hosts, a 100 MiB block-blob upload with bounded
  memory (Warp runs with a 300 MB heap), concurrency (16 clients hammering table upserts and queue dequeues, no duplicates) and
  WARP_POOL_MAX_SIZE=4 with slow uploaders (other requests stay responsive).

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests.
"""
import concurrent.futures
import gzip
import hashlib
import json
import os
import sys
import threading
import time

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "az_conformance"))
import az_client as C  # noqa: E402
import az_harness as H  # noqa: E402
import az_launch_warp as L  # noqa: E402
import az_signer as S  # noqa: E402

with gzip.open(os.path.join(HERE, "az_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def one():
    w = L.AzWarp(1, extra_env={"JAVA_TOOL_OPTIONS": "-Xmx300m"})
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.AzWarp(2)
    yield w
    w.close()


def ep(w):
    return C.Endpoint(w.blob, w.queue, w.table)


def _replay(w, case):
    bad = H.replay_golden(ep(w), case, GOLDEN[case])
    assert not bad, "\n".join(f"{st}\n  expected {json.dumps(exp)[:400]}\n  got      {json.dumps(got)[:400]}" for st, exp, got in bad[:5])


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_single_backend(one, case):
    _replay(one, case)


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_two_sharded_backends(two, case):
    _replay(two, case)


# ------------------------------------------------------------------------------------------------ sharding: rows on both hosts

def test_blobs_queues_and_entities_land_on_both_hosts(two):
    e = ep(two)
    assert C.do(e, "blob", "PUT", "/shard", [("restype", "container")]).status_code == 201
    for i in range(60):
        assert C.do(e, "blob", "PUT", f"/shard/blob-{i}", [], {"x-ms-blob-type": "BlockBlob"}, f"data-{i}").status_code == 201
    counts = [sql(pg, "SELECT count(*) FROM warp_azblob_blobs WHERE container='shard'")[0][0] for pg in two.pgs]
    assert sum(counts) == 60 and min(counts) > 10, counts
    # the container catalog lives on the first host only
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_azblob_containers WHERE name='shard'")[0][0] == 1
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_azblob_containers WHERE name='shard'")[0][0] == 0
    # listing merges both shards in order, with paging
    seen, marker = [], None
    while True:
        q = [("restype", "container"), ("comp", "list"), ("maxresults", "7")] + ([("marker", marker)] if marker else [])
        r = C.do(e, "blob", "GET", "/shard", q)
        import re
        seen += re.findall(r"<Name>(blob-\d+)</Name>", r.text)
        m = re.search(r"<NextMarker>(.+?)</NextMarker>", r.text)
        if not m:
            break
        marker = m.group(1)
    assert seen == sorted(f"blob-{i}" for i in range(60)) and len(seen) == 60
    # queues: whole queue on one shard, catalog on the first host
    for i in range(12):
        assert C.do(e, "queue", "PUT", f"/queue{i}").status_code == 201
        C.do(e, "queue", "POST", f"/queue{i}/messages", [], {}, "<QueueMessage><MessageText>m</MessageText></QueueMessage>")
    per = [sql(pg, "SELECT count(DISTINCT queue) FROM warp_azqueue_messages WHERE queue LIKE 'queue%'")[0][0] for pg in two.pgs]
    assert sum(per) == 12 and min(per) >= 1, per
    for i in range(12):
        holders = sum(sql(pg, "SELECT count(*) FROM warp_azqueue_messages WHERE queue=%s", (f"queue{i}",))[0][0] > 0 for pg in two.pgs)
        assert holders == 1
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_azqueue_queues")[0][0] == 0  # the catalog is only written on the first host
    # table: entities sharded by PartitionKey, queries scatter-gather in (PartitionKey, RowKey) order
    J = {"Content-Type": "application/json"}
    assert C.do(e, "table", "POST", "/Tables", [], J, json.dumps({"TableName": "shardt"})).status_code == 201
    for i in range(80):
        assert C.do(e, "table", "POST", "/shardt", [], {**J, "Prefer": "return-no-content"},
                    json.dumps({"PartitionKey": f"p{i % 20:02d}", "RowKey": f"r{i:03d}", "N": i})).status_code == 204
    per = [sql(pg, "SELECT count(*) FROM warp_aztable_entities WHERE tbl='shardt'")[0][0] for pg in two.pgs]
    assert sum(per) == 80 and min(per) > 10, per
    got, np_, nr = [], None, None
    while True:
        q = [("$top", "13")] + ([("NextPartitionKey", np_), ("NextRowKey", nr)] if np_ else [])
        r = C.do(e, "table", "GET", "/shardt()", q, {"Accept": "application/json;odata=nometadata"})
        got += [(x["PartitionKey"], x["RowKey"]) for x in r.json()["value"]]
        np_, nr = r.headers.get("x-ms-continuation-NextPartitionKey"), r.headers.get("x-ms-continuation-NextRowKey")
        if not np_:
            break
    assert got == sorted(got) and len(got) == 80
    # an entity group transaction is one partition, one transaction on its owner shard
    r = C.do(e, "table", "GET", "/shardt()", [("$filter", "PartitionKey eq 'p03'")], {"Accept": "application/json;odata=nometadata"})
    assert len(r.json()["value"]) == 4


# ------------------------------------------------------------------------------------------------ large upload, bounded memory

def test_100mib_block_blob_upload_with_bounded_memory(one):
    import subprocess
    e = ep(one)
    assert C.do(e, "blob", "PUT", "/bigc", [("restype", "container")]).status_code == 201
    total = 100 * 1024 * 1024
    digest = hashlib.md5()

    class Src:
        len = total

        def __init__(self):
            self.left = total
            self.blk = os.urandom(1024 * 1024)

        def read(self, n=-1):
            k = min(self.left, 1 << 20 if n < 0 else n)
            if k <= 0:
                return b""
            self.left -= k
            d = self.blk[:k]
            digest.update(d)
            return d

    path = "/devstoreaccount1/bigc/b100"
    hdrs = {"x-ms-version": "2021-08-06", "x-ms-date": S.rfc1123(), "x-ms-blob-type": "BlockBlob", "Content-Length": str(total)}
    hdrs["Authorization"] = S.sign("blob", "PUT", "devstoreaccount1", S.DEV_KEY, path, [], hdrs)
    r = requests.put(one.blob + path, data=Src(), headers=hdrs, timeout=600)
    assert r.status_code == 201, r.text
    assert r.headers["Content-MD5"] == __import__("base64").b64encode(digest.digest()).decode()
    g = C.do(e, "blob", "GET", "/bigc/b100", stream=True)
    h2, n = hashlib.md5(), 0
    for chunk in g.iter_content(1 << 20):
        h2.update(chunk)
        n += len(chunk)
    assert n == total and h2.hexdigest() == digest.hexdigest()
    # Warp ran with -Xmx300m: a buffered 100 MiB body (plus copies) would not have fit; also the chunks are rows, not one blob
    chunks = sql(one.pgs[0], "SELECT count(*) FROM warp_azblob_data d JOIN warp_azblob_data_owner o USING (data_id) WHERE o.container='bigc'")[0][0]
    assert chunks == 25
    rss = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(one.proc.process.pid)]).split()[0]) // 1024
    assert rss < 1500, rss


# ------------------------------------------------------------------------------------------------ concurrency

def test_16_clients_table_upserts_and_queue_dequeues_no_duplicates(two):
    e = ep(two)
    J = {"Content-Type": "application/json"}
    C.do(e, "table", "POST", "/Tables", [], J, json.dumps({"TableName": "hammer"}))
    C.do(e, "queue", "PUT", "/hammerq")

    def upsert(i):
        for k in range(15):
            r = C.do(e, "table", "PUT", "/hammer(PartitionKey='p%d',RowKey='counter')" % (i % 4), [], J,
                     json.dumps({"PartitionKey": "p%d" % (i % 4), "RowKey": "counter", "Last": i * 100 + k}))
            assert r.status_code == 204, r.text
            r = C.do(e, "table", "MERGE", "/hammer(PartitionKey='p%d',RowKey='r%d-%d')" % (i % 4, i, k), [], J,
                     json.dumps({"PartitionKey": "p%d" % (i % 4), "RowKey": "r%d-%d" % (i, k), "V": k}))
            assert r.status_code == 204, r.text

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        list(ex.map(upsert, range(16)))
    r = C.do(e, "table", "GET", "/hammer()", [("$top", "1000")], {"Accept": "application/json;odata=nometadata"})
    keys = [(x["PartitionKey"], x["RowKey"]) for x in r.json()["value"]]
    assert len(keys) == len(set(keys)) == 4 + 16 * 15

    n = 200
    for i in range(n):
        C.do(e, "queue", "POST", "/hammerq/messages", [], {}, f"<QueueMessage><MessageText>m{i}</MessageText></QueueMessage>")
    got, lock = [], threading.Lock()

    def consume(_):
        import re
        while True:
            r = C.do(e, "queue", "GET", "/hammerq/messages", [("numofmessages", "5"), ("visibilitytimeout", "120")])
            ids = re.findall(r"<MessageId>(.+?)</MessageId>", r.text)
            pops = re.findall(r"<PopReceipt>(.+?)</PopReceipt>", r.text)
            texts = re.findall(r"<MessageText>(.*?)</MessageText>", r.text)
            if not ids:
                return
            with lock:
                got.extend(texts)
            for i_, p_ in zip(ids, pops):
                assert C.do(e, "queue", "DELETE", f"/hammerq/messages/{i_}", [("popreceipt", p_)]).status_code == 204

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        list(ex.map(consume, range(16)))
    assert len(got) == n and len(set(got)) == n


def test_pool_of_4_connections_and_slow_uploaders_do_not_starve_other_requests():
    w = L.AzWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4"})
    try:
        e = ep(w)
        assert C.do(e, "blob", "PUT", "/slow", [("restype", "container")]).status_code == 201
        stop = threading.Event()

        def slow_upload(i):
            # 12 KiB body trickled in ~2 s: Warp reads it in chunks; no pooled connection may be held meanwhile
            path = f"/devstoreaccount1/slow/s{i}"
            hdrs = {"x-ms-version": "2021-08-06", "x-ms-date": S.rfc1123(), "x-ms-blob-type": "BlockBlob", "Content-Length": str(12 * 1024)}
            hdrs["Authorization"] = S.sign("blob", "PUT", "devstoreaccount1", S.DEV_KEY, path, [], hdrs)

            def body():
                for _ in range(12):
                    time.sleep(0.2)
                    yield b"x" * 1024

            class Src:
                len = 12 * 1024

                def __init__(self):
                    self.g = body()

                def read(self, n=-1):
                    return next(self.g, b"")

            return requests.put(w.blob + path, data=Src(), headers=hdrs, timeout=60).status_code

        with concurrent.futures.ThreadPoolExecutor(12) as ex:
            futs = [ex.submit(slow_upload, i) for i in range(12)]
            time.sleep(0.5)
            worst = 0.0
            for i in range(20):
                t = time.time()
                r = C.do(e, "table", "POST", "/Tables", [], {"Content-Type": "application/json"}, json.dumps({"TableName": f"t{i}resp"}))
                assert r.status_code == 201
                r = C.do(e, "blob", "GET", "/slow", [("restype", "container")])
                assert r.status_code == 200
                worst = max(worst, time.time() - t)
            assert worst < 3.0, worst
            assert all(f.result() == 201 for f in futs)
    finally:
        w.close()


# ------------------------------------------------------------------------------------------------ SAS / auth end-to-end and admin surface

def test_static_bearer_token_and_unknown_account(one):
    e = ep(one)
    r = C.do(e, "blob", "GET", "", [("comp", "list")], {"Authorization": "Bearer not-a-real-token"}, auth="anon")
    assert r.status_code == 401 and r.headers["x-ms-error-code"] == "InvalidAuthenticationInfo"
    r = C.do(e, "blob", "GET", "", [("comp", "list")], account="nosuchaccount")
    assert r.status_code == 404


def test_wrong_signature_error_carries_the_string_to_sign(one):
    e = ep(one)
    r = C.do(e, "blob", "GET", "/whatever/x", [], auth="badkey")
    assert r.status_code == 403 and r.headers["x-ms-error-code"] == "AuthenticationFailed"
    assert "AuthenticationErrorDetail" in r.text and "Server used following string to sign" in r.text
    r = C.do(e, "table", "GET", "/Tables", [], auth="badkey")
    assert r.status_code == 403 and "string to sign" in r.text


def test_second_account_has_its_own_key(one):
    e = ep(one)
    r = C.do(e, "blob", "PUT", "/acct2c", [("restype", "container")], account="acct2", key=L.ACCT2_KEY)
    assert r.status_code == 201
    # the dev account cannot see acct2's container and vice versa
    assert C.do(e, "blob", "GET", "/acct2c", [("restype", "container")]).status_code == 404
    assert C.do(e, "blob", "GET", "/acct2c", [("restype", "container")], account="acct2", key=L.ACCT2_KEY).status_code == 200
    assert C.do(e, "blob", "GET", "/acct2c", [("restype", "container")], account="acct2", key=S.DEV_KEY).status_code == 403


def test_operations_are_reported_with_azure_protocol_names(one):
    e = ep(one)
    C.do(e, "blob", "GET", "", [("comp", "list")])
    C.do(e, "queue", "GET", "", [("comp", "list")])
    C.do(e, "table", "GET", "/Tables")
    m = one.proc.metrics_text()
    for proto in ("azblobwire", "azqueuewire", "aztablewire"):
        assert proto in m, proto
