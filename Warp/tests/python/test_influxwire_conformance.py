"""Warp-side regression tests for influxwire's InfluxDB conformance (real Warp + real Postgres, no oracle needed).

The heart is a *golden corpus*: influx_conformance/golden.json.gz holds the answers a real InfluxDB 1.8 gave to every
request of influx_conformance/influx_corpus.py (recorded with `harness.py --record`, each case run twice on the oracle and
non-deterministic answers dropped). Every case is replayed here against Warp -- on one Postgres backend and on two
sharded backends -- and must produce byte-for-byte the same normalised answer. On top of that: the InfluxDB 2.x write
endpoint, the Flux refusal, native credentials, lenient/strict database creation, the official `influxdb` python
client, and a check that shards really split series and still answer exactly.

Needs WARP_TEST_PG_LOCAL=1 (or Docker) like the other Warp tests; see influx_conformance/README.md.
"""
import gzip
import json
import os
import sys

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "influx_conformance"))
import harness  # noqa: E402

from mcp_support import ADMIN_TOKEN  # noqa: E402
from warp_test_support import RealPostgres, WarpProcess  # noqa: E402

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

with gzip.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "influx_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


# ------------------------------------------------------------------------------------------------ fixtures

@pytest.fixture(scope="module")
def pg():
    p = RealPostgres()
    yield p
    p.close()


@pytest.fixture(scope="module")
def warp(pg):
    """Strict databases (real InfluxDB behaviour: writing to an unknown database is a 404)."""
    proc = WarpProcess(pg, "WARP_INFLUXWIRE_PORT", frontend_name="influxwire",
                       extra_env={"WARP_INFLUXWIRE_STRICT_DB": "true"})
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def sharded(pgs):
    proc = WarpProcess(pgs[0], "WARP_INFLUXWIRE_PORT", frontend_name="influxwire",
                       extra_env={"WARP_INFLUXWIRE_STRICT_DB": "true", "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["influxdb"]})
    api("POST", "/api/backend-sets/default/backends", {
        "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
        "enabledStores": ["influxdb"]}, expect=201)
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def secured(pg):
    """Lenient databases (the Warp default) plus native credentials."""
    proc = WarpProcess(pg, "WARP_INFLUXWIRE_PORT", frontend_name="influxwire", extra_env={
        "WARP_INFLUXWIRE_USER": "admin", "WARP_INFLUXWIRE_PASSWORD": "s3cret", "WARP_INFLUXWIRE_TOKEN": "tok-123"})
    yield proc
    proc.close()


def url(proc):
    return f"http://localhost:{proc.frontend_port}"


def q(proc, query, db="t", method="GET", **params):
    r = requests.request(method, url(proc) + "/query", params={"q": query, "db": db, **params}, timeout=30)
    return r


def series(proc, query, db="t"):
    r = q(proc, query, db)
    assert r.status_code == 200, r.text
    res = r.json()["results"][0]
    assert "error" not in res, res
    return res.get("series", [])


# ------------------------------------------------------------------------------------------------ golden corpus

def _replay(proc, case):
    bad = harness.replay_golden(url(proc), case, GOLDEN[case])
    assert not bad, "\n".join(f"{st}\n  expected {json.dumps(exp)[:400]}\n  got      {json.dumps(got)[:400]}"
                              for st, exp, got in bad[:5])


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_single_backend(warp, case):
    _replay(warp, case)


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_two_sharded_backends(sharded, case):
    _replay(sharded, case)


# ------------------------------------------------------------------------------------------------ endpoints

def test_ping_health_and_unknown_routes(warp):
    r = requests.get(url(warp) + "/ping")
    assert r.status_code == 204 and r.headers["X-Influxdb-Version"]
    assert requests.head(url(warp) + "/ping").status_code == 204
    assert requests.get(url(warp) + "/health").json()["status"] == "pass"
    assert requests.get(url(warp) + "/nope").status_code == 404
    assert requests.put(url(warp) + "/write", params={"db": "t"}).status_code == 405


def test_v2_write_endpoint_and_bucket_with_retention_policy(warp):
    requests.post(url(warp) + "/query", params={"q": "CREATE DATABASE v2b"})
    r = requests.post(url(warp) + "/api/v2/write", params={"org": "o", "bucket": "v2b", "precision": "s"},
                      data="m,h=a v=1i 1700000000\nm,h=b v=2i 1700000001", headers={"Authorization": "Token whatever"})
    assert r.status_code == 204, r.text
    s = series(warp, "SELECT * FROM m", "v2b")[0]
    assert s["values"] == [["2023-11-14T22:13:20Z", "a", 1], ["2023-11-14T22:13:21Z", "b", 2]]
    assert requests.post(url(warp) + "/api/v2/write", params={"org": "o", "bucket": "v2b/autogen"}, data="m v=3i 5").status_code == 204
    bad = requests.post(url(warp) + "/api/v2/write", params={"org": "o", "bucket": "v2b"}, data="m v=oops 1")
    assert bad.status_code == 400 and bad.json()["code"] == "invalid" and "unable to parse" in bad.json()["message"]
    assert requests.post(url(warp) + "/api/v2/write", params={"org": "o", "bucket": "nobucket_zz"}, data="m v=1 1").status_code == 404
    assert requests.post(url(warp) + "/api/v2/write", params={"org": "o"}, data="m v=1 1").status_code == 400


def test_flux_is_refused_clearly(warp):
    r = requests.post(url(warp) + "/api/v2/query", params={"org": "o"}, data='from(bucket:"b") |> range(start: -1h)',
                      headers={"Content-Type": "application/vnd.flux"})
    assert r.status_code == 501 and "Flux" in r.json()["message"] and "not supported" in r.json()["message"]


def test_strict_databases_and_gzip_bodies(warp):
    assert requests.post(url(warp) + "/write", params={"db": "nodb_zz"}, data="m v=1 1").status_code == 404
    requests.post(url(warp) + "/query", params={"q": "CREATE DATABASE gz"})
    body = gzip.compress(b"g v=1 1\ng v=2 2")
    assert requests.post(url(warp) + "/write", params={"db": "gz"}, data=body,
                         headers={"Content-Encoding": "gzip"}).status_code == 204
    assert [r[1] for r in series(warp, "SELECT * FROM g", "gz")[0]["values"]] == [1, 2]


def test_lenient_default_creates_the_database_and_credentials_are_enforced(secured):
    p = url(secured)
    # no credentials -> 401 in InfluxDB's shapes
    r = requests.post(p + "/write", params={"db": "lenient"}, data="m v=1 1")
    assert r.status_code == 401 and "error" in r.json()
    assert requests.get(p + "/query", params={"q": "SHOW DATABASES", "u": "admin", "p": "wrong"}).status_code == 401
    assert requests.post(p + "/api/v2/write", params={"org": "o", "bucket": "lenient"}, data="m v=1 1").json()["code"] == "unauthorized"
    # basic auth, u/p params, token header, token as password
    assert requests.post(p + "/write", params={"db": "lenient"}, data="m v=1 1", auth=("admin", "s3cret")).status_code == 204
    assert requests.post(p + "/write", params={"db": "lenient", "u": "admin", "p": "s3cret"}, data="m v=2 2").status_code == 204
    assert requests.post(p + "/write", params={"db": "lenient"}, data="m v=3 3",
                         headers={"Authorization": "Token tok-123"}).status_code == 204
    assert requests.post(p + "/write", params={"db": "lenient"}, data="m v=4 4", auth=("anyone", "tok-123")).status_code == 204
    assert requests.post(p + "/api/v2/write", params={"org": "o", "bucket": "lenient"}, data="m v=5 5",
                         headers={"Authorization": "Token tok-123"}).status_code == 204
    r = requests.get(p + "/query", params={"q": "SELECT count(v) FROM m", "db": "lenient"}, auth=("admin", "s3cret"))
    assert r.json()["results"][0]["series"][0]["values"][0][1] == 5  # database was created by the first write
    assert requests.get(p + "/ping").status_code == 204  # liveness needs no credentials


def test_official_influxdb_python_client(warp):
    influxdb = pytest.importorskip("influxdb")
    c = influxdb.InfluxDBClient(host="localhost", port=warp.frontend_port, database="pyc")
    c.create_database("pyc")
    assert {"name": "pyc"} in c.get_list_database()
    assert c.write_points([{"measurement": "cpu", "tags": {"host": "a"}, "time": "2024-01-01T00:00:00Z",
                            "fields": {"usage": 12.5, "n": 3}},
                           {"measurement": "cpu", "tags": {"host": "b"}, "time": "2024-01-01T00:00:10Z",
                            "fields": {"usage": 7.5, "n": 4}}])
    rs = c.query("SELECT mean(usage), sum(n) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' GROUP BY time(1m) fill(0)")
    pts = list(rs.get_points())
    assert pts[0]["mean"] == 10.0 and pts[0]["sum"] == 7
    assert list(c.query("SELECT usage FROM cpu GROUP BY host").get_points(tags={"host": "b"}))[0]["usage"] == 7.5
    assert c.query("SHOW MEASUREMENTS").raw["series"][0]["values"] == [["cpu"]]
    c.drop_database("pyc")
    assert {"name": "pyc"} not in c.get_list_database()


def test_pretty_chunked_csv_and_epoch(warp):
    requests.post(url(warp) + "/query", params={"q": "CREATE DATABASE fmt"})
    requests.post(url(warp) + "/write", params={"db": "fmt"}, data="\n".join(f"m v={i}i {i * 1000000000}" for i in range(7)))
    r = q(warp, "SELECT v FROM m", "fmt", chunked="true", chunk_size="3")
    docs = [json.loads(line) for line in r.text.splitlines()]
    assert [len(d["results"][0]["series"][0]["values"]) for d in docs] == [3, 3, 1]
    assert docs[0]["results"][0]["partial"] is True and "partial" not in docs[-1]["results"][0]
    r = requests.get(url(warp) + "/query", params={"q": "SELECT v FROM m LIMIT 2", "db": "fmt", "epoch": "ms"},
                     headers={"Accept": "application/csv"})
    assert r.text == "name,tags,time,v\nm,,0,0\nm,,1000,1\n"
    assert q(warp, "SHOW MEASUREMENTS", "fmt", pretty="true").text.startswith('{\n    "results": [\n        {\n            "statement_id": 0,')


# ------------------------------------------------------------------------------------------------ sharding

def _count(pg, table):
    import psycopg2
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def test_series_are_split_across_shards_and_every_function_stays_exact(sharded, pgs):
    requests.post(url(sharded) + "/query", params={"q": "CREATE DATABASE shd"})
    n = 240
    lines = "\n".join(f"cpu,host=h{i % 30} value={i}i,load={i * 0.5} {1700000000000000000 + i * 1_000_000_000}" for i in range(n))
    assert requests.post(url(sharded) + "/write", params={"db": "shd"}, data=lines, timeout=30).status_code == 204
    on0, on1 = _count(pgs[0], "warp_influx_cpu"), _count(pgs[1], "warp_influx_cpu")
    assert on0 + on1 == n and on0 > 30 and on1 > 30, (on0, on1)
    vals = list(range(n))

    def one(query):
        return series(sharded, query, "shd")[0]["values"][0][1:]

    assert one("SELECT count(value), sum(value), min(value), max(value), mean(value) FROM cpu") == \
        [n, sum(vals), 0, n - 1, sum(vals) / n]
    assert one("SELECT median(value), spread(value), mode(value) FROM cpu") == [(n - 1) / 2, n - 1, 0]
    assert one("SELECT percentile(value, 90), first(value), last(value) FROM cpu") == \
        [vals[int(n * 90 / 100 + 0.5) - 1], 0, n - 1]
    import statistics
    assert one("SELECT stddev(value) FROM cpu")[0] == pytest.approx(statistics.stdev(vals))
    assert [r[1] for r in series(sharded, "SELECT value FROM cpu ORDER BY time DESC LIMIT 3", "shd")[0]["values"]] == [n - 1, n - 2, n - 3]
    # a transformation walks the merged, time-ordered points of all shards
    assert set(r[1] for r in series(sharded, "SELECT difference(value) FROM cpu", "shd")[0]["values"]) == {1}
    assert set(round(r[1], 6) for r in series(sharded, "SELECT derivative(load, 1s) FROM cpu", "shd")[0]["values"]) == {0.5}
    assert series(sharded, "SELECT count(value) FROM cpu GROUP BY host", "shd")[0]["values"][0][1] == n // 30
    # time buckets are merged too (fill(previous) spans buckets whichever shard holds the points)
    s = series(sharded, "SELECT sum(value) FROM cpu WHERE time >= 1700000000000000000 AND time < 1700000240000000000 "
                        "GROUP BY time(60s)", "shd")[0]["values"]
    # buckets are aligned to the epoch (:00 of each minute), not to the first point
    want = {}
    for i in range(n):
        want[(1700000000 + i) // 60] = want.get((1700000000 + i) // 60, 0) + i
    assert [r[1] for r in s] == [want[k] for k in sorted(want)] and len(s) == 5
    # delete / drop reach every shard
    q(sharded, "DELETE FROM cpu WHERE time < 1700000120000000000", "shd", method="POST")
    assert one("SELECT count(value) FROM cpu") == [n - 120]
    q(sharded, "DROP MEASUREMENT cpu", "shd", method="POST")
    assert _count(pgs[0], "warp_influx_cpu") == 0 and _count(pgs[1], "warp_influx_cpu") == 0
