"""Single-point write / point-query RTT of influxwire against a real Postgres, for before/after comparisons.

  WARP_TEST_JAR=<jar> WARP_TEST_PG_LOCAL=1 python3 influx_rtt_bench.py [--writes 300] [--queries 100]

Prints client-observed min/p50/p90 (ms) for `POST /write` of one new-series point and for a small filtered
SELECT, plus the server-side average RTT from /api/metrics/summary.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("WARP_ADMIN_TOKEN", "warp-test-admin-token")
import requests  # noqa: E402
from warp_test_support import RealPostgres, WarpProcess  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--writes", type=int, default=300)
    ap.add_argument("--queries", type=int, default=100)
    a = ap.parse_args()
    pg = RealPostgres()
    warp = WarpProcess(pg, "WARP_INFLUXWIRE_PORT", frontend_name="influxwire")
    try:
        base = f"http://localhost:{warp.frontend_port}"
        requests.post(base + "/query", params={"q": "CREATE DATABASE bench"})  # pre-conformance Warp answers 400: databases were implicit
        s = requests.Session()
        for i in range(20):
            s.post(base + "/write", params={"db": "bench"}, data=f"temp,host=warm{i} value={i} {1700000000000000000 + i}")
        w = []
        for i in range(a.writes):
            t0 = time.perf_counter()
            r = s.post(base + "/write", params={"db": "bench"}, data=f"temp,host=h{i} value={i} {1700000000000000000 + i * 1000000}")
            w.append((time.perf_counter() - t0) * 1000)
            assert r.status_code == 204, r.text
        q = []
        for i in range(a.queries):
            t0 = time.perf_counter()
            r = s.get(base + "/query", params={"db": "bench", "q": f"SELECT value FROM temp WHERE host = 'h{i}'"})
            q.append((time.perf_counter() - t0) * 1000)
            assert r.status_code == 200 and "series" in r.json()["results"][0], r.text
        m = requests.get(f"http://localhost:{warp.metrics_port}/api/metrics/summary", timeout=5,
                         headers={"Authorization": "Bearer " + os.environ["WARP_ADMIN_TOKEN"]}).json()
        srv = [(x.get("sql"), x.get("avgRttMs")) for x in m.get("topSql", []) if "influx" in str(x).lower() or "write" in str(x.get("sql", "")).lower()][:3]
        print(f"write  n={len(w)} min={min(w):.3f} p50={pct(w, .5):.3f} p90={pct(w, .9):.3f} ms")
        print(f"query  n={len(q)} min={min(q):.3f} p50={pct(q, .5):.3f} p90={pct(q, .9):.3f} ms")
        print("server avgRttMs:", srv)
    finally:
        warp.close()
        pg.close()


if __name__ == "__main__":
    main()
