"""End-to-end proof that a real InfluxDB v1 client (plain HTTP line-protocol writes + InfluxQL
queries, per InfluxWireServer's own documented v1 surface) gets correct results through influxwire
into a real Postgres backend -- real subprocess, real Postgres container, no mocks. Uses `requests`
directly rather than an SDK: InfluxWireServer documents the real supported surface as
`POST /write?db=...` (line protocol) and `GET/POST /query?q=...` (InfluxQL), both plain HTTP, so a
raw HTTP client exercises exactly the same wire bytes a real influx-line-protocol writer or `curl`
would send -- no extra translation layer of our own hiding what's actually on the wire.

REAL GAP, not silently worked around (matching this project's own README.md style): influxwire is
NOT a SQL-text protocol from the client's point of view -- the write side is line protocol (a
structured, fully-rendered measurement+tags+fields+timestamp format, not parsed/bound query text)
and the read side is InfluxQL text but with no bind-parameter placeholder syntax in the v1 HTTP
API. This file tests one real write (line-protocol POST /write) and one real read (InfluxQL
SELECT via GET /query) and captures RTT for the write.
"""
import os
import time

import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

DB = "influxwire_it"
MEASUREMENT = "temp"


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    proc = WarpProcess(postgres, "WARP_INFLUXWIRE_PORT", frontend_name="influxwire")
    yield proc
    proc.close()


def base_url(warp):
    return f"http://localhost:{warp.frontend_port}"


def write_point(warp, line):
    resp = requests.post(f"{base_url(warp)}/write", params={"db": DB}, data=line, timeout=5)
    resp.raise_for_status()
    return resp


def query(warp, q):
    resp = requests.get(f"{base_url(warp)}/query", params={"db": DB, "q": q}, timeout=5)
    resp.raise_for_status()
    return resp.json()


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 3 & 1. One real write (line-protocol point), one real read (InfluxQL SELECT).
# ---------------------------------------------------------------------------

def test_write_point_then_query(warp):
    write_point(warp, f"{MEASUREMENT},host=alpha value=42 1700000000000000000")

    result = query(warp, f"SELECT value FROM {MEASUREMENT} WHERE host = 'alpha'")
    series = result["results"][0].get("series", [])
    assert len(series) == 1
    values = series[0]["values"]
    assert any(row[-1] == 42 for row in values)


def test_metrics_endpoint_reports_statements(warp):
    query(warp, "SHOW MEASUREMENTS")
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# RTT capture for the write op (line-protocol point write): client-observed
# AND server-side.
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp):
    # 1 warm-up call, not counted
    write_point(warp, f"{MEASUREMENT},host=warm value=0 1700000000000000000")

    n = 40
    client_times_ms = []
    for i in range(n):
        t0 = time.perf_counter()
        write_point(warp, f"{MEASUREMENT},host=rtt{i} value={i} 170000000{i:07d}000")
        client_times_ms.append((time.perf_counter() - t0) * 1000.0)

    client_times_ms.sort()
    client_p50 = client_times_ms[len(client_times_ms) // 2]
    client_min = client_times_ms[0]
    client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

    summary = metrics_summary(warp)
    entry = None
    for s in summary.get("topSql", []):
        sql = s.get("sql", "").lower()
        if "write" in sql or MEASUREMENT in sql:
            entry = s
            break
    assert entry is not None, f"no topSql entry for an influxwire write; topSql={summary.get('topSql')}"
    server_avg_rtt_ms = entry["avgRttMs"]
    assert server_avg_rtt_ms is not None

    print(f"\n[influxwire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
          f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

    # First actual measurement taken for this protocol (docs/PERFORMANCE.md does not exist in this
    # repo snapshot, so there is no prior baseline to defer to -- see docs/RTT_BASELINE_2026.md).
    # Observed server-side avgRttMs rounds to 1-2ms here (measured across repeated runs), somewhat
    # above pgwire/mongowire/dynamowire's ~1ms numbers; line-protocol parsing (tag/field/timestamp
    # tokenizing per point, done fresh per write) is real, non-trivial per-call work beyond a
    # single JDBC round trip, and this rounded integer-ms metric sits right at the 1/2ms boundary
    # call to call. <3ms is the reasoned bar for this protocol -- comfortably above the observed
    # noise band without hiding a real regression -- rather than a boundary value that would make
    # this assertion flaky on the metric's own integer rounding.
    assert server_avg_rtt_ms < 3.0, (
        f"server-side avg RTT {server_avg_rtt_ms}ms for influxwire point write exceeds the reasoned "
        f"sub-3ms baseline for a loopback Postgres call behind line-protocol parsing"
    )
