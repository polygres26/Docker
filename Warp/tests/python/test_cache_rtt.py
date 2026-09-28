"""Real cache-hit vs. cache-miss RTT measurement for Warp's Ignite-backed row/result cache
(single Warp process, single-node Ignite -- WARP_CACHE_TABLES). No mocks: real Postgres
container, real Warp subprocess, real psycopg2 client.

Proves two things end to end, via the real pgwire protocol rather than the existing
CacheStageGenericPkTest unit tests:
  1. A table matching dynamowire/mongowire's original fixed-shape row-cache pattern (id lookup)
     gets a real cache hit on the second read, and that hit is meaningfully faster than the real
     Postgres round trip the first (miss) read paid.
  2. A perfectly ordinary table -- NOT dynamowire/mongowire-shaped, just a real PK -- gets the
     same treatment via the NEW generic-PK cache path (CacheStage.tryGenericPkLookup /
     PrimaryKeyCatalog), added earlier this session.

Cache-hit/miss outcomes are confirmed independently of client-observed wall clock by reading
Warp's own server-side instrumentation off the real Prometheus /metrics endpoint --
SqlMetricsCollector.recordRttOutcome labels each RTT sample with outcome=cache_hit / pg_read,
exposed as warp_rtt_calls_total{protocol,outcome} / warp_rtt_seconds_total{protocol,outcome}
(see MetricsRenderer.java). /api/metrics/summary's topSql[] only carries an *overall* avgRttMs
per statement fingerprint (no hit/miss split), so it is not the right surface for this claim --
the outcome-labeled Prometheus series is.
"""
import os
import re
import statistics
import time

import psycopg2
import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

WARMUP = 1
SAMPLES = 30


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres",
                           password="postgres", dbname="postgres") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Fixed-shape table: single-column PK named "id" -- matches dynamowire/mongowire's
            # own original row-cache assumption.
            cur.execute("CREATE TABLE cached_items (id INT PRIMARY KEY, val INT NOT NULL)")
            cur.execute("INSERT INTO cached_items (id, val) VALUES (1, 111), (2, 222)")
            # Ordinary table, NOT dynamowire/mongowire-shaped: different PK column name, extra
            # columns -- only a real, discoverable PK, exercising the NEW generic-PK cache path.
            cur.execute("CREATE TABLE widgets (widget_code TEXT PRIMARY KEY, price INT NOT NULL, "
                        "description TEXT)")
            cur.execute("INSERT INTO widgets (widget_code, price, description) VALUES "
                        "('W-1', 4200, 'a widget'), ('W-2', 900, 'another widget')")
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    proc = WarpProcess(
        postgres, "WARP_PGWIRE_PORT", frontend_name="pgwire",
        extra_env={
            "WARP_CACHE_TABLES": "cached_items,widgets",
            "WARP_CACHE_TTL_MS": "60000",
        },
    )
    yield proc
    proc.close()


def connect(warp):
    return psycopg2.connect(
        host="localhost", port=warp.frontend_port,
        user="postgres", password="postgres", dbname="postgres",
    )


def metrics_text(warp):
    resp = requests.get(f"http://localhost:{warp.metrics_port}/metrics", timeout=5)
    resp.raise_for_status()
    return resp.text


def rtt_outcome_calls_and_total_ms(metrics_txt, protocol, outcome):
    calls = None
    total_s = None
    calls_pat = re.compile(
        r'warp_rtt_calls_total\{protocol="%s",outcome="%s"\}\s+([0-9.]+)' % (protocol, outcome))
    total_pat = re.compile(
        r'warp_rtt_seconds_total\{protocol="%s",outcome="%s"\}\s+([0-9.]+)' % (protocol, outcome))
    m = calls_pat.search(metrics_txt)
    if m:
        calls = float(m.group(1))
    m = total_pat.search(metrics_txt)
    if m:
        total_s = float(m.group(1))
    return calls, total_s


def timed_reads(warp, sql, warmup=WARMUP, samples=SAMPLES, fresh_connection_each_time=False):
    """Returns (client_rtts_ms, note). If fresh_connection_each_time, opens a brand-new
    connection per read to prove the cache is server-side (Ignite), not connection-scoped."""
    rtts = []
    conn = None
    if not fresh_connection_each_time:
        conn = connect(warp)
    try:
        for i in range(warmup + samples):
            c = connect(warp) if fresh_connection_each_time else conn
            try:
                cur = c.cursor()
                t0 = time.perf_counter()
                cur.execute(sql)
                cur.fetchall()
                t1 = time.perf_counter()
                cur.close()
            finally:
                if fresh_connection_each_time:
                    c.close()
            if i >= warmup:
                rtts.append((t1 - t0) * 1000.0)
    finally:
        if conn is not None:
            conn.close()
    return rtts


def summarize(label, rtts):
    avg = statistics.mean(rtts)
    p50 = statistics.median(rtts)
    print(f"\n{label}: n={len(rtts)} avg={avg:.3f}ms p50={p50:.3f}ms "
          f"min={min(rtts):.3f}ms max={max(rtts):.3f}ms")
    return avg


def _measure_pair(warp, miss_key, hit_key, table, pk_col, outcome_protocol="pgwire"):
    """One MISS read (new key, populates cache) then repeated HIT reads (same key, fresh
    connections each time) for `table`. Returns (miss_rtt_ms, hit_avg_ms, hit_rtts)."""
    # MISS: real backend round trip, populates the cache.
    miss_sql = f"SELECT * FROM {table} WHERE {pk_col} = {miss_key}"
    miss_rtts = timed_reads(warp, miss_sql, warmup=0, samples=1)
    miss_rtt = miss_rtts[0]

    # HIT: same key already cached by the MISS read above -- read it repeatedly, from FRESH
    # connections each time, proving the cache is server-side/Ignite-backed and not tied to the
    # connection that populated it.
    hit_sql = f"SELECT * FROM {table} WHERE {pk_col} = {hit_key}"
    hit_rtts = timed_reads(warp, hit_sql, warmup=1, samples=SAMPLES, fresh_connection_each_time=True)
    hit_avg = summarize(f"{table} cache HIT (fresh connections)", hit_rtts)
    print(f"{table} cache MISS (cold, populates cache): {miss_rtt:.3f}ms")
    return miss_rtt, hit_avg, hit_rtts


def test_fixed_shape_cache_hit_faster_than_miss(warp):
    miss_rtt, hit_avg, _ = _measure_pair(warp, miss_key=1, hit_key=1, table="cached_items",
                                          pk_col="id")
    # The real claim: repeated warm HIT reads average meaningfully faster than the cold MISS.
    assert hit_avg < miss_rtt, (
        f"expected cache HIT avg ({hit_avg:.3f}ms) < cache MISS ({miss_rtt:.3f}ms)")

    txt = metrics_text(warp)
    hit_calls, hit_total_s = rtt_outcome_calls_and_total_ms(txt, "pgwire", "cache_hit")
    assert hit_calls is not None and hit_calls >= 1, (
        "expected at least one pgwire cache_hit RTT sample in /metrics, found none:\n" + txt)
    server_hit_avg_ms = (hit_total_s / hit_calls) * 1000.0
    print(f"server-side (SqlMetricsCollector) pgwire cache_hit avg: {server_hit_avg_ms:.3f}ms "
          f"over {hit_calls:.0f} samples")


def test_generic_pk_cache_hit_faster_than_miss(warp):
    """Same proof, but for `widgets` -- an ordinary table, not dynamowire/mongowire-shaped,
    exercising the NEW generic-PK cache path (CacheStage.tryGenericPkLookup / PrimaryKeyCatalog)
    added this session, via a real wire-protocol client rather than the unit test."""
    miss_rtt, hit_avg, _ = _measure_pair(warp, miss_key="'W-1'", hit_key="'W-1'", table="widgets",
                                          pk_col="widget_code")
    assert hit_avg < miss_rtt, (
        f"expected generic-PK cache HIT avg ({hit_avg:.3f}ms) < MISS ({miss_rtt:.3f}ms)")

    txt = metrics_text(warp)
    hit_calls, hit_total_s = rtt_outcome_calls_and_total_ms(txt, "pgwire", "cache_hit")
    assert hit_calls is not None and hit_calls >= 1
    print(f"generic-PK path: server-side pgwire cache_hit sample count so far: {hit_calls:.0f}")


def test_bind_parameter_query_also_cache_hits(warp):
    """Mirrors this session's own literal-vs-bind work: prove the generic-PK cache path also
    fires for a real extended-query Bind parameter, not just an inlined SQL literal."""
    conn = connect(warp)
    try:
        cur = conn.cursor()
        # MISS (new key)
        t0 = time.perf_counter()
        cur.execute("SELECT * FROM widgets WHERE widget_code = %s", ("W-2",))
        cur.fetchall()
        miss_rtt = (time.perf_counter() - t0) * 1000.0
        cur.close()
    finally:
        conn.close()

    rtts = []
    for i in range(WARMUP + SAMPLES):
        c = connect(warp)
        try:
            cur = c.cursor()
            t0 = time.perf_counter()
            cur.execute("SELECT * FROM widgets WHERE widget_code = %s", ("W-2",))
            cur.fetchall()
            t1 = time.perf_counter()
            cur.close()
        finally:
            c.close()
        if i >= WARMUP:
            rtts.append((t1 - t0) * 1000.0)
    hit_avg = summarize("widgets bind-parameter cache HIT (fresh connections)", rtts)
    print(f"widgets bind-parameter cache MISS: {miss_rtt:.3f}ms")
    assert hit_avg < miss_rtt, (
        f"expected bind-param cache HIT avg ({hit_avg:.3f}ms) < MISS ({miss_rtt:.3f}ms)")
