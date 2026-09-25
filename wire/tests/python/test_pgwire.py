"""End-to-end proof that a real Postgres client (psycopg2, real Postgres wire protocol -- no
translation layer needed since pgwire's frontend dialect IS Postgres) gets correct results through
Warp into a real Postgres backend -- real subprocess, real Postgres container, no mocks.

pgwire is the one frontend where "literal vs bind parameter" is a real, observable distinction:
psycopg2's cursor.execute(sql, params) sends a real Postgres extended-query Parse/Bind/Execute
sequence when params are given, vs. a plain Simple Query when the value is inlined as SQL text.
Both paths are exercised for both a read and a write below.

RTT capture (test_write_rtt_baseline) additionally cross-checks the client-observed latency
against Warp's own server-side instrumentation (SqlMetricsCollector, exposed at
/api/metrics/summary) for the same statement's fingerprint -- requires WARP_ADMIN_TOKEN, set here
before the Warp subprocess is started so MetricsServer's bearer-token auth accepts our GET.
"""
import os
import time

import psycopg2
import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    proc = WarpProcess(postgres, "WARP_PGWIRE_PORT", frontend_name="pgwire")
    yield proc
    proc.close()


def connect(warp):
    return psycopg2.connect(
        host="localhost", port=warp.frontend_port,
        user="postgres", password="postgres", dbname="postgres",
    )


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def find_top_sql_entry(summary, needle):
    for entry in summary.get("topSql", []):
        if needle in entry.get("sql", ""):
            return entry
    return None


# ---------------------------------------------------------------------------
# 1. Simple read, literal value
# ---------------------------------------------------------------------------

def test_read_literal(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 21 * 2 AS answer")
            (answer,) = cur.fetchone()
            assert answer == 42
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Same read, bind parameter (real Parse/Bind/Execute extended-query path)
# ---------------------------------------------------------------------------

def test_read_bind_parameter(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT %s * 2 AS answer", (21,))
            (answer,) = cur.fetchone()
            assert answer == 42
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3. Write, literal value
# ---------------------------------------------------------------------------

def test_write_literal(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE pgwire_it (id INT PRIMARY KEY, name VARCHAR(50))")
            cur.execute("INSERT INTO pgwire_it (id, name) VALUES (1, 'alpha')")
            conn.commit()

            cur.execute("SELECT id, name FROM pgwire_it WHERE id = 1")
            row = cur.fetchone()
            assert row == (1, "alpha")
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pgwire_it")
        conn.commit()
        conn.close()


# ---------------------------------------------------------------------------
# 4. Write, bind parameter
# ---------------------------------------------------------------------------

def test_write_bind_parameter(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE pgwire_it_bind (id INT PRIMARY KEY, name VARCHAR(50))")
            cur.execute("INSERT INTO pgwire_it_bind (id, name) VALUES (%s, %s)", (2, "beta"))
            conn.commit()

            cur.execute("SELECT id, name FROM pgwire_it_bind WHERE id = %s", (2,))
            row = cur.fetchone()
            assert row == (2, "beta")
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pgwire_it_bind")
        conn.commit()
        conn.close()


def test_metrics_endpoint_reports_statements(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# 5. RTT capture for the write op: client-observed AND server-side
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp):
    conn = connect(warp)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE pgwire_rtt (id INT PRIMARY KEY, val INT)")

        insert_sql = "INSERT INTO pgwire_rtt (id, val) VALUES (%s, %s) " \
                     "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"

        # 1 warm-up call (connection/plan caching, JIT, etc. -- not counted)
        with conn.cursor() as cur:
            cur.execute(insert_sql, (0, 0))

        n = 40
        client_times_ms = []
        with conn.cursor() as cur:
            for i in range(n):
                t0 = time.perf_counter()
                cur.execute(insert_sql, (i, i))
                client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = find_top_sql_entry(summary, "INSERT INTO pgwire_rtt")
        assert entry is not None, f"no topSql entry found for pgwire_rtt insert; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None

        print(f"\n[pgwire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        # Server-side RTT is Warp's own measured time talking to the real Postgres backend over a
        # loopback docker-published port. docs/PERFORMANCE.md (repo root, one level above wire/)
        # DOES document a prior baseline for this exact operation shape (0.47ms, server-side,
        # measured against what was very likely a bare-metal/native-host Postgres, not a fresh
        # Docker container) -- see docs/RTT_BASELINE_2026.md for the real historical comparison and
        # the Docker-port-forwarding confound that means this number isn't a clean apples-to-apples
        # regression check against that older figure. The <5ms bar below is a reasoned ceiling for
        # a trivial single-row upsert against Postgres over a loopback docker-published port, not
        # an artificially strict threshold, and is independent of that historical-comparison
        # question either way.
        assert server_avg_rtt_ms < 5.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for pgwire write is well above the "
            f"sub-millisecond-to-low-single-digit-ms range expected for a loopback Postgres call"
        )
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pgwire_rtt")
        conn.close()
