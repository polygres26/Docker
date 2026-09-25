"""End-to-end proof that a real MySQL client (PyMySQL, real client/server wire protocol) gets
correct results through mywire's SQL dialect translation into a real Postgres backend -- real
subprocess, real Postgres container, no mocks.

KNOWN GAP documented (not silently worked around): mywire has no session-scoped connection,
same as mssqlwire -- MySqlWireSessionHandler opens a fresh Postgres connection per statement
(PgConnections.open(options), closed immediately after) rather than a persistent per-session
connection like orawire's LazyPooledConnection. COMMIT/ROLLBACK therefore have nothing to act on;
see test_transaction_rollback_discards_uncommitted_writes below.
"""
import os
import time

import pymysql
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
    proc = WarpProcess(postgres, "WARP_MYWIRE_PORT", frontend_name="mywire")
    yield proc
    proc.close()


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


def connect(warp):
    return pymysql.connect(
        host="localhost", port=warp.frontend_port,
        user="postgres", password="postgres", database="postgres",
    )


def test_simple_select(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 21 * 2 AS answer")
            (answer,) = cur.fetchone()
            assert answer == 42
    finally:
        conn.close()


def test_create_insert_select_round_trip(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE mywire_it (id INT PRIMARY KEY, name VARCHAR(50))")
            cur.execute("INSERT INTO mywire_it (id, name) VALUES (1, 'alpha')")
            cur.execute("INSERT INTO mywire_it (id, name) VALUES (2, 'beta')")
            conn.commit()

            cur.execute("SELECT id, name FROM mywire_it ORDER BY id")
            rows = cur.fetchall()
            assert rows == ((1, "alpha"), (2, "beta"))
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE mywire_it")
        conn.commit()
        conn.close()


# History: this was xfail(strict) -- "mywire has no session-scoped connection yet". The session connection has
# existed for a while, but "SET autocommit=0" only started ONE transaction: after the first COMMIT the session fell
# back to autocommit, so a later INSERT + ROLLBACK (as here: CREATE; COMMIT; INSERT; ROLLBACK) could not be undone.
# With connection multiplexing autocommit=0 is a session MODE (every statement after a COMMIT/ROLLBACK opens the
# next transaction), which is what real MySQL does, so the test now passes.
def test_transaction_rollback_discards_uncommitted_writes(warp):
    conn = connect(warp)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE mywire_it_txn (id INT PRIMARY KEY)")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("INSERT INTO mywire_it_txn (id) VALUES (1)")
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM mywire_it_txn")
            (count,) = cur.fetchone()
            assert count == 0
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE mywire_it_txn")
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


def test_write_rtt_baseline(warp):
    # Same methodology/harness as test_pgwire.py/test_orawire.py's own RTT tests -- see
    # docs/RTT_BASELINE_2026.md's confound section for why this controlled re-run matters.
    conn = connect(warp)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE mywire_rtt (id INT PRIMARY KEY, val INT)")

        insert_sql = "INSERT INTO mywire_rtt (id, val) VALUES (%s, %s)"

        with conn.cursor() as cur:
            cur.execute(insert_sql, (0, 0))  # warm-up, not counted

        n = 40
        client_times_ms = []
        with conn.cursor() as cur:
            for i in range(1, n + 1):
                t0 = time.perf_counter()
                cur.execute(insert_sql, (i, i))
                client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = find_top_sql_entry(summary, "mywire_rtt")
        assert entry is not None, f"no topSql entry found for mywire_rtt insert; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None

        print(f"\n[mywire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        assert server_avg_rtt_ms < 5.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for mywire write is well above the "
            f"sub-millisecond-to-low-single-digit-ms range expected for a loopback Postgres call"
        )
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS mywire_rtt")
        conn.close()
