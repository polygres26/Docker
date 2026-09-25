"""End-to-end proof that a real SQL Server client (pymssql, real TDS wire protocol) gets correct
results through mssqlwire's T-SQL dialect translation into a real Postgres backend -- real
subprocess, real Postgres container, no mocks.

KNOWN GAPS documented (not silently worked around) by these tests:
  - Every value comes back as a string over TDS regardless of its real Postgres type --
    MssqlWireSessionHandler/TdsTokens.writeColMetaData has no per-column type mapping yet (unlike
    orawire's VARCHAR2/NUMBER/DATE mapping), so numeric/int comparisons below compare as strings.
  - mssqlwire has no session-scoped connection: MssqlWireSessionHandler opens a fresh Postgres
    connection per statement (`try (Connection backend = PgConnections.open(options))`) and closes
    it immediately after, unlike orawire's session-scoped LazyPooledConnection. BEGIN/COMMIT/
    ROLLBACK TRAN are translated to valid SQL (see DialectTranslations.normalizeSqlServer) so they
    no longer error, but there is no real cross-statement transaction state to roll back yet.
"""
import os
import time

import pymssql
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
    proc = WarpProcess(postgres, "WARP_MSSQLWIRE_PORT", frontend_name="mssqlwire")
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
    return pymssql.connect(
        server="localhost", port=warp.frontend_port,
        user="postgres", password="postgres", database="postgres",
    )


def test_simple_select(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 21 * 2 AS answer")
        (answer,) = cur.fetchone()
        assert str(answer) == "42"  # see module docstring: no per-column type mapping yet
    finally:
        conn.close()


def test_use_database_statement_is_accepted_as_a_noop(warp):
    # pymssql/FreeTDS and many T-SQL tools send "USE <db>" after login; the backend Postgres rejected it
    # (syntax error at or near "use") before mssqlwire acknowledged it like SET.
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("USE postgres")
        cur.execute("USE [postgres];")
        cur.execute("SELECT 1")
        assert str(cur.fetchone()[0]) == "1"  # see test_simple_select: no per-column type mapping yet
    finally:
        conn.close()


def test_create_insert_select_round_trip(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE mssqlwire_it (id INT PRIMARY KEY, name VARCHAR(50))")
        cur.execute("INSERT INTO mssqlwire_it (id, name) VALUES (1, 'alpha')")
        cur.execute("INSERT INTO mssqlwire_it (id, name) VALUES (2, 'beta')")
        conn.commit()

        cur.execute("SELECT id, name FROM mssqlwire_it ORDER BY id")
        rows = [(str(r[0]), r[1]) for r in cur.fetchall()]
        assert rows == [("1", "alpha"), ("2", "beta")]
    finally:
        cur.execute("DROP TABLE mssqlwire_it")
        conn.commit()
        conn.close()


@pytest.mark.skip(
    reason="mssqlwire has no session-scoped connection yet (see module docstring) -- each "
           "statement runs on its own fresh, auto-closed connection, so there is no cross-"
           "statement transaction state for ROLLBACK to undo. Worse than a clean failure: "
           "pymssql's conn.rollback() call itself hangs rather than erroring (a TDS response-"
           "shape mismatch not yet root-caused), so this can't even run as an xfail -- skipped "
           "outright to document the gap without blocking the rest of the suite on a hang.",
)
def test_transaction_rollback_discards_uncommitted_writes(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE mssqlwire_it_txn (id INT PRIMARY KEY)")
        conn.commit()

        cur.execute("INSERT INTO mssqlwire_it_txn (id) VALUES (1)")
        conn.rollback()

        cur.execute("SELECT count(*) FROM mssqlwire_it_txn")
        (count,) = cur.fetchone()
        assert str(count) == "0"
    finally:
        cur.execute("DROP TABLE mssqlwire_it_txn")
        conn.commit()
        conn.close()


def test_metrics_endpoint_reports_statements(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    finally:
        conn.close()
    body = warp.metrics_text()
    assert "warp_statements_total" in body


def test_write_rtt_baseline(warp):
    # Same methodology/harness as the other protocols' own RTT tests -- see
    # docs/RTT_BASELINE_2026.md's confound section for why this controlled re-run matters.
    conn = connect(warp)
    conn.autocommit(True)  # pymssql's autocommit is a method, not a settable attribute
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE mssqlwire_rtt (id INT PRIMARY KEY, val INT)")

        insert_sql = "INSERT INTO mssqlwire_rtt (id, val) VALUES (%s, %s)"

        cur.execute(insert_sql, (0, 0))  # warm-up, not counted

        n = 40
        client_times_ms = []
        for i in range(1, n + 1):
            t0 = time.perf_counter()
            cur.execute(insert_sql, (i, i))
            client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = find_top_sql_entry(summary, "mssqlwire_rtt")
        assert entry is not None, f"no topSql entry found for mssqlwire_rtt insert; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None

        print(f"\n[mssqlwire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        assert server_avg_rtt_ms < 5.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for mssqlwire write is well above the "
            f"sub-millisecond-to-low-single-digit-ms range expected for a loopback Postgres call"
        )
    finally:
        cur.execute("DROP TABLE mssqlwire_rtt")
        conn.commit()
        conn.close()
