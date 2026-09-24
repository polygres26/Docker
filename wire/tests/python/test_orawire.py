"""End-to-end proof that a real Oracle client (python-oracledb), speaking the real O5LOGON/TTC
wire protocol, gets correct results through orawire's SQL dialect translation into a real
Postgres backend -- real subprocess, real Postgres container, no mocks.
"""
import os
import time

import oracledb
import pytest
import requests

from polywire_support import WarpProcess, RealPostgres

# Set here (not just in test_pgwire.py) so this file's own RTT test can hit /api/metrics/summary
# without depending on another test module having already exported it -- pytest files run
# independently, including via `pytest tests/python/test_orawire.py` alone.
ADMIN_TOKEN = "warp-polywire-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    proc = WarpProcess(postgres, "WARP_ORAWIRE_PORT", frontend_name="orawire")
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
    return oracledb.connect(
        user="postgres", password="postgres",
        dsn=f"localhost:{warp.frontend_port}/anything", disable_oob=True,
    )


def test_simple_select_from_dual(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 21 * 2 FROM DUAL")
        (answer,) = cur.fetchone()
        assert int(answer) == 42
    finally:
        conn.close()


def test_create_insert_select_round_trip(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE orawire_it (id INTEGER PRIMARY KEY, name VARCHAR(50))")
        cur.execute("INSERT INTO orawire_it (id, name) VALUES (1, 'alpha')")
        cur.execute("INSERT INTO orawire_it (id, name) VALUES (2, 'beta')")
        conn.commit()

        cur.execute("SELECT id, name FROM orawire_it ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert int(rows[0][0]) == 1 and rows[0][1] == "alpha"
        assert int(rows[1][0]) == 2 and rows[1][1] == "beta"
    finally:
        cur.execute("DROP TABLE orawire_it")
        conn.commit()
        conn.close()


def test_transaction_rollback_discards_uncommitted_writes(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE orawire_it_txn (id INTEGER PRIMARY KEY)")
        conn.commit()

        cur.execute("INSERT INTO orawire_it_txn (id) VALUES (1)")
        conn.rollback()

        cur.execute("SELECT count(*) FROM orawire_it_txn")
        (count,) = cur.fetchone()
        assert int(count) == 0
    finally:
        cur.execute("DROP TABLE orawire_it_txn")
        conn.commit()
        conn.close()


def test_metrics_endpoint_reports_statements(warp):
    conn = connect(warp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM DUAL")
        cur.fetchone()
    finally:
        conn.close()
    body = warp.metrics_text()
    assert "warp_statements_total" in body


def test_write_rtt_baseline(warp):
    # Same methodology as test_pgwire.py's own test_write_rtt_baseline, and the controlled
    # re-run docs/RTT_BASELINE_2026.md's own confound section calls for: identical Docker-
    # container-Postgres harness as the newer 8-protocol pass, so orawire's number here is
    # directly comparable to that pass's numbers (and to docs/PERFORMANCE.md's historical
    # 0.51ms/0.580ms-client figures), not conflated with a different backend provisioning shape.
    conn = connect(warp)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE orawire_rtt (id NUMBER PRIMARY KEY, val NUMBER)")

        insert_sql = "INSERT INTO orawire_rtt (id, val) VALUES (:1, :2)"

        # 1 warm-up call, not counted (connection/plan caching, JIT, etc.)
        cur.execute(insert_sql, [0, 0])

        n = 40
        client_times_ms = []
        for i in range(1, n + 1):
            t0 = time.perf_counter()
            cur.execute(insert_sql, [i, i])
            client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = find_top_sql_entry(summary, "orawire_rtt")
        assert entry is not None, f"no topSql entry found for orawire_rtt insert; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None

        print(f"\n[orawire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        assert server_avg_rtt_ms < 5.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for orawire write is well above the "
            f"sub-millisecond-to-low-single-digit-ms range expected for a loopback Postgres call"
        )
    finally:
        cur.execute("DROP TABLE orawire_rtt")
        conn.close()
