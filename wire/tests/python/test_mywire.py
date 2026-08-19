"""End-to-end proof that a real MySQL client (PyMySQL, real client/server wire protocol) gets
correct results through mywire's SQL dialect translation into a real Postgres backend -- real
subprocess, real Postgres container, no mocks.

KNOWN GAP documented (not silently worked around): mywire has no session-scoped connection,
same as mssqlwire -- MySqlWireSessionHandler opens a fresh Postgres connection per statement
(PgConnections.open(options), closed immediately after) rather than a persistent per-session
connection like orawire's LazyPooledConnection. COMMIT/ROLLBACK therefore have nothing to act on;
see test_transaction_rollback_discards_uncommitted_writes below.
"""
import pymysql
import pytest

from polywire_support import PolyWireProcess, RealPostgres


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def polywire(postgres):
    proc = PolyWireProcess(postgres, "POLYWIRE_MYWIRE_PORT", frontend_name="mywire")
    yield proc
    proc.close()


def connect(polywire):
    return pymysql.connect(
        host="localhost", port=polywire.frontend_port,
        user="postgres", password="postgres", database="postgres",
    )


def test_simple_select(polywire):
    conn = connect(polywire)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 21 * 2 AS answer")
            (answer,) = cur.fetchone()
            assert answer == 42
    finally:
        conn.close()


def test_create_insert_select_round_trip(polywire):
    conn = connect(polywire)
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


@pytest.mark.xfail(
    reason="mywire has no session-scoped connection yet (see module docstring) -- each "
           "statement runs on its own fresh, auto-closed connection, so there is no "
           "cross-statement transaction state for ROLLBACK to undo.",
    strict=True,
)
def test_transaction_rollback_discards_uncommitted_writes(polywire):
    conn = connect(polywire)
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


def test_metrics_endpoint_reports_statements(polywire):
    conn = connect(polywire)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()
    body = polywire.metrics_text()
    assert "polywire_statements_total" in body
