"""End-to-end proof that a real Oracle client (python-oracledb), speaking the real O5LOGON/TTC
wire protocol, gets correct results through orawire's SQL dialect translation into a real
Postgres backend -- real subprocess, real Postgres container, no mocks.
"""
import oracledb
import pytest

from warp_support import WarpProcess, RealPostgres


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
