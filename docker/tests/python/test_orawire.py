"""orawire: real Oracle O5LOGON/TTC wire protocol, real python-oracledb client (thin mode, no
Oracle client libs needed), translated to Postgres underneath.

Known gap, not silently worked around: DDL type translation doesn't cover Oracle-specific type
syntax yet (e.g. NUMBER, VARCHAR2 -- `CREATE TABLE ... NUMBER` fails with "type does not exist"
against the Postgres backend). These tests use ANSI-standard INTEGER/VARCHAR instead, which are
valid in both dialects directly and don't need translation -- same workaround wire's own private
integration tests already use.
"""
import uuid

import oracledb

from conftest import HOST, ORAWIRE_PORT


def connect():
    return oracledb.connect(
        user="postgres", password="postgres",
        dsn=f"{HOST}:{ORAWIRE_PORT}/anything", disable_oob=True,
    )


def test_simple_select_from_dual():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 21 * 2 FROM DUAL")
        (answer,) = cur.fetchone()
        assert int(answer) == 42
    finally:
        conn.close()


def test_create_insert_select_roundtrip():
    conn = connect()
    try:
        table = f"orawire_smoke_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name VARCHAR(50))")
        cur.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'warp')")
        conn.commit()
        cur.execute(f"SELECT name FROM {table} WHERE id = 1")
        (name,) = cur.fetchone()
        assert name == "warp"
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
    finally:
        conn.close()


def test_transaction_rollback():
    conn = connect()
    try:
        table = f"orawire_rollback_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        conn.commit()
        cur.execute(f"INSERT INTO {table} (id) VALUES (1)")
        conn.rollback()
        cur.execute(f"SELECT count(*) FROM {table}")
        (count,) = cur.fetchone()
        assert int(count) == 0
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
    finally:
        conn.close()
