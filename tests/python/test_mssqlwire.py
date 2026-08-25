"""mssqlwire: real SQL Server TDS wire protocol, real pymssql client, translated to Postgres
underneath.

Known gap, not silently worked around: every value comes back as a string over TDS regardless
of its real Postgres type (no per-column type mapping yet), so comparisons below are string
comparisons. mssqlwire also has no session-scoped connection (a fresh pooled Postgres connection
per statement), so there's no rollback test here.
"""
import uuid

import pymssql

from conftest import HOST, MSSQLWIRE_PORT


def connect():
    return pymssql.connect(server=HOST, port=MSSQLWIRE_PORT, user="postgres", password="postgres", database="postgres")


def test_simple_select():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 21 * 2 AS answer")
        (answer,) = cur.fetchone()
        assert str(answer) == "42"  # see module docstring: no per-column type mapping yet
    finally:
        conn.close()


def test_create_insert_select_roundtrip():
    conn = connect()
    try:
        table = f"mssqlwire_smoke_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'polywire')")
        cur.execute(f"SELECT name FROM {table} WHERE id = 1")
        (name,) = cur.fetchone()
        assert name == "polywire"
        cur.execute(f"DROP TABLE {table}")
    finally:
        conn.close()
