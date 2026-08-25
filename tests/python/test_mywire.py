"""mywire: real MySQL client/server protocol, real PyMySQL client, translated to Postgres
underneath. mywire has no session-scoped connection (a fresh pooled Postgres connection per
statement), so there's no cross-statement transaction state -- no rollback test here for that
reason, matching what wire's own private test suite documents.
"""
import uuid

import pymysql

from conftest import HOST, MYWIRE_PORT


def connect():
    return pymysql.connect(host=HOST, port=MYWIRE_PORT, database="postgres", user="postgres", password="postgres")


def test_simple_select():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 21 * 2")
        (answer,) = cur.fetchone()
        assert answer == 42
    finally:
        conn.close()


def test_create_insert_select_roundtrip():
    conn = connect()
    try:
        table = f"mywire_smoke_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'polywire')")
        cur.execute(f"SELECT name FROM {table} WHERE id = 1")
        (name,) = cur.fetchone()
        assert name == "polywire"
        cur.execute(f"DROP TABLE {table}")
    finally:
        conn.close()
