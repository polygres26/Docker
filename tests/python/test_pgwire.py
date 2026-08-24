"""pgwire: real Postgres wire protocol v3, real psycopg2 client -- this one's a direct
passthrough (no dialect translation), so it's the baseline every other protocol's result should
match.
"""
import uuid

import psycopg2

from conftest import HOST, PGWIRE_PORT


def connect():
    return psycopg2.connect(host=HOST, port=PGWIRE_PORT, dbname="postgres", user="postgres", password="postgres")


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
        table = f"pgwire_smoke_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'polywire')")
        conn.commit()
        cur.execute(f"SELECT name FROM {table} WHERE id = 1")
        (name,) = cur.fetchone()
        assert name == "polywire"
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
    finally:
        conn.close()


def test_transaction_rollback():
    conn = connect()
    try:
        table = f"pgwire_rollback_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY)")
        conn.commit()
        cur.execute(f"INSERT INTO {table} (id) VALUES (1)")
        conn.rollback()
        cur.execute(f"SELECT count(*) FROM {table}")
        (count,) = cur.fetchone()
        assert count == 0
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
    finally:
        conn.close()
