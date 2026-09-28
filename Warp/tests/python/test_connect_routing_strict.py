"""Connect-time routing, WARP_CONNECT_ROUTING=strict: a database / service name that is not a backend, a
set, or matched by a route is REJECTED with the protocol's own native "unknown database" error --
strict mode is the security boundary (non-strict is a convenience). Real drivers, real Warp, real Postgres.
"""
import json

import grpc
import oracledb
import psycopg2
import pymongo
import pymssql
import pymysql
import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

import warp_pb2
import warp_pb2_grpc
from connect_routing_support import Topology

# `postgres` (the database name psql/most tools default to) is allowed explicitly, and mapped to the
# default backend; everything else must be a backend, a set, or a route.
ROUTES = json.dumps([{"protocol": "postgres", "database": "postgres", "target": "default"},
                     {"database": "legacy_*", "target": "alpha"}])


@pytest.fixture(scope="module")
def topo():
    t = Topology(extra_env={"WARP_CONNECT_ROUTING": "strict", "WARP_CONNECTION_ROUTES": ROUTES})
    yield t
    t.close()


def test_pgwire_rejects_an_unknown_database_with_3D000_after_authentication(topo):
    with pytest.raises(psycopg2.OperationalError) as e:
        psycopg2.connect(host="localhost", port=topo.warp.frontend_port, user="postgres", password="postgres",
                         dbname="nope")
    assert 'database "nope" does not exist' in str(e.value)
    if e.value.pgcode is not None:
        assert e.value.pgcode == "3D000"
    # a wrong password is still an authentication failure, whatever the database name
    with pytest.raises(psycopg2.OperationalError) as e:
        psycopg2.connect(host="localhost", port=topo.warp.frontend_port, user="postgres", password="wrong",
                         dbname="nope")
    assert "password authentication failed" in str(e.value) and "does not exist" not in str(e.value)


def test_pgwire_accepts_backends_sets_and_routes(topo):
    for db, sql, expected in (("node_b", "SELECT name FROM items ORDER BY id", ["b-row", "b-row-2"]),
                              ("NODE_C", "SELECT name FROM items", ["c-row"]),
                              ("postgres", "SELECT name FROM items", ["default-row"]),
                              ("legacy_2019", "SELECT name FROM only_b", ["only-b-row"])):
        c = psycopg2.connect(host="localhost", port=topo.warp.frontend_port, user="postgres", password="postgres",
                             dbname=db)
        try:
            cur = c.cursor()
            cur.execute(sql)
            assert [r[0] for r in cur.fetchall()] == expected, db
        finally:
            c.close()


def test_mysql_rejects_with_1049_at_connect_and_on_use(topo):
    port = topo.port("WARP_MYWIRE_PORT")
    with pytest.raises(pymysql.err.OperationalError) as e:
        pymysql.connect(host="localhost", port=port, user="postgres", password="postgres", database="nope")
    assert e.value.args[0] == 1049 and "nope" in e.value.args[1]
    with pytest.raises(pymysql.err.OperationalError) as e:  # no database at all: blank is unknown in strict mode
        pymysql.connect(host="localhost", port=port, user="postgres", password="postgres")
    assert e.value.args[0] == 1049

    conn = pymysql.connect(host="localhost", port=port, user="postgres", password="postgres", database="node_b",
                           autocommit=True)
    try:
        cur = conn.cursor()
        with pytest.raises(pymysql.err.OperationalError) as e:
            cur.execute("USE nope")
        assert e.value.args[0] == 1049
        with pytest.raises(pymysql.err.OperationalError) as e:
            conn.select_db("nope")  # COM_INIT_DB
        assert e.value.args[0] == 1049
        # a rejected switch leaves the connection where it was
        cur.execute("SELECT name FROM items ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == ["b-row", "b-row-2"]
    finally:
        conn.close()


def test_sqlserver_rejects_with_4060_at_connect_and_on_use(topo):
    port = topo.port("WARP_MSSQLWIRE_PORT")
    with pytest.raises(pymssql.OperationalError) as e:
        pymssql.connect(server="localhost", port=port, user="postgres", password="postgres", database="nope")
    assert "nope" in str(e.value) and "Cannot open database" in str(e.value)

    conn = pymssql.connect(server="localhost", port=port, user="postgres", password="postgres",
                           database="node_c", autocommit=True)
    try:
        cur = conn.cursor()
        with pytest.raises(pymssql.Error) as e:
            cur.execute("USE nope")
        assert "Cannot open database" in str(e.value)
        cur.execute("SELECT name FROM items")
        assert [r[0] for r in cur.fetchall()] == ["c-row"]
    finally:
        conn.close()


def test_oracle_rejects_an_unknown_service_with_ora_12514(topo):
    dsn = f"localhost:{topo.port('WARP_ORAWIRE_PORT')}/nope"
    with pytest.raises(oracledb.Error) as e:
        oracledb.connect(user="postgres", password="postgres", dsn=dsn, disable_oob=True)
    assert "12514" in str(e.value), str(e.value)
    good = oracledb.connect(user="postgres", password="postgres", dsn=dsn.replace("/nope", "/node_b"),
                            disable_oob=True)
    try:
        cur = good.cursor()
        cur.execute("SELECT name FROM items ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == ["b-row", "b-row-2"]
    finally:
        good.close()


def test_mongo_rejects_an_unknown_database_with_namespace_not_found(topo):
    client = pymongo.MongoClient(host="localhost", port=topo.port("WARP_MONGOWIRE_PORT"),
                                 serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")  # handshake / admin commands are not routed
        with pytest.raises(pymongo.errors.PyMongoError) as e:
            client["nope"]["docs"].insert_one({"_id": 1})
        assert "26" in str(e.value) or "not found" in str(e.value), str(e.value)
        client["node_b"]["strict_docs"].insert_one({"_id": 1})
        assert client["node_b"]["strict_docs"].count_documents({}) == 1
    finally:
        client.close()


def test_bolt_rejects_an_unknown_database_with_database_not_found(topo):
    drv = GraphDatabase.driver(f"bolt://localhost:{topo.port('WARP_BOLTWIRE_PORT')}", auth=("postgres", "postgres"))
    try:
        with pytest.raises(ClientError) as e:
            with drv.session(database="nope") as s:
                s.run("RETURN 1 AS x").single()
        assert e.value.code == "Neo.ClientError.Database.DatabaseNotFound"
        with drv.session(database="node_c") as s:
            assert s.run("RETURN 1 AS x").single()["x"] == 1
    finally:
        drv.close()


def test_grpc_rejects_an_unknown_database_with_3D000(topo):
    channel = grpc.insecure_channel(f"localhost:{topo.warp.grpc_port}")
    stub = warp_pb2_grpc.QueryServiceStub(channel)
    try:
        r = stub.Execute(warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 1",
                                                 database="nope"), timeout=10)
        assert not r.success and r.sql_state == "3D000" and "nope" in r.error_message
        r = stub.Execute(warp_pb2.ExecuteRequest(username="postgres", password="postgres",
                                                 sql="SELECT name FROM items", database="node_c"), timeout=10)
        assert r.success and [row.values[0] for row in r.rows] == ["c-row"]
        # an omitted database is blank, which strict mode treats as unknown too
        r = stub.Execute(warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 1"),
                         timeout=10)
        assert not r.success and r.sql_state == "3D000"
    finally:
        channel.close()
