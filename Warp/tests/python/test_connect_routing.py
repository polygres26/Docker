"""Connect-time backend routing (implicit mode): a real client driver picks ONE backend, or a whole
backend SET, with the database / service name it already sends at connect time -- no client code change.

Real Warp process, three real Postgres servers holding DIFFERENT rows in the SAME table name (see
connect_routing_support.py), real drivers: psycopg2, PyMySQL, pymssql, python-oracledb, pymongo, the
neo4j driver and grpcio. Strict-mode rejections live in test_connect_routing_strict.py.
"""
import concurrent.futures
import json
import time

import grpc
import oracledb
import psycopg2
import pymongo
import pymssql
import pymysql
import pytest
import requests
from neo4j import GraphDatabase

import warp_pb2
import warp_pb2_grpc
from connect_routing_support import ADMIN_TOKEN, Topology, rows_in


@pytest.fixture(scope="module")
def topo():
    t = Topology(credentials="postgres=postgres;alice=alicepw;bob=bobpw")
    yield t
    t.close()


# ---- driver helpers ------------------------------------------------------------------------------

def pg(topo, db, user="postgres", password="postgres", autocommit=True):
    c = psycopg2.connect(host="localhost", port=topo.warp.frontend_port, user=user, password=password, dbname=db)
    c.autocommit = autocommit
    return c


def pg_query(topo, db, sql, params=None, **kw):
    c = pg(topo, db, **kw)
    try:
        cur = c.cursor()
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]
    finally:
        c.close()


def pg_error(topo, db, sql):
    c = pg(topo, db)
    try:
        cur = c.cursor()
        with pytest.raises(psycopg2.Error) as e:
            cur.execute(sql)
        return e.value
    finally:
        c.close()


def pg_try(topo, db, sql):
    """The error text of `sql`, or None when it succeeded (for polling)."""
    c = pg(topo, db)
    try:
        c.cursor().execute(sql)
        return None
    except psycopg2.Error as e:
        return str(e)
    finally:
        c.close()


def mysql(topo, db, user="postgres", password="postgres"):
    return pymysql.connect(host="localhost", port=topo.port("WARP_MYWIRE_PORT"), user=user, password=password,
                           database=db, autocommit=True)


def mssql(topo, db, user="postgres", password="postgres"):
    return pymssql.connect(server="localhost", port=topo.port("WARP_MSSQLWIRE_PORT"), user=user,
                           password=password, database=db, autocommit=True)


def oracle(topo, service, user="postgres", password="postgres"):
    return oracledb.connect(user=user, password=password,
                            dsn=f"localhost:{topo.port('WARP_ORAWIRE_PORT')}/{service}", disable_oob=True)


def one_column(cur, sql):
    cur.execute(sql)
    return [r[0] for r in cur.fetchall()]


def admin(topo, method, path, body=None):
    return requests.request(method, f"http://localhost:{topo.warp.metrics_port}{path}", json=body, timeout=30,
                            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})


def wait_for(predicate, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.3)
    raise AssertionError(f"condition not met in {timeout}s: {last!r}")


# ---- pgwire: implicit backend / set names --------------------------------------------------------

def test_database_name_selects_a_backend_and_only_that_backend(topo):
    assert pg_query(topo, "node_b", "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
    assert pg_query(topo, "node_c", "SELECT name FROM items ORDER BY id") == ["c-row"]
    assert pg_query(topo, "default", "SELECT name FROM items ORDER BY id") == ["default-row"]


def test_names_are_case_insensitive(topo):
    assert pg_query(topo, "NODE_B", "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
    assert pg_query(topo, "Node_C", "SELECT name FROM items") == ["c-row"]


def test_a_backend_connection_cannot_reach_another_backend_by_any_path(topo):
    # schema-qualified names that a schema rule would send to node_c / default for an unrouted client
    e = pg_error(topo, "node_b", "SELECT name FROM cs.items")
    assert "c-schema-row" not in str(e) and 'relation "cs.items" does not exist' in str(e)
    e = pg_error(topo, "node_b", "SELECT name FROM ds.items")
    assert "d-schema-row" not in str(e)
    # federated join across the two schema-rule backends: refused, never executed
    e = pg_error(topo, "node_b", "SELECT a.name, b.name FROM cs.items a JOIN ds.items b ON a.id = b.id")
    assert "outside this caller's scope" in str(e)
    # auto-discovery of a table that only exists on another backend: not visible from node_b
    pg_error(topo, "node_b", "SELECT name FROM only_c")
    pg_error(topo, "node_b", "SELECT name FROM only_d")
    # own table still fine
    assert pg_query(topo, "node_b", "SELECT name FROM only_b") == ["only-b-row"]
    # cross-backend join of two bare names: each side lives on a different backend
    pg_error(topo, "node_b", "SELECT * FROM only_b b JOIN only_c c ON b.id = c.id")
    # the backend name as a schema
    pg_error(topo, "node_b", "SELECT name FROM node_c.items")
    # a prepared statement (server-side PREPARE) stays on the pinned backend
    c = pg(topo, "node_b")
    try:
        cur = c.cursor()
        cur.execute("PREPARE q AS SELECT name FROM items WHERE id = $1")
        cur.execute("EXECUTE q(1)")
        assert cur.fetchall() == [("b-row",)]
    finally:
        c.close()


def test_extended_protocol_bind_parameters_stay_on_the_routed_backend(topo):
    assert pg_query(topo, "node_c", "SELECT name FROM items WHERE id = %s", (1,)) == ["c-row"]
    assert pg_query(topo, "node_b", "SELECT name FROM items WHERE id = %s", (2,)) == ["b-row-2"]
    assert pg_query(topo, "node_c", "SELECT name FROM items WHERE id = %s", (2,)) == []


def test_a_transaction_on_a_routed_backend_commits_and_rolls_back_only_there(topo):
    c = pg(topo, "node_b", autocommit=False)
    try:
        cur = c.cursor()
        cur.execute("INSERT INTO items VALUES (50, 'rolled-back')")
        cur.execute("SELECT name FROM items WHERE id = 50")
        assert cur.fetchall() == [("rolled-back",)]
        c.rollback()
        cur.execute("INSERT INTO items VALUES (51, 'committed')")
        c.commit()
    finally:
        c.close()
    assert rows_in(topo.by_backend["node_b"], "SELECT name FROM items WHERE id >= 50") == ["committed"]
    assert rows_in(topo.by_backend["node_c"], "SELECT name FROM items WHERE id >= 50") == []
    assert rows_in(topo.by_backend["default"], "SELECT name FROM items WHERE id >= 50") == []
    with pg(topo, "node_b") as c2:
        cur = c2.cursor()
        cur.execute("DELETE FROM items WHERE id >= 50")


def test_an_unknown_database_name_keeps_todays_behaviour_in_non_strict_mode(topo):
    # unrouted: the schema rule still reaches node_c exactly as it did before connect-time routing existed
    assert pg_query(topo, "no_such_database", "SELECT name FROM cs.items") == ["c-schema-row"]
    assert pg_query(topo, "postgres", "SELECT name FROM cs.items") == ["c-schema-row"]


def test_a_set_name_scopes_the_connection_to_the_sets_members(topo):
    # the set's default backend is its first member (node_b): unqualified statements go there
    assert pg_query(topo, "alpha", "SELECT name FROM only_b") == ["only-b-row"]
    # a router rule still applies, among the members: cs -> node_c is a member
    assert pg_query(topo, "alpha", "SELECT name FROM cs.items") == ["c-schema-row"]
    # a rule that resolves OUTSIDE the set is refused (42501), never executed
    e = pg_error(topo, "alpha", "SELECT name FROM ds.items")
    assert "outside this caller's scope group:alpha" in str(e)
    assert e.pgcode == "42501" or "42501" in str(e) or "outside this caller's scope" in str(e)
    # a cross-backend join of two members federates; a join reaching a non-member is refused
    c = pg(topo, "alpha")
    try:
        cur = c.cursor()
        cur.execute("SELECT b.name, c.name FROM only_b b JOIN only_c c ON b.id = c.id")
        assert cur.fetchall() == [("only-b-row", "only-c-row")]
        with pytest.raises(psycopg2.Error):
            cur.execute("SELECT b.name, d.name FROM only_b b JOIN only_d d ON b.id = d.id")
    finally:
        c.close()
    # same table name in two members of a PLAIN set is the existing, documented conflict -- not a silent pick
    e = pg_error(topo, "alpha", "SELECT name FROM items")
    assert "ambiguous" in str(e)


def idle_in_transaction(pg_server):
    return int(rows_in(pg_server, "SELECT count(*)::text FROM pg_stat_activity WHERE state = 'idle in transaction'")[0])


def test_multiplexing_a_transaction_holds_a_connection_on_the_routed_backend_only(topo):
    """The session lease is for the default backend; a session routed elsewhere must never hold (or
    even borrow) a default-backend connection for its transaction, and must not leak one to a sibling."""
    dflt, b, c = (topo.by_backend[n] for n in ("default", "node_b", "node_c"))
    assert (idle_in_transaction(dflt), idle_in_transaction(b), idle_in_transaction(c)) == (0, 0, 0)

    # pgwire
    conn = pg(topo, "node_b", autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO items VALUES (70, 'held')")
        assert (idle_in_transaction(dflt), idle_in_transaction(b), idle_in_transaction(c)) == (0, 1, 0)
        conn.rollback()
    finally:
        conn.close()
    wait_for(lambda: idle_in_transaction(b) == 0)

    # mywire
    my = pymysql.connect(host="localhost", port=topo.port("WARP_MYWIRE_PORT"), user="postgres",
                         password="postgres", database="node_c", autocommit=False)
    try:
        cur = my.cursor()
        cur.execute("INSERT INTO items VALUES (71, 'held')")
        assert (idle_in_transaction(dflt), idle_in_transaction(b), idle_in_transaction(c)) == (0, 0, 1)
        my.rollback()
    finally:
        my.close()
    wait_for(lambda: idle_in_transaction(c) == 0)

    # mssqlwire
    ms = pymssql.connect(server="localhost", port=topo.port("WARP_MSSQLWIRE_PORT"), user="postgres",
                         password="postgres", database="node_b", autocommit=False)
    try:
        cur = ms.cursor()
        cur.execute("INSERT INTO items VALUES (72, 'held')")
        assert (idle_in_transaction(dflt), idle_in_transaction(b), idle_in_transaction(c)) == (0, 1, 0)
        ms.rollback()
    finally:
        ms.close()
    wait_for(lambda: idle_in_transaction(b) == 0)
    for server in (b, c):
        assert rows_in(server, "SELECT name FROM items WHERE id >= 70") == []

    # a DEFAULT-backend (unrouted) session still pins its own default connection as before
    conn = pg(topo, "default", autocommit=False)
    try:
        conn.cursor().execute("INSERT INTO items VALUES (73, 'held')")
        assert idle_in_transaction(dflt) == 1
        conn.rollback()
    finally:
        conn.close()


def test_sessions_on_different_backends_interleave_without_cross_talk(topo):
    a, b, c = pg(topo, "node_b"), pg(topo, "node_c"), pg(topo, "alpha")
    try:
        ca, cb, cc = a.cursor(), b.cursor(), c.cursor()
        for _ in range(40):
            ca.execute("SELECT name FROM items WHERE id = 1")
            cb.execute("SELECT name FROM items WHERE id = 1")
            cc.execute("SELECT name FROM cs.items")
            cc2 = c.cursor()
            cc2.execute("SELECT name FROM only_b")
            assert ca.fetchall() == [("b-row",)]
            assert cb.fetchall() == [("c-row",)]
            assert cc.fetchall() == [("c-schema-row",)]
            assert cc2.fetchall() == [("only-b-row",)]
    finally:
        for x in (a, b, c):
            x.close()


def test_concurrent_clients_each_see_only_their_own_backend(topo):
    expected = {"node_b": ["b-row", "b-row-2"], "node_c": ["c-row"], "default": ["default-row"]}

    def worker(i):
        db = ["node_b", "node_c", "default"][i % 3]
        c = pg(topo, db)
        try:
            cur = c.cursor()
            for _ in range(25):
                cur.execute("SELECT name FROM items ORDER BY id")
                assert [r[0] for r in cur.fetchall()] == expected[db], db
        finally:
            c.close()
        return db

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
        assert sorted(pool.map(worker, range(18))).count("node_b") == 6


# ---- route by login user, explicit routes, admin API, hot reload ---------------------------------

def test_explicit_routes_admin_api_validation_and_connect_as(topo):
    data = admin(topo, "GET", "/api/backend-sets").json()
    by_set = {s["name"]: s for s in data["sets"]}
    assert by_set["alpha"]["connectAs"] == "alpha"
    assert {b["name"]: b["connectAs"] for b in by_set["alpha"]["backends"]} == {"node_b": "node_b", "node_c": "node_c"}
    assert data["connectionRouting"]["mode"] == "implicit" and data["connectionRouting"]["routes"] == []

    assert admin(topo, "POST", "/api/connection-routes", {"database": "x", "target": "nope"}).status_code == 400
    assert admin(topo, "POST", "/api/connection-routes", {"target": "node_b"}).status_code == 400
    assert admin(topo, "POST", "/api/connection-routes",
                 {"protocol": "gopher", "database": "x", "target": "node_b"}).status_code == 400
    assert admin(topo, "POST", "/api/connection-routes",
                 {"database": "x", "target": "alpha", "defaultBackend": "default"}).status_code == 400
    r = admin(topo, "POST", "/api/connection-routes", {"database": "shared_app", "user": "alice", "target": "node_b"})
    assert r.status_code == 201, r.text
    assert admin(topo, "POST", "/api/connection-routes",
                 {"database": "SHARED_APP", "user": "ALICE", "target": "node_c"}).status_code == 409
    r = admin(topo, "POST", "/api/connection-routes", {"database": "shared_app", "user": "bob", "target": "node_c"})
    assert r.status_code == 201
    assert [x["target"] for x in admin(topo, "GET", "/api/connection-routes").json()["routes"]] == ["node_b", "node_c"]
    # a backend a route points at cannot be deleted from under it
    d = admin(topo, "DELETE", "/api/backend-sets/alpha/backends/node_b")
    assert d.status_code == 409 and "connection route" in d.json()["error"]


def test_route_by_login_user_the_same_database_name_reaches_different_backends(topo):
    assert pg_query(topo, "shared_app", "SELECT name FROM items ORDER BY id", user="alice",
                    password="alicepw") == ["b-row", "b-row-2"]
    assert pg_query(topo, "shared_app", "SELECT name FROM items", user="bob", password="bobpw") == ["c-row"]
    # neither user matches -> unrouted (non-strict): today's behaviour, not an error
    assert pg_query(topo, "shared_app", "SELECT name FROM cs.items") == ["c-schema-row"]
    # bob (routed to node_c) cannot reach node_b's rows by any path
    e = pg_error_as(topo, "shared_app", "bob", "bobpw", "SELECT name FROM only_b")
    assert "only-b-row" not in str(e)
    my = mysql(topo, "shared_app", user="alice", password="alicepw")
    try:
        assert one_column(my.cursor(), "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
    finally:
        my.close()
    ora = oracle(topo, "shared_app", user="bob", password="bobpw")
    try:
        assert one_column(ora.cursor(), "SELECT name FROM items") == ["c-row"]
    finally:
        ora.close()


def pg_error_as(topo, db, user, password, sql):
    c = pg(topo, db, user=user, password=password)
    try:
        with pytest.raises(psycopg2.Error) as e:
            c.cursor().execute(sql)
        return e.value
    finally:
        c.close()


def test_a_route_added_through_the_admin_api_is_live_and_removal_takes_effect(topo):
    r = admin(topo, "POST", "/api/connection-routes", {"database": "sales_*", "target": "alpha",
                                                        "defaultBackend": "node_c"})
    assert r.status_code == 201, r.text
    # the set's default backend for this route is node_c: an unqualified table lives there
    assert pg_query(topo, "sales_eu", "SELECT name FROM only_c") == ["only-c-row"]
    assert pg_query(topo, "SALES_US", "SELECT name FROM only_c") == ["only-c-row"]
    rid = r.json()["id"]
    assert admin(topo, "PATCH", "/api/connection-routes/" + requests.utils.quote(rid, safe=""),
                 {"defaultBackend": "node_b"}).status_code == 200
    assert pg_query(topo, "sales_eu", "SELECT name FROM only_b") == ["only-b-row"]
    assert admin(topo, "DELETE", "/api/connection-routes/" + requests.utils.quote(rid, safe="")).status_code == 200
    # no route, not a backend/set name -> unrouted (non-strict): the ambiguity of unrouted `items` returns
    assert "ambiguous" in str(pg_error(topo, "sales_eu", "SELECT name FROM items"))
    assert admin(topo, "DELETE", "/api/connection-routes/" + requests.utils.quote(rid, safe="")).status_code == 404


def test_hot_reload_through_the_config_version_stream(topo):
    # PUT /api/config writes a new warp_config version; the LISTEN/NOTIFY reload every instance runs
    # picks up connectionRoutes with no restart
    routes = json.dumps([{"database": "reloaded", "target": "node_c"}])
    assert admin(topo, "PUT", "/api/config", {"connectionRoutes": routes}).status_code == 200
    wait_for(lambda: pg_query(topo, "reloaded", "SELECT name FROM items") == ["c-row"])
    assert admin(topo, "PUT", "/api/config", {"connectionRoutes": json.dumps(
        [{"database": "reloaded", "target": "node_b"}])}).status_code == 200
    wait_for(lambda: pg_query(topo, "reloaded", "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"])
    bad = admin(topo, "PUT", "/api/config", {"connectionRoutes": "not json"})
    assert bad.status_code >= 400
    assert admin(topo, "PUT", "/api/config", {"connectionRoutes": None}).status_code == 200
    # the user-scoped routes added above were replaced by that write too: back to unrouted
    wait_for(lambda: "ambiguous" in str(pg_try(topo, "reloaded", "SELECT name FROM items")))


# ---- mywire ---------------------------------------------------------------------------------------

def test_mysql_database_selects_a_backend_and_use_switches_it_mid_session(topo):
    conn = mysql(topo, "node_b")
    try:
        cur = conn.cursor()
        assert one_column(cur, "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
        cur.execute("USE node_c")
        assert one_column(cur, "SELECT name FROM items") == ["c-row"]
        conn.select_db("default")  # COM_INIT_DB
        assert one_column(cur, "SELECT name FROM items") == ["default-row"]
        conn.select_db("NODE_B")
        assert one_column(cur, "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
        # a routed MySQL client cannot reach another backend
        with pytest.raises(pymysql.MySQLError):
            cur.execute("SELECT name FROM only_c")
        with pytest.raises(pymysql.MySQLError):
            cur.execute("SELECT a.name FROM cs.items a JOIN ds.items b ON a.id = b.id")
    finally:
        conn.close()


def test_mysql_transaction_and_prepared_statement_on_a_routed_backend(topo):
    conn = pymysql.connect(host="localhost", port=topo.port("WARP_MYWIRE_PORT"), user="postgres",
                           password="postgres", database="node_c", autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO items VALUES (60, 'my-rolled-back')")
        conn.rollback()
        cur.execute("INSERT INTO items VALUES (61, 'my-committed')")
        conn.commit()
        cur.execute("SELECT name FROM items WHERE id = %s", (61,))  # pymysql interpolates client-side
        assert cur.fetchall() == (("my-committed",),)
    finally:
        conn.close()
    assert rows_in(topo.by_backend["node_c"], "SELECT name FROM items WHERE id >= 60") == ["my-committed"]
    assert rows_in(topo.by_backend["node_b"], "SELECT name FROM items WHERE id >= 60") == []
    with pg(topo, "node_c") as c2:
        c2.cursor().execute("DELETE FROM items WHERE id >= 60")


def test_mysql_set_name_scopes_the_connection(topo):
    conn = mysql(topo, "alpha")
    try:
        cur = conn.cursor()
        assert one_column(cur, "SELECT name FROM only_b") == ["only-b-row"]
        with pytest.raises(pymysql.MySQLError):
            cur.execute("SELECT name FROM ds.items")
    finally:
        conn.close()


# ---- mssqlwire -----------------------------------------------------------------------------------

def test_sqlserver_database_selects_a_backend_and_use_switches_it_mid_session(topo):
    conn = mssql(topo, "node_b")
    try:
        cur = conn.cursor()
        assert one_column(cur, "SELECT name FROM items ORDER BY id") == ["b-row", "b-row-2"]
        cur.execute("USE node_c")
        assert one_column(cur, "SELECT name FROM items") == ["c-row"]
        cur.execute("USE [default]")
        assert one_column(cur, "SELECT name FROM items") == ["default-row"]
        with pytest.raises(pymssql.Error):
            cur.execute("SELECT name FROM only_c")  # routed to `default`: node_c's table is not reachable
    finally:
        conn.close()


def test_sqlserver_set_name_scopes_the_connection(topo):
    conn = mssql(topo, "alpha")
    try:
        cur = conn.cursor()
        assert one_column(cur, "SELECT name FROM only_b") == ["only-b-row"]
        with pytest.raises(pymssql.Error):
            cur.execute("SELECT name FROM ds.items")
    finally:
        conn.close()


# ---- orawire -------------------------------------------------------------------------------------

def test_oracle_service_name_selects_a_backend(topo):
    for service, expected in (("node_b", ["b-row", "b-row-2"]), ("NODE_C", ["c-row"]), ("default", ["default-row"])):
        conn = oracle(topo, service)
        try:
            assert one_column(conn.cursor(), "SELECT name FROM items ORDER BY id") == expected, service
        finally:
            conn.close()
    conn = oracle(topo, "node_b")
    try:
        with pytest.raises(oracledb.Error):
            conn.cursor().execute("SELECT name FROM only_c")
    finally:
        conn.close()


def test_oracle_service_name_can_select_a_set(topo):
    conn = oracle(topo, "alpha")
    try:
        assert one_column(conn.cursor(), "SELECT name FROM only_b") == ["only-b-row"]
        with pytest.raises(oracledb.Error):
            conn.cursor().execute("SELECT name FROM ds.items")
    finally:
        conn.close()


# ---- mongowire ($db per command) -----------------------------------------------------------------

def test_mongo_db_name_selects_the_backend_that_stores_the_documents(topo):
    client = pymongo.MongoClient(host="localhost", port=topo.port("WARP_MONGOWIRE_PORT"),
                                 serverSelectionTimeoutMS=5000)
    try:
        client["node_b"]["docs"].insert_one({"_id": 1, "who": "b"})
        client["node_c"]["docs"].insert_one({"_id": 1, "who": "c"})
        assert client["node_b"]["docs"].find_one({"_id": 1})["who"] == "b"
        assert client["node_c"]["docs"].find_one({"_id": 1})["who"] == "c"
        assert [d["who"] for d in client["node_b"]["docs"].find({})] == ["b"]
        # the documents really live in the routed Postgres, and nowhere else
        assert rows_in(topo.by_backend["node_b"], "SELECT count(*)::text FROM node_b.docs") == ["1"]
        assert rows_in(topo.by_backend["node_c"], "SELECT count(*)::text FROM node_c.docs") == ["1"]
        assert rows_in(topo.by_backend["default"],
                       "SELECT count(*)::text FROM information_schema.tables WHERE table_name = 'docs'") == ["0"]
        assert rows_in(topo.by_backend["node_b"],
                       "SELECT count(*)::text FROM information_schema.tables WHERE table_schema = 'node_c'") == ["0"]
        # a set: documents hashed by _id across its members
        for i in range(2, 22):
            client["alpha"]["spread"].insert_one({"_id": i})
        assert client["alpha"]["spread"].count_documents({}) == 20
        on_b = int(rows_in(topo.by_backend["node_b"], "SELECT count(*)::text FROM alpha.spread")[0])
        on_c = int(rows_in(topo.by_backend["node_c"], "SELECT count(*)::text FROM alpha.spread")[0])
        assert on_b + on_c == 20 and on_b > 0 and on_c > 0
        # a database that is not a backend/set (non-strict): today's placement (the store's own host)
        client["plain"]["docs"].insert_one({"_id": 9})
        assert client["plain"]["docs"].count_documents({}) == 1
    finally:
        client.close()


# ---- boltwire (db in RUN) ------------------------------------------------------------------------

def test_bolt_database_selects_the_backend_that_stores_the_graph(topo):
    drv = GraphDatabase.driver(f"bolt://localhost:{topo.port('WARP_BOLTWIRE_PORT')}", auth=("postgres", "postgres"))
    try:
        with drv.session(database="node_b") as s:
            s.run("CREATE (n:Person {name: 'bolt-b'}) RETURN n.name AS name").single()
        with drv.session(database="node_c") as s:
            s.run("CREATE (n:Person {name: 'bolt-c'}) RETURN n.name AS name").single()
        with drv.session(database="node_b") as s:
            assert [r["name"] for r in s.run("MATCH (n:Person) RETURN n.name AS name")] == ["bolt-b"]
        with drv.session(database="node_c") as s:
            assert [r["name"] for r in s.run("MATCH (n:Person) RETURN n.name AS name")] == ["bolt-c"]
        assert rows_in(topo.by_backend["node_b"], "SELECT properties->>'name' FROM warp_graph_nodes") == ["bolt-b"]
        assert rows_in(topo.by_backend["node_c"], "SELECT properties->>'name' FROM warp_graph_nodes") == ["bolt-c"]
    finally:
        drv.close()


# ---- gRPC (ExecuteRequest.database) --------------------------------------------------------------

def test_grpc_database_field_selects_a_backend_and_stays_optional(topo):
    channel = grpc.insecure_channel(f"localhost:{topo.warp.grpc_port}")
    stub = warp_pb2_grpc.QueryServiceStub(channel)

    def run(sql, database=None):
        req = warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql=sql)
        if database is not None:
            req.database = database
        return stub.Execute(req, timeout=10)

    try:
        r = run("SELECT name FROM items ORDER BY id", "node_b")
        assert r.success and [row.values[0] for row in r.rows] == ["b-row", "b-row-2"]
        r = run("SELECT name FROM items", "NODE_C")
        assert r.success and [row.values[0] for row in r.rows] == ["c-row"]
        r = run("SELECT name FROM only_c", "node_b")
        assert not r.success
        # no database field at all: exactly the pre-existing behaviour
        r = run("SELECT name FROM cs.items")
        assert r.success and [row.values[0] for row in r.rows] == ["c-schema-row"]
    finally:
        channel.close()
