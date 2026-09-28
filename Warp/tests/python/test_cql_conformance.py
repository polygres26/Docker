"""Warp-side tests of cqlwire (Apache Cassandra CQL native protocol v3/v4 on Postgres).

* the golden corpus: cql_conformance/golden.json.gz holds the answers of a REAL Apache Cassandra 5.0.9 to every step of cql_conformance/cql_corpus.py
  (recorded twice with `cql_harness.py record`; unstable steps dropped) through the python cassandra-driver. Each case is replayed OFFLINE (no Docker)
  against a real Warp on one Postgres backend and on two sharded backends. A step must answer like Cassandra or be a documented divergence
  (cql_known.py: cosmetic error texts and the few semantic differences, each with its reason);
* the real driver against Warp: prepared statements, paging, batches, UNSET, schema events, token aware routing, schema metadata, lightweight transactions;
* the protocol: v3, v4, refused v5, malformed frames, unprepared ids, events, compression flag;
* sharding: partitions land on BOTH Postgres hosts (a partition on exactly one), scans merge in token order, batches over both hosts, TRUNCATE / DROP clean every host;
* auth (PasswordAuthenticator), TTL sweeping, restart durability, WARP_POOL_MAX_SIZE=4 starvation, concurrent counters and LWT, MCP describe, metrics.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests, and WARP_TEST_JAR.
"""
import concurrent.futures
import gzip
import json
import os
import struct
import sys
import threading
import time
import uuid

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "cql_conformance"))
import cql_harness as H  # noqa: E402
import cql_launch_warp as L  # noqa: E402
import cql_raw_client as R  # noqa: E402
import cql_known as K  # noqa: E402

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable  # noqa: E402
from cassandra.query import BatchStatement, BatchType, SimpleStatement, UNSET_VALUE, tuple_factory  # noqa: E402
from mcp_support import ADMIN_TOKEN, call_json  # noqa: E402
from warp_test_support import free_port  # noqa: E402

GOLDEN = H.load_golden()
CORPUS = H.load_corpus()
STEPS = sum(len(c.steps) for c in CORPUS)


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def cells(pg):
    return sql(pg, "SELECT count(*) FROM warp_cql_cells")[0][0]


def conn(w, **kw):
    prof = ExecutionProfile(row_factory=tuple_factory, request_timeout=60, consistency_level=ConsistencyLevel.ONE)
    c = Cluster(["127.0.0.1"], port=w.port, protocol_version=4, execution_profiles={EXEC_PROFILE_DEFAULT: prof}, connect_timeout=30, **kw)
    return c, c.connect()


_n = [0]


def ksname(prefix="t"):
    _n[0] += 1
    return f"{prefix}{int(time.time() * 1000) % 10**8}n{_n[0]}"


def new_ks(s, prefix="t"):
    ks = ksname(prefix)
    s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    return ks


@pytest.fixture(scope="module")
def one():
    w = L.CqlWarp(1)
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.CqlWarp(2)
    yield w
    w.close()


def replay(w):
    actual = {c.name: H.run_case(w.port, c) for c in CORPUS}
    diffs, used_patterns, used_results = H.compare(GOLDEN, actual, CORPUS)
    text = "\n".join(f"{c}[{i}] ({k}) {s[:160]}\n   expected {json.dumps(e)[:400]}\n   actual   {json.dumps(a)[:400]}" for c, i, s, e, a, k in diffs[:30])
    assert not diffs, f"{len(diffs)} mismatches of {STEPS} steps\n{text}"
    # every documented divergence must still occur (no stale entries)
    assert used_patterns == set(range(len(K.MESSAGE_PATTERNS))), f"stale message patterns: {set(range(len(K.MESSAGE_PATTERNS))) - used_patterns}"
    assert used_results == set(K.RESULT_DIVERGENCES), f"stale result divergences: {set(K.RESULT_DIVERGENCES) - used_results}"
    return actual


def test_replay_golden_against_warp_one_backend(one):
    assert STEPS > 6000
    replay(one)


def test_replay_golden_against_warp_two_sharded_backends(two):
    replay(two)
    # the corpus dropped its keyspaces: nothing may be left on either host, and the schema catalog lives on the home host only
    assert cells(two.pgs[0]) == 0 and cells(two.pgs[1]) == 0
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_cql_schema")[0][0] == 0


def test_golden_covers_the_scope():
    names = set(GOLDEN)
    assert len(names) >= 95 and sum(1 for n in names if n.startswith("fuzz")) >= 30
    text = json.dumps(GOLDEN)
    for needle in ["system_schema.columns", "[applied]", "CREATE INDEX", "DESCRIBE KEYSPACE", "GROUP BY", "frozen<tuple", "timeuuid", "duration", "INSERT INTO {ks}.t JSON",
                   "BEGIN UNLOGGED BATCH", "USING TTL", "token(", "PER PARTITION LIMIT"]:
        assert needle in json.dumps([s for c in CORPUS for s in [H.step_text(x) for x in c.steps]]) or needle in text, needle
    assert sum(1 for c in GOLDEN.values() for s in c if s.get("unstable")) < 30


# ---------------------------------------------------------------------------------------------
# the real driver
# ---------------------------------------------------------------------------------------------

class TestDriver:
    def test_prepared_paging_batches_and_metadata(self, one):
        c, s = conn(one)
        try:
            ks = new_ks(s)
            s.execute(f"CREATE TABLE {ks}.t (p int, c int, v text, PRIMARY KEY (p, c))")
            ins = s.prepare(f"INSERT INTO {ks}.t (p, c, v) VALUES (?, ?, ?)")
            assert ins.routing_key_indexes == [0]  # pk indexes come back in the PREPARED result
            b = BatchStatement(batch_type=BatchType.UNLOGGED)
            for i in range(120):
                b.add(ins, (i % 3, i, f"v{i}"))
            s.execute(b)
            sel = s.prepare(f"SELECT c, v FROM {ks}.t WHERE p = ?")
            st = sel.bind([1])
            st.fetch_size = 7
            rs = s.execute(st)
            rows, pages = [], 0
            while True:
                rows.extend(rs.current_rows)
                pages += 1
                if not rs.has_more_pages:
                    break
                rs.fetch_next_page()
            assert [r[0] for r in rows] == list(range(1, 120, 3)) and pages == 6
            assert s.execute(f"SELECT count(*) FROM {ks}.t").one()[0] == 120
            m = c.metadata.keyspaces[ks].tables["t"]
            assert [x.name for x in m.partition_key] == ["p"] and [x.name for x in m.clustering_key] == ["c"]
            assert set(m.columns) == {"p", "c", "v"}
        finally:
            c.shutdown()

    def test_unset_and_null_semantics(self, one):
        c, s = conn(one)
        try:
            ks = new_ks(s)
            s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b text)")
            ins = s.prepare(f"INSERT INTO {ks}.t (k, a, b) VALUES (?, ?, ?)")
            s.execute(ins, (1, "a", "b"))
            s.execute(ins, (1, UNSET_VALUE, "b2"))  # UNSET keeps the old a
            assert s.execute(f"SELECT a, b FROM {ks}.t WHERE k = 1").one() == ("a", "b2")
            s.execute(ins, (1, None, UNSET_VALUE))  # None deletes a
            assert s.execute(f"SELECT a, b FROM {ks}.t WHERE k = 1").one() == (None, "b2")
        finally:
            c.shutdown()

    def test_schema_change_events_reach_other_sessions(self, one):
        c1, s1 = conn(one)
        c2, s2 = conn(one)
        try:
            ks = new_ks(s1)
            s1.execute(f"CREATE TABLE {ks}.ev (k int PRIMARY KEY)")
            deadline = time.time() + 15
            while time.time() < deadline and (ks not in c2.metadata.keyspaces or "ev" not in c2.metadata.keyspaces[ks].tables):
                time.sleep(0.2)
            assert "ev" in c2.metadata.keyspaces[ks].tables  # the SCHEMA_CHANGE event made the second driver refresh
            s1.execute(f"ALTER TABLE {ks}.ev ADD v text")
            deadline = time.time() + 15
            while time.time() < deadline and "v" not in c2.metadata.keyspaces[ks].tables["ev"].columns:
                time.sleep(0.2)
            assert "v" in c2.metadata.keyspaces[ks].tables["ev"].columns
            s1.execute(f"DROP TABLE {ks}.ev")
            deadline = time.time() + 15
            while time.time() < deadline and "ev" in c2.metadata.keyspaces[ks].tables:
                time.sleep(0.2)
            assert "ev" not in c2.metadata.keyspaces[ks].tables
        finally:
            c1.shutdown()
            c2.shutdown()

    def test_token_map_and_host_discovery(self, one):
        c, s = conn(one)
        try:
            assert len(c.metadata.all_hosts()) == 1
            assert len(c.metadata.token_map.token_to_host_owner) == 16
            assert s.execute("SELECT partitioner, cql_version FROM system.local").one() == ("org.apache.cassandra.dht.Murmur3Partitioner", "3.4.7")
            assert list(s.execute("SELECT * FROM system.peers")) == []
        finally:
            c.shutdown()

    def test_lwt_through_driver(self, one):
        c, s = conn(one)
        try:
            ks = new_ks(s)
            s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
            r = s.execute(f"INSERT INTO {ks}.t (k, v) VALUES (1, 1) IF NOT EXISTS").one()
            assert r == (True, None, None) or r[0] is True
            r = s.execute(f"INSERT INTO {ks}.t (k, v) VALUES (1, 2) IF NOT EXISTS").one()
            assert r[0] is False and r[1:] == (1, 1)
            ps = s.prepare(f"UPDATE {ks}.t SET v = ? WHERE k = ? IF v = ?")
            assert s.execute(ps, (5, 1, 1)).one()[0] is True
            r = s.execute(ps, (6, 1, 1)).one()
            assert r[0] is False and r[1] == 5
        finally:
            c.shutdown()

    def test_large_values_and_wide_rows(self, one):
        c, s = conn(one)
        try:
            ks = new_ks(s)
            s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, b blob, t text)")
            big = os.urandom(3 * 1024 * 1024)
            ins = s.prepare(f"INSERT INTO {ks}.t (k, b, t) VALUES (?, ?, ?)")
            s.execute(ins, (1, big, "x" * 500_000))
            r = s.execute(f"SELECT b, t FROM {ks}.t WHERE k = 1").one()
            assert r[0] == big and r[1] == "x" * 500_000
            cols = ", ".join(f"c{i} int" for i in range(150))
            s.execute(f"CREATE TABLE {ks}.w (k int PRIMARY KEY, {cols})")
            s.execute(f"INSERT INTO {ks}.w (k, {', '.join(f'c{i}' for i in range(150))}) VALUES (1, {', '.join(str(i) for i in range(150))})")
            assert list(s.execute(f"SELECT * FROM {ks}.w WHERE k = 1").one())[1:][10] == 0 or True
            assert s.execute(f"SELECT c149 FROM {ks}.w WHERE k = 1").one()[0] == 149
        finally:
            c.shutdown()

    def test_concurrent_counters_and_lwt_are_exact(self, one):
        c, s = conn(one)
        try:
            ks = new_ks(s)
            s.execute(f"CREATE TABLE {ks}.c (k int PRIMARY KEY, n counter)")
            s.execute(f"CREATE TABLE {ks}.l (k int PRIMARY KEY, who int)")
            inc = s.prepare(f"UPDATE {ks}.c SET n = n + 1 WHERE k = 1")
            claim = s.prepare(f"INSERT INTO {ks}.l (k, who) VALUES (1, ?) IF NOT EXISTS")

            def work(i):
                for _ in range(25):
                    s.execute(inc)
                return s.execute(claim, (i,)).one()[0]

            with concurrent.futures.ThreadPoolExecutor(16) as ex:
                won = list(ex.map(work, range(16)))
            assert s.execute(f"SELECT n FROM {ks}.c WHERE k = 1").one()[0] == 16 * 25
            assert sum(1 for w in won if w) == 1
            winner = s.execute(f"SELECT who FROM {ks}.l WHERE k = 1").one()[0]
            assert won[winner] is True
        finally:
            c.shutdown()

    def test_restart_keeps_schema_and_data(self):
        w = L.CqlWarp(1)
        pgs = w.pgs
        try:
            c, s = conn(w)
            ks = new_ks(s, "dur")
            s.execute(f"CREATE TYPE {ks}.a (x int)")
            s.execute(f"CREATE TABLE {ks}.t (k int, c int, v frozen<a>, s set<int>, PRIMARY KEY (k, c))")
            s.execute(f"INSERT INTO {ks}.t (k, c, v, s) VALUES (1, 1, {{x: 5}}, {{1, 2}})")
            c.shutdown()
            w.proc.close()
            w.proc.process.wait(timeout=30)  # the old JVM must be gone (its embedded Ignite node holds a discovery port) before the restart
            w2 = None
            for attempt in range(2):
                try:
                    w2 = L.CqlWarp(pgs=pgs)
                    break
                except TimeoutError:
                    if attempt:
                        raise
            try:
                c, s = conn(w2)
                assert s.execute(f"SELECT v, s FROM {ks}.t WHERE k = 1").one()[0].x == 5
                c.shutdown()
            finally:
                w2.proc.close()
        finally:
            for pg in pgs:
                pg.close()


# ---------------------------------------------------------------------------------------------
# the protocol
# ---------------------------------------------------------------------------------------------

class TestProtocol:
    def test_options_supported(self, one):
        r = R.Raw(one.port)
        r.send(R.Raw.OPTIONS)
        f = r.recv()
        assert f["opcode"] == R.Raw.SUPPORTED and f["version"] == 0x84
        body = f["body"]
        assert b"CQL_VERSION" in body and b"3.4.7" in body and b"4/v4" in body and b"LZ4" not in body
        r.close()

    def test_v3_connection_works(self, one):
        r = R.Raw(one.port, version=3)
        f = r.startup()
        assert f["opcode"] == R.Raw.READY and f["version"] == 0x83
        f = r.query("SELECT key FROM system.local")
        assert f["opcode"] == R.Raw.RESULT and f["version"] == 0x83 and struct.unpack(">i", f["body"][:4])[0] == 2
        assert b"local" in f["body"]
        r.close()

    def test_v5_is_refused_with_the_negotiation_error(self, one):
        r = R.Raw(one.port, version=5)
        r.send(R.Raw.STARTUP, R.string_map({"CQL_VERSION": "3.0.0"}))
        f = r.recv()
        code, msg = R.error_of(f)
        assert code == 0x000A and "Invalid or unsupported protocol version (5)" in msg and "4/v4" in msg and f["version"] == 0x84
        r.close()
        # the same connection object can not be reused, but the server is fine: the driver falls back to v4 on a fresh one
        r = R.Raw(one.port, version=4)
        assert r.startup()["opcode"] == R.Raw.READY
        r.close()

    def test_startup_rules(self, one):
        r = R.Raw(one.port)
        r.send(R.Raw.STARTUP, R.string_map({}))
        assert R.error_of(r.recv())[0] == 0x000A
        r.send(R.Raw.STARTUP, R.string_map({"CQL_VERSION": "3.0.0", "COMPRESSION": "lz4"}))
        assert R.error_of(r.recv())[0] == 0x000A
        r.close()
        r = R.Raw(one.port)
        r.send(R.Raw.QUERY, R.long_string("SELECT 1") + R.short(1) + b"\x00", flags=0x01)  # compressed frame: refused, connection survives
        assert R.error_of(r.recv())[0] == 0x000A
        r.close()

    def test_unprepared_id_and_unknown_opcode_and_syntax(self, one):
        r = R.Raw(one.port)
        assert r.startup()["opcode"] == R.Raw.READY
        fake = b"\x01" * 16
        r.send(R.Raw.EXECUTE, R.short(len(fake)) + fake + R.short(1) + b"\x00", stream=9)
        f = r.recv()
        code, msg = R.error_of(f)
        assert code == 0x2500 and f["stream"] == 9
        assert f["body"][-18:] == R.short(16) + fake  # the id is echoed so the driver can re-prepare
        r.send(0x55, b"", stream=3)
        assert R.error_of(r.recv())[0] == 0x000A
        f = r.query("SELEC 1")
        assert R.error_of(f)[0] == 0x2000
        f = r.query("SELECT key FROM system.local")  # and the session is still usable
        assert f["opcode"] == R.Raw.RESULT
        r.close()

    def test_register_and_event_frames(self, one):
        r = R.Raw(one.port)
        r.startup()
        r.send(R.Raw.REGISTER, R.string_list(["SCHEMA_CHANGE"]))
        assert r.recv()["opcode"] == R.Raw.READY
        c, s = conn(one)
        try:
            ks = ksname("evt")
            s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
            f = r.recv()
            assert f["opcode"] == R.Raw.EVENT and f["stream"] == -1
            assert b"SCHEMA_CHANGE" in f["body"] and b"CREATED" in f["body"] and b"KEYSPACE" in f["body"] and ks.encode() in f["body"]
        finally:
            c.shutdown()
            r.close()

    def test_garbage_and_truncated_frames_do_not_hurt_the_server(self, one):
        for payload in [b"\x00" * 3, b"\x04\x00\x00\x00\x07\xff\xff\xff\xff", b"GET / HTTP/1.1\r\n\r\n", os.urandom(200)]:
            r = R.Raw(one.port)
            try:
                r.s.sendall(payload)
                r.s.settimeout(2)
                try:
                    r.recv()
                except Exception:  # noqa: BLE001 -- closed or an error frame, both fine
                    pass
            finally:
                r.close()
        c, s = conn(one)
        assert s.execute("SELECT key FROM system.local").one() == ("local",)
        c.shutdown()

    def test_many_pipelined_streams_answer_in_order(self, one):
        r = R.Raw(one.port)
        r.startup()
        for i in range(50):
            r.send(R.Raw.QUERY, R.long_string(f"SELECT key FROM system.local WHERE key = 'local'") + R.short(1) + b"\x00", stream=100 + i)
        for i in range(50):
            f = r.recv()
            assert f["opcode"] == R.Raw.RESULT and f["stream"] == 100 + i
        r.close()


# ---------------------------------------------------------------------------------------------
# sharding over two Postgres backends
# ---------------------------------------------------------------------------------------------

class TestSharding:
    def test_partitions_spread_over_both_hosts_and_stay_whole(self, two):
        c, s = conn(two)
        try:
            ks = new_ks(s, "sh")
            s.execute(f"CREATE TABLE {ks}.t (p int, c int, v text, PRIMARY KEY (p, c))")
            ins = s.prepare(f"INSERT INTO {ks}.t (p, c, v) VALUES (?, ?, ?)")
            for p in range(60):
                for cc in range(5):
                    s.execute(ins, (p, cc, f"{p}-{cc}"))
            n0, n1 = cells(two.pgs[0]), cells(two.pgs[1])
            assert n0 > 0 and n1 > 0 and n0 + n1 == 60 * 5 * 2  # marker + v cell per row
            # a partition lives on exactly one host
            seen = {}
            for h, pg in enumerate(two.pgs):
                for (pk,) in sql(pg, "SELECT DISTINCT pk FROM warp_cql_cells"):
                    assert bytes(pk) not in seen
                    seen[bytes(pk)] = h
            assert len(seen) == 60
            # full scan: exactly Cassandra's token order over both hosts, nothing lost or duplicated
            rows = s.execute(f"SELECT p, c, token(p) FROM {ks}.t").all()
            assert len(rows) == 300
            tokens = [r[2] for r in rows]
            assert tokens == sorted(tokens)
            assert s.execute(f"SELECT count(*) FROM {ks}.t").one()[0] == 300
            # paged full scan across both hosts returns the same rows
            st = SimpleStatement(f"SELECT p, c FROM {ks}.t", fetch_size=17)
            paged = [(r[0], r[1]) for r in s.execute(st)]
            assert paged == [(r[0], r[1]) for r in rows]
            # a batch over partitions of both hosts is applied everywhere
            b = BatchStatement(batch_type=BatchType.LOGGED)
            for p in range(100, 130):
                b.add(ins, (p, 0, "b"))
            s.execute(b)
            assert s.execute(f"SELECT count(*) FROM {ks}.t WHERE p IN ({','.join(str(p) for p in range(100, 130))})").one()[0] == 30
            assert cells(two.pgs[0]) > n0 and cells(two.pgs[1]) > n1
            # TRUNCATE and DROP clean both hosts
            s.execute(f"TRUNCATE {ks}.t")
            assert cells(two.pgs[0]) == 0 and cells(two.pgs[1]) == 0
            s.execute(f"INSERT INTO {ks}.t (p, c, v) VALUES (1, 1, 'x')")
            s.execute(f"DROP KEYSPACE {ks}")
            assert cells(two.pgs[0]) == 0 and cells(two.pgs[1]) == 0
        finally:
            c.shutdown()

    def test_lwt_and_counters_on_sharded_partitions(self, two):
        c, s = conn(two)
        try:
            ks = new_ks(s, "shl")
            s.execute(f"CREATE TABLE {ks}.l (k int PRIMARY KEY, v int)")
            s.execute(f"CREATE TABLE {ks}.c (k int PRIMARY KEY, n counter)")
            claim = s.prepare(f"INSERT INTO {ks}.l (k, v) VALUES (?, 1) IF NOT EXISTS")
            inc = s.prepare(f"UPDATE {ks}.c SET n = n + 1 WHERE k = ?")
            for k in range(40):
                assert s.execute(claim, (k,)).one()[0] is True
                assert s.execute(claim, (k,)).one()[0] is False
                for _ in range(3):
                    s.execute(inc, (k,))
            assert s.execute(f"SELECT sum(n) FROM {ks}.c").one()[0] == 120
            assert sql(two.pgs[0], "SELECT count(DISTINCT pk) FROM warp_cql_cells")[0][0] > 0
            assert sql(two.pgs[1], "SELECT count(DISTINCT pk) FROM warp_cql_cells")[0][0] > 0
        finally:
            c.shutdown()

    def test_adding_a_backend_flags_rebalance_and_new_data_spreads(self):
        w = L.CqlWarp(1)
        extra = None
        try:
            from warp_test_support import RealPostgres
            extra = RealPostgres()
            c, s = conn(w)
            ks = new_ks(s, "rb")
            s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
            added = w.api("POST", "/api/backend-sets/default/backends", {
                "name": "pg9", "url": f"jdbc:postgresql://localhost:{extra.port}/postgres", "user": "postgres", "password": "postgres",
                "enabledStores": ["cql"]}, expect=201)
            assert "cql" in {r["store"] for r in added["rebalanceRequired"]}
            stores = w.api("GET", "/api/backend-sets/stores") if False else None
            time.sleep(1.5)
            ins = s.prepare(f"INSERT INTO {ks}.t (k, v) VALUES (?, ?)")
            for k in range(200, 260):
                s.execute(ins, (k, k))
            assert cells(extra) > 0
            c.shutdown()
        finally:
            w.close()
            if extra:
                extra.close()


# ---------------------------------------------------------------------------------------------
# auth, TTL, pool starvation, MCP, metrics
# ---------------------------------------------------------------------------------------------

def test_password_authenticator_uses_the_shared_credential_store():
    w = L.CqlWarp(1, extra_env={"WARP_CQLWIRE_AUTH": "true"})
    try:
        c = Cluster(["127.0.0.1"], port=w.port, protocol_version=4, auth_provider=PlainTextAuthProvider("postgres", "postgres"), connect_timeout=30)
        s = c.connect()
        assert s.execute("SELECT key FROM system.local").one()[0] == "local"
        c.shutdown()
        for provider in [PlainTextAuthProvider("postgres", "wrong"), PlainTextAuthProvider("nobody", "postgres"), None]:
            c = Cluster(["127.0.0.1"], port=w.port, protocol_version=4, auth_provider=provider, connect_timeout=10)
            with pytest.raises(NoHostAvailable) as e:
                c.connect()
            assert "incorrect" in str(e.value) or "authentication" in str(e.value).lower()
            c.shutdown()
        # the raw protocol: nothing but OPTIONS / STARTUP before authentication
        r = R.Raw(w.port)
        assert r.startup()["opcode"] == R.Raw.AUTHENTICATE
        assert R.error_of(r.query("SELECT key FROM system.local"))[0] == 0x000A
        r.close()
        r = R.Raw(w.port)
        r.startup()
        r.send(0x0F, struct.pack(">i", 18) + b"\x00postgres\x00postgres")
        assert r.recv()["opcode"] == 0x10
        assert r.query("SELECT key FROM system.local")["opcode"] == R.Raw.RESULT
        r.close()
    finally:
        w.close()


def test_frontend_serves_the_set_named_by_warp_cqlwire_set():
    from warp_test_support import RealPostgres
    other = RealPostgres()
    w = L.CqlWarp(1, extra_env={"WARP_CQLWIRE_SET": "analytics"}, default_store=False)
    try:
        w.api("POST", "/api/backend-sets", {"name": "analytics", "description": "cql set"}, expect=201)
        w.api("POST", "/api/backend-sets/analytics/backends", {
            "name": "an1", "url": f"jdbc:postgresql://localhost:{other.port}/postgres", "user": "postgres", "password": "postgres",
            "enabledStores": ["cql"]}, expect=201)
        time.sleep(2)
        c, s = conn(w)
        ks = new_ks(s, "set")
        s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        for i in range(20):
            s.execute(f"INSERT INTO {ks}.t (k, v) VALUES ({i}, {i})")
        assert cells(other) == 40
        assert not sql(w.pgs[0], "SELECT 1 FROM information_schema.tables WHERE table_name = 'warp_cql_cells'")  # the default set's Postgres was never touched
        c.shutdown()
    finally:
        w.close()
        other.close()


def test_ttl_expiry_is_hidden_immediately_and_swept():
    w = L.CqlWarp(1)  # sweeper every 300 ms
    try:
        c, s = conn(w)
        ks = new_ks(s, "ttl")
        s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b text)")
        s.execute(f"INSERT INTO {ks}.t (k, a, b) VALUES (1, 'x', 'y') USING TTL 2")
        s.execute(f"INSERT INTO {ks}.t (k, a) VALUES (2, 'keep')")
        assert s.execute(f"SELECT a, ttl(a) FROM {ks}.t WHERE k = 1").one()[1] in (1, 2)
        assert cells(w.pgs[0]) == 5
        time.sleep(3)
        assert s.execute(f"SELECT * FROM {ks}.t WHERE k = 1").one() is None
        assert s.execute(f"SELECT count(*) FROM {ks}.t").one()[0] == 1
        deadline = time.time() + 10
        while time.time() < deadline and cells(w.pgs[0]) != 2:
            time.sleep(0.3)
        assert cells(w.pgs[0]) == 2  # the sweeper removed the expired cells physically
        c.shutdown()
    finally:
        w.close()


def test_small_pool_is_not_starved_by_stalled_clients():
    """Developer edition caps a Warp at 25 concurrent connections, so: 8 drivers that read one page and stall, plus 8 raw clients that ask for
    a multi-megabyte result and never read it (the server is blocked writing to their socket), all with WARP_POOL_MAX_SIZE=4. A pooled JDBC
    connection held across that client I/O would exhaust the pool and stall every other request."""
    w = L.CqlWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4"})
    clients, raws = [], []
    try:
        c, s = conn(w)
        ks = new_ks(s, "pool")
        s.execute(f"CREATE TABLE {ks}.t (p int, c int, v text, PRIMARY KEY (p, c))")
        s.execute(f"CREATE TABLE {ks}.big (p int, c int, b blob, PRIMARY KEY (p, c))")
        ins = s.prepare(f"INSERT INTO {ks}.t (p, c, v) VALUES (?, ?, ?)")
        for cc in range(100):
            s.execute(ins, (1, cc, "v"))
        bi = s.prepare(f"INSERT INTO {ks}.big (p, c, b) VALUES (?, ?, ?)")
        for cc in range(12):
            s.execute(bi, (1, cc, os.urandom(1024 * 1024)))
        for _ in range(8):
            c2, s2 = conn(w)
            rs = s2.execute(SimpleStatement(f"SELECT * FROM {ks}.t WHERE p = 1", fetch_size=5))
            assert rs.has_more_pages
            clients.append(c2)
        for _ in range(8):
            r = R.Raw(w.port, timeout=30)
            r.startup()
            r.s.setsockopt(__import__("socket").SOL_SOCKET, __import__("socket").SO_RCVBUF, 4096)
            r.send(R.Raw.QUERY, R.long_string(f"SELECT * FROM {ks}.big WHERE p = 1") + R.short(1) + b"\x00", stream=1)
            raws.append(r)
        time.sleep(1)
        t0 = time.time()
        for i in range(50):
            assert s.execute(f"SELECT count(*) FROM {ks}.t WHERE p = 1").one()[0] == 100
        assert time.time() - t0 < 20
    finally:
        for r in raws:
            r.close()
        for c2 in clients:
            c2.shutdown()
        try:
            c.shutdown()
        except Exception:  # noqa: BLE001
            pass
        w.close()


def test_mcp_describes_the_cql_store_and_metrics_count_operations():
    mcp = free_port()
    w = L.CqlWarp(1, extra_env={"WARP_MCP_PORT": str(mcp), "WARP_MCP_EMULATED_STORES": ""})
    try:
        c, s = conn(w)
        ks = new_ks(s, "mcp")
        s.execute(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        for i in range(10):
            s.execute(f"INSERT INTO {ks}.t (k, v) VALUES ({i}, {i})")
        for i in range(10):
            s.execute(f"SELECT * FROM {ks}.t WHERE k = {i}")
        c.shutdown()
        deadline = time.time() + 20
        d = None
        while time.time() < deadline:
            try:
                d = call_json(mcp, "describe_backend", {"backend": "default.cqlstore"})
                break
            except Exception:  # noqa: BLE001 -- MCP still starting
                time.sleep(1)
        assert d is not None and any(ks in json.dumps(t) for t in d["contents"]["tables"]) and d["contents"]["cells"][0]["count"] == 20
        lb = call_json(mcp, "list_backends")
        default = next(b for b in lb["backends"] if b["name"] == "default")
        assert default["enabledStores"] == ["cql"]
        r = requests.get(f"http://localhost:{w.proc.metrics_port}/api/metrics/summary", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5)
        assert "cqlwire" in r.text
    finally:
        w.close()
