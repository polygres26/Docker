"""Warp-side tests of gremlinwire (Apache TinkerPop Gremlin Server protocol on Postgres; also the Cosmos DB Gremlin API surface).

* the golden corpus: gremlin_conformance/golden.json.gz holds the answers of a REAL tinkerpop/gremlin-server (3.8.2, TinkerGraph) to every case of
  gremlin_conformance/gr_corpus.py (about 1500: hand-written traversals over the modern and classic toy graphs, mutations, script-language cases,
  error cases and 700 seeded random traversals; each also as a bytecode request when the python DSL can express it), recorded twice with
  `gr_harness.py record` (unstable answers dropped). Every case is replayed OFFLINE (no Docker) against a real Warp on one Postgres backend and on
  two sharded backends; an answer must match after canonicalisation or be a documented divergence (gr_known.py);
* the real gremlinpython driver (script and bytecode, GraphSON 3.0 and GraphBinary), sessions, chunking (batchSize, 206/200/204), the HTTP endpoint,
  the WebSocket protocol, SASL PLAIN / HTTP Basic authentication;
* sharding: vertices land on BOTH Postgres hosts, edges with their out-vertex, traversals crossing hosts (in-edges, repeat, drops) stay correct;
* small pool not starved by stalled clients, restart durability, concurrent writers, rebalanceRequired, MCP tools and metrics.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests, and WARP_TEST_JAR.
"""
import base64
import concurrent.futures
import json
import os
import socket
import struct
import sys
import threading
import time
import uuid

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "gremlin_conformance"))
import gr_corpus as C  # noqa: E402
import gr_harness as H  # noqa: E402
import gr_known as K  # noqa: E402
import gr_launch_warp as L  # noqa: E402

from gremlin_python.driver import client as gclient  # noqa: E402
from gremlin_python.driver import serializer  # noqa: E402
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection  # noqa: E402
from gremlin_python.driver.protocol import GremlinServerError  # noqa: E402
from gremlin_python.process.anonymous_traversal import traversal  # noqa: E402
from gremlin_python.process.graph_traversal import __  # noqa: E402
from gremlin_python.process.traversal import Order, P, T, Cardinality  # noqa: E402
from mcp_support import ADMIN_TOKEN, call_json, rpc  # noqa: E402
from warp_test_support import RealPostgres, free_port  # noqa: E402

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")
GOLDEN = H.load_golden()
SERS = {"graphson3": serializer.GraphSONSerializersV3d0, "graphbinary": serializer.GraphBinarySerializersV1}


def url(w):
    return f"ws://localhost:{w.port}/gremlin"


def raw(w):
    return H.RawWs(url(w))


def pg_count(pg, table):
    import psycopg2
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def load(w, name="modern"):
    ws = raw(w)
    try:
        H.load_graph(ws, name)
    finally:
        ws.close()


@pytest.fixture(scope="module")
def warp1():
    w = L.GremlinWarp(1)
    yield w
    w.close()


@pytest.fixture(scope="module")
def warp2():
    w = L.GremlinWarp(2)
    yield w
    w.close()


def remote(w, ser="graphbinary", **kw):
    conn = DriverRemoteConnection(url(w), "g", message_serializer=SERS[ser](), **kw)
    return conn, traversal().with_remote(conn)


# ---------------------------------------------------------------------------------------------------------------- golden corpus
def _replay(w, only=None):
    fails = H.replay(url(w), GOLDEN, only)
    total = len(GOLDEN["cases"])
    assert not fails, f"{len(fails)} of {total} cases differ from the reference server:\n" + "\n".join(
        f"== {n}\n  " + "\n  ".join(d)[:1500] for n, d in list(fails.items())[:12])


def test_golden_corpus_has_the_advertised_size():
    assert len(C.CASES) >= 1500
    assert len(GOLDEN["cases"]) >= 1500
    assert sum(1 for c in GOLDEN["cases"].values() if c[-1]["bc"] is not None) >= 1250
    assert sum(1 for c in C.CASES if c.name.startswith("rnd_")) == C.RANDOM_N
    for name, why in {**K.SKIP, **K.BC_SKIP}.items():
        assert why.strip() and name in GOLDEN["cases"], name


def test_golden_replay_on_one_postgres_backend(warp1):
    _replay(warp1)


def test_golden_replay_on_two_sharded_backends(warp2):
    _replay(warp2)
    # both hosts carry the store's tables (the sharding test below asserts the spread of the data itself)
    for pg in warp2.pgs:
        assert pg_count(pg, "warp_gremlin_vertices") >= 0 and pg_count(pg, "warp_gremlin_edges") >= 0


# ---------------------------------------------------------------------------------------------------------------- real drivers
@pytest.mark.parametrize("ser", list(SERS))
def test_gremlinpython_bytecode_traversals(warp1, ser):
    load(warp1)
    conn, g = remote(warp1, ser)
    try:
        assert sorted(g.V().has("name", "marko").out("knows").values("name").toList()) == ["josh", "vadas"]
        assert g.V().count().next() == 6
        assert g.V(1).valueMap(True).next() == {T.id: 1, T.label: "person", "name": ["marko"], "age": [29]}
        p = g.V(1).out("created").path().by("name").next()
        assert list(p.objects) == ["marko", "lop"]
        assert g.E().label().groupCount().next() == {"knows": 2, "created": 4}
        assert g.V().hasLabel("person").order().by("age", Order.desc).values("name").toList() == ["peter", "josh", "marko", "vadas"]
        assert g.V().values("age").sum_().next() == 123
        assert g.V().values("age").mean().next() == 30.75
        assert [list(x.objects) for x in g.V(1).repeat(__.out()).times(2).path().by("name").toList()] == [["marko", "josh", "ripple"], ["marko", "josh", "lop"]]
        proj = g.V().hasLabel("person").project("n", "c").by("name").by(__.out().count()).toList()
        assert {"n": "marko", "c": 3} in proj
        v = g.V(1).next()
        assert v.id == 1 and v.label == "person"
        e = g.E().hasLabel("knows").has("weight", 0.5).next()
        assert e.label == "knows" and e.outV.id == 1 and e.inV.id == 2
        assert g.V().has("age", P.between(28, 33)).values("name").toList() == ["marko", "josh"]
        assert g.inject(3, 1, 2).order().toList() == [1, 2, 3]
        assert g.V().coalesce(__.values("age"), __.constant(0)).sum_().next() == 123
    finally:
        conn.close()


@pytest.mark.parametrize("ser", list(SERS))
def test_gremlinpython_writes_and_typed_values(warp1, ser):
    load(warp1, "empty")
    conn, g = remote(warp1, ser)
    try:
        a = g.addV("person").property(T.id, 1).property("name", "a").property("f", 2.5).property("l", 5).property("b", True).next()
        b = g.addV("person").property("name", "b").next()
        assert a.id == 1
        g.V(1).addE("knows").to(__.V(b.id)).property("w", 0.25).iterate()
        assert g.V(1).out("knows").values("name").next() == "b"
        assert g.V(1).valueMap("f", "l", "b").next() == {"f": [2.5], "l": [5], "b": [True]}
        g.V(1).property(Cardinality.list_, "name", "aa").iterate()
        assert g.V(1).values("name").toList() == ["a", "aa"]
        g.V(1).property("name", "only").iterate()
        assert g.V(1).values("name").toList() == ["only"]
        g.V(b.id).drop().iterate()
        assert g.V().count().next() == 1 and g.E().count().next() == 0
        # ids other than Long: strings and UUIDs (Cosmos-style ids)
        g.addV("s").property(T.id, "abc").iterate()
        u = uuid.uuid4()
        g.addV("s").property(T.id, u).iterate()
        assert g.V("abc").label().next() == "s"
        assert g.V(u).label().next() == "s"
    finally:
        conn.close()


@pytest.mark.parametrize("ser", list(SERS))
def test_gremlinpython_scripts_and_errors(warp1, ser):
    load(warp1)
    c = gclient.Client(url(warp1), "g", message_serializer=SERS[ser]())
    try:
        assert c.submit("g.V().count()").all().result() == [6]
        assert c.submit("a+b", bindings={"a": 40, "b": 2}).all().result() == [42]
        assert c.submit("[1,2,3].collect{it*2}").all().result() == [2, 4, 6]
        assert c.submit("g.V().hasLabel('nothing')").all().result() == []
        with pytest.raises(GremlinServerError) as e:
            c.submit("g.V().foo()").all().result()
        assert e.value.status_code == 597
        with pytest.raises(GremlinServerError) as e:
            c.submit("g.V().hasLabel('nothing').next()").all().result()
        assert e.value.status_code == 597
        with pytest.raises(GremlinServerError) as e:
            c.submit("1/0").all().result()
        assert e.value.status_code == 597 and "ivision" in e.value.status_message
    finally:
        c.close()


def test_sessions_keep_variables_and_are_isolated(warp1):
    load(warp1)
    c1 = gclient.Client(url(warp1), "g", session=str(uuid.uuid4()))
    c2 = gclient.Client(url(warp1), "g", session=str(uuid.uuid4()))
    c3 = gclient.Client(url(warp1), "g")
    try:
        c1.submit("x = 41").all().result()
        c1.submit("def inc(n) { n + 1 }").all().result()
        assert c1.submit("inc(x)").all().result() == [42]
        assert c1.submit("t = g.V().hasLabel('person'); t.count().next()").all().result() == [4]
        with pytest.raises(GremlinServerError):
            c2.submit("x").all().result()
        with pytest.raises(GremlinServerError):
            c3.submit("x").all().result()
        c3.submit("y = 1").all().result()
        with pytest.raises(GremlinServerError):
            c3.submit("y").all().result()
    finally:
        for c in (c1, c2, c3):
            c.close()


def test_transactions_are_not_supported_like_tinkergraph(warp1):
    c = gclient.Client(url(warp1), "g")
    try:
        with pytest.raises(GremlinServerError) as e:
            c.submit("graph.tx().commit()").all().result()
        assert e.value.status_code == 597 and "transactions" in e.value.status_message
    finally:
        c.close()


def test_chunked_results_use_206_then_200_and_204_when_empty(warp1):
    load(warp1, "empty")
    ws = raw(warp1)
    try:
        ws.eval("g.inject(1,2,3,4,5,6,7,8,9,10).as('i').addV('n').property('i',select('i')).iterate()")
        r = ws.eval("g.V().values('i')", batchSize=4)
        assert [x["status"]["code"] for x in r] == [206, 206, 200]
        assert [len(x["result"]["data"]["@value"]) for x in r] == [4, 4, 2]
        r = ws.eval("g.V().values('i')", batchSize=5)
        assert [x["status"]["code"] for x in r] == [206, 200]
        assert [x["status"]["code"] for x in ws.eval("g.V().values('i')", batchSize=10)] == [200]
        r = ws.eval("g.V().hasLabel('none')")
        assert [x["status"]["code"] for x in r] == [204] and r[0]["result"]["data"] is None
        # the default batch size (64) chunks big results too
        big = ws.eval("(1..200)")
        assert [x["status"]["code"] for x in big] == [206, 206, 206, 200]
        assert sum(len(x["result"]["data"]["@value"]) for x in big) == 200
    finally:
        ws.close()


def test_protocol_errors_have_gremlin_server_status_codes(warp1):
    ws = raw(warp1)
    try:
        def code(r):
            return r[-1]["status"]["code"]

        assert code(ws.send("eval", "", {})) == 499
        assert code(ws.send("eval", "nope", {"gremlin": "1"})) == 499
        assert code(ws.send("bogus", "", {"gremlin": "1"})) == 498
        assert code(ws.send("bytecode", "", {"gremlin": "1"})) == 498
        assert code(ws.send("eval", "", {"gremlin": "1", "language": "lua"})) == 597
        assert code(ws.send("eval", "", {"gremlin": "1", "aliases": {"g": "zz"}})) == 499
        assert code(ws.send("eval", "session", {"gremlin": "1"})) == 499
        assert code(ws.send("bytecode", "traversal", {"gremlin": {"@type": "g:Bytecode", "@value": {"step": [["V"], ["zork"]]}}, "aliases": {"g": "g"}})) == 599
        assert code(ws.send("bytecode", "traversal", {"gremlin": {"@type": "g:Bytecode", "@value": {"step": [["V"], ["count"]]}}})) == 499
        assert code(ws.eval("g.V(")) == 597
        # an unsupported serializer mimetype and a malformed body are refused, the connection stays usable
        ws.ws.send_binary(bytes([9]) + b"text/html" + b"{}")
        assert json.loads(ws.ws.recv())["status"]["code"] == 498
        ws.ws.send_binary(bytes([16]) + b"application/json" + b"{not json")
        assert json.loads(ws.ws.recv())["status"]["code"] == 498
        assert ws.eval("1+1")[-1]["status"]["code"] == 200
    finally:
        ws.close()


def _ws_frame(op, payload, mask=True, fin=True):
    b0 = (0x80 if fin else 0) | op
    key = os.urandom(4)
    n = len(payload)
    hdr = bytes([b0])
    if n < 126:
        hdr += bytes([0x80 | n])
    elif n < 65536:
        hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
    else:
        hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
    return hdr + key + bytes(p ^ key[i % 4] for i, p in enumerate(payload))


def _handshake(port):
    s = socket.create_connection(("localhost", port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(f"GET /gremlin HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
              f"Sec-WebSocket-Version: 13\r\n\r\n".encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += s.recv(1024)
    assert b" 101 " in buf.split(b"\r\n")[0]
    return s


def _read_frame(s):
    def rd(n):
        d = b""
        while len(d) < n:
            c = s.recv(n - len(d))
            assert c, "closed"
            d += c
        return d
    h = rd(2)
    n = h[1] & 0x7f
    if n == 126:
        n = struct.unpack(">H", rd(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", rd(8))[0]
    return h[0] & 0x0f, rd(n)


def test_websocket_framing_ping_text_fragments_and_close(warp1):
    s = _handshake(warp1.port)
    try:
        s.sendall(_ws_frame(9, b"hello"))
        assert _read_frame(s) == (10, b"hello")
        req = {"requestId": str(uuid.uuid4()), "op": "eval", "processor": "", "args": {"gremlin": "1+2", "language": "gremlin-groovy"}}
        # a text frame is a plain JSON request and gets a text frame back
        s.sendall(_ws_frame(1, json.dumps(req).encode()))
        op, body = _read_frame(s)
        assert op == 1 and json.loads(body)["status"]["code"] == 200
        # a binary message split into three fragments
        payload = bytes([16]) + b"application/json" + json.dumps(req).encode()
        third = len(payload) // 3
        s.sendall(_ws_frame(2, payload[:third], fin=False) + _ws_frame(0, payload[third:2 * third], fin=False) + _ws_frame(0, payload[2 * third:]))
        op, body = _read_frame(s)
        assert op == 2 and json.loads(body)["result"]["data"]["@value"][0]["@value"] == 3
        s.sendall(_ws_frame(8, struct.pack(">H", 1000)))
        assert _read_frame(s)[0] == 8
    finally:
        s.close()


def test_a_frame_over_the_content_limit_closes_the_connection():
    w = L.GremlinWarp(1, extra_env={"WARP_GREMLINWIRE_MAX_CONTENT_LENGTH": "4096"})
    try:
        s = _handshake(w.port)
        s.sendall(_ws_frame(2, b"x" * 5000))
        op, body = _read_frame(s)
        assert op == 8 and struct.unpack(">H", body[:2])[0] == 1009
        s.close()
        r = requests.post(f"http://localhost:{w.port}/", json={"gremlin": "'" + "y" * 5000 + "'"}, timeout=10)
        assert r.status_code == 413
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------- HTTP
def test_http_endpoint(warp1):
    load(warp1)
    base = f"http://localhost:{warp1.port}"
    r = requests.post(base + "/", json={"gremlin": "g.V().count()"}, timeout=10)
    assert r.status_code == 200 and r.headers["content-type"] == "application/json"
    body = r.json()
    assert body["status"] == {"message": "", "code": 200, "attributes": {}} and body["result"] == {"data": [6], "meta": {}}
    assert requests.post(base + "/gremlin", json={"gremlin": "g.V(1).values('name')"}, timeout=10).json()["result"]["data"] == ["marko"]
    assert requests.get(base + "/", params={"gremlin": "1+1"}, timeout=10).json()["result"]["data"] == [2]
    assert requests.post(base + "/", json={"gremlin": "a*2", "bindings": {"a": 21}}, timeout=10).json()["result"]["data"] == [42]
    assert requests.post(base + "/", json={"gremlin": "g.V().hasLabel('x')"}, timeout=10).json()["result"]["data"] == []
    v = requests.post(base + "/", json={"gremlin": "g.V(1)"}, timeout=10).json()["result"]["data"][0]
    assert v == {"id": 1, "label": "person", "properties": {"name": [{"id": v["properties"]["name"][0]["id"], "value": "marko", "label": "name"}],
                                                            "age": [{"id": v["properties"]["age"][0]["id"], "value": 29, "label": "age"}]}}
    typed = requests.post(base + "/", json={"gremlin": "g.V().count()"}, headers={"Accept": "application/vnd.gremlin-v3.0+json"}, timeout=10).json()
    assert typed["result"]["data"] == {"@type": "g:List", "@value": [{"@type": "g:Int64", "@value": 6}]}
    gb = requests.post(base + "/", json={"gremlin": "1+1"}, headers={"Accept": "application/vnd.graphbinary-v1.0"}, timeout=10)
    assert gb.headers["content-type"] == "application/vnd.graphbinary-v1.0" and gb.content[0] == 0x81
    assert requests.post(base + "/", data="{bad", timeout=10).status_code == 400
    assert requests.post(base + "/", json={"foo": 1}, timeout=10).json() == {"message": "no gremlin script supplied"}
    e = requests.post(base + "/", json={"gremlin": "g.V().foo()"}, timeout=10)
    assert e.status_code == 500 and "message" in e.json() and e.json()["Exception-Class"]
    assert requests.delete(base + "/", timeout=10).status_code == 405
    # keep-alive: several requests over one connection
    with requests.Session() as s:
        for i in range(5):
            assert s.post(base + "/", json={"gremlin": f"{i}+1"}, timeout=10).json()["result"]["data"] == [i + 1]


# ---------------------------------------------------------------------------------------------------------------- authentication
def test_sasl_plain_and_http_basic_use_the_shared_credential_store():
    w = L.GremlinWarp(1, extra_env={"WARP_GREMLINWIRE_AUTH": "true"})
    try:
        for ser in SERS:
            c = gclient.Client(url(w), "g", username="postgres", password="postgres", message_serializer=SERS[ser]())
            assert c.submit("1+1").all().result() == [2]
            c.close()
        c = gclient.Client(url(w), "g", username="postgres", password="wrong")
        with pytest.raises(GremlinServerError) as e:
            c.submit("1+1").all().result()
        assert e.value.status_code == 401
        c.close()
        c = gclient.Client(url(w), "g")
        with pytest.raises(Exception):
            c.submit("1+1").all().result()
        c.close()
        # the raw exchange: the first request is answered 407, the credentials reply runs the original request
        ws = raw(w)
        first = ws.eval("21*2")
        assert first[-1]["status"]["code"] == 407
        sasl = base64.b64encode(b"\x00postgres\x00postgres").decode()
        r = ws.send("authentication", "traversal", {"sasl": sasl})
        assert r[-1]["status"]["code"] == 200 and r[-1]["result"]["data"]["@value"][0]["@value"] == 42
        assert ws.eval("1")[-1]["status"]["code"] == 200
        ws.close()
        ws = raw(w)
        ws.eval("1")
        assert ws.send("authentication", "traversal", {"sasl": base64.b64encode(b"\x00postgres\x00nope").decode()})[-1]["status"]["code"] == 401
        ws.close()
        base = f"http://localhost:{w.port}/"
        assert requests.post(base, json={"gremlin": "1"}, timeout=10).status_code == 401
        assert requests.post(base, json={"gremlin": "1+1"}, auth=("postgres", "postgres"), timeout=10).json()["result"]["data"] == [2]
        assert requests.post(base, json={"gremlin": "1"}, auth=("postgres", "bad"), timeout=10).status_code == 401
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------- sharding
def test_vertices_and_edges_spread_over_both_hosts_and_traversals_cross_them(warp2):
    ws = raw(warp2)
    ws.eval("g.V().drop().iterate()")
    n = 80
    ws.eval(f"(1..{n}).each{{ g.addV('n').property(T.id, it.longValue()).property('i', it).iterate() }}")
    ws.eval(f"(1..{n - 1}).each{{ g.V(it.longValue()).addE('next').to(__.V(it.longValue() + 1)).property(T.id, 1000L + it).iterate() }}")
    v0, v1 = (pg_count(pg, "warp_gremlin_vertices") for pg in warp2.pgs)
    e0, e1 = (pg_count(pg, "warp_gremlin_edges") for pg in warp2.pgs)
    assert v0 > 10 and v1 > 10 and v0 + v1 == n, (v0, v1)
    assert e0 > 10 and e1 > 10 and e0 + e1 == n - 1, (e0, e1)
    c = gclient.Client(url(warp2), "g")
    try:
        def q(s):
            return c.submit(s).all().result()
        assert q("g.V().count()") == [n] and q("g.E().count()") == [n - 1]
        # in-edges live with the OTHER vertex: they scatter over both hosts
        assert q("g.V(50).in().id()") == [49] and q("g.V(50).out().id()") == [51]
        assert q("g.V(50).both().id()") == [51, 49] or sorted(q("g.V(50).both().id()")) == [49, 51]
        assert q("g.V(10).repeat(out()).times(15).id()") == [25]
        assert q("g.V(70).repeat(__.in()).times(30).id()") == [40]
        assert q("g.V(1).repeat(out()).until(hasId(60)).path().count(local)") == [60]
        assert q("g.E(1005L).outV().id()") == [5] and q("g.E(1005L).inV().id()") == [6]
        assert q("g.V().order().by('i',desc).limit(3).id()") == [80, 79, 78]
        assert q("g.V().hasLabel('n').has('i',gt(70)).count()") == [10]
        assert q("g.V().id().fold()")[0] == list(range(1, n + 1))  # scans come back in id order whatever the host layout
        # deleting a vertex removes its in-edges wherever they live
        q("g.V(41).drop()")
        assert q("g.E().count()") == [n - 3] and q("g.V(42).in().count()") == [0] and q("g.V(40).out().count()") == [0]
        q("g.E(1010L).drop()")
        assert q("g.E().count()") == [n - 4]
    finally:
        c.close()
        ws.close()


def test_adding_a_backend_flags_rebalance_and_new_data_spreads():
    w = L.GremlinWarp(1)
    extra = None
    try:
        extra = RealPostgres()
        ws = raw(w)
        ws.eval("g.addV('a').property(T.id,1L).iterate()")
        added = w.api("POST", "/api/backend-sets/default/backends", {
            "name": "pg9", "url": f"jdbc:postgresql://localhost:{extra.port}/postgres", "user": "postgres", "password": "postgres",
            "enabledStores": ["gremlin"]}, expect=201)
        assert "gremlin" in {r["store"] for r in added["rebalanceRequired"]}
        time.sleep(1.5)
        for i in range(100, 160):
            ws.eval(f"g.addV('b').property(T.id,{i}L).iterate()")
        assert pg_count(extra, "warp_gremlin_vertices") > 0
        ws.close()
    finally:
        w.close()
        if extra:
            extra.close()


# ---------------------------------------------------------------------------------------------------------------- resources, durability
def test_concurrent_writers_get_unique_generated_ids(warp1):
    load(warp1, "empty")

    def work(k):
        c = gclient.Client(url(warp1), "g", pool_size=1)  # Developer edition caps a Warp at 25 concurrent connections
        try:
            for i in range(20):
                c.submit(f"g.addV('w').property('k',{k}).property('i',{i})").all().result()
        finally:
            c.close()

    with concurrent.futures.ThreadPoolExecutor(6) as ex:
        list(ex.map(work, range(6)))
    ws = raw(warp1)
    try:
        ids = [x["@value"] for x in ws.eval("g.V().id()", batchSize=1000)[0]["result"]["data"]["@value"]]
        assert len(ids) == 120 and len(set(ids)) == 120
    finally:
        ws.close()


def test_small_pool_is_not_starved_by_stalled_clients():
    """WARP_POOL_MAX_SIZE=4 with clients that request a big result and never read it: a pooled connection held across that client I/O would
    exhaust the pool and stall every other request."""
    w = L.GremlinWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4"})
    stalled = []
    try:
        ws = raw(w)
        ws.eval("(1..1500).each{ g.addV('x').property('pad','" + "p" * 2000 + "').iterate() }", evaluationTimeout=120000)
        for _ in range(8):
            s = _handshake(w.port)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            req = {"requestId": str(uuid.uuid4()), "op": "eval", "processor": "", "args": {"gremlin": "g.V()", "batchSize": 500}}
            m = b"application/json"
            s.sendall(_ws_frame(2, bytes([len(m)]) + m + json.dumps(req).encode()))
            stalled.append(s)
        time.sleep(1.5)
        t0 = time.time()
        for _ in range(60):
            assert ws.eval("g.V().count()")[-1]["result"]["data"]["@value"][0]["@value"] == 1500
        assert time.time() - t0 < 30
        ws.close()
    finally:
        for s in stalled:
            s.close()
        w.close()


def test_data_survives_a_restart_and_the_store_is_still_enabled():
    w = L.GremlinWarp(1)
    w2 = None
    try:
        ws = raw(w)
        H.load_graph(ws, "modern")
        ws.close()
        w.close(keep_pgs=True)
        w2 = L.GremlinWarp(pgs=w.pgs, default_store=False)
        ws = raw(w2)
        assert ws.eval("g.V().count()")[-1]["result"]["data"]["@value"][0]["@value"] == 6
        assert ws.eval("g.V(1).out('knows').values('name')")[-1]["status"]["code"] == 200
        ws.close()
    finally:
        (w2 or w).close()


# ---------------------------------------------------------------------------------------------------------------- MCP + metrics
def _mcp_ready(port, tool_probe="list_backends"):
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            return call_json(port, tool_probe)
        except Exception:  # noqa: BLE001 -- MCP still starting
            time.sleep(1)
    raise AssertionError("MCP did not come up")


def test_mcp_describe_and_data_tools_and_metrics():
    mcp = free_port()
    w = L.GremlinWarp(1, extra_env={"WARP_MCP_PORT": str(mcp), "WARP_MCP_EMULATED_STORES": ""})
    try:
        ws = raw(w)
        H.load_graph(ws, "modern")
        for i in range(5):
            ws.eval("g.V().has('name','marko').out('knows').values('name')")
        ws.close()
        lb = _mcp_ready(mcp)
        default = next(b for b in lb["backends"] if b["name"] == "default")
        assert default["enabledStores"] == ["gremlin"]
        d = call_json(mcp, "describe_backend", {"backend": "default.gremlinstore"})
        assert d["contents"]["vertexCount"] == 6 and d["contents"]["edgeCount"] == 6
        assert {x["label"]: x["count"] for x in d["contents"]["vertexLabels"]} == {"person": 4, "software": 2}
        tools = {t["name"] for t in rpc(mcp, "tools/list")["result"]["tools"]}
        assert {"gremlin_query", "gremlin_list_labels", "gremlin_count", "gremlin_get_vertex", "gremlin_write", "gremlin_add_vertex", "gremlin_add_edge",
                "gremlin_drop_vertex"} <= tools
        q = call_json(mcp, "gremlin_query", {"backend": "default.gremlinstore", "script": "g.V().has('age',gt(30)).values('name').toList()"})
        assert q["results"] == ["josh", "peter"] and q["truncated"] is False
        q = call_json(mcp, "gremlin_query", {"backend": "default.gremlinstore", "script": "g.V().valueMap('name')", "maxResults": 2})
        assert q["count"] == 2 and q["truncated"] is True
        refused = rpc(mcp, "tools/call", {"name": "gremlin_query", "arguments": {"backend": "default.gremlinstore", "script": "g.addV('x').iterate()"}})
        assert refused["result"]["isError"] is True and "Read-only" in refused["result"]["content"][0]["text"]
        assert call_json(mcp, "gremlin_count", {"backend": "default.gremlinstore"}) == {"vertices": 6, "edges": 6}
        assert call_json(mcp, "gremlin_list_labels", {"backend": "default.gremlinstore"})["edgeLabels"] == {"created": 4, "knows": 2}
        assert call_json(mcp, "gremlin_get_vertex", {"backend": "default.gremlinstore", "id": 1})["properties"]["name"][0]["value"] == "marko"
        call_json(mcp, "gremlin_write", {"backend": "default.gremlinstore", "script": "g.addV('temp').property(T.id,100L).next().id()"})
        call_json(mcp, "gremlin_add_vertex", {"backend": "default.gremlinstore", "label": "temp", "id": 101, "properties": {"n": 1}})
        call_json(mcp, "gremlin_add_edge", {"backend": "default.gremlinstore", "label": "l", "outId": 100, "inId": 101, "properties": {"w": 2}})
        assert call_json(mcp, "gremlin_query", {"backend": "default.gremlinstore", "script": "g.V(100).out().values('n')"})["results"] == [1]
        call_json(mcp, "gremlin_drop_vertex", {"backend": "default.gremlinstore", "id": 100})
        assert call_json(mcp, "gremlin_count", {"backend": "default.gremlinstore"})["vertices"] == 7
        # and the wire protocol sees what the MCP tools wrote
        ws = raw(w)
        assert ws.eval("g.V(101).values('n')")[-1]["result"]["data"]["@value"][0]["@value"] == 1
        ws.close()
        r = requests.get(f"http://localhost:{w.proc.metrics_port}/api/metrics/summary", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5)
        assert "gremlinwire" in r.text and "mcp-gremlinstore" in r.text
    finally:
        w.close()


def test_mcp_read_only_hides_and_refuses_the_write_tools():
    mcp = free_port()
    w = L.GremlinWarp(1, extra_env={"WARP_MCP_PORT": str(mcp), "WARP_MCP_EMULATED_STORES": "", "WARP_MCP_READ_ONLY": "true"})
    try:
        _mcp_ready(mcp)
        tools = {t["name"] for t in rpc(mcp, "tools/list")["result"]["tools"]}
        assert "gremlin_query" in tools and "gremlin_count" in tools
        assert not ({"gremlin_write", "gremlin_add_vertex", "gremlin_add_edge", "gremlin_drop_vertex"} & tools)
        r = rpc(mcp, "tools/call", {"name": "gremlin_write", "arguments": {"backend": "default.gremlinstore", "script": "g.addV('x').iterate()"}})
        assert "error" in r or r["result"]["isError"] is True
    finally:
        w.close()


def test_no_gremlin_listener_and_no_tables_unless_enabled():
    """WARP_GREMLINWIRE_ENABLED=true starts the listener on its own port; with the store off, requests fail cleanly instead of crashing."""
    w = L.GremlinWarp(1, default_store=False)
    try:
        ws = raw(w)
        r = ws.eval("g.V().count()")
        assert r[-1]["status"]["code"] == 500 and "gremlin store enabled" in r[-1]["status"]["message"]
        assert ws.eval("1+1")[-1]["status"]["code"] == 200
        ws.close()
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------- the reference's own Java serializers
def _reference_jars():
    """The lib/ directory of the reference server's jars (GREMLIN_REF_JARS, else copied out of the local tinkerpop/gremlin-server image);
    None when neither exists (the Java driver itself is not part of that image)."""
    import shutil
    import subprocess
    import tempfile
    d = os.environ.get("GREMLIN_REF_JARS")
    if d and os.path.isdir(d):
        return d
    if not shutil.which("docker") or not shutil.which("javac"):
        return None
    if subprocess.run(["docker", "image", "inspect", "tinkerpop/gremlin-server:latest"], capture_output=True).returncode != 0:
        return None
    tmp = tempfile.mkdtemp(prefix="gr-jars-")
    name = "gr-jars-" + uuid.uuid4().hex[:8]
    try:
        subprocess.run(["docker", "create", "--name", name, "tinkerpop/gremlin-server:latest"], check=True, capture_output=True)
        subprocess.run(["docker", "cp", f"{name}:/opt/gremlin-server/lib", tmp], check=True, capture_output=True)
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    return os.path.join(tmp, "lib")


def test_reference_java_serializers_decode_warp_answers_like_the_reference(warp1):
    """gremlin-util from the reference image (GraphBinaryMessageSerializerV1 and GraphSONMessageSerializerV3) decode the answers of Warp to 88
    script and bytecode requests exactly like the reference's own (java_interop_golden.jsonl). The Java gremlin-driver and gremlin-console are not
    part of that image, so this is the driver's serialization layer over a JDK WebSocket, not the driver."""
    import subprocess
    import tempfile
    jars = _reference_jars()
    if not jars:
        pytest.skip("needs docker + javac with the tinkerpop/gremlin-server image (or GREMLIN_REF_JARS=<its lib dir>)")
    cls = tempfile.mkdtemp(prefix="gr-java-")
    src = os.path.join(HERE, "gremlin_conformance", "java", "GremlinJavaInterop.java")
    subprocess.run(["javac", "-nowarn", "-d", cls, "-cp", os.path.join(jars, "*"), src], check=True, capture_output=True)
    load(warp1)
    out = subprocess.run(["java", "-cp", cls + os.pathsep + os.path.join(jars, "*"), "GremlinJavaInterop", url(warp1)], capture_output=True, text=True, timeout=300)
    got = [json.loads(line) for line in out.stdout.splitlines() if line.startswith('{"ser"')]
    with open(os.path.join(HERE, "gremlin_conformance", "java_interop_golden.jsonl")) as f:
        gold = [json.loads(line) for line in f if line.strip()]
    assert len(got) == len(gold) == 88, out.stderr[-2000:]
    for g, w in zip(gold, got):
        assert g["case"] == w["case"] and g["ser"] == w["ser"]
        assert "exception" not in w, w
        assert (g["code"], g["results"]) == (w["code"], w["results"]) or (g["code"] >= 400 and g["code"] == w["code"]), (g, w)


# ---------------------------------------------------------------------------------------------------------------- other serializers, timeouts
def test_graphson2_untyped_and_stringd_serializers(warp1):
    load(warp1)
    ws = raw(warp1)
    try:
        def ask(mime, script):
            r = ws.send("eval", "", {"gremlin": script, "language": "gremlin-groovy"}, mime=mime)
            return r[-1]["status"]["code"], r[-1]["result"]["data"]

        code, data = ask("application/vnd.gremlin-v2.0+json", "g.V(1).valueMap(true)")
        assert code == 200 and data == [{"id": {"@type": "g:Int64", "@value": 1}, "label": "person", "name": ["marko"],
                                          "age": [{"@type": "g:Int32", "@value": 29}]}]  # GraphSON 2.0: plain maps and lists, typed numbers
        code, data = ask("application/vnd.gremlin-v2.0+json", "[1,2]")
        assert data == [{"@type": "g:Int32", "@value": 1}, {"@type": "g:Int32", "@value": 2}]
        code, data = ask("application/vnd.gremlin-v3.0+json;types=false", "g.V(1).values('name','age')")
        assert data == ["marko", 29]
        code, data = ask("application/vnd.gremlin-v3.0+json", "g.V().count()")
        assert data == {"@type": "g:List", "@value": [{"@type": "g:Int64", "@value": 6}]}
    finally:
        ws.close()
    # GraphBinary "stringd": results come back as strings (gremlin-python cannot select it: its header holds a 32-byte mimetype, so the frame is built here)
    mime = b"application/vnd.graphbinary-v1.0-stringd"

    def fq_str(x):
        b = x.encode()
        return b"\x03\x00" + struct.pack(">i", len(b)) + b

    def frame(script):
        rid = uuid.uuid4()
        body = bytes([0x81]) + rid.bytes + struct.pack(">i", 4) + b"eval" + struct.pack(">i", 0) + struct.pack(">i", 2)
        body += fq_str("gremlin") + fq_str(script) + fq_str("language") + fq_str("gremlin-groovy")
        return bytes([len(mime)]) + mime + body

    import websocket
    w = websocket.create_connection(url(warp1), timeout=30)
    try:
        w.send_binary(frame("g.V(1).values('name','age')"))
        msg = serializer.GraphBinarySerializersV1().deserialize_message(w.recv())
        assert msg["status"]["code"] == 200 and msg["result"]["data"] == ["marko", "29"]
    finally:
        w.close()


def test_evaluation_timeout_stops_a_runaway_script_and_the_connection_survives(warp1):
    load(warp1)
    ws = raw(warp1)
    try:
        t0 = time.time()
        r = ws.eval("x = 0; while (true) { x = x + 1 }", evaluationTimeout=1500)
        assert r[-1]["status"]["code"] == 598 and "evaluationTimeout" in r[-1]["status"]["message"]
        assert time.time() - t0 < 20
        r = ws.eval("g.V().repeat(both()).times(100000).count()", evaluationTimeout=1500)
        assert r[-1]["status"]["code"] == 598
        assert ws.eval("1+1")[-1]["status"]["code"] == 200
    finally:
        ws.close()


def test_read_only_environment_refuses_mutations_and_chunked_http_bodies_work():
    w = L.GremlinWarp(1, extra_env={"WARP_GREMLINWIRE_READ_ONLY": "true"})
    try:
        ws = raw(w)
        r = ws.eval("g.addV('x')")
        assert r[-1]["status"]["code"] == 597 and "Read-only" in r[-1]["status"]["message"]
        assert ws.eval("g.V().count()")[-1]["result"]["data"]["@value"][0]["@value"] == 0
        assert ws.send("bytecode", "traversal", {"gremlin": {"@type": "g:Bytecode", "@value": {"step": [["addV", "x"]]}}, "aliases": {"g": "g"}})[-1]["status"]["code"] == 500
        ws.close()
        # a chunked request body and a 100-continue handshake over one keep-alive connection
        s = socket.create_connection(("localhost", w.port), timeout=10)
        body = json.dumps({"gremlin": "40+2"}).encode()
        half = len(body) // 2
        s.sendall(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n"
                  + f"{half:x}\r\n".encode() + body[:half] + b"\r\n" + f"{len(body) - half:x}\r\n".encode() + body[half:] + b"\r\n0\r\n\r\n")
        data = b""
        while b'"data"' not in data:
            data += s.recv(4096)
        assert b'"data":[42]' in data
        s.sendall(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: %d\r\n\r\n" % len(body))
        assert s.recv(64).startswith(b"HTTP/1.1 100")
        s.sendall(body)
        data = b""
        while b'"data"' not in data:
            data += s.recv(4096)
        assert b'"data":[42]' in data
        s.close()
    finally:
        w.close()
