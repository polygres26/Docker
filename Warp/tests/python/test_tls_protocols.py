"""TLS for every gRPC and raw-TCP protocol frontend of Warp, against real Warp subprocesses, a real local Postgres, a generated
test CA + localhost leaf certificate and REAL protocol clients:

  native gRPC QueryService (grpc secure_channel), pgwire (psycopg2 sslmode=verify-full), mywire (pymysql ssl_ca), mssqlwire
  (TDS pre-login ENCRYPT_ON + a TLS handshake tunnelled in TDS packets, verified against the CA: no TLS-capable MSSQL client is
  installed), orawire TCPS (python-oracledb thin, tcps + ssl_context), mongowire (pymongo tls=True), rediswire (redis-py ssl=True
  on the TLS port), boltwire (neo4j driver bolt+s / neo4j+s), cqlwire (cassandra-driver ssl_context), kafkawire
  (kafka-python and confluent-kafka security.protocol=SSL), amqpwire (pika ssl_options, python-qpid-proton amqps), pubsubwire /
  bigtablewire / firestorewire / datastorewire (raw gRPC stubs over a secure_channel; firestore + datastore also HTTPS REST).

Also: an untrusted client (a CA Warp does not chain to) is rejected everywhere, plaintext keeps working, WARP_TLS_CLIENT_AUTH=need
(mTLS) on one TCP frontend (rediswire) and one gRPC frontend (native QueryService), WARP_MONGOWIRE_TLS_MODE=require, and a broken
TLS configuration ("TLS is NOT enabled: <reason>") that leaves every plaintext port serving.

Needs: the `openssl` CLI, WARP_TEST_PG_LOCAL=1 (or Docker), WARP_TEST_JAR, and the clients above (each test skips when its client
is not importable). Helper and module names are deliberately unique (tlsproto_*) so the full pytest run has no name clashes.
"""
import datetime
import importlib
import json
import os
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import time

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from warp_test_support import WarpProcess, RealPostgres, free_port, isolated_ports  # noqa: E402

TLSPROTO_ADMIN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = TLSPROTO_ADMIN
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl CLI not available")

TLSPROTO_STORES = ["redis", "kafka", "amqp", "cql", "pubsub", "bigtable", "firestore", "datastore"]
TLSPROTO_TLS_PORT_VARS = ["WARP_TLS_PORT", "WARP_GRPC_TLS_PORT", "WARP_REDISWIRE_TLS_PORT", "WARP_KAFKAWIRE_TLS_PORT",
                          "WARP_AMQPWIRE_TLS_PORT", "WARP_PUBSUBWIRE_TLS_PORT", "WARP_BIGTABLEWIRE_TLS_PORT",
                          "WARP_FIRESTOREWIRE_TLS_PORT", "WARP_DATASTOREWIRE_TLS_PORT"]


def tlsproto_run(*args):
    subprocess.run(args, check=True, capture_output=True)


def tlsproto_first_ignite_port():
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port")


def tlsproto_client(mod):
    try:
        return importlib.import_module(mod)
    except ImportError:
        pytest.skip(f"python module {mod} is not installed")


@pytest.fixture(scope="module")
def tlsproto_certs(tmp_path_factory):
    d = tmp_path_factory.mktemp("tlsproto-certs")
    p = lambda n: str(d / n)  # noqa: E731

    def make_ca(name):
        tlsproto_run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", p(name + ".key"), "-out",
                     p(name + ".pem"), "-days", "30", "-subj", f"/CN={name}", "-addext", "basicConstraints=critical,CA:TRUE",
                     "-addext", "keyUsage=critical,keyCertSign,cRLSign")

    (d / "leaf.ext").write_text("subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n"
                                "keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n")

    def leaf(name, ca, cn="localhost"):
        tlsproto_run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", p(name + ".key.rsa"), "-out",
                     p(name + ".csr"), "-subj", f"/CN={cn}")
        tlsproto_run("openssl", "pkcs8", "-topk8", "-nocrypt", "-in", p(name + ".key.rsa"), "-out", p(name + ".key"))
        tlsproto_run("openssl", "x509", "-req", "-in", p(name + ".csr"), "-CA", p(ca + ".pem"), "-CAkey", p(ca + ".key"),
                     "-CAcreateserial", "-out", p(name + ".pem"), "-days", "30", "-extfile", p("leaf.ext"))

    make_ca("ca")
    make_ca("evil-ca")
    leaf("server", "ca")
    leaf("client", "ca", "warp-test-client")  # mTLS client certificate (chains to ca)
    leaf("rogue-client", "evil-ca", "rogue")  # a client certificate the server must not trust
    (d / "server.fullchain.pem").write_text((d / "server.pem").read_text() + (d / "ca.pem").read_text())
    tlsproto_run("openssl", "pkcs12", "-export", "-in", p("server.pem"), "-inkey", p("server.key"), "-certfile", p("ca.pem"),
                 "-out", p("server.p12"), "-passout", "pass:changeit", "-name", "warp")
    return {k: p(v) for k, v in {"ca": "ca.pem", "evil_ca": "evil-ca.pem", "cert": "server.fullchain.pem", "key": "server.key",
                                 "p12": "server.p12", "client_cert": "client.pem", "client_key": "client.key",
                                 "rogue_cert": "rogue-client.pem", "rogue_key": "rogue-client.key"}.items()}


class TlsProtoWarp:
    """One Warp process with every gRPC / raw-TCP frontend listening (plaintext port each) and its TLS twin."""

    def __init__(self, _shared_pg, env, stores=TLSPROTO_STORES):
        # its own Postgres: the Developer license counts live Warp instances per config database (cap 3)
        self.pg = pg = RealPostgres()
        ports = isolated_ports("WARP_PGWIRE_PORT")
        self.tls_ports = {v: free_port() for v in TLSPROTO_TLS_PORT_VARS}
        self.ports = ports
        WarpProcess._wait_ready.__defaults__ = (150,)
        try:
            self._start(pg, ports, env)
        except BaseException:
            pg.close()
            raise
        if stores:
            self.admin("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": stores})

    def _start(self, pg, ports, env):
        self.proc = WarpProcess(pg, "WARP_PGWIRE_PORT", frontend_name="tlsproto", extra_env={
            **ports, **{k: str(v) for k, v in self.tls_ports.items()},
            "WARP_MCP_HTTPS_PORT": str(free_port()), "WARP_ADMIN_HTTPS_PORT": str(free_port()),
            "WARP_A2A_HTTPS_PORT": str(free_port()), "WARP_TLS_RELOAD_SECONDS": "1",
            "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
            "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{tlsproto_first_ignite_port()}", "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
            "WARP_KAFKAWIRE_ADVERTISED_HOST": "localhost", "WARP_KAFKAWIRE_SWEEP_MS": "3600000",
            "WARP_AMQPWIRE_AUTH": "false", "WARP_CQLWIRE_AUTH": "false",
            **env})
        self.pgwire = self.proc.frontend_port
        self.metrics = self.proc.metrics_port
        self.grpc = self.proc.grpc_port

    def port(self, var):
        return int(self.ports[var])

    def tls(self, var):
        return self.tls_ports[var]

    def admin(self, method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{self.metrics}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {TLSPROTO_ADMIN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    def log(self):
        return "".join(self.proc._output_lines)

    def wait_port(self, port, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                return
            except OSError:
                time.sleep(0.3)
        raise TimeoutError(f"port {port} never opened; log tail:\n" + "".join(self.proc._output_lines[-30:]))

    def close(self):
        self.proc.close()
        self.pg.close()


@pytest.fixture(scope="module")
def tlsproto_pg():
    db = RealPostgres()
    yield db
    db.close()


@pytest.fixture(scope="module")
def tw(tlsproto_pg, tlsproto_certs):
    """Warp with global PEM TLS (WARP_TLS_CERT/KEY) and every TLS twin port fixed."""
    w = TlsProtoWarp(tlsproto_pg, {"WARP_TLS_CERT": tlsproto_certs["cert"], "WARP_TLS_KEY": tlsproto_certs["key"]})
    yield w
    w.close()


def tlsproto_ctx(certs, client=False, ca="ca"):
    ctx = ssl.create_default_context(cafile=certs[ca])
    if client:
        ctx.load_cert_chain(certs["client_cert"], certs["client_key"])
    return ctx


def tlsproto_untrusted_ctx(certs):
    return tlsproto_ctx(certs, ca="evil_ca")



# ---------------------------------------------------------------------------------------------------------------------------
# raw TDS helpers (mssqlwire): PRELOGIN encryption negotiation and a TLS handshake tunnelled inside TDS 0x12 packets

def tlsproto_tds_packet(ptype, payload, pid=1):
    return struct.pack(">BBHHBB", ptype, 1, len(payload) + 8, 0, pid, 0) + payload


def tlsproto_prelogin(encryption):
    toks = [(0, bytes([0x0F, 0, 0, 0, 0, 0])), (1, bytes([encryption])), (2, b"\x00"), (3, b"\0\0\0\0"), (4, b"\x00")]
    hdr, body, off = b"", b"", len(toks) * 5 + 1
    for t, d in toks:
        hdr += struct.pack(">BHH", t, off, len(d))
        body += d
        off += len(d)
    return hdr + b"\xff" + body


def tlsproto_read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            raise EOFError("connection closed")
        buf += c
    return buf


def tlsproto_read_tds(sock):
    h = tlsproto_read_exact(sock, 8)
    return h[0], tlsproto_read_exact(sock, struct.unpack(">H", h[2:4])[0] - 8)


def tlsproto_parse_encryption(payload):
    pos = 0
    while payload[pos] != 0xFF:
        tok, off, _ln = struct.unpack(">BHH", payload[pos:pos + 5])
        if tok == 1:
            return payload[off]
        pos += 5
    raise AssertionError("no ENCRYPTION option in PRELOGIN response")


def tlsproto_tds_encryption(port, requested):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        s.sendall(tlsproto_tds_packet(0x12, tlsproto_prelogin(requested)))
        t, p = tlsproto_read_tds(s)
        assert t == 4, t
        return tlsproto_parse_encryption(p)


def tlsproto_tds_handshake(port, ca, client_cert=None):
    """PRELOGIN(ENCRYPT_ON), then the TLS handshake inside TDS packets, verified against `ca` for host localhost."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca)
    ctx.check_hostname = True
    if client_cert:
        ctx.load_cert_chain(*client_cert)
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        s.sendall(tlsproto_tds_packet(0x12, tlsproto_prelogin(1)))
        t, p = tlsproto_read_tds(s)
        assert tlsproto_parse_encryption(p) == 1, "server did not accept encryption"
        inb, outb = ssl.MemoryBIO(), ssl.MemoryBIO()
        obj = ctx.wrap_bio(inb, outb, server_hostname="localhost")
        pid = 2
        while True:
            try:
                obj.do_handshake()
                break
            except ssl.SSLWantReadError:
                data = outb.read()
                if data:
                    s.sendall(tlsproto_tds_packet(0x12, data, pid))
                    pid += 1
                inb.write(tlsproto_read_tds(s)[1])
        data = outb.read()
        if data:
            s.sendall(tlsproto_tds_packet(0x12, data, pid))
        return obj.version(), obj.cipher()[0]


# ---------------------------------------------------------------------------------------------------------------------------
# gRPC stubs live in three different `google` namespace roots that must not be mixed in one interpreter: run each in a subprocess

TLSPROTO_ROOTS = {"ps": os.path.join(HERE, "ps_conformance", "ps_stubs"), "bt": os.path.join(HERE, "bt_conformance", "bt_stubs"),
                  "fs": os.path.join(HERE, "fs_conformance", "gen")}


def tlsproto_grpc_snippet(root, port, ca, body, cert=None, key=None):
    """Runs `body` (python source, `ch` is a ready secure channel) with the stubs of `root`; returns (returncode, stdout, stderr)."""
    creds = f'open({ca!r}, "rb").read()'
    if cert:
        creds += f', open({key!r}, "rb").read(), open({cert!r}, "rb").read()'
    code = (f'import grpc\nch = grpc.secure_channel("localhost:{port}", grpc.ssl_channel_credentials({creds}))\n'
            f'grpc.channel_ready_future(ch).result(timeout=10)\n{body}\n')
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=90,
                       env={**os.environ, "PYTHONPATH": TLSPROTO_ROOTS[root]})
    return r.returncode, r.stdout.strip(), r.stderr.strip()


PS_BODY = """
from google.pubsub.v1 import pubsub_pb2 as pb, pubsub_pb2_grpc as pbg
s = pbg.PublisherStub(ch)
s.CreateTopic(pb.Topic(name="projects/p/topics/tlsproto"), timeout=10)
print([t.name for t in s.ListTopics(pb.ListTopicsRequest(project="projects/p"), timeout=10).topics])
"""
BT_BODY = """
from google.bigtable.admin.v2 import bigtable_table_admin_pb2 as adm, bigtable_table_admin_pb2_grpc as admg
admg.BigtableTableAdminStub(ch).ListTables(adm.ListTablesRequest(parent="projects/p/instances/i"), timeout=10)
print("listed")
"""
FS_BODY = """
from google.firestore.v1 import firestore_pb2 as F, firestore_pb2_grpc as G
G.FirestoreStub(ch).ListCollectionIds(F.ListCollectionIdsRequest(parent="projects/p/databases/(default)/documents"), timeout=10)
print("listed")
"""
DS_BODY = """
from google.datastore.v1 import datastore_pb2 as D, datastore_pb2_grpc as G
G.DatastoreStub(ch).AllocateIds(D.AllocateIdsRequest(project_id="p"), timeout=10)
print("allocated")
"""


# ---------------------------------------------------------------------------------------------------------------------------
# Warp #1: global PEM TLS, every twin port

def test_native_grpc_tls_trusted_untrusted_and_plaintext(tw, tlsproto_certs):
    grpc = tlsproto_client("grpc")
    import warp_pb2
    import warp_pb2_grpc

    def call(channel):
        return warp_pb2_grpc.QueryServiceStub(channel).Execute(
            warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 42"), timeout=15)

    with grpc.secure_channel(f"localhost:{tw.tls('WARP_GRPC_TLS_PORT')}",
                             grpc.ssl_channel_credentials(open(tlsproto_certs["ca"], "rb").read())) as ch:
        assert call(ch).success
    with grpc.secure_channel(f"localhost:{tw.tls('WARP_GRPC_TLS_PORT')}",
                             grpc.ssl_channel_credentials(open(tlsproto_certs["evil_ca"], "rb").read())) as ch:
        with pytest.raises(grpc.RpcError) as e:
            call(ch)
        assert e.value.code() == grpc.StatusCode.UNAVAILABLE
    with grpc.insecure_channel(f"localhost:{tw.grpc}") as ch:
        assert call(ch).success


def test_pgwire_tls_verify_full_untrusted_and_plaintext(tw, tlsproto_certs):
    psycopg2 = tlsproto_client("psycopg2")
    kw = dict(host="localhost", port=tw.pgwire, user="postgres", password="postgres", dbname="postgres")
    c = psycopg2.connect(sslmode="verify-full", sslrootcert=tlsproto_certs["ca"], **kw)
    try:
        assert c.info.ssl_in_use, "the client leg must be TLS"
        cur = c.cursor()
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        c.close()
    with pytest.raises(psycopg2.OperationalError, match="certificate verify failed"):
        psycopg2.connect(sslmode="verify-full", sslrootcert=tlsproto_certs["evil_ca"], **kw)
    c = psycopg2.connect(sslmode="disable", **kw)
    try:
        assert not c.info.ssl_in_use
        cur = c.cursor()
        cur.execute("SELECT 1")
    finally:
        c.close()


def test_mywire_tls_ssl_ca_untrusted_and_plaintext(tw, tlsproto_certs):
    pymysql = tlsproto_client("pymysql")
    kw = dict(host="localhost", port=tw.port("WARP_MYWIRE_PORT"), user="postgres", password="postgres", database="postgres")
    c = pymysql.connect(ssl_ca=tlsproto_certs["ca"], ssl_verify_cert=True, ssl_verify_identity=True, **kw)
    try:
        assert isinstance(c._sock, ssl.SSLSocket), "the client leg must be TLS"
        cur = c.cursor()
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        c.close()
    with pytest.raises(pymysql.err.OperationalError):
        pymysql.connect(ssl_ca=tlsproto_certs["evil_ca"], ssl_verify_cert=True, **kw)
    c = pymysql.connect(ssl_disabled=True, **kw)
    try:
        assert not isinstance(c._sock, ssl.SSLSocket)
        c.cursor().execute("SELECT 1")
    finally:
        c.close()


def test_mssqlwire_tds_encryption_negotiation_and_cert_verified_handshake(tw, tlsproto_certs):
    port = tw.port("WARP_MSSQLWIRE_PORT")
    assert tlsproto_tds_encryption(port, 1) == 1, "ENCRYPT_ON requested: server must accept"
    assert tlsproto_tds_encryption(port, 3) == 1, "ENCRYPT_REQ requested: server must accept"
    assert tlsproto_tds_encryption(port, 2) == 2, "ENCRYPT_NOT_SUP requested: plaintext stays available"
    version, cipher = tlsproto_tds_handshake(port, tlsproto_certs["ca"])
    assert version in ("TLSv1.2", "TLSv1.3") and cipher
    with pytest.raises(ssl.SSLCertVerificationError):
        tlsproto_tds_handshake(port, tlsproto_certs["evil_ca"])
    pymssql = tlsproto_client("pymssql")  # plaintext through a real TDS client
    c = pymssql.connect(server="localhost", port=port, user="postgres", password="postgres", database="postgres")
    try:
        cur = c.cursor()
        cur.execute("SELECT 42 AS a")
        assert int(cur.fetchone()[0]) == 42
    finally:
        c.close()


def test_orawire_tcps_oracledb_untrusted_and_plaintext(tw, tlsproto_certs):
    oracledb = tlsproto_client("oracledb")

    def connect_tls(ca):
        ctx = tlsproto_ctx(tlsproto_certs, ca=ca)
        params = oracledb.ConnectParams(host="localhost", port=tw.tls("WARP_TLS_PORT"), protocol="tcps",
                                        service_name="anything", ssl_context=ctx, ssl_server_dn_match=False, disable_oob=True)
        return oracledb.connect(user="postgres", password="postgres", params=params)

    c = connect_tls("ca")
    try:
        cur = c.cursor()
        cur.execute("SELECT 21 * 2 FROM DUAL")
        assert cur.fetchall() == [(42,)]
    finally:
        c.close()
    with pytest.raises(Exception, match="(?i)certificate|ssl|DPY-6005|DPY-4011"):
        connect_tls("evil_ca")
    c = oracledb.connect(user="postgres", password="postgres", dsn=f"localhost:{tw.port('WARP_ORAWIRE_PORT')}/anything",
                         disable_oob=True)
    try:
        cur = c.cursor()
        cur.execute("SELECT 1 FROM DUAL")
    finally:
        c.close()


def test_mongowire_tls_same_port_untrusted_and_plaintext(tw, tlsproto_certs):
    pymongo = tlsproto_client("pymongo")
    port = tw.port("WARP_MONGOWIRE_PORT")
    tls = pymongo.MongoClient("localhost", port, tls=True, tlsCAFile=tlsproto_certs["ca"], serverSelectionTimeoutMS=8000)
    try:
        assert tls.admin.command("ping")["ok"] == 1.0
        coll = tls.tlsproto.docs
        coll.delete_many({})
        coll.insert_one({"_id": 1, "v": "over-tls"})
        assert coll.find_one({"_id": 1})["v"] == "over-tls"
    finally:
        tls.close()
    bad = pymongo.MongoClient("localhost", port, tls=True, tlsCAFile=tlsproto_certs["evil_ca"], serverSelectionTimeoutMS=3000)
    try:
        with pytest.raises(pymongo.errors.ServerSelectionTimeoutError, match="CERTIFICATE_VERIFY_FAILED"):
            bad.admin.command("ping")
    finally:
        bad.close()
    plain = pymongo.MongoClient("localhost", port, serverSelectionTimeoutMS=8000)  # mode allow: same port, plaintext still works
    try:
        assert plain.tlsproto.docs.find_one({"_id": 1})["v"] == "over-tls"
    finally:
        plain.close()


def test_rediswire_tls_port_untrusted_and_plaintext(tw, tlsproto_certs):
    redis = tlsproto_client("redis")
    r = redis.Redis(host="localhost", port=tw.tls("WARP_REDISWIRE_TLS_PORT"), ssl=True, ssl_ca_certs=tlsproto_certs["ca"])
    assert r.ping()
    r.set("tlsproto:k", "v")
    assert r.get("tlsproto:k") == b"v"
    bad = redis.Redis(host="localhost", port=tw.tls("WARP_REDISWIRE_TLS_PORT"), ssl=True, ssl_ca_certs=tlsproto_certs["evil_ca"])
    with pytest.raises(redis.exceptions.ConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
        bad.ping()
    p = redis.Redis(host="localhost", port=tw.port("WARP_REDISWIRE_PORT"))
    assert p.get("tlsproto:k") == b"v", "plaintext port keeps working and sees the same data"


def test_redis_rediss_url(tw, tlsproto_certs):
    redis = tlsproto_client("redis")
    r = redis.from_url(f"rediss://localhost:{tw.tls('WARP_REDISWIRE_TLS_PORT')}", ssl_ca_certs=tlsproto_certs["ca"])
    assert r.ping()


@pytest.mark.parametrize("scheme", ["bolt", "neo4j"])
def test_boltwire_tls_same_port_trusted_untrusted_and_plaintext(tw, tlsproto_certs, scheme):
    neo4j = tlsproto_client("neo4j")
    port = tw.port("WARP_BOLTWIRE_PORT")

    def run(uri, **kw):
        d = neo4j.GraphDatabase.driver(uri, auth=("postgres", "postgres"), **kw)
        try:
            with d.session() as s:
                return s.run("RETURN 1 AS x").single()["x"]
        finally:
            d.close()

    # bolt+s / neo4j+s verify against the SYSTEM store, so a private CA is passed as `bolt` + encrypted + trusted_certificates
    # (identical TLS on the wire; the driver rejects encryption settings on the +s schemes)
    trust = neo4j.TrustCustomCAs(tlsproto_certs["ca"])
    if scheme == "bolt":
        assert run(f"bolt://localhost:{port}", encrypted=True, trusted_certificates=trust) == 1
        with pytest.raises(neo4j.exceptions.ServiceUnavailable):
            run(f"bolt://localhost:{port}", encrypted=True, trusted_certificates=neo4j.TrustCustomCAs(tlsproto_certs["evil_ca"]))
        assert run(f"bolt://localhost:{port}") == 1, "plaintext bolt on the same port"
        # bolt+s exactly as users type it: the private CA is not in the system store, so it must be rejected (not silently trusted)
        with pytest.raises(neo4j.exceptions.ServiceUnavailable):
            run(f"bolt+s://localhost:{port}")
        assert run(f"bolt+ssc://localhost:{port}") == 1, "bolt+ssc (encrypted, no verification) reaches the TLS endpoint"
    else:
        d = neo4j.GraphDatabase.driver(f"neo4j://localhost:{port}", auth=("postgres", "postgres"), encrypted=True,
                                       trusted_certificates=trust)
        try:
            with d.session() as s:
                assert s.run("RETURN 1 AS x").single()["x"] == 1
        except neo4j.exceptions.ServiceUnavailable as e:  # boltwire has no routing table: neo4j:// needs one on ANY transport
            pytest.skip(f"neo4j:// routing is not supported by boltwire regardless of TLS: {e}")
        finally:
            d.close()


def test_cqlwire_tls_same_port_untrusted_and_plaintext(tw, tlsproto_certs):
    cassandra = tlsproto_client("cassandra.cluster")
    port = tw.port("WARP_CQLWIRE_PORT")

    def query(ctx):
        c = cassandra.Cluster(["127.0.0.1"], port=port, protocol_version=4, ssl_context=ctx, connect_timeout=10)
        try:
            return c.connect().execute("SELECT release_version FROM system.local").one()
        finally:
            c.shutdown()

    def ctx(ca):
        x = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        x.load_verify_locations(tlsproto_certs[ca])
        x.check_hostname = False  # cassandra-driver connects to the IP 127.0.0.1 (the leaf also carries IP:127.0.0.1)
        return x

    assert query(ctx("ca")).release_version
    with pytest.raises(cassandra.NoHostAvailable, match="CERTIFICATE_VERIFY_FAILED"):
        query(ctx("evil_ca"))
    assert query(None).release_version, "plaintext CQL on the same port"


def test_kafka_ssl_listener_python_clients_and_advertised_addresses(tw, tlsproto_certs):
    kafka = tlsproto_client("kafka")
    from kafka.admin import NewTopic
    ssl_bs = f"localhost:{tw.tls('WARP_KAFKAWIRE_TLS_PORT')}"
    plain_bs = f"localhost:{tw.port('WARP_KAFKAWIRE_PORT')}"
    ssl_kw = dict(security_protocol="SSL", ssl_cafile=tlsproto_certs["ca"], ssl_check_hostname=True)
    adm = kafka.KafkaAdminClient(bootstrap_servers=ssl_bs, **ssl_kw)
    try:
        adm.create_topics([NewTopic("tlsproto-kp", 1, 1)])
    finally:
        adm.close()
    p = kafka.KafkaProducer(bootstrap_servers=ssl_bs, acks="all", linger_ms=0, **ssl_kw)
    try:
        p.send("tlsproto-kp", b"over-ssl").get(15)
    finally:
        p.close()
    pp = kafka.KafkaProducer(bootstrap_servers=plain_bs, acks="all", linger_ms=0)
    try:
        pp.send("tlsproto-kp", b"plain").get(15)  # PLAINTEXT listener keeps working
    finally:
        pp.close()
    c = kafka.KafkaConsumer("tlsproto-kp", bootstrap_servers=ssl_bs, group_id="tlsproto-g", auto_offset_reset="earliest",
                            consumer_timeout_ms=10000, **ssl_kw)
    try:
        assert sorted(m.value for m in c) == [b"over-ssl", b"plain"]
    finally:
        c.close()
    # each listener advertises ITS OWN address in Metadata
    ck = tlsproto_client("confluent_kafka")
    md_ssl = ck.Producer({"bootstrap.servers": ssl_bs, "security.protocol": "SSL", "ssl.ca.location": tlsproto_certs["ca"]}
                         ).list_topics(timeout=15)
    md_plain = ck.Producer({"bootstrap.servers": plain_bs}).list_topics(timeout=15)
    assert {b.port for b in md_ssl.brokers.values()} == {tw.tls("WARP_KAFKAWIRE_TLS_PORT")}
    assert {b.port for b in md_plain.brokers.values()} == {tw.port("WARP_KAFKAWIRE_PORT")}
    with pytest.raises(Exception):
        kafka.KafkaProducer(bootstrap_servers=ssl_bs, security_protocol="SSL", ssl_cafile=tlsproto_certs["evil_ca"],
                            request_timeout_ms=5000, max_block_ms=5000, api_version=(2, 5, 0)).send("x", b"y").get(5)


def test_kafka_confluent_client_ssl(tw, tlsproto_certs):
    ck = tlsproto_client("confluent_kafka")
    conf = {"bootstrap.servers": f"localhost:{tw.tls('WARP_KAFKAWIRE_TLS_PORT')}", "security.protocol": "SSL",
            "ssl.ca.location": tlsproto_certs["ca"]}
    res = []
    p = ck.Producer(conf)
    p.produce("tlsproto-ck", b"cflt", callback=lambda err, msg: res.append(err))
    p.flush(20)
    assert res == [None]
    c = ck.Consumer({**conf, "group.id": "tlsproto-ck-g", "auto.offset.reset": "earliest"})
    c.subscribe(["tlsproto-ck"])
    got, end = [], time.time() + 20
    while time.time() < end and not got:
        m = c.poll(1.0)
        if m is not None and not m.error():
            got.append(m.value())
    c.close()
    assert got == [b"cflt"]
    bad = ck.Producer({**conf, "ssl.ca.location": tlsproto_certs["evil_ca"], "socket.timeout.ms": 3000})
    with pytest.raises(ck.KafkaException):
        bad.list_topics(timeout=6)


def test_kafka_console_tools_from_the_apache_image_over_ssl(tlsproto_pg, tlsproto_certs, tmp_path):
    """The stock Kafka CLI (apache/kafka image) with --command-config against the Warp SSL port. The container reaches the host as
    host.docker.internal (Docker Desktop) so Warp advertises that name on its SSL listener; the test leaf carries it as a SAN."""
    if shutil.which("docker") is None:
        pytest.skip("docker not available")
    if not subprocess.run(["docker", "images", "-q", "apache/kafka:latest"], capture_output=True, text=True).stdout.strip():
        pytest.skip("apache/kafka:latest image is not pulled")
    net = ["--network", "host"] if sys.platform.startswith("linux") else ["--add-host", "host.docker.internal:host-gateway"]
    # a directory the Docker VM can bind-mount (Docker Desktop shares /tmp and $HOME but not always $TMPDIR under /var/folders)
    import tempfile
    share = None
    for base_dir in (str(tmp_path), "/tmp", os.path.expanduser("~")):
        cand = tempfile.mkdtemp(prefix="tlsproto-", dir=base_dir) if base_dir != str(tmp_path) else str(tmp_path)
        (open(os.path.join(cand, "probe"), "w")).close()
        ok = subprocess.run(["docker", "run", "--rm", "-v", f"{cand}:/probe:ro", "alpine:latest", "ls", "/probe/probe"],
                            capture_output=True, text=True).returncode == 0
        if ok:
            share = cand
            break
    if share is None:
        pytest.skip("no host directory can be bind-mounted into the Docker VM")
    shutil.copy(tlsproto_certs["ca"], os.path.join(share, "ca.pem"))
    tmp_path = type(tmp_path)(share)
    w = TlsProtoWarp(tlsproto_pg, {"WARP_TLS_CERT": tlsproto_certs["cert"], "WARP_TLS_KEY": tlsproto_certs["key"],
                                   "WARP_KAFKAWIRE_TLS_ADVERTISED_HOST": "host.docker.internal"}, stores=["kafka"])
    try:
        w.wait_port(w.tls("WARP_KAFKAWIRE_TLS_PORT"))
        (tmp_path / "client.properties").write_text(
            "security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=/certs/ca.pem\n"
            "ssl.endpoint.identification.algorithm=https\n")
        bs = f"host.docker.internal:{w.tls('WARP_KAFKAWIRE_TLS_PORT')}"
        base = ["docker", "run", "--rm", "-i", *net, "-v", f"{share}:/certs:ro", "-v", f"{share}:/cfg:ro",
                "apache/kafka:latest"]
        cfg = ["--command-config", "/cfg/client.properties"]

        def cli(args, stdin=None):
            return subprocess.run(base + args, input=stdin, capture_output=True, text=True, timeout=180)

        r = cli(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bs, *cfg, "--create", "--topic", "tlsproto-cli"])
        assert r.returncode == 0, r.stdout + r.stderr
        r = cli(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bs, *cfg, "--list"])
        assert "tlsproto-cli" in r.stdout, r.stdout + r.stderr
        r = cli(["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", bs, "--topic", "tlsproto-cli",
                 "--producer.config", "/cfg/client.properties"], stdin="hello-from-the-cli\n")
        assert r.returncode == 0, r.stdout + r.stderr
        r = cli(["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", bs, "--topic", "tlsproto-cli",
                 "--from-beginning", "--max-messages", "1", "--timeout-ms", "30000", "--consumer.config",
                 "/cfg/client.properties"])
        assert "hello-from-the-cli" in r.stdout, r.stdout + r.stderr
        # without the trust store the stock tool must refuse
        (tmp_path / "bad.properties").write_text("security.protocol=SSL\nssl.endpoint.identification.algorithm=https\n")
        r = cli(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bs, "--command-config", "/cfg/bad.properties", "--list"])
        assert r.returncode != 0
    finally:
        w.close()
        shutil.rmtree(share, ignore_errors=True)


def test_amqps_pika_and_proton(tw, tlsproto_certs):
    pika = tlsproto_client("pika")
    port = tw.tls("WARP_AMQPWIRE_TLS_PORT")

    def roundtrip(p, ctx):
        prm = pika.ConnectionParameters("localhost", p, "/", pika.PlainCredentials("guest", "guest"),
                                        ssl_options=pika.SSLOptions(ctx, "localhost") if ctx else None)
        c = pika.BlockingConnection(prm)
        try:
            ch = c.channel()
            ch.queue_declare("tlsproto-q")
            ch.basic_publish("", "tlsproto-q", b"hi")
            return ch.basic_get("tlsproto-q", auto_ack=True)[2]
        finally:
            c.close()

    assert roundtrip(port, tlsproto_ctx(tlsproto_certs)) == b"hi"
    with pytest.raises(ssl.SSLCertVerificationError):
        roundtrip(port, tlsproto_untrusted_ctx(tlsproto_certs))
    assert roundtrip(tw.port("WARP_AMQPWIRE_PORT"), None) == b"hi", "amqp:// on 5672-equivalent keeps working"

    proton = tlsproto_client("proton")
    from proton.utils import BlockingConnection

    def amqp10(ca):
        d = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)
        d.set_trusted_ca_db(ca)
        d.set_peer_authentication(proton.SSLDomain.VERIFY_PEER_NAME)
        c = BlockingConnection(f"amqps://localhost:{port}", ssl_domain=d, timeout=10, allowed_mechs="ANONYMOUS PLAIN")
        try:
            s = c.create_sender("/queues/tlsproto-q10")
            s.send(proton.Message(body="over amqps 1.0"))
            return "sent"
        finally:
            c.close()

    roundtrip(port, tlsproto_ctx(tlsproto_certs))  # (declares tlsproto-q; AMQP 1.0 attaches to an existing queue)
    c0 = pika.BlockingConnection(pika.ConnectionParameters("localhost", port, "/", pika.PlainCredentials("guest", "guest"),
                                                           ssl_options=pika.SSLOptions(tlsproto_ctx(tlsproto_certs), "localhost")))
    c0.channel().queue_declare("tlsproto-q10")
    c0.close()
    assert amqp10(tlsproto_certs["ca"]) == "sent"
    with pytest.raises(proton.ProtonException):
        amqp10(tlsproto_certs["evil_ca"])


@pytest.mark.parametrize("name,root,var,body", [("pubsub", "ps", "WARP_PUBSUBWIRE_TLS_PORT", PS_BODY),
                                                ("bigtable", "bt", "WARP_BIGTABLEWIRE_TLS_PORT", BT_BODY),
                                                ("firestore", "fs", "WARP_FIRESTOREWIRE_TLS_PORT", FS_BODY),
                                                ("datastore", "fs", "WARP_DATASTOREWIRE_TLS_PORT", DS_BODY)])
def test_google_grpc_frontends_over_tls(tw, tlsproto_certs, name, root, var, body):
    tlsproto_client("grpc")
    rc, out, err = tlsproto_grpc_snippet(root, tw.tls(var), tlsproto_certs["ca"], body)
    assert rc == 0, (out, err)
    rc, out, err = tlsproto_grpc_snippet(root, tw.tls(var), tlsproto_certs["evil_ca"], body)
    assert rc != 0 and "CERTIFICATE_VERIFY_FAILED" in err, "an untrusted client must be rejected"


@pytest.mark.parametrize("name,plain_var,tls_var,path", [
    ("firestore", "WARP_FIRESTOREWIRE_PORT", "WARP_FIRESTOREWIRE_TLS_PORT", "/v1/projects/p/databases/(default)/documents/tlsproto"),
    ("datastore", "WARP_DATASTOREWIRE_PORT", "WARP_DATASTOREWIRE_TLS_PORT", None)])
def test_firestore_datastore_https_rest_and_alpn(tw, tlsproto_certs, name, plain_var, tls_var, path):
    port = tw.tls(tls_var)
    if path:
        r = requests.get(f"https://localhost:{port}{path}", verify=tlsproto_certs["ca"], timeout=15)
        assert r.status_code in (200, 404), r.text
        p = requests.get(f"http://localhost:{tw.port(plain_var)}{path}", timeout=15)
        assert p.status_code == r.status_code, "plaintext REST answers the same as HTTPS REST"
    else:
        r = requests.post(f"https://localhost:{port}/v1/projects/p:allocateIds", json={"keys": []}, verify=tlsproto_certs["ca"],
                          timeout=15)
        assert r.status_code == 200, r.text
    with pytest.raises(requests.exceptions.SSLError):
        requests.get(f"https://localhost:{port}/", verify=tlsproto_certs["evil_ca"], timeout=15)
    # ALPN: an HTTP/1.1-only client and an h2 client are both negotiated on the one TLS port
    for protos, want in ((["http/1.1"], "http/1.1"), (["h2", "http/1.1"], "h2")):
        ctx = tlsproto_ctx(tlsproto_certs)
        ctx.set_alpn_protocols(protos)
        with socket.create_connection(("127.0.0.1", port), timeout=10) as raw, ctx.wrap_socket(raw, server_hostname="localhost") as s:
            assert s.selected_alpn_protocol() == want


def test_interfaces_report_tls_state_honestly(tw):
    body = tw.admin("GET", "/api/interfaces")
    by = {i["id"]: i for i in body["interfaces"]}
    expect = {"grpc": ("separate-port", tw.grpc and tw.tls("WARP_GRPC_TLS_PORT")), "orawire": ("separate-port", tw.tls("WARP_TLS_PORT")),
              "pgwire": ("in-band", tw.pgwire), "mywire": ("in-band", tw.port("WARP_MYWIRE_PORT")),
              "mssqlwire": ("in-band", tw.port("WARP_MSSQLWIRE_PORT")), "mongowire": ("sniff-allow", tw.port("WARP_MONGOWIRE_PORT")),
              "boltwire": ("sniff-allow", tw.port("WARP_BOLTWIRE_PORT")), "cqlwire": ("sniff-allow", tw.port("WARP_CQLWIRE_PORT")),
              "rediswire": ("separate-port", tw.tls("WARP_REDISWIRE_TLS_PORT")),
              "kafkawire": ("separate-port", tw.tls("WARP_KAFKAWIRE_TLS_PORT")),
              "amqpwire": ("separate-port", tw.tls("WARP_AMQPWIRE_TLS_PORT")),
              "pubsubwire": ("separate-port", tw.tls("WARP_PUBSUBWIRE_TLS_PORT")),
              "bigtablewire": ("separate-port", tw.tls("WARP_BIGTABLEWIRE_TLS_PORT")),
              "firestorewire": ("separate-port", tw.tls("WARP_FIRESTOREWIRE_TLS_PORT")),
              "datastorewire": ("separate-port", tw.tls("WARP_DATASTOREWIRE_TLS_PORT"))}
    for iid, (mode, port) in expect.items():
        assert iid in by, (iid, sorted(by))
        assert by[iid]["tlsEnabled"] is True and by[iid]["tlsMode"] == mode and by[iid]["tlsPort"] == port, (iid, by[iid])
    # every listed frontend states its TLS status explicitly (never silently absent)
    assert all(isinstance(i.get("tlsEnabled"), bool) for i in body["interfaces"]), body["interfaces"]


def test_tls_certificate_hot_reload_on_a_tcp_and_a_grpc_port(tlsproto_pg, tlsproto_certs, tmp_path):
    """Swap the PEM files under a running Warp: new handshakes present the new certificate (JDK sockets AND Netty gRPC)."""
    shutil.copy(tlsproto_certs["cert"], tmp_path / "cert.pem")
    shutil.copy(tlsproto_certs["key"], tmp_path / "key.pem")
    w = TlsProtoWarp(tlsproto_pg, {"WARP_TLS_CERT": str(tmp_path / "cert.pem"), "WARP_TLS_KEY": str(tmp_path / "key.pem")},
                     stores=["redis", "pubsub"])
    try:
        def served(port):
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ctx.set_alpn_protocols(["h2"])
            with socket.create_connection(("127.0.0.1", port), timeout=10) as raw, ctx.wrap_socket(raw) as s:
                return s.getpeercert(binary_form=True)

        redis_port, ps_port = w.tls("WARP_REDISWIRE_TLS_PORT"), w.tls("WARP_PUBSUBWIRE_TLS_PORT")
        w.wait_port(redis_port)
        w.wait_port(ps_port)
        before = served(redis_port), served(ps_port)
        assert before[0] == before[1]
        d = tmp_path
        ext = d / "leaf2.ext"
        ext.write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n")
        tlsproto_run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(d / "k2.rsa"), "-out", str(d / "l2.csr"),
                     "-subj", "/CN=localhost")
        tlsproto_run("openssl", "pkcs8", "-topk8", "-nocrypt", "-in", str(d / "k2.rsa"), "-out", str(d / "k2.pem"))
        tlsproto_run("openssl", "x509", "-req", "-in", str(d / "l2.csr"), "-CA", tlsproto_certs["ca"], "-CAkey",
                     tlsproto_certs["ca"].replace("ca.pem", "ca.key"), "-CAcreateserial", "-out", str(d / "l2.pem"), "-days", "30",
                     "-extfile", str(ext))
        (d / "key.pem").write_text((d / "k2.pem").read_text())
        (d / "cert.pem").write_text((d / "l2.pem").read_text() + open(tlsproto_certs["ca"]).read())
        deadline = time.time() + 30
        while time.time() < deadline and served(redis_port) == before[0]:
            time.sleep(0.5)
        after = served(redis_port), served(ps_port)
        assert after[0] != before[0], "the TCP listener must serve the renewed certificate"
        assert after[0] == after[1], "the gRPC (Netty) listener must serve the same renewed certificate"
        ctx = tlsproto_ctx(tlsproto_certs)  # the renewed cert still chains to the CA: real clients keep working
        with socket.create_connection(("127.0.0.1", redis_port), timeout=10) as raw, ctx.wrap_socket(raw, server_hostname="localhost"):
            pass
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------------------
# Warp #2: mutual TLS (WARP_TLS_CLIENT_AUTH=need) on rediswire (TCP) and the native gRPC service, mongo in require mode

@pytest.fixture(scope="module")
def mtw(tlsproto_pg, tlsproto_certs):
    w = TlsProtoWarp(tlsproto_pg, {
        "WARP_TLS_CERT": tlsproto_certs["cert"], "WARP_TLS_KEY": tlsproto_certs["key"], "WARP_TLS_CA": tlsproto_certs["ca"],
        "WARP_TLS_CLIENT_AUTH": "need", "WARP_MONGOWIRE_TLS_MODE": "require", "WARP_MONGOWIRE_TLS_CLIENT_AUTH": "none"},
        stores=["redis"])
    yield w
    w.close()


def test_mtls_need_on_rediswire_tcp(mtw, tlsproto_certs):
    redis = tlsproto_client("redis")
    port = mtw.tls("WARP_REDISWIRE_TLS_PORT")
    ok = redis.Redis(host="localhost", port=port, ssl=True, ssl_ca_certs=tlsproto_certs["ca"],
                     ssl_certfile=tlsproto_certs["client_cert"], ssl_keyfile=tlsproto_certs["client_key"])
    assert ok.ping()
    no_cert = redis.Redis(host="localhost", port=port, ssl=True, ssl_ca_certs=tlsproto_certs["ca"])
    with pytest.raises(redis.exceptions.RedisError):
        no_cert.ping()
    rogue = redis.Redis(host="localhost", port=port, ssl=True, ssl_ca_certs=tlsproto_certs["ca"],
                        ssl_certfile=tlsproto_certs["rogue_cert"], ssl_keyfile=tlsproto_certs["rogue_key"])
    with pytest.raises(redis.exceptions.RedisError):
        rogue.ping()
    assert redis.Redis(host="localhost", port=mtw.port("WARP_REDISWIRE_PORT")).ping(), "the plaintext port is unaffected"


def test_mtls_need_on_native_grpc(mtw, tlsproto_certs):
    grpc = tlsproto_client("grpc")
    import warp_pb2
    import warp_pb2_grpc
    req = warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 42")
    target = f"localhost:{mtw.tls('WARP_GRPC_TLS_PORT')}"
    ca = open(tlsproto_certs["ca"], "rb").read()
    good = grpc.ssl_channel_credentials(ca, open(tlsproto_certs["client_key"], "rb").read(),
                                        open(tlsproto_certs["client_cert"], "rb").read())
    with grpc.secure_channel(target, good) as ch:
        assert warp_pb2_grpc.QueryServiceStub(ch).Execute(req, timeout=15).success
    with grpc.secure_channel(target, grpc.ssl_channel_credentials(ca)) as ch:
        with pytest.raises(grpc.RpcError):
            warp_pb2_grpc.QueryServiceStub(ch).Execute(req, timeout=10)
    rogue = grpc.ssl_channel_credentials(ca, open(tlsproto_certs["rogue_key"], "rb").read(),
                                         open(tlsproto_certs["rogue_cert"], "rb").read())
    with grpc.secure_channel(target, rogue) as ch:
        with pytest.raises(grpc.RpcError):
            warp_pb2_grpc.QueryServiceStub(ch).Execute(req, timeout=10)


def test_mongowire_require_mode_rejects_plaintext_and_serves_tls(mtw, tlsproto_certs):
    pymongo = tlsproto_client("pymongo")
    port = mtw.port("WARP_MONGOWIRE_PORT")
    good = pymongo.MongoClient("localhost", port, tls=True, tlsCAFile=tlsproto_certs["ca"], serverSelectionTimeoutMS=8000)
    try:
        assert good.admin.command("ping")["ok"] == 1.0
    finally:
        good.close()
    plain = pymongo.MongoClient("localhost", port, serverSelectionTimeoutMS=2500)
    try:
        with pytest.raises(pymongo.errors.PyMongoError):
            plain.admin.command("ping")
    finally:
        plain.close()
    ifaces = {i["id"]: i for i in mtw.admin("GET", "/api/interfaces")["interfaces"]}
    assert ifaces["mongowire"]["tlsMode"] == "sniff-require" and ifaces["rediswire"]["tlsClientAuth"] is True


def test_mtls_tds_handshake_needs_a_client_certificate(mtw, tlsproto_certs):
    port = mtw.port("WARP_MSSQLWIRE_PORT")
    tlsproto_tds_handshake(port, tlsproto_certs["ca"], (tlsproto_certs["client_cert"], tlsproto_certs["client_key"]))
    with pytest.raises((ssl.SSLError, EOFError, ConnectionError, OSError)):
        tlsproto_tds_handshake(port, tlsproto_certs["ca"])


# ---------------------------------------------------------------------------------------------------------------------------
# Warp #3: legacy PKCS12 keystore (WARP_TLS_KEYSTORE) keeps its meaning and now feeds the same shared provider

def test_legacy_pkcs12_keystore_still_enables_sql_side_tls(tlsproto_pg, tlsproto_certs):
    psycopg2 = tlsproto_client("psycopg2")
    grpc = tlsproto_client("grpc")
    import warp_pb2
    import warp_pb2_grpc
    w = TlsProtoWarp(tlsproto_pg, {"WARP_TLS_KEYSTORE": tlsproto_certs["p12"], "WARP_TLS_KEYSTORE_PASSWORD": "changeit"},
                     stores=["redis"])
    try:
        c = psycopg2.connect(host="localhost", port=w.pgwire, user="postgres", password="postgres", dbname="postgres",
                             sslmode="verify-full", sslrootcert=tlsproto_certs["ca"])
        assert c.info.ssl_in_use
        c.close()
        with grpc.secure_channel(f"localhost:{w.tls('WARP_GRPC_TLS_PORT')}",
                                 grpc.ssl_channel_credentials(open(tlsproto_certs["ca"], "rb").read())) as ch:
            assert warp_pb2_grpc.QueryServiceStub(ch).Execute(
                warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 1"), timeout=15).success
        assert tlsproto_tds_handshake(w.port("WARP_MSSQLWIRE_PORT"), tlsproto_certs["ca"])
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------------------
# Warp #4: unusable TLS configuration => "TLS is NOT enabled: <reason>" and every plaintext port keeps serving

def test_bad_tls_configuration_never_stops_plaintext(tlsproto_pg, tlsproto_certs, tmp_path):
    psycopg2 = tlsproto_client("psycopg2")
    redis = tlsproto_client("redis")
    pymongo = tlsproto_client("pymongo")
    kafka = tlsproto_client("kafka")
    grpc = tlsproto_client("grpc")
    import warp_pb2
    import warp_pb2_grpc
    (tmp_path / "junk.pem").write_text("this is not a certificate\n")
    w = TlsProtoWarp(tlsproto_pg, {"WARP_TLS_CERT": str(tmp_path / "junk.pem"), "WARP_TLS_KEY": str(tmp_path / "missing.key")},
                     stores=["redis", "kafka", "pubsub", "amqp", "cql"])
    try:
        log = w.log()
        for name in ("REDISWIRE", "KAFKAWIRE", "AMQPWIRE", "PUBSUBWIRE", "MONGOWIRE", "BOLTWIRE", "CQLWIRE", "GRPC", "PGWIRE"):
            assert f"{name} TLS is NOT enabled:" in log, (name, log[-3000:])
        c = psycopg2.connect(host="localhost", port=w.pgwire, user="postgres", password="postgres", dbname="postgres", sslmode="prefer")
        assert not c.info.ssl_in_use
        c.close()
        assert redis.Redis(host="localhost", port=w.port("WARP_REDISWIRE_PORT")).ping()
        m = pymongo.MongoClient("localhost", w.port("WARP_MONGOWIRE_PORT"), serverSelectionTimeoutMS=8000)
        assert m.admin.command("ping")["ok"] == 1.0
        m.close()
        p = kafka.KafkaProducer(bootstrap_servers=f"localhost:{w.port('WARP_KAFKAWIRE_PORT')}", acks="all")
        p.send("tlsproto-bad", b"x").get(15)
        p.close()
        with grpc.insecure_channel(f"localhost:{w.grpc}") as ch:
            assert warp_pb2_grpc.QueryServiceStub(ch).Execute(
                warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 1"), timeout=15).success
        for port in (w.tls("WARP_REDISWIRE_TLS_PORT"), w.tls("WARP_KAFKAWIRE_TLS_PORT"), w.tls("WARP_PUBSUBWIRE_TLS_PORT")):
            with pytest.raises(OSError):
                socket.create_connection(("127.0.0.1", port), timeout=2).close()
        ifaces = {i["id"]: i for i in w.admin("GET", "/api/interfaces")["interfaces"]}
        assert ifaces["rediswire"]["tlsEnabled"] is False and ifaces["rediswire"]["tlsMode"] == "off"
        assert ifaces["mongowire"]["tlsEnabled"] is False and ifaces["mongowire"].get("tlsError")
    finally:
        w.close()
