"""Connection-pool saturation experiment (opt-in): N concurrent CLIENT connections against a Warp whose
backend pool (Warp -> Postgres) is limited to WARP_POOL_MAX_SIZE=10. For each protocol/driver it records
whether a client WAITS or gets an ERROR when all 10 server-side connections are in use, how long it
waited and what the error was, while a separate monitor samples pg_stat_activity and Warp's own
/metrics pool gauges (warp_pool_connections / warp_pool_waiting) as server-side ground truth.

The saturation experiment is skipped unless WARP_RUN_POOL_TESTS=1 so the normal suite is not slowed (the
multiplexing tests at the bottom always run). Run directly for the full experiment (raise the fd limit first: `ulimit -n 8192`):

    WARP_RUN_POOL_TESTS=1 python3 test_connection_pooling.py --protocols pg,my --n 25 --mode hold \
        --variant default --out /path/results.json

Modes
  hold  every client connects, runs the workload, then KEEPS its connection open until every client has
        finished the workload (models long-lived application connections).
  quick every client closes its connection as soon as its own workload finishes (models
        connect-run-close clients).

No mocks: real Postgres container, real Warp jar, real client drivers.
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # noqa: E402

# The saturation EXPERIMENT (main() / test_pool_saturation_smoke) is opt-in; the connection-multiplexing tests
# further down (test_idle_clients_*, test_25_concurrent_*, correctness, security, exhaustion) always run.
opt_in_experiment = pytest.mark.skipif(os.environ.get("WARP_RUN_POOL_TESTS") != "1",
                                       reason="opt-in pool saturation experiment: set WARP_RUN_POOL_TESTS=1")

ADMIN_TOKEN = "warp-test-admin-token"
SLEEP_S = 0.1

# frontend env var per protocol name
PORT_VARS = {
    "pg": "WARP_PGWIRE_PORT", "my": "WARP_MYWIRE_PORT", "mssql": "WARP_MSSQLWIRE_PORT",
    "ora": "WARP_ORAWIRE_PORT", "mongo": "WARP_MONGOWIRE_PORT", "dynamo": "WARP_DYNAMOWIRE_PORT",
    "sqs": "WARP_SQSWIRE_PORT", "os": "WARP_OSWIRE_PORT", "influx": "WARP_INFLUXWIRE_PORT",
    "bolt": "WARP_BOLTWIRE_PORT",
}


# ---------------------------------------------------------------------------------------------
# Stack: Postgres + one Warp with pool max 10 and permissive QoS
# ---------------------------------------------------------------------------------------------
def first_free_discovery_port():
    import socket
    for port in range(47500, 47600):
        for fam, addr in ((socket.AF_INET6, "::"), (socket.AF_INET, "0.0.0.0")):
            with socket.socket(fam, socket.SOCK_STREAM) as sk:
                try:
                    sk.bind((addr, port))
                except OSError:
                    break
        else:
            return port
    raise RuntimeError("no free Ignite discovery port")


class Stack:
    def __init__(self, extra_env=None, protocols=None, pg_setup=None):
        WarpProcess._wait_ready.__defaults__ = (120,)  # Ignite + 10 frontends start slower than one
        self.pg = RealPostgres()
        if pg_setup is not None:
            pg_setup(self.pg)  # e.g. create roles/tables before Warp starts
        env = {"WARP_POOL_MAX_SIZE": "10", "WARP_ADMIN_TOKEN": ADMIN_TOKEN,
               "WARP_QOS_RATE_PER_SEC": "100000", "WARP_QOS_BURST": "100000"}
        protocols = protocols or list(PORT_VARS)
        ports = isolated_ports()
        # every frontend gets an isolated port (an unset one would fall back to a default port that the
        # dev Warp on this machine may hold)
        self.ports = {p: int(ports[v]) for p, v in PORT_VARS.items()}
        for p, port in self.ports.items():
            env[PORT_VARS[p]] = str(port)
        # a2a / mcp / s3 are not needed here; pushing them to unused ports keeps them from colliding
        env["WARP_A2A_PORT"] = ports["WARP_A2A_PORT"]
        env["WARP_MCP_PORT"] = ports["WARP_MCP_PORT"]
        env["WARP_S3WIRE_PORT"] = ports["WARP_S3WIRE_PORT"]
        # Ignite discovery: stray orphaned Warp JVMs on this box hold 47500-47507 and make a fresh Warp hang
        # in joinTopology. Enable static discovery seeded with ONLY the first free port in 47500..47599
        # (the node binds the first free port, so it finds itself and becomes its own coordinator).
        env.update({"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
                    "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_discovery_port()}",
                    "WARP_OTEL_ENDPOINT": "disabled"})
        if extra_env:
            env.update({k: str(v) for k, v in extra_env.items()})
        first = "pg" if "pg" in self.ports else next(iter(self.ports))
        var = PORT_VARS[first]
        env.pop(var)  # WarpProcess assigns this one itself
        try:
            self.warp = WarpProcess(self.pg, var, frontend_name=first, extra_env=env)
        except BaseException:
            self.pg.close()  # never leak the Postgres container when Warp fails to start
            raise
        self.ports[first] = self.warp.frontend_port
        self.grpc_port = self.warp.grpc_port
        self.metrics_port = self.warp.metrics_port

        import atexit, signal
        atexit.register(self.close)
        signal.signal(signal.SIGTERM, lambda *_: (self.close(), os._exit(143)))

    def close(self):
        if getattr(self, "_closed", False):
            return
        self._closed = True
        try:
            self.warp.close()
        finally:
            self.pg.close()

    def warp_log(self):
        return "".join(self.warp._output_lines)


def pg_direct(stack):
    import psycopg2
    c = psycopg2.connect(host="localhost", port=stack.pg.port, user="postgres", password="postgres",
                         dbname="postgres")
    c.autocommit = True
    return c


def add_sleep_triggers(stack, seconds=SLEEP_S):
    """Make every INSERT/UPDATE on a protocol's data table hold its backend connection ~100ms (a
    stand-in for a 'slow-ish operation' on protocols that have no SQL sleep). Only touches tables
    the protocols created (warp_search_*, warp_influx_*, warp_graph_*, mongo/dynamo/sqs data tables); Warp's own
    config/nodes/xa/firewall/translation-cache/failed-statement/catalog/audit tables are left alone."""
    c = pg_direct(stack)
    cur = c.cursor()
    cur.execute("CREATE OR REPLACE FUNCTION pool_test_sleep() RETURNS trigger AS $$ BEGIN "
                f"PERFORM pg_sleep({seconds}); RETURN NEW; END $$ LANGUAGE plpgsql")
    cur.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN "
                "('pg_catalog','information_schema') AND tablename NOT IN ('warp_config','warp_nodes','warp_xa_log','warp_firewall_rules',"
                "'warp_translation_cache','warp_failed_statements','_dynamo_tables','sqs_queues_catalog','pool_t') "
                "AND tablename NOT LIKE 'audit%'")
    tables = cur.fetchall()
    for schema, table in tables:
        cur.execute(f'DROP TRIGGER IF EXISTS pool_test_sleep_trg ON "{schema}"."{table}"')
        cur.execute(f'CREATE TRIGGER pool_test_sleep_trg BEFORE INSERT OR UPDATE ON "{schema}"."{table}" '
                    "FOR EACH ROW EXECUTE FUNCTION pool_test_sleep()")
    c.close()
    return [f"{s}.{t}" for s, t in tables]


# ---------------------------------------------------------------------------------------------
# Server-side monitor
# ---------------------------------------------------------------------------------------------
class Monitor(threading.Thread):
    """Samples (a) pg_stat_activity from a direct connection and (b) Warp /metrics pool gauges."""

    def __init__(self, stack, interval=0.05):
        super().__init__(daemon=True)
        self.stack, self.interval = stack, interval
        self.stop_flag = threading.Event()
        self.samples = []  # (t, pg_client_backends, pg_active, pool_active, pool_waiting, pool_max)
        self.t0 = time.time()
        self.per_pool_active, self.per_pool_wait = {}, {}

    def _pool_metrics(self):
        import http.client
        try:
            conn = http.client.HTTPConnection("localhost", self.stack.metrics_port, timeout=2)
            conn.request("GET", "/metrics")
            text = conn.getresponse().read().decode()
        except Exception:  # noqa: BLE001
            return None
        act = wait = mx = 0
        for line in text.splitlines():
            m = re.search(r'pool="([^"]*)"', line)
            if not m:
                continue
            v = int(float(line.rsplit(" ", 1)[1]))
            key = m.group(1).split("|")[0].replace("jdbc:", "")[:60]
            if line.startswith("warp_pool_connections") and 'state="active"' in line:
                act += v
                self.per_pool_active[key] = max(self.per_pool_active.get(key, 0), v)
            elif line.startswith("warp_pool_waiting"):
                wait += v
                self.per_pool_wait[key] = max(self.per_pool_wait.get(key, 0), v)
            elif line.startswith("warp_pool_max_size"):
                mx = max(mx, v)
        return act, wait, mx

    def run(self):
        try:
            c = pg_direct(self.stack)
        except Exception:  # noqa: BLE001
            return
        cur = c.cursor()
        while not self.stop_flag.is_set():
            try:
                cur.execute("SELECT count(*), count(*) FILTER (WHERE state='active') FROM pg_stat_activity "
                            "WHERE backend_type='client backend' AND datname='postgres' AND pid<>pg_backend_pid()")
                total, active = cur.fetchone()
                pm = self._pool_metrics() or (None, None, None)
                self.samples.append((time.time() - self.t0, total, active) + pm)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(self.interval)
        c.close()

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=5)

    def summary(self):
        if not self.samples:
            return {}
        pool_act = [s[3] for s in self.samples if s[3] is not None]
        pool_wait = [s[4] for s in self.samples if s[4] is not None]
        return {
            "max_pg_client_backends": max(s[1] for s in self.samples),
            "max_pg_active_backends": max(s[2] for s in self.samples),
            "max_hikari_active": max(pool_act) if pool_act else None,
            "max_hikari_waiting": max(pool_wait) if pool_wait else None,
            "hikari_max_size": max((s[5] or 0) for s in self.samples),
            "per_pool_max_active": self.per_pool_active, "per_pool_max_waiting": self.per_pool_wait,
            "samples": len(self.samples),
        }


# ---------------------------------------------------------------------------------------------
# Protocol adapters: connect(idx) -> handle ; workload(handle, idx) -> list[(step, seconds)] ;
# close(handle). `setup` runs once before the timed phase.
# ---------------------------------------------------------------------------------------------
class Adapter:
    name = "?"
    driver = "?"
    shared_client = False  # HTTP-ish adapters share one pooled client object across threads
    slow_via_trigger = False

    def __init__(self, stack):
        self.stack = stack
        self.port = stack.ports.get(self.name)

    def setup(self, n, slow=True):
        pass

    def connect(self, idx):
        return None

    def workload(self, h, idx):
        return []

    def one_stmt(self, h, idx):
        """ONE quick, read-only statement/request (no open transaction) for the idle-holder test."""
        raise NotImplementedError

    def close(self, h):
        try:
            if h is not None and hasattr(h, "close"):
                h.close()
        except Exception:  # noqa: BLE001
            pass

    def teardown(self):
        pass


def timed(steps, name, fn):
    t = time.time()
    r = fn()
    steps.append((name, time.time() - t))
    return r


class SqlAdapter(Adapter):
    sleep_sql = "SELECT pg_sleep(0.1)"
    one_sql = "SELECT 1"

    def one_stmt(self, h, idx):
        cur = h.cursor()
        cur.execute(self.one_sql)
        cur.fetchall()
        cur.close()

    def workload(self, h, idx):
        steps = []
        for name, sql in (("select1", self.one_sql), ("sleep100ms", self.sleep_sql), ("select1b", self.one_sql)):
            def run(sql=sql):
                cur = h.cursor()
                cur.execute(sql)
                cur.fetchall()
                cur.close()
            timed(steps, name, run)
        return steps


class Pg(SqlAdapter):
    name, driver = "pg", "psycopg2"

    def connect(self, idx):
        import psycopg2
        c = psycopg2.connect(host="localhost", port=self.port, user="postgres", password="postgres",
                             dbname="postgres", connect_timeout=60)
        c.autocommit = True
        return c


class My(SqlAdapter):
    name, driver = "my", "pymysql"

    def connect(self, idx):
        import pymysql
        return pymysql.connect(host="localhost", port=self.port, user="postgres", password="postgres",
                               database="postgres", connect_timeout=60, read_timeout=120, autocommit=True)


class Mssql(SqlAdapter):
    name, driver = "mssql", "pymssql"

    def connect(self, idx):
        import pymssql
        # no database= : pymssql then sends `USE postgres`, which the Postgres backend rejects
        return pymssql.connect(server="localhost", port=self.port, user="postgres", password="postgres",
                               login_timeout=60, timeout=120, autocommit=True)


class Ora(SqlAdapter):
    name, driver = "ora", "python-oracledb (thin)"
    one_sql = "SELECT 1 FROM dual"
    # a bare `SELECT pg_sleep(..) FROM dual` returns a null ExecutionResult in orawire (NPE, and the
    # session then hangs); routing pg_sleep through a WHERE predicate is a working ~100ms statement
    sleep_sql = "SELECT 1 FROM dual WHERE pg_sleep(0.1) IS NOT NULL"

    def connect(self, idx):
        import oracledb
        c = oracledb.connect(user="postgres", password="postgres",
                             dsn=f"localhost:{self.port}/anything", disable_oob=True, tcp_connect_timeout=60)
        # POOL_ORA_MODE: plain (default, driver autocommit off, no commit) | commit (COMMIT after each
        # statement) | autocommit (connection.autocommit = True)
        if os.environ.get("POOL_ORA_MODE") == "autocommit":
            c.autocommit = True
        return c

    def one_stmt(self, h, idx):
        SqlAdapter.one_stmt(self, h, idx)
        if os.environ.get("POOL_ORA_MODE") == "commit":
            h.commit()


class Mongo(Adapter):
    name, driver = "mongo", "pymongo"
    slow_via_trigger = True

    def setup(self, n, slow=True):
        from pymongo import MongoClient
        self.coll_name = "pool_" + uuid.uuid4().hex[:6]
        # ONE client shared by all threads: pymongo opens one socket per concurrent operation (+ its
        # own monitor sockets), so this is N op sockets + ~2 monitor sockets against the accept gate.
        self.client = MongoClient(host="localhost", port=self.port, maxPoolSize=max(n, 1), minPoolSize=0,
                                  serverSelectionTimeoutMS=60000, socketTimeoutMS=120000,
                                  connectTimeoutMS=60000, waitQueueTimeoutMS=120000, directConnection=True)
        self.client["pooldb"][self.coll_name].insert_one({"_id": "seed", "v": 0})
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        # force this thread's own socket now (ping is served by warp itself, not the backend)
        self.client.admin.command("ping")
        return self.client

    def workload(self, h, idx):
        steps = []
        coll = h["pooldb"][self.coll_name]
        timed(steps, "insert(+100ms trigger)", lambda: coll.insert_one({"_id": f"d{idx}", "v": idx}))
        timed(steps, "find_one", lambda: coll.find_one({"_id": f"d{idx}"}))
        return steps

    def close(self, h):
        pass

    def teardown(self):
        try:
            self.client.close()
        except Exception:  # noqa: BLE001
            pass


class Bolt(Adapter):
    name, driver = "bolt", "neo4j (bolt)"
    slow_via_trigger = True

    def setup(self, n, slow=True):
        from neo4j import GraphDatabase
        d = GraphDatabase.driver(f"bolt://localhost:{self.port}", auth=("postgres", "postgres"))
        with d.session() as s:
            s.run("CREATE (n:PoolSeed {v: 0}) RETURN n.v AS v").single()
        d.close()
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        from neo4j import GraphDatabase
        d = GraphDatabase.driver(f"bolt://localhost:{self.port}", auth=("postgres", "postgres"),
                                 connection_timeout=60, max_connection_lifetime=3600)
        s = d.session()
        s.run("RETURN 1 AS v").single()  # handshake + first message
        return (d, s)

    def workload(self, h, idx):
        steps = []
        d, s = h
        timed(steps, "CREATE(+100ms trigger)", lambda: s.run(f"CREATE (n:PoolT {{v: {idx}}}) RETURN n.v AS v").single())
        return steps

    def close(self, h):
        try:
            h[1].close()
            h[0].close()
        except Exception:  # noqa: BLE001
            pass


class HttpBase(Adapter):
    shared_client = True
    slow_via_trigger = True


class Dynamo(HttpBase):
    name, driver = "dynamo", "boto3 (dynamodb)"

    def setup(self, n, slow=True):
        import boto3
        from botocore.config import Config
        self.client = boto3.client("dynamodb", endpoint_url=f"http://localhost:{self.port}", region_name="us-east-1",
                                   aws_access_key_id="test", aws_secret_access_key="test",
                                   config=Config(retries={"max_attempts": 0}, max_pool_connections=max(n, 10),
                                                 read_timeout=120, connect_timeout=60))
        self.table = "pool_" + uuid.uuid4().hex[:6]
        self.client.create_table(TableName=self.table, KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                                 AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                                 BillingMode="PAY_PER_REQUEST")
        self.client.put_item(TableName=self.table, Item={"id": {"S": "seed"}})
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        return self.client

    def workload(self, h, idx):
        steps = []
        timed(steps, "PutItem(+100ms trigger)", lambda: h.put_item(TableName=self.table, Item={"id": {"S": f"k{idx}"}}))
        timed(steps, "GetItem", lambda: h.get_item(TableName=self.table, Key={"id": {"S": f"k{idx}"}}))
        return steps


class Sqs(HttpBase):
    name, driver = "sqs", "boto3 (sqs)"

    def setup(self, n, slow=True):
        import boto3
        from botocore.config import Config
        self.client = boto3.client("sqs", endpoint_url=f"http://localhost:{self.port}", region_name="us-east-1",
                                   aws_access_key_id="test", aws_secret_access_key="test",
                                   config=Config(retries={"max_attempts": 0}, max_pool_connections=max(n, 10),
                                                 read_timeout=120, connect_timeout=60))
        self.qurl = self.client.create_queue(QueueName="pool" + uuid.uuid4().hex[:6])["QueueUrl"]
        self.client.send_message(QueueUrl=self.qurl, MessageBody="seed")
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        return self.client

    def workload(self, h, idx):
        steps = []
        timed(steps, "SendMessage(+100ms trigger)", lambda: h.send_message(QueueUrl=self.qurl, MessageBody=f"m{idx}"))
        timed(steps, "ReceiveMessage", lambda: h.receive_message(QueueUrl=self.qurl, MaxNumberOfMessages=1))
        return steps


class Os(HttpBase):
    name, driver = "os", "requests (HTTP keep-alive)"

    def setup(self, n, slow=True):
        import requests
        from requests.adapters import HTTPAdapter
        self.index = "pool" + uuid.uuid4().hex[:6]
        self.base = f"http://localhost:{self.port}"
        self.sess = requests.Session()
        self.sess.mount("http://", HTTPAdapter(pool_connections=1, pool_maxsize=max(n, 10)))
        r = self.sess.put(f"{self.base}/{self.index}/_doc/seed", json={"v": 0}, timeout=30)
        r.raise_for_status()
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        return self.sess

    def workload(self, h, idx):
        steps = []
        timed(steps, "PUT _doc(+100ms trigger)", lambda: h.put(f"{self.base}/{self.index}/_doc/d{idx}",
                                                              json={"v": idx}, timeout=120).raise_for_status())
        timed(steps, "GET _doc", lambda: h.get(f"{self.base}/{self.index}/_doc/d{idx}", timeout=120).raise_for_status())
        return steps


class Influx(HttpBase):
    name, driver = "influx", "requests (HTTP keep-alive)"

    def setup(self, n, slow=True):
        import requests
        from requests.adapters import HTTPAdapter
        self.base = f"http://localhost:{self.port}"
        self.sess = requests.Session()
        self.sess.mount("http://", HTTPAdapter(pool_connections=1, pool_maxsize=max(n, 10)))
        self.sess.post(f"{self.base}/write", params={"db": "pooldb"}, data="pm,host=seed v=0", timeout=30).raise_for_status()
        self.tables = add_sleep_triggers(self.stack) if slow else []

    def connect(self, idx):
        return self.sess

    def workload(self, h, idx):
        steps = []
        timed(steps, "write(+100ms trigger)", lambda: h.post(f"{self.base}/write", params={"db": "pooldb"},
                                                             data=f"pm,host=h{idx} v={idx}", timeout=120).raise_for_status())
        timed(steps, "query", lambda: h.get(f"{self.base}/query", params={"db": "pooldb", "q": "SELECT * FROM pm LIMIT 1"},
                                            timeout=120).raise_for_status())
        return steps


class Grpc(Adapter):
    name, driver = "grpc", "grpcio (native QueryService)"

    def __init__(self, stack):
        super().__init__(stack)
        self.port = stack.grpc_port

    def connect(self, idx):
        import grpc
        import warp_pb2_grpc
        ch = grpc.insecure_channel(f"localhost:{self.port}", options=[("grpc.use_local_subchannel_pool", 1)])
        grpc.channel_ready_future(ch).result(timeout=60)
        return (ch, warp_pb2_grpc.QueryServiceStub(ch))

    def workload(self, h, idx):
        import warp_pb2
        steps = []
        ch, stub = h
        for name, sql in (("select1", "SELECT 1"), ("sleep100ms", "SELECT pg_sleep(0.1)")):
            def run(sql=sql):
                r = stub.Execute(warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql=sql, params=[]),
                                 timeout=120)
                if not r.success:
                    raise RuntimeError(r.error_message)
            timed(steps, name, run)
        return steps

    def close(self, h):
        try:
            h[0].close()
        except Exception:  # noqa: BLE001
            pass


def _mongo_one(self, h, idx):
    h["pooldb"][self.coll_name].find_one({"_id": "seed"})


def _bolt_one(self, h, idx):
    h[1].run("RETURN 1 AS v").single()


def _dynamo_one(self, h, idx):
    h.get_item(TableName=self.table, Key={"id": {"S": "seed"}})


def _sqs_one(self, h, idx):
    h.get_queue_attributes(QueueUrl=self.qurl, AttributeNames=["All"])


def _os_one(self, h, idx):
    h.get(f"{self.base}/{self.index}/_doc/seed", timeout=120).raise_for_status()


def _influx_one(self, h, idx):
    h.get(f"{self.base}/query", params={"db": "pooldb", "q": "SELECT * FROM pm LIMIT 1"}, timeout=120).raise_for_status()


def _grpc_one(self, h, idx):
    import warp_pb2
    r = h[1].Execute(warp_pb2.ExecuteRequest(username="postgres", password="postgres", sql="SELECT 1", params=[]),
                     timeout=120)
    if not r.success:
        raise RuntimeError(r.error_message)


Mongo.one_stmt, Bolt.one_stmt, Dynamo.one_stmt, Sqs.one_stmt = _mongo_one, _bolt_one, _dynamo_one, _sqs_one
Os.one_stmt, Influx.one_stmt, Grpc.one_stmt = _os_one, _influx_one, _grpc_one

ADAPTERS = {c.name: c for c in (Pg, My, Mssql, Ora, Mongo, Bolt, Dynamo, Sqs, Os, Influx, Grpc)}
BORROW = {  # verified against the source + observed pool gauges
    "pg": "per statement/transaction (SessionConnectionLease; pinned by open tx / session state)",
    "my": "per statement/transaction (SessionConnectionLease; pinned by open tx / session state)",
    "mssql": "per statement/transaction (SessionConnectionLease; pinned by open tx / session state)",
    "ora": "per statement; pinned by an uncommitted write (LazyPooledConnection)",
    "bolt": "per RUN (SessionConnectionLease)",
    "mongo": "per operation",
    "dynamo": "per request", "sqs": "per request", "os": "per request", "influx": "per request",
    "grpc": "per RPC",
}


# ---------------------------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------------------------
def log_counts(stack):
    log = stack.warp_log()
    return {"license_rejects": log.count("license: rejecting connection"),
            "listener_died": len(re.findall(r"wire listener on port \d+ failed", log)),
            "pool_timeouts": log.count("Connection is not available, request timed out")}


def run_case(stack, proto, n, mode, hold_extra=1.0, deadline=180):
    before = log_counts(stack)
    ad = ADAPTERS[proto](stack)
    ad.setup(n, slow=True)
    mon = Monitor(stack)
    mon.start()
    time.sleep(0.3)
    results = [None] * n
    start_barrier = threading.Barrier(n + 1)
    release = threading.Event()
    done_lock = threading.Lock()
    workload_done = [0]
    t_start = [0.0]

    def worker(i):
        rec = {"i": i, "ok": False, "phase": None, "err_type": None, "err_msg": None, "err_at": None,
               "connect_s": None, "total_s": None, "steps": []}
        h = None
        try:
            start_barrier.wait(timeout=120)
            t0 = time.time()
            rec["phase"] = "connect"
            h = ad.connect(i)
            rec["connect_s"] = time.time() - t0
            rec["phase"] = "workload"
            rec["steps"] = ad.workload(h, i)
            rec["total_s"] = time.time() - t0
            rec["ok"] = True
        except BaseException as e:  # noqa: BLE001
            rec["err_type"] = type(e).__name__
            rec["err_msg"] = re.sub(r"\s+", " ", str(e))[:300]
            rec["err_at"] = time.time() - t_start[0]
            rec["total_s"] = time.time() - t0 if "t0" in dir() else None
        finally:
            with done_lock:
                workload_done[0] += 1
            if mode == "hold" and rec["ok"]:
                release.wait(timeout=deadline)
            if h is not None:
                ad.close(h)
            results[i] = rec

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(n)]
    for t in threads:
        t.start()
    t_start[0] = time.time()
    start_barrier.wait(timeout=120)
    t_start[0] = time.time()
    end_by = time.time() + deadline
    while time.time() < end_by:
        with done_lock:
            if workload_done[0] >= n:
                break
        time.sleep(0.1)
    time.sleep(hold_extra)
    release.set()
    for t in threads:
        t.join(timeout=30)
    wall = time.time() - t_start[0]
    mon.stop()
    ad.teardown()
    after = log_counts(stack)
    res = summarize(proto, ad, n, mode, results, mon, wall)
    res["warp_log_delta"] = {k: after[k] - before[k] for k in after}
    return res


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def summarize(proto, ad, n, mode, results, mon, wall):
    results = [r for r in results if r]
    ok = [r for r in results if r["ok"]]
    bad = [r for r in results if not r["ok"]]
    errs = {}
    for r in bad:
        key = f'{r["phase"]}: {r["err_type"]}: {r["err_msg"]}'
        # collapse embedded numbers/ports so identical errors group together
        key = re.sub(r"\d{4,}", "#", key)
        e = errs.setdefault(key, {"count": 0, "first_at": r["err_at"], "last_at": r["err_at"],
                                  "lat": []})
        e["count"] += 1
        e["first_at"] = min(e["first_at"], r["err_at"])
        e["last_at"] = max(e["last_at"], r["err_at"])
        if r["total_s"] is not None:
            e["lat"].append(r["total_s"])
    lat = [r["total_s"] for r in ok]
    return {
        "protocol": proto, "driver": ad.driver, "borrow_scope": BORROW.get(proto), "n": n, "mode": mode,
        "succeeded": len(ok), "failed": len(bad), "wall_s": round(wall, 2),
        "ok_latency_s": {"p50": pct(lat, 50), "p90": pct(lat, 90), "p99": pct(lat, 99), "max": max(lat) if lat else None,
                         "min": min(lat) if lat else None},
        "ok_connect_max_s": max([r["connect_s"] for r in ok if r["connect_s"] is not None], default=None),
        "errors": [{"msg": k, "count": v["count"], "first_at_s": round(v["first_at"], 3),
                    "last_at_s": round(v["last_at"], 3),
                    "client_wait_before_error_p50_s": round(pct(v["lat"], 50), 3) if v["lat"] else None}
                   for k, v in sorted(errs.items(), key=lambda kv: -kv[1]["count"])],
        "server": mon.summary(),
    }


def pool_snapshot(stack):
    """(hikari_active_total, hikari_waiting_total) from Warp's own /metrics right now."""
    m = Monitor(stack)
    r = m._pool_metrics()
    return (r[0], r[1]) if r else (None, None)


def idle_holders_case(stack, proto, holders=10, extra=2):
    """`holders` clients each connect, run ONE quick read-only statement (no open transaction) and then sit
    idle while staying connected. Then client `holders+1` (and +2) connect and run one quick statement.
    Reports, for each, success/latency/error and Hikari active/waiting -- i.e. whether idle sessions pin
    backend connections."""
    ad = ADAPTERS[proto](stack)
    ad.setup(holders + extra + 1, slow=False)
    time.sleep(0.5)
    base_active, _ = pool_snapshot(stack)
    handles, rows = [], []

    def one(idx, label):
        rec = {"client": label, "ok": False, "connect_s": None, "stmt_s": None, "err": None}
        h = None
        t0 = time.time()
        try:
            h = ad.connect(idx)
            rec["connect_s"] = round(time.time() - t0, 3)
            t1 = time.time()
            ad.one_stmt(h, idx)
            rec["stmt_s"] = round(time.time() - t1, 3)
            rec["ok"] = True
        except BaseException as e:  # noqa: BLE001
            rec["err"] = f'{type(e).__name__}: {re.sub(chr(10), " ", str(e))[:160]}'
            rec["total_s"] = round(time.time() - t0, 3)
        rec["hikari_active_after"], rec["hikari_waiting_after"] = pool_snapshot(stack)
        return h, rec

    for i in range(holders):
        h, rec = one(i, f"holder{i + 1}")
        rows.append(rec)
        if h is not None:
            handles.append(h)
    time.sleep(1.0)
    idle_active, idle_wait = pool_snapshot(stack)
    for j in range(extra):
        h, rec = one(holders + j, f"extra{holders + j + 1}")
        rows.append(rec)
        if h is not None:
            handles.append(h)
    for h in handles:
        ad.close(h)
    ad.teardown()
    ok_extra = [r for r in rows[holders:]]
    return {"protocol": proto, "driver": ad.driver, "borrow_scope": BORROW.get(proto), "holders": holders,
            "hikari_active_baseline_before": base_active, "hikari_active_with_holders_idle": idle_active,
            "hikari_waiting_with_holders_idle": idle_wait, "holder_ok": sum(1 for r in rows[:holders] if r["ok"]),
            "extras": ok_extra, "holder_rows": rows[:holders]}


def print_holders(r):
    print(f'\n### IDLE-HOLDERS {r["protocol"]} [{r["driver"]}] borrow={r["borrow_scope"]}')
    print(f'  hikari active: baseline={r["hikari_active_baseline_before"]} with {r["holders"]} idle holders='
          f'{r["hikari_active_with_holders_idle"]} (waiting {r["hikari_waiting_with_holders_idle"]}); '
          f'holders ok={r["holder_ok"]}/{r["holders"]}')
    for e in r["extras"]:
        print(f'  {e["client"]}: ok={e["ok"]} connect={e["connect_s"]} stmt={e["stmt_s"]} total_if_err={e.get("total_s")} '
              f'err={e["err"]} active_after={e["hikari_active_after"]} waiting_after={e["hikari_waiting_after"]}')


def release_probe(stack, proto):
    """Does ONE client pin a backend connection while idle, per transaction-state variant? Reports Hikari
    active minus baseline (0 = released, 1 = held) after the client has gone idle for 1s. SQL protocols
    only. Variants: A plain SELECT; B autocommit INSERT; C INSERT+COMMIT; D INSERT with an OPEN transaction."""
    c = pg_direct(stack)
    c.cursor().execute("CREATE TABLE IF NOT EXISTS pool_t (id int)")
    c.close()
    ad = ADAPTERS[proto](stack)
    out = {"protocol": proto, "variants": {}}
    time.sleep(0.5)
    base, _ = pool_snapshot(stack)
    ins = "INSERT INTO pool_t (id) VALUES (1)"

    def probe(name, fn):
        h = ad.connect(0)
        try:
            fn(h)
            time.sleep(1.0)
            act, _ = pool_snapshot(stack)
            out["variants"][name] = (act - base) if act is not None else None
        except BaseException as e:  # noqa: BLE001
            out["variants"][name] = f"ERR {type(e).__name__}: {str(e)[:80]}"
        finally:
            ad.close(h)
            time.sleep(1.5)

    def run(h, sql):
        cur = h.cursor()
        cur.execute(sql)
        try:
            cur.fetchall()
        except Exception:  # noqa: BLE001
            pass
        cur.close()

    def set_ac(h, val):
        if proto == "pg":
            h.autocommit = val
        elif proto == "my":
            h.autocommit(val)
        elif proto == "mssql":
            h.autocommit(val)
        elif proto == "ora":
            h.autocommit = val

    probe("A select (autocommit as driver default)", lambda h: run(h, ad.one_sql))
    def b(h):
        set_ac(h, True)
        run(h, ins)
    probe("B autocommit INSERT", b)
    def c_(h):
        set_ac(h, False)
        run(h, ins)
        h.commit()
    probe("C INSERT then COMMIT", c_)
    def d(h):
        set_ac(h, False)
        run(h, ins)
    probe("D INSERT, transaction left open", d)
    out["baseline_active"] = base
    return out


def fmt(x):
    return "-" if x is None else (f"{x:.3f}" if isinstance(x, float) else str(x))


def print_result(r):
    L = r["ok_latency_s"]
    print(f'\n### {r["protocol"]} [{r["driver"]}] N={r["n"]} mode={r["mode"]} borrow={r["borrow_scope"]}')
    print(f'  ok={r["succeeded"]} failed={r["failed"]} wall={r["wall_s"]}s  ok-latency p50/p90/p99/max='
          f'{fmt(L["p50"])}/{fmt(L["p90"])}/{fmt(L["p99"])}/{fmt(L["max"])}s  connect_max={fmt(r["ok_connect_max_s"])}')
    for e in r["errors"]:
        print(f'  ERR x{e["count"]} first@{e["first_at_s"]}s last@{e["last_at_s"]}s '
              f'(client waited p50 {e["client_wait_before_error_p50_s"]}s): {e["msg"][:200]}')
    srv = {k: v for k, v in r["server"].items() if not k.startswith("per_pool")}
    print(f'  server: {srv}  warp_log_delta={r.get("warp_log_delta")}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocols", default="pg")
    ap.add_argument("--n", type=int, nargs="+", default=[25])
    ap.add_argument("--mode", default="hold", choices=["hold", "quick"])
    ap.add_argument("--variant", default="default")
    ap.add_argument("--env", action="append", default=[], help="extra Warp env KEY=VALUE (repeatable)")
    ap.add_argument("--idle-holders", action="store_true", help="run the 10 idle holders + 11th/12th client test")
    ap.add_argument("--release-probe", action="store_true", help="per transaction-state release probe (SQL protocols)")
    ap.add_argument("--fresh", action="store_true", help="fresh Postgres+Warp for every (protocol, N) case")
    ap.add_argument("--deadline", type=int, default=180)
    ap.add_argument("--out", default=None)
    ap.add_argument("--log", default=None, help="write the Warp log here")
    args = ap.parse_args()
    extra = dict(kv.split("=", 1) for kv in args.env)
    protos = args.protocols.split(",")
    stack = Stack(extra_env=extra, protocols=protos)
    all_results = []
    try:
        for p in protos:
            if args.idle_holders:
                r = idle_holders_case(stack, p)
                r["variant"], r["env"] = args.variant, extra
                print_holders(r)
                all_results.append(r)
                sys.stdout.flush()
                continue
            if args.release_probe:
                r = release_probe(stack, p)
                print("RELEASE-PROBE", json.dumps(r))
                all_results.append(r)
                sys.stdout.flush()
                continue
            for n in args.n:
                if args.fresh and all_results:
                    stack.close()
                    stack = Stack(extra_env=extra, protocols=protos)
                r = run_case(stack, p, n, args.mode, deadline=args.deadline)
                r["variant"] = args.variant
                r["env"] = extra
                print_result(r)
                all_results.append(r)
                sys.stdout.flush()
    finally:
        log = stack.warp_log()
        stack.close()
        if args.log:
            with open(args.log, "w") as f:
                f.write(log)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(all_results, f, indent=1)


@opt_in_experiment
def test_pool_saturation_smoke():
    """Opt-in smoke: pgwire, 25 hold-mode clients against pool=10 -> the pool must never exceed 10."""
    stack = Stack(protocols=["pg"])
    try:
        r = run_case(stack, "pg", 25, "hold")
        print_result(r)
        assert r["server"]["max_hikari_active"] <= 10
    finally:
        stack.close()


# =============================================================================================
# Connection multiplexing (many clients -> few backend connections): REAL tests, no mocks.
#
# pgwire / mywire / mssqlwire / orawire / boltwire used to hold ONE pooled backend connection per client session
# for the session's whole life; with WARP_POOL_MAX_SIZE=N the (N+1)th idle client starved everyone. They now
# borrow per statement/transaction and pin only while the session holds a transaction or backend session state
# (core/SessionConnectionLease, docs/WARP_GUIDE.md "Connection multiplexing"). These tests drive real client
# drivers against a real Warp jar + Postgres with a deliberately tiny pool.
# =============================================================================================
import socket
import struct

MUX_PROTOS = ["pg", "my", "mssql", "ora", "ora_noac", "bolt"]
SQL_PROTOS = ["pg", "my", "mssql", "ora", "ora_noac"]


def _base(proto):
    return "ora" if proto == "ora_noac" else proto


class SqlClient:
    """One real driver connection to a wire frontend, with a uniform tiny API."""

    def __init__(self, stack, proto, autocommit=True, user="postgres", password="postgres"):
        self.proto = proto
        port = stack.ports[_base(proto)]
        b = _base(proto)
        if b == "pg":
            import psycopg2
            self.c = psycopg2.connect(host="localhost", port=port, user=user, password=password, dbname="postgres",
                                      connect_timeout=30)
            self.c.autocommit = autocommit
        elif b == "my":
            import pymysql
            self.c = pymysql.connect(host="localhost", port=port, user=user, password=password, database="postgres",
                                     connect_timeout=30, read_timeout=60, autocommit=autocommit)
        elif b == "mssql":
            import pymssql
            self.c = pymssql.connect(server="localhost", port=port, user=user, password=password, login_timeout=30,
                                     timeout=60, autocommit=autocommit)
        elif b == "ora":
            import oracledb
            self.c = oracledb.connect(user=user, password=password, dsn=f"localhost:{port}/anything",
                                      disable_oob=True, tcp_connect_timeout=30)
            self.c.autocommit = (proto == "ora") and autocommit
        self.select1 = "SELECT 1 FROM dual" if b == "ora" else "SELECT 1"

    def ph(self, n):
        """Placeholder for parameter n (1-based)."""
        return f":{n}" if _base(self.proto) == "ora" else "%s"

    def run(self, sql, params=None):
        cur = self.c.cursor()
        try:
            cur.execute(sql, params) if params is not None else cur.execute(sql)
            if cur.description:
                return cur.fetchall()
            return None
        finally:
            cur.close()

    def commit(self):
        self.c.commit()

    def rollback(self):
        self.c.rollback()

    def close(self):
        try:
            self.c.close()
        except Exception:  # noqa: BLE001
            pass


class BoltClient:
    def __init__(self, stack):
        from neo4j import GraphDatabase
        self.d = GraphDatabase.driver(f"bolt://localhost:{stack.ports['bolt']}", auth=("postgres", "postgres"),
                                      connection_timeout=30)
        self.s = self.d.session()

    def run(self, cypher):
        return self.s.run(cypher).data()

    def close(self):
        try:
            self.s.close()
            self.d.close()
        except Exception:  # noqa: BLE001
            pass


def mux_client(stack, proto, **kw):
    return BoltClient(stack) if proto == "bolt" else SqlClient(stack, proto, **kw)


def one_backend_stmt(cl):
    """One statement that really reaches the backend (boltwire's RETURN <literal> is answered by Warp itself)."""
    if isinstance(cl, BoltClient):
        return cl.run("MATCH (n:MuxSeed) RETURN n.v AS v LIMIT 1")
    return cl.run(cl.select1)


def insert_stmt(cl, idx):
    if isinstance(cl, BoltClient):
        return cl.run(f"CREATE (n:MuxT {{v: {idx}}}) RETURN n.v AS v")
    cl.run(f"INSERT INTO mux_t (id, who) VALUES ({cl.ph(1)}, {cl.ph(2)})", (idx, cl.proto))
    if cl.proto == "ora_noac":
        cl.commit()
    return None


def wait_pool_idle(stack, timeout=6.0):
    """Hikari active connections (Warp's own /metrics gauge) back to 0."""
    deadline = time.time() + timeout
    act = None
    while time.time() < deadline:
        act, _ = pool_snapshot(stack)
        if act == 0:
            return 0
        time.sleep(0.1)
    return act


def wait_pool_active(stack, n, timeout=6.0):
    deadline = time.time() + timeout
    act = None
    while time.time() < deadline:
        act, _ = pool_snapshot(stack)
        if act is not None and act >= n:
            return act
        time.sleep(0.1)
    return act


def wait_pool_is(stack, n, timeout=6.0):
    deadline = time.time() + timeout
    act = None
    while time.time() < deadline:
        act, _ = pool_snapshot(stack)
        if act == n:
            return n
        time.sleep(0.1)
    return act


def seed_mux_schema(stack):
    c = pg_direct(stack)
    cur = c.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS mux_t (id int, who text)")
    cur.execute("DELETE FROM mux_t")
    c.close()
    from neo4j import GraphDatabase
    d = GraphDatabase.driver(f"bolt://localhost:{stack.ports['bolt']}", auth=("postgres", "postgres"))
    with d.session() as s:
        s.run("CREATE (n:MuxSeed {v: 1}) RETURN n.v AS v").single()
    d.close()


@pytest.fixture(scope="module")
def mux_stack():
    """Warp with a backend pool of just TWO connections."""
    stack = Stack(extra_env={"WARP_POOL_MAX_SIZE": "2", "WARP_POOL_CONNECT_TIMEOUT_MS": "1500"})
    try:
        seed_mux_schema(stack)
        yield stack
    finally:
        stack.close()


# ---- (a) idle clients must not hold backend connections ---------------------------------------------------
@pytest.mark.parametrize("proto", MUX_PROTOS)
def test_idle_clients_do_not_starve_others(mux_stack, proto):
    """6 clients each run a statement and go idle (still connected); with pool=2 the old hold-for-session model
    starved the 3rd. Now: the pool is fully free while they idle, and extra clients run with no wait."""
    holders, extras = [], []
    try:
        for i in range(6):
            cl = mux_client(mux_stack, proto)
            one_backend_stmt(cl)
            holders.append(cl)
        assert wait_pool_idle(mux_stack) == 0, "idle clients still hold backend connections"
        for i in range(3):
            t0 = time.time()
            cl = mux_client(mux_stack, proto)
            r = one_backend_stmt(cl)
            elapsed = time.time() - t0
            extras.append(cl)
            assert r, f"extra client {i} got no result"
            assert elapsed < 1.2, f"extra client {i} waited {elapsed:.2f}s (pool timeout is 1.5s): idle holders starve it"
        # holders are still perfectly usable after all that
        for cl in holders:
            assert one_backend_stmt(cl)
    finally:
        for cl in holders + extras:
            cl.close()


@pytest.mark.parametrize("proto", ["pg", "my", "mssql", "ora"])
def test_committed_or_finished_transaction_releases(mux_stack, proto):
    """A client that wrote inside an explicit transaction and committed is idle afterwards: pool free again."""
    cl = mux_client(mux_stack, proto, autocommit=False)
    try:
        insert_stmt(cl, 1000)
        assert wait_pool_active(mux_stack, 1, 3) >= 1, "an open write transaction must hold its connection"
        cl.commit()
        assert wait_pool_idle(mux_stack) == 0, "connection not released after COMMIT"
    finally:
        cl.close()
        cl2 = mux_client(mux_stack, proto)
        cl2.run("DELETE FROM mux_t WHERE id = 1000")
        cl2.close()


# ---- (b) 25 concurrent clients (Developer cap) sharing a pool of 2 -------------------------------------------
@pytest.mark.parametrize("proto", MUX_PROTOS)
def test_25_concurrent_clients_share_pool_of_two(mux_stack, proto):
    time.sleep(3.0)  # let the previous test's sockets finish closing on the Warp side (25-connection license cap)
    n = 25
    barrier = threading.Barrier(n)
    results, errors = [None] * n, []
    peak = {"active": 0}
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            act, _ = pool_snapshot(mux_stack)
            if act is not None:
                peak["active"] = max(peak["active"], act)
            time.sleep(0.05)

    def worker(i):
        cl = None
        try:
            barrier.wait(timeout=60)
            # FreeTDS (pymssql) resolves "localhost" to ::1 AND 127.0.0.1 and briefly opens both sockets, which the
            # Developer 25-connection gate counts; a few tens of ms of stagger keeps 25 concurrent clients under it.
            time.sleep(i * 0.03)
            t0 = time.time()
            cl = mux_client(mux_stack, proto)
            for k in range(4):
                one_backend_stmt(cl)
                insert_stmt(cl, 10_000 + i * 10 + k)
            one_backend_stmt(cl)
            time.sleep(0.3)  # think time while connected: must not hold a connection
            one_backend_stmt(cl)
            results[i] = time.time() - t0
        except BaseException as e:  # noqa: BLE001
            errors.append(f"client {i}: {type(e).__name__}: {str(e)[:200]}")
        finally:
            if cl is not None:
                cl.close()

    mon = threading.Thread(target=sampler, daemon=True)
    mon.start()
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)
    stop.set()
    assert not errors, f"{len(errors)}/{n} clients failed sharing a pool of 2: {errors[:3]}"
    lat = sorted(r for r in results if r is not None)
    print(f"\n[mux 25 clients / pool=2] {proto}: ok={len(lat)} p50={lat[len(lat) // 2]:.2f}s "
          f"p90={lat[int(len(lat) * 0.9)]:.2f}s max={lat[-1]:.2f}s peak hikari active={peak['active']}")
    assert peak["active"] <= 2
    assert wait_pool_idle(mux_stack) == 0


# ---- (c) correctness --------------------------------------------------------------------------------------
def _count(cl):
    return cl.run("SELECT count(*) FROM mux_t WHERE id = 555")[0][0]


@pytest.mark.parametrize("proto", ["pg", "my", "mssql", "ora"])
def test_transaction_isolation_and_rollback_commit(mux_stack, proto):
    a = mux_client(mux_stack, proto, autocommit=False)
    b = mux_client(mux_stack, proto)  # autocommit reader on the OTHER connection
    try:
        a.run(f"INSERT INTO mux_t (id, who) VALUES (555, {a.ph(1)})", ("a",))
        assert int(_count(b)) == 0, "uncommitted row visible to another client"
        a.rollback()
        assert int(_count(b)) == 0
        a.run(f"INSERT INTO mux_t (id, who) VALUES (555, {a.ph(1)})", ("a",))
        assert int(_count(b)) == 0
        a.commit()
        assert int(_count(b)) == 1, "committed row not visible"
    finally:
        a.close()
        b.close()
        c = pg_direct(mux_stack)
        c.cursor().execute("DELETE FROM mux_t WHERE id = 555")
        c.close()


def test_pg_transaction_stays_on_one_connection_and_others_share_the_rest(mux_stack):
    a = SqlClient(mux_stack, "pg", autocommit=False)
    others = [SqlClient(mux_stack, "pg") for _ in range(5)]
    try:
        a.run("SELECT 1")
        pid = a.run("SELECT pg_backend_pid()")[0][0]
        seen = set()
        for _ in range(5):
            for o in others:
                seen.add(o.run("SELECT pg_backend_pid()")[0][0])
            assert a.run("SELECT pg_backend_pid()")[0][0] == pid, "a transaction must keep one backend connection"
        assert pid not in seen, "another client ran on the connection pinned by an open transaction"
        assert len(seen) == 1, f"5 clients should share the ONE remaining connection, saw {len(seen)} backends"
        a.rollback()
    finally:
        for c in [a] + others:
            c.close()


def test_pg_many_clients_multiplex_onto_at_most_pool_size_backends(mux_stack):
    clients = [SqlClient(mux_stack, "pg") for _ in range(12)]
    try:
        pids = set()
        for _ in range(4):
            for c in clients:
                pids.add(c.run("SELECT pg_backend_pid()")[0][0])
        assert len(pids) <= 2, f"12 clients used {len(pids)} backends with WARP_POOL_MAX_SIZE=2"
    finally:
        for c in clients:
            c.close()


def test_pg_plain_set_is_replayed_not_pinned_and_never_leaks(mux_stack):
    """`SET name = value` is what drivers send at connect (psycopg2: SET DATESTYLE). It must not pin: Warp records
    it and replays it on whichever connection the session gets next, and other clients never see it."""
    a = SqlClient(mux_stack, "pg")
    helpers = [SqlClient(mux_stack, "pg") for _ in range(4)]
    try:
        a.run("SET application_name = 'mux_owner'")
        assert wait_pool_idle(mux_stack) == 0, "a plain SET pinned the session's connection"
        for _ in range(10):  # other clients hammer the pool between every one of a's statements
            for h in helpers:
                assert h.run("SHOW application_name")[0][0] != "mux_owner", "SET leaked to another client"
            assert a.run("SHOW application_name")[0][0] == "mux_owner", "SET state not replayed for its own session"
        a.run("SET TIME ZONE 'Asia/Tokyo'")
        for h in helpers:
            h.run("SELECT 1")
        assert a.run("SHOW timezone")[0][0] == "Asia/Tokyo"
        assert a.run("SHOW application_name")[0][0] == "mux_owner"
        a.run("RESET application_name")
        assert a.run("SHOW application_name")[0][0] != "mux_owner"
        assert a.run("SHOW timezone")[0][0] == "Asia/Tokyo"
        assert wait_pool_idle(mux_stack) == 0
    finally:
        a.close()
        for h in helpers:
            h.close()
    probe = SqlClient(mux_stack, "pg")
    try:
        for _ in range(3):
            assert probe.run("SHOW timezone")[0][0] != "Asia/Tokyo", "SET TIME ZONE leaked after its owner left"
    finally:
        probe.close()


def test_pg_default_psycopg2_client_holds_no_connection_while_idle(mux_stack):
    import psycopg2
    conns = [psycopg2.connect(host="localhost", port=mux_stack.ports["pg"], user="postgres", password="postgres",
                              dbname="postgres") for _ in range(6)]  # default autocommit=False, never used
    try:
        assert wait_pool_idle(mux_stack) == 0, "idle psycopg2 clients (connect-time SET DATESTYLE etc.) pin connections"
    finally:
        for c in conns:
            c.close()


def test_pg_temp_table_and_set_role_pin_and_are_wiped(mux_stack):
    a = SqlClient(mux_stack, "pg")
    helpers = [SqlClient(mux_stack, "pg") for _ in range(4)]
    try:
        a.run("CREATE TEMP TABLE mux_tmp (id int)")
        a.run("INSERT INTO mux_tmp VALUES (42)")
        assert wait_pool_active(mux_stack, 1, 3) >= 1, "a temp table must pin the session"
        for _ in range(10):
            for h in helpers:
                h.run("SELECT 1")
            assert a.run("SELECT id FROM mux_tmp")[0][0] == 42, "temp table lost: session was not pinned"
    finally:
        a.close()
        for h in helpers:
            h.close()
    time.sleep(0.5)
    probe = SqlClient(mux_stack, "pg")
    try:
        for _ in range(3):
            assert probe.run("SELECT count(*) FROM pg_tables WHERE tablename = 'mux_tmp'")[0][0] == 0
    finally:
        probe.close()
    # SET ROLE is not replayable (RESET ALL skips it): pinned, and wiped when its owner leaves
    c = pg_direct(mux_stack)
    c.cursor().execute("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mux_role') THEN "
                       "CREATE ROLE mux_role; END IF; END $$")
    c.close()
    r = SqlClient(mux_stack, "pg")
    r.run("SET ROLE mux_role")
    assert r.run("SELECT current_user")[0][0] == "mux_role"
    r.close()
    time.sleep(0.5)
    probe = SqlClient(mux_stack, "pg")
    try:
        assert probe.run("SELECT current_user")[0][0] == "postgres", "SET ROLE leaked to the next client"
    finally:
        probe.close()


def test_pg_closing_a_pinned_session_releases_its_connection(mux_stack):
    a = SqlClient(mux_stack, "pg", autocommit=False)
    a.run("INSERT INTO mux_t (id, who) VALUES (777, 'x')")
    assert wait_pool_active(mux_stack, 1, 3) >= 1
    a.c.close()  # client vanishes mid-transaction
    assert wait_pool_idle(mux_stack) == 0, "closing a pinned session must release its backend connection"
    probe = SqlClient(mux_stack, "pg")
    assert probe.run("SELECT count(*) FROM mux_t WHERE id = 777")[0][0] == 0, "uncommitted work must be rolled back"
    probe.close()


# ---- raw pgwire extended protocol (named prepared statements, portals with partial fetch) -----------------------
class RawPg:
    """Minimal pgwire v3 client speaking the EXTENDED protocol by hand (drivers hide Parse/Bind/Execute)."""

    def __init__(self, port, user="postgres", password="postgres"):
        self.s = socket.create_connection(("localhost", port), timeout=30)
        params = b"user\0" + user.encode() + b"\0database\0postgres\0\0"
        self.s.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)
        while True:
            t, body = self.read()
            if t == b"R" and struct.unpack("!i", body[:4])[0] == 3:
                pw = password.encode() + b"\0"
                self.s.sendall(b"p" + struct.pack("!i", 4 + len(pw)) + pw)
            elif t == b"Z":
                return
            elif t == b"E":
                raise RuntimeError(body)

    def read(self):
        hdr = self._exact(5)
        t, ln = hdr[:1], struct.unpack("!i", hdr[1:])[0]
        return t, self._exact(ln - 4)

    def _exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.s.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf

    def send(self, t, body=b""):
        self.s.sendall(t + struct.pack("!i", 4 + len(body)) + body)

    @staticmethod
    def cstr(s):
        return s.encode() + b"\0"

    def parse(self, name, sql):
        self.send(b"P", self.cstr(name) + self.cstr(sql) + struct.pack("!h", 0))

    def bind(self, portal, stmt, params=()):
        body = self.cstr(portal) + self.cstr(stmt) + struct.pack("!h", 0) + struct.pack("!h", len(params))
        for p in params:
            b = str(p).encode()
            body += struct.pack("!i", len(b)) + b
        self.send(b"B", body + struct.pack("!h", 0))

    def execute(self, portal, max_rows=0):
        self.send(b"E", self.cstr(portal) + struct.pack("!i", max_rows))

    def flush(self):
        self.send(b"H")

    def simple(self, sql):
        """Simple-query round trip -> (rows, error_fields dict or None)."""
        self.send(b"Q", self.cstr(sql))
        rows, _, err = self.collect({b"Z"})
        return rows, (self.error_fields(err) if err else None)

    @staticmethod
    def error_fields(body):
        fields, i = {}, 0
        while i < len(body) and body[i] != 0:
            code = chr(body[i])
            end = body.index(b"\0", i + 1)
            fields[code] = body[i + 1:end].decode()
            i = end + 1
        return fields

    def sync(self):
        self.send(b"S")

    def collect(self, until):
        """Read messages until one of the type bytes in `until`; return (rows, tags, error)."""
        rows, tags, err = [], [], None
        while True:
            t, body = self.read()
            if t == b"D":
                n = struct.unpack("!h", body[:2])[0]
                off, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", body[off:off + 4])[0]
                    off += 4
                    if ln < 0:
                        row.append(None)
                    else:
                        row.append(body[off:off + ln].decode())
                        off += ln
                rows.append(row)
            elif t == b"E":
                err = body
            tags.append(t)
            if t in until:
                return rows, tags, err

    def close(self):
        try:
            self.send(b"X")
            self.s.close()
        except Exception:  # noqa: BLE001
            pass


def test_pg_extended_protocol_prepared_statement_reused_across_releases(mux_stack):
    raw = RawPg(mux_stack.ports["pg"])
    noise = [SqlClient(mux_stack, "pg") for _ in range(3)]
    try:
        raw.parse("S_inc", "SELECT $1::int + 1")
        raw.flush()
        raw.collect({b"1"})
        for i in range(200):
            raw.bind("", "S_inc", (i,))
            raw.execute("")
            raw.sync()
            rows, tags, err = raw.collect({b"Z"})
            assert err is None and rows == [[str(i + 1)]], (i, rows, err)
            if i % 10 == 0:  # other clients take turns on the pool between executions -> the connection is gone
                for n in noise:
                    n.run("SELECT 1")
        # after all that the named statement is still usable, on whichever connection is free now
        assert wait_pool_idle(mux_stack) == 0
        raw.bind("", "S_inc", (1000,))
        raw.execute("")
        raw.sync()
        rows, _, err = raw.collect({b"Z"})
        assert rows == [[str(1001)]] and err is None
    finally:
        raw.close()
        for n in noise:
            n.close()


def test_pg_extended_protocol_partial_fetch_portal_survives_other_clients(mux_stack):
    raw = RawPg(mux_stack.ports["pg"])
    noise = [SqlClient(mux_stack, "pg") for _ in range(3)]
    try:
        raw.parse("S_gen", "SELECT generate_series(1, 10)")
        raw.bind("P_gen", "S_gen")
        raw.flush()
        raw.collect({b"2"})
        got = []
        for expect_suspend in (True, True, True, False):
            raw.execute("P_gen", 3)
            raw.flush()
            rows, tags, err = raw.collect({b"s", b"C"})
            assert err is None
            got += [int(r[0]) for r in rows]
            assert (tags[-1] == b"s") == expect_suspend
            for n in noise:  # the portal is half-read; the pool is used by everyone else in the meantime
                n.run("SELECT 1")
        raw.sync()
        raw.collect({b"Z"})
        assert got == list(range(1, 11))
        assert wait_pool_idle(mux_stack) == 0, "a half-read portal must not hold a backend connection"
    finally:
        raw.close()
        for n in noise:
            n.close()


def test_pg_sql_cursor_with_hold_pins_and_fetches_across_statements(mux_stack):
    a = SqlClient(mux_stack, "pg")
    others = [SqlClient(mux_stack, "pg") for _ in range(3)]
    try:
        a.run("DECLARE hold_cur CURSOR WITH HOLD FOR SELECT generate_series(1, 6)")
        got = []
        for _ in range(3):
            got += [r[0] for r in a.run("FETCH 2 FROM hold_cur")]
            for o in others:
                o.run("SELECT 1")
        assert got == [1, 2, 3, 4, 5, 6], "SQL-level cursor lost between statements: session was not pinned"
    finally:
        a.close()
        for o in others:
            o.close()


def test_pg_in_transaction_cursor_fetch(mux_stack):
    a = SqlClient(mux_stack, "pg", autocommit=False)
    others = [SqlClient(mux_stack, "pg") for _ in range(3)]
    try:
        a.run("DECLARE tx_cur CURSOR FOR SELECT generate_series(1, 6)")
        got = []
        for _ in range(3):
            got += [r[0] for r in a.run("FETCH 2 FROM tx_cur")]
            for o in others:
                o.run("SELECT 1")
        assert got == [1, 2, 3, 4, 5, 6]
        a.rollback()
    finally:
        a.close()
        for o in others:
            o.close()


# ---- (c) security: identity never leaks between clients sharing a physical connection ----------------------------
@pytest.fixture(scope="class")
def rls_stack():
    """Pool of ONE connection + real per-user Postgres logins: every client of every protocol shares it."""

    def setup(pg):
        import psycopg2
        c = psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres")
        c.autocommit = True
        cur = c.cursor()
        cur.execute("CREATE ROLE alice LOGIN PASSWORD 'alicepw'")
        cur.execute("CREATE ROLE bob LOGIN PASSWORD 'bobpw'")
        cur.execute("CREATE TABLE rls_docs (owner text, body text)")
        cur.execute("INSERT INTO rls_docs VALUES ('alice', 'alice-secret'), ('bob', 'bob-secret')")
        # the pool user is a superuser (bypasses RLS), so the policy is expressed as a view on the identity Warp sets
        cur.execute("CREATE VIEW my_docs AS SELECT body FROM rls_docs "
                    "WHERE owner = current_setting('warp.user_id', true)")
        c.close()

    stack = Stack(extra_env={"WARP_POOL_MAX_SIZE": "1", "WARP_AUTH_MODE": "postgres_roles",
                             "WARP_AUTH_REFRESH_SECONDS": "1", "WARP_POOL_CONNECT_TIMEOUT_MS": "4000"},
                  pg_setup=setup)
    try:
        time.sleep(2.5)  # role verifier cache refresh
        yield stack
    finally:
        stack.close()


class TestRlsIdentityIsolation:
    """One class = one Warp instance that lives only for these tests (the Developer license caps live instances)."""

    def test_rls_identity_does_not_leak_between_clients_on_one_physical_connection(self, rls_stack):
        alice = SqlClient(rls_stack, "pg", user="alice", password="alicepw")
        bob = SqlClient(rls_stack, "pg", user="bob", password="bobpw")
        my_anon = SqlClient(rls_stack, "my")  # mywire never carries an identity (its own shared credential)
        try:
            pid_seen = set()
            for i in range(30):
                a_rows = alice.run("SELECT body FROM my_docs")
                pid_seen.add(alice.run("SELECT pg_backend_pid()")[0][0])
                b_rows = bob.run("SELECT body FROM my_docs")
                m_rows = my_anon.run("SELECT body FROM my_docs")
                pid_seen.add(bob.run("SELECT pg_backend_pid()")[0][0])
                assert a_rows == [("alice-secret",)], f"alice saw {a_rows}"
                assert b_rows == [("bob-secret",)], f"bob saw {b_rows}"
                assert list(m_rows) == [], f"an identity-less mywire client saw {m_rows}: identity leaked across clients"
            assert len(pid_seen) == 1, f"the test needs every client on ONE physical connection, saw {pid_seen}"
            # identity value itself, both directions
            assert alice.run("SELECT current_setting('warp.user_id', true)")[0][0] == "alice"
            assert bob.run("SELECT current_setting('warp.user_id', true)")[0][0] == "bob"
        finally:
            for c in (alice, bob, my_anon):
                c.close()


    def test_rls_identity_does_not_leak_into_mssqlwire_or_after_a_pinned_session(self, rls_stack):
        alice = SqlClient(rls_stack, "pg", user="alice", password="alicepw")
        ms = SqlClient(rls_stack, "mssql", user="bob", password="bobpw")
        try:
            for _ in range(10):
                assert alice.run("SELECT body FROM my_docs") == [("alice-secret",)]
                assert ms.run("SELECT body FROM my_docs") == [("bob-secret",)], "mssqlwire client saw another identity's rows"
            # a pinned session (SET) belonging to alice: her identity must not survive on the connection once she leaves
            alice.run("SET application_name = 'alice_pinned'")
            assert alice.run("SELECT body FROM my_docs") == [("alice-secret",)]
            alice.close()
            time.sleep(0.5)
            anon = SqlClient(rls_stack, "my")
            assert list(anon.run("SELECT body FROM my_docs")) == []
            anon.close()
        finally:
            ms.close()
            alice.close()



# ---- (d) pool exhaustion when every connection is legitimately pinned ------------------------------------------
def _pin_all(stack, proto="pg", n=2):
    pinned = []
    for i in range(n):
        cl = SqlClient(stack, proto, autocommit=False)
        cl.run("INSERT INTO mux_t (id, who) VALUES (%s, 'pin')", (900 + i,))
        pinned.append(cl)
    return pinned


def test_pool_exhaustion_by_pinned_transactions_gives_clear_error_after_wait(mux_stack):
    pinned = _pin_all(mux_stack)
    try:
        assert wait_pool_active(mux_stack, 2, 3) == 2
        # the accept thread and the handshake must not be affected by an exhausted pool (a raw client: drivers such
        # as psycopg2 already run statements while connecting, which legitimately need a backend connection)
        t0 = time.time()
        victim = RawPg(mux_stack.ports["pg"])
        assert time.time() - t0 < 1.0, "connecting must not wait for the pool (handler construction borrows nothing)"
        t0 = time.time()
        rows, err = victim.simple("SELECT 1")
        waited = time.time() - t0
        assert err is not None, "expected the pool-exhaustion error"
        msg = err.get("M", "")
        assert "pool exhausted" in msg and "WARP_POOL_MAX_SIZE" in msg and "waited" in msg, msg
        assert err.get("C") == "53300", err
        assert 1.2 < waited < 4.0, f"expected to wait ~WARP_POOL_CONNECT_TIMEOUT_MS=1500ms, waited {waited:.2f}s"
        victim.close()
    finally:
        for p in pinned:
            p.close()
    assert wait_pool_idle(mux_stack) == 0


@pytest.mark.parametrize("proto", ["my", "mssql", "ora", "bolt"])
def test_pool_exhaustion_error_is_clear_in_every_protocol(mux_stack, proto):
    pinned = _pin_all(mux_stack)
    cl = None
    try:
        assert wait_pool_active(mux_stack, 2, 3) == 2
        cl = mux_client(mux_stack, proto)
        t0 = time.time()
        with pytest.raises(Exception) as ei:
            one_backend_stmt(cl)
        waited = time.time() - t0
        msg = str(ei.value)
        assert "pool exhausted" in msg or "Warp backend" in msg, f"opaque error for {proto}: {msg}"
        assert "WARP_POOL_MAX_SIZE" in msg, msg
        assert waited < 5.0
        print(f"\n[pool exhaustion] {proto}: waited {waited:.2f}s -> {msg[:160]!r}")
    finally:
        if cl is not None:
            cl.close()
        for p in pinned:
            p.close()
    assert wait_pool_idle(mux_stack) == 0


@pytest.fixture(scope="class")
def qos_stack():
    stack = Stack(extra_env={"WARP_POOL_MAX_SIZE": "2", "WARP_POOL_CONNECT_TIMEOUT_MS": "6000",
                             "WARP_QOS_POOL_WAIT_THRESHOLD": "1"})
    try:
        seed_mux_schema(stack)
        yield stack
    finally:
        stack.close()


class TestQosPoolWaitThreshold:

    def test_qos_pool_wait_threshold_rejects_fast_when_someone_already_waits(self, qos_stack):
        first = SqlClient(qos_stack, "pg")   # connected while the pool is still free (drivers run statements on connect)
        second = SqlClient(qos_stack, "pg")
        pinned = _pin_all(qos_stack)
        waiter_done = []
        try:
            assert wait_pool_active(qos_stack, 2, 3) == 2

            def wait_for_connection():
                try:
                    first.run("SELECT 1")
                    waiter_done.append("ok")
                except Exception as e:  # noqa: BLE001
                    waiter_done.append(str(e))

            t = threading.Thread(target=wait_for_connection, daemon=True)
            t.start()
            deadline = time.time() + 4
            while time.time() < deadline and pool_snapshot(qos_stack)[1] < 1:  # one statement now waits for the pool
                time.sleep(0.05)
            assert pool_snapshot(qos_stack)[1] >= 1
            t0 = time.time()
            with pytest.raises(Exception) as ei:
                second.run("SELECT 1")
            rejected_in = time.time() - t0
            assert getattr(ei.value, "pgcode", None) == "53300", (getattr(ei.value, "pgcode", None), str(ei.value))
            assert "saturated" in str(ei.value) and "WARP_QOS_POOL_WAIT_THRESHOLD" in str(ei.value), str(ei.value)
            assert rejected_in < 1.0, f"QoS must reject immediately, took {rejected_in:.2f}s"
            # free a connection: the waiter proceeds
            pinned[0].rollback()
            pinned[0].close()
            t.join(timeout=10)
            assert waiter_done == ["ok"], waiter_done
            first.close()
            second.close()
        finally:
            for p in pinned:
                p.close()



# ---- kill switch ------------------------------------------------------------------------------------------------
@pytest.fixture(scope="class")
def off_stack():
    stack = Stack(extra_env={"WARP_POOL_MAX_SIZE": "2", "WARP_POOL_CONNECT_TIMEOUT_MS": "1500",
                             "WARP_MULTIPLEX_SESSIONS": "false"})
    try:
        seed_mux_schema(stack)
        yield stack
    finally:
        stack.close()


class TestKillSwitch:

    def test_kill_switch_restores_hold_for_session_behaviour(self, off_stack):
        """WARP_MULTIPLEX_SESSIONS=false: every session keeps its first connection until it disconnects (the old model),
        so with pool=2 the third client starves -- and is fine again once a holder leaves."""
        a = SqlClient(off_stack, "pg")
        b = SqlClient(off_stack, "pg")
        try:
            a.run("SELECT 1")
            b.run("SELECT 1")
            assert wait_pool_active(off_stack, 2, 3) == 2, "with multiplexing off an idle session keeps its connection"
            with pytest.raises(Exception) as ei:
                SqlClient(off_stack, "pg").run("SELECT 1")
            assert "pool exhausted" in str(ei.value)
            a.close()
            assert wait_pool_is(off_stack, 1) == 1
            c = SqlClient(off_stack, "pg")
            assert c.run("SELECT 1") == [(1,)]
            c.close()
        finally:
            a.close()
            b.close()



# ---- listener resilience (accept-thread) -------------------------------------------------------------------------
def test_listeners_survive_aborted_connections(mux_stack):
    ports = {p: mux_stack.ports[p] for p in ("pg", "my", "mssql", "ora", "bolt")}
    for name, port in ports.items():
        for _ in range(60):
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))  # RST on close
            try:
                s.connect(("localhost", port))
                s.send(b"\x00\x01garbage")
            except OSError:
                pass
            s.close()
    time.sleep(1.0)
    for proto in ("pg", "my", "mssql", "ora", "bolt"):
        cl = mux_client(mux_stack, proto)
        assert one_backend_stmt(cl), proto
        cl.close()
    assert not re.search(r"wire listener on port \d+ failed|Bolt \(boltwire\) listener on port \d+ failed",
                         mux_stack.warp_log()), "a listener died on aborted connections"


if __name__ == "__main__":
    main()
