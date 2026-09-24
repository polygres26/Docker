"""End-to-end proof that a real gRPC client (grpcio, real protobuf-framed HTTP/2 wire protocol)
gets correct results through Warp's own native QueryService into a real Postgres backend -- real
subprocess, real Postgres container, no mocks.

Unlike mongowire/dynamowire/sqswire/oswire/influxwire, warp.proto's ExecuteRequest (see
src/main/proto/warp.proto) carries a real, separate `sql` text field AND a real `params` field
(positional bind values, text-encoded) -- so, like pgwire/boltwire, both a literal-inlined-in-SQL-
text form and a real bind-parameter form are genuinely distinct wire shapes here, and both are
tested for read and write below.

Python stubs (warp_pb2.py / warp_pb2_grpc.py in this directory) are generated at test-authoring
time via `python -m grpc_tools.protoc -I<repo>/src/main/proto --python_out=. --grpc_python_out=.
warp.proto` run from tests/python/ -- no repo python stub previously existed. The generated files
are checked in alongside this test (not regenerated on every run) to avoid adding a protoc
toolchain dependency to routine `pytest` runs; regenerate them if warp.proto changes.
"""
import os
import time

import grpc
import pytest
import requests

import warp_pb2
import warp_pb2_grpc

from polywire_support import WarpProcess, RealPostgres

ADMIN_TOKEN = "warp-polywire-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    # gRPC's port is WARP_GRPC_PORT, always allocated by WarpProcess itself (see
    # polywire_support.py) -- there's no separate frontend_env_var to pass here since every
    # WarpProcess already starts the native gRPC QueryService.
    proc = WarpProcess(postgres, "WARP_PGWIRE_PORT", frontend_name="grpc")
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def grpc_channel(warp):
    # Real bug, found live investigating why gRPC's client-observed p50 (2.0-2.4ms) was 2x+
    # pgwire's (0.94-1.00ms) despite gRPC's own server-side avg RTT already being a healthy ~1ms:
    # this fixture used to be a bare `stub(warp)` helper that called `grpc.insecure_channel(...)`
    # freshly INSIDE every single execute() call -- opening a brand-new TCP connection and paying
    # a full HTTP/2 connection preface (SETTINGS frame round trip) on every RPC, instead of the one
    # persistent HTTP/2 connection any real long-lived gRPC client (and grpcio's own channel
    # object) is designed to amortize across many calls. That handshake cost, not anything in
    # Warp's own QueryServiceImpl/StatementPipeline path, was the real gap versus pgwire (whose
    # test client, like a real Postgres client, opens one TCP connection per session and reuses
    # it for every statement). A single module-scoped channel, reused by every call below, matches
    # how every real gRPC client actually behaves.
    channel = grpc.insecure_channel(f"localhost:{warp.grpc_port}")
    grpc.channel_ready_future(channel).result(timeout=10)
    yield channel
    channel.close()


@pytest.fixture(scope="module")
def grpc_stub(grpc_channel):
    return warp_pb2_grpc.QueryServiceStub(grpc_channel)


def execute(warp, sql, params=None, stub=None):
    request = warp_pb2.ExecuteRequest(
        username="postgres", password="postgres", sql=sql, params=params or [],
    )
    if stub is None:
        # Fallback for any call site that hasn't been updated to pass the shared stub -- opens its
        # own channel like before, just for that one call. Every test below passes the real
        # module-scoped stub instead so this path isn't actually exercised in the RTT-sensitive test.
        channel = grpc.insecure_channel(f"localhost:{warp.grpc_port}")
        stub = warp_pb2_grpc.QueryServiceStub(channel)
    response = stub.Execute(request, timeout=5)
    assert response.success, f"gRPC Execute failed: {response.error_message}"
    return response


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 1. Simple read, literal value
# ---------------------------------------------------------------------------

def test_read_literal(warp, grpc_stub):
    resp = execute(warp, "SELECT 21 * 2 AS answer", stub=grpc_stub)
    assert resp.is_query
    assert resp.rows[0].values[0] == "42"


# ---------------------------------------------------------------------------
# 2. Same read, bind parameter
# ---------------------------------------------------------------------------

def test_read_bind_parameter(warp, grpc_stub):
    # Warp's native gRPC dialect (SourceDialect.WARP_NATIVE) does no dialect translation of its
    # own -- confirmed by grepping DialectTranslationStage.java, which has no WARP_NATIVE case --
    # so the SQL text is handed straight to a real JDBC PreparedStatement, meaning placeholders are
    # plain JDBC "?" positional markers, not Postgres-native "$1" (confirmed live: "$1::int" fails
    # with "column index out of range: 1, number of columns: 0" -- Postgres's own driver never
    # sees a "$1" to bind because the JDBC layer parameterizes on "?").
    resp = execute(warp, "SELECT ? * 2 AS answer", params=["21"], stub=grpc_stub)
    assert resp.is_query
    assert resp.rows[0].values[0] == "42"


# ---------------------------------------------------------------------------
# 3. Write, literal value
# ---------------------------------------------------------------------------

def test_write_literal(warp, grpc_stub):
    execute(warp, "CREATE TABLE grpc_it (id INT PRIMARY KEY, name VARCHAR(50))", stub=grpc_stub)
    try:
        execute(warp, "INSERT INTO grpc_it (id, name) VALUES (1, 'alpha')", stub=grpc_stub)
        resp = execute(warp, "SELECT name FROM grpc_it WHERE id = 1", stub=grpc_stub)
        assert resp.rows[0].values[0] == "alpha"
    finally:
        execute(warp, "DROP TABLE IF EXISTS grpc_it", stub=grpc_stub)


# ---------------------------------------------------------------------------
# 4. Write, bind parameter
# ---------------------------------------------------------------------------

def test_write_bind_parameter(warp, grpc_stub):
    execute(warp, "CREATE TABLE grpc_it_bind (id INT PRIMARY KEY, name VARCHAR(50))", stub=grpc_stub)
    try:
        execute(warp, "INSERT INTO grpc_it_bind (id, name) VALUES (?, ?)", params=["2", "beta"], stub=grpc_stub)
        resp = execute(warp, "SELECT name FROM grpc_it_bind WHERE id = ?", params=["2"], stub=grpc_stub)
        assert resp.rows[0].values[0] == "beta"
    finally:
        execute(warp, "DROP TABLE IF EXISTS grpc_it_bind", stub=grpc_stub)


def test_metrics_endpoint_reports_statements(warp, grpc_stub):
    execute(warp, "SELECT 1", stub=grpc_stub)
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# RTT capture for the write op: client-observed AND server-side.
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp, grpc_stub):
    execute(warp, "CREATE TABLE grpc_rtt (id INT PRIMARY KEY, val INT)", stub=grpc_stub)
    try:
        insert_sql = "INSERT INTO grpc_rtt (id, val) VALUES (?, ?) " \
                     "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"

        # 1 warm-up call, not counted
        execute(warp, insert_sql, params=["0", "0"], stub=grpc_stub)

        n = 40
        client_times_ms = []
        for i in range(n):
            t0 = time.perf_counter()
            execute(warp, insert_sql, params=[str(i), str(i)], stub=grpc_stub)
            client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = None
        for s in summary.get("topSql", []):
            if "grpc_rtt" in s.get("sql", ""):
                entry = s
                break
        assert entry is not None, f"no topSql entry for grpc_rtt insert; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None

        print(f"\n[gRPC write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        # First actual measurement taken for this protocol (docs/PERFORMANCE.md does not exist in
        # this repo snapshot -- see docs/RTT_BASELINE_2026.md for the first-ever baseline this run
        # establishes). <5ms is the reasoned bar for a trivial single-row upsert against Postgres
        # over loopback, matching pgwire's own bar since gRPC's QueryService goes through the same
        # StatementPipeline/JDBC path.
        assert server_avg_rtt_ms < 5.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for gRPC write is well above the expected "
            f"sub-millisecond-to-low-single-digit-ms range for a loopback Postgres call"
        )
    finally:
        execute(warp, "DROP TABLE IF EXISTS grpc_rtt", stub=grpc_stub)
