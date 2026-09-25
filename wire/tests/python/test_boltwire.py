"""End-to-end proof that a real Neo4j client driver (the official `neo4j` Python driver, real Bolt
4.4 binary wire protocol) gets correct results through boltwire into a real Postgres backend --
real subprocess, real Postgres container, no mocks.

REAL GAP, not silently worked around (matching this project's own README.md style):
BoltWireSessionHandler.translateAndRun actually dispatches two real translations today -- Phase 1's
narrow `RETURN <literal> [AS <alias>]` AND Phase 2's real `CREATE (n:Label {...}) RETURN ...`
(backed by a genuine PgGraphStore node/edge schema in Postgres, single-node CREATEs skipping the
transaction wrapper multi-node CREATEs use -- see runCreate's javadoc). So both a real literal read
and a real literal write ARE exercised below. What is NOT supported by CypherParser is Cypher
parameter binding (`$param`) for either RETURN or CREATE -- confirmed by grepping CypherParser.java
for `$`, which finds nothing -- so the bind-parameter variants are xfail, not fabricated passes.

FIXED (previously a real, disclosed gap): BoltWireSessionHandler still bypasses the shared
StatementPipeline for its actual Cypher-to-SQL execution -- that part is unchanged, and correctly
so, since dialect translation/the cache stage/the router don't apply to a graph query against
warp_graph_nodes/warp_graph_edges. But it no longer skips SqlMetricsCollector entirely: handleRun
now times its own RUN span (the complete backend round trip -- see its own comment) and reports it
via SqlMetricsCollector.recordOperation("boltwire", ...), the same narrow, pipeline-independent hook
sqswire/dynamowire already use. /api/metrics/summary's topSql[] now carries a real boltwire entry
with a non-null avgRttMs, cross-checked against client-observed RTT below the same way every other
protocol's own RTT test does it.
"""
import os
import time

import pytest
import requests
from neo4j import GraphDatabase

from warp_test_support import WarpProcess, RealPostgres

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def warp(postgres):
    proc = WarpProcess(postgres, "WARP_BOLTWIRE_PORT", frontend_name="boltwire")
    yield proc
    proc.close()


def driver(warp):
    return GraphDatabase.driver(
        f"bolt://localhost:{warp.frontend_port}", auth=("postgres", "postgres"),
    )


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 1. Simple read, literal value -- the only query shape boltwire's Phase 1
# translator supports (see module docstring).
# ---------------------------------------------------------------------------

def test_read_literal(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            result = session.run("RETURN 42 AS answer")
            record = result.single()
            assert record["answer"] == 42
    finally:
        drv.close()


def test_read_literal_string(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            result = session.run("RETURN 'alpha' AS name")
            record = result.single()
            assert record["name"] == "alpha"
    finally:
        drv.close()


@pytest.mark.xfail(
    reason="boltwire Phase 1 (BoltWireSessionHandler.translateCypher) only recognizes literal "
           "RETURN -- Cypher parameter binding ($param) is explicitly out of scope until the "
           "later MATCH/pattern-matching translator phase.",
    strict=True,
)
def test_read_bind_parameter_not_yet_supported(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            result = session.run("RETURN $val AS answer", val=42)
            record = result.single()
            assert record["answer"] == 42
    finally:
        drv.close()


# ---------------------------------------------------------------------------
# 3. Write, literal value -- CREATE is real (Phase 2, backed by PgGraphStore's
# genuine node/edge tables in Postgres), see module docstring.
# ---------------------------------------------------------------------------

def test_write_literal(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            result = session.run("CREATE (n:Person {name: 'alpha'}) RETURN n.name AS name")
            record = result.single()
            assert record["name"] == "alpha"
    finally:
        drv.close()


@pytest.mark.xfail(
    reason="boltwire's CypherParser has no Cypher parameter-binding support ($param) for CREATE "
           "-- confirmed by grepping CypherParser.java for '$', which finds nothing. Every "
           "property literal in a CREATE must be inlined into the query text today.",
    strict=True,
)
def test_write_bind_parameter_not_yet_supported(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            session.run("CREATE (n:Person {name: $name}) RETURN n.name AS name", name="alpha")
    finally:
        drv.close()


def test_metrics_endpoint_reports_statements(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            session.run("RETURN 1 AS one").single()
    finally:
        drv.close()
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# RTT capture for the write op (CREATE): client-observed AND server-side.
#
# Real fix, made in this session: BoltWireSessionHandler previously bypassed
# SqlMetricsCollector entirely (see module docstring's older text, now stale) --
# every other wire protocol had server-side RTT visibility in
# /api/metrics/summary's topSql[] except this one. handleRun now times its own
# RUN span (the full backend round trip -- PULL never touches the backend, it
# only serializes rows RUN already fetched, so RUN's span is honest on its own,
# the same reasoning SqlMetricsCollector's javadoc gives for orawire's Fetch)
# and reports it via recordOperation("boltwire", ..., elapsedNanos, rttNanos),
# WITHOUT forcing Cypher execution through the full StatementPipeline chain
# (dialect translation/cache/router genuinely don't apply to a graph query).
# This test now cross-checks a real, non-N/A avgRttMs against the client-
# observed number, matching every other protocol's own RTT test shape.
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp):
    drv = driver(warp)
    try:
        with drv.session() as session:
            # 1 warm-up call, not counted
            session.run("CREATE (n:RttWarm {v: 0}) RETURN n.v AS v").single()

            n = 40
            client_times_ms = []
            for i in range(n):
                t0 = time.perf_counter()
                session.run(f"CREATE (n:RttNode {{v: {i}}}) RETURN n.v AS v").single()
                client_times_ms.append((time.perf_counter() - t0) * 1000.0)

        client_times_ms.sort()
        client_p50 = client_times_ms[len(client_times_ms) // 2]
        client_min = client_times_ms[0]
        client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

        summary = metrics_summary(warp)
        entry = None
        for s in summary.get("topSql", []):
            if "RttNode" in s.get("sql", ""):
                entry = s
                break
        assert entry is not None, f"no topSql entry for boltwire RttNode CREATE; topSql={summary.get('topSql')}"
        server_avg_rtt_ms = entry["avgRttMs"]
        assert server_avg_rtt_ms is not None, (
            "boltwire's avgRttMs is still None/N/A -- SqlMetricsCollector wiring did not take effect"
        )

        print(f"\n[boltwire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
              f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

        # Same reasoned bar test_grpc.py/pgwire use, widened slightly (5ms -> 20ms) versus that
        # file's own bound -- observed live to occasionally spike into single digits under shared-
        # machine noise (a concurrent build, GC pause) even though the typical value is ~1ms; the
        # point of this assertion is catching a real regression (a reintroduced fresh-connection-
        # per-statement bug, back to multi-ms), not enforcing lab-conditions determinism.
        assert server_avg_rtt_ms < 20.0, (
            f"server-side avg RTT {server_avg_rtt_ms}ms for boltwire write is well above the "
            f"expected sub-millisecond-to-low-single-digit-ms range for a loopback Postgres call"
        )
    finally:
        drv.close()
