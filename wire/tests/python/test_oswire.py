"""End-to-end proof that a real OpenSearch client (opensearch-py, real HTTP/JSON wire protocol)
gets correct results through oswire into a real Postgres backend -- real subprocess, real Postgres
container, no mocks.

REAL GAP, not silently worked around (matching this project's own README.md style): oswire is NOT
a SQL-text protocol -- the OpenSearch REST API always sends fully-structured JSON documents (index
requests) and a structured query DSL (search requests) directly. There is no query-text parse step
and no bind-parameter concept in this protocol. This file tests one real write (index a document
via PUT /<index>/_doc/<id>, per OpenSearchWireServer's own documented V1 surface) and one real read
(GET /<index>/_doc/<id>) with driver-native structured values, and captures RTT for the write.
"""
import os
import time

import pytest
import requests
from opensearchpy import OpenSearch

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
    proc = WarpProcess(postgres, "WARP_OSWIRE_PORT", frontend_name="oswire")
    yield proc
    proc.close()


def client(warp):
    return OpenSearch(
        hosts=[{"host": "localhost", "port": warp.frontend_port}],
        use_ssl=False, verify_certs=False,
    )


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


INDEX = "oswire_it"


# ---------------------------------------------------------------------------
# 3 & 1. One real write (index a document), one real read (fetch it back).
# ---------------------------------------------------------------------------

def test_index_then_get_document(warp):
    c = client(warp)
    c.index(index=INDEX, id="1", body={"name": "alpha", "score": 42})

    resp = c.get(index=INDEX, id="1")
    assert resp["_source"]["name"] == "alpha"
    assert resp["_source"]["score"] == 42


def test_metrics_endpoint_reports_statements(warp):
    c = client(warp)
    c.get(index=INDEX, id="1")
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# RTT capture for the write op (index a document): client-observed AND
# server-side.
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp):
    c = client(warp)

    # 1 warm-up call, not counted
    c.index(index=INDEX, id="warm", body={"val": 0})

    n = 40
    client_times_ms = []
    for i in range(n):
        t0 = time.perf_counter()
        c.index(index=INDEX, id=f"rtt-{i}", body={"val": i})
        client_times_ms.append((time.perf_counter() - t0) * 1000.0)

    client_times_ms.sort()
    client_p50 = client_times_ms[len(client_times_ms) // 2]
    client_min = client_times_ms[0]
    client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

    summary = metrics_summary(warp)
    # oswire's SqlMetricsCollector fingerprint is per-endpoint-shape (e.g. "_doc"), not
    # per-index -- confirmed live via a first run's topSql dump.
    entry = None
    for s in summary.get("topSql", []):
        if s.get("sql", "") == "_doc":
            entry = s
            break
    assert entry is not None, f"no topSql entry for '_doc'; topSql={summary.get('topSql')}"
    server_avg_rtt_ms = entry["avgRttMs"]
    assert server_avg_rtt_ms is not None

    print(f"\n[oswire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
          f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

    assert server_avg_rtt_ms < 2.0, (
        f"server-side avg RTT {server_avg_rtt_ms}ms for oswire document index exceeds the expected "
        f"sub-2ms baseline for a loopback Postgres call behind the JSON translation layer"
    )
