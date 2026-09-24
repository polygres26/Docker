"""End-to-end proof that a real Amazon SQS client (boto3, real SQS HTTP/JSON wire protocol) gets
correct results through sqswire into a real Postgres backend -- real subprocess, real Postgres
container, no mocks.

REAL GAP, not silently worked around (matching this project's own README.md style): sqswire is NOT
a SQL-text protocol -- boto3's SQS HTTP/JSON wire protocol always sends structured message bodies
directly. There is no query-text parse step and no bind-parameter concept, unlike pgwire/boltwire.
This file tests one real write (SendMessage, the enqueue side) and one real read (ReceiveMessage,
the dequeue side) with driver-native structured values, and captures RTT for the write.
"""
import os
import time

import boto3
import pytest
import requests
from botocore.config import Config

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
    proc = WarpProcess(postgres, "WARP_SQSWIRE_PORT", frontend_name="sqswire")
    yield proc
    proc.close()


def client(warp):
    return boto3.client(
        "sqs",
        endpoint_url=f"http://localhost:{warp.frontend_port}",
        region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 0}),
    )


def metrics_summary(warp):
    resp = requests.get(
        f"http://localhost:{warp.metrics_port}/api/metrics/summary",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="module")
def queue_url(warp):
    c = client(warp)
    resp = c.create_queue(QueueName="sqswire-it")
    return resp["QueueUrl"]


# ---------------------------------------------------------------------------
# 3 & 1. One real write (SendMessage), one real read (ReceiveMessage).
# ---------------------------------------------------------------------------

def test_send_then_receive_message(warp, queue_url):
    c = client(warp)
    c.send_message(QueueUrl=queue_url, MessageBody="hello-alpha")

    resp = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    messages = resp.get("Messages", [])
    assert len(messages) == 1
    assert messages[0]["Body"] == "hello-alpha"


def test_metrics_endpoint_reports_statements(warp, queue_url):
    c = client(warp)
    c.send_message(QueueUrl=queue_url, MessageBody="metrics-probe")
    body = warp.metrics_text()
    assert "warp_statements_total" in body


# ---------------------------------------------------------------------------
# RTT capture for the write op (SendMessage): client-observed AND server-side.
# ---------------------------------------------------------------------------

def test_write_rtt_baseline(warp, queue_url):
    c = client(warp)

    # 1 warm-up call, not counted
    c.send_message(QueueUrl=queue_url, MessageBody="warm")

    n = 40
    client_times_ms = []
    for i in range(n):
        t0 = time.perf_counter()
        c.send_message(QueueUrl=queue_url, MessageBody=f"rtt-{i}")
        client_times_ms.append((time.perf_counter() - t0) * 1000.0)

    client_times_ms.sort()
    client_p50 = client_times_ms[len(client_times_ms) // 2]
    client_min = client_times_ms[0]
    client_p90 = client_times_ms[int(len(client_times_ms) * 0.9)]

    summary = metrics_summary(warp)
    entry = None
    for s in summary.get("topSql", []):
        if "SendMessage" in s.get("sql", ""):
            entry = s
            break
    assert entry is not None, f"no topSql entry for SendMessage; topSql={summary.get('topSql')}"
    server_avg_rtt_ms = entry["avgRttMs"]
    assert server_avg_rtt_ms is not None

    print(f"\n[sqswire write RTT] client min={client_min:.3f}ms p50={client_p50:.3f}ms "
          f"p90={client_p90:.3f}ms | server avgRttMs={server_avg_rtt_ms}")

    assert server_avg_rtt_ms < 2.0, (
        f"server-side avg RTT {server_avg_rtt_ms}ms for sqswire SendMessage exceeds the expected "
        f"sub-2ms baseline for a loopback Postgres call behind the JSON translation layer"
    )
