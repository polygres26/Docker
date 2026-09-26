"""Fixtures helpers for the awswire tests (SNS, Kinesis, Secrets Manager, SSM, KMS, STS and the unified AWS endpoint): a real Warp
process plus one or two real native Postgres servers. No mocks."""
import socket

import psycopg2
import requests

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

AWS_STORES = ["sns", "kinesis", "awsparams"]


def first_free_ignite_port():
    # Ignite binds the first port it CAN bind (0.0.0.0) and must find that same port in its seed list: test bindability,
    # not connectability (a stray JVM on another interface makes connect_ex look free).
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port in 47500..47599")


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def start_aws_warp(pg, second_pg=None, extra=None, stores=None):
    """Warp with the unified AWS endpoint on `awswire_port` and the sns/kinesis/awsparams stores enabled on `pg` (and
    `second_pg`, sharding across both). KMS runs with the insecure development key unless `extra` sets WARP_KMS_MASTER_KEY."""
    stores = stores or AWS_STORES
    env = {**isolated_ports("WARP_AWSWIRE_PORT"), "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
           "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
           "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_ignite_port()}", "WARP_KINESISWIRE_SWEEP_SECONDS": "1",
           "WARP_KMS_INSECURE_DEV_KEY": "true", "WARP_ADMIN_TOKEN": ADMIN_TOKEN}
    env.update(extra or {})
    proc = WarpProcess(pg, "WARP_AWSWIRE_PORT", frontend_name="awswire", extra_env=env)

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": stores})
    if second_pg is not None:
        api("POST", "/api/backend-sets/default/backends", {"name": "pg2", "url": pg_url(second_pg), "user": "postgres",
                                                          "password": "postgres", "enabledStores": stores}, expect=201)
    proc.api = api
    proc.endpoint = f"http://localhost:{proc.frontend_port}"
    proc.sqs_port = int(env["WARP_SQSWIRE_PORT"]) if "WARP_SQSWIRE_PORT" in env else None
    return proc
