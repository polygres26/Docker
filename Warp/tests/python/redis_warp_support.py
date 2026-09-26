"""Shared fixtures helpers for the rediswire tests: real Warp process(es) + real native Postgres, no mocks."""
import socket
import subprocess

import psycopg2
import requests

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports


def first_free_ignite_port():
    # Ignite binds the first port it CAN bind (0.0.0.0) and must find that same port in its seed list, so test
    # bindability, not connectability: a listener on another interface (or a stray JVM) makes connect_ex look free.
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


def start_warp(pg, extra=None, second_pg=None):
    """Warp with the redis store enabled on `pg` (and `second_pg`, sharding across both)."""
    env = {**isolated_ports("WARP_REDISWIRE_PORT"), "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_REDISWIRE_ENABLED": "true",
           "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
           "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_ignite_port()}", "WARP_REDISWIRE_SWEEP_MS": "200",
           "WARP_ADMIN_TOKEN": ADMIN_TOKEN}
    env.update(extra or {})
    proc = WarpProcess(pg, "WARP_REDISWIRE_PORT", frontend_name="rediswire", extra_env=env)

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["redis"]})
    if second_pg is not None:
        api("POST", "/api/backend-sets/default/backends", {"name": "pg2", "url": pg_url(second_pg), "user": "postgres",
                                                          "password": "postgres", "enabledStores": ["redis"]}, expect=201)
    proc.api = api
    return proc
