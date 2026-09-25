"""Starts Warp (WARP_TEST_JAR) on local Postgres process(es) with the influxwire frontend; 1 or N sharded backends."""
import os
import socket
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")
os.environ.setdefault("WARP_ADMIN_TOKEN", "warp-test-admin-token")
import requests  # noqa: E402
from warp_test_support import RealPostgres, WarpProcess, isolated_ports  # noqa: E402

ADMIN = os.environ["WARP_ADMIN_TOKEN"]


def first_free_seed():
    for p in range(47500, 47600):
        if socket.socket().connect_ex(("127.0.0.1", p)) != 0:
            return p
    raise RuntimeError("no free ignite port")


def start_warp(shards=1, extra_env=None):
    pgs = [RealPostgres() for _ in range(shards)]
    env = {"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
           "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_seed()}", "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
           **(extra_env or {})}
    env.setdefault("WARP_INFLUXWIRE_STRICT_DB", "true")
    proc = WarpProcess(pgs[0], "WARP_INFLUXWIRE_PORT", frontend_name="influxwire", extra_env=env)

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    if shards > 1:
        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["influxdb"]})
        for i, pg in enumerate(pgs[1:], start=2):
            api("POST", "/api/backend-sets/default/backends", {
                "name": f"pg{i}", "url": f"jdbc:postgresql://localhost:{pg.port}/postgres", "user": "postgres",
                "password": "postgres", "enabledStores": ["influxdb"]}, expect=201)

    def stop():
        proc.close()
        for pg in pgs:
            pg.close()

    proc.pgs = pgs
    start_warp.last = proc
    return f"http://localhost:{proc.frontend_port}", stop
