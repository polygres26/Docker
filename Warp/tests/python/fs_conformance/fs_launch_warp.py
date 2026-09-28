"""Starts Warp (WARP_TEST_JAR) on local Postgres process(es) with the firestorewire frontend; 1 or N sharded backends.

  python3 fs_launch_warp.py [SHARDS]      # prints the Firestore host:port and sleeps (Ctrl-C stops Warp and Postgres)
"""
import os
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")
os.environ.setdefault("WARP_ADMIN_TOKEN", "warp-test-admin-token")
import requests  # noqa: E402
from warp_test_support import RealPostgres, WarpProcess, isolated_ports  # noqa: E402

ADMIN = os.environ["WARP_ADMIN_TOKEN"]
# a loaded machine (other JVMs, native Postgres servers) can take longer than the default 30 s to bring Warp up
WarpProcess._wait_ready.__defaults__ = (int(os.environ.get("WARP_TEST_READY_TIMEOUT", "120")),)
STORES = ["firestore"]


def first_free_ignite_port():
    # Ignite binds the first port it CAN bind (0.0.0.0) and must find that same port in its seed list
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port")


class FsWarp:
    def __init__(self, shards=1, extra_env=None, pgs=None, store_env="WARP_FIRESTOREWIRE_PORT", stores=None):
        self.pgs = []
        try:
            self._start(shards, extra_env, pgs, store_env, stores)
        except BaseException:
            for pg in self.pgs:
                pg.close()  # never leak a native Postgres server (each holds a System V shared-memory segment)
            raise

    def _start(self, shards, extra_env, pgs, store_env, stores):
        self.pgs = pgs or []
        while len(self.pgs) < shards:
            self.pgs.append(RealPostgres())
        self.stores = stores or STORES
        env = {**isolated_ports(store_env), "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
               "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_ignite_port()}", "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
               **(extra_env or {})}
        self.env = env
        self.proc = WarpProcess(self.pgs[0], store_env, frontend_name="firestorewire", extra_env=env)
        self.port = self.proc.frontend_port
        self.host = f"localhost:{self.port}"
        self.url = f"http://{self.host}"

        def api(method, path, body=None, expect=200):
            r = requests.request(method, f"http://localhost:{self.proc.metrics_port}{path}", json=body, timeout=60,
                                 headers={"Authorization": f"Bearer {ADMIN}"})
            assert r.status_code == expect, (r.status_code, r.text)
            return r.json()

        self.api = api
        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": self.stores})
        for i, pg in enumerate(self.pgs[1:], start=2):
            api("POST", "/api/backend-sets/default/backends", {
                "name": f"pg{i}", "url": f"jdbc:postgresql://localhost:{pg.port}/postgres", "user": "postgres",
                "password": "postgres", "enabledStores": self.stores}, expect=201)

    def close(self):
        self.proc.close()
        for pg in self.pgs:
            pg.close()


if __name__ == "__main__":
    import signal
    import time
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    w = FsWarp(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
    try:
        print(w.host, flush=True)
        time.sleep(3600)
    finally:
        w.close()
