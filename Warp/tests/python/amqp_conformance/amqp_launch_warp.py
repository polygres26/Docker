"""Starts Warp (WARP_TEST_JAR) on local Postgres process(es) with the amqpwire frontend; 1 or N sharded backends."""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")
os.environ.setdefault("WARP_ADMIN_TOKEN", "warp-test-admin-token")
import requests  # noqa: E402
from warp_test_support import RealPostgres, WarpProcess, isolated_ports  # noqa: E402

# embedded Ignite membership can take 40+ s to join on a loaded machine; the shared default wait is 30 s
WarpProcess._wait_ready.__defaults__ = (150,)
ADMIN = os.environ["WARP_ADMIN_TOKEN"]
STORES = ["amqp"]


def first_free_ignite_port():
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port")


class AmqpWarp:
    """Warp with the amqp store enabled on `shards` real Postgres servers (or the given `pgs`). AMQP 0-9-1 on .port;
    extra_env adds Warp environment variables (WARP_AMQPWIRE_AUTH, WARP_AMQPWIRE_VHOSTS, ...)."""

    def __init__(self, shards=1, extra_env=None, pgs=None, default_store=True):
        self.pgs = pgs or [RealPostgres() for _ in range(shards)]
        env = {**isolated_ports("WARP_AMQPWIRE_PORT"),
               "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
               "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_ignite_port()}", "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
               "WARP_AMQPWIRE_SWEEP_MS": "200", "WARP_AMQPWIRE_POLL_MS": "100", **(extra_env or {})}
        self.env = env
        self.mcp_port = int(env["WARP_MCP_PORT"])
        try:
            self.proc = WarpProcess(self.pgs[0], "WARP_AMQPWIRE_PORT", frontend_name="amqpwire", extra_env=env)
        except BaseException:
            for pg in self.pgs:
                pg.close()
            raise
        self.port = self.proc.frontend_port

        def api(method, path, body=None, expect=200):
            r = requests.request(method, f"http://localhost:{self.proc.metrics_port}{path}", json=body, timeout=60,
                                 headers={"Authorization": f"Bearer {ADMIN}"})
            assert r.status_code == expect, (r.status_code, r.text)
            return r.json()

        self.api = api
        if not default_store:
            return
        api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": STORES})
        for i, pg in enumerate(self.pgs[1:], start=2):
            api("POST", "/api/backend-sets/default/backends", {
                "name": f"pg{i}", "url": f"jdbc:postgresql://localhost:{pg.port}/postgres", "user": "postgres",
                "password": "postgres", "enabledStores": STORES}, expect=201)

    def restart(self, graceful=True):
        """Stops Warp and starts a new process on the same Postgres servers (the store and the queues are in Postgres)."""
        if graceful:
            self.proc.close()
        else:
            self.proc.process.kill()
            self.proc.process.wait()
        self.proc = WarpProcess(self.pgs[0], "WARP_AMQPWIRE_PORT", frontend_name="amqpwire", extra_env=self.env)
        self.port = self.proc.frontend_port
        return self.port

    def close(self):
        self.proc.close()
        for pg in self.pgs:
            pg.close()
