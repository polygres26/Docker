#!/usr/bin/env python3
"""Start a throwaway Warp with oswire on free ports and 1 or 2 local Postgres backends (2 = one backend set,
the `opensearch` store sharded across both). Prints/writes {"url", "pids"} and stays up until SIGTERM/SIGINT.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 launch_warp.py --state state.json [--shards 2]
"""
import argparse, json, os, signal, subprocess, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import requests  # noqa: E402
from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # noqa: E402

TOKEN = "warp-test-admin-token"


def first_free_discovery_port():
    for p in range(47500, 47600):
        r = subprocess.run(["lsof", "-nP", f"-iTCP:{p}", "-sTCP:LISTEN"], capture_output=True, text=True)
        if not r.stdout.strip():
            return p
    raise RuntimeError("no free Ignite discovery port")


class Stack:
    """Importable: `with Stack(shards=2) as s: s.url`."""

    def __init__(self, shards=1):
        os.environ["WARP_ADMIN_TOKEN"] = TOKEN
        self.pgs = [RealPostgres() for _ in range(shards)]
        self.warp = None
        try:
            env = isolated_ports(exclude="WARP_OSWIRE_PORT")
            env.update({"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
                        "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_discovery_port()}",
                        "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})
            self.warp = WarpProcess(self.pgs[0], "WARP_OSWIRE_PORT", frontend_name="oswire", extra_env=env)
            self.url = f"http://localhost:{self.warp.frontend_port}"
            self.metrics_port = self.warp.metrics_port
            if shards > 1:
                self._shard()
        except BaseException:
            self.close()
            raise

    def api(self, method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{self.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    def _shard(self):
        self.api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["opensearch"]})
        for i, pg in enumerate(self.pgs[1:], start=2):
            self.api("POST", "/api/backend-sets/default/backends", {
                "name": f"pg{i}", "url": f"jdbc:postgresql://localhost:{pg.port}/postgres", "user": "postgres",
                "password": "postgres", "enabledStores": ["opensearch"]}, expect=201)

    def close(self):
        for o in [self.warp, *self.pgs]:
            try:
                o and o.close()
            except Exception:  # noqa: BLE001
                pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--shards", type=int, default=1)
    a = ap.parse_args()
    stack = None

    def cleanup(*_):
        stack and stack.close()
        sys.exit(0)
    signal.signal(signal.SIGTERM, cleanup); signal.signal(signal.SIGINT, cleanup)
    stack = Stack(a.shards)
    json.dump({"url": stack.url, "pid": os.getpid(), "warp_pid": stack.warp.process.pid}, open(a.state, "w"))
    print(json.dumps({"url": stack.url}), flush=True)
    while stack.warp.process.poll() is None:
        time.sleep(1)
    print("warp exited", "".join(stack.warp._output_lines[-30:]), file=sys.stderr)
    cleanup()


if __name__ == "__main__":
    main()
