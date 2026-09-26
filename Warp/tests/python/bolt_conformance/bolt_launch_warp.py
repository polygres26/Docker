#!/usr/bin/env python3
"""Start a throwaway Warp with boltwire on free ports and one local Postgres backend (Neo4j is one backend per set).
Prints/writes {"uri", "pid"} and stays up until SIGTERM/SIGINT.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 bolt_launch_warp.py --state state.json
Importable: `with Stack() as s: s.uri`.
"""
import argparse, json, os, signal, subprocess, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # noqa: E402

TOKEN = "warp-test-admin-token"


def first_free_discovery_port():
    for p in range(47500, 47600):
        r = subprocess.run(["lsof", "-nP", f"-iTCP:{p}", "-sTCP:LISTEN"], capture_output=True, text=True)
        if not r.stdout.strip():
            return p
    raise RuntimeError("no free Ignite discovery port")


class Stack:
    def __init__(self, extra_env=None):
        os.environ["WARP_ADMIN_TOKEN"] = TOKEN
        self.pg = RealPostgres()
        self.warp = None
        try:
            env = isolated_ports(exclude="WARP_BOLTWIRE_PORT")
            env.update({"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
                        "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_discovery_port()}",
                        "WARP_TRUSTED_BACKEND_HOSTS": "localhost"})
            env.update(extra_env or {})
            self.warp = WarpProcess(self.pg, "WARP_BOLTWIRE_PORT", frontend_name="boltwire", extra_env=env)
            self.uri = f"bolt://localhost:{self.warp.frontend_port}"
            self.metrics_port = self.warp.metrics_port
        except BaseException:
            self.close()
            raise

    def close(self):
        for o in [self.warp, self.pg]:
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
    a = ap.parse_args()
    stack = None

    def cleanup(*_):
        stack and stack.close()
        sys.exit(0)
    signal.signal(signal.SIGTERM, cleanup); signal.signal(signal.SIGINT, cleanup)
    stack = Stack()
    json.dump({"uri": stack.uri, "pid": os.getpid(), "warp_pid": stack.warp.process.pid,
               "metrics_port": stack.metrics_port}, open(a.state, "w"))
    print(json.dumps({"uri": stack.uri}), flush=True)
    while stack.warp.process.poll() is None:
        time.sleep(1)
    print("warp exited", "".join(stack.warp._output_lines[-30:]), file=sys.stderr)
    cleanup()


if __name__ == "__main__":
    main()
