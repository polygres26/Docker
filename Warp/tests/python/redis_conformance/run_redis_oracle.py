#!/usr/bin/env python3
"""Runs the rediswire differential corpus against a REAL Redis container (the oracle) and/or a Warp/rediswire endpoint.

  run_redis_oracle.py record [--image redis:7]            start a Redis container, record answers into golden/redis_golden.json.gz
  run_redis_oracle.py diff --warp HOST:PORT [--case X]    run corpus against the container and Warp side by side, print mismatches
  run_redis_oracle.py replay --warp HOST:PORT             compare Warp with the golden file (no Docker)

The container is started with an ephemeral host port and always removed (`docker rm -f -v`).
"""
import argparse
import json
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import redis_diff as D  # noqa: E402


def free_port():
    import socket
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class Oracle:
    def __init__(self, image="redis:7"):
        self.name = f"warp-redis-oracle-{uuid.uuid4().hex[:8]}"
        self.port = free_port()
        subprocess.run(["docker", "run", "-d", "--name", self.name, "-p", f"{self.port}:6379", image], check=True, capture_output=True)
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                D.Resp("127.0.0.1", self.port, timeout=2).execute("PING")
                return
            except OSError:
                time.sleep(0.2)
        self.close()
        raise TimeoutError("redis container did not start")

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True)


def hostport(s):
    h, _, p = s.rpartition(":")
    return h or "127.0.0.1", int(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["record", "diff", "replay"])
    ap.add_argument("--image", default="redis:7")
    ap.add_argument("--warp")
    ap.add_argument("--oracle", help="use an already running Redis HOST:PORT instead of starting a container")
    ap.add_argument("--case", action="append", help="corpus file name(s) without .txt")
    ap.add_argument("--only", help="substring of the case name")
    args = ap.parse_args()
    cases = D.load_corpus(args.case)
    if args.only:
        cases = [c for c in cases if args.only in c[0]]
    steps_by_name = {c[0]: c[2] for c in cases}
    oracle = None
    try:
        if args.mode in ("record", "diff"):
            if args.oracle:
                h, p = hostport(args.oracle)
            else:
                oracle = Oracle(args.image)
                h, p = "127.0.0.1", oracle.port
            r = D.Runner(h, p)
            golden = D.record(r, cases)
            r.close()
            if args.mode == "record":
                if args.case or args.only:
                    try:
                        merged = D.load_golden()
                    except OSError:
                        merged = {}
                    merged.update(golden)
                    golden = merged
                D.save_golden(golden)
                print(f"recorded {len(golden)} cases into {D.GOLDEN}")
                return 0
        else:
            golden = D.load_golden()
        h, p = hostport(args.warp)
        w = D.Runner(h, p)
        problems = []
        total = 0
        for name, resp3, steps in cases:
            if name not in golden:
                problems.append(f"{name}: not in golden file")
                continue
            actual = w.run_case(resp3, steps)
            total += len(golden[name])
            problems += D.compare_case(name, steps, golden[name], actual)
        w.close()
        for pr in problems:
            print(pr)
        print(f"{len(cases)} cases, {total} replies compared, {len(problems)} mismatches")
        return 1 if problems else 0
    finally:
        if oracle:
            oracle.close()


if __name__ == "__main__":
    sys.exit(main())
