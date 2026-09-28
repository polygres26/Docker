#!/usr/bin/env python3
"""redis-benchmark-like RTT/throughput for rediswire (and a real Redis for comparison).

  redis_rtt_bench.py HOST:PORT [HOST:PORT ...]     single-client p50/p99, pipelined-100 rate, 16-client ops/s
"""
import statistics
import sys
import threading
import time

from redis_resp_client import Resp

CMDS = {
    "SET": lambda i: ("SET", f"bk:{i % 1000}", "value"),
    "GET": lambda i: ("GET", f"bk:{i % 1000}"),
    "INCR": lambda i: ("INCR", "bk:ctr"),
    "LPUSH": lambda i: ("LPUSH", "bk:list", "v"),
    "ZADD": lambda i: ("ZADD", "bk:z", str(i % 5000), f"m{i % 5000}"),
}


def hp(s):
    h, _, p = s.rpartition(":")
    return h or "127.0.0.1", int(p)


def single(host, port, name, n=3000):
    c = Resp(host, port)
    f = CMDS[name]
    for i in range(300):
        c(*f(i))
    ts = []
    for i in range(n):
        t = time.perf_counter()
        c(*f(i))
        ts.append((time.perf_counter() - t) * 1000)
    ts.sort()
    c.close()
    return ts[n // 2], ts[int(n * 0.99)]


def pipelined(host, port, name, total=20000):
    c = Resp(host, port)
    f = CMDS[name]
    t = time.perf_counter()
    for start in range(0, total, 100):
        c.pipeline([f(i) for i in range(start, start + 100)])
    d = time.perf_counter() - t
    c.close()
    return total / d


def clients16(host, port, name, per=1500):
    f = CMDS[name]

    def work():
        c = Resp(host, port)
        for i in range(per):
            c(*f(i))
        c.close()

    ts = [threading.Thread(target=work) for _ in range(16)]
    t = time.perf_counter()
    [x.start() for x in ts]
    [x.join() for x in ts]
    return 16 * per / (time.perf_counter() - t)


if __name__ == "__main__":
    for target in sys.argv[1:]:
        host, port = hp(target)
        Resp(host, port)("FLUSHALL")
        print(f"== {target}")
        print(f"{'cmd':6} {'p50 ms':>8} {'p99 ms':>8} {'pipe100 ops/s':>14} {'16 clients ops/s':>17}")
        for name in CMDS:
            p50, p99 = single(host, port, name)
            print(f"{name:6} {p50:8.3f} {p99:8.3f} {pipelined(host, port, name):14.0f} {clients16(host, port, name):17.0f}")
