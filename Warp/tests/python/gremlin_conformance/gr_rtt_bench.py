"""RTT bench of gremlinwire vs a real Apache TinkerPop Gremlin Server (TinkerGraph): the same requests over one persistent WebSocket, sequential, GraphSON 3.0, median and p99 in
milliseconds, plus 8-client throughput. `python3 gr_rtt_bench.py --ref ws://localhost:18182/gremlin [--n N]` starts Warp on local Postgres itself (1 and 2 sharded backends) and prints a markdown
section for docs/RTT_BASELINE_2026.md. The reference runs in Docker (`docker run --memory 1g -p 18182:8182 tinkerpop/gremlin-server`), Warp's Postgres backends natively."""
import argparse
import concurrent.futures
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")
import gr_harness as H  # noqa: E402

READS = [
    ("script 1+1 (protocol floor)", "1+1"),
    ("g.V().count()", "g.V().count()"),
    ("g.V(1).values('name')", "g.V(1).values('name')"),
    ("g.V(1).out().values('name')", "g.V(1).out().values('name')"),
    ("g.V().has('name','marko').out('knows').values('name')", "g.V().has('name','marko').out('knows').values('name')"),
    ("g.V(1).out().out().path()", "g.V(1).out().out().path()"),
    ("g.V(1).repeat(out()).times(2).values('name')", "g.V(1).repeat(out()).times(2).values('name')"),
    ("g.V().hasLabel('person').group().by(label).by(count())", "g.V().hasLabel('person').group().by(label).by(count())"),
]


def timed(n, fn):
    xs = []
    for i in range(n):
        t = time.perf_counter()
        r = fn(i)
        xs.append((time.perf_counter() - t) * 1000)
        assert r[-1]["status"]["code"] in (200, 204), r[-1]["status"]  # never time an error
    xs.sort()
    return statistics.median(xs), xs[max(0, int(len(xs) * 0.99) - 1)]


def bench(url, n):
    ws = H.RawWs(url)
    H.load_graph(ws, "modern")
    res = {}
    for name, s in READS:
        ws.eval(s)  # warm up
        res[name] = timed(n, lambda i, s=s: ws.eval(s))
    args = H.bytecode_args("g.V(1).out().values('name')")
    res["bytecode g.V(1).out().values('name')"] = timed(n, lambda i: ws.bytecode(args))
    res["write: g.addV('b').property('i', n)"] = timed(n, lambda i: ws.eval(f"g.addV('b').property('i',{i})"))
    res["write: g.V(1).property('n', n)"] = timed(n, lambda i: ws.eval(f"g.V(1).property('n',{i})"))
    res["write: addV + addE (one script)"] = timed(max(20, n // 3), lambda i: ws.eval(f"g.addV('c').as('a').addV('c').as('b').addE('l').from('a').to('b')"))
    ws.eval("g.V().hasLabel('b','c').drop().iterate()")
    ws.close()

    def client(_):
        w = H.RawWs(url)
        t = time.perf_counter()
        for _ in range(n):
            w.eval("g.V(1).out().values('name')")
        w.close()
        return n / (time.perf_counter() - t)

    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        res["throughput, 8 clients (ops/s)"] = (sum(ex.map(client, range(8))), 0.0)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="ws://localhost:18182/gremlin")
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    import gr_launch_warp as L
    ref = bench(a.ref, a.n)
    warp = {}
    for shards in (1, 2):
        w = L.GremlinWarp(shards)
        try:
            warp[shards] = bench(f"ws://localhost:{w.port}/gremlin", a.n)
        finally:
            w.close()
    print("| request | Gremlin Server 3.8 (TinkerGraph) median / p99 (ms) | Warp, 1 Postgres median / p99 | Warp, 2 sharded Postgres median / p99 |")
    print("|---|---|---|---|")
    for k in ref:
        if k.startswith("throughput"):
            print(f"| {k} | {ref[k][0]:.0f} | {warp[1][k][0]:.0f} | {warp[2][k][0]:.0f} |")
        else:
            print(f"| {k} | {ref[k][0]:.2f} / {ref[k][1]:.2f} | {warp[1][k][0]:.2f} / {warp[1][k][1]:.2f} | {warp[2][k][0]:.2f} / {warp[2][k][1]:.2f} |")


if __name__ == "__main__":
    main()
