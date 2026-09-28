"""Round-trip times of small Azure Storage operations: Azurite (oracle, Docker) vs Warp (native Postgres).

  python3 az_rtt_bench.py --azurite-ports 32775,32776,32777 [--warp-shards 1|2] [--n 300]

Every request is signed (SharedKey) and sent over one keep-alive connection, sequentially; the client is the same for both
sides. Prints median / p95 in milliseconds per operation.
"""
import argparse
import json
import statistics
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests  # noqa: E402

import az_client as C  # noqa: E402

J = {"Content-Type": "application/json"}


def bench(ep, n, tag):
    sess = requests.Session()
    orig = requests.request
    requests.request = lambda method, url, **kw: sess.request(method, url, **kw)
    try:
        run = f"{tag}{int(time.time()) % 100000}"
        C.do(ep, "blob", "PUT", f"/rtt{run}", [("restype", "container")])
        C.do(ep, "queue", "PUT", f"/rtt{run}")
        C.do(ep, "table", "POST", "/Tables", [], J, json.dumps({"TableName": f"rtt{run}"}))
        res = {}

        def t(name, fn):
            xs = []
            for i in range(n):
                s = time.perf_counter()
                r = fn(i)
                xs.append((time.perf_counter() - s) * 1000)
                assert r.status_code < 300, (name, r.status_code, r.text[:200])
            xs.sort()
            res[name] = (statistics.median(xs), xs[int(len(xs) * 0.95) - 1])

        body = b"x" * 1024
        t("blob PUT 1 KiB", lambda i: C.do(ep, "blob", "PUT", f"/rtt{run}/b{i}", [], {"x-ms-blob-type": "BlockBlob"}, body))
        t("blob GET 1 KiB", lambda i: C.do(ep, "blob", "GET", f"/rtt{run}/b{i}"))
        t("queue put message", lambda i: C.do(ep, "queue", "POST", f"/rtt{run}/messages", [], {},
                                              "<QueueMessage><MessageText>hello</MessageText></QueueMessage>"))
        t("queue get message", lambda i: C.do(ep, "queue", "GET", f"/rtt{run}/messages", [("visibilitytimeout", "600")]))
        t("table insert entity", lambda i: C.do(ep, "table", "POST", f"/rtt{run}", [], {**J, "Prefer": "return-no-content"},
                                                json.dumps({"PartitionKey": f"p{i % 10}", "RowKey": f"r{i}", "V": i})))
        t("table get entity", lambda i: C.do(ep, "table", "GET", f"/rtt{run}(PartitionKey='p{i % 10}',RowKey='r{i}')"))
        t("table query (PartitionKey eq)", lambda i: C.do(ep, "table", "GET", f"/rtt{run}()", [("$filter", f"PartitionKey eq 'p{i % 10}'"), ("$top", "20")]))
        return res
    finally:
        requests.request = orig


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--azurite-ports")
    ap.add_argument("--warp-shards", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    out = {}
    if a.azurite_ports:
        p = a.azurite_ports.split(",")
        out["Azurite (Docker)"] = bench(C.Endpoint(*[f"http://localhost:{x}" for x in p]), a.n, "a")
    import az_launch_warp
    w = az_launch_warp.AzWarp(a.warp_shards)
    try:
        out[f"Warp ({a.warp_shards} native Postgres)"] = bench(C.Endpoint(w.blob, w.queue, w.table), a.n, "w")
    finally:
        w.close()
    names = list(next(iter(out.values())))
    print(f"{'operation':34}" + "".join(f"{k:>34}" for k in out))
    for n in names:
        print(f"{n:34}" + "".join(f"{v[n][0]:>18.2f} ms med /{v[n][1]:>6.2f} p95" for v in out.values()))


if __name__ == "__main__":
    main()
