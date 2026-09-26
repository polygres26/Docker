"""RTT bench of cosmoswire (Warp only: no real Cosmos service or emulator was available): sequential requests over one keep-alive connection, median and
p99 in milliseconds, on one and on two Postgres backends. `python3 cosmos_rtt_bench.py [--n 300]` starts Warp on local Postgres itself and prints
a markdown section for docs/RTT_BASELINE_2026.md. Raw signed REST requests are used so the numbers are gateway round trips, not SDK overhead."""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")

import cosmos_launch_warp as L  # noqa: E402
import cosmos_raw_client as RC  # noqa: E402


def stats(xs):
    xs = sorted(xs)
    return statistics.median(xs), xs[max(0, int(len(xs) * 0.99) - 1)]


def bench(port, n):
    raw = RC.Raw(port)
    assert raw.req("POST", "/dbs", {"id": "bench"}).status_code == 201
    assert raw.req("POST", "/dbs/bench/colls", {"id": "c", "partitionKey": {"paths": ["/pk"], "kind": "Hash", "version": 2}}).status_code == 201
    p = "/dbs/bench/colls/c"
    out = {}

    def hdr(k):
        return {"x-ms-documentdb-partitionkey": '["%s"]' % k}

    for i in range(1000):
        raw.req("POST", p + "/docs", {"id": "seed%04d" % i, "pk": "k%03d" % (i % 100), "n": i, "s": "x" * 100}, hdr("k%03d" % (i % 100)))

    def timed(label, fn, count=n):
        xs = []
        for i in range(count):
            t0 = time.perf_counter()
            fn(i)
            xs.append((time.perf_counter() - t0) * 1000)
        out[label] = stats(xs)

    timed("Create item (1 KB)", lambda i: raw.req("POST", p + "/docs", {"id": "c%05d" % i, "pk": "k%03d" % (i % 100), "s": "y" * 1000}, hdr("k%03d" % (i % 100))))
    timed("Point read", lambda i: raw.req("GET", p + "/docs/seed%04d" % (i % 1000), headers=hdr("k%03d" % ((i % 1000) % 100))))
    timed("Upsert item", lambda i: raw.req("POST", p + "/docs", {"id": "seed%04d" % (i % 1000), "pk": "k%03d" % ((i % 1000) % 100), "n": -i},
                                            {"x-ms-documentdb-is-upsert": "True", **hdr("k%03d" % ((i % 1000) % 100))}))
    timed("Replace item", lambda i: raw.req("PUT", p + "/docs/seed%04d" % (i % 1000), {"id": "seed%04d" % (i % 1000), "pk": "k%03d" % ((i % 1000) % 100), "n": i},
                                             hdr("k%03d" % ((i % 1000) % 100))))
    timed("Patch item (set + incr)", lambda i: raw.req("PATCH", p + "/docs/seed%04d" % (i % 1000), {"operations": [{"op": "set", "path": "/a", "value": i},
                                                        {"op": "incr", "path": "/m", "value": 1}]}, {**hdr("k%03d" % ((i % 1000) % 100)), "Content-Type": "application/json-patch+json"}))
    timed("Delete item", lambda i: raw.req("DELETE", p + "/docs/c%05d" % i, headers=hdr("k%03d" % (i % 100))))
    timed("Query, one partition key (10 items)", lambda i: raw.query(p, "SELECT * FROM c WHERE c.pk = @k", [{"name": "@k", "value": "k%03d" % (i % 100)}], pk=["k%03d" % (i % 100)]))
    timed("Query by id in one partition", lambda i: raw.query(p, "SELECT * FROM c WHERE c.id = @i AND c.pk = @k", [{"name": "@i", "value": "seed%04d" % (i % 100)},
                                                        {"name": "@k", "value": "k%03d" % (i % 100)}]))
    timed("Cross-partition SELECT VALUE COUNT(1) (1000 items)", lambda i: raw.query(p, "SELECT VALUE COUNT(1) FROM c"), max(50, n // 4))
    timed("Cross-partition filter scan (1000 items, 10 hits)", lambda i: raw.query(p, "SELECT c.id FROM c WHERE c.n < 10"), max(50, n // 4))
    timed("Cross-partition ORDER BY n DESC TOP 10 (1000 items)", lambda i: raw.query(p, "SELECT TOP 10 c.id, c.n FROM c ORDER BY c.n DESC"), max(50, n // 4))
    timed("Cross-partition GROUP BY pk COUNT (1000 items)", lambda i: raw.query(p, "SELECT c.pk, COUNT(1) AS n FROM c GROUP BY c.pk"), max(50, n // 4))
    timed("Transactional batch of 5 creates", lambda i: raw.req("POST", p + "/docs", [{"operationType": "Create", "resourceBody": {"id": "b%d-%d" % (i, j), "pk": "bp"}}
                                                                                   for j in range(5)], {"x-ms-cosmos-is-batch-request": "True", **hdr("bp")}), max(50, n // 4))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=300)
    n = ap.parse_args().n
    res = {}
    for shards in (1, 2):
        w = L.CosmosWarp(shards)
        try:
            res[shards] = bench(w.port, n)
        finally:
            w.close()
    print("### Azure Cosmos DB for NoSQL (cosmoswire), Warp only")
    print()
    print("Sequential raw signed REST requests on one connection (no real Cosmos service or emulator to compare with), median / p99 in ms, n=%d." % n)
    print()
    print("| Operation | 1 Postgres backend | 2 Postgres backends |")
    print("|---|---|---|")
    for k in res[1]:
        print("| %s | %.2f / %.2f | %.2f / %.2f |" % (k, *res[1][k], *res[2][k]))


if __name__ == "__main__":
    main()
