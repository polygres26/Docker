"""Round-trip times of small Bigtable operations: Google's official Bigtable emulator (oracle, Docker) vs Warp (native Postgres).

  python3 bt_rtt_bench.py --oracle localhost:PORT [--warp-shards 1|2] [--n 300]

Every call goes over one gRPC channel, sequentially, from the same client for both sides. Prints median / p95 in milliseconds per
operation. The emulator keeps everything in memory, Warp commits to Postgres: two products with different guarantees.
"""
import argparse
import base64
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bt_client as C  # noqa: E402


def b(s):
    return base64.b64encode(s.encode()).decode()


def bench(target, n, tag):
    c = C.BtClient(target)
    parent = f"projects/rtt-{tag}{int(time.time()) % 100000}/instances/i"
    T = parent + "/tables/rtt"
    assert c.call("CreateTable", {"parent": parent, "table_id": "rtt", "table": {"column_families": {"cf": {}}}})[0] == "OK"
    res = {}

    def t(name, fn, cnt=n):
        xs = []
        for i in range(cnt):
            st = time.perf_counter()
            r = fn(i)
            xs.append((time.perf_counter() - st) * 1000)
            assert r[0] == "OK", (name, r[:2])
        xs.sort()
        res[name] = (statistics.median(xs), xs[max(0, int(len(xs) * 0.95) - 1)])

    def sc(i, size=1000):
        return {"set_cell": {"family_name": "cf", "column_qualifier": b("q"), "timestamp_micros": 1000, "value": b("x" * size)}}
    for i in range(30):  # warm-up
        c.call("MutateRow", {"table_name": T, "row_key": b(f"w{i}"), "mutations": [sc(i)]})
    t("MutateRow (1 KB cell)", lambda i: c.call("MutateRow", {"table_name": T, "row_key": b(f"r{i:06d}"), "mutations": [sc(i)]}))
    t("MutateRows (10 rows)", lambda i: c.call("MutateRows", {"table_name": T, "entries": [
        {"row_key": b(f"m{i:05d}-{j}"), "mutations": [sc(j)]} for j in range(10)]}), max(20, n // 5))
    t("ReadRows (1 row by key)", lambda i: c.call("ReadRows", {"table_name": T, "rows": {"row_keys": [b(f"r{i:06d}")]}}))
    t("ReadRows (scan 100 rows)", lambda i: c.call("ReadRows", {"table_name": T, "rows": {"row_ranges": [
        {"start_key_closed": b(f"r{i % 100:06d}")}]}, "rows_limit": 100}), max(20, n // 5))
    t("ReadModifyWriteRow (increment)", lambda i: c.call("ReadModifyWriteRow", {"table_name": T, "row_key": b("ctr"), "rules": [
        {"family_name": "cf", "column_qualifier": b("n"), "increment_amount": 1}]}))
    t("CheckAndMutateRow", lambda i: c.call("CheckAndMutateRow", {"table_name": T, "row_key": b(f"r{i:06d}"),
                                                                   "predicate_filter": {"pass_all_filter": True}, "true_mutations": [sc(i, 10)]}))
    t("GetTable", lambda i: c.call("GetTable", {"name": T}))
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp-shards", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    out = {}
    if a.oracle:
        out["Bigtable emulator (Docker)"] = bench(a.oracle, a.n, "o")
    import bt_launch_warp
    w = bt_launch_warp.BtWarp(a.warp_shards)
    try:
        out[f"Warp ({a.warp_shards} native Postgres)"] = bench(f"localhost:{w.grpc_port}", a.n, "w")
    finally:
        w.close()
    names = list(next(iter(out.values())))
    print(f"{'operation':36}" + "".join(f"{k:>38}" for k in out))
    for nm in names:
        print(f"{nm:36}" + "".join(f"{v[nm][0]:>22.2f} ms med /{v[nm][1]:>6.2f} p95" for v in out.values()))


if __name__ == "__main__":
    main()
