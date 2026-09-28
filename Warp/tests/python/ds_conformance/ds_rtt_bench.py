"""Round-trip times of small Datastore operations: the official Datastore emulator (oracle, Docker) vs Warp (native Postgres).

  python3 ds_rtt_bench.py --oracle HOST:PORT [--warp-shards 1|2] [--n 300]

Raw gRPC from one channel per side, sequential calls, the same client and requests for both. Prints median / p95 in ms.
"""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "fs_conformance", "gen"))
import grpc  # noqa: E402
from google.datastore.v1 import datastore_pb2 as DP, datastore_pb2_grpc as DG, entity_pb2 as E, query_pb2 as Q  # noqa: E402

MD = [("authorization", "Bearer owner")]


def key(kind, ident):
    return E.Key(path=[E.Key.PathElement(kind=kind, id=ident)])


def bench(host, n, tag):
    ch = grpc.insecure_channel(host)
    st = DG.DatastoreStub(ch)
    project = f"rtt{tag}{int(time.time()) % 100000}"
    res = {}

    def t(label, fn, count=n):
        for i in range(20):
            fn(-1 - i)
        xs = []
        for i in range(count):
            s = time.perf_counter()
            fn(i)
            xs.append((time.perf_counter() - s) * 1000)
        xs.sort()
        res[label] = (statistics.median(xs), xs[int(len(xs) * 0.95) - 1])

    props = {"a": E.Value(integer_value=1), "s": E.Value(string_value="x" * 200)}

    def upsert(i):
        st.Commit(DP.CommitRequest(project_id=project, mode=DP.CommitRequest.NON_TRANSACTIONAL,
                                   mutations=[DP.Mutation(upsert=E.Entity(key=key("B", 1000 + i), properties=props))]), metadata=MD)

    t("Commit: upsert 1 entity (1 KiB)", upsert)
    for i in range(200):
        upsert(i)
    t("Lookup 1 key", lambda i: st.Lookup(DP.LookupRequest(project_id=project, keys=[key("B", 1000 + i % 200)]), metadata=MD))
    t("Lookup 10 keys", lambda i: st.Lookup(DP.LookupRequest(project_id=project, keys=[key("B", 1000 + (i + k) % 200) for k in range(10)]), metadata=MD))
    q = Q.Query(kind=[Q.KindExpression(name="B")])
    q.limit.value = 20
    t("RunQuery: kind, limit 20", lambda i: st.RunQuery(DP.RunQueryRequest(project_id=project, query=q), metadata=MD))
    q2 = Q.Query(kind=[Q.KindExpression(name="B")])
    f = q2.filter.property_filter
    f.property.name = "a"
    f.op = Q.PropertyFilter.EQUAL
    f.value.integer_value = 1
    q2.limit.value = 20
    t("RunQuery: filter a == 1, limit 20", lambda i: st.RunQuery(DP.RunQueryRequest(project_id=project, query=q2), metadata=MD))

    def txn(i):
        tx = st.BeginTransaction(DP.BeginTransactionRequest(project_id=project), metadata=MD).transaction
        r = DP.LookupRequest(project_id=project, keys=[key("B", 1000 + i % 200)])
        r.read_options.transaction = tx
        st.Lookup(r, metadata=MD)
        st.Commit(DP.CommitRequest(project_id=project, mode=DP.CommitRequest.TRANSACTIONAL, transaction=tx,
                                   mutations=[DP.Mutation(upsert=E.Entity(key=key("B", 1000 + i % 200), properties=props))]), metadata=MD)

    t("Transaction: begin + lookup + commit", txn, count=max(50, n // 3))
    ch.close()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp-shards", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    out = {}
    if a.oracle:
        out["Datastore emulator (Docker)"] = bench(a.oracle, a.n, "o")
    import ds_launch_warp
    w = ds_launch_warp.DsWarp(a.warp_shards)
    try:
        out[f"Warp ({a.warp_shards} native Postgres)"] = bench(w.host, a.n, "w")
    finally:
        w.close()
    names = list(next(iter(out.values())))
    print(f"{'operation':40}" + "".join(f"{k:>36}" for k in out))
    for nm in names:
        print(f"{nm:40}" + "".join(f"{v[nm][0]:>20.2f} ms med /{v[nm][1]:>6.2f} p95" for v in out.values()))


if __name__ == "__main__":
    main()
