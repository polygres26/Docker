"""Round-trip times of small Firestore operations: the official Firestore emulator (oracle, Docker) vs Warp (native Postgres).

  python3 fs_rtt_bench.py --oracle HOST:PORT [--warp-shards 1|2] [--n 300]

Raw gRPC from one channel per side, sequential calls, the same client and requests for both. Prints median / p95 in ms.
"""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "gen"))
import grpc  # noqa: E402
from google.firestore.v1 import common_pb2, document_pb2 as D, firestore_pb2 as F, firestore_pb2_grpc as G, query_pb2 as Q, write_pb2 as W  # noqa: E402

MD = [("authorization", "Bearer owner")]


def bench(host, n, tag):
    ch = grpc.insecure_channel(host)
    st = G.FirestoreStub(ch)
    db = f"projects/rtt{tag}{int(time.time()) % 100000}/databases/(default)"
    res = {}

    def name(p):
        return f"{db}/documents/{p}"

    def t(label, fn, count=n):
        for i in range(20):  # warm-up
            fn(-1 - i)
        xs = []
        for i in range(count):
            s = time.perf_counter()
            fn(i)
            xs.append((time.perf_counter() - s) * 1000)
        xs.sort()
        res[label] = (statistics.median(xs), xs[int(len(xs) * 0.95) - 1])

    body = {"a": D.Value(integer_value=1), "s": D.Value(string_value="x" * 200)}

    def setdoc(i):
        st.Commit(F.CommitRequest(database=db, writes=[W.Write(update=D.Document(name=name(f"b/d{i}"), fields=body))]), metadata=MD)

    t("Commit: set 1 document (1 KiB)", setdoc)
    for i in range(0, 200):
        setdoc(i)
    t("GetDocument", lambda i: st.GetDocument(F.GetDocumentRequest(name=name(f"b/d{i % 200}")), metadata=MD))
    t("BatchGetDocuments (10)", lambda i: list(st.BatchGetDocuments(F.BatchGetDocumentsRequest(database=db, documents=[name(f"b/d{(i + k) % 200}") for k in range(10)]), metadata=MD)))
    q = Q.StructuredQuery(**{"from": [Q.StructuredQuery.CollectionSelector(collection_id="b")]})
    q.limit.value = 20
    t("RunQuery: collection, limit 20", lambda i: list(st.RunQuery(F.RunQueryRequest(parent=db + "/documents", structured_query=q), metadata=MD)))
    q2 = Q.StructuredQuery(**{"from": [Q.StructuredQuery.CollectionSelector(collection_id="b")]})
    f = q2.where.field_filter
    f.field.field_path = "a"
    f.op = Q.StructuredQuery.FieldFilter.EQUAL
    f.value.integer_value = 1
    q2.order_by.add(field=Q.StructuredQuery.FieldReference(field_path="__name__"))
    q2.limit.value = 20
    t("RunQuery: filter a == 1, limit 20", lambda i: list(st.RunQuery(F.RunQueryRequest(parent=db + "/documents", structured_query=q2), metadata=MD)))
    agg = Q.StructuredAggregationQuery(structured_query=Q.StructuredQuery(**{"from": [Q.StructuredQuery.CollectionSelector(collection_id="b")]}))
    agg.aggregations.add(count=Q.StructuredAggregationQuery.Aggregation.Count(), alias="n")
    t("RunAggregationQuery: count (200 docs)", lambda i: list(st.RunAggregationQuery(F.RunAggregationQueryRequest(parent=db + "/documents", structured_aggregation_query=agg), metadata=MD)), count=max(50, n // 3))

    def txn(i):
        tx = st.BeginTransaction(F.BeginTransactionRequest(database=db, options=common_pb2.TransactionOptions(read_write=common_pb2.TransactionOptions.ReadWrite())), metadata=MD).transaction
        st.GetDocument(F.GetDocumentRequest(name=name(f"b/d{i % 200}"), transaction=tx), metadata=MD)
        st.Commit(F.CommitRequest(database=db, transaction=tx, writes=[W.Write(update=D.Document(name=name(f"b/d{i % 200}"), fields={"a": D.Value(integer_value=i)}))]), metadata=MD)

    t("Transaction: begin + get + commit", txn, count=max(50, n // 3))
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
        out["Firestore emulator (Docker)"] = bench(a.oracle, a.n, "o")
    import fs_launch_warp
    w = fs_launch_warp.FsWarp(a.warp_shards)
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
