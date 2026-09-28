"""RTT bench of cqlwire vs a real Apache Cassandra: the same statements through the python driver (prepared, one connection, sequential),
median and p99 in milliseconds. `python3 cql_rtt_bench.py --cassandra-port 19142 [--rows N]` starts Warp on local Postgres itself and prints a markdown
section for docs/RTT_BASELINE_2026.md."""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile  # noqa: E402
from cassandra.query import tuple_factory  # noqa: E402


def connect(port):
    prof = ExecutionProfile(row_factory=tuple_factory, request_timeout=60, consistency_level=ConsistencyLevel.ONE)
    c = Cluster(["127.0.0.1"], port=port, protocol_version=4, execution_profiles={EXEC_PROFILE_DEFAULT: prof})
    return c, c.connect()


def timed(n, fn):
    xs = []
    for i in range(n):
        t = time.perf_counter()
        fn(i)
        xs.append((time.perf_counter() - t) * 1000)
    xs.sort()
    return statistics.median(xs), xs[int(len(xs) * 0.99) - 1]


def bench(port, n, ks):
    c, s = connect(port)
    s.execute(f"DROP KEYSPACE IF EXISTS {ks}")
    s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    s.execute(f"CREATE TABLE {ks}.kv (k int PRIMARY KEY, v text, n int)")
    s.execute(f"CREATE TABLE {ks}.ts (p int, c int, v text, PRIMARY KEY (p, c))")
    s.execute(f"CREATE TABLE {ks}.cnt (k int PRIMARY KEY, n counter)")
    ins = s.prepare(f"INSERT INTO {ks}.kv (k, v, n) VALUES (?, ?, ?)")
    sel = s.prepare(f"SELECT v, n FROM {ks}.kv WHERE k = ?")
    upd = s.prepare(f"UPDATE {ks}.kv SET n = ? WHERE k = ?")
    tin = s.prepare(f"INSERT INTO {ks}.ts (p, c, v) VALUES (?, ?, ?)")
    tsel = s.prepare(f"SELECT c, v FROM {ks}.ts WHERE p = 1 LIMIT 10")
    cin = s.prepare(f"UPDATE {ks}.cnt SET n = n + 1 WHERE k = ?")
    lwt = s.prepare(f"INSERT INTO {ks}.kv (k, v, n) VALUES (?, 'x', 1) IF NOT EXISTS")
    res = {}
    res["INSERT (prepared)"] = timed(n, lambda i: s.execute(ins, (i, "value-%d" % i, i)))
    res["SELECT by key (prepared)"] = timed(n, lambda i: s.execute(sel, (i % n,)))
    res["UPDATE (prepared)"] = timed(n, lambda i: s.execute(upd, (i * 2, i % n)))
    res["counter UPDATE"] = timed(n, lambda i: s.execute(cin, (i % 50,)))
    res["LWT INSERT IF NOT EXISTS"] = timed(max(20, n // 4), lambda i: s.execute(lwt, (10_000_000 + i,)))
    for i in range(50):
        s.execute(tin, (1, i, "v"))
    res["SELECT LIMIT 10 in partition"] = timed(n, lambda i: s.execute(tsel))
    res["SELECT count(*) of 50-row partition"] = timed(max(20, n // 4), lambda i: s.execute(f"SELECT count(*) FROM {ks}.ts WHERE p = 1"))
    s.execute(f"DROP KEYSPACE {ks}")
    c.shutdown()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cassandra-port", type=int, default=19142)
    ap.add_argument("--rows", type=int, default=300)
    a = ap.parse_args()
    import cql_launch_warp as L
    cass = bench(a.cassandra_port, a.rows, "rttbench")
    warp = {}
    for shards in (1, 2):
        w = L.CqlWarp(shards)
        try:
            warp[shards] = bench(w.port, a.rows, "rttbench")
        finally:
            w.close()
    print("| operation | Cassandra 5.0 median / p99 (ms) | Warp, 1 Postgres median / p99 | Warp, 2 sharded Postgres median / p99 |")
    print("|---|---|---|---|")
    for k in cass:
        print(f"| {k} | {cass[k][0]:.2f} / {cass[k][1]:.2f} | {warp[1][k][0]:.2f} / {warp[1][k][1]:.2f} | {warp[2][k][0]:.2f} / {warp[2][k][1]:.2f} |")


if __name__ == "__main__":
    main()
