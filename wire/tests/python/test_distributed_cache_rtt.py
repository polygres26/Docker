"""Real 2-node distributed cache-hit RTT: TWO separate real Warp processes, both with
WARP_CLUSTER_ENABLED=true joining one real Ignite cluster (static TCP discovery, same-machine),
both pointed at the SAME real Postgres backend, both with the same WARP_CACHE_TABLES.

The claim under test: a cache entry that nodeA's read populates is visible to nodeB's read of
the SAME key WITHOUT nodeB ever touching the real Postgres backend for that query -- i.e. the
Ignite cache is genuinely shared across processes, not per-process/local.

See cluster_support.py for the discovery config (WarpCluster.java's own "static" IP finder mode,
confirmed from source -- no code changes needed for a same-machine 2-node test).
"""
import os
import statistics
import time

import psycopg2
import pytest

from polywire_support import RealPostgres
from cluster_support import start_cluster_node

WARMUP = 1
SAMPLES = 20


@pytest.fixture(scope="module")
def postgres():
    pg = RealPostgres()
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres",
                           password="postgres", dbname="postgres") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE dist_items (id INT PRIMARY KEY, val INT NOT NULL)")
            # id=1 is nodeA's cross-node-hit key (populated once, read repeatedly below). ids
            # 2..21 are a POOL of never-before-read keys for nodeB's own-MISS measurement --
            # after the cache-populate path stopped blocking the response on a synchronous Ignite
            # put (see CacheStage's own comment on this), a real backend MISS and a real
            # cross-node Ignite HIT both land in the same low-single-digit-millisecond range on
            # a loopback test rig, so a single-sample MISS vs. a 20-sample-averaged HIT is noisy
            # enough to flip the comparison by chance. Averaging the MISS side over an equally
            # sized pool of distinct, never-touched keys (each queried exactly once, so it's a
            # genuine MISS every time) gives both sides the same statistical footing.
            cur.execute("INSERT INTO dist_items (id, val) VALUES " + ", ".join(
                f"({i}, {i * 111})" for i in range(1, 22)))
    yield pg
    pg.close()


@pytest.fixture(scope="module")
def node_a(postgres):
    proc = start_cluster_node(postgres, cache_tables="dist_items", frontend_name="nodeA")
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def node_b(postgres, node_a):
    # Start nodeB only after nodeA is already up and has joined the (1-node-so-far) cluster --
    # nodeB's own Ignite discovery start() call blocks until it has joined, and by then both
    # nodes' membership view includes each other, so nodeB's own startup log is the one that
    # should show "current size=2".
    proc = start_cluster_node(postgres, cache_tables="dist_items", frontend_name="nodeB")
    yield proc
    proc.close()


def connect(proc):
    return psycopg2.connect(
        host="localhost", port=proc.frontend_port,
        user="postgres", password="postgres", dbname="postgres",
    )


def joined_cluster_size(proc):
    """Scrapes the real "warp cluster joined, current size=N" log line WarpCluster.start()
    prints (com.sayonora.wire.cluster.WarpCluster), from this process's own captured stdout."""
    for line in proc._output_lines:
        if "warp cluster joined" in line:
            try:
                return int(line.strip().split("current size=")[1].split()[0])
            except (IndexError, ValueError):
                pass
    return None


def test_both_nodes_join_one_real_ignite_cluster(node_a, node_b):
    # Give a brief grace period: nodeB's own log write can trail its process's readiness check
    # (which only waits on /metrics + the pgwire port, not on this particular log line) by a
    # fraction of a second.
    deadline = time.time() + 15
    size_a = size_b = None
    while time.time() < deadline:
        size_a = joined_cluster_size(node_a)
        size_b = joined_cluster_size(node_b)
        if size_a is not None and size_b is not None:
            break
        time.sleep(0.3)
    print(f"\nnodeA joined-cluster log size={size_a}, nodeB joined-cluster log size={size_b}")
    assert size_b is not None, (
        "nodeB never logged 'warp cluster joined' -- real Ignite clustering did not start; "
        "last output:\n" + "".join(node_b._output_lines[-60:]))
    assert size_b >= 2, (
        f"nodeB joined a cluster of size {size_b}, expected >=2 (nodeA + nodeB) -- real "
        f"clustering did not actually connect the two processes; last output:\n"
        + "".join(node_b._output_lines[-60:]))


def timed_read(proc, sql):
    conn = connect(proc)
    try:
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        row = cur.fetchone()
        t1 = time.perf_counter()
        cur.close()
        return (t1 - t0) * 1000.0, row
    finally:
        conn.close()


def timed_reads(proc, sql, warmup=WARMUP, samples=SAMPLES):
    rtts = []
    for i in range(warmup + samples):
        rtt, _ = timed_read(proc, sql)
        if i >= warmup:
            rtts.append(rtt)
    return rtts


def test_cross_node_cache_hit(node_a, node_b):
    # 1. nodeA reads id=1 -- a real MISS, populates the SHARED Ignite cache.
    node_a_miss_rtt, row = timed_read(node_a, "SELECT val FROM dist_items WHERE id = 1")
    assert row == (111,)
    print(f"\nnodeA cache MISS (id=1, populates shared cache): {node_a_miss_rtt:.3f}ms")

    # 2. nodeB reads the SAME key (id=1) -- nodeB's process never touched Postgres for this key.
    #    If the Ignite cache is truly shared, this is a cache HIT: fast, matching Part A's own
    #    hit-speed profile, not a fresh backend round trip.
    node_b_hit_rtts = timed_reads(node_b, "SELECT val FROM dist_items WHERE id = 1")
    node_b_hit_avg = statistics.mean(node_b_hit_rtts)
    print(f"nodeB cache HIT on nodeA-populated key (id=1), n={len(node_b_hit_rtts)}: "
          f"avg={node_b_hit_avg:.3f}ms min={min(node_b_hit_rtts):.3f}ms "
          f"max={max(node_b_hit_rtts):.3f}ms")

    # 3. nodeB reads a POOL of DIFFERENT, never-cached keys (id=2..21) -- its own genuine MISSes,
    #    proving nodeB's own backend path works normally and the fast result above isn't just
    #    "nodeB is always fast for some unrelated reason". Each key is queried exactly once (a
    #    real MISS every time, never a repeat that could itself turn into a hit) and averaged over
    #    the same sample count as the HIT measurement above -- a single-sample MISS vs. a
    #    20-sample-averaged HIT is noisy enough, now that both are genuinely fast, to flip the
    #    comparison by chance (see the fixture's own comment on this).
    own_miss_rtts = []
    for key_id in range(2, 2 + SAMPLES):
        rtt, row2 = timed_read(node_b, f"SELECT val FROM dist_items WHERE id = {key_id}")
        assert row2 == (key_id * 111,)
        own_miss_rtts.append(rtt)
    node_b_own_miss_avg = statistics.mean(own_miss_rtts)
    print(f"nodeB cache MISS avg on its OWN never-before-read keys (id=2..{1 + SAMPLES}), "
          f"n={len(own_miss_rtts)}: avg={node_b_own_miss_avg:.3f}ms min={min(own_miss_rtts):.3f}ms "
          f"max={max(own_miss_rtts):.3f}ms")

    print(f"\nSUMMARY: nodeA MISS={node_a_miss_rtt:.3f}ms | "
          f"nodeB cross-node HIT avg={node_b_hit_avg:.3f}ms | "
          f"nodeB own MISS avg={node_b_own_miss_avg:.3f}ms")

    assert node_b_hit_avg < node_a_miss_rtt, (
        f"expected nodeB's cross-node cache HIT ({node_b_hit_avg:.3f}ms) to be faster than "
        f"nodeA's cold MISS ({node_a_miss_rtt:.3f}ms)")
    assert node_b_hit_avg < node_b_own_miss_avg, (
        f"expected nodeB's cross-node cache HIT avg ({node_b_hit_avg:.3f}ms) to be faster than "
        f"nodeB's OWN genuine MISS avg ({node_b_own_miss_avg:.3f}ms) -- if not, nodeB may be quietly "
        f"re-reading Postgres itself instead of getting a real cross-node cache hit")
