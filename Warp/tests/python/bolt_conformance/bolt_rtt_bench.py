#!/usr/bin/env python3
"""RTT of a simple CREATE and a simple MATCH through boltwire (client p50 in ms, one session, 300 measured after 50 warm-up).
  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 bolt_rtt_bench.py"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from neo4j import GraphDatabase
from bolt_launch_warp import Stack


def p50(f, n=300, warm=50):
    for i in range(warm):
        f(i)
    xs = []
    for i in range(n):
        t = time.perf_counter(); f(i); xs.append((time.perf_counter() - t) * 1000)
    xs.sort()
    return xs[len(xs) // 2], xs[int(len(xs) * .9)]


with Stack() as st:
    d = GraphDatabase.driver(st.uri, auth=None)
    with d.session() as s:
        s.run("CREATE (n:Seed {id: 1})").consume()
        c = p50(lambda i: s.run("CREATE (n:B {v: %d}) RETURN n.v AS v" % i).single())
        m = p50(lambda i: s.run("MATCH (n:Seed {id: 1}) RETURN n.id AS id").single())
        l = p50(lambda i: s.run("RETURN %d AS x" % i).single())
    print("CREATE p50=%.3f p90=%.3f | MATCH p50=%.3f p90=%.3f | RETURN literal p50=%.3f" % (c + m + (l[0],)))
    d.close()
