"""Dev tool: start Warp, run the corpus, print the mismatches against golden.json.gz."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")
import cql_harness as H  # noqa: E402
import cql_launch_warp as L  # noqa: E402

only = sys.argv[1] if len(sys.argv) > 1 else None
w = L.CqlWarp(int(os.environ.get("SHARDS", "1")))
try:
    corpus = [c for c in H.load_corpus() if not only or only in c.name]
    actual = {c.name: H.run_case(w.port, c) for c in corpus}
    diffs, used_p, used_r = H.compare(H.load_golden(), actual, corpus)
    gold = H.load_golden()
    total = ident = unstable = waits = 0
    for cs in corpus:
        for i, _ in enumerate(cs.steps):
            g = gold[cs.name][i]
            if g.get("unstable"):
                unstable += 1
            elif "wait" in g:
                waits += 1
            else:
                total += 1
                ident += g == actual[cs.name][i]
    print(f"STEPS compared {total}: identical {ident}, known message-only {sum(1 for _ in range(0))}, unstable-dropped {unstable}, waits {waits}, cases {len(corpus)}")
    kinds = {}
    for d in diffs:
        kinds[d[5]] = kinds.get(d[5], 0) + 1
    print("known patterns hit", sorted(used_p), "known results hit", sorted(used_r))
    print("MISMATCHES", len(diffs), kinds, "of", sum(len(c.steps) for c in corpus))
    for case, i, step, e, a, kind in diffs:
        print(f"--- {case}[{i}] ({kind}): {step[:230]}")
        print("   expected:", json.dumps(e)[:600])
        print("   actual  :", json.dumps(a)[:600])
finally:
    w.close()
