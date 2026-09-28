"""Development helper: runs the corpus against a broker on --port and prints the steps that differ from golden.json.gz."""
import argparse
import json
import sys

import amqp_harness as H
import amqp_known as K

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--only", nargs="*")
ap.add_argument("--golden", default=H.GOLDEN)
a = ap.parse_args()
import gzip
golden = json.load(gzip.open(a.golden, "rt"))
cases = [c for c in H.load_corpus() if not a.only or c.name in a.only]
actual = {c.name: H.run_case("127.0.0.1", a.port, c) for c in cases}
diffs, up, ur = H.compare(golden, actual, cases, K)
for c, i, s, e, act, k in diffs:
    print(f"{c}[{i}] ({k}) {s[:150]}\n   expected {json.dumps(e)[:900]}\n   actual   {json.dumps(act)[:900]}")
print(len(diffs), "differences in", sum(len(actual[c.name]) for c in cases), "steps")
