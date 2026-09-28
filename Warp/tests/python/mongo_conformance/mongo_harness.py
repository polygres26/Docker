#!/usr/bin/env python3
"""Differential harness: run the corpus against a real mongod (oracle) and against Warp and compare.

  python3 mongo_harness.py --oracle mongodb://localhost:PORT --warp mongodb://localhost:PORT2 [--filter find_] [--show diff|msg|all]
  python3 mongo_harness.py --oracle URL --start-warp [--shards 2]
  python3 mongo_harness.py --oracle URL --record golden/oracle_7.0.json.gz      # record the oracle's answers (twice, non-deterministic steps dropped)
  python3 mongo_harness.py --oracle URL --dump case_name                        # print the oracle's answers for one case
"""
import argparse
import gzip
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pymongo import MongoClient  # noqa: E402

import mongo_corpus as corpus  # noqa: E402
try:
    import mongo_known as known  # noqa: E402
except ImportError:
    known = None


def client(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=15000, retryWrites=False)


def run_all(uri, names, tag):
    c = client(uri)
    out = {}
    for i, n in enumerate(names):
        t0 = time.time()
        out[n] = corpus.run_case(n, corpus.CASES[n], c, "wc%d_x" % i)
        if os.environ.get("MONGO_HARNESS_VERBOSE"):
            print("  [%s] %s %.1fs" % (tag, n, time.time() - t0), file=sys.stderr, flush=True)
    c.close()
    return out


def record(oracle, names, path):
    a = run_all(oracle, names, "a")
    b = run_all(oracle, names, "b")
    gold = {}
    dropped = []
    for n in names:
        bm = {s["l"]: s for s in b[n]}
        steps = []
        for s in a[n]:
            o = bm.get(s["l"])
            same = o is not None and (o["o"] == s["o"] or (s.get("un") and "ok" in s["o"] and "ok" in o["o"]
                                                            and corpus._deep_sorted(o["o"]["ok"]) == corpus._deep_sorted(s["o"]["ok"])))
            if same:
                steps.append(s)
            else:
                dropped.append("%s/%s" % (n, s["l"]))
        gold[n] = steps
    info = MongoClient(oracle).admin.command("buildInfo")
    with gzip.open(path, "wt") as f:
        json.dump({"meta": {"server": "mongod " + info["version"], "recorded": time.strftime("%Y-%m-%d"), "cases": len(names),
                            "steps": sum(len(v) for v in gold.values()), "dropped_nondeterministic": dropped},
                   "cases": gold}, f)
    print("recorded %d cases, %d steps, dropped %d non-deterministic steps -> %s" % (
        len(names), sum(len(v) for v in gold.values()), len(dropped), path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter", default="")
    ap.add_argument("--show", default="diff", choices=["diff", "msg", "all", "none"])
    ap.add_argument("--record")
    ap.add_argument("--golden", help="compare Warp against a recorded golden file instead of a live oracle")
    ap.add_argument("--dump")
    ap.add_argument("--out")
    a = ap.parse_args()
    names = [n for n in corpus.CASES if re.search(a.filter, n)]
    if a.dump:
        for n in names:
            print("== " + n)
            for s in run_all(a.oracle, [n], "d")[n]:
                print(s["l"], json.dumps(s["o"]))
        return
    if a.record:
        record(a.oracle, names, a.record)
        return
    stack = None
    warp = a.warp
    if a.start_warp:
        import mongo_launch_warp
        stack = mongo_launch_warp.Stack(a.shards)
        warp = stack.uri
    try:
        if a.golden:
            with gzip.open(a.golden, "rt") as f:
                gold = json.load(f)["cases"]
            names = [n for n in names if n in gold]
        else:
            gold = run_all(a.oracle, names, "o")
        got = run_all(warp, names, "w")
    finally:
        if stack:
            stack.close()
    counts = {"same": 0, "msg": 0, "diff": 0, "accepted": 0}
    per_case = {}
    lines = []
    for n in names:
        res = corpus.compare_steps(gold[n], got[n], sharded=a.shards > 1, case=n)
        for label, verdict, detail in res:
            counts[verdict] += 1
            per_case.setdefault(n, []).append((label, verdict))
            if a.show == "all" or (verdict == "diff" and a.show in ("diff", "msg")) or (verdict == "msg" and a.show == "msg"):
                lines.append("%s/%s [%s] %s" % (n, label, verdict, detail))
    print("\n".join(lines))
    print("TOTAL steps=%d %s" % (sum(counts.values()), counts))
    if a.out:
        json.dump({"counts": counts, "lines": lines}, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
