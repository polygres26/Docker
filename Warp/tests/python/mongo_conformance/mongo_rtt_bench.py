#!/usr/bin/env python3
"""insertOne / find-by-_id round trips against a mongo-wire endpoint (Warp or a real mongod), for before/after comparisons.

  python3 mongo_rtt_bench.py mongodb://localhost:PORT [--n 300]
"""
import argparse
import statistics
import time

from pymongo import MongoClient


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uri")
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    c = MongoClient(a.uri, serverSelectionTimeoutMS=10000)["rttbench"]["c"]
    try:
        c.drop()
    except Exception:  # noqa: BLE001 -- servers without drop (the pre-rewrite Warp)
        c.delete_many({})
    c.insert_one({"_id": -1})
    ins, fnd = [], []
    for i in range(a.n):
        t = time.perf_counter()
        c.insert_one({"_id": i, "val": i, "s": "x" * 50})
        ins.append((time.perf_counter() - t) * 1000)
    for i in range(a.n):
        t = time.perf_counter()
        c.find_one({"_id": i})
        fnd.append((time.perf_counter() - t) * 1000)
    for name, v in (("insertOne", ins), ("find_one(_id)", fnd)):
        v.sort()
        print("%-14s p50=%.3fms p90=%.3fms min=%.3fms mean=%.3fms" % (name, v[len(v) // 2], v[int(len(v) * .9)], v[0], statistics.mean(v)))
    c.delete_many({})


if __name__ == "__main__":
    main()
