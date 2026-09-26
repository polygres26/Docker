"""Differential harness: replays ds_corpus.py cases (raw Datastore v1 gRPC and REST calls) against the official Datastore emulator
(oracle) and Warp's datastorewire and diffs the normalised answers.

  python3 ds_harness.py --oracle HOST:PORT --warp HOST:PORT [--filter substr] [--show diff|all]
  python3 ds_harness.py --oracle HOST:PORT --start-warp [--shards 2]
  python3 ds_harness.py --oracle HOST:PORT --record golden.json.gz      # records the oracle (each case twice, unstable steps dropped)
  python3 ds_harness.py --oracle HOST:PORT --oracle-only [--filter x]   # corpus discovery: print the oracle's answers

Offline replay of the golden file against a Warp is in ../test_datastore_conformance.py (uses replay_golden()).
A step is {"m": METHOD, "r": request-as-proto-JSON, ...}; strings "@P@" expand to the run's project and {"$ref": [step, "dotted.path"]}
takes a value from an earlier ANSWER of the same run (transaction ids, cursors, allocated keys, versions).
Compared: status code, message, entities, ordering, batch metadata. Not compared (Warp adds them like real Datastore, the emulator does
not): commitTime / createTime / updateTime / readTime and indexUpdates; entity versions, cursors and transaction ids are opaque.
"""
import argparse
import copy
import gzip
import json
import os
import re
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "fs_conformance", "gen"))

import grpc  # noqa: E402
import requests  # noqa: E402
from google.protobuf import json_format  # noqa: E402
from google.datastore.v1 import datastore_pb2 as DP, datastore_pb2_grpc as DG  # noqa: E402

import ds_corpus  # noqa: E402
from ds_known import KNOWN, MSG_ONLY  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        ds_corpus.CASES[_n][_j]["known_diff"] = True
for _k in MSG_ONLY:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        ds_corpus.CASES[_n][_j]["msg_only"] = True

MD = [("authorization", "Bearer owner")]
REQ = {n: getattr(DP, n + "Request") for n in ("Lookup", "RunQuery", "RunAggregationQuery", "BeginTransaction", "Commit", "Rollback",
                                                "AllocateIds", "ReserveIds")}
DROP_KEYS = {"commitTime", "createTime", "updateTime", "readTime", "indexUpdates", "moreResults", "snapshotVersion"}
OPAQUE = {"transaction", "startCursor", "endCursor", "skippedCursor", "cursor", "version", "baseVersion", "snapshotVersion",
          "previousTransaction"}


class Run:
    def __init__(self, host, case):
        self.host = host
        self.case = case
        self.project = "wt-" + uuid.uuid4().hex[:10]
        self.answers = []
        self.channel = grpc.insecure_channel(host, options=[("grpc.max_receive_message_length", 64 << 20)])
        self.stub = DG.DatastoreStub(self.channel)

    def close(self):
        self.channel.close()

    def sub(self, x):
        if isinstance(x, dict):
            if set(x) == {"$ref"}:
                return self.deref(x["$ref"])
            return {self.sub(k): self.sub(v) for k, v in x.items()}
        if isinstance(x, list):
            return [self.sub(v) for v in x]
        if isinstance(x, str):
            return x.replace("@P@", self.project)
        return x

    def deref(self, ref):
        step, path = ref
        cur = self.answers[step]
        for part in path.split("."):
            if part == "":
                continue
            cur = cur[int(part)] if isinstance(cur, list) else cur[part]
        return cur

    def step(self, s):
        m = s["m"]
        if m == "SLEEP":
            time.sleep(s["r"])
            return {"code": "OK"}
        if m == "REST":
            return self.rest(s)
        r = self.sub(s["r"])
        r.setdefault("projectId", self.project)
        req = json_format.ParseDict(r, REQ[m]())
        try:
            return {"code": "OK", "resp": json_format.MessageToDict(getattr(self.stub, m)(req, metadata=MD, timeout=30))}
        except grpc.RpcError as e:
            return {"code": e.code().name, "msg": e.details()}

    def rest(self, s):
        url = f"http://{self.host}" + self.sub(s["path"])
        body = self.sub(s.get("body"))
        r = requests.request(s["method"], url, json=body, timeout=30, headers={"Authorization": "Bearer owner"})
        try:
            data = r.json()
        except ValueError:
            data = r.text
        return {"code": f"HTTP{r.status_code}", "resp": data}


def run_case(host, name, steps):
    run = Run(host, name)
    out = []
    try:
        for s in steps:
            try:
                a = run.step(s)
            except Exception as e:  # noqa: BLE001
                a = {"code": "EXC", "msg": f"{type(e).__name__}: {e}"}
            run.answers.append(a.get("resp"))
            out.append(a)
        return run, out
    finally:
        run.close()


ISO = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z$")


def norm(run, x, key=""):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            if k in DROP_KEYS:
                continue
            if k == "path" and isinstance(v, list):
                out[k] = [_norm_elem(e) for e in v]
                continue
            out[k] = norm(run, v, k)
        return out
    if isinstance(x, list):
        return [norm(run, v, key) for v in x]
    if isinstance(x, str):
        if key in OPAQUE:
            return "<opaque>"
        return x.replace(run.project, "@P@")
    return x


def _norm_elem(e):
    e = dict(e)
    if str(e.get("kind", "")).startswith("Auto") and "id" in e:
        e["id"] = "<id>"
    return e


def canon(x):
    return json.dumps(x, sort_keys=True)


def normalize_answer(run, a):
    b = dict(a)
    if "resp" in b:
        b["resp"] = norm(run, b["resp"])
    if "msg" in b:
        b["msg"] = b["msg"].replace(run.project, "@P@")
    return b


def cases(filt=None):
    for name in sorted(ds_corpus.CASES):
        if not filt or any(f in name for f in filt.split(",")):
            yield name, ds_corpus.CASES[name]


def run_norm(host, name, steps):
    run, raws = run_case(host, name, steps)
    return [normalize_answer(run, r) for r in raws]


def stable_record(oracle, name, steps):
    a = run_norm(oracle, name, steps)
    b = run_norm(oracle, name, steps)
    return [x if canon(x) == canon(y) else None for x, y in zip(a, b)]


def strip_msgs(x):
    if isinstance(x, dict):
        return {k: strip_msgs(v) for k, v in x.items() if k not in ("msg", "message")}
    if isinstance(x, list):
        return [strip_msgs(v) for v in x]
    return x


def brief(a, n=900):
    if a is None:
        return "None"
    r = a.get("resp")
    if isinstance(r, dict) and "batch" in r and isinstance(r["batch"], dict) and "entityResults" in r["batch"]:
        b = r["batch"]
        ents = []
        for e in b.get("entityResults", []):
            k = e["entity"]["key"]["path"]
            ents.append("/".join(str(p.get("name", p.get("id", "?"))) if i == len(k) - 1 else f'{p["kind"]}:{p.get("name", p.get("id"))}'
                                 for i, p in enumerate(k)) + ("" if "properties" in e["entity"] else "(k)"))
        meta = {k: v for k, v in b.items() if k not in ("entityResults", "endCursor", "snapshotVersion")}
        return json.dumps({"code": a["code"], "keys": ents, "batch": meta})[:n]
    return json.dumps(a)[:n]


def compare(step, exp, got):
    if exp is None:
        return None
    if canon(exp) == canon(got):
        return None
    if step.get("msg_only") and canon(strip_msgs(exp)) == canon(strip_msgs(got)):
        return "msg"
    if exp.get("code") == got.get("code") and canon({k: v for k, v in exp.items() if k != "msg"}) == canon(
            {k: v for k, v in got.items() if k != "msg"}):
        return "msg"
    return "diff"


def replay_golden(host, name, golden):
    """Offline: Warp's answers to a case against the recorded oracle answers. Returns [(step, expected, got)] of real differences."""
    steps = ds_corpus.CASES[name]
    got = run_norm(host, name, steps)
    bad = []
    for i, (st, exp, g) in enumerate(zip(steps, golden, got)):
        kind = compare(st, exp, g)
        if kind is None or st.get("known_diff"):
            continue
        if kind == "msg" and st.get("msg_only"):
            continue
        bad.append((f"{name}#{i} {st['m']} {json.dumps(st.get('r', st.get('path', '')))[:200]}", exp, g))
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter")
    ap.add_argument("--show", default="diff")
    ap.add_argument("--record")
    ap.add_argument("--oracle-only", action="store_true")
    a = ap.parse_args()
    warp = None
    if a.start_warp:
        sys.path.insert(0, os.path.join(HERE, "..", "fs_conformance"))
        import fs_launch_warp
        warp = fs_launch_warp.FsWarp(a.shards, store_env="WARP_DATASTOREWIRE_PORT", stores=["datastore"])
        a.warp = warp.host
    try:
        if a.record:
            golden = {}
            for name, steps in cases(a.filter):
                golden[name] = stable_record(a.oracle, name, steps)
                print(name, "recorded", sum(1 for x in golden[name] if x is None), "unstable", flush=True)
            with gzip.open(a.record, "wt") as f:
                json.dump(golden, f, sort_keys=True)
            return
        totals = {"steps": 0, "same": 0, "msg": 0, "diff": 0, "known": 0}
        for name, steps in cases(a.filter):
            o = run_norm(a.oracle, name, steps) if a.oracle else None
            w = run_norm(a.warp, name, steps) if a.warp and not a.oracle_only else None
            for i, st in enumerate(steps):
                totals["steps"] += 1
                if a.oracle_only:
                    print(f"--- {name}#{i} {st['m']} {json.dumps(st.get('r', st.get('path', '')))[:230]}\n   ORACLE {brief(o[i], 1200)}")
                    continue
                kind = compare(st, o[i], w[i])
                if kind is None:
                    totals["same"] += 1
                    if a.show == "all":
                        print(f"ok   {name}#{i} {st['m']}")
                    continue
                if st.get("known_diff"):
                    totals["known"] += 1
                    continue
                if kind == "msg" and st.get("msg_only"):
                    totals["msg"] += 1
                    continue
                totals[kind] += 1
                print(f"{kind.upper():4} {name}#{i} {st['m']} {json.dumps(st.get('r', st.get('path', '')))[:300]}\n   oracle {brief(o[i])}\n   warp   {brief(w[i])}")
        print(json.dumps(totals))
    finally:
        if warp:
            warp.close()


if __name__ == "__main__":
    main()
