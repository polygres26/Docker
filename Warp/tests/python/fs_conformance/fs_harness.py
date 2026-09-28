"""Differential harness: replays fs_corpus.py cases (raw Firestore v1 gRPC and REST calls) against the official Firestore emulator
(oracle) and Warp's firestorewire and diffs the normalised answers (status code, message, documents, ordering).

  python3 fs_harness.py --oracle HOST:PORT --warp HOST:PORT [--filter substr] [--show diff|all]
  python3 fs_harness.py --oracle HOST:PORT --start-warp [--shards 2]
  python3 fs_harness.py --oracle HOST:PORT --record golden.json.gz      # records the oracle (each case twice, unstable steps dropped)

Offline replay of the golden file against a Warp is in ../test_firestore_conformance.py (uses replay_golden()).
A step is {"m": METHOD, "r": request-as-proto-JSON, ...}; strings "@DB@" / "@P@" expand to the case's database / project and
{"$ref": [step, "dotted.path"]} takes a value from an earlier ANSWER of the same run (timestamps, transaction ids).
"""
import argparse
import base64
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
sys.path.insert(0, os.path.join(HERE, "gen"))

import grpc  # noqa: E402
import requests  # noqa: E402
from google.protobuf import json_format  # noqa: E402
from google.firestore.v1 import firestore_pb2 as F, firestore_pb2_grpc as G  # noqa: E402

import fs_corpus  # noqa: E402
from fs_known import KNOWN, MSG_ONLY  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        fs_corpus.CASES[_n][_j]["known_diff"] = True
for _k in MSG_ONLY:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        fs_corpus.CASES[_n][_j]["msg_only"] = True

STREAMING = {"BatchGetDocuments", "RunQuery", "RunAggregationQuery"}
MD = [("authorization", "Bearer owner")]

REQ = {name: getattr(F, name + "Request") for name in (
    "GetDocument", "ListDocuments", "CreateDocument", "UpdateDocument", "DeleteDocument", "BatchGetDocuments", "BeginTransaction",
    "Commit", "Rollback", "RunQuery", "RunAggregationQuery", "PartitionQuery", "ListCollectionIds", "BatchWrite")}

ISO = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z$")
AUTOID = re.compile(r"^[A-Za-z0-9]{20}$")


# ------------------------------------------------------------------------------------------------------------- execution

class Run:
    """One execution of a case against one target: the project (isolation), and the raw answers for $ref."""

    def __init__(self, host, case):
        self.host = host
        self.case = case
        self.project = "wt-" + uuid.uuid4().hex[:10]
        self.db = f"projects/{self.project}/databases/(default)"
        self.answers = []
        self.channel = grpc.insecure_channel(host, options=[("grpc.max_receive_message_length", 64 << 20)])
        self.stub = G.FirestoreStub(self.channel)
        self.now = time.time()

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
            return x.replace("@DB@", self.db).replace("@P@", self.project)
        return x

    def deref(self, ref):
        step, path = ref
        cur = self.answers[step]
        for part in path.split("."):
            if part == "":
                continue
            cur = cur[int(part)] if isinstance(cur, list) else cur[part]
        return cur

    # each step returns a JSON-able dict: {"code": "OK"|..., "msg": ..., "resp": ...}
    def step(self, s):
        m = s["m"]
        if m == "SLEEP":
            time.sleep(s["r"])
            return {"code": "OK"}
        if m == "REST":
            return self.rest(s)
        if m == "LISTEN":
            return listen_step(self, s)
        if m == "WRITESTREAM":
            return write_stream_step(self, s)
        req = json_format.ParseDict(self.sub(s["r"]), REQ[m]())
        try:
            call = getattr(self.stub, m)
            if m in STREAMING:
                out = [json_format.MessageToDict(x) for x in call(req, metadata=MD, timeout=30)]
                return {"code": "OK", "resp": out}
            return {"code": "OK", "resp": json_format.MessageToDict(call(req, metadata=MD, timeout=30))}
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
        for i, s in enumerate(steps):
            try:
                a = run.step(s)
            except Exception as e:  # noqa: BLE001 -- a harness/protocol failure is data too
                a = {"code": "EXC", "msg": f"{type(e).__name__}: {e}"}
            run.answers.append(a.get("resp"))
            out.append({"raw": a, "run": run})
        return run, [x["raw"] for x in out]
    finally:
        run.close()


# ------------------------------------------------------------------------------------------------------------- normalisation

def norm(run, x, key=""):
    if isinstance(x, dict):
        return {k: norm(run, v, k) for k, v in x.items()}
    if isinstance(x, list):
        return [norm(run, v, key) for v in x]
    if isinstance(x, str):
        x = x.replace(run.project, "@P@")
        if key in ("transaction", "newTransaction") or key.endswith("Token") and key != "nextPageToken":
            return "<opaque>"
        if key == "streamId":
            return "<sid>"
        if key == "nextPageToken":
            return "<page-token>" if x else x
        if key.endswith("Time") and ISO.match(x) or key == "timestampValue" and ISO.match(x) and abs(_epoch(x) - run.now) < 86400:
            return "<ts>"
        parts = x.split("/")
        if len(parts) > 1 and AUTOID.match(parts[-1]) and re.search(r"[A-Za-z]", parts[-1]):
            parts[-1] = "<id>"
            return "/".join(parts)
        return re.sub(r"\((\d{15,})\)", "(<ver>)", x)
    return x


def _epoch(s):
    import datetime
    s = s.rstrip("Z")
    base, _, frac = s.partition(".")
    t = datetime.datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
    return t


def canon(x):
    """Order-insensitive canonical form for maps, ordered for lists."""
    return json.dumps(x, sort_keys=True)


def normalize_answer(run, a):
    b = dict(a)
    if "resp" in b:
        b["resp"] = norm(run, b["resp"])
    if "msg" in b:
        b["msg"] = norm(run, b["msg"])
    return b


# ------------------------------------------------------------------------------------------------------------- streams

def listen_step(run, s):
    """A LISTEN step: {"m": "LISTEN", "script": [...]}: each script item is
    {"send": ListenRequest-json} | {"recv": N, "timeout": s} (collect N responses) | {"do": step} (run another step meanwhile)
    | {"sleep": s}. The answer is the list of collected ListenResponses (targetChange readTime/resumeToken normalised)."""
    import queue
    import threading
    q_out = queue.Queue()
    got = []
    req_q = queue.Queue()
    done = object()

    def gen():
        while True:
            x = req_q.get()
            if x is done:
                return
            yield x

    call = run.stub.Listen(gen(), metadata=MD + [("google-cloud-resource-prefix", run.db)], timeout=60)

    def reader():
        try:
            for r in call:
                q_out.put(json_format.MessageToDict(r))
        except grpc.RpcError as e:
            q_out.put({"_error": e.code().name, "_msg": e.details()})
        q_out.put(None)

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    try:
        for item in s["script"]:
            if "send" in item:
                req_q.put(json_format.ParseDict(run.sub(item["send"]), F.ListenRequest()))
            elif "sleep" in item:
                time.sleep(item["sleep"])
            elif "do" in item:
                r = run.step(item["do"])
                run.answers.append(r.get("resp"))
            elif "recv" in item:
                deadline = time.time() + item.get("timeout", 8)
                n = item["recv"]
                while (n is None or n > 0) and time.time() < deadline:
                    try:
                        x = q_out.get(timeout=max(0.05, deadline - time.time()))
                    except queue.Empty:
                        break
                    if x is None:
                        break
                    got.append(x)
                    if n is not None:
                        n -= 1
                    if n is None and "until_quiet" in item:
                        deadline = time.time() + item["until_quiet"]
    finally:
        req_q.put(done)
        call.cancel()
    return {"code": "OK", "resp": got}


def write_stream_step(run, s):
    import queue
    req_q = queue.Queue()
    done = object()

    def gen():
        while True:
            x = req_q.get()
            if x is done:
                return
            yield x

    call = run.stub.Write(gen(), metadata=MD + [("google-cloud-resource-prefix", run.db)], timeout=60)
    out = []
    try:
        state = {}
        for item in s["script"]:
            d = copy.deepcopy(item)
            d = run.sub(d)
            if d.get("streamToken") == "$last":
                d["streamToken"] = state.get("token", "")
            if d.get("streamId") == "$last":
                d["streamId"] = state.get("id", "")
            req_q.put(json_format.ParseDict(d, F.WriteRequest()))
            try:
                r = next(call)
            except StopIteration:
                out.append({"_end": True})
                break
            md = json_format.MessageToDict(r)
            state["token"] = md.get("streamToken", "")
            state["id"] = md.get("streamId", "")
            out.append(md)
    except grpc.RpcError as e:
        out.append({"_error": e.code().name, "_msg": e.details()})
    finally:
        req_q.put(done)
        call.cancel()
    return {"code": "OK", "resp": out}


# ------------------------------------------------------------------------------------------------------------- compare

def cases(filt=None):
    for name in sorted(fs_corpus.CASES):
        if not filt or any(f in name for f in filt.split(",")):
            yield name, fs_corpus.CASES[name]


def run_norm(host, name, steps):
    run, raws = run_case(host, name, steps)
    return [normalize_answer(run, r) for r in raws]


def stable_record(oracle, name, steps):
    """The oracle answers twice (fresh projects); steps whose normalised answer differs between the runs are dropped (None)."""
    a = run_norm(oracle, name, steps)
    b = run_norm(oracle, name, steps)
    return [x if canon(x) == canon(y) else None for x, y in zip(a, b)]


def brief(a, n=900):
    """A compact one-line rendering of an answer (documents as their last path segment) for reports."""
    if a is None:
        return "None"
    r = a.get("resp")
    if isinstance(r, list) and r and all(isinstance(x, dict) and ("document" in x or "found" in x or "missing" in x or "readTime" in x
                                                                    or "result" in x) for x in r):
        parts = []
        extra = []
        for x in r:
            d = x.get("document") or x.get("found")
            if d is not None:
                parts.append(d["name"].split("/documents/")[-1] + ("" if "fields" in d or True else ""))
            elif "missing" in x:
                parts.append("MISSING:" + x["missing"].split("/documents/")[-1])
            elif "result" in x:
                parts.append(json.dumps(x["result"], sort_keys=True))
            else:
                extra.append({k: v for k, v in x.items() if k != "readTime"} or "readTime-only")
            for k in ("skippedResults", "transaction", "done"):
                if k in x:
                    extra.append({k: x[k]})
        out = {"code": a["code"], "docs": parts}
        if extra:
            out["extra"] = extra
        return json.dumps(out)[:n]
    return json.dumps(a)[:n]


def strip_msgs(x):
    if isinstance(x, dict):
        return {k: strip_msgs(v) for k, v in x.items() if k not in ("msg", "message", "_msg")}
    if isinstance(x, list):
        return [strip_msgs(v) for v in x]
    return x


def compare(step, exp, got):
    """None if equal; 'msg' if only the message differs; else 'diff'."""
    if exp is None:
        return None
    if canon(exp) == canon(got):
        return None
    if step.get("msg_only") and canon(strip_msgs(exp)) == canon(strip_msgs(got)):
        return "msg"
    if exp.get("code") == got.get("code") and canon({k: v for k, v in exp.items() if k != "msg"}) == canon({k: v for k, v in got.items() if k != "msg"}):
        return "msg"
    return "diff"


def replay_golden(host, name, golden):
    """Offline: Warp's answers to a case against the recorded oracle answers. Returns [(step, expected, got)] of real differences."""
    steps = fs_corpus.CASES[name]
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
    ap.add_argument("--oracle-only", action="store_true", help="print the oracle's answers (corpus discovery)")
    a = ap.parse_args()
    warp = None
    if a.start_warp:
        import fs_launch_warp
        warp = fs_launch_warp.FsWarp(a.shards)
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
