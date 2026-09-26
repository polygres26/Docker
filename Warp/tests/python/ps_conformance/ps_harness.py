"""Differential harness: replays ps_corpus.py cases against Google's Pub/Sub emulator (oracle) and Warp (pubsubwire) and diffs the
normalised answers.

  python3 ps_harness.py --oracle localhost:PORT --start-warp [--shards 2] [--filter name] [--show diff|all]
  python3 ps_harness.py --oracle localhost:PORT --record golden.json.gz     # records the oracle (each case twice, unstable steps dropped)
Offline replay of golden.json.gz against a Warp is in ../test_pubsub_conformance.py (uses replay_golden()).
"""
import argparse
import base64
import datetime
import gzip
import json
import os
import random
import re
import string
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ps_client as C  # noqa: E402
import ps_corpus  # noqa: E402
from ps_known import KNOWN, MSG_FAMILIES, WARP_STRIP  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        ps_corpus.CASES[_n][_j]["known_diff"] = KNOWN[_k]
for _name, _steps in ps_corpus.CASES.items():
    for _i, _s in enumerate(_steps):
        _s["_case"], _s["_i"] = _name, _i

VAR = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
ISO = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")


class Norm:
    def __init__(self, run, project):
        self.run, self.project = run, project

    def text(self, s):
        if s is None:
            return s
        s = s.replace(self.run, "@R@").replace(self.project, "@P@")
        return ISO.sub("<ts>", s)

    def body(self, o, key=None):
        if isinstance(o, dict):
            return {k: self.body(v, k) for k, v in sorted(o.items())}
        if isinstance(o, list):
            return [self.body(x, key) for x in o]
        if isinstance(o, str):
            if key in ("message_id", "message_ids", "ack_id", "ack_ids", "revision_id", "revision_create_time", "next_page_token",
                       "publish_time", "expire_time", "etag"):
                return "<" + key + ">"
            return self.text(o)
        return o


def getpath(o, path):
    for p in path.split("."):
        o = o[int(p)] if isinstance(o, list) else o[p]
    return o


def data_of(m):
    raw = base64.b64decode(m.get("data", "")) if m.get("data") else b""
    return raw.decode("utf-8", "replace")


class Runner:
    def __init__(self, client, run, project):
        self.c = client
        self.vars = {"P": project, "R": run}
        self.norm = Norm(run, project)

    def sub(self, o):
        if isinstance(o, dict):
            return {k: self.sub(v) for k, v in o.items()}
        if isinstance(o, list):
            return [self.sub(v) for v in o]
        if isinstance(o, str):
            return VAR.sub(lambda m: str(self.vars.get(m.group(1), m.group(0))), o)
        return o

    def unary(self, name, req):
        code, msg, body, _, _ = self.c.call(name, self.sub(req))
        return code, msg, body

    def step(self, s):
        if s.get("sleep"):
            time.sleep(s["sleep"])
        n = self.norm
        if "rpc" in s:
            code, msg, body = self.unary(s["rpc"], s["req"])
            for var, path in (s.get("save") or {}).items():
                try:
                    self.vars[var] = getpath(body, path)
                except (KeyError, IndexError, TypeError):
                    self.vars[var] = "missing-" + var
            return {"code": code, "msg": n.text(msg), "body": n.body(body)}
        if "pull" in s:
            p = s["pull"]
            code, msg, body = self.unary("Pull", {"subscription": p["sub"], "max_messages": p["n"], "return_immediately": True})
            out = {"code": code, "msg": n.text(msg)}
            if body is not None:
                rms = body.get("received_messages", [])
                out["data"] = [data_of(r["message"]) for r in rms]
                out["attrs"] = [r["message"].get("attributes", {}) for r in rms]
                out["ordering_keys"] = [r["message"].get("ordering_key", "") for r in rms]
                out["attempts"] = [r.get("delivery_attempt", 0) for r in rms]
                out["has_ids"] = all(r["message"].get("message_id") and r["message"].get("publish_time") and r.get("ack_id") for r in rms)
                for var, path in (s.get("save") or {}).items():
                    try:
                        self.vars[var] = getpath(body, path)
                    except (KeyError, IndexError, TypeError):
                        self.vars[var] = "missing-" + var
                if s.get("ack", True) and rms:
                    ac, am, _ = self.unary("Acknowledge", {"subscription": p["sub"], "ack_ids": [r["ack_id"] for r in rms]})
                    out["ack"] = ac
            return out
        if "drain" in s:
            d = s["drain"]
            by_key, rounds = {}, 0
            for _ in range(d.get("rounds", 5)):
                code, msg, body = self.unary("Pull", {"subscription": d["sub"], "max_messages": 10, "return_immediately": True})
                rms = (body or {}).get("received_messages", [])
                for r in rms:
                    by_key.setdefault(r["message"].get("ordering_key", ""), []).append(data_of(r["message"]))
                if rms:
                    self.unary("Acknowledge", {"subscription": d["sub"], "ack_ids": [r["ack_id"] for r in rms]})
                time.sleep(0.1)
            return {"by_key": by_key}
        if "nack_loop" in s:
            d = s["nack_loop"]
            counts, attempts = [], []
            for _ in range(d["times"]):
                code, msg, body = self.unary("Pull", {"subscription": d["sub"], "max_messages": 10, "return_immediately": True})
                rms = (body or {}).get("received_messages", [])
                counts.append(len(rms))
                attempts.append([r.get("delivery_attempt", 0) for r in rms])
                if rms:
                    self.unary("ModifyAckDeadline", {"subscription": d["sub"], "ack_ids": [r["ack_id"] for r in rms],
                                                    "ack_deadline_seconds": 0})
                time.sleep(0.4)
            time.sleep(1.5)  # let a dead-letter forward happen
            return {"counts": counts, "attempts": attempts}
        if "stream" in s:
            d = self.sub(s["stream"])
            got, resps, err = self.c.streaming_pull(d["sub"], max_messages=d.get("max_messages"), timeout=d.get("timeout", 4.0),
                                                    ack=d.get("ack", False), max_outstanding=d.get("max_outstanding", 1000))
            props = None
            for r in resps:
                if r.HasField("subscription_properties"):
                    props = {"eod": r.subscription_properties.exactly_once_delivery_enabled,
                             "ordering": r.subscription_properties.message_ordering_enabled}
            return {"data": sorted(data_of({"data": base64.b64encode(m.message.data).decode()}) for m in got),
                    "err": [err[0], n.text(err[1])] if err else None, "props": props}
        if "mark" in s:
            self.vars[s["mark"]] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return {"marked": True}
        raise ValueError(s)


def run_case(client, name, steps, stamp=None):
    run = "".join(random.choice(string.ascii_lowercase) for _ in range(6))
    project = f"psc-{run}"
    r = Runner(client, run, project)
    out = []
    for s in steps:
        try:
            out.append(r.step(s))
        except Exception as e:  # noqa: BLE001 -- recorded as a result, compared like any other
            out.append({"harness_error": type(e).__name__ + ": " + str(e)[:200]})
    return out


def _strip(o):
    if isinstance(o, dict):
        return {k: _strip(v) for k, v in o.items() if not any(k == f and v == val for f, val in WARP_STRIP)}
    if isinstance(o, list):
        return [_strip(x) for x in o]
    return o


def _family(msg):
    for rx, name in MSG_FAMILIES:
        if msg is not None and rx.match(msg):
            return name
    return None


def diff_step(gold, got, step):
    """The first difference kind between an oracle result and Warp's: None, 'family' (same error, different words),
    'code', 'msg', 'body'."""
    if gold is None:
        return None  # unstable in the recording
    got = _strip(got)
    if gold.get("code") != got.get("code"):
        return "code"
    for k in sorted(set(gold) | set(got)):
        if k in ("code", "msg"):
            continue
        if gold.get(k) != got.get(k):
            return "body"
    if gold.get("msg") != got.get("msg"):
        fg, fw = _family(gold.get("msg")), _family(got.get("msg"))
        return "family" if fg and fg == fw else "msg"
    return None


def replay_golden(golden, client, only=None, log=None):
    """Runs every golden case against `client`; returns (identical, known, unexpected list of (case, step, kind, gold, got))."""
    identical = known = 0
    unexpected = []
    for name, steps in ps_corpus.CASES.items():
        if only and only != name:
            continue
        gold = golden["cases"].get(name)
        if gold is None:
            continue
        got = run_case(client, name, steps)
        for i, (g, w) in enumerate(zip(gold, got)):
            d = diff_step(g, w, steps[i])
            if d is None:
                identical += 1
            elif d == "family" or (steps[i].get("known_diff") and (steps[i]["known_diff"][0] == "any"
                                                                    or _covers(steps[i]["known_diff"][0], d))):
                known += 1
            else:
                unexpected.append((name, i, d, g, w))
    return identical, known, unexpected


def _covers(kind, d):
    order = ["msg", "body", "code"]
    return order.index(kind) >= order.index(d)


def record(client, path, only=None):
    cases = {}
    for name, steps in ps_corpus.CASES.items():
        if only and only not in name:
            continue
        a = run_case(client, name, steps)
        b = run_case(client, name, steps)
        cases[name] = [x if x == y else None for x, y in zip(a, b)]
        print(f"recorded {name}: {sum(1 for x in cases[name] if x is None)} unstable of {len(a)}", flush=True)
    with gzip.open(path, "wt") as f:
        json.dump({"cases": cases}, f, indent=0, sort_keys=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter")
    ap.add_argument("--show", default="diff")
    ap.add_argument("--record")
    a = ap.parse_args()
    if a.record:
        record(C.PsClient(a.oracle), a.record, a.filter)
        return
    warp = None
    target = a.warp
    if a.start_warp:
        import ps_launch_warp as L
        warp = L.PsWarp(shards=a.shards)
        target = f"localhost:{warp.grpc_port}"
    try:
        wc = C.PsClient(target)
        oc = C.PsClient(a.oracle)
        ident = kn = unexp = 0
        for name, steps in ps_corpus.CASES.items():
            if a.filter and a.filter not in name:
                continue
            g = run_case(oc, name, steps)
            w = run_case(wc, name, steps)
            for i, (x, y) in enumerate(zip(g, w)):
                d = diff_step(x, y, steps[i])
                known = steps[i].get("known_diff")
                if d is None:
                    ident += 1
                    if a.show == "all":
                        print(f"= {name}#{i} {steps[i].get('rpc', list(steps[i])[0])}")
                elif d == "family" or (known and (known[0] == "any" or _covers(known[0], d))):
                    kn += 1
                else:
                    unexp += 1
                    print(f"! {name}#{i} [{d}] {steps[i].get('rpc', list(steps[i])[0])}\n    oracle: {json.dumps(x)[:500]}\n    warp:   {json.dumps(y)[:500]}")
        print(f"identical={ident} known={kn} unexpected={unexp}")
    finally:
        if warp:
            warp.close()


if __name__ == "__main__":
    main()
