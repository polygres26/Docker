"""Differential harness: replays bt_corpus.py cases against Google's Bigtable emulator (oracle) and Warp (bigtablewire) and diffs the
normalised answers.

  python3 bt_harness.py --oracle localhost:PORT --start-warp [--shards 2] [--filter name] [--show diff|all]
  python3 bt_harness.py --oracle localhost:PORT --record golden.json.gz     # records the oracle (each case twice, unstable steps dropped)
Offline replay of golden.json.gz against a Warp is in ../test_bigtable_conformance.py (uses replay_golden()).

Normalisation: ReadRows chunk streams are assembled into rows (the emulator repeats the row key on every chunk, real Bigtable
sends it once: both are accepted) and the cells of a row are put in the canonical (family, qualifier, timestamp descending)
order, because the emulator returns families in random map order; MutateRows answers become (index, code, message) triples;
SampleRowKeys is compared structurally (the emulator's sampling positions are its own); server timestamps become <now>.
"""
import argparse
import base64
import gzip
import hashlib
import json
import os
import random
import re
import string
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bt_client as C  # noqa: E402
import bt_corpus  # noqa: E402
from bt_known import KNOWN, MSG_FAMILIES, WARP_STRIP  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        bt_corpus.CASES[_n][_j]["known_diff"] = KNOWN[_k]
for _name, _steps in bt_corpus.CASES.items():
    for _i, _s in enumerate(_steps):
        _s["_case"], _s["_i"] = _name, _i

VAR = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
ISO = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")


def rep(raw):
    """A readable, stable rendering of bytes."""
    if len(raw) > 200:
        return f"<{len(raw)} bytes sha1={hashlib.sha1(raw).hexdigest()[:12]}>"
    try:
        s = raw.decode("utf-8")
        if all(ch.isprintable() or ch in "\t\n" for ch in s):
            return s
    except UnicodeDecodeError:
        pass
    return "0x" + raw.hex()


def near_now(ts):
    return abs(int(ts) - time.time() * 1e6) < 600e6


class Norm:
    def __init__(self, run, project):
        self.run, self.project = run, project

    def text(self, s):
        if s is None:
            return s
        s = s.replace(self.run, "@R@").replace(self.project, "@P@")
        return ISO.sub("<ts>", s)

    def ts(self, v):
        return "<now>" if near_now(v) else int(v)

    def body(self, o, key=None):
        if isinstance(o, dict):
            return {k: self.body(v, k) for k, v in sorted(o.items())}
        if isinstance(o, list):
            return [self.body(x, key) for x in o]
        if isinstance(o, str):
            if key in ("start_time", "end_time"):
                return "<ts>"
            if key == "timestamp_micros":
                return self.ts(o)
            return self.text(o)
        return o


def getpath(o, path):
    for p in path.split("."):
        o = o[int(p)] if isinstance(o, list) else o[p]
    return o


def assemble(chunks_by_msg):
    """CellChunk dict stream -> ([row dict], protocol error or None). Accepts the emulator's repeated row keys."""
    rows, cur, err = [], None, None
    fam = qual = None
    pieces, cell, in_split = [], None, False
    for msg in chunks_by_msg:
        for ch in msg.get("chunks", []):
            if ch.get("reset_row"):
                cur, pieces, in_split = None, [], False
                continue
            key = base64.b64decode(ch["row_key"]) if ch.get("row_key") else None
            if cur is None:
                cur = {"k": key or b"", "cells": []}
                fam = qual = None
            elif key is not None and key != cur["k"]:
                err = "row key changed inside an uncommitted row"
                cur = {"k": key, "cells": []}
            is_cell = in_split or any(k in ch for k in ("family_name", "qualifier", "value", "timestamp_micros", "labels", "value_size"))
            if is_cell:
                if not in_split:
                    if "family_name" in ch:
                        fam = ch["family_name"]
                    if "qualifier" in ch:
                        qual = base64.b64decode(ch["qualifier"])
                    if fam is None:
                        err = "chunk without a family"
                    cell = (fam, qual or b"", int(ch.get("timestamp_micros", 0) or 0), list(ch.get("labels", [])))
                pieces.append(base64.b64decode(ch.get("value", "")))
                if int(ch.get("value_size", 0) or 0) > 0:
                    in_split = True
                    continue
                in_split = False
                cur["cells"].append((cell[0], cell[1], cell[2], b"".join(pieces), cell[3]))
                pieces = []
            if ch.get("commit_row"):
                rows.append(cur)
                cur = None
    if cur is not None:
        err = err or "stream ended inside a row"
    return rows, err


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

    def step(self, s):
        if s.get("sleep"):
            time.sleep(s["sleep"])
        n = self.norm
        name = s["rpc"]
        code, msg, body = self.c.call(name, self.sub(s["req"]), timeout=120)
        out = {"code": code, "msg": n.text(msg)}
        if code != "OK":
            return out
        if name == "ReadRows":
            rows, err = assemble(body)
            rows_out = []
            for r in rows:
                cells = sorted(r["cells"], key=lambda c: (c[0] or "", c[1], -c[2]))  # stable: ties keep filter order
                rows_out.append({"k": rep(r["k"]), "cells": [[c[0], rep(c[1]), n.ts(c[2]) if c[2] else 0, rep(c[3]), c[4]] for c in cells]})
            out["rows"] = rows_out
            if err:
                out["proto_err"] = err
        elif name == "MutateRows":
            ents = sorted((int(e.get("index", 0) or 0), int(e.get("status", {}).get("code", 0) or 0), n.text(e.get("status", {}).get("message", "")))
                          for m in body for e in m.get("entries", []))
            out["entries"] = [list(e) for e in ents]
        elif name == "SampleRowKeys":
            keys = [base64.b64decode(m.get("row_key", "")) for m in body]
            offs = [int(m.get("offset_bytes", 0) or 0) for m in body]
            nonempty = keys[:-1]
            out["sample"] = {"ends_with_empty_key": bool(keys) and keys[-1] == b"",
                             "keys_ascending": all(a < b for a, b in zip(nonempty, nonempty[1:])),
                             "offsets_nondecreasing": all(a <= b for a, b in zip(offs, offs[1:]))}
        else:
            b = json.loads(json.dumps(body))
            if s.get("names"):
                b["tables"] = sorted((t for t in b.get("tables", []) if "-" + self.vars["R"] in t["name"]), key=lambda t: t["name"])
            if name == "ReadModifyWriteRow":  # the emulator answers families in random map order
                b.get("row", {}).get("families", []).sort(key=lambda f: f["name"])
            if name == "UpdateTable" and "name" in b:
                b["name"] = "<op>"
            out["body"] = n.body(b)
            for var, path in (s.get("save") or {}).items():
                try:
                    self.vars[var] = getpath(body, path)
                except (KeyError, IndexError, TypeError):
                    self.vars[var] = "missing-" + var
        return out


def run_case(client, name, steps):
    run = "".join(random.choice(string.ascii_lowercase) for _ in range(6))
    project = f"btc-{run}"
    r = Runner(client, run, project)
    out = []
    for s in steps:
        try:
            out.append(r.step(s))
        except Exception as e:  # noqa: BLE001 -- recorded as a result, compared like any other
            out.append({"harness_error": type(e).__name__ + ": " + str(e)[:200]})
    return out


def _strip(o, rpc):
    if isinstance(o, dict):
        return {k: _strip(v, rpc) for k, v in o.items()
                if not any(k == f and v == val and rpc in rpcs for f, val, rpcs in WARP_STRIP)}
    if isinstance(o, list):
        return [_strip(x, rpc) for x in o]
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
    got = _strip(got, step["rpc"])
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


def _covers(kind, d):
    order = ["msg", "body", "code"]
    return order.index(kind) >= order.index(d)


def classify(step, d):
    """'identical' | 'known' | 'unexpected' for a difference kind d (None = identical)."""
    if d is None:
        return "identical"
    known = step.get("known_diff")
    if d == "family" or (known and (known[0] == "any" or _covers(known[0], d))):
        return "known"
    return "unexpected"


def replay_golden(golden, client, only=None):
    """Runs every golden case against `client`; returns (identical, known, unexpected list of (case, step, kind, gold, got))."""
    identical = known = 0
    unexpected = []
    for name, steps in bt_corpus.CASES.items():
        if only and only != name:
            continue
        gold = golden["cases"].get(name)
        if gold is None:
            continue
        got = run_case(client, name, steps)
        for i, (g, w) in enumerate(zip(gold, got)):
            d = diff_step(g, w, steps[i])
            c = classify(steps[i], d)
            if g is None:
                continue
            if c == "identical":
                identical += 1
            elif c == "known":
                known += 1
            else:
                unexpected.append((name, i, d, g, w))
    return identical, known, unexpected


def record(client, path, only=None, times=2):
    cases = {}
    for name, steps in bt_corpus.CASES.items():
        if only and only not in name:
            continue
        runs = [run_case(client, name, steps) for _ in range(times)]
        cases[name] = [runs[0][i] if all(r[i] == runs[0][i] for r in runs[1:]) else None for i in range(len(steps))]
        print(f"recorded {name}: {sum(1 for x in cases[name] if x is None)} unstable of {len(steps)}", flush=True)
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
    ap.add_argument("--times", type=int, default=2)
    a = ap.parse_args()
    if a.record:
        record(C.BtClient(a.oracle), a.record, a.filter, a.times)
        return
    warp = None
    target = a.warp
    if a.start_warp:
        import bt_launch_warp as L
        warp = L.BtWarp(shards=a.shards)
        target = f"localhost:{warp.grpc_port}"
    try:
        wc = C.BtClient(target)
        oc = C.BtClient(a.oracle)
        ident = kn = unexp = 0
        for name, steps in bt_corpus.CASES.items():
            if a.filter and a.filter not in name:
                continue
            g = run_case(oc, name, steps)
            w = run_case(wc, name, steps)
            for i, (x, y) in enumerate(zip(g, w)):
                d = diff_step(x, y, steps[i])
                c = classify(steps[i], d)
                if c == "identical":
                    ident += 1
                    if a.show == "all":
                        print(f"= {name}#{i} {steps[i]['rpc']}")
                elif c == "known":
                    kn += 1
                else:
                    unexp += 1
                    print(f"! {name}#{i} [{d}] {steps[i]['rpc']} {json.dumps(steps[i]['req'])[:300]}\n    oracle: {json.dumps(x)[:700]}\n    warp:   {json.dumps(y)[:700]}")
        print(f"identical={ident} known={kn} unexpected={unexp}")
    finally:
        if warp:
            warp.close()


if __name__ == "__main__":
    main()
