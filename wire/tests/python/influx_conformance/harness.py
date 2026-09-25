"""Differential runner: replays corpus.CASES against real InfluxDB 1.8 and Warp influxwire and diffs the
normalised responses.

  python3 harness.py --oracle http://localhost:8086 --warp http://localhost:9999 [--filter substr] [--out results/x.json]
  python3 harness.py --oracle URL --start-warp [--shards 2]      # starts local-Postgres-backed Warp(s) itself

Verdicts per step: same | msg (same status+shape, only an error/message text differs) | diff.
"""
import argparse
import gzip
import json
import os
import re
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import corpus  # noqa: E402

TIMEOUT = 60
NOW_YEAR = str(time.gmtime().tm_year)


def dbname(case):
    return "hc_" + re.sub(r"[^a-z0-9_]", "_", case.lower())[:60]


def sub(x, db):
    if isinstance(x, str):
        return x.replace("{db}", db)
    if isinstance(x, dict):
        return {k: sub(v, db) for k, v in x.items()}
    return x


def do_request(base, method, path, params, body, headers, gz=False):
    h = dict(headers or {})
    data = body
    if gz and body is not None:
        data = gzip.compress(body.encode())
        h["Content-Encoding"] = "gzip"
    elif isinstance(body, str):
        data = body.encode("utf-8")
    try:
        r = requests.request(method, base + path, params=params or None, data=data, headers=h, timeout=TIMEOUT)
    except requests.RequestException as e:
        return {"status": -1, "error": type(e).__name__ + ": " + str(e)[:200]}
    return {"status": r.status_code, "ctype": (r.headers.get("Content-Type") or "").split(";")[0].strip(),
            "text": r.text, "hdr": {k: r.headers.get(k) for k in ("X-Influxdb-Version", "X-Influxdb-Build") if r.headers.get(k)}}


def run_step(base, step, db):
    kind = step[0]
    if kind in ("w", "wgz"):
        params = dict(sub(step[2], db)); params.setdefault("db", db)
        hdr = step[3] if len(step) > 3 else {}
        return do_request(base, "POST", "/write", params, sub(step[1], db), hdr, gz=(kind == "wgz"))
    if kind in ("q", "qh"):
        params = dict(sub(step[2], db))
        params["q"] = sub(step[1], db)
        params.setdefault("db", db)
        if kind == "q":
            method = step[3] if len(step) > 3 else "GET"
            hdr = {}
        else:
            method, hdr = "GET", step[3]
        if method == "POST":
            return do_request(base, "POST", "/query", params, None, hdr)
        return do_request(base, "GET", "/query", params, None, hdr)
    if kind == "raw":
        _, method, path, params, body, hdr = step
        return do_request(base, method, path, sub(params, db), sub(body, db) if isinstance(body, str) else body, hdr)
    raise ValueError(kind)


RFC = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")


def norm_json(o, opts, db):
    if isinstance(o, dict):
        o = {k: norm_json(v, opts, db) for k, v in o.items()}
        if "dbs" in opts and o.get("name") == "databases" and "values" in o:
            o["values"] = [v for v in o["values"] if v[0].startswith(db)]
        return o
    if isinstance(o, list):
        return [norm_json(v, opts, db) for v in o]
    if isinstance(o, str) and ("nowbuckets" in opts or "time" in opts) and RFC.fullmatch(o) and o.startswith(NOW_YEAR):
        return "<NOW>"
    return o


def normalise(resp, opts, db, raw_text):
    if resp.get("status") == -1:
        return resp
    out = {"status": resp["status"], "ctype": resp["ctype"]}
    text = resp["text"].replace(db, "<DB>")
    if "version" in opts:
        text = re.sub(r'"version":"[^"]*"', '"version":"<V>"', text)
    if raw_text:
        if "nowbuckets" in opts or "time" in opts:
            text = RFC.sub(lambda m: "<NOW>" if m.group(0).startswith(NOW_YEAR) else m.group(0), text)
        out["body"] = text
        return out
    try:
        j = json.loads(text)
        out["body"] = norm_json(j, opts, "<DB>")
        out["json"] = True
    except ValueError:
        out["body"] = text
    if "hdrs" in opts:
        out["hdr"] = resp["hdr"]
    return out


def shape(o):
    if isinstance(o, dict):
        return {k: shape(v) for k, v in o.items() if k not in ("error", "text", "messages")}
    if isinstance(o, list):
        return [shape(v) for v in o]
    return o


def verdict(a, b):
    if a == b:
        return "same"
    if a.get("status") == b.get("status") and a.get("json") and b.get("json") and shape(a["body"]) == shape(b["body"]):
        return "msg"
    return "diff"


def static_ignored(step, opts):
    """Steps whose oracle answer cannot be compared (server state, clocks, engine internals)."""
    txt = json.dumps(step)
    if any(s in txt for s in opts.get("skip_compare", [])):
        return True
    if any(k in txt for k in opts.get("accepted", {})):
        return True
    return any(k in txt for k, why in corpus.ACCEPT_GLOBAL.items() if why)


def record_golden(oracle, out_path, names=None):
    """Runs every case twice on the real InfluxDB and stores its (normalised) answers as the golden corpus.
    Steps whose two answers differ (clock / hash-order dependent) or that are statically ignored are stored as null."""
    import gzip as _gz
    golden = {}
    for name, (steps, opts) in corpus.CASES.items():
        if names and name not in names:
            continue
        db = dbname(name)
        runs = []
        for _ in range(2):
            do_request(oracle, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
            do_request(oracle, "POST", "/query", {"q": f"CREATE DATABASE {db}"}, None, {})
            runs.append([normalise(run_step(oracle, st, db), opts.get("norm", []), db, opts.get("raw_text", False)) for st in steps])
        do_request(oracle, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
        out = []
        for st, a, b in zip(steps, *runs):
            out.append(None if (a != b or static_ignored(st, opts)) else a)
        golden[name] = out
    with _gz.open(out_path, "wt") as f:
        json.dump(golden, f, separators=(",", ":"))
    return golden


def replay_golden(warp, name, golden_case):
    """Replays one case against Warp and returns the list of (step, expected, actual) mismatches (message-only
    differences in error texts count as mismatches too: the golden corpus is exact)."""
    steps, opts = corpus.CASES[name]
    db = dbname(name)
    do_request(warp, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
    do_request(warp, "POST", "/query", {"q": f"CREATE DATABASE {db}"}, None, {})
    bad = []
    for st, exp in zip(steps, golden_case):
        got = normalise(run_step(warp, st, db), opts.get("norm", []), db, opts.get("raw_text", False))
        if exp is not None and got != exp:
            bad.append((st, exp, got))
    do_request(warp, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
    return bad


def run_case(name, steps, opts, oracle, warp):
    db = dbname(name)
    res = []
    for base in (oracle, warp):
        do_request(base, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
        do_request(base, "POST", "/query", {"q": f"CREATE DATABASE {db}"}, None, {})
    skip = opts.get("skip_compare", [])
    raw_text = opts.get("raw_text", False)
    for i, step in enumerate(steps):
        o = run_step(oracle, step, db)
        w = run_step(warp, step, db)
        no, nw = normalise(o, opts.get("norm", []), db, raw_text), normalise(w, opts.get("norm", []), db, raw_text)
        v = verdict(no, nw)
        if any(s in json.dumps(step) for s in skip):
            v = "skipped"
        elif v != "same" and any(k in json.dumps(step) for k in opts.get("accepted", {})):
            v = "accepted"
        elif v != "same" and any(k in json.dumps(step) for k, why in corpus.ACCEPT_GLOBAL.items() if why):
            v = "accepted"
        res.append({"step": list(step), "verdict": v, "oracle": no, "warp": nw})
    for base in (oracle, warp):
        do_request(base, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle", required=True)
    ap.add_argument("--warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--show", default="diff", help="which verdicts to print in detail: diff|msg|all|none")
    ap.add_argument("--limit-detail", type=int, default=40)
    ap.add_argument("--dump", action="store_true", help="oracle only: print each step and the oracle response")
    ap.add_argument("--record", default="", help="oracle only: record the golden corpus (gzip JSON) used by test_influxwire_conformance.py")
    a = ap.parse_args()
    stop = None
    if a.record:
        g = record_golden(a.oracle, a.record, [a.filter] if a.filter else None)
        print(f"recorded {len(g)} cases, {sum(len(v) for v in g.values())} steps, {sum(1 for v in g.values() for x in v if x is None)} ignored")
        return
    if a.dump:
        for name, (steps, opts) in corpus.CASES.items():
            if a.filter and a.filter not in name:
                continue
            db = dbname(name)
            do_request(a.oracle, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
            do_request(a.oracle, "POST", "/query", {"q": f"CREATE DATABASE {db}"}, None, {})
            for i, st in enumerate(steps):
                r = normalise(run_step(a.oracle, st, db), opts.get("norm", []), db, opts.get("raw_text", False))
                print(f"## {name}#{i} {json.dumps(st)[:200]}\n   {r['status']} {json.dumps(r.get('body'))[:700]}")
            do_request(a.oracle, "POST", "/query", {"q": f"DROP DATABASE {db}"}, None, {})
        return
    if a.start_warp:
        from warp_start import start_warp
        a.warp, stop = start_warp(a.shards)
    try:
        counts, per_case, detail = {}, {}, []
        for name, (steps, opts) in corpus.CASES.items():
            if a.filter and a.filter not in name:
                continue
            res = run_case(name, steps, opts, a.oracle, a.warp)
            per_case[name] = res
            for i, r in enumerate(res):
                counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
                if r["verdict"] in (a.show, ) or a.show == "all" and r["verdict"] != "same":
                    detail.append((name, i, r))
        total = sum(counts.values())
        cases_pass = sum(1 for rs in per_case.values() if all(r["verdict"] in ("same", "skipped", "accepted") for r in rs))
        print(f"steps={total} " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" | cases {cases_pass}/{len(per_case)} fully identical")
        for name, i, r in detail[: a.limit_detail]:
            print(f"--- {name}#{i} [{r['verdict']}] {json.dumps(r['step'])[:300]}")
            print("   oracle:", json.dumps(r["oracle"])[:500])
            print("   warp  :", json.dumps(r["warp"])[:500])
        if a.out:
            os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
            with open(a.out, "w") as f:
                json.dump({"counts": counts, "cases_identical": cases_pass, "cases": len(per_case),
                           "failures": {n: [r for r in rs if r["verdict"] not in ("same", "skipped", "accepted")] for n, rs in per_case.items()
                                        if any(r["verdict"] not in ("same", "skipped", "accepted") for r in rs)}}, f, indent=1)
    finally:
        if stop:
            stop()


if __name__ == "__main__":
    main()
