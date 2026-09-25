#!/usr/bin/env python3
"""Differential harness: the same request sequences against a REAL OpenSearch and against Warp (oswire), with
normalised comparison of status codes, error types and response shapes.

  diff_harness.py --oracle http://localhost:9200 --warp http://localhost:19200 [--record golden/oracle.json] [--filter txt]
  diff_harness.py --golden golden/oracle.json --warp http://localhost:19200          # no OpenSearch needed

`--record` stores the oracle's normalised responses so Warp can later be checked against them without Docker
(tests/python/test_oswire_conformance.py does exactly that). Cases live in os_corpus.py.
"""
import argparse
import json
import math
import os
import re
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import os_corpus as corpus  # noqa: E402
import known  # noqa: E402

VOLATILE = {"took", "uuid", "index_uuid", "creation_date", "cluster_uuid", "timestamp", "epoch", "_scroll_id", "pit_id", "id",
            "pit_id", "creation_time", "keep_alive", "build_hash", "build_date", "cluster_name", "node", "name", "tagline",
            "took_ms", "max_score", "_seq_no", "_primary_term", "seq_no", "primary_term", "shard", "node_name", "version_created"}
AUTO_ID = re.compile(r"^[A-Za-z0-9_-]{20}$")


class Client:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.s = requests.Session()

    def call(self, method, path, body=None, params=None, ndjson=False):
        headers = {"Content-Type": "application/x-ndjson" if ndjson else "application/json"}
        data = None
        if body is not None:
            if ndjson:
                data = "".join((x if isinstance(x, str) else json.dumps(x)) + "\n" for x in body)
            elif isinstance(body, str):
                data = body
            else:
                data = json.dumps(body)
        r = self.s.request(method, self.url + path, params=params, data=data.encode() if data else None, headers=headers, timeout=120)
        ctype = r.headers.get("content-type", "")
        try:
            js = r.json() if r.content and "json" in ctype else (r.text if r.content else None)
        except ValueError:
            js = r.text
        return r.status_code, js

    def reset(self):
        st, rows = self.call("GET", "/_cat/indices", params={"format": "json", "h": "index", "expand_wildcards": "all"})
        if st == 200 and isinstance(rows, list):
            for row in rows:
                if not row["index"].startswith("."):
                    self.call("DELETE", "/" + row["index"], params={"ignore_unavailable": "true"})
        for kind, key in (("_template", None), ("_index_template", "index_templates"), ("_component_template", "component_templates")):
            st, js = self.call("GET", "/" + kind)
            if st == 200 and isinstance(js, dict):
                names = list(js.keys()) if key is None else [t["name"] for t in js.get(key, [])]
                for n in names:
                    if n.startswith(("query_insights", ".")) or n in ("ml-commons-all",):
                        continue
                    self.call("DELETE", "/%s/%s" % (kind, n))


def shape(v, depth):
    """Structure only: dict keys down to `depth`, leaves replaced by their JSON type."""
    if isinstance(v, dict):
        return {k: shape(x, depth - 1) for k, x in v.items() if k not in ("took", "timestamp", "cluster_uuid", "cluster_name")} if depth > 0 \
            else "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


def normalise(v, opts, path=""):
    """Volatile fields removed, auto ids masked, floats rounded; keeps structure."""
    if isinstance(v, dict):
        if v.get("type") == "json_parse_exception" and "reason" in v:
            v = {k: x for k, x in v.items() if k != "reason"}
        out = {}
        for k, x in v.items():
            if k in VOLATILE and not (k == "name" and path.endswith("aliases")):
                continue
            if k == "_score" and not opts.get("scores"):
                continue
            if k == "_shards" and not opts.get("shards"):
                continue
            if k == "sort" and opts.get("nosort"):
                continue
            if k == "_id" and isinstance(x, str) and AUTO_ID.match(x):
                out[k] = "<auto-id>"
                continue
            out[k] = normalise(x, opts, path + "." + k)
        return out
    if isinstance(v, list):
        items = [normalise(x, opts, path + "[]") for x in v]
        if path.endswith("hits.hits") and not opts.get("ordered"):
            items.sort(key=lambda h: json.dumps(h.get("_id"), sort_keys=True) if isinstance(h, dict) else str(h))
        if path.endswith("items") and not opts.get("ordered", True):
            pass
        return items
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return str(v)
        return round(v, 6) if opts.get("scores") or abs(v) > 1e-3 else v
    return v


def reason_key(js):
    """Comparable core of an error response: status + type (+ root cause type)."""
    if isinstance(js, dict) and "error" in js and isinstance(js["error"], dict):
        e = js["error"]
        rc = [c.get("type") for c in e.get("root_cause", [])]
        return {"error": {"type": e.get("type"), "root_cause_types": rc}, "status": js.get("status")}
    return js


def diff(a, b, path="", tol=1e-5, out=None):
    out = [] if out is None else out
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append("%s.%s: oracle=<missing> warp=%s" % (path, k, trunc(b[k])))
            elif k not in b:
                out.append("%s.%s: oracle=%s warp=<missing>" % (path, k, trunc(a[k])))
            else:
                diff(a[k], b[k], path + "." + k, tol, out)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append("%s: length oracle=%d warp=%d (%s | %s)" % (path, len(a), len(b), trunc(a), trunc(b)))
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, "%s[%d]" % (path, i), tol, out)
    elif isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        if abs(a - b) > tol * max(1.0, abs(a), abs(b)):
            out.append("%s: oracle=%s warp=%s" % (path, a, b))
    elif a != b:
        out.append("%s: oracle=%s warp=%s" % (path, trunc(a), trunc(b)))
    return out


def trunc(x, n=160):
    s = json.dumps(x, sort_keys=True) if not isinstance(x, str) else x
    return s if len(s) <= n else s[:n] + "..."


def subst(v, ctx):
    if isinstance(v, str) and v.startswith("$") and v[1:] in ctx:
        return ctx[v[1:]]
    if isinstance(v, dict):
        return {k: subst(x, ctx) for k, x in v.items()}
    if isinstance(v, list):
        return [subst(x, ctx) for x in v]
    return v


def run_steps(client, steps):
    """Runs a case's steps; returns a list of normalised (status, body) for every step marked to be compared.
    `$scroll_id` / `$pit_id` in a later step's body or path refer to the value returned by an earlier step."""
    results = []
    ctx = {}
    for st in steps:
        method, path = st["method"], st["path"]
        for k, v in ctx.items():
            path = path.replace("$" + k, v)
        status, body = client.call(method, path, subst(st.get("body"), ctx), st.get("params"), st.get("ndjson", False))
        if isinstance(body, dict):
            for key in ("scroll_id", "pit_id"):
                src = "_scroll_id" if key == "scroll_id" else "pit_id"
                if body.get(src):
                    ctx[key] = body[src]
        rec = {"status": status}
        if st.get("cmp", True):
            if status >= 400 and isinstance(body, dict) and "error" in body and not st.get("full_error"):
                rec["body"] = reason_key(body)
            elif st.get("shape") is not None and status < 400:
                rec["body"] = shape(body, st["shape"])
            elif isinstance(body, str) and st.get("text_normalise"):
                rec["body"] = st["text_normalise"](body)
            else:
                rec["body"] = normalise(body, st.get("opts", {}))
        results.append(rec)
    return results


def run_case(c, golden_case, warp, sharded=False):
    """Replays one corpus case on `warp` and returns the list of differences from the recorded oracle output."""
    warp.reset()
    steps = c["steps"]
    if sharded:
        # relevance scores are per shard (each Postgres host scores on its own documents, like a shard) and so is
        # the order of score-ranked hits; everything else must still match
        steps = [dict(st, opts={**st.get("opts", {}), "scores": False,
                                "ordered": st.get("opts", {}).get("ordered", False) and not st.get("opts", {}).get("scores", False)})
                 for st in steps]
    got = run_steps(warp, steps)
    exp = golden_case
    diffs = []
    for i, (e, g) in enumerate(zip(exp, got)):
        step = steps[i]
        if e["status"] != g["status"]:
            diffs.append("step %d %s %s: status oracle=%s warp=%s" % (i, step["method"], step["path"], e["status"], g["status"]))
            continue
        if "body" in e:
            eb = e["body"]
            gb = g.get("body")
            if sharded:
                eb, gb = _strip_scores(eb), _strip_scores(gb)
                if c["steps"][i].get("opts", {}).get("scores"):
                    # score-ranked order is per shard: compare the hit set, not the order
                    eb, gb = _sort_hits(eb), _sort_hits(gb)
            diffs += ["step %d %s %s %s" % (i, step["method"], step["path"], d) for d in diff(eb, gb)]
    return diffs


def _sort_hits(v):
    if isinstance(v, dict):
        out = {k: _sort_hits(x) for k, x in v.items()}
        hits = out.get("hits")
        if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
            hits["hits"] = sorted(hits["hits"], key=lambda h: str(h.get("_id")) if isinstance(h, dict) else str(h))
        return out
    if isinstance(v, list):
        return [_sort_hits(x) for x in v]
    return v


def _strip_scores(v):
    if isinstance(v, dict):
        return {k: _strip_scores(x) for k, x in v.items() if k not in ("_score", "max_score")}
    if isinstance(v, list):
        return [_strip_scores(x) for x in v]
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp", required=True)
    ap.add_argument("--golden", help="compare against a recorded oracle file instead of a live oracle")
    ap.add_argument("--record", help="write the oracle's normalised responses here")
    ap.add_argument("--filter")
    ap.add_argument("--out", help="write per-case results json")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--sharded", action="store_true", help="Warp runs the index over several hosts: compare without per-shard scores/score order")
    a = ap.parse_args()
    cases = corpus.all_cases()
    if a.filter:
        cases = [c for c in cases if a.filter in c["name"]]
    oracle = Client(a.oracle) if a.oracle else None
    warp = Client(a.warp)
    golden = json.load(open(a.golden)) if a.golden else {}
    recorded = {}
    results = {}
    n_ok = 0
    for c in cases:
        name = c["name"]
        warp.reset()
        if oracle:
            oracle.reset()
            exp = run_steps(oracle, c["steps"])
            recorded[name] = exp
        else:
            exp = golden.get(name)
            if exp is None:
                results[name] = {"status": "skip", "diffs": ["no golden record"]}
                continue
        if a.sharded:
            diffs = run_case(c, exp, warp, sharded=True)
        else:
            got = run_steps(warp, c["steps"])
            diffs = []
            for i, (e, g) in enumerate(zip(exp, got)):
                step = c["steps"][i]
                if e["status"] != g["status"]:
                    diffs.append("step %d %s %s: status oracle=%s warp=%s (%s)" % (i, step["method"], step["path"], e["status"], g["status"],
                                                                                    trunc(g.get("body"))))
                    continue
                if "body" in e:
                    diffs += ["step %d %s %s %s" % (i, step["method"], step["path"], d) for d in diff(e["body"], g.get("body"))]
        reason = c.get("known") or known.KNOWN.get(name) or (known.KNOWN_SHARDED.get(name) if a.sharded else None)
        if reason:
            results[name] = {"status": "known" if diffs else "pass", "diffs": diffs[:8], "known": reason}
            n_ok += 0 if diffs else 1
        elif diffs:
            results[name] = {"status": "diff", "diffs": diffs[:8]}
        else:
            results[name] = {"status": "pass", "diffs": []}
            n_ok += 1
        if not a.quiet and diffs:
            print("DIFF %s" % name)
            for d in diffs[:4]:
                print("    " + d[:300])
    if a.record:
        json.dump(recorded, open(a.record, "w"), sort_keys=True, separators=(",", ":"))
    counts = {}
    for r in results.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(json.dumps({"cases": len(cases), **counts}))
    if a.out:
        json.dump(results, open(a.out, "w"), indent=0, sort_keys=True)


if __name__ == "__main__":
    main()
