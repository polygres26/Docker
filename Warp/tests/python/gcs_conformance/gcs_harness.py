"""Differential harness: replays gcs_corpus.py cases against fake-gcs-server (oracle) and Warp and diffs the normalised answers.

  python3 gcs_harness.py --oracle http://localhost:PORT --start-warp [--shards 2] [--filter list_] [--show diff|all]
  python3 gcs_harness.py --oracle ... --record golden.json.gz        # records the oracle (each case twice, unstable steps dropped)
Offline replay of the golden file against a Warp is in ../test_gcs_conformance.py (uses replay_golden()).
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
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gcs_client as C  # noqa: E402
import gcs_corpus  # noqa: E402
from gcs_known import KNOWN  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split("#")
    _a, _, _b = _i.partition("-")
    for _j in range(int(_a), int(_b or _a) + 1):
        gcs_corpus.CASES[_n][_j]["known_diff"] = True
for _name, _steps in gcs_corpus.CASES.items():
    for _i, _s in enumerate(_steps):
        _s["_case"], _s["_i"] = _name, _i

# accept-ranges / x-goog-metageneration are not compared: fake-gcs-server sends the first only on some answers, the second never
KEEP_HEADERS = {"content-type", "content-range", "range", "x-goog-generation", "x-goog-hash",
                "x-goog-stored-content-length", "x-goog-stored-content-encoding", "content-encoding", "content-disposition",
                "etag", "last-modified", "location", "x-guploader-uploadid", "x-goog-component-count"}
# fields fake-gcs-server does not fill in like GCS (or fills with its own values); compared nowhere, see gcs_known.py
DROP = {"etag", "selfLink", "mediaLink", "id", "acl", "defaultObjectAcl", "owner", "projectNumber", "timeStorageClassUpdated",
        "softDeletePolicy", "rpo", "iamConfiguration", "defaultEventBasedHold", "hardDeleteTime", "softDeleteTime", "timeFinalized",
        "rewriteToken"}
ISO = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")
GEN = re.compile(r"(?<![0-9])1[0-9]{15}(?![0-9])")
RFC = re.compile(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d\d (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d\d:\d\d:\d\d GMT")
HOSTP = re.compile(r"https?://[^/\"'<>\s]+")
UPID = re.compile(r"upload_id=[0-9A-Za-z_-]+")


class Norm:
    def __init__(self, run):
        self.run = run
        self.gens = {}

    def text(self, s):
        if s is None:
            return s
        s = s.replace(self.run, "@R@")
        s = HOSTP.sub("http://<host>", s)
        s = UPID.sub("upload_id=<uid>", s)
        s = RFC.sub("<date>", s)
        s = ISO.sub("<ts>", s)
        return GEN.sub(lambda m: self.gens.setdefault(m.group(0), f"<g{len(self.gens) + 1}>"), s)

    def json(self, o, key=None):
        if isinstance(o, dict):
            if "error" in o and isinstance(o["error"], dict):
                e = o["error"]
                return {"error": {"code": e.get("code"), "reasons": [x.get("reason") for x in e.get("errors", [])]}}
            return {k: self.json(v, k) for k, v in sorted(o.items())
                    if k not in DROP and v != "" and not (k == "versioning" and v == {"enabled": False})}
        if isinstance(o, list):
            return [self.json(x, key) for x in o]
        if isinstance(o, str):
            if key in ("nextPageToken",):
                return "<opaque>"
            return self.text(o)
        return o

    def headers(self, h):
        out = {}
        for k, v in h.items():
            lk = k.lower()
            if lk not in KEEP_HEADERS:
                continue
            if lk == "content-type":
                v = re.sub(r";\s*charset=[^;]+", "", v, flags=re.I).strip().lower()
                v = re.sub(r"boundary=\S+", "boundary=<b>", v)
            elif lk in ("etag", "last-modified"):
                v = "<present>"
            elif lk == "x-guploader-uploadid":
                v = "<uid>"
            else:
                v = self.text(v)
            out[lk] = v
        return out

    def body(self, resp, step):
        raw = resp.content
        if not raw:
            return None
        if step.get("hash"):
            return "sha1:" + hashlib.sha1(raw).hexdigest()
        ct = resp.headers.get("Content-Type", "")
        if "json" in ct:
            try:
                return self.json(json.loads(raw))
            except ValueError:
                pass
        if step.get("raw"):
            return "b64:" + base64.b64encode(raw).decode()
        try:
            return self.text(raw.decode("utf-8"))
        except UnicodeDecodeError:
            return "b64:" + base64.b64encode(raw).decode()


VAR = re.compile(r"<<(\w+)>>")


def subst(x, run, vars_):
    if isinstance(x, str):
        return VAR.sub(lambda m: vars_.get(m.group(1), ""), x.replace("@R@", run))
    if isinstance(x, bytes):
        return x  # bodies are never templated (binary payloads could contain "@R@")
    if isinstance(x, (list, tuple)):
        return type(x)(subst(i, run, vars_) for i in x)
    if isinstance(x, dict):
        return {k: subst(v, run, vars_) for k, v in x.items()}
    return x


def run_step(base, step, run, vars_, auth=None):
    st = subst({k: v for k, v in step.items() if k in ("path", "query", "headers")}, run, vars_)
    body = step.get("body", b"")
    body = body() if callable(body) else subst(body, run, vars_)
    if isinstance(body, str):
        body = body.encode("utf-8")
    path, query = st["path"], st["query"]
    if path.startswith("http"):
        u = urllib.parse.urlparse(path)
        path = u.path
        query = urllib.parse.parse_qsl(u.query, keep_blank_values=True) + list(query)
    r = C.do(base, step["method"], path, query, st["headers"], body, auth=auth)
    for name, spec in step.get("cap", {}).items():
        kind, _, arg = spec.partition(":")
        if kind == "h":
            vars_[name] = r.headers.get(arg, "")
        elif kind == "x":
            m = re.search(arg, r.text)
            vars_[name] = m.group(1) if m else ""
        elif kind == "j":
            try:
                v = r.json()
                for part in arg.split("."):
                    v = v[int(part)] if part.isdigit() else v[part]
                vars_[name] = str(v)
            except Exception:  # noqa: BLE001
                vars_[name] = ""
    return r


def run_case(base, case, run, auth=None):
    vars_ = {}
    n = Norm(run)
    out = []
    for step in case:
        try:
            r = run_step(base, step, run, vars_, auth)
            h = n.headers(r.headers)
            if r.status_code in (204, 304):
                h.pop("content-type", None)
            if step["method"] == "HEAD":
                h.pop("content-type", None) if r.status_code >= 400 else None
            out.append({"status": r.status_code, "headers": h, "body": n.body(r, step)})
        except Exception as e:  # noqa: BLE001
            out.append({"error": repr(e)[:200]})
    return out


def new_run():
    return "".join(random.choice(string.ascii_lowercase) for _ in range(6))


def verdict(exp, got, step):
    """same | msg (same error status, different text) | known (documented) | diff"""
    if exp == got:
        return "same"
    if exp.get("status") == got.get("status") and exp.get("status", 0) >= 400:
        return "msg"
    if 200 <= exp.get("status", 0) < 300 and 200 <= got.get("status", 0) < 300 and not exp.get("body") and not got.get("body"):
        return "msg"  # 200 vs 204 with no body: fake-gcs-server answers deletes with 200
    if step.get("known_diff"):
        return "known"
    return "diff"


def compatible(exp, got, step):
    return verdict(exp, got, step) != "diff"


def replay_golden(base, name, golden, auth=None):
    """Replays one golden case; returns [(step, expected, got)] mismatches (steps recorded as None are skipped)."""
    case = gcs_corpus.CASES[name]
    got = run_case(base, case, new_run(), auth)
    bad = []
    for i, (exp, g) in enumerate(zip(golden, got)):
        if exp is not None and not compatible(exp, g, case[i]):
            bad.append((f"{name}#{i} {case[i]['method']} {case[i].get('path', '')} {case[i].get('query', '')}", exp, g))
    return bad


def record(base, out_path, only=None):
    golden = {}
    for name, case in gcs_corpus.CASES.items():
        if only and only not in name:
            continue
        a = run_case(base, case, new_run())
        b = run_case(base, case, new_run())
        golden[name] = [x if x == y else None for x, y in zip(a, b)]
    with gzip.open(out_path, "wt") as f:
        json.dump(golden, f)
    return golden


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle", help="base URL of fake-gcs-server")
    ap.add_argument("--warp", help="base URL of Warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter")
    ap.add_argument("--show", default="diff")
    ap.add_argument("--record")
    ap.add_argument("--golden")
    ap.add_argument("--report")
    args = ap.parse_args()
    if args.record:
        g = record(args.oracle, args.record, args.filter)
        print("recorded", len(g), "cases,", sum(1 for v in g.values() for x in v if x is None), "unstable steps dropped")
        return
    warp, stop = args.warp, (lambda: None)
    if args.start_warp:
        import gcs_launch_warp
        w = gcs_launch_warp.GcsWarp(args.shards)
        warp, stop = w.url, w.close
    classes = {"same": 0, "msg": 0, "known": 0, "diff": 0}
    total = 0
    diffs, listing = [], []
    try:
        for name, case in gcs_corpus.CASES.items():
            if args.filter and args.filter not in name:
                continue
            if args.golden:
                with gzip.open(args.golden, "rt") as f:
                    exp = json.load(f)[name]
            else:
                exp = run_case(args.oracle, case, new_run())
            got = run_case(warp, case, new_run())
            for i, (e, g) in enumerate(zip(exp, got)):
                if e is None:
                    continue
                total += 1
                v = verdict(e, g, case[i])
                classes[v] += 1
                if v != "same":
                    listing.append((v, name, i, case[i], e, g))
                if v == "diff":
                    diffs.append((name, i, case[i], e, g))
    finally:
        stop()
    print(f"steps compared: {total}, {classes}")
    if args.report:
        with open(args.report, "w") as f:
            for v, name, i, step, e, g in listing:
                f.write(f"{v}\t{name}#{i}\t{step['method']} {step.get('path', '')} {step.get('query', '')}\toracle={e.get('status')} warp={g.get('status')}\n")
    if args.show != "none":
        for name, i, step, e, g in diffs:
            print(f"--- {name}#{i} {step['method']} {step.get('path', '')} {step.get('query', '')}")
            for k in ("status", "headers", "body"):
                if e.get(k) != g.get(k):
                    print(f"  {k}\n    oracle: {json.dumps(e.get(k))[:600]}\n    warp:   {json.dumps(g.get(k))[:600]}")


if __name__ == "__main__":
    main()
