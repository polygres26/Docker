"""Differential harness: replays az_corpus.py cases against Azurite (oracle) and Warp and diffs the normalised answers.

  python3 az_harness.py --oracle-ports 32775,32776,32777 --start-warp [--shards 2] [--filter blob_] [--show diff|all]
  python3 az_harness.py --oracle-ports ... --record golden.json.gz        # record the oracle (each case twice, unstable steps dropped)
Offline replay of the golden file against a Warp is in ../test_azure_conformance.py (uses replay_golden()).
"""
import argparse
import base64
import datetime
import gzip
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import az_client as C  # noqa: E402
import az_signer as S  # noqa: E402

import az_corpus  # noqa: E402
from az_known import KNOWN  # noqa: E402

for _k in KNOWN:
    _n, _i = _k.split('#')
    az_corpus.CASES[_n][int(_i)]['known_diff'] = True

KEEP_HEADERS = {
    "content-type", "content-md5", "content-range", "content-disposition",
    "accept-ranges", "x-ms-error-code", "x-ms-blob-type", "x-ms-lease-status", "x-ms-lease-state",
    "x-ms-lease-duration", "x-ms-lease-time", "x-ms-blob-sequence-number", "x-ms-blob-committed-block-count",
    "x-ms-blob-append-offset", "x-ms-blob-content-length", "x-ms-copy-status", "x-ms-tag-count", "x-ms-snapshot", "x-ms-approximate-messages-count",
    "x-ms-blob-public-access", "x-ms-continuation-nextpartitionkey", "x-ms-continuation-nextrowkey",
    "x-ms-continuation-nexttablename", "preference-applied", "x-ms-popreceipt",
    "x-ms-time-next-visible", "x-ms-sku-name", "x-ms-account-kind", "etag", "last-modified", "x-ms-lease-id",
    "x-ms-copy-id", "access-control-allow-methods", "access-control-max-age",
    "x-ms-creation-time",
    "x-ms-range-get-content-md5", "www-authenticate",
}
KEEP_PREFIX = ("x-ms-meta-",)

UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
RFC = re.compile(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d\d (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d\d:\d\d:\d\d GMT")
ISO = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")
ETAG = re.compile(r"0x[0-9A-Fa-f]{6,}")
HOSTP = re.compile(r"https?://[^/\"'<>\s]+")


def norm_text(s, run):
    if s is None:
        return s
    s = s.replace(run, "@R@")
    s = HOSTP.sub("http://<host>", s)
    s = RFC.sub(lambda m: "<future>" if m.group(0).endswith("9999 23:59:59 GMT") else "<date>", s)
    s = ISO.sub("<ts>", s)
    s = UUID.sub("<uuid>", s)
    s = ETAG.sub("<etag>", s)
    s = re.sub(r"datetime'[^']*'", "datetime'<ts>'", s)
    return s


def norm_headers(h, run):
    out = {}
    for k, v in h.items():
        lk = k.lower()
        if lk in KEEP_HEADERS or lk.startswith(KEEP_PREFIX):
            if lk == "content-type":
                v = v.replace("; charset=utf-8", ";charset=utf-8").replace("application/xml;charset=utf-8", "application/xml")
                v = re.sub(r"boundary=\S+", "boundary=<b>", v).replace(";streaming=true;charset=utf-8", "")
            if lk.startswith("x-ms-continuation"):
                v = "<opaque>"
            if lk in ("etag", "last-modified", "x-ms-creation-time", "x-ms-popreceipt", "x-ms-time-next-visible", "x-ms-snapshot"):
                v = "<present>"
            if lk == "x-ms-lease-id":
                v = "<uuid>"
            if lk == "x-ms-copy-id":
                v = "<uuid>"
            out[lk] = norm_text(v, run)
    return out


def _canon_xml(el, run):
    kids = list(el)
    text = (el.text or "").strip()
    if el.tag in ("Message",):
        text = norm_text(text.split("\n")[0], run)
    else:
        text = norm_text(text, run)
    if el.tag in ("NextMarker", "PopReceipt", "Marker") and text:
        text = "<opaque>"
    if el.tag == "ServiceEndpoint":
        text = "<host>"
    attrs = {k: norm_text(v, run) for k, v in sorted(el.attrib.items())}
    children = [_canon_xml(k, run) for k in kids]
    if el.tag in ("Properties", "Metadata"):
        children.sort(key=lambda c: c[0])
    if el.tag == "Blobs":
        children.sort(key=lambda c: json.dumps(c, sort_keys=True))
    if el.tag == "DefaultServiceVersion":
        text = "<v>"
    if el.tag in ("AuthenticationErrorDetail",):
        text = "<detail>"
    return [el.tag.split("}")[-1], attrs, text, children]


def norm_body(resp, run):
    ct = resp.headers.get("Content-Type", "")
    raw = resp.content
    if not raw:
        return None
    if "multipart" in ct:
        return _canon_multipart(raw.decode("utf-8", "replace"), run)
    if "multipart" in ct + "x":
        t = raw.decode("utf-8", "replace")
        t = re.sub(r"(batchresponse|changesetresponse)_[0-9a-f-]+", r"\1_<id>", t)
        t = re.sub(r"x-ms-request-id: .*", "x-ms-request-id: <id>", t)
        t = re.sub(r"x-ms-version: .*", "x-ms-version: <v>", t)
        t = re.sub(r"(?i)\r?\n(etag|date|server|content-length): .*", "", t)
        return norm_text(t, run)
    if "xml" in ct or raw.lstrip().startswith(b"<"):
        try:
            return _canon_xml(ET.fromstring(raw), run)
        except ET.ParseError:
            pass
    if "json" in ct:
        try:
            return _canon_json(json.loads(raw), run)
        except ValueError:
            pass
    try:
        return norm_text(raw.decode("utf-8"), run)
    except UnicodeDecodeError:
        return "b64:" + base64.b64encode(raw).decode()


def _canon_multipart(t, run):
    """One entry per sub-response: status code, whether an ETag is present, canonical error/entity body."""
    parts = []
    for m in re.finditer(r"HTTP/1\.1 (\d{3})[^\r\n]*\r?\n(.*?)(?=\r?\n--(?:batchresponse|changesetresponse)|\Z)", t, re.S):
        head, _, body = m.group(2).partition("\r\n\r\n")
        body = body.strip()
        cb = None
        if body:
            try:
                cb = _canon_json(json.loads(body), run)
            except ValueError:
                try:
                    cb = _canon_xml(ET.fromstring(body), run)
                except ET.ParseError:
                    cb = norm_text(body, run)
        hdrs = sorted(h.split(":")[0].lower() for h in head.split("\r\n") if h.startswith("x-ms-error-code") or h.lower().startswith("etag"))
        parts.append([m.group(1), hdrs, cb])
    return parts


def _canon_json(o, run):
    if isinstance(o, dict):
        out = {}
        for k, v in sorted(o.items()):
            if k == "odata.metadata":
                out[k] = "<meta>"
                continue
            if k == "odata.error":
                v = {"code": v.get("code"), "message": (v.get("message") or {}).get("value", "").split("\n")[0]}
                v = {"code": v["code"], "message": norm_text(v["message"], run)}
                out[k] = v
                continue
            out[norm_text(k, run)] = _canon_json(v, run)
        return out
    if isinstance(o, list):
        return [_canon_json(x, run) for x in o]
    if isinstance(o, str):
        return norm_text(o, run)
    return o


def normalise(resp, run):
    h = norm_headers(resp.headers, run)
    if resp.status_code == 204 or resp.status_code == 304:
        h.pop("content-type", None)
    if resp.request.method not in ("GET", "HEAD"):
        h.pop("content-md5", None)
    return {"status": resp.status_code, "headers": h, "body": norm_body(resp, run)}


# ------------------------------------------------------------------------------------------------ execution

VAR = re.compile(r"<<(\w+)>>")


def subst(x, run, vars_):
    if isinstance(x, str):
        x = x.replace("@R@", run)
        return VAR.sub(lambda m: vars_.get(m.group(1), ""), x)
    if isinstance(x, bytes):
        return subst(x.decode("latin-1"), run, vars_).encode("latin-1")
    if isinstance(x, (list, tuple)):
        return type(x)(subst(i, run, vars_) for i in x)
    if isinstance(x, dict):
        return {k: subst(v, run, vars_) for k, v in x.items()}
    return x


def make_sas(step, run, vars_):
    spec = step["sas"]
    now = datetime.datetime.now(datetime.timezone.utc)
    exp = S.iso(now + datetime.timedelta(seconds=spec.get("exp", 3600)))
    start = S.iso(now + datetime.timedelta(seconds=spec["start"])) if "start" in spec else None
    if spec["kind"] == "account":
        return S.account_sas(S.DEV_ACCOUNT, S.DEV_KEY, spec["ss"], spec["srt"], spec["sp"], exp, start=start,
                             protocol=spec.get("spr"))
    res = subst(spec["resource"], run, vars_)
    return S.service_sas(step["svc"], S.DEV_ACCOUNT, S.DEV_KEY, res, spec["sp"], exp, start=start, sr=spec.get("sr"),
                         identifier=spec.get("si"), protocol=spec.get("spr"), ip=spec.get("ip"),
                         table_range=spec.get("range"))


def run_step(ep, step, run, vars_):
    st = subst({k: v for k, v in step.items() if k in ("path", "query", "headers", "body")}, run, vars_)
    body = st.get("body", b"")
    if callable(body):
        body = body()
    kw = dict(auth=step.get("auth", "key"))
    if step.get("auth") == "sas":
        kw["sas"] = make_sas(step, run, vars_)
    if step.get("auth") == "bearer":
        kw["auth"] = "anon"
        st["headers"] = {**st.get("headers", {}), "Authorization": "Bearer not-a-real-token"}
    if step.get("acct2"):
        kw["account"] = "acct2"
    if step.get("delay"):
        time.sleep(step["delay"])
    r = C.do(ep, step["svc"], step["method"], st.get("path", ""), st.get("query", []), st.get("headers", {}), body, **kw)
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


def run_case(ep, case, run):
    for _i, _s in enumerate(case):
        _s.setdefault('_i', _i)
    vars_ = {}
    out = []
    for step in case:
        try:
            r = run_step(ep, step, run, vars_)
            n = normalise(r, run)
            if step.get("raw_body"):
                n["body"] = "b64:" + base64.b64encode(r.content).decode()
            if step.get("skip_body"):
                n["body"] = None
        except Exception as e:  # noqa: BLE001
            n = {"error": repr(e)[:200]}
        out.append(n)
    return out


def new_run():
    import random
    import string
    return "".join(random.choice(string.ascii_lowercase) for _ in range(6))


def replay_golden(ep, name, golden):
    """Replay one golden case; returns [(step, expected, got)] mismatches (steps recorded as None are skipped)."""
    case = az_corpus.CASES[name]
    run = new_run()
    got = run_case(ep, case, run)
    bad = []
    for i, (exp, g) in enumerate(zip(golden, got)):
        if exp is None:
            continue
        if not compatible(exp, g, case[i]):
            bad.append((f"{name}#{i} {case[i]['method']} {case[i].get('path', '')} {case[i].get('query', '')}", exp, g))
    return bad


def verdict(exp, got, step):
    """same | msg (same error status, different code/text/body) | known (documented) | diff"""
    if exp == got:
        return "same"
    if exp.get("status") == got.get("status") and exp.get("status", 0) >= 400:
        return "msg"
    if step.get("known_diff") or f"{step.get('_case')}#{step.get('_i')}" in KNOWN:
        return "known"
    return "diff"


def compatible(exp, got, step):
    return verdict(exp, got, step) != "diff"


def record(ep, out_path, only=None):
    golden = {}
    for name, case in az_corpus.CASES.items():
        if only and only not in name:
            continue
        a = run_case(ep, case, new_run())
        b = run_case(ep, case, new_run())
        golden[name] = [x if x == y else None for x, y in zip(a, b)]
    with gzip.open(out_path, "wt") as f:
        json.dump(golden, f)
    return golden


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle-ports", help="blob,queue,table ports of Azurite on localhost")
    ap.add_argument("--warp", help="blob,queue,table urls of Warp")
    ap.add_argument("--start-warp", action="store_true")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--filter")
    ap.add_argument("--show", default="diff")
    ap.add_argument("--record")
    ap.add_argument("--golden")
    ap.add_argument("--report")
    args = ap.parse_args()
    oracle = None
    if args.oracle_ports:
        p = args.oracle_ports.split(",")
        oracle = C.Endpoint(*[f"http://localhost:{x}" for x in p])
    if args.record:
        g = record(oracle, args.record, args.filter)
        print("recorded", len(g), "cases,", sum(1 for v in g.values() for x in v if x is None), "unstable steps dropped")
        return
    warp = None
    stop = lambda: None  # noqa: E731
    if args.start_warp:
        import az_launch_warp
        w = az_launch_warp.AzWarp(args.shards)
        warp = C.Endpoint(w.blob, w.queue, w.table)
        stop = w.close
    elif args.warp:
        warp = C.Endpoint(*args.warp.split(","))
    total = same = 0
    classes = {"same": 0, "msg": 0, "known": 0, "diff": 0}
    diffs = []
    listing = []
    try:
        for name, case in az_corpus.CASES.items():
            if args.filter and args.filter not in name:
                continue
            run = new_run()
            if args.golden:
                with gzip.open(args.golden, "rt") as f:
                    exp = json.load(f)[name]
            else:
                exp = run_case(oracle, case, run)
                run = new_run()
            got = run_case(warp, case, run)
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
                f.write(f"{v}\t{name}#{i}\t{step['method']} {step.get('path', '')} {step.get('query', '')}\t"
                        f"oracle={e.get('status')} warp={g.get('status')}\n")
    if args.show != "none":
        for name, i, step, e, g in diffs:
            print(f"--- {name}#{i} {step['method']} {step.get('path', '')} {step.get('query', '')}")
            if args.show == "diff":
                for k in ("status", "headers", "body"):
                    if k == "headers" and e.get(k) != g.get(k):
                        eh, gh = e.get(k) or {}, g.get(k) or {}
                        d = {n: (eh.get(n), gh.get(n)) for n in set(eh) | set(gh) if eh.get(n) != gh.get(n)}
                        print(f"  headers {json.dumps(d)[:500]}")
                    elif e.get(k) != g.get(k):
                        print(f"  {k}\n    oracle: {json.dumps(e.get(k))[:700]}\n    warp:   {json.dumps(g.get(k))[:700]}")


if __name__ == "__main__":
    main()
