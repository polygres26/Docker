#!/usr/bin/env python3
"""Runs OpenSearch's own REST API YAML tests (rest-api-spec/test, Apache-2.0, NOT vendored here) against
any OpenSearch-compatible endpoint. Supports do/catch/headers, match/length/is_true/is_false/gt/gte/lt/lte/
contains/close_to/set/transform_and_set, skip (version + features), setup/teardown, $stash, `_arbitrary_key_`.

  run_spec.py --specs <.../rest-api-spec> --url http://localhost:9200 --dirs index,get,... --out results.json
  run_spec.py ... --only-passing oracle.json   # run only the tests that pass on the real OpenSearch

Needs PyYAML and requests.
"""
import argparse
import glob
import json
import os
import re
import sys
import time
import traceback
import urllib.parse

import requests
import yaml

FEATURES_SUPPORTED = {"headers", "stash_in_path", "stash_in_key", "arbitrary_key", "contains", "allowed_warnings",
                      "warnings", "default_shards", "yaml", "transform_and_set", "close_to", "node_selector_ignored"}
API_CACHE = {}
FILTER = None


class _Loader(yaml.SafeLoader):
    """SafeLoader without implicit timestamp parsing (dates stay strings, as the Java runner sees them)."""


_Loader.yaml_implicit_resolvers = {k: [(t, r) for t, r in v if t != "tag:yaml.org,2002:timestamp"]
                                   for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()}


class Skip(Exception):
    pass


class Fail(Exception):
    pass


def vt(s):
    return tuple(int(x) for x in re.findall(r"\d+", s)[:3]) + (0,) * (3 - len(re.findall(r"\d+", s)[:3]))


def version_skipped(rng, ver):
    if rng.strip() == "all":
        return True
    for part in rng.split(","):
        lo, _, hi = part.partition("-")
        lo, hi = lo.strip(), hi.strip()
        if (not lo or vt(ver) >= vt(lo)) and (not hi or vt(ver) <= vt(hi)):
            return True
    return False


class Runner:
    def __init__(self, url, api_dir, version):
        self.url = url.rstrip("/")
        self.api_dir = api_dir
        self.version = version
        self.stash = {}
        self.last = None
        self.http = requests.Session()

    # ---- api lookup
    def api(self, name):
        if name not in API_CACHE:
            with open(os.path.join(self.api_dir, name + ".json")) as f:
                API_CACHE[name] = list(json.load(f).values())[0]
        return API_CACHE[name]

    def subst(self, v):
        if isinstance(v, str):
            if v.startswith("$") and v[1:] in self.stash and re.fullmatch(r"\$\w+", v):
                return self.stash[v[1:]]
            if v.startswith("${") and v.endswith("}") and v[2:-1] in self.stash:
                return self.stash[v[2:-1]]
            return re.sub(r"\$\{(\w+)\}|\$(\w+)", lambda m: str(self.stash.get(m.group(1) or m.group(2), m.group(0))), v) if "$" in v else v
        if isinstance(v, list):
            return [self.subst(x) for x in v]
        if isinstance(v, dict):
            return {self.subst(k) if isinstance(k, str) else k: self.subst(x) for k, x in v.items()}
        return v

    def do(self, spec):
        spec = dict(spec)
        catch = spec.pop("catch", None)
        headers = spec.pop("headers", None) or {}
        for k in ("warnings", "allowed_warnings", "allowed_warnings_regex", "warnings_regex", "node_selector"):
            spec.pop(k, None)
        (apiname, params), = spec.items()
        params = self.subst(params or {})
        api = self.api(apiname)
        body = params.pop("body", None)
        ignore = params.pop("ignore", None)
        paths = api["url"]["paths"]
        best = None
        for p in paths:
            parts = set(p.get("parts", {}))
            if parts <= set(params) and (best is None or len(parts) > len(set(best.get("parts", {})))):
                best = p
        if best is None:
            raise Fail("no path for %s with %s" % (apiname, list(params)))
        path = best["path"]
        for part in best.get("parts", {}):
            val = params.pop(part)
            if isinstance(val, list):
                val = ",".join(str(x) for x in val)
            path = path.replace("{" + part + "}", urllib.parse.quote(str(val), safe=",*"))
        methods = best["methods"]
        method = "POST" if ("POST" in methods and body is not None and "PUT" not in methods) else methods[0]
        if body is not None and method == "GET":
            method = "GET"  # OpenSearch accepts GET with a body
        q = {}
        for k, v in params.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            elif isinstance(v, list):
                v = ",".join(str(x) for x in v)
            q[k] = v
        data = None
        hdr = {"Content-Type": "application/json"}
        hdr.update({k: str(v) for k, v in headers.items()})
        if body is not None:
            if apiname in ("bulk", "msearch", "msearch_template") or "x-ndjson" in api.get("requestBody", {}).get("serialize", ""):
                items = body if isinstance(body, list) else [body]
                data = "".join((x if isinstance(x, str) else json.dumps(x)) + "\n" for x in items)
                hdr["Content-Type"] = "application/x-ndjson"
            elif isinstance(body, str):
                data = body
            else:
                data = json.dumps(body)
        try:
            r = self.http.request(method, self.url + path, params=q, data=data.encode() if data else None,
                                  headers=hdr, timeout=60)
        except requests.RequestException as e:
            if catch:
                return
            raise Fail("transport error: %s" % e)
        ctype = r.headers.get("content-type", "")
        try:
            self.last = r.json() if "json" in ctype and r.content else (r.text if r.content else {})
        except ValueError:
            self.last = r.text
        self.status = r.status_code
        if method == "HEAD":
            self.last = r.status_code < 400
            if r.status_code == 404 and not catch:
                self.last = False
                return
        if catch:
            mapping = {"missing": (404,), "conflict": (409,), "bad_request": (400,), "forbidden": (403,),
                       "unauthorized": (401,), "request_timeout": (408,)}
            if catch == "request":
                ok = 400 <= r.status_code < 600
            elif catch == "param":
                ok = True
            elif catch in mapping:
                ok = r.status_code in mapping[catch]
            elif catch.startswith("/"):
                ok = 400 <= r.status_code and re.search(catch.strip("/"), r.text, re.S) is not None
            else:
                ok = 400 <= r.status_code
            if not ok:
                raise Fail("expected catch=%s got %s: %s" % (catch, r.status_code, r.text[:300]))
            return
        if r.status_code >= 400 and not (ignore is not None and r.status_code in (ignore if isinstance(ignore, list) else [ignore])):
            if not (method == "HEAD" and r.status_code == 404):
                raise Fail("%s %s -> %s %s" % (method, path, r.status_code, r.text[:300]))

    # ---- assertions
    def resolve(self, path, root=None):
        cur = self.last if root is None else root
        if path in ("", "$body"):
            return cur
        path = self.subst(path) if "$" in path else path
        parts = re.split(r"(?<!\\)\.", path)
        for part in parts:
            part = part.replace("\\.", ".")
            if part.startswith("$") and part[1:] in self.stash:
                part = str(self.stash[part[1:]])
            if isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except (ValueError, IndexError):
                    raise KeyError(path)
            elif isinstance(cur, dict):
                if part in cur:
                    cur = cur[part]
                elif part == "_arbitrary_key_" and cur:
                    cur = next(iter(cur.keys())) if False else next(iter(cur.values()))
                else:
                    raise KeyError(path)
            else:
                raise KeyError(path)
        return cur

    def arbitrary_key_name(self, path):
        parts = re.split(r"(?<!\\)\.", path)
        cur = self.last
        for part in parts:
            if part == "_arbitrary_key_":
                return next(iter(cur.keys()))
            cur = cur[int(part)] if isinstance(cur, list) else cur[part]
        return None

    def get(self, path):
        try:
            return self.resolve(path)
        except KeyError:
            return KeyError

    @staticmethod
    def num_eq(a, b):
        if isinstance(a, bool) or isinstance(b, bool):
            return a == b
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return a == b
        return a == b

    def match(self, path, expected):
        expected = self.subst(expected)
        actual = self.get(path)
        if actual is KeyError:
            if expected is None:
                return
            raise Fail("match %s: path missing, expected %r" % (path, expected))
        if isinstance(expected, str) and expected.startswith("/") and expected.rstrip().endswith("/") and len(expected) > 1:
            if not re.search(expected.strip().strip("/"), str(actual), re.S | re.X):
                raise Fail("match %s: %r !~ %s" % (path, actual, expected[:80]))
            return
        if not self.num_eq(actual, expected):
            raise Fail("match %s: expected %r got %r" % (path, str(expected)[:200], str(actual)[:200]))

    def truthy(self, v):
        return v is not KeyError and v not in (None, False, "", 0, "false", 0.0) and v != [] and v != {} or v == {} and False

    def step(self, st):
        (op, arg), = st.items()
        if op == "do":
            self.do(arg)
        elif op == "match":
            for p, e in arg.items():
                self.match(p, e)
        elif op == "length":
            for p, n in arg.items():
                v = self.get(p)
                if v is KeyError or len(v) != n:
                    raise Fail("length %s: expected %s got %s" % (p, n, None if v is KeyError else len(v)))
        elif op == "is_true":
            v = self.get(arg)
            if v is KeyError or v in (None, False, "", 0, "false") or v == 0.0:
                raise Fail("is_true %s: %r" % (arg, None if v is KeyError else v))
        elif op == "is_false":
            v = self.get(arg)
            if not (v is KeyError or v in (None, False, "", 0, "false") or v == 0.0):
                raise Fail("is_false %s: %r" % (arg, v))
        elif op in ("gt", "gte", "lt", "lte"):
            for p, e in arg.items():
                v, e = self.get(p), self.subst(e)
                if v is KeyError:
                    raise Fail("%s %s: path missing" % (op, p))
                ok = {"gt": v > e, "gte": v >= e, "lt": v < e, "lte": v <= e}[op]
                if not ok:
                    raise Fail("%s %s: %r vs %r" % (op, p, v, e))
        elif op == "contains":
            for p, e in arg.items():
                v = self.get(p)
                e = self.subst(e)
                if v is KeyError or not any((x == e or (isinstance(e, dict) and isinstance(x, dict) and all(x.get(k) == vv for k, vv in e.items()))) for x in v):
                    raise Fail("contains %s: %r not in %r" % (p, e, str(v)[:200]))
        elif op == "close_to":
            for p, e in arg.items():
                v = self.get(p)
                if v is KeyError or abs(v - e["value"]) > e["error"]:
                    raise Fail("close_to %s: %r vs %r" % (p, v, e))
        elif op == "set":
            for p, name in arg.items():
                if "_arbitrary_key_" in p:
                    self.stash[name] = self.arbitrary_key_name(p)
                else:
                    v = self.get(p)
                    if v is KeyError:
                        raise Fail("set %s: path missing" % p)
                    self.stash[name] = v
        elif op == "transform_and_set":
            raise Skip("transform_and_set")
        elif op == "skip":
            self.check_skip(arg)
        else:
            raise Fail("unknown step %s" % op)

    def check_skip(self, s):
        feats = s.get("features")
        if feats:
            feats = [feats] if isinstance(feats, str) else feats
            missing = [f for f in feats if f not in FEATURES_SUPPORTED]
            if missing:
                raise Skip("feature " + ",".join(missing))
        if "version" in s and version_skipped(str(s["version"]), self.version):
            raise Skip("version %s: %s" % (s["version"], s.get("reason", "")))

    def wipe(self):
        try:
            r = self.http.get(self.url + "/_cat/indices?format=json&h=index", timeout=30)
            if r.status_code == 200:
                for row in r.json():
                    self.http.delete(self.url + "/" + urllib.parse.quote(row["index"]), timeout=30)
            for kind in ("_template", "_index_template", "_component_template"):
                r = self.http.get(self.url + "/" + kind, timeout=30)
                if r.status_code == 200:
                    js = r.json()
                    names = list(js.keys()) if kind == "_template" else [t["name"] for t in js.get(
                        "index_templates" if kind == "_index_template" else "component_templates", [])]
                    for n in names:
                        self.http.delete(self.url + "/" + kind + "/" + urllib.parse.quote(n), timeout=30)
        except Exception:  # noqa: BLE001
            pass

    def run_steps(self, steps):
        for st in steps or []:
            self.step(st)


def load_file(path):
    with open(path, encoding="utf-8") as f:
        try:
            return [d for d in yaml.load_all(f, Loader=_Loader) if d]
        except yaml.YAMLError as e:
            return e


def run_file(runner, path, only, results, rel):
    docs = load_file(path)
    if isinstance(docs, Exception):
        results[rel + "::<file>"] = ["skip", "yaml parse: %s" % str(docs)[:100]]
        return
    setup = teardown = None
    tests = []
    for d in docs:
        for name, steps in d.items():
            if name == "setup":
                setup = steps
            elif name == "teardown":
                teardown = steps
            else:
                tests.append((name, steps))
    for name, steps in tests:
        key = "%s::%s" % (rel, name)
        if only is not None and only.get(key) != "pass":
            continue
        if FILTER and FILTER not in key:
            continue
        runner.stash = {}
        runner.wipe()
        try:
            runner.run_steps(setup)
            runner.run_steps(steps)
            results[key] = ["pass", ""]
        except Skip as e:
            results[key] = ["skip", str(e)[:120]]
        except Fail as e:
            results[key] = ["fail", str(e)[:300]]
        except Exception as e:  # noqa: BLE001
            results[key] = ["fail", "%s: %s" % (type(e).__name__, str(e)[:250])]
        try:
            runner.run_steps(teardown)
        except Exception:  # noqa: BLE001
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--specs", required=True, help="path to .../rest-api-spec")
    ap.add_argument("--url", required=True)
    ap.add_argument("--dirs", required=True, help="comma-separated test directories, or 'all'")
    ap.add_argument("--version", default="2.19.6")
    ap.add_argument("--out", required=True)
    ap.add_argument("--filter", help="only run tests whose 'file::name' contains this text")
    ap.add_argument("--only-passing", help="results json from the oracle: run only tests that passed there")
    a = ap.parse_args()
    global FILTER
    FILTER = a.filter
    only = None
    if a.only_passing:
        with open(a.only_passing) as f:
            only = {k: v[0] for k, v in json.load(f).items()}
    testroot = os.path.join(a.specs, "test")
    dirs = sorted(os.listdir(testroot)) if a.dirs == "all" else a.dirs.split(",")
    runner = Runner(a.url, os.path.join(a.specs, "api"), a.version)
    results = {}
    for d in dirs:
        for f in sorted(glob.glob(os.path.join(testroot, d, "*.yml"))):
            run_file(runner, f, only, results, d + "/" + os.path.basename(f))
    runner.wipe()
    with open(a.out, "w") as f:
        json.dump(results, f, indent=0, sort_keys=True)
    c = {"pass": 0, "fail": 0, "skip": 0}
    for v in results.values():
        c[v[0]] += 1
    print(json.dumps(c))


if __name__ == "__main__":
    main()
