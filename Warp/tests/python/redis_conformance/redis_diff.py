"""Differential conformance engine for the Redis frontend (rediswire): replays a text corpus of command sequences
against a server and normalises the replies (RESP2 and RESP3 wire types, error strings included).

Modes (see run_redis_oracle.py): record the answers of a REAL Redis into a gzip-JSON golden file, replay the
golden file against Warp offline (no Docker), or run corpus against Redis and Warp side by side.

Corpus grammar (corpus/*.txt), one entry per line:
  ## case name [resp3]          new case: both connections reset, FLUSHALL, optional HELLO 3 on the main connection
  # comment
  [@conn] [!opt ...] CMD args   run a command and record its reply; @conn picks/creates a named connection
  @conn > CMD args              send only (e.g. a blocking command); the reply is collected with '<'
  @conn < [ms]                  read one pending reply (records "TIMEOUT" when nothing arrives within ms, default 500)
  !sleep MS                     pause
Options on a command line: !sort (sort every list of scalars, for unordered replies), !ttl (numeric, compare within 2),
!ignore (execute, record nothing), !type (record only the reply's wire type), !len (record only the length),
!approx (numbers within 1%), !sortpairs (sort a flat list of pairs).
Arguments use redis-cli quoting: "double quoted" with \\n \\r \\t \\xNN \\\\ \\" escapes, 'single quoted'.
"""
import gzip
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from redis_resp_client import (Push, RMap, RSet, Resp, RespError, Simple, Verbatim)  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS_DIR = os.path.join(HERE, "corpus")
GOLDEN = os.path.join(HERE, "golden", "redis_golden.json.gz")


def split_args(line):
    """redis-cli style argument splitting (sdssplitargs)."""
    out = []
    i, n = 0, len(line)
    while True:
        while i < n and line[i] in " \t":
            i += 1
        if i >= n:
            return out
        cur = bytearray()
        if line[i] == '"':
            i += 1
            while i < n and line[i] != '"':
                c = line[i]
                if c == "\\" and i + 1 < n:
                    i += 1
                    e = line[i]
                    if e == "x" and i + 2 < n and re.match(r"[0-9a-fA-F]{2}", line[i + 1:i + 3]):
                        cur.append(int(line[i + 1:i + 3], 16))
                        i += 2
                    else:
                        cur += {"n": b"\n", "r": b"\r", "t": b"\t", "b": b"\b", "a": b"\a"}.get(e, e.encode())
                else:
                    cur += c.encode()
                i += 1
            i += 1
        elif line[i] == "'":
            i += 1
            while i < n and line[i] != "'":
                if line[i] == "\\" and i + 1 < n and line[i + 1] == "'":
                    i += 1
                cur += line[i].encode()
                i += 1
            i += 1
        else:
            while i < n and line[i] not in " \t":
                cur += line[i].encode()
                i += 1
        out.append(bytes(cur))


def parse_corpus(text):
    """-> list of (case_name, resp3, [steps]) where a step is a dict."""
    cases = []
    cur = None
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#") and not line.startswith("##"):
            continue
        if line.startswith("##"):
            title = line[2:].strip()
            resp3 = title.endswith(" resp3")
            if resp3:
                title = title[:-6].strip()
            cur = (title, resp3, [])
            cases.append(cur)
            continue
        if cur is None:
            raise ValueError(f"line {lineno}: command outside a case")
        step = {"conn": "main", "opts": set(), "mode": "run", "line": lineno}
        if line.startswith("!sleep"):
            cur[2].append({"mode": "sleep", "ms": int(line.split()[1]), "line": lineno})
            continue
        m = re.match(r"@(\w+)\s*(>|<)?\s*(.*)$", line)
        if m:
            step["conn"] = m.group(1)
            if m.group(2) == ">":
                step["mode"] = "send"
            elif m.group(2) == "<":
                step["mode"] = "read"
                step["timeout"] = int(m.group(3) or 500)
                cur[2].append(step)
                continue
            line = m.group(3)
        while line.startswith("!"):
            opt, _, line = line.partition(" ")
            step["opts"].add(opt[1:])
            line = line.lstrip()
        step["args"] = split_args(line)
        cur[2].append(step)
    return cases


def load_corpus(names=None):
    cases = []
    for fn in sorted(os.listdir(CORPUS_DIR)):
        if not fn.endswith(".txt"):
            continue
        if names and fn[:-4] not in names:
            continue
        with open(os.path.join(CORPUS_DIR, fn)) as f:
            for c in parse_corpus(f.read()):
                cases.append((f"{fn[:-4]}::{c[0]}", c[1], c[2]))
    return cases


# ---------------------------------------------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------------------------------------------

def to_json(x):
    """Reply -> JSON-able value that keeps the RESP wire type."""
    if isinstance(x, RespError):
        return {"-": x.text}
    if isinstance(x, Simple):
        return {"+": str(x)}
    if isinstance(x, bool):
        return {"#": x}
    if isinstance(x, bytes) and not isinstance(x, Verbatim):
        return x.decode("latin-1")
    if isinstance(x, Verbatim):
        return {"=": x.decode("latin-1")}
    if isinstance(x, float):
        return {",": "inf" if x == float("inf") else "-inf" if x == float("-inf") else repr(x)}
    if isinstance(x, RMap):
        return {"%": [[to_json(k), to_json(v)] for k, v in x]}
    if isinstance(x, RSet):
        return {"~": [to_json(i) for i in x]}
    if isinstance(x, Push):
        return {">": [to_json(i) for i in x]}
    if isinstance(x, list):
        return [to_json(i) for i in x]
    return x


def sort_scalars(j):
    """Recursively sort lists whose elements are all scalars (strings/ints/None), for unordered replies."""
    if isinstance(j, list):
        j = [sort_scalars(i) for i in j]
        if all(isinstance(i, (str, int, type(None))) and not isinstance(i, bool) for i in j):
            return sorted(j, key=lambda v: (v is None, str(v)))
        return sorted(j, key=lambda v: json.dumps(v, sort_keys=True))
    if isinstance(j, dict):
        return {k: sort_scalars(v) for k, v in j.items()}
    return j


def sort_pairs(j):
    if isinstance(j, list) and len(j) % 2 == 0:
        pairs = [(j[i], j[i + 1]) for i in range(0, len(j), 2)]
        pairs.sort(key=lambda p: json.dumps(p[0]))
        return [v for p in pairs for v in p]
    return j


def wire_type(x):
    return type(x).__name__ if not isinstance(x, bytes) else "bytes"


def record_value(reply, opts):
    if "ignore" in opts:
        return None
    if "type" in opts:
        return {"type": wire_type(reply)}
    if "len" in opts:
        return {"len": len(reply) if hasattr(reply, "__len__") else reply}
    j = to_json(reply)
    if "sort" in opts:
        j = sort_scalars(j)
    if "sortpairs" in opts:
        j = sort_pairs(j)
    return j


def equal(a, b, opts):
    if "ignore" in opts:
        return True
    if "ttl" in opts and isinstance(a, int) and isinstance(b, int) and not isinstance(a, bool):
        return abs(a - b) <= 2
    if "approx" in opts:
        try:
            fa = float(a if not isinstance(a, dict) else list(a.values())[0])
            fb = float(b if not isinstance(b, dict) else list(b.values())[0])
            return abs(fa - fb) <= 0.01 * max(abs(fa), abs(fb), 1e-9)
        except (TypeError, ValueError):
            return a == b
    return a == b


# ---------------------------------------------------------------------------------------------------------------
# running
# ---------------------------------------------------------------------------------------------------------------

class Runner:
    def __init__(self, host, port, password=None):
        self.host, self.port, self.password = host, port, password
        self.admin = Resp(host, port, password=password)

    def close(self):
        self.admin.close()

    def new_conn(self, resp3=False):
        return Resp(self.host, self.port, password=self.password, protocol=3 if resp3 else 2)

    def run_case(self, resp3, steps):
        """-> list of recorded values (one per step that records something)."""
        self.admin.execute("FLUSHALL")
        conns = {}
        out = []

        def conn(name):
            if name not in conns:
                conns[name] = self.new_conn(resp3 if name == "main" else False)
            return conns[name]

        try:
            for st in steps:
                if st["mode"] == "sleep":
                    time.sleep(st["ms"] / 1000.0)
                    continue
                c = conn(st["conn"])
                if st["mode"] == "read":
                    r = c.try_read(st["timeout"] / 1000.0)
                    out.append("TIMEOUT" if r is None else record_value(r, set()))
                    continue
                if st["mode"] == "send":
                    c.send(*st["args"])
                    continue
                try:
                    r = c.execute(*st["args"])
                except (ConnectionError, OSError) as e:
                    out.append({"conn-error": type(e).__name__})
                    conns.pop(st["conn"], None)
                    continue
                v = record_value(r, st["opts"])
                if "ignore" not in st["opts"]:
                    out.append(v)
        finally:
            for c in conns.values():
                c.close()
        return out


def record(runner, cases):
    golden = {}
    for name, resp3, steps in cases:
        golden[name] = runner.run_case(resp3, steps)
    return golden


def save_golden(golden, path=GOLDEN):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt") as f:
        json.dump(golden, f, indent=0, sort_keys=True)


def load_golden(path=GOLDEN):
    with gzip.open(path, "rt") as f:
        return json.load(f)


def compare_case(name, steps, expected, actual):
    """-> list of mismatch descriptions."""
    problems = []
    recorded = [s for s in steps if s["mode"] in ("run", "read") and "ignore" not in s.get("opts", set())]
    if len(expected) != len(actual):
        problems.append(f"{name}: expected {len(expected)} replies, got {len(actual)}")
    for i, (e, a) in enumerate(zip(expected, actual)):
        st = recorded[i] if i < len(recorded) else {"opts": set()}
        if not equal(a, e, st.get("opts", set())):
            cmd = b" ".join(st.get("args", [b"<read>"])).decode("latin-1") if st.get("args") else "<read>"
            problems.append(f"{name} line {st.get('line')}: {cmd}\n      redis: {json.dumps(e)}\n      warp:  {json.dumps(a)}")
    return problems
