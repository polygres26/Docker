#!/usr/bin/env python3
"""A small Gherkin runner for the openCypher TCK (github.com/opencypher/openCypher, tck/features, Apache-2.0;
nothing third-party is vendored here) driven through the official `neo4j` Python driver.

Usage:
  # 1. which scenarios are valid on a real Neo4j (the oracle)?
  python3 bolt_tck.py --uri bolt://localhost:7687 --features <openCypher>/tck --out oracle.json
  # 2. run only those against Warp
  python3 bolt_tck.py --uri bolt://localhost:<boltwire port> --features <openCypher>/tck \
        --only-valid oracle.json --out warp.json

Supported steps: Given an empty graph / any graph / the binary-tree-N graph, And having executed, And parameters are,
When executing (control) query, Then the result should be (empty | in any order | in order | (ignoring element order for
lists)), And no side effects / the side effects should be, Then a <ErrorType> should be raised at <phase>: <Detail>.
Scenarios that register test procedures ("there exists a procedure") are skipped: neither Neo4j nor Warp can register
them from a client. Side effects are computed by diffing node/relationship/label/property totals before and after the
query, so they do not depend on the server's own summary counters. Error scenarios record the Neo4j status code the
oracle returned; against Warp the same code must come back (stricter than the TCK, which only names an error class).
"""
import argparse
import json
import math
import os
import re
import signal
import sys
import time

from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship, Path
import neo4j.time as ntime
import neo4j.spatial as nspatial


# --------------------------------------------------------------------------------------------- Gherkin parsing

class Step:
    def __init__(self, keyword, text, table=None, doc=None):
        self.keyword, self.text, self.table, self.doc = keyword, text, table, doc


class Scenario:
    def __init__(self, sid, name, steps):
        self.id, self.name, self.steps = sid, name, steps


def parse_table_row(line):
    parts = line.strip().strip("|")
    cells, cur, i, esc = [], "", 0, False
    while i < len(parts):
        ch = parts[i]
        if esc:
            cur += "\\" + ch if ch not in "|" else "|"
            esc = False
        elif ch == "\\":
            esc = True
        elif ch == "|":
            cells.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    cells.append(cur.strip())
    return cells


def parse_feature(path, rel):
    lines = open(path, encoding="utf-8").read().split("\n")
    background, scenarios = [], []
    i = 0
    cur = None  # dict(kind, name, steps, examples)
    section = None

    def flush():
        nonlocal cur
        if cur is None:
            return
        if cur["kind"] == "background":
            background.extend(cur["steps"])
        else:
            scenarios.append(cur)
        cur = None

    while i < len(lines):
        raw = lines[i]
        line = raw.strip()
        i += 1
        if not line or line.startswith("#") or line.startswith("@") or line.startswith("Feature:"):
            continue
        if line.startswith("Background:"):
            flush()
            cur = {"kind": "background", "name": "", "steps": [], "examples": []}
            section = "steps"
        elif line.startswith("Scenario Outline:") or line.startswith("Scenario:"):
            flush()
            outline = line.startswith("Scenario Outline:")
            name = line.split(":", 1)[1].strip()
            cur = {"kind": "outline" if outline else "scenario", "name": name, "steps": [], "examples": []}
            section = "steps"
        elif line.startswith("Examples:"):
            section = "examples"
            cur["examples"].append([])
        elif line.startswith('"""'):
            doc = []
            while i < len(lines) and not lines[i].strip().startswith('"""'):
                doc.append(lines[i])
                i += 1
            i += 1
            # strip the docstring's indentation
            indent = min((len(d) - len(d.lstrip()) for d in doc if d.strip()), default=0)
            cur["steps"][-1].doc = "\n".join(d[indent:] for d in doc)
        elif line.startswith("|"):
            row = parse_table_row(line)
            if section == "examples":
                cur["examples"][-1].append(row)
            else:
                st = cur["steps"][-1]
                st.table = (st.table or []) + [row]
        else:
            m = re.match(r"(Given|When|Then|And|But)\s+(.*)$", line)
            if m and cur is not None:
                cur["steps"].append(Step(m.group(1), m.group(2)))
    flush()

    out = []
    for sc in scenarios:
        m = re.match(r"\[(\d+)\]", sc["name"])
        num = m.group(1) if m else sc["name"]
        base = f"{rel}::[{num}]"
        if sc["kind"] == "scenario":
            out.append(Scenario(base, sc["name"], background + sc["steps"]))
        else:
            n = 0
            for ex in sc["examples"]:
                if not ex:
                    continue
                header, rows = ex[0], ex[1:]
                for row in rows:
                    n += 1
                    binding = dict(zip(header, row))
                    steps = [subst(s, binding) for s in sc["steps"]]
                    out.append(Scenario(f"{base}#{n}", sc["name"], background + steps))
    return out


def subst(step, binding):
    def rep(t):
        if t is None:
            return None
        for k, v in binding.items():
            t = t.replace("<" + k + ">", v)
        return t
    table = [[rep(c) for c in row] for row in step.table] if step.table else step.table
    return Step(step.keyword, rep(step.text), table, rep(step.doc))


# --------------------------------------------------------------------------------------------- TCK value grammar

class ValueParser:
    """Parses the expected-value notation of a TCK result table into canonical values."""

    def __init__(self, s):
        self.s, self.i = s, 0

    def ws(self):
        while self.i < len(self.s) and self.s[self.i].isspace():
            self.i += 1

    def parse(self):
        v = self.value()
        self.ws()
        if self.i != len(self.s):
            raise ValueError(f"trailing text in TCK value {self.s!r} at {self.i}")
        return v

    def peek(self, n=1):
        return self.s[self.i:self.i + n]

    def value(self):
        self.ws()
        c = self.peek()
        if c == "'":
            return ("str", self.string().replace("Z[UTC]", "Z"))
        if c == "[":
            # relationship literal `[:T {..}]` or list
            j = self.i + 1
            while j < len(self.s) and self.s[j].isspace():
                j += 1
            if j < len(self.s) and self.s[j] == ":":
                return self.rel()[0]
            return self.list()
        if c == "{":
            return ("map", self.map())
        if c == "(":
            return self.node()
        if c == "<":
            return self.path()
        for word, val in (("null", ("null",)), ("true", ("bool", True)), ("false", ("bool", False))):
            if self.s.startswith(word, self.i):
                self.i += len(word)
                return val
        m = re.compile(r"-?(NaN|Infinity|Inf)").match(self.s, self.i)
        if m:
            self.i = m.end()
            t = m.group(0)
            return ("float", "nan" if "NaN" in t else ("-inf" if t.startswith("-") else "inf"))
        m = re.compile(r"-?\d+(\.\d+)?([eE][-+]?\d+)?").match(self.s, self.i)
        if m:
            self.i = m.end()
            t = m.group(0)
            if "." in t or "e" in t.lower():
                return ("float", fmt_float(float(t)))
            return ("int", int(t))
        raise ValueError(f"cannot parse TCK value {self.s!r} at {self.i}")

    def string(self):
        assert self.peek() == "'"
        self.i += 1
        out = []
        while True:
            c = self.s[self.i]
            if c == "\\":
                n = self.s[self.i + 1]
                out.append({"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f"}.get(n, n))
                self.i += 2
            elif c == "'":
                self.i += 1
                return "".join(out)
            else:
                out.append(c)
                self.i += 1

    def list(self):
        self.i += 1
        items = []
        self.ws()
        if self.peek() == "]":
            self.i += 1
            return ("list", items)
        while True:
            items.append(self.value())
            self.ws()
            if self.peek() == ",":
                self.i += 1
                continue
            assert self.peek() == "]", f"list expected ] in {self.s!r} at {self.i}"
            self.i += 1
            return ("list", items)

    def map(self):
        self.i += 1
        m = {}
        self.ws()
        if self.peek() == "}":
            self.i += 1
            return m
        while True:
            self.ws()
            mk = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|`[^`]*`").match(self.s, self.i)
            key = mk.group(0).strip("`")
            self.i = mk.end()
            self.ws()
            assert self.peek() == ":"
            self.i += 1
            m[key] = self.value()
            self.ws()
            if self.peek() == ",":
                self.i += 1
                continue
            assert self.peek() == "}"
            self.i += 1
            return m

    def node(self):
        self.i += 1
        labels = []
        props = {}
        self.ws()
        while self.peek() == ":":
            self.i += 1
            mk = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|`[^`]*`").match(self.s, self.i)
            labels.append(mk.group(0).strip("`"))
            self.i = mk.end()
            self.ws()
        if self.peek() == "{":
            props = self.map()
        self.ws()
        assert self.peek() == ")", f"node expected ) in {self.s!r} at {self.i}"
        self.i += 1
        return canon_node(labels, props)

    def rel(self):
        """Parses `[:T {..}]` and returns (canonical rel, type, props)."""
        self.i += 1
        self.ws()
        rtype, props = None, {}
        if self.peek() == ":":
            self.i += 1
            mk = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|`[^`]*`").match(self.s, self.i)
            rtype = mk.group(0).strip("`")
            self.i = mk.end()
        self.ws()
        if self.peek() == "{":
            props = self.map()
        self.ws()
        assert self.peek() == "]", f"rel expected ] in {self.s!r} at {self.i}"
        self.i += 1
        return canon_rel(rtype, props), rtype, props

    def path(self):
        self.i += 1  # <
        elems = [self.node()]
        while True:
            self.ws()
            if self.peek() == ">":
                self.i += 1
                return ("path", elems)
            if self.peek(2) == "<-":
                self.i += 2
                self.ws()
                r = self.rel()[0]
                self.ws()
                assert self.peek() == "-"
                self.i += 1
                elems.append(("dir", "<", r))
            else:
                assert self.peek() == "-", f"path expected - in {self.s!r} at {self.i}"
                self.i += 1
                self.ws()
                r = self.rel()[0]
                self.ws()
                if self.peek(2) == "->":
                    self.i += 2
                    elems.append(("dir", ">", r))
                else:
                    assert self.peek() == "-"
                    self.i += 1
                    elems.append(("dir", "-", r))
            self.ws()
            elems.append(self.node())


def fmt_float(f):
    if math.isnan(f):
        return "nan"
    if math.isinf(f):
        return "inf" if f > 0 else "-inf"
    return "%.9g" % f


def canon_node(labels, props):
    return ("node", tuple(sorted(labels)), tuple(sorted(props.items())))


def canon_rel(rtype, props):
    return ("rel", rtype, tuple(sorted(props.items())))


# --------------------------------------------------------------------------------------------- actual value canonicalisation

def _frac(nanos):
    if nanos == 0:
        return ""
    if nanos % 1000000 == 0:
        return ".%03d" % (nanos // 1000000)
    if nanos % 1000 == 0:
        return ".%06d" % (nanos // 1000)
    return ".%09d" % nanos


def _hms(h, m, s, nanos):
    out = "%02d:%02d" % (h, m)
    if s or nanos:
        out += ":%02d" % s + _frac(nanos)
    return out


def _offset(td):
    if td is None:
        return ""
    total = int(td.total_seconds())
    if total == 0:
        return "Z"
    sign = "+" if total > 0 else "-"
    total = abs(total)
    h, rem = divmod(total, 3600)
    m, sec = divmod(rem, 60)
    return "%s%02d:%02d" % (sign, h, m) + (":%02d" % sec if sec else "")


def temporal_str(v):
    """Neo4j's own toString shapes for temporal values (the TCK writes them as strings)."""
    if isinstance(v, ntime.Date):
        return "%04d-%02d-%02d" % (v.year, v.month, v.day)
    if isinstance(v, ntime.Time):
        h, m, s = v.hour_minute_second_nanosecond[0], v.hour_minute_second_nanosecond[1], v.hour_minute_second_nanosecond[2]
        s_whole = int(s)
        nanos = v.nanosecond
        return _hms(v.hour, v.minute, v.second, nanos) + (_offset(v.utcoffset()) if v.tzinfo is not None else "")
    if isinstance(v, ntime.DateTime):
        out = "%04d-%02d-%02dT" % (v.year, v.month, v.day) + _hms(v.hour, v.minute, v.second, v.nanosecond)
        if v.tzinfo is not None:
            out += _offset(v.utcoffset())
            z = getattr(v.tzinfo, "zone", None) or getattr(v.tzinfo, "key", None)
            if z and z != "UTC" and not (_offset(v.utcoffset()) != "Z" and False):
                out += "[" + z + "]"
            elif z == "UTC":
                out += "[UTC]"
        return out
    if isinstance(v, ntime.Duration):
        return duration_str(v)
    return str(v)


def duration_str(d):
    months, days, secs, nanos = d.months, d.days, d.seconds, d.nanoseconds
    out = "P"
    y = int(months / 12)
    mo = months - 12 * y
    if y:
        out += f"{y}Y"
    if mo:
        out += f"{mo}M"
    if days:
        out += f"{days}D"
    total_ns = secs * 1000000000 + nanos
    t = ""
    if total_ns:
        neg = total_ns < 0
        a = abs(total_ns)
        h, rem = divmod(a, 3600 * 1000000000)
        mi, rem = divmod(rem, 60 * 1000000000)
        sec, ns = divmod(rem, 1000000000)
        sg = "-" if neg else ""
        if h:
            t += f"{sg}{h}H"
        if mi:
            t += f"{sg}{mi}M"
        if sec or ns:
            frac = ("%09d" % ns).rstrip("0") if ns else ""
            t += f"{sg}{sec}" + ("." + frac if frac else "") + "S"
    if t:
        out += "T" + t
    if out == "P":
        out = "PT0S"
    return out


def canon(v):
    if v is None:
        return ("null",)
    if isinstance(v, bool):
        return ("bool", v)
    if isinstance(v, int):
        return ("int", v)
    if isinstance(v, float):
        return ("float", fmt_float(v))
    if isinstance(v, str):
        return ("str", v)
    if isinstance(v, (ntime.Date, ntime.Time, ntime.DateTime, ntime.Duration)):
        return ("str", temporal_str(v).replace("Z[UTC]", "Z"))
    if isinstance(v, (list, tuple)) and not isinstance(v, Path):
        return ("list", [canon(x) for x in v])
    if isinstance(v, (bytes, bytearray)):
        return ("bytes", bytes(v).hex())
    if isinstance(v, dict):
        return ("map", {k: canon(x) for k, x in v.items()})
    if isinstance(v, Node):
        return canon_node(list(v.labels), {k: canon(x) for k, x in v.items()})
    if isinstance(v, Relationship):
        return canon_rel(v.type, {k: canon(x) for k, x in v.items()})
    if isinstance(v, Path):
        elems = [canon(v.nodes[0])]
        for idx, r in enumerate(v.relationships):
            prev = v.nodes[idx]
            d = ">" if r.start_node.element_id == prev.element_id and r.end_node.element_id != prev.element_id else (
                "<" if r.end_node.element_id == prev.element_id and r.start_node.element_id != prev.element_id else ">")
            elems.append(("dir", d, canon(r)))
            elems.append(canon(v.nodes[idx + 1]))
        return ("path", elems)
    if isinstance(v, (ntime.Date, ntime.Time, ntime.DateTime, ntime.Duration)):
        return ("str", temporal_str(v))
    if isinstance(v, nspatial.Point):
        return ("str", str(v))
    return ("str", str(v))


def finalize(v, ignore_list_order):
    """Turns nested canonical values into hashable/sortable text (lists optionally sorted)."""
    t = v[0]
    if t == "list":
        items = [finalize(x, ignore_list_order) for x in v[1]]
        if ignore_list_order:
            items.sort()
        return "L[" + ",".join(items) + "]"
    if t == "map":
        return "M{" + ",".join(f"{k}:{finalize(x, ignore_list_order)}" for k, x in sorted(v[1].items())) + "}"
    if t == "node":
        return "N(" + ":".join(v[1]) + "{" + ",".join(f"{k}:{finalize(x, ignore_list_order)}" for k, x in v[2]) + "})"
    if t == "rel":
        return "R[" + str(v[1]) + "{" + ",".join(f"{k}:{finalize(x, ignore_list_order)}" for k, x in v[2]) + "}]"
    if t == "path":
        return "P<" + "".join(finalize(x, ignore_list_order) for x in v[1]) + ">"
    if t == "dir":
        return v[1] + finalize(v[2], ignore_list_order)
    return repr(v)


# --------------------------------------------------------------------------------------------- execution

class Timeout(Exception):
    pass


def _alarm(signum, frame):
    raise Timeout("scenario timed out")


ERROR_CLASSES = {  # TCK error type -> plausible Neo4j status-code suffixes (validity on the oracle)
    "SyntaxError": ("SyntaxError", "TypeError", "ArgumentError", "ParameterMissing"),
}

def snapshot(session):
    """Distinct label names and the total property count (the TCK's +/-labels and +/-properties are net differences
    of these two, while +/-nodes and +/-relationships count creations/deletions and come from the summary)."""
    labels = {r["l"] for r in session.run("MATCH (n) UNWIND labels(n) AS l RETURN DISTINCT l")}
    props = session.run("MATCH (n) RETURN sum(size(keys(n))) AS c").single()["c"] or 0
    props += session.run("MATCH ()-[r]->() RETURN sum(size(keys(r))) AS c").single()["c"] or 0
    return labels, props


def side_effects(c, before, after):
    eff = {}
    for name, v in (("+nodes", c.nodes_created), ("-nodes", c.nodes_deleted),
                    ("+relationships", c.relationships_created), ("-relationships", c.relationships_deleted),
                    ("+labels", len(after[0] - before[0])), ("-labels", len(before[0] - after[0])),
                    ("+properties", max(after[1] - before[1], 0)), ("-properties", max(before[1] - after[1], 0))):
        if v:
            eff[name] = v
    return eff


def graph_query(name, root):
    p = os.path.join(root, "graphs", name)
    for f in os.listdir(p):
        if f.endswith(".cypher"):
            return open(os.path.join(p, f), encoding="utf-8").read()
    raise FileNotFoundError(p)


def reset(session, drop_schema):
    session.run("MATCH (n) DETACH DELETE n").consume()
    if drop_schema:
        try:
            for r in list(session.run("SHOW CONSTRAINTS YIELD name")):
                session.run(f"DROP CONSTRAINT `{r['name']}`").consume()
            for r in list(session.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'")):
                session.run(f"DROP INDEX `{r['name']}`").consume()
        except Exception:  # noqa: BLE001
            pass


def error_code(e):
    return getattr(e, "code", None) or type(e).__name__


class Skip(Exception):
    pass


def run_scenario(driver, sc, root, oracle_entry=None):
    """Returns (status, detail, extra). status: pass|fail|skip; extra carries {"error_code": ...} for error scenarios."""
    if any(s.text.startswith("there exists a procedure") for s in sc.steps):
        raise Skip("needs client-registered test procedure")
    drop_schema = any(re.search(r"\b(CONSTRAINT|INDEX)\b", (s.doc or ""), re.I) for s in sc.steps)
    params, result, error, effects, extra = {}, None, None, None, {}
    with driver.session() as session:
        reset(session, drop_schema)
        for st in sc.steps:
            t = st.text
            if t in ("an empty graph", "any graph"):
                continue
            m = re.match(r"the (binary-tree-\d) graph$", t)
            if m:
                session.run(graph_query(m.group(1), root)).consume()
                continue
            if t.startswith("having executed"):
                session.run(st.doc, params).consume()
                continue
            if t.startswith("parameters are"):
                params = {row[0]: to_python(ValueParser(row[1]).parse()) for row in st.table}
                continue
            if t.startswith("executing query") or t.startswith("executing control query"):
                control = t.startswith("executing control")
                before = None if control else snapshot(session)
                try:
                    res = session.run(st.doc, params)
                    keys = list(res.keys())
                    records = [list(r.values()) for r in res]
                    summary = res.consume()
                    result, error = (keys, records), None
                except Timeout:
                    raise
                except Exception as e:  # noqa: BLE001
                    result, error = None, e
                if not control:
                    if error is None:
                        effects = side_effects(summary.counters, before, snapshot(session))
                    else:
                        effects = {}
                continue
            if t.startswith("the result should be empty"):
                if error is not None:
                    return "fail", f"unexpected error {error_code(error)}: {error}", extra
                if result[1]:
                    return "fail", f"expected empty result, got {len(result[1])} rows", extra
                continue
            m = re.match(r"the result should be,? ?(in any order|in order)?\s*(\(ignoring element order for lists\))?\s*:?$", t)
            if m:
                if error is not None:
                    return "fail", f"unexpected error {error_code(error)}: {str(error)[:200]}", extra
                ordered = m.group(1) == "in order"
                ign = m.group(2) is not None
                header, rows = st.table[0], st.table[1:]
                if result[0] != header:
                    return "fail", f"columns {result[0]} != {header}", extra
                exp = [[finalize(ValueParser(c).parse(), ign) for c in row] for row in rows]
                act = [[finalize(canon(c), ign) for c in row] for row in result[1]]
                if not ordered:
                    exp.sort()
                    act.sort()
                if exp != act:
                    return "fail", f"rows differ: expected {exp[:3]}.. got {act[:3]}..", extra
                continue
            if t == "no side effects":
                if error is not None:
                    return "fail", f"unexpected error {error_code(error)}: {str(error)[:200]}", extra
                if effects:
                    return "fail", f"unexpected side effects {effects}", extra
                continue
            if t.startswith("the side effects should be"):
                if error is not None:
                    return "fail", f"unexpected error {error_code(error)}: {str(error)[:200]}", extra
                want = {row[0]: int(row[1]) for row in st.table}
                if want != effects:
                    return "fail", f"side effects {effects} != expected {want}", extra
                continue
            m = re.match(r"an? (\w+) should be raised at (\w+(?: \w+)?):\s*(.*)$", t)
            if m:
                if error is None:
                    return "fail", f"expected {m.group(1)} at {m.group(2)}, query succeeded", extra
                code = error_code(error)
                extra["error_code"] = code
                if not str(code).startswith("Neo."):
                    return "fail", f"non-Neo4j error {code}: {str(error)[:200]}", extra
                if oracle_entry is not None and oracle_entry.get("error_code") != code:
                    return "fail", f"error code {code} != oracle {oracle_entry.get('error_code')}: {str(error)[:200]}", extra
                continue
            raise Skip(f"unsupported step: {t}")
    return "pass", "", extra


def to_python(v):
    t = v[0]
    if t == "null":
        return None
    if t in ("bool", "int", "str"):
        return v[1]
    if t == "float":
        return float(v[1].replace("nan", "nan"))
    if t == "list":
        return [to_python(x) for x in v[1]]
    if t == "map":
        return {k: to_python(x) for k, x in v[1].items()}
    raise Skip("graph values as parameters are not supported")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--features", required=True, help="the openCypher `tck` directory (contains features/ and graphs/)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--only-valid", help="oracle results json: run only scenarios that passed there")
    ap.add_argument("--filter", default="", help="substring filter on scenario id")
    ap.add_argument("--timeout", type=int, default=30, help="per-scenario seconds")
    a = ap.parse_args()

    oracle = None
    if a.only_valid:
        import gzip
        raw = json.load(gzip.open(a.only_valid, "rt") if a.only_valid.endswith(".gz") else open(a.only_valid))
        oracle = raw["scenarios"] if "scenarios" in raw else {k: dict(v, status="pass") for k, v in raw["valid"].items()}
    scenarios = []
    fdir = os.path.join(a.features, "features")
    for dp, _, fns in sorted(os.walk(fdir)):
        for fn in sorted(fns):
            if fn.endswith(".feature"):
                scenarios.extend(parse_feature(os.path.join(dp, fn), os.path.relpath(os.path.join(dp, fn), fdir)))
    driver = GraphDatabase.driver(a.uri, auth=None, max_connection_lifetime=3600)
    signal.signal(signal.SIGALRM, _alarm)
    results = {}
    counts = {"pass": 0, "fail": 0, "skip": 0}
    t0 = time.time()
    for sc in scenarios:
        if a.filter and a.filter not in sc.id:
            continue
        entry = None
        if oracle is not None:
            entry = oracle.get(sc.id)
            if entry is None or entry["status"] != "pass":
                continue
        signal.alarm(a.timeout)
        try:
            status, detail, extra = run_scenario(driver, sc, a.features, entry)
        except Skip as e:
            status, detail, extra = "skip", str(e), {}
        except Timeout:
            status, detail, extra = "fail", "timeout", {}
        except Exception as e:  # noqa: BLE001
            status, detail, extra = "fail", f"harness/driver error: {type(e).__name__}: {str(e)[:300]}", {}
        finally:
            signal.alarm(0)
        counts[status] += 1
        results[sc.id] = {"status": status, "detail": detail, "name": sc.name, **extra}
        if status == "fail" and oracle is not None:
            print("FAIL", sc.id, detail[:200], flush=True)
    json.dump({"counts": counts, "scenarios": results}, open(a.out, "w"), indent=1, sort_keys=True)
    print(json.dumps(counts), f"{time.time() - t0:.0f}s")
    driver.close()


if __name__ == "__main__":
    main()
