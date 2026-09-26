"""Differential harness of cqlwire: runs the corpus (cql_corpus.py) with the real python cassandra-driver, records the answers of a
real Apache Cassandra (5.0) into golden.json.gz (`python3 cql_harness.py record --port N`), and compares a run against Warp with them
(`compare`, used offline by test_cql_conformance.py). Results are normalized to JSON so both sides are comparable byte for byte."""
import argparse
import base64
import datetime
import decimal
import gzip
import ipaddress
import json
import os
import re
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra import util as cutil  # noqa: E402
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile  # noqa: E402
from cassandra.query import BatchStatement, BatchType, SimpleStatement, tuple_factory  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402

GOLDEN = os.path.join(HERE, "golden.json.gz")


# ------------------------------------------------------------------ step constructors used by the corpus

class P:
    """A prepared statement executed with params."""

    def __init__(self, cql, params=(), page=None):
        self.cql, self.params, self.page = cql, list(params), page


class Q:
    """A simple statement with options: page size (fetch all pages, recording page sizes), x = row transform."""

    def __init__(self, cql, page=None, x=None, sort=False):
        self.cql, self.page, self.x, self.sort = cql, page, x, sort


class B:
    """A batch: kind logged|unlogged|counter, entries are cql strings or (cql, params) prepared entries."""

    def __init__(self, kind, entries):
        self.kind, self.entries = kind, entries


class M:
    """The schema metadata of the keyspace as the driver rebuilt it from system_schema (export_as_string)."""

    def __init__(self, ks=None):
        self.ks = ks


class U:
    """Executes a prepared statement with an UNSET_VALUE for some params (index list)."""

    def __init__(self, cql, params, unset=()):
        self.cql, self.params, self.unset = cql, list(params), list(unset)


class W:
    """Wait seconds (TTL expiry)."""

    def __init__(self, s):
        self.s = s


class Case:
    def __init__(self, name, steps, ks=True):
        self.name, self.steps, self.ks = name, steps, ks


# ------------------------------------------------------------------ normalization

def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if v != v:
            return {"f": "NaN"}
        return {"f": repr(v)}
    if isinstance(v, decimal.Decimal):
        return {"d": str(v)}
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return {"b": bytes(v).hex()}
    if isinstance(v, datetime.datetime):
        return {"ts": v.isoformat()}
    if isinstance(v, cutil.Date):
        return {"date": str(v)}
    if isinstance(v, cutil.Time):
        return {"time": str(v)}
    if isinstance(v, cutil.Duration):
        return {"dur": [v.months, v.days, v.nanoseconds]}
    if isinstance(v, uuid.UUID):
        return {"u": str(v)}
    if isinstance(v, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return {"ip": str(v)}
    if isinstance(v, (cutil.SortedSet, set, frozenset)):
        return {"set": [norm(x) for x in v]}
    if isinstance(v, (cutil.OrderedMap, cutil.OrderedMapSerializedKey, dict)):
        items = v.items() if hasattr(v, "items") else v
        return {"map": [[norm(k), norm(x)] for k, x in items]}
    if isinstance(v, list):
        return [norm(x) for x in v]
    if isinstance(v, tuple):
        if hasattr(v, "_fields"):
            return {"udt": {f: norm(getattr(v, f)) for f in v._fields}}
        return {"tup": [norm(x) for x in v]}
    return {"other": repr(v)}


def type_name(t):
    try:
        return t.cql_parameterized_type()
    except Exception:  # noqa: BLE001
        return getattr(t, "typename", str(t))


ERR_RE = re.compile(r'message="(.*)"$', re.S)


def norm_error(e):
    name = type(e).__name__
    msg = str(e)
    m = ERR_RE.search(msg)
    if m:
        msg = m.group(1)
    if hasattr(e, "keyspace") and name == "AlreadyExists":
        msg = f"{e.keyspace}.{e.table}"
    return {"err": name, "msg": msg}


def fetch(session, stmt, params=None, page=None):
    pages = []
    if page:
        stmt.fetch_size = page
    rs = session.execute(stmt, params) if params is not None else session.execute(stmt)
    cols = list(rs.column_names or [])
    types = [type_name(t) for t in (rs.column_types or [])]
    rows = []
    while True:
        cur = list(rs.current_rows)
        pages.append(len(cur))
        rows.extend(cur)
        if not rs.has_more_pages:
            break
        rs.fetch_next_page()
    return cols, types, rows, pages


def run_step(session, step, ks):
    def sub(c):
        return c.replace("{ks}", ks)

    try:
        if isinstance(step, str):
            step = Q(step)
        if isinstance(step, W):
            time.sleep(step.s)
            return {"wait": step.s}
        if isinstance(step, Q):
            stmt = SimpleStatement(sub(step.cql))
            cols, types, rows, pages = fetch(session, stmt, None, step.page)
            if step.x:
                rows = step.x(rows)
                return {"rows": [[norm(c) for c in r] if isinstance(r, tuple) else norm(r) for r in rows]}
            out = {"cols": cols, "types": types, "rows": [[norm(c) for c in r] for r in rows]}
            if step.sort:
                out["rows"].sort(key=lambda r: json.dumps(r, sort_keys=True))
            if step.page:
                out["pages"] = pages
            return out
        if isinstance(step, P):
            ps = session.prepare(sub(step.cql))
            cols, types, rows, pages = fetch(session, ps.bind(step.params), None, step.page)
            out = {"cols": cols, "types": types, "rows": [[norm(c) for c in r] for r in rows]}
            if step.page:
                out["pages"] = pages
            return out
        if isinstance(step, M):
            session.cluster.refresh_schema_metadata()
            km = session.cluster.metadata.keyspaces[step.ks or ks]
            return {"cql": km.export_as_string()}
        if isinstance(step, U):
            from cassandra.query import UNSET_VALUE
            ps = session.prepare(sub(step.cql))
            params = [UNSET_VALUE if i in step.unset else p for i, p in enumerate(step.params)]
            rs = session.execute(ps.bind(params))
            return {"rows": [[norm(c) for c in r] for r in rs.current_rows]}
        if isinstance(step, B):
            kind = {"logged": BatchType.LOGGED, "unlogged": BatchType.UNLOGGED, "counter": BatchType.COUNTER}[step.kind]
            b = BatchStatement(batch_type=kind)
            for e in step.entries:
                if isinstance(e, tuple):
                    b.add(session.prepare(sub(e[0])), e[1])
                else:
                    b.add(SimpleStatement(sub(e)))
            rs = session.execute(b)
            cols = list(rs.column_names or [])
            rows = list(rs.current_rows)
            return {"cols": cols, "rows": [[norm(c) for c in r] for r in rows]} if cols else {"rows": []}
    except Exception as e:  # noqa: BLE001 -- errors are part of the recorded behaviour
        return norm_error(e)
    raise ValueError(step)


def connect(port, user=None, password=None, host="127.0.0.1"):
    prof = ExecutionProfile(row_factory=tuple_factory, request_timeout=120, consistency_level=ConsistencyLevel.ONE)
    auth = PlainTextAuthProvider(user, password) if user else None
    c = Cluster([host], port=port, protocol_version=4, execution_profiles={EXEC_PROFILE_DEFAULT: prof}, auth_provider=auth,
                connect_timeout=30, control_connection_timeout=30)
    return c, c.connect()


def ks_name(case):
    return "k_" + re.sub(r"[^a-z0-9_]", "_", case.name.lower())


REPL = "{'class': 'SimpleStrategy', 'replication_factor': 1}"


def run_case(port, case, user=None, password=None):
    ks = ks_name(case)
    cluster, session = connect(port, user, password)
    try:
        if case.ks:
            session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
            session.execute(f"CREATE KEYSPACE {ks} WITH replication = {REPL}")
        out = []
        for s in case.steps:
            out.append(run_step(session, s, ks))
        return out
    finally:
        try:
            if case.ks:
                session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
        except Exception:  # noqa: BLE001
            pass
        cluster.shutdown()


def load_corpus():
    import cql_corpus
    return cql_corpus.cases()


def load_golden():
    with gzip.open(GOLDEN, "rt") as f:
        return json.load(f)


def step_text(step):
    if isinstance(step, str):
        return step
    if isinstance(step, (Q, P, U)):
        return step.cql
    if isinstance(step, M):
        return "METADATA"
    if isinstance(step, B):
        return "BATCH " + step.kind
    return "WAIT"


def record(port, runs=2, only=None):
    corpus = load_corpus()
    golden = {}
    for case in corpus:
        if only and only not in case.name:
            continue
        results = [run_case(port, case) for _ in range(runs)]
        stable = []
        unstable = 0
        for i, s in enumerate(case.steps):
            first = results[0][i]
            if all(r[i] == first for r in results[1:]):
                stable.append(first)
            else:
                stable.append({"unstable": True})
                unstable += 1
        golden[case.name] = stable
        print(f"recorded {case.name}: {len(stable)} steps, {unstable} unstable", flush=True)
    if only:
        try:
            old = load_golden()
        except OSError:
            old = {}
        old.update(golden)
        golden = old
    with gzip.open(GOLDEN, "wt") as f:
        json.dump(golden, f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["record", "run"])
    ap.add_argument("--port", type=int, default=19142)
    ap.add_argument("--only")
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--out")
    a = ap.parse_args()
    if a.mode == "record":
        record(a.port, a.runs, a.only)
    else:
        res = {}
        for case in load_corpus():
            if a.only and a.only not in case.name:
                continue
            res[case.name] = run_case(a.port, case)
        with open(a.out, "w") as f:
            json.dump(res, f)


if __name__ == "__main__":
    import cql_harness  # the corpus imports this module by name: run its copy so the step classes are the same

    cql_harness.main()


def compare(golden, actual, corpus):
    """Mismatches between the recorded Cassandra answers and a run: [(case, index, step text, expected, actual, kind)] where kind is
    'result' (rows or error class differ), 'msg' (same error class, other text and not a documented cosmetic difference) or
    'stale' (a documented divergence that no longer occurs)."""
    import cql_known as K
    out = []
    used_patterns, used_results = set(), set()
    for case in corpus:
        if case.name not in actual:
            continue
        gold, got = golden[case.name], actual[case.name]
        for i, step in enumerate(case.steps):
            e, a = gold[i], got[i]
            if e.get("unstable") or "wait" in e:
                continue
            key = (case.name, i)
            if e == a:
                if key in K.RESULT_DIVERGENCES:
                    out.append((case.name, i, step_text(step), e, a, "stale"))
                continue
            if "err" in e and "err" in a and e["err"] == a["err"]:
                hit = next((n for n, (rx, _) in enumerate(K.MESSAGE_PATTERNS) if rx.search(e["msg"])), None)
                if hit is not None:
                    used_patterns.add(hit)
                    continue
                out.append((case.name, i, step_text(step), e, a, "msg"))
                continue
            if key in K.RESULT_DIVERGENCES:
                used_results.add(key)
                continue
            out.append((case.name, i, step_text(step), e, a, "result"))
    return out, used_patterns, used_results
