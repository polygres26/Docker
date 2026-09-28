"""Recording (against a real Apache TinkerPop Gremlin Server) and offline replay (against Warp) of the gr_corpus.py cases.

Both directions speak the Gremlin Server WebSocket protocol raw (GraphSON 3.0, the application/json mimetype): every case's script is sent as an
`eval` request and, when the python DSL can express it, the same traversal as a `bytecode` request. The recorded answer is the merged chunk data
(typed GraphSON), the final status code, the error message and the chunk status codes. `compare` decides whether Warp's answer counts as the same:
status codes equal, results equal after canonicalisation (maps and sets unordered, the top-level result list unordered unless the traversal orders
it, VertexProperty ids ignored -- they are per-server counters).

    python gr_harness.py record ws://localhost:18182/gremlin      # rewrites golden.json.gz (run twice: unstable answers are dropped)
    python gr_harness.py diff ws://localhost:PORT/gremlin [prefix] # dev loop: compare a live server with the golden
"""
import gzip
import json
import os
import re
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gr_corpus as C  # noqa: E402
import gr_known as K  # noqa: E402

GOLDEN_PATH = os.path.join(HERE, "golden.json.gz")
MIME = "application/json"


# ---------------------------------------------------------------------------------------------------------------- wire
class RawWs:
    def __init__(self, url, timeout=60):
        import websocket
        self.ws = websocket.create_connection(url, timeout=timeout)

    def close(self):
        try:
            self.ws.close()
        except Exception:  # noqa: BLE001
            pass

    def send(self, op, processor, args, mime=MIME):
        msg = {"requestId": str(uuid.uuid4()), "op": op, "processor": processor, "args": args}
        m = mime.encode()
        self.ws.send_binary(bytes([len(m)]) + m + json.dumps(msg).encode())
        out = []
        while True:
            raw = self.ws.recv()
            r = json.loads(raw)
            out.append(r)
            if r["status"]["code"] != 206:
                return out

    def eval(self, script, **kw):
        args = {"gremlin": script, "language": "gremlin-groovy"}
        args.update(kw)
        return self.send("eval", "", args)

    def bytecode(self, args):
        return self.send("bytecode", "traversal", args)


def merge(resps):
    """One answer from the chunks of a response stream."""
    data = []
    chunks = []
    for r in resps:
        d = r["result"]["data"]
        items = d["@value"] if isinstance(d, dict) and "@type" in d else (d or [])
        data.extend(items)
        chunks.append([r["status"]["code"], len(items)])
    last = resps[-1]["status"]
    return {"code": last["code"], "message": last["message"], "data": data, "chunks": chunks}


# ---------------------------------------------------------------------------------------------------------------- canonical form
def _canon(v):
    if isinstance(v, dict) and "@type" in v:
        t, val = v["@type"], v["@value"]
        if t == "g:List":
            return ["L", [_canon(x) for x in val]]
        if t == "g:Set":
            return ["S", sorted((_canon(x) for x in val), key=_key)]
        if t == "g:BulkSet":
            items = [(_canon(val[i]), val[i + 1]["@value"] if isinstance(val[i + 1], dict) else val[i + 1]) for i in range(0, len(val), 2)]
            return ["B", sorted(items, key=_key)]
        if t == "g:Map":
            pairs = [[_canon(val[i]), _canon(val[i + 1])] for i in range(0, len(val), 2)]
            return ["M", sorted(pairs, key=_key)]
        if t == "g:Vertex":
            props = {k: [_canon(p) for p in ps] for k, ps in (val.get("properties") or {}).items()}
            return ["V", _canon(val["id"]), val["label"], props]
        if t == "g:VertexProperty":
            meta = {k: _canon(p) for k, p in (val.get("properties") or {}).items()}
            return ["VP", val["label"], _canon(val["value"]), meta]
        if t == "g:Edge":
            props = {k: _canon(p) for k, p in (val.get("properties") or {}).items()}
            return ["E", _canon(val["id"]), val["label"], _canon(val["outV"]), val["outVLabel"], _canon(val["inV"]), val["inVLabel"], props]
        if t == "g:Property":
            return ["P", val["key"], _canon(val["value"])]
        if t == "g:Path":
            return ["PATH", [sorted(_canon(s)[1], key=_key) for s in val["labels"]["@value"]], [_canon(o) for o in val["objects"]["@value"]]]
        if t == "g:Tree":
            return ["TREE", sorted(([_canon(e["key"]), _canon(e["value"])] for e in val), key=_key)]
        return [t, _canon(val)]
    if isinstance(v, list):
        return [_canon(x) for x in v]
    if isinstance(v, dict):
        return {k: _canon(x) for k, x in v.items()}
    return v


def _key(x):
    return json.dumps(x, sort_keys=True)


def _unbulk(data):
    """bytecode answers are lists of g:Traverser (value + bulk): expand each to `bulk` copies of its value."""
    out = []
    for x in data:
        if isinstance(x, dict) and x.get("@type") == "g:Traverser":
            v = x["@value"]
            out.extend([v["value"]] * int(v["bulk"]["@value"]))
        else:
            out.append(x)
    return out


def canon_data(data, ordered, bc=False):
    if bc:
        data = _unbulk(data)
    items = [_canon(x) for x in data]
    return items if ordered else sorted(items, key=_key)


def compare(gold, got, case, mode="eval"):
    """[] when `got` (an answer of Warp) matches `gold` (the recorded answer of the reference server), else the differences."""
    diffs = []
    if gold["code"] != got["code"]:
        diffs.append(f"status {gold['code']} != {got['code']}: ref={gold['message'][:100]!r} warp={got['message'][:200]!r}")
        return diffs
    if gold["code"] >= 400:
        if case.msg and gold["message"] != got["message"]:
            diffs.append(f"message {gold['message']!r} != {got['message']!r}")
        return diffs
    if case.batch is not None and gold["chunks"] != got["chunks"]:
        diffs.append(f"chunks {gold['chunks']} != {got['chunks']}")
    a = canon_data(gold["data"], case.ordered, mode == "bc")
    b = canon_data(got["data"], case.ordered, mode == "bc")
    if a != b:
        diffs.append(f"data differs:\n   ref : {json.dumps(a)[:600]}\n   warp: {json.dumps(b)[:600]}")
    return diffs


# ---------------------------------------------------------------------------------------------------------------- python DSL translation
_RENAME = {"in", "as", "is", "not", "and", "or", "from", "with", "filter", "range", "sum", "max", "min", "id", "all", "list", "set", "global", "any", "format"}
_PRED = {"eq", "neq", "lt", "lte", "gt", "gte", "within", "without", "between", "inside", "outside"}
_TEXTP = {"containing", "notContaining", "startingWith", "notStartingWith", "endingWith", "notEndingWith", "regex", "notRegex"}
_ENUM = {"id": "T.id_", "label": "T.label", "key": "T.key", "value": "T.value", "keys": "Column.keys", "values": "Column.values",
         "asc": "Order.asc", "desc": "Order.desc", "shuffle": "Order.shuffle", "local": "Scope.local", "global": "Scope.global_",
         "single": "Cardinality.single", "list": "Cardinality.list_", "set": "Cardinality.set_", "first": "Pop.first", "last": "Pop.last",
         "all": "Pop.all_", "mixed": "Pop.mixed", "sum": "Operator.sum_", "minus": "Operator.minus", "mult": "Operator.mult", "div": "Operator.div",
         "min": "Operator.min_", "max": "Operator.max_", "assign": "Operator.assign", "none": "Pick.none", "any": "Pick.any_"}
_TOK = re.compile(r"""('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")|(\d+\.\d+[dDfF]|\d+[lL]|\d+\.\d+|\d+)|([A-Za-z_][A-Za-z_0-9]*)|(\s+)|(.)""")


def to_python(script):
    toks = [m for m in _TOK.finditer(script)]
    out = []
    prev = ""  # previous significant token text
    for i, m in enumerate(toks):
        s, num, ident, ws, other = m.groups()
        if s is not None:
            out.append(s)
            prev = s
        elif num is not None:
            if num[-1] in "lL":
                out.append(f"long({num[:-1]})")
            elif num[-1] in "dD":
                out.append(num[:-1])
            elif num[-1] in "fF":
                raise ValueError("float literal: the python DSL has no Float type")
            elif "." in num:
                raise ValueError("BigDecimal literal")
            else:
                out.append(num)
            prev = num
        elif ident is not None:
            nxt = ""
            for m2 in toks[i + 1:]:
                if m2.group(4) is None:
                    nxt = m2.group(0)
                    break
            if prev == ".":
                out.append(ident + "_" if ident in _RENAME else ident)
            elif ident in ("true", "false", "null"):
                out.append({"true": "True", "false": "False", "null": "None"}[ident])
            elif ident in ("g", "T", "P", "TextP", "Order", "Scope", "Column", "Cardinality", "Pop", "Operator", "Direction", "Pick", "Merge",
                           "WithOptions", "__", "UUID"):
                out.append(ident)
            elif nxt == "(":
                if ident in _PRED:
                    out.append("P." + ident)
                elif ident in _TEXTP:
                    out.append("TextP." + ident)
                else:
                    out.append("__." + (ident + "_" if ident in _RENAME else ident))
            elif ident in _ENUM:
                out.append(_ENUM[ident])
            else:
                out.append(ident)
            prev = ident
        elif ws is not None:
            out.append(ws)
        else:
            out.append(other)
            prev = other
    py = "".join(out)
    # qualified enum members
    py = re.sub(r"\bT\.(id|label|key|value)\b(?!_)", lambda m: "T.id_" if m.group(1) == "id" else m.group(0), py)
    py = py.replace("Cardinality.list)", "Cardinality.list_)").replace("Cardinality.set)", "Cardinality.set_)")
    py = py.replace("Scope.global)", "Scope.global_)").replace("Pop.all)", "Pop.all_)")
    py = re.sub(r"\bOrder\.desc\b", "Order.desc", py)
    return py


_NS = None


def _namespace():
    global _NS
    if _NS is None:
        from gremlin_python.process.graph_traversal import __, GraphTraversalSource
        from gremlin_python.process.traversal import (Cardinality, Column, Direction, Merge, Operator, Order, P, Pick, Pop, Scope, T,
                                                      TextP, TraversalStrategies, WithOptions)
        from gremlin_python.statics import long
        from gremlin_python.structure.graph import Graph
        _NS = dict(__=__, P=P, TextP=TextP, T=T, Order=Order, Scope=Scope, Column=Column, Cardinality=Cardinality, Pop=Pop, Operator=Operator,
                   Direction=Direction, Pick=Pick, Merge=Merge, WithOptions=WithOptions, long=long,
                   g=GraphTraversalSource(Graph(), TraversalStrategies()))
    return _NS


def bytecode_args(script):
    """The `args` of a bytecode request for `script` (GraphSON 3.0 typed), or None when the python DSL cannot express it."""
    from gremlin_python.structure.io import graphsonV3d0
    try:
        py = to_python(script)
        t = eval(py, dict(_namespace()))  # noqa: S307 -- the corpus is trusted input
        bc = t.bytecode
    except Exception:  # noqa: BLE001 -- not expressible in the python DSL
        return None
    return {"gremlin": json.loads(graphsonV3d0.GraphSONWriter().write_object(bc)), "aliases": {"g": "g"}}


# ---------------------------------------------------------------------------------------------------------------- running cases
def load_graph(ws, name):
    for s in C.GRAPHS[name]:
        r = ws.eval(s)
        assert r[-1]["status"]["code"] in (200, 204), (s, r[-1]["status"])


def run_case(ws, case, batch_default=None):
    """The answers of one case: [{script, eval, bc}] for each step."""
    out = []
    for i, script in enumerate(case.steps):
        kw = {"batchSize": case.batch} if case.batch is not None and i == len(case.steps) - 1 else {}
        e = merge(ws.eval(script, **kw))
        entry = {"script": script, "eval": e, "bc": None}
        if case.bc and i == len(case.steps) - 1:
            args = bytecode_args(script)
            if args is not None:
                if case.batch is not None:
                    args["batchSize"] = case.batch
                entry["bc"] = {"args": args, "resp": merge(ws.bytecode(args))}
        out.append(entry)
    return out


def run_all(url, only=None, on_case=None):
    """Runs every case (grouped by graph so the toy graphs are loaded once) and yields (case, answers)."""
    ws = RawWs(url)
    try:
        loaded = None
        cases = [c for c in C.CASES if only is None or c.name.startswith(only)]
        order = sorted(cases, key=lambda c: (c.mutating, ["modern", "classic", "empty"].index(c.graph)))
        for c in order:
            if c.mutating or loaded != c.graph:
                load_graph(ws, c.graph)
                loaded = c.graph if not c.mutating else None
            yield c, run_case(ws, c)
    finally:
        ws.close()


def load_golden():
    with gzip.open(GOLDEN_PATH, "rt") as f:
        return json.load(f)


def record(url, out_path=GOLDEN_PATH, passes=2):
    """Records the reference server's answers `passes` times and keeps only the answers that were identical every time."""
    runs = []
    for p in range(passes):
        run = {}
        n = 0
        for c, ans in run_all(url):
            run[c.name] = ans
            n += 1
            if n % 200 == 0:
                print(f"  pass {p + 1}: {n} cases", flush=True)
        runs.append(run)
    golden = {"reference": "tinkerpop/gremlin-server (TinkerGraph, GraphSON 3.0 over WebSocket)", "cases": {}}
    dropped = []
    for name, first in runs[0].items():
        stable = True
        for other in runs[1:]:
            for a, b in zip(first, other[name]):
                ka = compare(a["eval"], b["eval"], next(c for c in C.CASES if c.name == name))
                if ka:
                    stable = False
                if (a["bc"] is None) != (b["bc"] is None) or (a["bc"] and compare(a["bc"]["resp"], b["bc"]["resp"], next(c for c in C.CASES if c.name == name))):
                    stable = False
        if stable:
            golden["cases"][name] = first
        else:
            dropped.append(name)
    golden["dropped_unstable"] = dropped
    with gzip.open(out_path, "wt") as f:
        json.dump(golden, f, separators=(",", ":"))
    print(f"recorded {len(golden['cases'])} cases, dropped {len(dropped)} unstable: {dropped[:20]}")


def replay(url, golden, only=None):
    """Runs the golden cases against `url`; returns {name: [differences]} for the cases that differ (known divergences excluded)."""
    failures = {}
    for c, ans in run_all(url, only=only):
        g = golden["cases"].get(c.name)
        if g is None:
            continue
        skip = K.skip_reason(c.name)
        if skip:
            continue
        diffs = []
        for gs, ws_ in zip(g, ans):
            if gs["script"] != ws_["script"]:
                diffs.append("corpus changed since recording: " + gs["script"])
                continue
            if not K.eval_divergence(c.name, gs["eval"], ws_["eval"]):
                diffs += [f"[eval] {d}" for d in compare(gs["eval"], ws_["eval"], c)]
            if gs["bc"] is not None and ws_["bc"] is not None and not K.bc_divergence(c.name, gs["bc"]["resp"], ws_["bc"]["resp"]):
                diffs += [f"[bytecode] {d}" for d in compare(gs["bc"]["resp"], ws_["bc"]["resp"], c, "bc")]
        if diffs:
            failures[c.name] = [c.script] + diffs
    return failures


if __name__ == "__main__":
    cmd = sys.argv[1]
    url = sys.argv[2]
    if cmd == "record":
        record(url)
    elif cmd == "diff":
        only = sys.argv[3] if len(sys.argv) > 3 else None
        gold = load_golden()
        fails = replay(url, gold, only)
        for name, d in fails.items():
            print("==", name)
            for x in d:
                print("  ", x)
        n = len([c for c in C.CASES if c.name in gold["cases"] and (only is None or c.name.startswith(only))])
        print(f"{n - len(fails)}/{n} cases match; {len(fails)} differ")
