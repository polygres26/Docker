"""Recording / replay harness of the Kafka conformance corpus (kf_corpus.py).

`record` runs every case against a REAL Apache Kafka (docker, KRaft) and stores the normalized observations in golden.json.gz; the pytest
(test_kafka_conformance.py) replays the same cases OFFLINE against Warp and diffs them. Values that differ by construction (broker id,
member ids, producer ids, topic ids, timestamps, error message text) are normalized away; every remaining difference must be explained
in kf_known.py.
"""
import gzip
import json
import os
import re
import sys
import threading
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kafka_raw_client as RC  # noqa: E402

GOLDEN = os.environ.get("KF_GOLDEN") or os.path.join(HERE, "golden.json.gz")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
BROKER_KEYS = {"node_id", "leader_id", "controller_id", "broker_id", "replica_nodes", "isr_nodes", "offline_replicas_", "preferred_read_replica_"}
DROP_KEYS = {"throttle_time_ms", "session_id", "session_lifetime_ms", "unknown_tags", "tags", "cluster_id", "authorized_operations", "rack"}
MSG_KEYS = {"error_message", "batch_index_error_message"}


class Ctx:
    """What a corpus case sees: a way to reach the broker under test, deterministic names and a recorder for observations."""

    def __init__(self, host, port, case, tag=""):
        self.host = host
        self.port = port
        self.case = case
        self.tag = tag
        self.steps = []
        self.aliases = {}
        self._n = 0
        self._raws = []

    @property
    def bootstrap(self):
        return "%s:%d" % (self.host, self.port)

    def name(self, suffix=""):
        """A topic / group name unique to this case and this run (`tag` keeps two runs against one broker apart)."""
        self._n += 1
        return "%s%s_%s%d" % (self.case.replace("test_", "").replace("__", "_")[:40], self.tag, suffix or "n", self._n)

    def raw(self, client_id="raw"):
        r = RC.Raw(self.host, self.port, client_id=client_id)
        self._raws.append(r)
        return r

    def alias(self, kind, value):
        key = (kind, value)
        if key not in self.aliases:
            self.aliases[key] = "<%s#%d>" % (kind, 1 + sum(1 for k in self.aliases if k[0] == kind))
        return self.aliases[key]

    def norm(self, v, key=None):
        if isinstance(v, dict):
            out = {}
            for k, x in v.items():
                if k in DROP_KEYS:
                    continue
                out[k] = self.norm(x, k)
            return out
        if isinstance(v, (list, tuple)):
            return [self.norm(x, key) for x in v]
        if isinstance(v, bytes):
            return {"bytes": v.hex()}
        if key in BROKER_KEYS and isinstance(v, int):
            return "<broker>"
        if key in ("host", "port", "client_host") and v is not None:
            return "<" + key + ">"
        if key in MSG_KEYS and isinstance(v, str):
            return "<msg>"
        if key == "topic_id" and isinstance(v, str) and UUID_RE.match(v) and v != "00000000-0000-0000-0000-000000000000":
            return "<topic-id>"
        if key == "producer_id" and isinstance(v, int) and v >= 0:
            return self.alias("pid", v)
        if key in ("member_id", "leader") and isinstance(v, str) and v and len(v) > 8:
            return self.alias("member", v)
        if isinstance(v, str):
            for (kind, val), al in list(self.aliases.items()):
                if kind == "member" and val and val in v:
                    v = v.replace(val, al)
            return re.sub(r"Zq\d", "<tag>", v)
        return v

    def step(self, label, value):
        self.steps.append([label, self.norm(value)])

    def close(self):
        for r in self._raws:
            r.close()


def warmup(host, port):
    """A real broker creates its group-coordination state lazily: join a throw-away group until the coordinator answers."""
    import kf_corpus as C
    r = RC.Raw(host, port, client_id="warmup")
    try:
        t0 = time.time()
        pending = ["warmup-group-%d" % i for i in range(120)]  # __consumer_offsets has 50 partitions: touch every one
        while pending and time.time() - t0 < 120:
            g = pending[0]
            d = r.call("DescribeGroups", 5, groups=[g], include_authorized_operations=False)
            if d["groups"][0]["error_code"] in (14, 15, 16):
                time.sleep(0.5)
                continue
            pending.pop(0)
        return not pending
    finally:
        r.close()
    return False


def run_case(fn, host, port, tag=""):
    ctx = Ctx(host, port, fn.__name__, tag)
    try:
        fn(ctx)
    except Exception as e:  # noqa: BLE001
        ctx.steps.append(["EXCEPTION", {"type": type(e).__name__, "text": str(e)[:200], "tb": traceback.format_exc()[-800:]}])
    finally:
        ctx.close()
    return ctx.steps


def load_golden():
    with gzip.open(GOLDEN, "rt") as f:
        return json.load(f)


def save_golden(data):
    with gzip.open(GOLDEN, "wt", compresslevel=9) as f:
        json.dump(data, f, sort_keys=True, separators=(",", ":"))


def diff(a, b, path=""):
    """Paths at which two normalized values differ."""
    out = []
    if type(a) != type(b) and not (isinstance(a, (int, float)) and isinstance(b, (int, float))):
        return [(path or "$", a, b)]
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append((path + "." + k, a.get(k, "<absent>"), b.get(k, "<absent>")))
            else:
                out.extend(diff(a[k], b[k], path + "." + k))
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append((path + ".len", len(a), len(b)))
        for i, (x, y) in enumerate(zip(a, b)):
            out.extend(diff(x, y, "%s[%d]" % (path, i)))
    elif a != b:
        out.append((path or "$", a, b))
    return out


def record(host, port, runs=2, only=None):
    """Record every case `runs` times against the broker at host:port; steps that differ between runs are dropped (unstable)."""
    import kf_corpus as C
    cases = C.cases()
    warmup(host, port)
    data = {"cases": {}, "meta": {"runs": runs, "recorded": time.strftime("%Y-%m-%d")}}
    for fn in cases:
        if only and only not in fn.__name__:
            continue
        results = []
        for i in range(runs):
            tags = (os.environ.get("KF_TAGS") or "Zq0,Zq1").split(",")
            results.append(run_case(fn, host, port, tag=tags[i % len(tags)]))
        base = results[0]
        keep = []
        for idx, step in enumerate(base):
            same = all(idx < len(r) and r[idx] == step for r in results[1:])
            if same:
                keep.append(step)
            else:
                keep.append([step[0], {"__unstable__": True}])
        data["cases"][fn.__name__] = keep
        print("recorded %-46s %3d steps, %d unstable" % (fn.__name__, len(keep), sum(1 for s in keep if s[1] == {"__unstable__": True})), flush=True)
    return data


if __name__ == "__main__":
    only = sys.argv[3] if len(sys.argv) > 3 else None
    g = record(sys.argv[1], int(sys.argv[2]), only=only)
    if only:
        try:
            old = load_golden()
        except OSError:
            old = {"cases": {}, "meta": g["meta"]}
        old["cases"].update(g["cases"])
        g = old
    save_golden(g)
    print("golden written:", GOLDEN, sum(len(v) for v in g["cases"].values()), "steps")


def compare(case, golden_steps, got_steps):
    """(identical, known, unstable, unexplained[(label, path, golden, got)], known_reasons{reason: n}) of one case."""
    import kf_known as K
    ident = known = unstable = 0
    bad = []
    reasons = {}
    gm = {}
    for lab, v in golden_steps:
        gm.setdefault(lab, []).append(v)
    seen = {}
    for lab, v in got_steps:
        i = seen.get(lab, 0)
        seen[lab] = i + 1
        if lab not in gm or i >= len(gm[lab]):
            r = K.match(case, lab, "$")
            if r:
                known += 1
                reasons[r] = reasons.get(r, 0) + 1
            else:
                bad.append((lab, "$", "<no such step in golden>", v))
            continue
        g = gm[lab][i]
        if g == {"__unstable__": True}:
            unstable += 1
            continue
        d = diff(g, v)
        if not d:
            ident += 1
            continue
        unexplained = []
        why = set()
        for path, a, b in d:
            r = K.match(case, lab, path)
            if r:
                why.add(r)
            else:
                unexplained.append((lab, path, a, b))
        if unexplained:
            bad.extend(unexplained)
        else:
            known += 1
            for r in why:
                reasons[r] = reasons.get(r, 0) + 1
    for lab, vs in gm.items():
        if seen.get(lab, 0) < len(vs):
            r = K.match(case, lab, "$")
            if r:
                known += 1
            else:
                bad.append((lab, "$", "<golden step not reached>", None))
    return ident, known, unstable, bad, reasons
