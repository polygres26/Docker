"""Differential harness of amqpwire: runs the corpus (amqp_corpus.py) over raw AMQP 0-9-1 connections, records what a REAL RabbitMQ answers into
golden.json.gz (`python3 amqp_harness.py record --port N`), and compares a run against Warp with it (`compare`, used offline by
test_amqp_conformance.py). Everything a client can observe -- reply methods, error codes and texts, delivery order, tags, redelivery flags,
content header properties, returns, confirms -- is normalized to JSON so both sides are comparable byte for byte."""
import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import amqp_raw as R  # noqa: E402

GOLDEN = os.path.join(HERE, "golden.json.gz")
CASES = []


def case(name):
    def deco(fn):
        CASES.append((name, fn))
        return fn
    return deco


class Case:
    def __init__(self, name, fn):
        self.name, self.fn = name, fn


def load_corpus():
    import amqp_corpus  # noqa: F401  (registers the cases)
    return [Case(n, f) for n, f in CASES]


def load_golden():
    with gzip.open(GOLDEN, "rt") as f:
        return json.load(f)


class Rec:
    """Records the steps of one scenario. Names made with n() carry a unique prefix that is normalized to P in the recording."""

    def __init__(self, host, port, name):
        self.host, self.port, self.name = host, port, name
        self.prefix = "t" + uuid.uuid4().hex[:8]
        self.steps = []
        self.conns = {}
        self.dead = set()
        self.names = []
        self.gen, self.ctag, self.tok = {}, {}, {}

    # ---- names and normalization
    def n(self, name):
        full = f"{self.prefix}.{name}"
        self.names.append(full)
        return full

    def norm(self, v, key=None):
        if isinstance(v, dict):
            return {self.norm(k): self.norm(x, k) for k, x in v.items()}
        if isinstance(v, list):
            return [self.norm(x, key) for x in v]
        if isinstance(v, bytes):
            return v.hex()
        if isinstance(v, str):
            v = v.replace(self.prefix, "P")
            v = re.sub(r"amq\.gen-[A-Za-z0-9_-]+", lambda m: "<gen%d>" % self.gen.setdefault(m.group(0), len(self.gen) + 1), v)
            v = re.sub(r"amq\.ctag-[A-Za-z0-9_-]+", lambda m: "<ctag%d>" % self.ctag.setdefault(m.group(0), len(self.ctag) + 1), v)
            v = re.sub(r"(amq\.rabbitmq\.reply-to\.)([A-Za-z0-9_@=./+-]+)", lambda m: m.group(1) + "<tok%d>" % self.tok.setdefault(m.group(2), len(self.tok) + 1), v)
            return v
        return v

    def _event(self, e):
        e = dict(e)
        if "body" in e and isinstance(e["body"], bytes):
            b = e["body"]
            e["body"] = self._body(b)
        e = self.norm(e)
        # the timestamp of a death is the only volatile part of x-death
        h = (e.get("props") or {}).get("headers")
        if isinstance(h, dict) and isinstance(h.get("x-death"), list):
            for d in h["x-death"]:
                if isinstance(d, dict) and "time" in d:
                    d["time"] = "<ts>"
        return e

    @staticmethod
    def _body(b):
        if len(b) > 512:
            return {"len": len(b), "sha1": hashlib.sha1(b).hexdigest()}
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return {"hex": b.hex()}

    # ---- connections
    def conn(self, cn="a", record=True, **kw):
        c, info = R.open_connection(self.host, self.port, **kw)
        self.conns[cn] = c
        c.pending = None
        c.confirm = {}
        c.confirm_next = {}
        step = {"cmd": "open " + cn, "out": []}
        st = info["start"]
        if st and "a" in st:
            a = st["a"]
            caps = (a["server_properties"] or {}).get("capabilities", {})
            step["out"].append({"m": "connection.start", "a": {"version": [a["version_major"], a["version_minor"]], "locales": a["locales"],
                                                                 "capabilities": sorted(caps)}})
        for k in ("tune", "open"):
            if info.get(k):
                step["out"].append(self._event(info[k]))
        if record:
            self.steps.append(step)
        return c

    def chan(self, cn, ch):
        self.x(cn, ch, "channel.open")

    def collect(self, c, first=5.0, quiet=0.06, expect_reply=True):
        """Reads events (method frames joined with their content) until the connection is quiet."""
        out = []
        timeout = first if expect_reply else quiet
        while True:
            if c.pending is not None:
                e = c.pending
                c.pending = None
            else:
                e = c.read_event(timeout)
            if e is None:
                break
            timeout = quiet
            if e.get("closed"):
                out.append({"closed": True})
                break
            if "m" in e and e["m"] in ("basic.deliver", "basic.return", "basic.get-ok"):
                m = e
                h = c.read_event(2)
                body = b""
                if h and h.get("header"):
                    m = dict(e)
                    m["props"] = h["props"]
                    nframes = 0
                    while len(body) < h["size"]:
                        b = c.read_event(2)
                        if not b or "body" not in b:
                            break
                        body += b["body"]
                        nframes += 1
                    m["body"] = body
                    if nframes > 1:
                        m["body_frames"] = nframes
                out.append(m)
            else:
                out.append(e)
            if e.get("m") in ("basic.ack", "basic.nack") and e["ch"] in c.confirm:
                out.pop()
                out.extend(self._expand_confirm(c, e))
            if e.get("m") == "channel.close":
                c.send(e["ch"], "channel.close-ok")
            elif e.get("m") == "connection.close-ok":
                break
            elif e.get("m") == "connection.close":
                try:
                    c.send(0, "connection.close-ok")
                except OSError:
                    pass
                self.dead.add(id(c))
                break
        return out

    @staticmethod
    def _expand_confirm(c, e):
        st = c.confirm[e["ch"]]
        tag, mult = e["a"]["delivery_tag"], e["a"]["multiple"]
        tags = sorted(t for t in st if t <= tag) if mult else ([tag] if tag in st else [])
        for t in tags:
            st.discard(t)
        return [{"ch": e["ch"], "m": e["m"], "a": {"delivery_tag": t, "multiple": False}} for t in tags]

    def x(self, cn, ch, name, quiet=0.06, reply=True, **args):
        """Sends a method and records everything the broker sends back."""
        c = self.conns[cn]
        rec = {"cmd": f"{cn}:{ch} {name}", "args": self.norm(args)}
        if name == "confirm.select":
            c.confirm.setdefault(ch, set())
            c.confirm_next[ch] = 0
        try:
            c.send(ch, name, **args)
        except OSError:
            rec["out"] = [{"closed": True}]
            self.steps.append(rec)
            return rec["out"]
        raw = self.collect(c, 5.0 if reply else 0.2, quiet, reply)
        self.last_raw = raw
        rec["out"] = [self._event(e) for e in raw]
        self.steps.append(rec)
        return rec["out"]

    def pub(self, cn, ch, exchange, rk, body=b"", mandatory=False, immediate=False, quiet=0.15, wait=False, **props):
        c = self.conns[cn]
        if isinstance(body, str):
            body = body.encode()
        rec = {"cmd": f"{cn}:{ch} basic.publish", "args": self.norm({"exchange": exchange, "rk": rk, "mandatory": mandatory, "immediate": immediate,
                                                                    "props": props, "body": self._body(body)})}
        hd = rec["args"]["props"].get("headers")
        if isinstance(hd, dict) and isinstance(hd.get("x-death"), list):
            rec["args"]["props"]["headers"] = dict(hd, **{"x-death": [dict(d, time="<ts>") if isinstance(d, dict) and "time" in d else d for d in hd["x-death"]]})
        if ch in c.confirm:
            c.confirm_next[ch] += 1
            c.confirm[ch].add(c.confirm_next[ch])
        try:
            c.publish(ch, exchange, rk, body, mandatory, immediate, **props)
        except OSError:
            rec["out"] = [{"closed": True}]
            self.steps.append(rec)
            return
        rec["out"] = [self._event(e) for e in self.collect(c, 5.0 if wait else quiet, quiet, wait)]
        self.steps.append(rec)

    def drain(self, cn, quiet=0.3, first=None):
        """Records whatever arrives on a connection without sending anything."""
        c = self.conns[cn]
        rec = {"cmd": f"{cn} drain", "out": [self._event(e) for e in self.collect(c, first if first else quiet, quiet, first is not None)]}
        self.steps.append(rec)
        return rec["out"]

    def raw(self, cn, data, quiet=0.3):
        c = self.conns[cn]
        rec = {"cmd": f"{cn} raw {len(data)} bytes", "args": self.norm({"hex": data.hex()})}
        try:
            c.send_raw(data)
        except OSError:
            pass
        rec["out"] = [self._event(e) for e in self.collect(c, 2.0, quiet, True)]
        self.steps.append(rec)
        return rec["out"]

    def getall(self, cn, ch, queue, ack=True, limit=200):
        """basic.get until empty; records ONE step with the messages (routing key, body, props, redelivered, tag) in order."""
        c = self.conns[cn]
        got = []
        for _ in range(limit):
            c.send(ch, "basic.get", queue=queue, no_ack=False)
            evs = self.collect(c, 5.0, 0.0)
            if not evs or evs[0].get("m") != "basic.get-ok":
                got.append(self._event(evs[0]) if evs else {"timeout": True})
                break
            e = evs[0]
            got.append(self._event(e))
            if ack:
                c.send(ch, "basic.ack", delivery_tag=e["a"]["delivery_tag"], multiple=False)
        self.steps.append({"cmd": f"{cn}:{ch} getall {queue}".replace(self.prefix, "P"), "out": got})
        return got

    def hb(self, cn, secs):
        """Records whether at least one heartbeat frame arrives within secs."""
        c = self.conns[cn]
        end = time.time() + secs
        seen = False
        while time.time() < end and not seen:
            e = c.read_event(max(0.05, end - time.time()))
            seen = bool(e and e.get("heartbeat"))
        self.steps.append({"cmd": f"{cn} heartbeat within {secs}s", "out": [{"heartbeat_seen": seen}]})

    def header_probe(self, data):
        """Connects, sends an arbitrary protocol header, records what the server answers before closing."""
        import socket as sk
        s = sk.create_connection((self.host, self.port), timeout=5)
        s.sendall(data)
        got = b""
        try:
            while True:
                d = s.recv(4096)
                if not d:
                    break
                got += d
                if len(got) >= 8:
                    break
        except sk.timeout:
            pass
        s.close()
        self.steps.append({"cmd": "header probe " + data.hex(), "out": [{"answer": got.hex()}]})

    def tok_raw(self, i):
        return next(k for k, v in self.tok.items() if v == i)

    def sleep(self, s):
        time.sleep(s)

    def close_all(self):
        for c in self.conns.values():
            c.close()
        # clean the broker: delete every named queue and exchange
        try:
            c, _ = R.open_connection(self.host, self.port)
            c.send(1, "channel.open")
            c.read_event(3)
            for nme in self.names:
                for meth, arg in (("queue.delete", "queue"), ("exchange.delete", "exchange")):
                    ch = 1
                    c.send(ch, meth, **{arg: nme})
                    e = c.read_event(3)
                    if e and e.get("m") == "channel.close":
                        c.send(1, "channel.close-ok")
                        c.send(1, "channel.open")
                        c.read_event(3)
            c.send(0, "connection.close", reply_code=200, reply_text="bye")
            c.read_event(2)
            c.close()
        except Exception:  # noqa: BLE001
            pass


def run_case(host, port, case):
    s = Rec(host, port, case.name)
    try:
        case.fn(s)
    except Exception as e:  # noqa: BLE001
        s.steps.append({"cmd": "EXCEPTION", "out": [{"exception": type(e).__name__ + ": " + str(e)[:200]}]})
    finally:
        s.close_all()
    return s.steps


def record(host, port, runs=2, only=None):
    cases = [c for c in load_corpus() if not only or c.name in only]
    golden = {}
    for c in cases:
        results = [run_case(host, port, c) for _ in range(runs)]
        steps = results[0]
        for i, st in enumerate(steps):
            for other in results[1:]:
                if i >= len(other) or other[i] != st:
                    st["unstable"] = True
        for other in results[1:]:
            if len(other) != len(steps):
                for st in steps:
                    st["unstable"] = True
        golden[c.name] = steps
        print(c.name, len(steps), sum(1 for s in steps if s.get("unstable")), flush=True)
    return golden


def compare(golden, actual, cases, known):
    """Returns (diffs, used_message_patterns, used_result_divergences)."""
    diffs = []
    used_p, used_r = set(), set()
    for c in cases:
        exp = golden.get(c.name)
        act = actual.get(c.name)
        if exp is None:
            continue
        for i, st in enumerate(exp):
            if st.get("unstable"):
                continue
            if i >= len(act):
                diffs.append((c.name, i, st["cmd"], st["out"], "<missing>", "missing"))
                continue
            key = (c.name, i)
            if key in known.RESULT_DIVERGENCES:
                used_r.add(key)
                continue
            e = normalize_known(st["out"], known, used_p)
            a = normalize_known(act[i]["out"], known, used_p)
            if e != a or st["cmd"] != act[i]["cmd"]:
                diffs.append((c.name, i, st["cmd"], e, a, "result"))
        if len(act) != len(exp):
            diffs.append((c.name, len(exp), "step count", len(exp), len(act), "count"))
    return diffs, used_p, used_r


def normalize_known(out, known, used):
    """Applies the documented cosmetic divergences (reply texts, server identification) to a list of events."""
    def fix(v, key=None):
        if isinstance(v, dict):
            return {k: fix(x, k) for k, x in v.items()}
        if isinstance(v, list):
            return [fix(x, key) for x in v]
        if isinstance(v, str) and key == "reply_text":
            for i, (pat, rep) in enumerate(known.MESSAGE_PATTERNS):
                if re.search(pat, v):
                    used.add(i)
                    return re.sub(pat, rep, v)
        return v
    return [fix(e) for e in out]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["record", "run"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--out", default=GOLDEN)
    a = ap.parse_args()
    if a.cmd == "record":
        g = record(a.host, a.port, a.runs, a.only)
        if a.only and os.path.exists(a.out):
            old = json.load(gzip.open(a.out, "rt"))
            old.update(g)
            g = old
        with gzip.open(a.out, "wt") as f:
            json.dump(g, f, indent=0, sort_keys=True)
    else:
        for c in load_corpus():
            if a.only and c.name not in a.only:
                continue
            print("==", c.name)
            for st in run_case(a.host, a.port, c):
                print(json.dumps(st)[:600])


if __name__ == "__main__":
    sys.modules["amqp_harness"] = sys.modules["__main__"]
    main()
