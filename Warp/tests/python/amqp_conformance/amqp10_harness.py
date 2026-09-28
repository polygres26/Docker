"""Differential harness of the AMQP 1.0 side of amqpwire: runs amqp10_corpus.py over raw AMQP 1.0 frames (amqp10_raw.py) -- SASL, open, begin, attach, flow, transfer,
disposition, detach, end, close, with the message sections of every transfer decoded -- against a REAL RabbitMQ 4 (`python3 amqp10_harness.py record --port N`,
golden10.json.gz) and against Warp (offline replay in test_amqp_conformance.py). AMQP 0-9-1 steps of a scenario (declaring queues, reading what a 1.0 publisher
sent) use amqp_raw.py on the same port, so the cross-protocol conversion is compared too."""
import argparse
import gzip
import json
import os
import re
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import amqp10_raw as R  # noqa: E402
import amqp_raw as R9  # noqa: E402

GOLDEN = os.path.join(HERE, "golden10.json.gz")
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
    import amqp10_corpus  # noqa: F401
    return [Case(n, f) for n, f in CASES]


def load_golden():
    with gzip.open(GOLDEN, "rt") as f:
        return json.load(f)


class Rec10:
    def __init__(self, host, port, name):
        self.host, self.port, self.name = host, port, name
        self.prefix = "t" + uuid.uuid4().hex[:8]
        self.steps, self.conns, self.names = [], {}, []
        self.c9 = None
        self.gen = {}
        self.tags = {}

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
            return v
        return v

    # ---- frames
    def _perf(self, f):
        if f is None:
            return None
        if f.get("closed"):
            return {"closed": True}
        if f.get("empty"):
            return {"heartbeat": True, "ch": f["ch"]}
        p = R.plain(f["perf"])
        out = {"ch": f["ch"], "p": p}
        name = p.get("@") if isinstance(p, dict) else None
        if name == "transfer":
            if "delivery-tag" in p:
                p["delivery-tag"] = "<tag>"
        if name == "flow":
            for k in ("incoming-window", "outgoing-window"):
                p.pop(k, None)
        if name == "open":
            p["container-id"] = "<id>"
            if "properties" in p:
                p["properties"] = sorted(str(k[0]) for k in p["properties"]["map"])
        if name == "begin" and False:
            pass
        if f.get("rest"):
            out["msg"] = [R.plain(x) for x in R.read_sections(f["rest"])]
        return self.norm(out)

    def drain(self, cn, quiet=0.6, first=None):
        c = self.conns[cn]
        out = []
        f = c.read_frame(first if first else quiet)
        while f:
            out.append(self._perf(f))
            if f.get("closed"):
                break
            f = c.read_frame(quiet)
        return out

    def hb(self, cn, secs):
        """Records whether an empty (heartbeat) frame arrives within secs."""
        c = self.conns[cn]
        end = time.time() + secs
        seen = False
        while time.time() < end and not seen:
            f = c.read_frame(max(0.05, end - time.time()))
            seen = bool(f and f.get("empty"))
        self.steps.append({"cmd": f"{cn} heartbeat within {secs}s", "out": [{"heartbeat_seen": seen}]})

    def conn(self, cn="a", **kw):
        c, seen = R.open10(self.host, self.port, **kw)
        self.conns[cn] = c
        step = {"cmd": "open " + cn, "out": []}
        for s in seen:
            if isinstance(s, dict) and "@" in s:
                if s["@"] == "open":
                    s["container-id"] = "<id>"
                    if "properties" in s:
                        s["properties"] = sorted(str(k[0]) for k in s["properties"]["map"])
                if s["@"] == "flow":
                    for k in ("incoming-window", "outgoing-window"):
                        s.pop(k, None)
            step["out"].append(self.norm(s) if not (isinstance(s, dict) and s.get("closed")) else s)
        self.steps.append(step)
        return c

    def x(self, cn, perf_name, body=b"", quiet=0.6, ch=0, label=None, **fields):
        c = self.conns[cn]
        rec = {"cmd": f"{cn}:{ch} {perf_name}" + (f" {label}" if label else ""), "args": self.norm(self._args(fields))}
        try:
            c.send(ch, perf_name, body=body, **fields)
        except OSError:
            rec["out"] = [{"closed": True}]
            self.steps.append(rec)
            return rec["out"]
        rec["out"] = self.drain(cn, quiet)
        self.steps.append(rec)
        return rec["out"]

    @staticmethod
    def _args(fields):
        def conv(v):
            if isinstance(v, R.Desc):
                return {"desc": hex(v.code), "v": conv(v.value)}
            if isinstance(v, (bytes, bytearray)):
                return bytes(v).hex()
            if isinstance(v, list):
                return [conv(x) for x in v]
            if isinstance(v, dict):
                return {str(k): conv(x) for k, x in v.items()}
            if isinstance(v, int):
                return int(v)
            if isinstance(v, str):
                return str(v)
            return v
        return {k: conv(v) for k, v in fields.items()}

    # ---- helpers for the common steps
    def session(self, cn="a", ch=0, window=2000):
        self.x(cn, "begin", ch=ch, next_outgoing_id=R.UInt(0), incoming_window=R.UInt(window), outgoing_window=R.UInt(window), label="session")

    def attach_recv(self, cn, name, address, handle=0, snd=0, rcv=0, credit=None, ch=0, dynamic=False, quiet=0.6):
        src = [address] if not dynamic else [None, None, None, None, True]
        out = self.x(cn, "attach", ch=ch, name=name, handle=R.UInt(handle), role=True, snd_settle_mode=R.UByte(snd), rcv_settle_mode=R.UByte(rcv),
                     source=R.Desc(0x28, src), target=R.Desc(0x29, [None]), label="receiver", quiet=quiet)
        if credit:
            self.flow(cn, handle, credit, delivery_count=0, ch=ch)
        return out

    def flow(self, cn, handle, credit, delivery_count=0, next_in=0, next_out=0, drain=False, echo=False, ch=0, quiet=0.6):
        return self.x(cn, "flow", ch=ch, next_incoming_id=R.UInt(next_in), incoming_window=R.UInt(2000), next_outgoing_id=R.UInt(next_out), outgoing_window=R.UInt(2000),
                      handle=R.UInt(handle), delivery_count=R.UInt(delivery_count), link_credit=R.UInt(credit), drain=(True if drain else None), echo=(True if echo else None),
                      label="credit", quiet=quiet)

    def attach_send(self, cn, name, address, handle=1, snd=2, rcv=0, ch=0, quiet=0.6):
        return self.x(cn, "attach", ch=ch, name=name, handle=R.UInt(handle), role=False, snd_settle_mode=R.UByte(snd), rcv_settle_mode=R.UByte(rcv),
                      source=R.Desc(0x28, [None]), target=R.Desc(0x29, [address]), initial_delivery_count=R.UInt(0), label="sender", quiet=quiet)

    def send(self, cn, handle, delivery_id, message, settled=False, tag=None, ch=0, quiet=0.6, more=None):
        return self.x(cn, "transfer", body=message, ch=ch, handle=R.UInt(handle), delivery_id=R.UInt(delivery_id), delivery_tag=R.Binary(tag or (b"t%d" % delivery_id)),
                      message_format=R.UInt(0), settled=settled, more=more, quiet=quiet, label="message")

    def disp(self, cn, first, last=None, state=None, settled=True, ch=0, quiet=0.6):
        return self.x(cn, "disposition", ch=ch, role=True, first=R.UInt(first), last=R.UInt(first if last is None else last), settled=settled, state=state, quiet=quiet)

    def detach(self, cn, handle, ch=0):
        return self.x(cn, "detach", ch=ch, handle=R.UInt(handle), closed=True)

    # ---- AMQP 0-9-1 on the same port, recorded normalized
    def c091(self):
        if self.c9 is None:
            self.c9, _ = R9.open_connection(self.host, self.port)
            self.c9.send(1, "channel.open")
            self.c9.read_event(5)
        return self.c9

    def v091(self, method, quiet=0.15, **args):
        c = self.c091()
        c.send(1, method, **args)
        out = []
        e = c.read_event(5)
        while e:
            if e.get("m") in ("basic.get-ok", "basic.deliver", "basic.return"):
                h = c.read_event(2)
                body = b""
                if h and h.get("header"):
                    e = dict(e)
                    e["props"] = h["props"]
                    while len(body) < h["size"]:
                        b = c.read_event(2)
                        if not b or "body" not in b:
                            break
                        body += b["body"]
                    e["body"] = body.decode("utf-8", "replace") if len(body) < 200 else {"len": len(body)}
            out.append(e)
            if e.get("m") == "channel.close":
                c.send(e["ch"], "channel.close-ok")
                c.send(1, "channel.open")
                c.read_event(3)
            e = c.read_event(quiet)
        self.steps.append({"cmd": "0-9-1 " + method, "args": self.norm(args), "out": self.norm(out)})
        return out

    def q091(self, name, durable=True, **arguments):
        return self.v091("queue.declare", queue=name, durable=durable, arguments=arguments)

    def pub091(self, exchange, rk, body=b"", **props):
        c = self.c091()
        c.publish(1, exchange, rk, body, **props)
        time.sleep(0.15)
        self.steps.append({"cmd": "0-9-1 publish", "args": self.norm({"exchange": exchange, "rk": rk, "props": props}), "out": []})

    def sleep(self, s):
        time.sleep(s)

    def close_all(self):
        for c in self.conns.values():
            c.close()
        try:
            c = self.c091()
            for nme in self.names:
                for meth, arg in (("queue.delete", "queue"), ("exchange.delete", "exchange")):
                    c.send(1, meth, **{arg: nme})
                    e = c.read_event(3)
                    if e and e.get("m") == "channel.close":
                        c.send(1, "channel.close-ok")
                        c.send(1, "channel.open")
                        c.read_event(3)
            c.close()
        except Exception:  # noqa: BLE001
            pass
        if self.c9:
            self.c9.close()


def run_case(host, port, case):
    s = Rec10(host, port, case.name)
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
        golden[c.name] = steps
        print(c.name, len(steps), sum(1 for s in steps if s.get("unstable")), flush=True)
    return golden


def compare(golden, actual, cases, known):
    diffs, used_r = [], set()
    for c in cases:
        exp, act = golden.get(c.name), actual.get(c.name)
        if exp is None:
            continue
        for i, st in enumerate(exp):
            if st.get("unstable"):
                continue
            if i >= len(act):
                diffs.append((c.name, i, st["cmd"], st["out"], "<missing>", "missing"))
                continue
            key = (c.name, i)
            if key in known.RESULT_DIVERGENCES10:
                used_r.add(key)
                continue
            if st["out"] != act[i]["out"] or st["cmd"] != act[i]["cmd"]:
                diffs.append((c.name, i, st["cmd"], st["out"], act[i]["out"], "result"))
        if len(act) != len(exp):
            diffs.append((c.name, len(exp), "step count", len(exp), len(act), "count"))
    return diffs, used_r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["record", "run", "diff"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--out", default=GOLDEN)
    ap.add_argument("--golden", default=GOLDEN)
    a = ap.parse_args()
    if a.cmd == "record":
        g = record(a.host, a.port, a.runs, a.only)
        if a.only and os.path.exists(a.out):
            old = json.load(gzip.open(a.out, "rt"))
            old.update(g)
            g = old
        with gzip.open(a.out, "wt") as f:
            json.dump(g, f, indent=0, sort_keys=True)
    elif a.cmd == "run":
        for c in load_corpus():
            if a.only and c.name not in a.only:
                continue
            print("==", c.name)
            for st in run_case(a.host, a.port, c):
                print(json.dumps(st)[:900])
    else:
        import amqp_known as K
        golden = json.load(gzip.open(a.golden, "rt"))
        cases = [c for c in load_corpus() if not a.only or c.name in a.only]
        actual = {c.name: run_case(a.host, a.port, c) for c in cases}
        diffs, ur = compare(golden, actual, cases, K)
        for c, i, s, e, act, k in diffs:
            print(f"{c}[{i}] ({k}) {s[:150]}\n   expected {json.dumps(e)[:1200]}\n   actual   {json.dumps(act)[:1200]}")
        print(len(diffs), "differences in", sum(len(actual[c.name]) for c in cases), "steps")


if __name__ == "__main__":
    sys.modules["amqp10_harness"] = sys.modules["__main__"]
    main()
