"""A raw AMQP 1.0 client: SASL and AMQP frames on a socket, the type system both ways, performatives decoded to plain dicts. Used by the AMQP 1.0
differential corpus (real RabbitMQ 4 vs Warp) so attach refusals, dispositions, flow and error conditions are compared frame by frame."""
import socket
import struct
import time
import uuid

DESC = {0x10: "open", 0x11: "begin", 0x12: "attach", 0x13: "flow", 0x14: "transfer", 0x15: "disposition", 0x16: "detach", 0x17: "end", 0x18: "close",
        0x1d: "error", 0x28: "source", 0x29: "target", 0x23: "received", 0x24: "accepted", 0x25: "rejected", 0x26: "released", 0x27: "modified",
        0x40: "sasl-mechanisms", 0x41: "sasl-init", 0x42: "sasl-challenge", 0x43: "sasl-response", 0x44: "sasl-outcome",
        0x70: "header", 0x71: "delivery-annotations", 0x72: "message-annotations", 0x73: "properties", 0x74: "application-properties", 0x75: "data",
        0x76: "amqp-sequence", 0x77: "amqp-value", 0x78: "footer"}
CODE = {v: k for k, v in DESC.items()}
FIELDS = {
    "open": ["container-id", "hostname", "max-frame-size", "channel-max", "idle-time-out", "outgoing-locales", "incoming-locales", "offered-capabilities",
             "desired-capabilities", "properties"],
    "begin": ["remote-channel", "next-outgoing-id", "incoming-window", "outgoing-window", "handle-max", "offered-capabilities", "desired-capabilities", "properties"],
    "attach": ["name", "handle", "role", "snd-settle-mode", "rcv-settle-mode", "source", "target", "unsettled", "incomplete-unsettled", "initial-delivery-count",
               "max-message-size", "offered-capabilities", "desired-capabilities", "properties"],
    "flow": ["next-incoming-id", "incoming-window", "next-outgoing-id", "outgoing-window", "handle", "delivery-count", "link-credit", "available", "drain", "echo",
             "properties"],
    "transfer": ["handle", "delivery-id", "delivery-tag", "message-format", "settled", "more", "rcv-settle-mode", "state", "resume", "aborted", "batchable"],
    "disposition": ["role", "first", "last", "settled", "state", "batchable"],
    "detach": ["handle", "closed", "error"], "end": ["error"], "close": ["error"], "error": ["condition", "description", "info"],
    "source": ["address", "durable", "expiry-policy", "timeout", "dynamic", "dynamic-node-properties", "distribution-mode", "filter", "default-outcome", "outcomes",
               "capabilities"],
    "target": ["address", "durable", "expiry-policy", "timeout", "dynamic", "dynamic-node-properties", "capabilities"],
    "rejected": ["error"], "modified": ["delivery-failed", "undeliverable-here", "message-annotations"], "received": ["section-number", "section-offset"],
    "sasl-mechanisms": ["sasl-server-mechanisms"], "sasl-init": ["mechanism", "initial-response", "hostname"], "sasl-outcome": ["code", "additional-data"],
    "header": ["durable", "priority", "ttl", "first-acquirer", "delivery-count"],
    "properties": ["message-id", "user-id", "to", "subject", "reply-to", "correlation-id", "content-type", "content-encoding", "absolute-expiry-time", "creation-time",
                   "group-id", "group-sequence", "reply-to-group-id"],
}


class Sym(str):
    pass


class UByte(int):
    pass


class UShort(int):
    pass


class UInt(int):
    pass


class ULong(int):
    pass


class Ts(int):
    pass


class Desc:
    def __init__(self, code, value):
        self.code, self.value = code, value


class Binary(bytes):
    pass


def enc(v):
    if v is None:
        return b"\x40"
    if isinstance(v, bool):
        return b"\x41" if v else b"\x42"
    if isinstance(v, UByte):
        return b"\x50" + bytes([v])
    if isinstance(v, UShort):
        return b"\x60" + struct.pack(">H", v)
    if isinstance(v, UInt):
        return b"\x43" if v == 0 else (b"\x52" + bytes([v]) if v < 256 else b"\x70" + struct.pack(">I", v))
    if isinstance(v, ULong):
        return b"\x44" if v == 0 else (b"\x53" + bytes([v]) if v < 256 else b"\x80" + struct.pack(">Q", v))
    if isinstance(v, Ts):
        return b"\x83" + struct.pack(">q", v)
    if isinstance(v, int):
        return b"\x54" + struct.pack(">b", v) if -128 <= v <= 127 else b"\x81" + struct.pack(">q", v)
    if isinstance(v, float):
        return b"\x82" + struct.pack(">d", v)
    if isinstance(v, Sym):
        b = v.encode()
        return (b"\xa3" + bytes([len(b)]) if len(b) < 256 else b"\xb3" + struct.pack(">I", len(b))) + b
    if isinstance(v, str):
        b = v.encode()
        return (b"\xa1" + bytes([len(b)]) if len(b) < 256 else b"\xb1" + struct.pack(">I", len(b))) + b
    if isinstance(v, (bytes, bytearray)):
        return (b"\xa0" + bytes([len(v)]) if len(v) < 256 else b"\xb0" + struct.pack(">I", len(v))) + bytes(v)
    if isinstance(v, uuid.UUID):
        return b"\x98" + v.bytes
    if isinstance(v, Desc):
        return b"\x00" + enc(ULong(v.code)) + enc(v.value)
    if isinstance(v, list):
        if not v:
            return b"\x45"
        body = b"".join(enc(x) for x in v)
        if len(body) + 1 < 256 and len(v) < 256:
            return b"\xc0" + bytes([len(body) + 1, len(v)]) + body
        return b"\xd0" + struct.pack(">II", len(body) + 4, len(v)) + body
    if isinstance(v, dict):
        body = b"".join(enc(k) + enc(x) for k, x in v.items())
        n = len(v) * 2
        if len(body) + 1 < 256 and n < 256:
            return b"\xc1" + bytes([len(body) + 1, n]) + body
        return b"\xd1" + struct.pack(">II", len(body) + 4, n) + body
    raise ValueError(repr(v))


def perf(_p, **fields):
    """Encodes a performative from its named fields (trailing nulls trimmed)."""
    name = _p
    names = FIELDS[name]
    vals = [fields.get(n.replace("-", "_")) for n in names]
    while vals and vals[-1] is None:
        vals.pop()
    return enc(Desc(CODE[name], vals))


class Rd:
    def __init__(self, b, p=0):
        self.b, self.p = b, p

    def take(self, n):
        if self.p + n > len(self.b):
            raise ValueError("short")
        r = self.b[self.p:self.p + n]
        self.p += n
        return r

    def value(self):
        c = self.take(1)[0]
        if c == 0x00:
            d = self.value()
            v = self.value()
            return Desc(d if isinstance(d, int) else d, v)
        return self.typed(c)

    def typed(self, c):
        t = self.take
        if c == 0x40:
            return None
        if c == 0x41:
            return True
        if c == 0x42:
            return False
        if c == 0x56:
            return t(1)[0] != 0
        if c in (0x43, 0x44):
            return 0
        if c in (0x50, 0x52, 0x53):
            return t(1)[0]
        if c == 0x60:
            return struct.unpack(">H", t(2))[0]
        if c == 0x70:
            return struct.unpack(">I", t(4))[0]
        if c == 0x80:
            return struct.unpack(">Q", t(8))[0]
        if c in (0x51, 0x54, 0x55):
            return struct.unpack(">b", t(1))[0]
        if c == 0x61:
            return struct.unpack(">h", t(2))[0]
        if c == 0x71:
            return struct.unpack(">i", t(4))[0]
        if c == 0x81:
            return struct.unpack(">q", t(8))[0]
        if c == 0x72:
            return struct.unpack(">f", t(4))[0]
        if c == 0x82:
            return struct.unpack(">d", t(8))[0]
        if c == 0x83:
            return {"timestamp": struct.unpack(">q", t(8))[0]}
        if c == 0x98:
            return {"uuid": str(uuid.UUID(bytes=t(16)))}
        if c == 0xa0:
            return {"binary": t(t(1)[0]).hex()}
        if c == 0xb0:
            return {"binary": t(struct.unpack(">I", t(4))[0]).hex()}
        if c == 0xa1:
            return t(t(1)[0]).decode()
        if c == 0xb1:
            return t(struct.unpack(">I", t(4))[0]).decode()
        if c == 0xa3:
            return {"symbol": t(t(1)[0]).decode()}
        if c == 0xb3:
            return {"symbol": t(struct.unpack(">I", t(4))[0]).decode()}
        if c == 0x45:
            return []
        if c in (0xc0, 0xd0, 0xc1, 0xd1, 0xe0, 0xf0):
            small = c in (0xc0, 0xc1, 0xe0)
            size = t(1)[0] if small else struct.unpack(">I", t(4))[0]
            body = Rd(t(size))
            count = body.take(1)[0] if small else struct.unpack(">I", body.take(4))[0]
            if c in (0xc0, 0xd0):
                return [body.value() for _ in range(count)]
            if c in (0xc1, 0xd1):
                out = []
                for _ in range(count // 2):
                    k = body.value()
                    out.append([k, body.value()])
                return {"map": out}
            elem = body.take(1)[0]
            return [Desc(body.value(), body.typed(body.take(1)[0])) if elem == 0 else body.typed(elem) for _ in range(count)]
        raise ValueError(f"format 0x{c:x}")


def plain(v):
    """Desc -> {'perf': name, fields...} for JSON friendly comparison."""
    if isinstance(v, Desc):
        name = DESC.get(v.code, hex(v.code) if isinstance(v.code, int) else str(v.code))
        if isinstance(v.value, list) and name in FIELDS:
            out = {"@": name}
            for n, x in zip(FIELDS[name], v.value):
                if x is not None:
                    out[n] = plain(x)
            return out
        return {"@": name, "value": plain(v.value)}
    if isinstance(v, list):
        return [plain(x) for x in v]
    if isinstance(v, dict) and "map" in v:
        return {"map": [[plain(k), plain(x)] for k, x in v["map"]]}
    return v


class Closed(Exception):
    pass


class Raw10:
    def __init__(self, host, port, timeout=10):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.eof = False

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass

    def send_raw(self, b):
        self.s.sendall(b)

    def frame(self, ftype, channel, payload):
        self.s.sendall(struct.pack(">IBBH", 8 + len(payload), 2, ftype, channel) + payload)

    def send(self, channel, _p, body=b"", **fields):
        self.frame(0, channel, perf(_p, **fields) + body)

    def sasl(self, _p, **fields):
        self.frame(1, 0, perf(_p, **fields))

    def _fill(self, n, timeout):
        end = time.time() + timeout
        while len(self.buf) < n:
            left = end - time.time()
            if left <= 0:
                return False
            self.s.settimeout(left)
            try:
                d = self.s.recv(65536)
            except socket.timeout:
                return False
            except OSError:
                d = b""
            if not d:
                self.eof = True
                return False
            self.buf += d
        return True

    def read_header(self, timeout=5):
        if not self._fill(8, timeout):
            return None
        h, self.buf = self.buf[:8], self.buf[8:]
        return h

    def read_frame(self, timeout=1.0):
        """(type, channel, decoded performative, trailing bytes) or None on timeout, {'closed': True} at EOF."""
        if not self._fill(8, timeout):
            return {"closed": True} if self.eof and not self.buf else None
        size, doff, ftype, ch = struct.unpack(">IBBH", self.buf[:8])
        if not self._fill(size, timeout):
            return {"closed": True} if self.eof else None
        payload = self.buf[doff * 4:size]
        self.buf = self.buf[size:]
        if not payload:
            return {"empty": True, "ch": ch}
        r = Rd(payload)
        d = r.value()
        return {"type": ftype, "ch": ch, "perf": d, "rest": payload[r.p:]}


def read_sections(rest):
    out = []
    r = Rd(rest)
    while r.p < len(rest):
        out.append(r.value())
    return out


def message(body=None, data=None, header=None, properties=None, app=None, ann=None):
    """Encodes a message: sections as given (dicts of field values)."""
    out = b""
    if header is not None:
        out += enc(Desc(0x70, [header.get(n.replace("-", "_")) for n in FIELDS["header"]]))
    if ann:
        out += enc(Desc(0x72, ann))
    if properties is not None:
        vals = [properties.get(n.replace("-", "_")) for n in FIELDS["properties"]]
        while vals and vals[-1] is None:
            vals.pop()
        out += enc(Desc(0x73, vals))
    if app:
        out += enc(Desc(0x74, app))
    if data is not None:
        out += enc(Desc(0x75, Binary(data)))
    elif body is not None:
        out += enc(Desc(0x77, body))
    return out


def open10(host, port, user="guest", password="guest", mech="PLAIN", sasl=True, max_frame=65536, idle=None, begin=True, incoming_window=2000):
    """SASL (PLAIN by default), AMQP header, open, and (by default) begin on channel 0. Returns (conn, frames seen)."""
    c = Raw10(host, port)
    seen = []
    if sasl:
        c.send_raw(b"AMQP\x03\x01\x00\x00")
        seen.append({"header": c.read_header().hex()})
        f = c.read_frame(5)
        seen.append(plain(f["perf"]) if f and "perf" in f else f)
        if mech == "PLAIN":
            c.sasl("sasl-init", mechanism=Sym("PLAIN"), initial_response=Binary(f"\0{user}\0{password}".encode()))
        elif mech == "ANONYMOUS":
            c.sasl("sasl-init", mechanism=Sym("ANONYMOUS"), initial_response=Binary(b"anon"))
        else:
            c.sasl("sasl-init", mechanism=Sym(mech), initial_response=Binary(b"x"))
        f = c.read_frame(5)
        seen.append(plain(f["perf"]) if f and "perf" in f else f)
        if not (f and "perf" in f and plain(f["perf"]).get("code") == 0):
            return c, seen
    c.send_raw(b"AMQP\x00\x01\x00\x00")
    h = c.read_header()
    seen.append({"header": h.hex() if h else None})
    c.send(0, "open", container_id="raw10", hostname="localhost", max_frame_size=UInt(max_frame), channel_max=UShort(100), idle_time_out=(UInt(idle) if idle else None))
    f = c.read_frame(5)
    seen.append(plain(f["perf"]) if f and "perf" in f else f)
    if begin:
        c.send(0, "begin", next_outgoing_id=UInt(0), incoming_window=UInt(incoming_window), outgoing_window=UInt(incoming_window))
        f = c.read_frame(5)
        seen.append(plain(f["perf"]) if f and "perf" in f else f)
    return c, seen


def attach_receiver(c, name, address, handle=0, credit=10, snd_settle=0, rcv_settle=0, timeout=3, channel=0, dynamic=False, filt=None):
    src = [address, None, None, None, dynamic or None, None, None, filt] if (dynamic or filt) else [address]
    c.send(channel, "attach", name=name, handle=UInt(handle), role=True, snd_settle_mode=UByte(snd_settle), rcv_settle_mode=UByte(rcv_settle),
           source=Desc(0x28, src), target=Desc(0x29, [None]))
    out = []
    f = c.read_frame(timeout)
    while f:
        out.append(f)
        if f.get("perf") is not None and DESC.get(f["perf"].code) in ("attach", "detach"):
            break
        f = c.read_frame(0.3)
    if credit and out and DESC.get(out[0]["perf"].code) == "attach":
        c.send(channel, "flow", next_incoming_id=UInt(0), incoming_window=UInt(2000), next_outgoing_id=UInt(0), outgoing_window=UInt(2000), handle=UInt(handle),
               delivery_count=UInt(0), link_credit=UInt(credit))
    return out


def attach_sender(c, name, address, handle=1, snd_settle=2, rcv_settle=0, timeout=3, channel=0):
    c.send(channel, "attach", name=name, handle=UInt(handle), role=False, snd_settle_mode=UByte(snd_settle), rcv_settle_mode=UByte(rcv_settle),
           source=Desc(0x28, [None]), target=Desc(0x29, [address]), initial_delivery_count=UInt(0))
    out = []
    f = c.read_frame(timeout)
    while f:
        out.append(f)
        if f.get("perf") is not None and DESC.get(f["perf"].code) in ("detach",):
            break
        if len(out) >= 2 and DESC.get(out[-1]["perf"].code) == "flow":
            break
        f = c.read_frame(0.4)
    return out
