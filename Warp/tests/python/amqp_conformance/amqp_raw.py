"""A raw AMQP 0-9-1 client: frames on a socket, method table both ways, decoded replies. Used by the differential corpus (real RabbitMQ vs
Warp) so reply codes, reply texts, frame order and error frames are compared byte for byte, not through a client library's view."""
import socket
import struct
import time

# name -> (class, method, [(arg name, type)]); types: octet short long longlong shortstr longstr table bits:a,b,c
SPEC = {
    "connection.start": (10, 10, [("version_major", "octet"), ("version_minor", "octet"), ("server_properties", "table"), ("mechanisms", "longstr"), ("locales", "longstr")]),
    "connection.start-ok": (10, 11, [("client_properties", "table"), ("mechanism", "shortstr"), ("response", "longstr"), ("locale", "shortstr")]),
    "connection.secure": (10, 20, [("challenge", "longstr")]),
    "connection.tune": (10, 30, [("channel_max", "short"), ("frame_max", "long"), ("heartbeat", "short")]),
    "connection.tune-ok": (10, 31, [("channel_max", "short"), ("frame_max", "long"), ("heartbeat", "short")]),
    "connection.open": (10, 40, [("virtual_host", "shortstr"), ("reserved", "shortstr"), ("insist", "bits:insist")]),
    "connection.open-ok": (10, 41, [("reserved", "shortstr")]),
    "connection.close": (10, 50, [("reply_code", "short"), ("reply_text", "shortstr"), ("class_id", "short"), ("method_id", "short")]),
    "connection.close-ok": (10, 51, []),
    "connection.blocked": (10, 60, [("reason", "shortstr")]),
    "connection.unblocked": (10, 61, []),
    "channel.open": (20, 10, [("reserved", "shortstr")]),
    "channel.open-ok": (20, 11, [("reserved", "longstr")]),
    "channel.flow": (20, 20, [("active", "bits:active")]),
    "channel.flow-ok": (20, 21, [("active", "bits:active")]),
    "channel.close": (20, 40, [("reply_code", "short"), ("reply_text", "shortstr"), ("class_id", "short"), ("method_id", "short")]),
    "channel.close-ok": (20, 41, []),
    "exchange.declare": (40, 10, [("reserved", "short"), ("exchange", "shortstr"), ("type", "shortstr"), ("flags", "bits:passive,durable,auto_delete,internal,no_wait"), ("arguments", "table")]),
    "exchange.declare-ok": (40, 11, []),
    "exchange.delete": (40, 20, [("reserved", "short"), ("exchange", "shortstr"), ("flags", "bits:if_unused,no_wait")]),
    "exchange.delete-ok": (40, 21, []),
    "exchange.bind": (40, 30, [("reserved", "short"), ("destination", "shortstr"), ("source", "shortstr"), ("routing_key", "shortstr"), ("flags", "bits:no_wait"), ("arguments", "table")]),
    "exchange.bind-ok": (40, 31, []),
    "exchange.unbind": (40, 40, [("reserved", "short"), ("destination", "shortstr"), ("source", "shortstr"), ("routing_key", "shortstr"), ("flags", "bits:no_wait"), ("arguments", "table")]),
    "exchange.unbind-ok": (40, 51, []),
    "queue.declare": (50, 10, [("reserved", "short"), ("queue", "shortstr"), ("flags", "bits:passive,durable,exclusive,auto_delete,no_wait"), ("arguments", "table")]),
    "queue.declare-ok": (50, 11, [("queue", "shortstr"), ("message_count", "long"), ("consumer_count", "long")]),
    "queue.bind": (50, 20, [("reserved", "short"), ("queue", "shortstr"), ("exchange", "shortstr"), ("routing_key", "shortstr"), ("flags", "bits:no_wait"), ("arguments", "table")]),
    "queue.bind-ok": (50, 21, []),
    "queue.purge": (50, 30, [("reserved", "short"), ("queue", "shortstr"), ("flags", "bits:no_wait")]),
    "queue.purge-ok": (50, 31, [("message_count", "long")]),
    "queue.delete": (50, 40, [("reserved", "short"), ("queue", "shortstr"), ("flags", "bits:if_unused,if_empty,no_wait")]),
    "queue.delete-ok": (50, 41, [("message_count", "long")]),
    "queue.unbind": (50, 50, [("reserved", "short"), ("queue", "shortstr"), ("exchange", "shortstr"), ("routing_key", "shortstr"), ("arguments", "table")]),
    "queue.unbind-ok": (50, 51, []),
    "basic.qos": (60, 10, [("prefetch_size", "long"), ("prefetch_count", "short"), ("flags", "bits:global")]),
    "basic.qos-ok": (60, 11, []),
    "basic.consume": (60, 20, [("reserved", "short"), ("queue", "shortstr"), ("consumer_tag", "shortstr"), ("flags", "bits:no_local,no_ack,exclusive,no_wait"), ("arguments", "table")]),
    "basic.consume-ok": (60, 21, [("consumer_tag", "shortstr")]),
    "basic.cancel": (60, 30, [("consumer_tag", "shortstr"), ("flags", "bits:no_wait")]),
    "basic.cancel-ok": (60, 31, [("consumer_tag", "shortstr")]),
    "basic.publish": (60, 40, [("reserved", "short"), ("exchange", "shortstr"), ("routing_key", "shortstr"), ("flags", "bits:mandatory,immediate")]),
    "basic.return": (60, 50, [("reply_code", "short"), ("reply_text", "shortstr"), ("exchange", "shortstr"), ("routing_key", "shortstr")]),
    "basic.deliver": (60, 60, [("consumer_tag", "shortstr"), ("delivery_tag", "longlong"), ("flags", "bits:redelivered"), ("exchange", "shortstr"), ("routing_key", "shortstr")]),
    "basic.get": (60, 70, [("reserved", "short"), ("queue", "shortstr"), ("flags", "bits:no_ack")]),
    "basic.get-ok": (60, 71, [("delivery_tag", "longlong"), ("flags", "bits:redelivered"), ("exchange", "shortstr"), ("routing_key", "shortstr"), ("message_count", "long")]),
    "basic.get-empty": (60, 72, [("reserved", "shortstr")]),
    "basic.ack": (60, 80, [("delivery_tag", "longlong"), ("flags", "bits:multiple")]),
    "basic.reject": (60, 90, [("delivery_tag", "longlong"), ("flags", "bits:requeue")]),
    "basic.recover-async": (60, 100, [("flags", "bits:requeue")]),
    "basic.recover": (60, 110, [("flags", "bits:requeue")]),
    "basic.recover-ok": (60, 111, []),
    "basic.nack": (60, 120, [("delivery_tag", "longlong"), ("flags", "bits:multiple,requeue")]),
    "confirm.select": (85, 10, [("flags", "bits:no_wait")]),
    "confirm.select-ok": (85, 11, []),
    "tx.select": (90, 10, []),
    "tx.select-ok": (90, 11, []),
    "tx.commit": (90, 20, []),
    "tx.commit-ok": (90, 21, []),
    "tx.rollback": (90, 30, []),
    "tx.rollback-ok": (90, 31, []),
}
BY_ID = {(c, m): (n, a) for n, (c, m, a) in SPEC.items()}
PROPS = [("content_type", "shortstr"), ("content_encoding", "shortstr"), ("headers", "table"), ("delivery_mode", "octet"), ("priority", "octet"),
         ("correlation_id", "shortstr"), ("reply_to", "shortstr"), ("expiration", "shortstr"), ("message_id", "shortstr"), ("timestamp", "longlong"),
         ("type", "shortstr"), ("user_id", "shortstr"), ("app_id", "shortstr"), ("cluster_id", "shortstr")]


class Ts(int):
    """A field-table timestamp."""


class Dec(tuple):
    """A field-table decimal (scale, value)."""


class I8(int):
    pass


class I16(int):
    pass


class I64(int):
    pass


class U(str):
    """long string that must stay bytes-ish: unused, kept for symmetry."""


def enc_value(v):
    if v is None:
        return b"V"
    if isinstance(v, bool):
        return b"t" + bytes([1 if v else 0])
    if isinstance(v, I8):
        return b"b" + struct.pack(">b", v)
    if isinstance(v, I16):
        return b"s" + struct.pack(">h", v)
    if isinstance(v, I64):
        return b"l" + struct.pack(">q", v)
    if isinstance(v, Ts):
        return b"T" + struct.pack(">Q", v)
    if isinstance(v, int):
        return b"I" + struct.pack(">i", v)
    if isinstance(v, float):
        return b"d" + struct.pack(">d", v)
    if isinstance(v, Dec):
        return b"D" + bytes([v[0]]) + struct.pack(">i", v[1])
    if isinstance(v, str):
        b = v.encode()
        return b"S" + struct.pack(">I", len(b)) + b
    if isinstance(v, bytes):
        return b"x" + struct.pack(">I", len(v)) + v
    if isinstance(v, list):
        body = b"".join(enc_value(x) for x in v)
        return b"A" + struct.pack(">I", len(body)) + body
    if isinstance(v, dict):
        return b"F" + enc_table(v)
    raise ValueError(v)


def enc_table(t):
    body = b""
    for k, v in (t or {}).items():
        kb = k.encode()
        body += bytes([len(kb)]) + kb + enc_value(v)
    return struct.pack(">I", len(body)) + body


class Rd:
    def __init__(self, b, p=0):
        self.b, self.p = b, p

    def take(self, n):
        if self.p + n > len(self.b):
            raise ValueError("short")
        r = self.b[self.p:self.p + n]
        self.p += n
        return r

    def u8(self):
        return self.take(1)[0]

    def u16(self):
        return struct.unpack(">H", self.take(2))[0]

    def u32(self):
        return struct.unpack(">I", self.take(4))[0]

    def u64(self):
        return struct.unpack(">Q", self.take(8))[0]

    def shortstr(self):
        return self.take(self.u8()).decode("utf-8", "replace")

    def longstr(self):
        return self.take(self.u32()).decode("utf-8", "replace")

    def table(self):
        n = self.u32()
        end = self.p + n
        t = {}
        while self.p < end:
            k = self.shortstr()
            t[k] = self.value()
        return t

    def value(self):
        c = chr(self.u8())
        if c == "t":
            return self.u8() != 0
        if c == "b":
            return struct.unpack(">b", self.take(1))[0]
        if c == "B":
            return self.u8()
        if c in "sU":
            return struct.unpack(">h", self.take(2))[0]
        if c == "u":
            return self.u16()
        if c == "I":
            return struct.unpack(">i", self.take(4))[0]
        if c == "i":
            return self.u32()
        if c in "lL":
            return struct.unpack(">q", self.take(8))[0]
        if c == "f":
            return struct.unpack(">f", self.take(4))[0]
        if c == "d":
            return struct.unpack(">d", self.take(8))[0]
        if c == "D":
            return {"decimal": [self.u8(), struct.unpack(">i", self.take(4))[0]]}
        if c == "S":
            return self.longstr()
        if c == "x":
            return {"bytes": self.take(self.u32()).hex()}
        if c == "A":
            n = self.u32()
            end = self.p + n
            out = []
            while self.p < end:
                out.append(self.value())
            return out
        if c == "T":
            return {"ts": self.u64()}
        if c == "F":
            return self.table()
        if c == "V":
            return None
        raise ValueError("field type " + c)


def enc_args(spec, args):
    out = b""
    bits = []
    names = []

    def flush():
        nonlocal bits, names
        if names:
            v = 0
            for i, n in enumerate(names):
                if args.get(n):
                    v |= 1 << i
            out_bits.append(bytes([v]))
            names = []

    out_parts = []
    out_bits = out_parts
    pending = []
    for name, typ in spec:
        if typ.startswith("bits:"):
            v = 0
            for i, n in enumerate(typ[5:].split(",")):
                if args.get(n):
                    v |= 1 << i
            out_parts.append(bytes([v]))
            continue
        val = args.get(name)
        if typ == "octet":
            out_parts.append(bytes([val or 0]))
        elif typ == "short":
            out_parts.append(struct.pack(">H", val or 0))
        elif typ == "long":
            out_parts.append(struct.pack(">I", val or 0))
        elif typ == "longlong":
            out_parts.append(struct.pack(">Q", val or 0))
        elif typ == "shortstr":
            b = (val or "").encode()
            out_parts.append(bytes([len(b)]) + b)
        elif typ == "longstr":
            b = val if isinstance(val, bytes) else (val or "").encode()
            out_parts.append(struct.pack(">I", len(b)) + b)
        elif typ == "table":
            out_parts.append(enc_table(val))
    return b"".join(out_parts)


def dec_args(spec, payload, p):
    r = Rd(payload, p)
    out = {}
    for name, typ in spec:
        if typ.startswith("bits:"):
            v = r.u8()
            for i, n in enumerate(typ[5:].split(",")):
                out[n] = bool(v & (1 << i))
        elif typ == "octet":
            out[name] = r.u8()
        elif typ == "short":
            out[name] = r.u16()
        elif typ == "long":
            out[name] = r.u32()
        elif typ == "longlong":
            out[name] = r.u64()
        elif typ == "shortstr":
            out[name] = r.shortstr()
        elif typ == "longstr":
            out[name] = r.longstr()
        elif typ == "table":
            out[name] = r.table()
    return out


def enc_props(props):
    flags = 0
    body = b""
    for i, (name, typ) in enumerate(PROPS):
        if props.get(name) is not None:
            flags |= 1 << (15 - i)
            v = props[name]
            if typ == "shortstr":
                b = v.encode()
                body += bytes([len(b)]) + b
            elif typ == "table":
                body += enc_table(v)
            elif typ == "octet":
                body += bytes([v])
            elif typ == "longlong":
                body += struct.pack(">Q", v)
    return struct.pack(">H", flags) + body


def dec_props(b):
    r = Rd(b)
    flags = r.u16()
    out = {}
    for i, (name, typ) in enumerate(PROPS):
        if flags & (1 << (15 - i)):
            out[name] = {"shortstr": r.shortstr, "table": r.table, "octet": r.u8, "longlong": r.u64}[typ]()
    return out


class Closed(Exception):
    pass


class RawConn:
    def __init__(self, host, port, timeout=10):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.eof = False
        self.frame_max = 131072

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass

    def send_raw(self, b):
        self.s.sendall(b)

    def frame(self, typ, ch, payload):
        self.s.sendall(struct.pack(">BHI", typ, ch, len(payload)) + payload + b"\xce")

    def send(self, ch, name, **args):
        c, m, spec = SPEC[name]
        self.frame(1, ch, struct.pack(">HH", c, m) + enc_args(spec, args))

    def send_content(self, ch, props, body, class_id=60):
        self.frame(2, ch, struct.pack(">HHQ", class_id, 0, len(body)) + enc_props(props))
        step = self.frame_max - 8
        for i in range(0, len(body), step):
            self.frame(3, ch, body[i:i + step])

    def publish(self, ch, exchange, rk, body=b"", mandatory=False, immediate=False, **props):
        self.send(ch, "basic.publish", exchange=exchange, routing_key=rk, mandatory=mandatory, immediate=immediate)
        self.send_content(ch, props, body)

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

    def read_frame(self, timeout=1.0):
        """(type, channel, payload) or None on timeout; raises Closed at EOF with nothing buffered."""
        if not self._fill(7, timeout):
            if self.eof and not self.buf:
                raise Closed()
            return None
        typ, ch, size = struct.unpack(">BHI", self.buf[:7])
        if not self._fill(7 + size + 1, timeout):
            if self.eof:
                raise Closed()
            return None
        payload = self.buf[7:7 + size]
        end = self.buf[7 + size]
        self.buf = self.buf[8 + size:]
        return typ, ch, payload, end

    def read_event(self, timeout=1.0):
        """A decoded event: method (with args) / header / body / heartbeat, or None on timeout. 'closed' at EOF."""
        try:
            f = self.read_frame(timeout)
        except Closed:
            return {"closed": True}
        if f is None:
            return None
        typ, ch, payload, end = f
        if end != 0xCE:
            return {"bad_frame_end": end}
        if typ == 1:
            c, m = struct.unpack(">HH", payload[:4])
            if (c, m) in BY_ID:
                name, spec = BY_ID[(c, m)]
                return {"ch": ch, "m": name, "a": dec_args(spec, payload, 4)}
            return {"ch": ch, "m": f"unknown.{c}.{m}"}
        if typ == 2:
            c, w, size = struct.unpack(">HHQ", payload[:12])
            return {"ch": ch, "header": True, "class": c, "size": size, "props": dec_props(payload[12:])}
        if typ == 3:
            return {"ch": ch, "body": payload}
        if typ == 8:
            return {"ch": ch, "heartbeat": True}
        return {"ch": ch, "frame_type": typ}


def open_connection(host, port, user="guest", password="guest", vhost="/", heartbeat=0, frame_max=131072, client_props=None, mech="PLAIN"):
    c = RawConn(host, port)
    c.send_raw(b"AMQP\x00\x00\x09\x01")
    start = c.read_event(10)
    props = client_props if client_props is not None else {
        "product": "amqp-raw", "capabilities": {"consumer_cancel_notify": True, "publisher_confirms": True, "basic.nack": True, "connection.blocked": True,
                                                 "authentication_failure_close": True}}
    resp = f"\0{user}\0{password}".encode()
    c.send(0, "connection.start-ok", client_properties=props, mechanism=mech, response=resp, locale="en_US")
    tune = c.read_event(10)
    if not tune or tune.get("m") != "connection.tune":
        return c, {"start": start, "tune": tune, "open": None}
    c.frame_max = min(frame_max, tune["a"]["frame_max"] or frame_max)
    c.send(0, "connection.tune-ok", channel_max=tune["a"]["channel_max"], frame_max=c.frame_max, heartbeat=heartbeat)
    c.send(0, "connection.open", virtual_host=vhost, reserved="", insist=False)
    ok = c.read_event(10)
    return c, {"start": start, "tune": tune, "open": ok}
