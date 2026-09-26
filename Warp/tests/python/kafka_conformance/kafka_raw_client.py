"""Raw Kafka protocol client for error-code / wire checks: encodes any request at any version and decodes the response using
the official Kafka message schemas that ship inside kafka-python 3.x (kafka.protocol.schemas). Requests are plain dicts/lists."""
import importlib
import pkgutil
import socket
import struct

import kafka.protocol as kp
from kafka.protocol.api_message import ApiMessage

REG = {}


def _scan():
    for m in pkgutil.walk_packages(kp.__path__, "kafka.protocol."):
        if ".old" in m.name or m.name.endswith("generate_stubs"):
            continue
        try:
            mod = importlib.import_module(m.name)
        except Exception:  # noqa: BLE001
            continue
        for n in dir(mod):
            c = getattr(mod, n)
            if isinstance(c, type) and issubclass(c, ApiMessage) and getattr(c, "_json", None) is not None:
                REG[c._json["name"]] = c


_scan()


def _ensure(f):
    if f.is_struct() or f.is_struct_array():
        if not f.has_data_class():
            from kafka.protocol.data_container import DataContainer
            f.set_data_class(type(f.type_str, (DataContainer,), {"_struct": f}))
        for g in f._fields:
            _ensure(g)


def _conv(f, val):
    """dict -> DataContainer of the struct field `f` (recursively)."""
    kw = {}
    for g in f._fields:
        if g.name in val:
            v = val[g.name]
            if v is not None and g.is_struct_array():
                v = [_conv(g, x) for x in v]
            elif v is not None and g.is_struct():
                v = _conv(g, v)
            kw[g.name] = v
    return f.data_class(**kw)


def _build(cls, ver, dct):
    top = cls[ver]
    kw = {}
    for f in cls._struct._fields:
        _ensure(f)
        if f.name in dct:
            v = dct[f.name]
            if v is not None and f.is_struct_array():
                v = [_conv(f, x) for x in v]
            elif v is not None and f.is_struct():
                v = _conv(f, v)
            kw[f.name] = v
    return top(**kw)


class Raw:
    """One connection; `call` sends a request and returns the decoded response as a dict."""

    def __init__(self, host, port, client_id="raw", timeout=30):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.cid = 0
        self.client_id = client_id

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass

    def _recv(self, n):
        buf = b""
        while len(buf) < n:
            c = self.s.recv(n - len(buf))
            if not c:
                raise ConnectionError("closed")
            buf += c
        return buf

    def send(self, name, ver, **fields):
        Q = REG[name + "Request"]
        r = _build(Q, ver, fields)
        self.cid += 1
        r.with_header(correlation_id=self.cid, client_id=self.client_id)
        self.s.sendall(r.encode(header=True, framed=True))
        return self.cid

    def recv(self, name, ver):
        P = REG[name + "Response"]
        n = struct.unpack(">i", self._recv(4))[0]
        return P.decode(self._recv(n), version=ver, header=True).to_dict(json=False)

    def call(self, name, ver, **fields):
        self.send(name, ver, **fields)
        return self.recv(name, ver)

    def send_raw(self, payload):
        self.s.sendall(struct.pack(">i", len(payload)) + payload)

    def recv_raw(self):
        n = struct.unpack(">i", self._recv(4))[0]
        return self._recv(n)
