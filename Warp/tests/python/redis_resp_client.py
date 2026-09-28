"""A minimal, dependency-free RESP2/RESP3 client (raw sockets) for the rediswire tests.

The redis-py package is not installed on the machines this suite runs on, so the rediswire tests speak the
protocol themselves. Replies keep their wire type: simple strings are `Simple`, bulk strings `bytes`, errors
`RespError` (returned, not raised, unless `raise_errors`), RESP3 maps `RMap`, sets `RSet`, pushes `Push`,
verbatim strings `Verbatim`, doubles `float`, booleans `bool`, nulls `None`.
"""
import socket
import time


class Simple(str):
    pass


class RespError(Exception):
    def __init__(self, text):
        super().__init__(text)
        self.text = text

    def __eq__(self, other):
        return isinstance(other, RespError) and other.text == self.text

    def __hash__(self):
        return hash(self.text)

    def __repr__(self):
        return f"RespError({self.text!r})"


class RMap(list):
    """RESP3 map as an ordered list of (key, value) pairs."""

    def as_dict(self):
        return dict(self)


class RSet(list):
    pass


class Push(list):
    pass


class Verbatim(bytes):
    pass


class RespProtocolError(Exception):
    pass


def enc(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, bytes):
            b = a
        elif isinstance(a, bool):
            b = b"1" if a else b"0"
        elif isinstance(a, (int, float)):
            b = repr(a).encode() if isinstance(a, float) else str(a).encode()
        else:
            b = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


class Resp:
    def __init__(self, host="127.0.0.1", port=6379, timeout=30, password=None, username=None, protocol=2):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.protocol = 2
        if password is not None:
            self.execute("AUTH", *([username] if username else []), password)
        if protocol == 3:
            self.execute("HELLO", 3)
            self.protocol = 3

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    # -- reading ----------------------------------------------------------------------------------
    def _fill(self):
        chunk = self.sock.recv(1 << 20)
        if not chunk:
            raise ConnectionError("connection closed by server")
        self.buf += chunk

    def _line(self):
        while True:
            i = self.buf.find(b"\r\n")
            if i >= 0:
                line, self.buf = self.buf[:i], self.buf[i + 2:]
                return line
            self._fill()

    def _take(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read_reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return Simple(rest.decode())
        if t == b"-":
            return RespError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._take(n)
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read_reply() for _ in range(n)]
        if t == b"_":
            return None
        if t == b"#":
            return rest == b"t"
        if t == b",":
            s = rest.decode()
            return float("inf") if s == "inf" else float("-inf") if s == "-inf" else float(s)
        if t == b"%":
            return RMap((self.read_reply(), self.read_reply()) for _ in range(int(rest)))
        if t == b"~":
            return RSet(self.read_reply() for _ in range(int(rest)))
        if t == b">":
            return Push(self.read_reply() for _ in range(int(rest)))
        if t == b"=":
            data = self._take(int(rest))
            return Verbatim(data[4:])
        if t == b"(":
            return int(rest)
        raise RespProtocolError(f"bad reply type {line!r}")

    # -- commands ---------------------------------------------------------------------------------
    def send(self, *args):
        self.sock.sendall(enc(*args))

    def execute(self, *args, raise_errors=False):
        self.send(*args)
        r = self.read_reply()
        if raise_errors and isinstance(r, RespError):
            raise r
        return r

    def __call__(self, *args):
        return self.execute(*args)

    def pipeline(self, commands):
        self.sock.sendall(b"".join(enc(*c) for c in commands))
        return [self.read_reply() for _ in commands]

    def raw(self, data: bytes):
        self.sock.sendall(data)

    def try_read(self, timeout=0.2):
        """Next reply if one arrives within `timeout` seconds, else None."""
        old = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            if not self.buf:
                self._fill()
            return self.read_reply()
        except (socket.timeout, TimeoutError):
            return None
        finally:
            self.sock.settimeout(old)


def norm(x):
    """Normalise a reply for comparison: bytes -> str (utf-8/latin-1), containers recursively."""
    if isinstance(x, RespError):
        return x
    if isinstance(x, (RMap,)):
        return {norm(k): norm(v) for k, v in x}
    if isinstance(x, RSet):
        return sorted(norm(i) for i in x) if all(isinstance(i, (bytes, str)) for i in x) else [norm(i) for i in x]
    if isinstance(x, list):
        return [norm(i) for i in x]
    if isinstance(x, bytes):
        try:
            return x.decode()
        except UnicodeDecodeError:
            return x.decode("latin-1")
    if isinstance(x, Simple):
        return str(x)
    return x
