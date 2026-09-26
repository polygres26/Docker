"""A minimal CQL native protocol client (v3/v4) used to test frames the python driver never sends: unsupported versions, malformed frames,
events, unprepared statements. Frames are built by hand from the protocol specification."""
import socket
import struct


def short(n):
    return struct.pack(">H", n)


def string(s):
    b = s.encode()
    return short(len(b)) + b


def long_string(s):
    b = s.encode()
    return struct.pack(">i", len(b)) + b


def string_map(m):
    return short(len(m)) + b"".join(string(k) + string(v) for k, v in m.items())


def string_list(items):
    return short(len(items)) + b"".join(string(i) for i in items)


class Raw:
    ERROR, STARTUP, READY, AUTHENTICATE, OPTIONS, SUPPORTED, QUERY, RESULT, PREPARE, EXECUTE, REGISTER, EVENT = 0, 1, 2, 3, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC

    def __init__(self, port, version=4, host="127.0.0.1", timeout=10):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.version = version

    def send(self, opcode, body=b"", stream=0, version=None, flags=0):
        v = self.version if version is None else version
        self.s.sendall(struct.pack(">BBhBI", v, flags, stream, opcode, len(body)) + body)

    def recv(self):
        hdr = self._exact(9)
        ver, flags, stream, opcode, length = struct.unpack(">BBhBI", hdr)
        return {"version": ver, "flags": flags, "stream": stream, "opcode": opcode, "body": self._exact(length)}

    def _exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.s.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf

    def startup(self):
        self.send(self.STARTUP, string_map({"CQL_VERSION": "3.0.0"}))
        return self.recv()

    def query(self, cql, consistency=1, stream=1):
        self.send(self.QUERY, long_string(cql) + short(consistency) + b"\x00", stream=stream)
        return self.recv()

    def close(self):
        self.s.close()


def error_of(frame):
    assert frame["opcode"] == Raw.ERROR, frame
    code = struct.unpack(">i", frame["body"][:4])[0]
    n = struct.unpack(">H", frame["body"][4:6])[0]
    return code, frame["body"][6:6 + n].decode()
