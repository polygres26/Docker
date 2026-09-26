"""Type-exact BSON -> JSON-able conversion used to compare answers of different servers.

Plain JSON values stand for int32 / string / bool / null; every other BSON type is a one-key tagged object
({"$long": "5"}, {"$double": "1.5"}, {"$oid": ...}, {"$date": ms}, ...). Documents keep their key order (a
Python dict), so field order is part of the comparison, as it is in MongoDB.
"""
import binascii
import datetime
import struct

import bson
from bson import Binary, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.code import Code
from bson.dbref import DBRef


def _cstr(b, i):
    j = b.index(b"\0", i)
    return b[i:j].decode("utf-8", "surrogatepass"), j + 1


def _double(d):
    if d != d:
        return {"$double": "NaN"}
    if d in (float("inf"), float("-inf")):
        return {"$double": "Infinity" if d > 0 else "-Infinity"}
    return {"$double": repr(d)}


def _value(t, b, i):
    if t == 0x01:
        return _double(struct.unpack_from("<d", b, i)[0]), i + 8
    if t in (0x02, 0x0D, 0x0E):
        n = struct.unpack_from("<i", b, i)[0]
        s = b[i + 4:i + 4 + n - 1].decode("utf-8", "surrogatepass")
        i += 4 + n
        return (s if t == 0x02 else {"$code": s} if t == 0x0D else {"$sym": s}), i
    if t == 0x03:
        return _doc(b, i)
    if t == 0x04:
        d, j = _doc(b, i)
        return list(d.values()), j
    if t == 0x05:
        n = struct.unpack_from("<i", b, i)[0]
        sub = b[i + 4]
        data = b[i + 5:i + 5 + n]
        return {"$bin": [sub, binascii.hexlify(data).decode()]}, i + 5 + n
    if t == 0x06:
        return {"$undefined": True}, i
    if t == 0x07:
        return {"$oid": binascii.hexlify(b[i:i + 12]).decode()}, i + 12
    if t == 0x08:
        return b[i] == 1, i + 1
    if t == 0x09:
        return {"$date": struct.unpack_from("<q", b, i)[0]}, i + 8
    if t == 0x0A:
        return None, i
    if t == 0x0B:
        p, i = _cstr(b, i)
        f, i = _cstr(b, i)
        return {"$re": [p, f]}, i
    if t == 0x0C:
        n = struct.unpack_from("<i", b, i)[0]
        ns = b[i + 4:i + 4 + n - 1].decode()
        i += 4 + n
        return {"$dbptr": [ns, binascii.hexlify(b[i:i + 12]).decode()]}, i + 12
    if t == 0x0F:
        n = struct.unpack_from("<i", b, i)[0]
        m = struct.unpack_from("<i", b, i + 4)[0]
        code = b[i + 8:i + 8 + m - 1].decode()
        scope, j = _doc(b, i + 8 + m)
        return {"$codews": [code, scope]}, j
    if t == 0x10:
        return struct.unpack_from("<i", b, i)[0], i + 4
    if t == 0x11:
        inc, sec = struct.unpack_from("<II", b, i)
        return {"$ts": [sec, inc]}, i + 8
    if t == 0x12:
        return {"$long": str(struct.unpack_from("<q", b, i)[0])}, i + 8
    if t == 0x13:
        return {"$dec": str(Decimal128.from_bid(b[i:i + 16]))}, i + 16
    if t == 0xFF:
        return {"$minKey": 1}, i
    if t == 0x7F:
        return {"$maxKey": 1}, i
    raise ValueError("unknown bson type 0x%02x" % t)


def _doc(b, i):
    n = struct.unpack_from("<i", b, i)[0]
    end = i + n
    i += 4
    out = {}
    while b[i] != 0:
        t = b[i]
        name, i = _cstr(b, i + 1)
        v, i = _value(t, b, i)
        out[name] = v
    return out, end


def tdecode(raw):
    """Typed, JSON-able tree of one BSON document (bytes)."""
    d, _ = _doc(bytes(raw), 0)
    return d


def tpy(v):
    """Typed tree of a decoded Python value (for driver results that arrive as Python objects)."""
    if v is None or isinstance(v, (bool, str)):
        return v
    if isinstance(v, Int64):
        return {"$long": str(int(v))}
    if isinstance(v, int):
        return v if -2**31 <= v < 2**31 else {"$long": str(v)}
    if isinstance(v, float):
        return _double(v)
    if isinstance(v, ObjectId):
        return {"$oid": str(v)}
    if isinstance(v, datetime.datetime):
        return {"$date": bson.datetime_ms.DatetimeMS(v).__int__() if hasattr(bson, "datetime_ms") else int(v.timestamp() * 1000)}
    if isinstance(v, Decimal128):
        return {"$dec": str(v)}
    if isinstance(v, Binary):
        return {"$bin": [v.subtype, binascii.hexlify(bytes(v)).decode()]}
    if isinstance(v, Regex):
        return {"$re": [v.pattern, "".join(c for c in "imsxlu" if v.flags & {"i": 2, "m": 8, "s": 16, "x": 64, "l": 4, "u": 32}[c])]}
    if isinstance(v, Timestamp):
        return {"$ts": [v.time, v.inc]}
    if isinstance(v, MinKey):
        return {"$minKey": 1}
    if isinstance(v, MaxKey):
        return {"$maxKey": 1}
    if isinstance(v, Code):
        return {"$code": str(v)}
    if isinstance(v, DBRef):
        return {"$dbref": [v.collection, tpy(v.id)]}
    if isinstance(v, (bytes, bytearray)):
        return {"$bin": [0, binascii.hexlify(bytes(v)).decode()]}
    if isinstance(v, dict):
        return {str(k): tpy(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [tpy(x) for x in v]
    if hasattr(v, "raw") and hasattr(v, "keys"):
        return tdecode(v.raw)
    return {"$py": repr(v)}


def mask(tree, kinds):
    """Replace values of the given tagged kinds ("$date", "$ts", ...) with a constant."""
    if isinstance(tree, dict):
        if len(tree) == 1:
            (k, _), = tree.items()
            if k in kinds:
                return {k: "<masked>"}
        return {k: mask(v, kinds) for k, v in tree.items()}
    if isinstance(tree, list):
        return [mask(v, kinds) for v in tree]
    return tree


class OidMap:
    """Replaces ObjectIds with per-case sequence numbers (identical relationships, comparable values)."""

    def __init__(self):
        self.map = {}

    def apply(self, tree):
        if isinstance(tree, dict):
            if len(tree) == 1 and "$oid" in tree:
                h = tree["$oid"]
                return {"$oid": "<%d>" % self.map.setdefault(h, len(self.map) + 1)}
            return {k: self.apply(v) for k, v in tree.items()}
        if isinstance(tree, list):
            return [self.apply(v) for v in tree]
        return tree
