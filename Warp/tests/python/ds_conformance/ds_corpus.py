"""The Datastore differential corpus: named cases, each a list of steps replayed in order against a fresh project on the oracle (the
official Cloud Datastore emulator) and on Warp. See ds_harness.py for the step format and normalisation. Kinds starting with "Auto"
have server-allocated ids, normalised to <id>."""
import base64

CASES = {}


# ------------------------------------------------------------------------------------------------------------------ helpers

def I(n):
    return {"integerValue": str(n)}


def D(x):
    return {"doubleValue": x}


def S(s):
    return {"stringValue": s}


def B(b):
    return {"booleanValue": b}


NULL = {"nullValue": "NULL_VALUE"}
NAN = {"doubleValue": "NaN"}


def T(iso):
    return {"timestampValue": iso}


def BL(raw):
    return {"blobValue": base64.b64encode(raw).decode()}


def KV(*path):
    return {"keyValue": K(*path)}


def GEO(lat, lng):
    return {"geoPointValue": {"latitude": lat, "longitude": lng}}


def A(*vs):
    return {"arrayValue": {"values": list(vs)}} if vs else {"arrayValue": {}}


def E(**props):
    return {"entityValue": {"properties": props}}


def NX(v):
    """Excluded from indexes."""
    return {**v, "excludeFromIndexes": True}


def K(*path, ns=None):
    """K(("Kind", "name"), ("Child", 5)) -> a key; an element with id None is incomplete."""
    els = []
    for kind, ident in path:
        e = {"kind": kind}
        if isinstance(ident, int) and ident != 0:
            e["id"] = str(ident)
        elif isinstance(ident, str):
            e["name"] = ident
        els.append(e)
    k = {"path": els}
    if ns is not None:
        k["partitionId"] = {"projectId": "@P@", "namespaceId": ns}
    return k


def ent(key, **props):
    return {"key": key, "properties": props}


def rpc(m, r=None, **kw):
    return {"m": m, "r": r if r is not None else {}, **kw}


def lookup(*keys, ro=None):
    r = {"keys": list(keys)}
    if ro is not None:
        r["readOptions"] = ro
    return rpc("Lookup", r)


def commit(*muts, txn=None, mode=None):
    r = {"mutations": list(muts)}
    if txn is not None:
        r["transaction"] = txn
        r["mode"] = mode or "TRANSACTIONAL"
    else:
        r["mode"] = mode or "NON_TRANSACTIONAL"
    return rpc("Commit", r)


def up(key, **props):
    return {"upsert": ent(key, **props)}


def ins(key, **props):
    return {"insert": ent(key, **props)}


def upd(key, **props):
    return {"update": ent(key, **props)}


def dele(key):
    return {"delete": key}


def seed(kind, rows):
    """One commit upserting kind/<name-or-id> -> props."""
    return commit(*[up(K((kind, i)), **p) for i, p in rows.items()])


def prop(name):
    return {"name": name}


def pf(name, op, v):
    return {"propertyFilter": {"property": prop(name), "op": op, "value": v}}


def anc(key):
    return {"propertyFilter": {"property": prop("__key__"), "op": "HAS_ANCESTOR", "value": {"keyValue": key}}}


def AND(*fs):
    return {"compositeFilter": {"op": "AND", "filters": list(fs)}}


def OR(*fs):
    return {"compositeFilter": {"op": "OR", "filters": list(fs)}}


def order(name, d="ASCENDING"):
    return {"property": prop(name), "direction": d}


def q(kind=None, where=None, order_by=None, limit=None, offset=None, start=None, end=None, project=None, distinct=None):
    d = {}
    if kind is not None:
        d["kind"] = [{"name": kind}]
    if where is not None:
        d["filter"] = where
    if order_by:
        d["order"] = order_by
    if limit is not None:
        d["limit"] = limit
    if offset is not None:
        d["offset"] = offset
    if start is not None:
        d["startCursor"] = start
    if end is not None:
        d["endCursor"] = end
    if project is not None:
        d["projection"] = [{"property": prop(p)} for p in project]
    if distinct is not None:
        d["distinctOn"] = [prop(p) for p in distinct]
    return d


def query(qq, ns=None, ro=None, gql=None):
    r = {}
    if qq is not None:
        r["query"] = qq
    if gql is not None:
        r["gqlQuery"] = gql
    if ns is not None:
        r["partitionId"] = {"projectId": "@P@", "namespaceId": ns}
    if ro is not None:
        r["readOptions"] = ro
    return rpc("RunQuery", r)


def agg(qq, aggs, ns=None):
    r = {"aggregationQuery": {"nestedQuery": qq, "aggregations": aggs}}
    if ns is not None:
        r["partitionId"] = {"projectId": "@P@", "namespaceId": ns}
    return rpc("RunAggregationQuery", r)


def COUNT(alias=None, up_to=None):
    c = {"count": {}}
    if up_to is not None:
        c["count"]["upTo"] = str(up_to)
    if alias:
        c["alias"] = alias
    return c


def SUM(p, alias=None):
    c = {"sum": {"property": prop(p)}}
    if alias:
        c["alias"] = alias
    return c


def AVG(p, alias=None):
    c = {"avg": {"property": prop(p)}}
    if alias:
        c["alias"] = alias
    return c


def ref(step, path):
    return {"$ref": [step, path]}


def begin(ro=False):
    return rpc("BeginTransaction", {"transactionOptions": {"readOnly": {}} if ro else {"readWrite": {}}})


def case(n, steps):
    CASES[n] = steps


def RS(method, path, body=None):
    """REST call below /v1/projects/@P@ (path like ':lookup')."""
    return {"m": "REST", "method": method, "path": "/v1/projects/@P@" + path, "body": body}


# ------------------------------------------------------------------------------------------------------------------ 1. CRUD

case("crud", [
    commit(ins(K(("User", "alice")), name=S("Alice"), age=I(30))),                                                            # 0
    lookup(K(("User", "alice"))),                                                                                            # 1
    commit(ins(K(("User", "alice")), name=S("dup"))),                                                                        # 2 already exists
    commit(upd(K(("User", "bob")), name=S("Bob"))),                                                                          # 3 not found
    commit(up(K(("User", "bob")), name=S("Bob"), age=I(25))),                                                                # 4
    commit(upd(K(("User", "bob")), name=S("Robert"))),                                                                       # 5 update replaces
    lookup(K(("User", "bob"))),                                                                                              # 6
    lookup(K(("User", "alice")), K(("User", "nobody")), K(("User", "bob"))),                                                 # 7 found + missing
    lookup(K(("User", "alice")), K(("User", "alice"))),                                                                      # 8 duplicate keys
    commit(dele(K(("User", "bob")))),                                                                                        # 9
    commit(dele(K(("User", "bob")))),                                                                                        # 10 delete twice
    lookup(K(("User", "bob"))),                                                                                              # 11
    commit(ins(K(("User", 7)), name=S("numeric"))),                                                                          # 12 numeric id
    lookup(K(("User", 7))),                                                                                                  # 13
    commit(ins(K(("Auto", 0)), n=I(1))),                                                                                     # 14 incomplete key -> allocated
    commit(up(K(("Auto", 0)), n=I(2))),                                                                                      # 15
    commit(ins(K(("Auto", 0)), n=I(3)), ins(K(("Auto", 0)), n=I(4))),                                                        # 16 two allocations
    query(q("Auto", order_by=[order("n")])),                                                                                 # 17
    commit(ins(K(("User", "p"), ("AutoPost", 0)), t=S("child"))),                                                                # 18 child with incomplete key
    commit(ins(K(("User", 0), ("AutoPost", "x")), t=S("bad"))),                                                                  # 19 incomplete ancestor
    lookup(K(("User", 0))),                                                                                                  # 20 incomplete key lookup
    commit(up({"path": []}, a=I(1))),                                                                                        # 21 empty key path
    commit(up({"path": [{"kind": "", "name": "x"}]}, a=I(1))),                                                               # 22 empty kind
    commit(up({"path": [{"kind": "K", "name": ""}]}, a=I(1))),                                                               # 23 empty name (incomplete)
    commit(up(K(("__bad__", "x")), a=I(1))),                                                                                 # 24 reserved kind
    commit(up(K(("K", "x" * 1501)), a=I(1))),                                                                                # 25 long name
    commit(up(K(("K", -5)), a=I(1))) if False else commit(up({"path": [{"kind": "K", "id": "-5"}]}, a=I(1))),               # 26 negative id
    commit(up(K(("K", "x")), **{"": I(1)})),                                                                                 # 27 empty property name
    commit(up(K(("K", "x")), **{"__bad__": I(1)})),                                                                          # 28 reserved property
    commit(up(K(("K", "x")), **{"a.b": I(1)})),                                                                              # 29 dotted property
    commit(up({"partitionId": {"projectId": "other"}, "path": [{"kind": "K", "name": "x"}]}, a=I(1))),                       # 30 other project
    commit(),                                                                                                                # 31 no mutations
    commit({}),                                                                                                              # 32 empty mutation
    commit(up(K(("K", "x")), a=I(1)), up(K(("K", "x")), a=I(2))),                                                            # 33 two mutations same key
    lookup(K(("K", "x"))),                                                                                                   # 34
    commit(ins(K(("K", "y")), a=I(1)), ins(K(("K", "y")), a=I(2))),                                                          # 35 insert twice
    lookup(),                                                                                                                # 36 no keys
    lookup(K(("K", "x")), ro={"readConsistency": "EVENTUAL"}),                                                               # 37
    lookup(K(("K", "x")), ro={"readConsistency": "STRONG"}),                                                                 # 38
    lookup(K(("K", "x")), ro={"transaction": "AAAA"}),                                                                       # 39
])

# ------------------------------------------------------------------------------------------------------------------ 2. values & ordering

VALS = {
    "a_null": {"v": NULL}, "b_false": {"v": B(False)}, "c_true": {"v": B(True)}, "d_nan": {"v": NAN},
    "e_neg": {"v": I(-1)}, "f_int2": {"v": I(2)}, "g_dbl": {"v": D(1.5)}, "s_dbl2": {"v": D(2.0)}, "t_inf": {"v": D(float("inf")) if False else {"doubleValue": "Infinity"}},
    "h_ts": {"v": T("2020-01-01T00:00:00Z")}, "i_str": {"v": S("apple")}, "j_str2": {"v": S("banana")}, "j2_uni": {"v": S("é")},
    "k_blob": {"v": BL(b"\x01\x02")}, "l_blob2": {"v": BL(b"\x01\x03")},
    "m_key": {"v": KV(("K", "z"))}, "n_key2": {"v": KV(("A", 5))}, "n2_key3": {"v": KV(("A", "b"), ("C", 1))},
    "o_geo": {"v": GEO(1, 2)}, "p_geo": {"v": GEO(1, 3)},
    "q_ent": {"v": E(a=I(1))}, "r_arr": {"v": A(I(5), S("x"))}, "r2_arr": {"v": A(I(1), I(9))}, "r3_empty": {"v": A()},
    "z_nov": {"other": I(1)},
}
case("values_order", [
    seed("T", VALS),                                                                                                         # 0
    query(q("T", order_by=[order("v")])),                                                                                    # 1
    query(q("T", order_by=[order("v", "DESCENDING")])),                                                                      # 2
    query(q("T", where=pf("v", "GREATER_THAN", I(0)))),                                                                      # 3
    query(q("T", where=pf("v", "LESS_THAN", I(3)))),                                                                         # 4
    query(q("T", where=pf("v", "EQUAL", I(2)))),                                                                             # 5
    query(q("T", where=pf("v", "EQUAL", D(2.0)))),                                                                           # 6
    query(q("T", where=pf("v", "GREATER_THAN", S("apple")))),                                                                # 7
    query(q("T", where=pf("v", "LESS_THAN", S("b")))),                                                                       # 8
    query(q("T", where=pf("v", "GREATER_THAN", T("2019-01-01T00:00:00Z")))),                                                 # 9
    query(q("T", where=pf("v", "EQUAL", B(True)))),                                                                          # 10
    query(q("T", where=pf("v", "GREATER_THAN", B(False)))),                                                                  # 11
    query(q("T", where=pf("v", "GREATER_THAN", BL(b"\x01\x02")))),                                                           # 12
    query(q("T", where=pf("v", "EQUAL", NULL))),                                                                             # 13
    query(q("T", where=pf("v", "GREATER_THAN", NULL))),                                                                      # 14
    query(q("T", where=pf("v", "EQUAL", NAN))),                                                                              # 15
    query(q("T", where=pf("v", "GREATER_THAN", D(1.0)))),                                                                    # 16
    query(q("T", where=pf("v", "EQUAL", KV(("K", "z"))["keyValue"] and KV(("K", "z"))))),                                    # 17
    query(q("T", where=pf("v", "GREATER_THAN", KV(("A", 5))))),                                                              # 18
    query(q("T", where=pf("v", "EQUAL", GEO(1, 2)))),                                                                        # 19
    query(q("T", where=pf("v", "GREATER_THAN", GEO(1, 2)))),                                                                 # 20
    query(q("T", where=pf("v", "EQUAL", S("x")))),                                                                           # 21 array element match
    query(q("T", where=pf("v", "EQUAL", I(5)))),                                                                             # 22
    query(q("T", where=pf("v", "GREATER_THAN", I(8)))),                                                                      # 23
    query(q("T", where=pf("v.a", "EQUAL", I(1)))),                                                                           # 24 embedded entity path
    query(q("T", where=pf("v", "EQUAL", E(a=I(1))))),                                                                        # 25 entity equality
    query(q("T", where=pf("v", "EQUAL", A(I(5), S("x"))))),                                                                  # 26 array equality
    query(q("T", where=pf("other", "EQUAL", I(1)))),                                                                         # 27
    query(q("T", order_by=[order("other")])),                                                                                # 28
    query(q("T", where=pf("v", "NOT_EQUAL", I(2)))),                                                                         # 29
    query(q("T", where=pf("v", "IN", A(I(2), S("apple"), NULL)))),                                                           # 30
    query(q("T", where=pf("v", "NOT_IN", A(I(2), S("apple"))))),                                                             # 31
    query(q("T", where=pf("v", "GREATER_THAN", I(1)), order_by=[order("v", "DESCENDING")])),                                 # 32 array order with filter
    query(q("T", project=["v"])),                                                                                            # 33 projection incl. arrays
    query(q("T", project=["v"], order_by=[order("v")])),                                                                     # 34
    query(q("T", project=["__key__"])),                                                                                      # 35 keys only
])

case("values_special", [
    commit(up(K(("S", "big")), i=I(9223372036854775807), n=I(-9223372036854775808), d=D(1e300), z=D(-0.0), s=S("x" * 1500)),
           up(K(("S", "long")), s=S("x" * 1501)),
           up(K(("S", "longx")), s=NX(S("x" * 1501))),
           up(K(("S", "blob")), b=BL(b"x" * 1500)),
           up(K(("S", "blobx")), b=BL(b"x" * 1501))),                                                                        # 0
    commit(up(K(("S", "big")), i=I(9223372036854775807), n=I(-9223372036854775808), d=D(1e300), z=D(-0.0), s=S("x" * 1500))),  # 1
    commit(up(K(("S", "long")), s=S("x" * 1501))),                                                                           # 2
    commit(up(K(("S", "longx")), s=NX(S("x" * 1501)))),                                                                      # 3
    commit(up(K(("S", "blobx")), b=BL(b"x" * 1501))),                                                                        # 4
    lookup(K(("S", "big")), K(("S", "longx"))),                                                                              # 5
    commit(up(K(("S", "ts")), t=T("2020-01-01T00:00:00.123456789Z"))),                                                       # 6 nanos
    lookup(K(("S", "ts"))),                                                                                                  # 7
    commit(up(K(("S", "arr")), a=A(I(1), A(I(2))))),                                                                         # 8 nested array
    commit(up(K(("S", "emb")), e=E(x=I(1), y=E(z=S("deep")), arr=A(I(1), I(2))))),                                           # 9
    lookup(K(("S", "emb"))),                                                                                                 # 10
    query(q("S", where=pf("e.y.z", "EQUAL", S("deep")))),                                                                    # 11
    query(q("S", where=pf("e.arr", "EQUAL", I(2)))),                                                                         # 12
    commit(up(K(("S", "ix")), a=NX(I(1)), b=I(2))),                                                                          # 13
    query(q("S", where=pf("a", "EQUAL", I(1)))),                                                                             # 14 unindexed property is not queryable
    query(q("S", where=pf("b", "EQUAL", I(2)))),                                                                             # 15
    commit(up(K(("S", "meaning")), a=I(1), b=NX(BL(b"x")))),                                                                 # 16
    lookup(K(("S", "meaning"))),                                                                                             # 17
    commit(up(K(("S", "geo")), g=GEO(91, 0))),                                                                               # 18 bad geo
    commit(up(K(("S", "keyv")), k=KV(("Other", "a")))),                                                                      # 19
    lookup(K(("S", "keyv"))),                                                                                                # 20
    commit(up(K(("S", "nokey")), e={"entityValue": {"key": K(("Inner", 1)), "properties": {"a": I(1)}}})),                  # 21 entity value with key
    lookup(K(("S", "nokey"))),                                                                                               # 22
    commit(up(K(("S", "nullarr")), a=A(NULL, I(1), NULL))),                                                                  # 23
    query(q("S", where=pf("a", "EQUAL", NULL))),                                                                             # 24
])

# ------------------------------------------------------------------------------------------------------------------ 3. filters & order

PEOPLE = {
    "p1": {"name": S("Ann"), "age": I(30), "city": S("Paris"), "tags": A(S("a"), S("b"))},
    "p2": {"name": S("Bob"), "age": I(25), "city": S("Rome"), "tags": A(S("b"))},
    "p3": {"name": S("Cid"), "age": I(35), "city": S("Paris"), "tags": A(S("c"))},
    "p4": {"name": S("Dee"), "age": I(25), "city": S("Oslo"), "tags": A()},
    "p5": {"name": S("Eve"), "age": D(30.0), "city": NULL, "tags": A(S("a"), S("c"))},
    "p6": {"name": S("Fay"), "city": S("Rome")},
    "p7": {"name": S("Gus"), "age": I(40), "city": S("Paris"), "tags": A(S("d"))},
    "p8": {"name": S("Hal"), "age": I(22), "city": S("Oslo"), "tags": A(S("a"))},
}
case("filters", [
    seed("P", PEOPLE),                                                                                                       # 0
    query(q("P", where=pf("city", "EQUAL", S("Paris")))),                                                                    # 1
    query(q("P")),                                                                                                           # 2 key order
    query(q("P", where=pf("age", "GREATER_THAN", I(25)))),                                                                   # 3
    query(q("P", where=pf("age", "LESS_THAN_OR_EQUAL", I(25)))),                                                             # 4
    query(q("P", where=pf("age", "NOT_EQUAL", I(25)))),                                                                      # 5
    query(q("P", where=pf("age", "IN", A(I(25), I(40))))),                                                                   # 6
    query(q("P", where=pf("age", "NOT_IN", A(I(25), I(40))))),                                                               # 7
    query(q("P", where=pf("tags", "EQUAL", S("a")))),                                                                        # 8 array contains
    query(q("P", where=AND(pf("tags", "EQUAL", S("a")), pf("tags", "EQUAL", S("b"))))),                                      # 9 array both
    query(q("P", where=AND(pf("city", "EQUAL", S("Paris")), pf("age", "GREATER_THAN", I(30))))),                             # 10
    query(q("P", where=OR(pf("city", "EQUAL", S("Oslo")), pf("age", "GREATER_THAN", I(35))))),                               # 11
    query(q("P", where=OR(AND(pf("city", "EQUAL", S("Paris")), pf("age", "LESS_THAN", I(40))), pf("name", "EQUAL", S("Bob"))))),  # 12
    query(q("P", where=pf("city", "EQUAL", NULL))),                                                                          # 13
    query(q("P", where=AND(pf("age", "GREATER_THAN", I(20)), pf("city", "GREATER_THAN", S("A"))))),                          # 14 two inequality props
    query(q("P", where=AND(pf("age", "GREATER_THAN", I(20)), pf("age", "LESS_THAN", I(31))))),                               # 15 range one prop
    query(q("P", where=pf("__key__", "GREATER_THAN", {"keyValue": K(("P", "p5"))}))),                                        # 16
    query(q("P", where=pf("__key__", "EQUAL", {"keyValue": K(("P", "p3"))}))),                                               # 17
    query(q("P", where=pf("__key__", "IN", A(KV(("P", "p1")), KV(("P", "p8")))))),                                           # 18
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("name")])),                                         # 19 inequality then other order
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("age", "DESCENDING"), order("name")])),             # 20
    query(q("P", order_by=[order("age")])),                                                                                  # 21 missing property excluded
    query(q("P", order_by=[order("age", "DESCENDING"), order("name", "DESCENDING")])),                                       # 22
    query(q("P", order_by=[order("city"), order("__key__", "DESCENDING")])),                                                 # 23
    query(q("P", order_by=[order("__key__", "DESCENDING")])),                                                                # 24
    query(q("P", order_by=[order("tags")])),                                                                                 # 25 order by array
    query(q("P", order_by=[order("tags", "DESCENDING")])),                                                                   # 26
    query(q("P", where=pf("tags", "GREATER_THAN", S("a")), order_by=[order("tags")])),                                       # 27
    query(q("P", where=pf("tags", "GREATER_THAN", S("a")))),                                                                 # 28
    query(q("P", where=pf("nonexistent", "EQUAL", I(1)))),                                                                   # 29
    query(q("P", where=pf("age", "EQUAL", I(25)), order_by=[order("name", "DESCENDING")])),                                  # 30
    query(q("P", where={})),                                                                                                 # 31 empty filter
    query(q("P", where=AND())),                                                                                              # 32
    query(q("P", where=pf("age", "OPERATOR_UNSPECIFIED", I(1)))),                                                            # 33
    query(q("P", where=pf("age", "IN", A()))),                                                                               # 34
    query(q("P", where=pf("age", "IN", I(1)))),                                                                              # 35
    query(q("P", where=pf("age", "HAS_ANCESTOR", KV(("P", "p1"))))),                                                         # 36 bad ancestor property
    query(q("P", order_by=[order("age"), order("age")])),                                                                    # 37 duplicate order
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("name"), order("age")])),                           # 38
    query(q("P", where=AND(pf("age", "NOT_EQUAL", I(25)), pf("city", "NOT_IN", A(S("Rome")))))),                             # 39
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("__key__")])),                                      # 40
])

case("cursors_limits", [
    seed("P", PEOPLE),                                                                                                       # 0
    query(q("P", order_by=[order("name")], limit=3)),                                                                        # 1
    query(q("P", order_by=[order("name")], limit=3, offset=2)),                                                              # 2
    query(q("P", order_by=[order("name")], limit=3, start=ref(1, "batch.endCursor"))),                                       # 3 next page
    query(q("P", order_by=[order("name")], limit=3, start=ref(3, "batch.endCursor"))),                                       # 4 last page
    query(q("P", order_by=[order("name")], limit=3, start=ref(4, "batch.endCursor"))),                                       # 5 empty page
    query(q("P", order_by=[order("name")], end=ref(1, "batch.endCursor"))),                                                  # 6 end cursor
    query(q("P", order_by=[order("name")], start=ref(1, "batch.endCursor"), end=ref(3, "batch.endCursor"))),                 # 7
    query(q("P", offset=100)),                                                                                               # 8
    query(q("P", limit=0)),                                                                                                  # 9
    query(q("P", limit=100)),                                                                                                # 10
    query(q("P", offset=3)),                                                                                                 # 11
    query(q("P", order_by=[order("name")], limit=3, offset=1, start=ref(1, "batch.endCursor"))),                             # 12
    query(q("P", order_by=[order("age")], limit=2)),                                                                         # 13
    query(q("P", order_by=[order("age")], limit=2, start=ref(13, "batch.endCursor"))),                                       # 14
    query(q("P", order_by=[order("name")], start=ref(13, "batch.endCursor"))),                                               # 15 cursor of another query
    query(q("P", order_by=[order("name")], start="AAAA")),                                                                   # 16 garbage cursor
    query(q("P", limit=-1)),                                                                                                 # 17
    query(q("P", offset=-1)),                                                                                                # 18
    query(q("P", order_by=[order("name", "DESCENDING")], limit=2)),                                                          # 19
    query(q("P", order_by=[order("name", "DESCENDING")], limit=2, start=ref(19, "batch.endCursor"))),                        # 20
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("age"), order("name")], limit=3)),                  # 21
    query(q("P", where=pf("age", "GREATER_THAN", I(20)), order_by=[order("age"), order("name")], limit=3, start=ref(21, "batch.endCursor"))),  # 22
    query(q("P", order_by=[order("name")], limit=8)),                                                                        # 23 limit == count
    query(q("P", order_by=[order("name")], limit=3, offset=6)),                                                              # 24 offset past limit region
])

# ------------------------------------------------------------------------------------------------------------------ 4. entity groups, ancestors, kinds

case("ancestors", [
    commit(up(K(("Root", "r1")), n=I(1)), up(K(("Root", "r2")), n=I(2)),
           up(K(("Root", "r1"), ("Child", "c1")), n=I(3)), up(K(("Root", "r1"), ("Child", "c2")), n=I(4)),
           up(K(("Root", "r1"), ("Child", "c1"), ("Leaf", 1)), n=I(5)),
           up(K(("Root", "r2"), ("Child", "c1")), n=I(6)),
           up(K(("Other", "o1")), n=I(7)),
           up(K(("Root", "r1"), ("Other", "o1")), n=I(8)),
           up(K(("Root", "r1", ), ("Child", 9)), n=I(9))),                                                                   # 0
    query(q("Child", where=anc(K(("Root", "r1"))))),                                                                         # 1
    query(q(None, where=anc(K(("Root", "r1"))))),                                                                            # 2 kindless ancestor (includes the root)
    query(q("Child", where=anc(K(("Root", "r1"), ("Child", "c1"))))),                                                        # 3
    query(q(None, where=anc(K(("Root", "r1"), ("Child", "c1"))))),                                                           # 4
    query(q("Leaf", where=anc(K(("Root", "r1"))))),                                                                          # 5
    query(q(None)),                                                                                                          # 6 kindless: all, key order
    query(q(None, order_by=[order("__key__", "DESCENDING")])),                                                               # 7
    query(q(None, where=pf("__key__", "GREATER_THAN", {"keyValue": K(("Root", "r1"), ("Child", "c1"))}))),                   # 8
    query(q(None, where=pf("n", "GREATER_THAN", I(3)))),                                                                     # 9 kindless with property filter
    query(q(None, order_by=[order("n")])),                                                                                   # 10
    query(q("Child", where=AND(anc(K(("Root", "r1"))), pf("n", "GREATER_THAN", I(3))), order_by=[order("n", "DESCENDING")])),  # 11
    query(q("Child", where=anc(K(("Root", "r1"))), order_by=[order("n")])),                                                  # 12
    query(q("Child", where=anc(K(("Nope", "x"))))),                                                                          # 13
    query(q("Child", where=pf("__key__", "HAS_ANCESTOR", KV(("Root", "r1"))))),                                              # 14 (wrong value shape)
    query(q("Child", where=anc(K(("Root", 0))))),                                                                            # 15 incomplete ancestor
    query(q(None, where=pf("__key__", "HAS_ANCESTOR", {"stringValue": "x"}))),                                               # 16
    lookup(K(("Root", "r1"), ("Child", "c1"), ("Leaf", 1))),                                                                 # 17
    query(q("Root", limit=1)),                                                                                               # 18
    query(q(None, limit=3)),                                                                                                 # 19
    query(q(None, project=["__key__"])),                                                                                     # 20
    query(q("Child", where=anc(K(("Root", "r1"))), project=["n"], order_by=[order("n")])),                                   # 21
    query(q("Child", where=anc(K(("Root", "r1"))), distinct=["n"], order_by=[order("n")])),                               # 22
])

case("namespaces", [
    commit(up(K(("N", "a"), ns="ns1"), v=I(1)), up(K(("N", "a")), v=I(2)), up(K(("N", "b"), ns="ns2"), v=I(3))),             # 0
    lookup(K(("N", "a"))),                                                                                                   # 1
    lookup(K(("N", "a"), ns="ns1")),                                                                                         # 2
    query(q("N")),                                                                                                           # 3 default namespace
    query(q("N"), ns="ns1"),                                                                                                 # 4
    query(q("N"), ns="ns2"),                                                                                                 # 5
    query(q("__namespace__")),                                                                                               # 6 metadata queries
    query(q("__kind__")),                                                                                                    # 7
    query(q("__kind__"), ns="ns1"),                                                                                          # 8
    commit(up(K(("N", "c"), ns="__bad__"), v=I(1))),                                                                         # 9 reserved namespace
])

# ------------------------------------------------------------------------------------------------------------------ 5. projection, distinct

case("projection", [
    commit(up(K(("Q", 1)), a=I(1), b=S("x"), t=A(S("p"), S("q"))), up(K(("Q", 2)), a=I(1), b=S("y"), t=A(S("q"))),
           up(K(("Q", 3)), a=I(2), b=S("x")), up(K(("Q", 4)), b=S("z")), up(K(("Q", 5)), a=NX(I(9)), b=S("w"))),             # 0
    query(q("Q", project=["a"])),                                                                                            # 1
    query(q("Q", project=["a", "b"], order_by=[order("a"), order("b")])),                                                    # 2
    query(q("Q", project=["t"], order_by=[order("t")])),                                                                     # 3 array projection yields one row per value
    query(q("Q", project=["a"], distinct=["a"], order_by=[order("a")])),                                                     # 4
    query(q("Q", project=["a", "b"], distinct=["a", "b"], order_by=[order("a"), order("b")])),                               # 5
    query(q("Q", project=["a"], distinct=["b"])),                                                                            # 6 distinct on non-projected
    query(q("Q", distinct=["a"], order_by=[order("a")])),                                                                    # 7 distinct without projection
    query(q("Q", project=["b"], where=pf("a", "EQUAL", I(1)))),                                                              # 8
    query(q("Q", project=["b"], where=pf("a", "GREATER_THAN", I(0)), order_by=[order("a"), order("b")])),                    # 9
    query(q("Q", project=["a"], order_by=[order("b")])),                                                                     # 10 order by non-projected
    query(q("Q", project=["nonexistent"])),                                                                                  # 11
    query(q("Q", project=["a", "a"])),                                                                                       # 12 duplicate projection
    query(q("Q", project=["a"], limit=1, offset=1, order_by=[order("a")])),                                                  # 13
])

# ------------------------------------------------------------------------------------------------------------------ 6. transactions

case("transactions", [
    seed("X", {"a": {"n": I(1)}, "b": {"n": I(2)}}),                                                                         # 0
    begin(),                                                                                                                 # 1
    lookup(K(("X", "a")), ro={"transaction": ref(1, "transaction")}),                                                        # 2
    commit(up(K(("X", "a")), n=I(10)), txn=ref(1, "transaction")),                                                           # 3
    lookup(K(("X", "a"))),                                                                                                   # 4
    commit(up(K(("X", "a")), n=I(11)), txn=ref(1, "transaction")),                                                           # 5 reuse
    begin(),                                                                                                                 # 6
    rpc("Rollback", {"transaction": ref(6, "transaction")}),                                                                 # 7
    commit(up(K(("X", "a")), n=I(12)), txn=ref(6, "transaction")),                                                           # 8 after rollback
    rpc("Rollback", {"transaction": ref(6, "transaction")}),                                                                 # 9
    begin(ro=True),                                                                                                          # 10
    lookup(K(("X", "a")), ro={"transaction": ref(10, "transaction")}),                                                       # 11
    commit(up(K(("X", "a")), n=I(13)), txn=ref(10, "transaction")),                                                          # 12 write in read-only
    rpc("Rollback", {"transaction": "AAAA"}),                                                                                # 13
    commit(up(K(("X", "a")), n=I(1)), txn="AAAA"),                                                                           # 14
    begin(),                                                                                                                 # 15
    query(q("X", order_by=[order("n")]), ro={"transaction": ref(15, "transaction")}),                                        # 16 query in txn
    commit(up(K(("X", "c")), n=I(3)), txn=ref(15, "transaction")),                                                           # 17
    lookup(K(("X", "c"))),                                                                                                   # 18
    begin(),                                                                                                                 # 19 mode mismatch
    commit(up(K(("X", "d")), n=I(4)), txn=ref(19, "transaction"), mode="NON_TRANSACTIONAL"),                                 # 20
    commit(up(K(("X", "e")), n=I(4)), mode="TRANSACTIONAL"),                                                                 # 21 transactional without txn
    rpc("Commit", {"mode": "MODE_UNSPECIFIED", "mutations": [up(K(("X", "f")), n=I(5))]}),                                   # 22
    rpc("Commit", {"singleUseTransaction": {"readWrite": {}}, "mode": "TRANSACTIONAL", "mutations": [up(K(("X", "g")), n=I(6))]}),  # 23
    lookup(K(("X", "g"))),                                                                                                   # 24
    rpc("Lookup", {"keys": [K(("X", "a"))], "readOptions": {"newTransaction": {"readWrite": {}}}}),                          # 25
    begin(),                                                                                                                 # 26 previous_transaction
    rpc("BeginTransaction", {"transactionOptions": {"readWrite": {"previousTransaction": ref(26, "transaction")}}}),         # 27
    commit(txn=ref(27, "transaction")),                                                                                      # 28 empty txn commit
    commit(ins(K(("X", "a")), n=I(1)), up(K(("X", "h")), n=I(1))),                                                           # 29 atomic failure
    lookup(K(("X", "h"))),                                                                                                   # 30
    commit(up(K(("X", "a")), n=I(1)), dele(K(("X", "a")))),                                                                  # 31 write then delete same key
    commit(dele(K(("X", "b"))), up(K(("X", "b")), n=I(2))),                                                                  # 32
    lookup(K(("X", "a")), K(("X", "b"))),                                                                                    # 33
])

# ------------------------------------------------------------------------------------------------------------------ 7. preconditions & versions

case("preconditions", [
    commit(up(K(("V", "a")), n=I(1))),                                                                                       # 0
    lookup(K(("V", "a"))),                                                                                                   # 1
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"update": ent(K(("V", "a")), n=I(2)), "baseVersion": ref(1, "found.0.version")}]}},  # 2 base version ok
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"update": ent(K(("V", "a")), n=I(3)), "baseVersion": ref(1, "found.0.version")}]}},  # 3 stale
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"delete": K(("V", "a")), "baseVersion": ref(1, "found.0.version")}]}},  # 4 stale delete
    lookup(K(("V", "a"))),                                                                                                   # 5
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"upsert": ent(K(("V", "new")), n=I(1)), "baseVersion": "1"}]}},  # 6 base version on a missing entity
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"upsert": ent(K(("V", "new")), n=I(1)), "baseVersion": "0"}]}},  # 7
    {"m": "Commit", "r": {"mode": "NON_TRANSACTIONAL", "mutations": [{"insert": ent(K(("V", "x")), n=I(1)), "baseVersion": "5"}]}},  # 8
])

# ------------------------------------------------------------------------------------------------------------------ 8. aggregations

case("aggregations", [
    seed("P", PEOPLE),                                                                                                       # 0
    agg(q("P"), [COUNT("c")]),                                                                                               # 1
    agg(q("P"), [COUNT()]),                                                                                                  # 2
    agg(q("P", where=pf("city", "EQUAL", S("Paris"))), [COUNT("c"), SUM("age", "s"), AVG("age", "a")]),                      # 3
    agg(q("P"), [SUM("age", "s"), AVG("age", "a")]),                                                                         # 4
    agg(q("P"), [COUNT("c", up_to=3)]),                                                                                      # 5
    agg(q("P", where=pf("city", "EQUAL", S("Nowhere"))), [COUNT("c"), SUM("age", "s"), AVG("age", "a")]),                    # 6
    agg(q("P"), [SUM("name", "s"), AVG("name", "a")]),                                                                       # 7
    agg(q("P", limit=3), [COUNT("c")]),                                                                                      # 8
    agg(q("P", order_by=[order("age")], limit=2, offset=1), [SUM("age", "s")]),                                              # 9
    agg(q("P"), []),                                                                                                         # 10
    agg(q("P"), [COUNT("a"), COUNT("a")]),                                                                                   # 11
    agg(q("P"), [COUNT("c", up_to=-1)]),                                                                                     # 12
    agg(q(None), [COUNT("c")]),                                                                                              # 13 kindless
    agg(q("P", project=["age"]), [COUNT("c")]),                                                                              # 14
    agg(q("P"), [{"alias": "z"}]),                                                                                           # 15
    agg(q("P", where=pf("age", "GREATER_THAN", I(24))), [COUNT("c")]),                                                       # 16
    agg(q("P", where=pf("tags", "EQUAL", S("a"))), [COUNT("c"), SUM("age", "s")]),                                           # 17
])

# ------------------------------------------------------------------------------------------------------------------ 9. ids

case("ids", [
    rpc("AllocateIds", {"keys": [K(("Auto", 0)), K(("Auto", 0))]}),                                                          # 0
    rpc("AllocateIds", {"keys": [K(("Auto", 5))]}),                                                                          # 1 complete key
    rpc("AllocateIds", {"keys": [K(("Root", "r"), ("Auto", 0))]}),                                                           # 2 child
    rpc("AllocateIds", {"keys": []}),                                                                                        # 3
    rpc("AllocateIds", {"keys": [K(("Auto", 0), ("Child", 0))]}),                                                            # 4 incomplete ancestor
    rpc("ReserveIds", {"keys": [K(("Auto", 1000))]}),                                                                        # 5
    rpc("ReserveIds", {"keys": [K(("Auto", 0))]}),                                                                           # 6 incomplete
    rpc("ReserveIds", {"keys": [K(("Auto", "named"))]}),                                                                     # 7 named key
    rpc("ReserveIds", {"keys": []}),                                                                                         # 8
    commit(ins(K(("Auto", 0)), a=I(1))),                                                                                     # 9 allocation continues
    rpc("AllocateIds", {"keys": [{"path": [{"kind": "Auto"}]}]}),                                                            # 10 element without id/name
])

# ------------------------------------------------------------------------------------------------------------------ 10. REST

case("rest", [
    RS("POST", ":commit", {"mode": "NON_TRANSACTIONAL", "mutations": [up(K(("R", "a")), n=I(1), s=S("x")), up(K(("R", "b")), n=I(2))]}),  # 0
    RS("POST", ":lookup", {"keys": [K(("R", "a")), K(("R", "zz"))]}),                                                        # 1
    RS("POST", ":runQuery", {"query": q("R", order_by=[order("n", "DESCENDING")])}),                                         # 2
    RS("POST", ":runQuery", {"query": q("R", where=pf("n", "GREATER_THAN", I(5)))}),                                         # 3
    RS("POST", ":beginTransaction", {}),                                                                                     # 4
    RS("POST", ":rollback", {"transaction": ref(4, "transaction")}),                                                         # 5
    RS("POST", ":allocateIds", {"keys": [K(("Auto", 0))]}),                                                                  # 6
    RS("POST", ":reserveIds", {"keys": [K(("Auto", 5000))]}),                                                                # 7
    RS("POST", ":runAggregationQuery", {"aggregationQuery": {"nestedQuery": q("R"), "aggregations": [COUNT("c")]}}),         # 8
    RS("POST", ":commit", {"mode": "NON_TRANSACTIONAL", "mutations": [ins(K(("R", "a")), n=I(9))]}),                        # 9 already exists
    RS("POST", ":lookup", {"keys": [{"path": [{"kind": "R"}]}]}),                                                            # 10 incomplete
    RS("POST", ":lookup", "not json"),                                                                                       # 11
    RS("POST", ":lookup", {"keys": [{"path": [{"kind": "R", "bogus": 1}]}]}),                                                # 12
    RS("GET", ":lookup"),                                                                                                    # 13
    RS("POST", ":nonsense", {}),                                                                                             # 14
])


# ------------------------------------------------------------------------------------------------------------------ 11. GQL

def gql(text, named=None, positional=None, literals=True):
    g = {"queryString": text, "allowLiterals": literals}
    if named:
        g["namedBindings"] = {k: ({"cursor": v["cursor"]} if isinstance(v, dict) and "cursor" in v else {"value": v}) for k, v in named.items()}
    if positional:
        g["positionalBindings"] = [{"value": v} for v in positional]
    return g


case("gql", [
    seed("P", PEOPLE),                                                                                                       # 0
    query(None, gql=gql("SELECT * FROM P")),                                                                                 # 1
    query(None, gql=gql("SELECT * FROM P WHERE age > 25 ORDER BY age LIMIT 3")),                                             # 2
    query(None, gql=gql("SELECT * FROM P WHERE age > @a ORDER BY age DESC LIMIT @n", named={"a": I(25), "n": I(2)}, literals=False)),  # 3
    query(None, gql=gql("SELECT * FROM P WHERE age > 25", literals=False)),                                                  # 4 disallowed literal
    query(None, gql=gql("SELECT * FROM P WHERE city = @1 AND age >= @2", positional=[S("Paris"), I(30)], literals=False)),   # 5
    query(None, gql=gql("SELECT __key__ FROM P WHERE city = 'Oslo'")),                                                       # 6
    query(None, gql=gql("SELECT name, age FROM P WHERE city = 'Paris' ORDER BY name")),                                      # 7
    query(None, gql=gql("SELECT * FROM P ORDER BY name DESC LIMIT 2 OFFSET 1")),                                             # 8
    query(None, gql=gql("SELECT * FROM P WHERE __key__ HAS ANCESTOR KEY(P, 'p1')")),                                         # 9
    query(None, gql=gql("SELECT * FROM P WHERE __key__ > KEY(P, 'p5')")),                                                    # 10
    query(None, gql=gql("SELECT * FROM P WHERE tags = 'a'")),                                                                # 11
    query(None, gql=gql("SELECT * FROM P WHERE name = 'Ann' AND age = 30")),                                                 # 12
    query(None, gql=gql("SELECT * FROM P WHERE age = NULL")),                                                                # 13
    query(None, gql=gql("SELECT * FROM P WHERE age = true")),                                                                # 14
    query(None, gql=gql("SELECT * FROM")),                                                                                   # 15 syntax error
    query(None, gql=gql("SELECT * FROM P WHERE")),                                                                           # 16
    query(None, gql=gql("SELECT * FROM P WHERE age > @missing", literals=False)),                                            # 17 unbound
    query(None, gql=gql("DELETE FROM P")),                                                                                   # 18
    query(None, gql=gql("SELECT * FROM P LIMIT 2")),                                                                         # 19
    query(None, gql=gql("SELECT * FROM P WHERE city = 'Paris' ORDER BY age LIMIT @lim OFFSET @off", named={"lim": I(1), "off": I(1)}, literals=False)),  # 20
    query(None, gql=gql("SELECT DISTINCT city FROM P ORDER BY city")),                                                       # 21
    query(None, gql=gql("SELECT DISTINCT ON (city) name, city FROM P ORDER BY city")),                                       # 22
    query(None, gql=gql("select * from P where age < 30 order by age asc")),                                                 # 23 lower case
    query(None, gql=gql("SELECT * FROM P WHERE age IN (25, 40)")),                                                           # 24
    query(None, gql=gql("SELECT * FROM P WHERE age != 25")),                                                                 # 25
    query(None, gql=gql("SELECT * FROM P WHERE age > 20 AND age < 31")),                                                     # 26
    query(None, gql=gql("SELECT * FROM `P` WHERE `age` > 26")),                                                              # 27 quoted names
    rpc("RunAggregationQuery", {"gqlQuery": gql("SELECT COUNT(*) FROM P")}),                                                 # 28
])
