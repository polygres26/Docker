"""The differential corpus: named cases, each a list of steps replayed in order against a fresh project on the oracle (the official
Firestore emulator) and on Warp. See fs_harness.py for the step format and normalisation."""
import base64

CASES = {}

# ------------------------------------------------------------------------------------------------------------------ value helpers


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


def BY(raw):
    return {"bytesValue": base64.b64encode(raw).decode()}


def REF(path):
    return {"referenceValue": "@DB@/documents/" + path}


def GEO(lat, lng):
    return {"geoPointValue": {"latitude": lat, "longitude": lng}}


def A(*vs):
    return {"arrayValue": {"values": list(vs)}} if vs else {"arrayValue": {}}


def M(**kv):
    return {"mapValue": {"fields": kv}} if kv else {"mapValue": {}}


def MD(d):
    return {"mapValue": {"fields": d}}


def VEC(*xs):
    return M(__type__=S("__vector__"), value=A(*[D(x) for x in xs]))


def nest(n):
    v = I(1)
    for _ in range(n):
        v = M(x=v)
    return v


def name(path):
    return "@DB@/documents/" + path


def doc(path, **fields):
    return {"name": name(path), "fields": fields}


# ------------------------------------------------------------------------------------------------------------------ step builders

def rpc(m, r=None, **kw):
    return {"m": m, "r": r if r is not None else {}, **kw}


def write_update(path, fields=None, mask=None, exists=None, update_time=None, transforms=None):
    w = {"update": {"name": name(path), "fields": fields or {}}}
    if mask is not None:
        w["updateMask"] = {"fieldPaths": mask}
    if exists is not None:
        w["currentDocument"] = {"exists": exists}
    if update_time is not None:
        w["currentDocument"] = {"updateTime": update_time}
    if transforms:
        w["updateTransforms"] = transforms
    return w


def write_delete(path, exists=None, update_time=None):
    w = {"delete": name(path)}
    if exists is not None:
        w["currentDocument"] = {"exists": exists}
    if update_time is not None:
        w["currentDocument"] = {"updateTime": update_time}
    return w


def commit(*writes, txn=None):
    r = {"database": "@DB@", "writes": list(writes)}
    if txn is not None:
        r["transaction"] = txn
    return rpc("Commit", r)


def setdoc(path, **fields):
    return commit(write_update(path, fields))


def get(path, mask=None, txn=None, read_time=None):
    r = {"name": name(path)}
    if mask is not None:
        r["mask"] = {"fieldPaths": mask}
    if txn is not None:
        r["transaction"] = txn
    if read_time is not None:
        r["readTime"] = read_time
    return rpc("GetDocument", r)


def seed(coll, docs):
    """One commit writing coll/<id> for every id -> fields."""
    return commit(*[write_update(f"{coll}/{i}", f) for i, f in docs.items()])


def field(f):
    return {"fieldPath": f}


def fil(f, op, v):
    return {"fieldFilter": {"field": field(f), "op": op, "value": v}}


def un(f, op):
    return {"unaryFilter": {"field": field(f), "op": op}}


def AND(*fs):
    return {"compositeFilter": {"op": "AND", "filters": list(fs)}}


def OR(*fs):
    return {"compositeFilter": {"op": "OR", "filters": list(fs)}}


def order(f, d="ASCENDING"):
    return {"field": field(f), "direction": d}


def cur(*vals, before=True):
    return {"values": list(vals), "before": before}


def sq(coll, where=None, order_by=None, start=None, end=None, limit=None, offset=None, select=None, group=False):
    if coll is not None:
        q = {"from": [{"collectionId": coll, "allDescendants": group}]}
    else:
        q = {"from": [{"allDescendants": True}] if group else []}
    if where is not None:
        q["where"] = where
    if order_by:
        q["orderBy"] = order_by
    if start is not None:
        q["startAt"] = start
    if end is not None:
        q["endAt"] = end
    if limit is not None:
        q["limit"] = limit
    if offset is not None:
        q["offset"] = offset
    if select is not None:
        q["select"] = {"fields": [field(f) for f in select]}
    return q


def query(q, parent="", txn=None, read_time=None, new_txn=None):
    r = {"parent": "@DB@/documents" + (("/" + parent) if parent else ""), "structuredQuery": q}
    if txn is not None:
        r["transaction"] = txn
    if read_time is not None:
        r["readTime"] = read_time
    if new_txn is not None:
        r["newTransaction"] = new_txn
    return rpc("RunQuery", r)


def agg(q, aggs, parent=""):
    return rpc("RunAggregationQuery", {"parent": "@DB@/documents" + (("/" + parent) if parent else ""),
                                       "structuredAggregationQuery": {"structuredQuery": q, "aggregations": aggs}})


def COUNT(alias=None, up_to=None):
    c = {"count": {}}
    if up_to is not None:
        c["count"]["upTo"] = str(up_to)
    if alias:
        c["alias"] = alias
    return c


def SUM(f, alias=None):
    c = {"sum": {"field": field(f)}}
    if alias:
        c["alias"] = alias
    return c


def AVG(f, alias=None):
    c = {"avg": {"field": field(f)}}
    if alias:
        c["alias"] = alias
    return c


def ref(step, path):
    return {"$ref": [step, path]}


def batchget(paths, mask=None, txn=None):
    r = {"database": "@DB@", "documents": [name(p) for p in paths]}
    if mask is not None:
        r["mask"] = {"fieldPaths": mask}
    if txn is not None:
        r["transaction"] = txn
    return rpc("BatchGetDocuments", r)


def begin(ro=False, read_time=None):
    if ro:
        return rpc("BeginTransaction", {"database": "@DB@", "options": {"readOnly": {"readTime": read_time} if read_time else {}}})
    return rpc("BeginTransaction", {"database": "@DB@", "options": {"readWrite": {}}})


def listdocs(parent, coll, **kw):
    r = {"parent": "@DB@/documents" + (("/" + parent) if parent else ""), "collectionId": coll}
    r.update(kw)
    return rpc("ListDocuments", r)


def colls(parent="", **kw):
    r = {"parent": "@DB@/documents" + (("/" + parent) if parent else "")}
    r.update(kw)
    return rpc("ListCollectionIds", r)


def RREST(method, path, body=None):
    """REST call with an absolute path below /v1/ ('@DB@/documents/...')."""
    return {"m": "REST", "method": method, "path": "/v1/" + path, "body": body}


def case(n, steps):
    CASES[n] = steps


# ------------------------------------------------------------------------------------------------------------------ 1. CRUD

case("crud", [
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "alice",
                           "document": {"fields": {"name": S("Alice"), "age": I(30)}}}),                                   # 0
    get("users/alice"),                                                                                                      # 1
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "alice",
                           "document": {"fields": {"name": S("dup")}}}),                                                     # 2 already exists
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "zed", "document": {"fields": {"x": I(1)}}}),  # 3
    get("users/nobody"),                                                                                                     # 4
    rpc("UpdateDocument", {"document": doc("users/alice", name=S("Alicia"), city=S("Paris")),
                           "updateMask": {"fieldPaths": ["name", "city"]}}),                                                 # 5
    get("users/alice"),                                                                                                      # 6
    rpc("UpdateDocument", {"document": doc("users/alice", only=S("x"))}),                                                    # 7 replace
    get("users/alice"),                                                                                                      # 8
    rpc("UpdateDocument", {"document": doc("users/bob", n=I(1)), "updateMask": {"fieldPaths": ["n"]}}),                      # 9 creates
    rpc("UpdateDocument", {"document": doc("users/carol", n=I(1)), "currentDocument": {"exists": True}}),                    # 10 not found
    rpc("UpdateDocument", {"document": doc("users/bob", n=I(2)), "currentDocument": {"exists": False}}),                     # 11 exists
    rpc("UpdateDocument", {"document": doc("users/bob", n=I(2)), "currentDocument": {"exists": True}}),                      # 12
    rpc("UpdateDocument", {"document": doc("users/bob", n=I(3)), "currentDocument": {"updateTime": ref(9, "updateTime")}}),  # 13 stale
    rpc("UpdateDocument", {"document": doc("users/bob", n=I(4)), "currentDocument": {"updateTime": ref(12, "updateTime")}}),  # 14 ok
    rpc("DeleteDocument", {"name": name("users/bob"), "currentDocument": {"updateTime": ref(9, "updateTime")}}),             # 15
    rpc("DeleteDocument", {"name": name("users/bob"), "currentDocument": {"exists": True}}),                                 # 16
    rpc("DeleteDocument", {"name": name("users/bob")}),                                                                      # 17 already gone
    rpc("DeleteDocument", {"name": name("users/bob"), "currentDocument": {"exists": True}}),                                 # 18
    listdocs("", "users"),                                                                                                   # 19
    listdocs("", "users", pageSize=1),                                                                                       # 20
    listdocs("", "users", pageSize=1, pageToken=ref(20, "nextPageToken")),                                                   # 21
    listdocs("", "users", mask={"fieldPaths": ["only"]}),                                                                    # 22
    get("users/alice", mask=["only"]),                                                                                       # 23
    get("users/alice", mask=["nope"]),                                                                                       # 24
    listdocs("", "nothing"),                                                                                                 # 25
    get("users"),                                                                                                            # 26 odd path
    get("users/alice/extra"),                                                                                                # 27
    rpc("GetDocument", {"name": "projects/x/databases/(default)/docs/users/alice"}),                                         # 28
    rpc("GetDocument", {"name": "nonsense"}),                                                                                # 29
    get("users/.."),                                                                                                         # 30
    get("users/__x__"),                                                                                                      # 31
    setdoc("noop/a", v=I(1), w=A(I(1))),                                                                                     # 32
    setdoc("noop/a", v=I(1), w=A(I(1))),                                                                                     # 33 identical rewrite
    rpc("UpdateDocument", {"document": doc("noop/a", v=I(2)), "currentDocument": {"updateTime": ref(32, "writeResults.0.updateTime")}}),  # 34 still at the first version?
    rpc("UpdateDocument", {"document": doc("noop/a", v=I(2)), "currentDocument": {"updateTime": ref(34, "updateTime")}}),   # 35 identical update keeps the time
    rpc("UpdateDocument", {"document": doc("noop/a", v=I(2)), "currentDocument": {"updateTime": ref(34, "updateTime")}}),   # 36
    rpc("ListDocuments", {"parent": "@DB@/documents", "collectionId": "users", "orderBy": "only desc"}),                     # 37
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "auto", "document": {"fields": {"x": I(1)}}}),        # 38 auto id
    listdocs("", "auto"),                                                                                                    # 39
    rpc("CreateDocument", {"parent": "@DB@/documents/users/alice", "collectionId": "sub", "documentId": "s1", "document": {"fields": {}}}),  # 40
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "a/b", "documentId": "s1", "document": {"fields": {}}}),  # 41 slash in collection id
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "a/b", "document": {"fields": {}}}),  # 42 slash in doc id
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "__id__", "document": {"fields": {}}}),  # 43 reserved
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "users", "documentId": "x" * 1501, "document": {"fields": {}}}),  # 44 too long
    rpc("CreateDocument", {"parent": "@DB@/documents", "collectionId": "", "documentId": "q", "document": {"fields": {}}}),  # 45 no collection
    rpc("CreateDocument", {"parent": "@DB@/documents/users", "collectionId": "sub", "documentId": "q", "document": {"fields": {}}}),  # 46 parent is a collection
])

# ------------------------------------------------------------------------------------------------------------------ 2. value types

TYPES = {
    "a_null": {"v": NULL}, "b_false": {"v": B(False)}, "c_true": {"v": B(True)}, "d_nan": {"v": NAN},
    "e_neg": {"v": I(-1)}, "f_zero": {"v": I(0)}, "g_dbl": {"v": D(1.5)}, "h_two": {"v": I(2)}, "h2_dbl": {"v": D(2.0)},
    "i_ts": {"v": T("2020-01-01T00:00:00Z")}, "j_ts": {"v": T("2021-06-01T12:30:00.123456Z")},
    "k_apple": {"v": S("apple")}, "l_banana": {"v": S("banana")}, "m_e": {"v": S("é")}, "m2_emoji": {"v": S("\U0001F600")},
    "n_by": {"v": BY(b"\x01\x02")}, "o_by": {"v": BY(b"\x01\x03")},
    "p_ref": {"v": REF("t/a_null")}, "q_ref": {"v": REF("t/b_false")},
    "r_geo": {"v": GEO(1, 2)}, "s_geo": {"v": GEO(1, 3)},
    "t_arr": {"v": A(I(1), I(2))}, "u_arr": {"v": A(I(1), I(3))}, "v_arr": {"v": A(I(1))},
    "w_map": {"v": M(a=I(1))}, "x_map": {"v": M(a=I(2))}, "y_map": {"v": M(b=I(1))},
    "z_nov": {"other": I(1)},
}
case("types_order", [
    seed("t", TYPES),                                                                                                        # 0
    query(sq("t", order_by=[order("v")])),                                                                                   # 1
    query(sq("t", order_by=[order("v", "DESCENDING")])),                                                                     # 2
    query(sq("t", where=fil("v", "GREATER_THAN", I(0)))),                                                                    # 3
    query(sq("t", where=fil("v", "LESS_THAN", I(2)))),                                                                       # 4
    query(sq("t", where=fil("v", "GREATER_THAN_OR_EQUAL", D(2.0)))),                                                         # 5
    query(sq("t", where=fil("v", "EQUAL", I(2)))),                                                                           # 6 int 2 and double 2.0
    query(sq("t", where=fil("v", "EQUAL", D(1.5)))),                                                                         # 7
    query(sq("t", where=fil("v", "LESS_THAN", S("b")))),                                                                     # 8
    query(sq("t", where=fil("v", "GREATER_THAN", S("apple")))),                                                              # 9
    query(sq("t", where=fil("v", "GREATER_THAN", T("2020-06-01T00:00:00Z")))),                                               # 10
    query(sq("t", where=fil("v", "LESS_THAN", BY(b"\x01\x03")))),                                                            # 11
    query(sq("t", where=fil("v", "GREATER_THAN", REF("t/a_null")))),                                                         # 12
    query(sq("t", where=fil("v", "GREATER_THAN", GEO(1, 2)))),                                                               # 13
    query(sq("t", where=fil("v", "GREATER_THAN", A(I(1))))),                                                                 # 14
    query(sq("t", where=fil("v", "GREATER_THAN", M(a=I(1))))),                                                               # 15
    query(sq("t", where=fil("v", "EQUAL", NULL))),                                                                           # 16
    query(sq("t", where=un("v", "IS_NULL"))),                                                                                # 17
    query(sq("t", where=un("v", "IS_NAN"))),                                                                                 # 18
    query(sq("t", where=un("v", "IS_NOT_NAN"))),                                                                             # 19
    query(sq("t", where=un("v", "IS_NOT_NULL"))),                                                                            # 20
    query(sq("t", where=fil("v", "EQUAL", NAN))),                                                                            # 21
    query(sq("t", where=fil("v", "NOT_EQUAL", I(2)))),                                                                       # 22
    query(sq("t", where=fil("v", "NOT_EQUAL", NULL))),                                                                       # 23
    query(sq("t", where=fil("v", "EQUAL", A(I(1), I(2))))),                                                                  # 24
    query(sq("t", where=fil("v", "EQUAL", M(a=I(1))))),                                                                      # 25
    query(sq("t", where=fil("v", "EQUAL", GEO(1, 2)))),                                                                      # 26
    query(sq("t", where=fil("v", "EQUAL", T("2020-01-01T00:00:00Z")))),                                                      # 27
    query(sq("t", where=fil("v", "EQUAL", BY(b"\x01\x02")))),                                                                # 28
    query(sq("t", where=fil("v", "EQUAL", REF("t/a_null")))),                                                                # 29
    query(sq("t", where=fil("v", "EQUAL", B(True)))),                                                                        # 30
    query(sq("t", order_by=[order("other")])),                                                                               # 31 field missing in most
    query(sq("t", where=fil("v", "LESS_THAN", NULL))),                                                                       # 32
    query(sq("t", where=fil("v", "GREATER_THAN", B(False)))),                                                                # 33
    query(sq("t", where=fil("v", "GREATER_THAN", NAN))),                                                                     # 34
])

case("types_special", [
    commit(write_update("s/vec", {"v": VEC(1.0, 2.0)}), write_update("s/vec2", {"v": VEC(1.0, 3.0)}),
           write_update("s/vec3", {"v": VEC(0.5)}), write_update("s/m1", {"v": M(a=I(1))}),
           write_update("s/arrmix", {"v": A(I(1), S("x"), NULL, M(q=A(I(2))))}),
           write_update("s/big", {"i": I(9223372036854775807), "n": I(-9223372036854775808), "d": D(1e300), "z": D(-0.0)}),
           write_update("s/uni", {"kéy": S("v"), "sp ace": I(1), "a": M(b=M(c=I(3)))})),                                # 0
    get("s/vec"), get("s/big"), get("s/uni"), get("s/arrmix"),                                                               # 1-4
    query(sq("s", order_by=[order("v")])),                                                                                   # 5
    query(sq("s", where=fil("v", "EQUAL", VEC(1.0, 2.0)))),                                                                  # 6
    query(sq("s", where=fil("a.b.c", "EQUAL", I(3)))),                                                                       # 7
    query(sq("s", where=fil("`sp ace`", "EQUAL", I(1)))),                                                                    # 8
    query(sq("s", where=fil("`kéy`", "EQUAL", S("v")))),                                                                # 9
    query(sq("s", where=fil("sp ace", "EQUAL", I(1)))),                                                                      # 10 invalid path
    query(sq("s", where=fil("i", "GREATER_THAN", D(9.223372036854775e18)))),                                                 # 11
    query(sq("s", where=fil("z", "EQUAL", D(0.0)))),                                                                         # 12
    query(sq("s", where=fil("v", "ARRAY_CONTAINS", NULL))),                                                                  # 13
    query(sq("s", where=fil("v", "ARRAY_CONTAINS", A(I(2))))),                                                               # 14
    setdoc("s/nested", v=A(A(I(1)))),                                                                                        # 15 nested array
    setdoc("s/nested2", v=M(a=A(A(I(1))))),                                                                                  # 16
])

# ------------------------------------------------------------------------------------------------------------------ 3. filters, ordering, cursors

PEOPLE = {
    "p1": {"name": S("Ann"), "age": I(30), "city": S("Paris"), "tags": A(S("a"), S("b")), "addr": M(zip=S("75001"))},
    "p2": {"name": S("Bob"), "age": I(25), "city": S("Rome"), "tags": A(S("b")), "addr": M(zip=S("00100"))},
    "p3": {"name": S("Cid"), "age": I(35), "city": S("Paris"), "tags": A(S("c")), "addr": M(zip=S("75002"))},
    "p4": {"name": S("Dee"), "age": I(25), "city": S("Oslo"), "tags": A(), "addr": M()},
    "p5": {"name": S("Eve"), "age": D(30.0), "city": NULL, "tags": A(S("a"), S("c"))},
    "p6": {"name": S("Fay"), "city": S("Rome")},
    "p7": {"name": S("Gus"), "age": I(40), "city": S("Paris"), "tags": A(S("d"))},
    "p8": {"name": S("Hal"), "age": I(22), "city": S("Oslo"), "tags": A(S("a"))},
}
case("filters", [
    seed("p", PEOPLE),                                                                                                       # 0
    query(sq("p", where=fil("city", "EQUAL", S("Paris")))),                                                                  # 1
    query(sq("p", where=fil("age", "GREATER_THAN", I(25)))),                                                                 # 2
    query(sq("p", where=fil("age", "LESS_THAN_OR_EQUAL", I(25)))),                                                           # 3
    query(sq("p", where=fil("age", "NOT_EQUAL", I(25)))),                                                                    # 4
    query(sq("p", where=fil("age", "IN", A(I(25), I(40))))),                                                                 # 5
    query(sq("p", where=fil("age", "NOT_IN", A(I(25), I(40))))),                                                             # 6
    query(sq("p", where=fil("tags", "ARRAY_CONTAINS", S("a")))),                                                             # 7
    query(sq("p", where=fil("tags", "ARRAY_CONTAINS_ANY", A(S("c"), S("d"))))),                                              # 8
    query(sq("p", where=AND(fil("city", "EQUAL", S("Paris")), fil("age", "GREATER_THAN", I(30))))),                          # 9
    query(sq("p", where=OR(fil("city", "EQUAL", S("Oslo")), fil("age", "GREATER_THAN", I(35))))),                            # 10
    query(sq("p", where=OR(AND(fil("city", "EQUAL", S("Paris")), fil("age", "LESS_THAN", I(40))), fil("name", "EQUAL", S("Bob"))))),  # 11
    query(sq("p", where=fil("addr.zip", "EQUAL", S("75001")))),                                                              # 12
    query(sq("p", where=un("city", "IS_NULL"))),                                                                             # 13
    query(sq("p", where=un("age", "IS_NOT_NULL"))),                                                                          # 14
    query(sq("p", where=fil("city", "IN", A(S("Rome"), NULL)))),                                                             # 15
    query(sq("p", where=fil("city", "NOT_IN", A(S("Rome"))))),                                                               # 16
    query(sq("p", where=fil("age", "IN", A()))),                                                                             # 17 empty IN
    query(sq("p", where=fil("age", "IN", I(1)))),                                                                            # 18 non-array
    query(sq("p", where=fil("tags", "ARRAY_CONTAINS_ANY", A()))),                                                            # 19
    query(sq("p", where=AND(fil("age", "GREATER_THAN", I(20)), fil("city", "GREATER_THAN", S("A"))))),                       # 20 two inequalities
    query(sq("p", where=AND(fil("age", "NOT_EQUAL", I(25)), fil("city", "NOT_IN", A(S("Rome")))))),                          # 21
    query(sq("p", where=AND(fil("tags", "ARRAY_CONTAINS", S("a")), fil("tags", "ARRAY_CONTAINS", S("b"))))),                 # 22
    query(sq("p", where=AND(fil("tags", "ARRAY_CONTAINS_ANY", A(S("a"))), fil("city", "IN", A(S("Paris")))))),               # 23
    query(sq("p", where=fil("__name__", "EQUAL", REF("p/p3")))),                                                             # 24
    query(sq("p", where=fil("__name__", "GREATER_THAN", REF("p/p5")))),                                                      # 25
    query(sq("p", where=fil("__name__", "IN", A(REF("p/p1"), REF("p/p8"))))),                                                # 26
    query(sq("p", where=fil("name", "EQUAL", I(1)))),                                                                        # 27
    query(sq("p", where={})),                                                                                                # 28 empty filter
    query(sq("p", where=AND())),                                                                                             # 29
    query(sq("p", where=fil("age", "OPERATOR_UNSPECIFIED", I(1)))),                                                          # 30
    query(sq("p", where=fil("age", "GREATER_THAN", I(20)), order_by=[order("name")])),                                       # 31 inequality then other order
    query(sq("p", where=fil("age", "GREATER_THAN", I(20)), order_by=[order("age", "DESCENDING"), order("name")])),           # 32
    query(sq("p", where=fil("city", "EQUAL", S("Paris")), order_by=[order("age")])),                                         # 33
    query(sq("p", where=fil("nonexistent", "EQUAL", I(1)))),                                                                 # 34
    query(sq("p", where=fil("name", "GREATER_THAN", S("Bob")), select=["name"])),                                            # 35
    query(sq("p", select=[])),                                                                                               # 36 empty projection
    query(sq("p", select=["addr.zip", "city"])),                                                                             # 37
    query(sq("p", where=OR(fil("age", "EQUAL", I(25)), fil("age", "EQUAL", I(30))), order_by=[order("age"), order("name", "DESCENDING")])),  # 38
    query(sq("p", where=OR(fil("age", "EQUAL", I(25)), fil("city", "EQUAL", S("Rome"))), order_by=[order("name", "DESCENDING")])),        # 39
    query(sq("p", where=AND(fil("city", "EQUAL", S("Paris")), OR(fil("age", "LESS_THAN", I(31)), fil("age", "GREATER_THAN", I(39)))))),   # 40
])

case("order_cursors", [
    seed("p", PEOPLE),                                                                                                       # 0
    query(sq("p", order_by=[order("age")])),                                                                                 # 1 docs without age excluded
    query(sq("p", order_by=[order("age", "DESCENDING")])),                                                                   # 2
    query(sq("p", order_by=[order("age"), order("name", "DESCENDING")])),                                                    # 3
    query(sq("p", order_by=[order("__name__", "DESCENDING")])),                                                              # 4
    query(sq("p")),                                                                                                          # 5 default name order
    query(sq("p", order_by=[order("age")], limit=3)),                                                                        # 6
    query(sq("p", order_by=[order("age")], limit=3, offset=2)),                                                              # 7
    query(sq("p", limit=0)),                                                                                                 # 8
    query(sq("p", offset=100)),                                                                                              # 9
    query(sq("p", order_by=[order("age")], start=cur(I(25)))),                                                               # 10 startAt
    query(sq("p", order_by=[order("age")], start=cur(I(25), before=False))),                                                 # 11 startAfter
    query(sq("p", order_by=[order("age")], end=cur(I(30)))),                                                                 # 12 endBefore
    query(sq("p", order_by=[order("age")], end=cur(I(30), before=False))),                                                   # 13 endAt
    query(sq("p", order_by=[order("age")], start=cur(I(25), before=False), end=cur(I(35), before=False))),                   # 14
    query(sq("p", order_by=[order("age"), order("name")], start=cur(I(25), S("Bob"), before=False))),                        # 15
    query(sq("p", order_by=[order("age", "DESCENDING")], start=cur(I(30), before=False))),                                   # 16
    query(sq("p", order_by=[order("age", "DESCENDING")], end=cur(I(30)))),                                                   # 17
    query(sq("p", order_by=[order("age")], start=cur(I(25), I(26), before=True))),                                           # 18 too many values
    query(sq("p", start=cur(REF("p/p3"), before=False))),                                                                    # 19 name cursor
    query(sq("p", start=cur(REF("p/p3")), end=cur(REF("p/p6")))),                                                            # 20
    query(sq("p", order_by=[order("__name__", "DESCENDING")], start=cur(REF("p/p6")))),                                      # 21
    query(sq("p", start=cur(S("p3")))),                                                                                      # 22 name cursor wrong type
    query(sq("p", order_by=[order("age")], start=cur(D(30.0)))),                                                             # 23 mixed numeric
    query(sq("p", order_by=[order("age")], start=cur(S("x")))),                                                              # 24 different type bracket
    query(sq("p", order_by=[order("age"), order("age")])),                                                                   # 25 duplicate order
    query(sq("p", order_by=[order("a..b")])),                                                                                # 26
    query(sq("p", limit=-1)),                                                                                                # 27
    query(sq("p", offset=-1)),                                                                                               # 28
    query(sq("p", order_by=[order("name", "DIRECTION_UNSPECIFIED")])),                                                       # 29
    query(sq("p", where=fil("age", "GREATER_THAN", I(22)), start=cur(I(30), before=False))),                                 # 30
    query(sq("p", order_by=[order("city"), order("age", "DESCENDING")], limit=4)),                                           # 31
])

# ------------------------------------------------------------------------------------------------------------------ 4. nesting, groups, collections

case("groups", [
    commit(write_update("c/a", {"n": I(1)}), write_update("c/b", {"n": I(2)}),
           write_update("c/a/c/x", {"n": I(3)}), write_update("c/a/c/y", {"n": I(4)}), write_update("c/b/c/z", {"n": I(5)}),
           write_update("c/a/d/q", {"n": I(6)}), write_update("d/r", {"n": I(7)}), write_update("e/f/g/h/c/deep", {"n": I(8)})),  # 0
    query(sq("c")),                                                                                                          # 1 top-level only
    query(sq("c", group=True)),                                                                                              # 2 collection group
    query(sq("c"), parent="c/a"),                                                                                            # 3 subcollection
    query(sq("c", group=True), parent="c/a"),                                                                                # 4 group below a doc
    query(sq("c", group=True, where=fil("n", "GREATER_THAN", I(3)), order_by=[order("n", "DESCENDING")])),                   # 5
    query(sq("c", group=True, order_by=[order("__name__", "DESCENDING")])),                                                  # 6
    query(sq(None, group=True)),                                                                                             # 7 no collection id, all descendants
    query(sq(None)),                                                                                                         # 8 no from
    colls(),                                                                                                                 # 9
    colls("c/a"),                                                                                                            # 10
    colls("c/zzz"),                                                                                                          # 11
    colls("e/f"),                                                                                                            # 12 (missing doc with subcollections)
    colls("", pageSize=2),                                                                                                   # 13
    colls("", pageSize=2, pageToken=ref(13, "nextPageToken")),                                                               # 14
    listdocs("", "e"),                                                                                                       # 15 e has only a missing doc
    listdocs("", "e", showMissing=True),                                                                                     # 16
    listdocs("e/f", "g", showMissing=True),                                                                                  # 17
    listdocs("c/a", "c"),                                                                                                    # 18
    listdocs("c", "c"),                                                                                                      # 19 bad parent
    colls("c"),                                                                                                              # 20
    agg(sq("c", group=True), [COUNT("n")]),                                                                                  # 21
    query(sq("c", order_by=[order("n", "DESCENDING")]), parent="c/a"),                                                       # 22
])

# ------------------------------------------------------------------------------------------------------------------ 5. transforms

TS = {"setToServerValue": "REQUEST_TIME"}


def ft(path, **kw):
    d = {"fieldPath": path}
    d.update(kw)
    return d


case("transforms", [
    setdoc("x/a", n=I(5), d=D(1.5), s=S("s"), arr=A(I(1), I(2), I(3)), m=M(k=I(1))),                                        # 0
    commit(write_update("x/a", transforms=[ft("n", increment=I(3))], mask=[])),                                              # 1 increment int
    get("x/a"),                                                                                                              # 2
    commit(write_update("x/a", transforms=[ft("n", increment=D(0.5)), ft("d", increment=I(2)), ft("new", increment=I(7)),
                                           ft("s", increment=I(1))], mask=[])),                                              # 3
    get("x/a"),                                                                                                              # 4
    commit(write_update("x/a", transforms=[ft("n", maximum=I(100)), ft("d", minimum=D(-1.0)), ft("mx", maximum=I(4)),
                                           ft("s", maximum=I(9))], mask=[])),                                                # 5
    get("x/a"),                                                                                                              # 6
    commit(write_update("x/a", transforms=[ft("n", maximum=D(100.0))], mask=[])),                                            # 7 equal mixed keeps stored
    commit(write_update("x/a", transforms=[ft("n", minimum=D(50.5))], mask=[])),                                             # 8
    get("x/a"),                                                                                                              # 9
    commit(write_update("x/a", transforms=[ft("arr", appendMissingElements={"values": [I(3), I(4), D(4.0), I(5)]})], mask=[])),  # 10
    get("x/a", mask=["arr"]),                                                                                                # 11
    commit(write_update("x/a", transforms=[ft("arr", removeAllFromArray={"values": [I(1), D(4.0)]})], mask=[])),             # 12
    get("x/a", mask=["arr"]),                                                                                                # 13
    commit(write_update("x/a", transforms=[ft("s", appendMissingElements={"values": [I(1)]}), ft("fresh", removeAllFromArray={"values": [I(1)]})], mask=[])),  # 14
    get("x/a", mask=["s", "fresh"]),                                                                                         # 15
    commit(write_update("x/a", transforms=[ft("ts", **TS), ft("m.when", **TS)], mask=[])),                                   # 16
    get("x/a", mask=["ts", "m"]),                                                                                            # 17
    commit(write_update("x/new", {"a": I(1)}, transforms=[ft("t", **TS), ft("c", increment=I(1))])),                        # 18 create with transform
    get("x/new"),                                                                                                            # 19
    commit(write_update("x/a", {"n": I(1)}, mask=["n"], transforms=[ft("n", increment=I(1))])),                              # 20 mask + transform same field
    commit(write_update("x/inc", transforms=[ft("n", increment=I(9223372036854775807))], mask=[]),
           write_update("x/inc", transforms=[ft("n", increment=I(5))], mask=[])),                                            # 21 overflow
    get("x/inc"),                                                                                                            # 22
    commit(write_update("x/a", transforms=[ft("n", increment=S("x"))], mask=[])),                                            # 23 bad operand
    commit(write_update("x/a", transforms=[ft("n", maximum=NULL)], mask=[])),                                                # 24
    commit(write_update("x/a", transforms=[ft("n", increment=I(1)), ft("n", increment=I(1))], mask=[])),                     # 25 same field twice
    commit(write_update("x/a", transforms=[ft("n", increment=I(1)), ft("n.sub", increment=I(1))], mask=[])),                 # 26 overlapping
    commit(write_update("x/a", transforms=[ft("", increment=I(1))], mask=[])),                                               # 27 empty path
    commit(write_update("x/a", transforms=[{"fieldPath": "n"}], mask=[])),                                                   # 28 no transform
    commit(write_update("x/a", transforms=[ft("n", setToServerValue="SERVER_VALUE_UNSPECIFIED")], mask=[])),                # 29
    commit(write_update("x/b", {"n": I(1)}), write_update("x/b", {"m": I(2)}, mask=["m"])),                                  # 30 two writes same doc
    get("x/b"),                                                                                                              # 31
    commit(write_update("x/c", {"a": I(1)}), write_update("x/c", exists=False)),                                              # 32
    commit(write_update("x/nan", {"v": D(1.0)}, transforms=[ft("v", maximum=NAN)])),                                        # 33
    get("x/nan"),                                                                                                            # 34
])

# ------------------------------------------------------------------------------------------------------------------ 6. commits & masks

case("commit_masks", [
    setdoc("m/a", a=I(1), b=M(c=I(2), d=I(3)), e=A(I(1))),                                                                   # 0
    commit(write_update("m/a", {"b": M(c=I(9))}, mask=["b.c"])),                                                             # 1 nested mask
    get("m/a"),                                                                                                              # 2
    commit(write_update("m/a", {}, mask=["b.d"])),                                                                           # 3 delete nested field
    get("m/a"),                                                                                                              # 4
    commit(write_update("m/a", {"z": I(1)}, mask=["a"])),                                                                    # 5 mask names absent field: deletes a
    get("m/a"),                                                                                                              # 6
    commit(write_update("m/a", {"a": I(1), "extra": I(5)}, mask=["a"])),                                                     # 7 body field not in mask ignored
    get("m/a"),                                                                                                              # 8
    commit(write_update("m/a", {"b": I(1)}, mask=["b.c"])),                                                                  # 9 mask below non-map
    commit(write_update("m/a", {"a": I(1)}, mask=["a", "a"])),                                                               # 10 duplicate mask
    commit(write_update("m/a", {"a": I(1)}, mask=["a.b", "a"])),                                                             # 11 overlapping
    commit(write_update("m/a", {"a": I(1)}, mask=["`a"])),                                                                   # 12
    commit(write_update("m/a", {"a": I(1)}, mask=[""])),                                                                     # 13
    commit(write_update("m/na", {"a": I(1)}, mask=["a", "q"])),                                                              # 14 create with mask
    get("m/na"),                                                                                                             # 15
    commit(write_update("m/a", exists=True), write_update("m/nb", {"a": I(1)}, exists=True)),                                # 16 atomic failure
    get("m/nb"),                                                                                                             # 17
    commit(write_update("m/a", {"a": I(5)}), write_delete("m/a"), write_update("m/a", {"b": I(6)})),                         # 18 sequence on one doc
    get("m/a"),                                                                                                              # 19
    commit(),                                                                                                                # 20 empty
    commit({}),                                                                                                              # 21 empty write
    commit(write_delete("m/a"), write_delete("m/a")),                                                                        # 22
    commit(write_update("m/z", {"a": I(1)}), write_update("m/z2", {"__bad__": I(1)})),                                       # 23 reserved field name
    setdoc("m/deep19", a=nest(18)), setdoc("m/deep20", a=nest(19)), setdoc("m/deep21", a=nest(20)), setdoc("m/deep30", a=nest(30)),  # 24-27 depth limit
    setdoc("m/rsv", **{"__x__": I(1)}),                                                                                      # 25
    setdoc("m/emp", **{"": I(1)}),                                                                                           # 26 empty field name
    setdoc("m/ok", **{"a b": I(1), "c.d": I(2), "`": I(3), "1x": I(4)}),                                                     # 27 odd field names
    get("m/ok"),                                                                                                             # 28
    setdoc("m/bt", **{"ts": T("2020-01-01T00:00:00.123456789Z"), "d": D(float("inf")) if False else D(1.0e-320)}),           # 29 nanos precision
    get("m/bt"),                                                                                                             # 30
    setdoc("m/ts0", ts=T("0001-01-01T00:00:00Z"), ts1=T("9999-12-31T23:59:59.999999Z")),                                    # 31
    setdoc("m/geobad", g=GEO(91, 0)),                                                                                        # 33
    get("m/ts0"),                                                                                                            # 34
    setdoc("m/big", s=S("x" * 1048000)),                                                                                     # 35 near limit
    setdoc("m/toobig", s=S("x" * 1048576)),                                                                                  # 36 over limit
    commit(write_update("m//x", {"a": I(1)})),                                                                               # 37 bad path
    commit(write_update("m", {"a": I(1)})),                                                                                  # 38 collection path
    commit(write_update("a/b/c", {"a": I(1)})),                                                                              # 39 odd segments
    commit({"update": {"name": "projects/other/databases/(default)/documents/a/b", "fields": {}}}),                          # 40 other project
])

# ------------------------------------------------------------------------------------------------------------------ 7. transactions

case("transactions", [
    seed("tx", {"a": {"n": I(1)}, "b": {"n": I(2)}}),                                                                        # 0
    begin(),                                                                                                                 # 1
    get("tx/a", txn=ref(1, "transaction")),                                                                                  # 2
    commit(write_update("tx/a", {"n": I(10)}), txn=ref(1, "transaction")),                                                   # 3
    get("tx/a"),                                                                                                             # 4
    commit(write_update("tx/a", {"n": I(11)}), txn=ref(1, "transaction")),                                                   # 5 reuse
    begin(),                                                                                                                 # 6
    rpc("Rollback", {"database": "@DB@", "transaction": ref(6, "transaction")}),                                             # 7
    commit(write_update("tx/a", {"n": I(12)}), txn=ref(6, "transaction")),                                                   # 8 after rollback
    rpc("Rollback", {"database": "@DB@", "transaction": ref(6, "transaction")}),                                             # 9 twice
    begin(ro=True),                                                                                                          # 15
    get("tx/a", txn=ref(15, "transaction")),                                                                                 # 16
    setdoc("tx/a", n=I(500)),                                                                                                # 17
    get("tx/a", txn=ref(15, "transaction")),                                                                                 # 18 snapshot read
    query(sq("tx", order_by=[order("n")]), txn=ref(15, "transaction")),                                                      # 19
    commit(write_update("tx/a", {"n": I(1)}), txn=ref(15, "transaction")),                                                   # 20 write in read-only
    get("tx/a", txn="AAAA"),                                                                                                 # 21 bogus txn
    commit(write_update("tx/a", {"n": I(1)}), txn="AAAA"),                                                                   # 22
    rpc("Rollback", {"database": "@DB@", "transaction": "AAAA"}),                                                             # 23
    begin(),                                                                                                                 # 24 empty commit
    commit(txn=ref(24, "transaction")),                                                                                      # 25
    query(sq("tx"), new_txn={"readOnly": {}}),                                                                                # 26 new_transaction
    rpc("BeginTransaction", {"database": "@DB@"}),                                                                           # 27 no options
    commit(write_update("tx/a", {"n": I(7)}), txn=ref(27, "transaction")),                                                   # 28
    rpc("BeginTransaction", {"database": "@DB@", "options": {"readWrite": {"retryTransaction": ref(1, "transaction")}}}),     # 29 retry
    get("tx/a", read_time=ref(4, "updateTime")),                                                                             # 30 read_time at an old version
    get("tx/a", read_time="2001-01-01T00:00:00Z"),                                                                           # 31 too old
    get("tx/a", read_time="2999-01-01T00:00:00Z"),                                                                           # 32 future
    query(sq("tx", order_by=[order("n")]), read_time=ref(4, "updateTime")),                                                  # 33
    batchget(["tx/a", "tx/none"], txn=ref(15, "transaction")),                                                               # 34
    rpc("BatchGetDocuments", {"database": "@DB@", "documents": [name("tx/a")], "newTransaction": {"readOnly": {}}}),         # 35
])

# ------------------------------------------------------------------------------------------------------------------ 8. aggregations

case("aggregations", [
    seed("p", PEOPLE),                                                                                                       # 0
    agg(sq("p"), [COUNT("c")]),                                                                                              # 1
    agg(sq("p"), [COUNT()]),                                                                                                 # 2 default alias
    agg(sq("p", where=fil("city", "EQUAL", S("Paris"))), [COUNT("c"), SUM("age", "s"), AVG("age", "a")]),                    # 3
    agg(sq("p"), [SUM("age", "s"), AVG("age", "a")]),                                                                        # 4 mixed int/double
    agg(sq("p"), [COUNT("c", up_to=3)]),                                                                                     # 5
    agg(sq("p"), [COUNT("c", up_to=100)]),                                                                                   # 6
    agg(sq("p", where=fil("city", "EQUAL", S("Nowhere"))), [COUNT("c"), SUM("age", "s"), AVG("age", "a")]),                  # 7 empty
    agg(sq("p"), [SUM("name", "s"), AVG("name", "a")]),                                                                      # 8 non numeric
    agg(sq("p", limit=3), [COUNT("c")]),                                                                                     # 9
    agg(sq("p", order_by=[order("age")], limit=2, offset=1), [SUM("age", "s")]),                                             # 10
    agg(sq("p"), []),                                                                                                        # 11 none
    agg(sq("p"), [COUNT("a"), COUNT("a")]),                                                                                  # 12 dup alias
    agg(sq("p"), [COUNT("a"), COUNT("b"), COUNT("c"), COUNT("d"), COUNT("e"), COUNT("f")]),                                  # 13 too many
    agg(sq("p"), [COUNT("c", up_to=-1)]),                                                                                    # 14
    agg(sq("p"), [SUM("addr.zip", "s"), SUM("nofield", "t")]),                                                               # 15
    agg(sq("p", where=fil("age", "GREATER_THAN", I(24))), [COUNT("c")]),                                                     # 16
    agg(sq("nothing"), [COUNT("c")]),                                                                                        # 17
    commit(write_update("q/a", {"v": I(9223372036854775807)}), write_update("q/b", {"v": I(1)})),                           # 18
    agg(sq("q"), [SUM("v", "s"), AVG("v", "a")]),                                                                            # 19 overflow
    agg(sq("p"), [COUNT("c"), {"alias": "z"}]),                                                                              # 20 no operator
])

# ------------------------------------------------------------------------------------------------------------------ 9. batchget / batchwrite

case("batch", [
    seed("b", {"a": {"n": I(1)}, "c": {"n": I(3)}}),                                                                         # 0
    batchget(["b/a", "b/x", "b/c"]),                                                                                         # 1
    batchget(["b/c", "b/a"], mask=["n"]),                                                                                    # 2
    batchget(["b/a", "b/a"]),                                                                                                # 3 duplicate
    batchget([]),                                                                                                            # 4
    batchget(["b"]),                                                                                                         # 5 bad name
    rpc("BatchWrite", {"database": "@DB@", "writes": [write_update("bw/a", {"n": I(1)}), write_update("bw/b", {"n": I(2)}, exists=True),
                                                       write_delete("b/a"), write_update("b/c", exists=False)]}),            # 6
    get("bw/a"), get("bw/b"), get("b/a"),                                                                                    # 7-9
    rpc("BatchWrite", {"database": "@DB@", "writes": [write_update("bw/a", {"n": I(1)}), write_update("bw/a", {"n": I(2)})]}),  # 10 duplicate
    rpc("BatchWrite", {"database": "@DB@", "writes": []}),                                                                   # 11
    rpc("BatchWrite", {"database": "@DB@", "writes": [{}]}),                                                                 # 12
    rpc("PartitionQuery", {"parent": "@DB@/documents", "structuredQuery": sq("b", group=True), "partitionCount": "3"}),      # 13
    rpc("PartitionQuery", {"parent": "@DB@/documents", "structuredQuery": sq("b"), "partitionCount": "3"}),                  # 14 not a group
    rpc("PartitionQuery", {"parent": "@DB@/documents", "structuredQuery": sq("b", group=True), "partitionCount": "-1"}),     # 15
])

# ------------------------------------------------------------------------------------------------------------------ 10. REST

case("rest", [
    RREST("PATCH", "@DB@/documents/r/a", {"fields": {"n": I(1), "s": S("x")}}),                                              # 0
    RREST("GET", "@DB@/documents/r/a"),                                                                                      # 1
    RREST("GET", "@DB@/documents/r/none"),                                                                                   # 2
    RREST("POST", "@DB@/documents/r?documentId=b", {"fields": {"n": I(2)}}),                                                 # 3
    RREST("POST", "@DB@/documents/r?documentId=b", {"fields": {"n": I(2)}}),                                                 # 4 exists
    RREST("GET", "@DB@/documents/r"),                                                                                        # 5 list
    RREST("GET", "@DB@/documents/r?pageSize=1"),                                                                             # 6
    RREST("PATCH", "@DB@/documents/r/a?updateMask.fieldPaths=n&currentDocument.exists=true", {"fields": {"n": I(5)}}),       # 7
    RREST("PATCH", "@DB@/documents/r/zz?currentDocument.exists=true", {"fields": {"n": I(5)}}),                              # 8
    RREST("POST", "@DB@/documents:commit", {"writes": [{"update": {"name": name("r/c"), "fields": {"n": I(3)}},
                                                        "updateTransforms": [{"fieldPath": "n", "increment": I(4)}]}]}),     # 9
    RREST("POST", "@DB@/documents:batchGet", {"documents": [name("r/a"), name("r/none")]}),                                  # 10
    RREST("POST", "@DB@/documents:runQuery", {"structuredQuery": sq("r", order_by=[order("n", "DESCENDING")])}),              # 11
    RREST("POST", "@DB@/documents:runQuery", {"structuredQuery": sq("nothing")}),                                            # 12
    RREST("POST", "@DB@/documents:runAggregationQuery", {"structuredAggregationQuery": {"structuredQuery": sq("r"), "aggregations": [COUNT("c")]}}),  # 13
    RREST("POST", "@DB@/documents:beginTransaction", {}),                                                                    # 14
    RREST("POST", "@DB@/documents:rollback", {"transaction": ref(14, "transaction")}),                                       # 15
    RREST("POST", "@DB@/documents:listCollectionIds", {}),                                                                   # 16
    RREST("DELETE", "@DB@/documents/r/a"),                                                                                   # 17
    RREST("DELETE", "@DB@/documents/r/a?currentDocument.exists=true"),                                                       # 18
    RREST("GET", "@DB@/documents/r/b?mask.fieldPaths=n"),                                                                    # 19
    RREST("POST", "@DB@/documents:commit", {"writes": [{"update": {"name": name("r/d"), "fields": {"n": {"bogus": 1}}}}]}),   # 20 bad json
    RREST("POST", "@DB@/documents:commit", "not json"),                                                                      # 21
    RREST("GET", "@DB@/documents/r/b?readTime=2001-01-01T00:00:00Z"),                                                        # 22
    RREST("POST", "@DB@/documents:batchWrite", {"writes": [{"delete": name("r/b")}]}),                                       # 23
])

# ------------------------------------------------------------------------------------------------------------------ 11. more query edge cases

case("query_edges", [
    seed("q", {"a": {"v": I(1)}, "b": {"v": NULL}, "c": {"v": NAN}, "d": {"v": S("x")}, "e": {"w": I(1)}}),                 # 0
    query(sq("q", where=fil("v", "NOT_IN", A(I(1), NULL)))),                                                                 # 1
    query(sq("q", where=fil("v", "NOT_EQUAL", NAN))),                                                                        # 2
    query(sq("q", where=fil("v", "NOT_IN", A(NAN)))),                                                                        # 3
    query(sq("q", where=fil("v", "IN", A(NAN)))),                                                                            # 4
    query(sq("q", where=fil("v", "IN", A(I(1), NAN, NULL)))),                                                                # 5
    query(sq("q", where=un("v", "IS_NOT_NAN"))),                                                                             # 6
    query(sq("q", offset=5)),                                                                                                # 7
    query(sq("q", offset=4)),                                                                                                # 8
    query(sq("q", offset=3, limit=5)),                                                                                       # 9
    query(sq("q", offset=1, limit=0)),                                                                                       # 10
    query(sq("q", offset=5, limit=2)),                                                                                       # 11
    query(sq("q", offset=2, limit=3)),                                                                                       # 12 offset == remaining boundary
    query(sq("q", order_by=[order("v")], start=cur(I(1)), end=cur(I(1)))),                                                   # 13
    query(sq("q", order_by=[order("__name__")], start=cur(REF("q/b"), before=False))),                                       # 14
    query(sq("q", order_by=[order("v"), order("__name__")], start=cur(I(1), REF("q/a"), before=False))),                     # 15
    query(sq("q", order_by=[order("v"), order("__name__")], start=cur(I(1), S("a"), before=False))),                         # 16
    query(sq("q", order_by=[order("__name__")], start=cur(REF("zz/b")))),                                                    # 17
    query(sq("q", order_by=[order("v")], start=cur(NULL))),                                                                  # 18 null cursor
    query(sq("q", order_by=[order("v")], start=cur(NAN), end=cur(I(1), before=False))),                                      # 19 NaN cursor
    query(sq("q", where=OR(fil("v", "EQUAL", I(1)), fil("w", "EQUAL", I(1))))),                                              # 20 or across fields
    query(sq("q", where=OR(fil("v", "EQUAL", I(1)), un("v", "IS_NAN"))), parent=""),                                         # 21
    query(sq("q", where=fil("v", "GREATER_THAN", I(0))), read_time="2001-01-01T00:00:00Z"),                                  # 22
    query(sq("q", select=["v"], order_by=[order("v", "DESCENDING")])),                                                       # 23
    query(sq("q", select=["__name__"])),                                                                                     # 24
    query(sq("q", group=True, where=fil("__name__", "GREATER_THAN", REF("q/b")))),                                           # 25
    query({"from": [{"collectionId": "q"}, {"collectionId": "r"}]}),                                                         # 26 two from
    query({}),                                                                                                               # 27 empty query
    query({"from": [{"collectionId": "q"}], "findNearest": {"vectorField": field("v"), "queryVector": VEC(1.0), "distanceMeasure": "EUCLIDEAN", "limit": 2}}),  # 28
    rpc("RunQuery", {"parent": "@DB@/documents"}),                                                                           # 29 no query
    rpc("RunQuery", {"parent": "@DB@/documents/q"}),                                                                         # 30 collection parent
    rpc("RunQuery", {"parent": "projects/x", "structuredQuery": sq("q")}),                                                   # 31
    rpc("RunQuery", {"parent": "", "structuredQuery": sq("q")}),                                                             # 32
])

# ------------------------------------------------------------------------------------------------------------------ 12. streams

def LISTEN(script):
    return {"m": "LISTEN", "script": script}


def WSTREAM(script):
    return {"m": "WRITESTREAM", "script": script}


def qtarget(tid, q, **kw):
    d = {"database": "@DB@", "addTarget": {"query": {"parent": "@DB@/documents", "structuredQuery": q}, "targetId": tid}}
    d["addTarget"].update(kw)
    return d


def dtarget(tid, docs, **kw):
    d = {"database": "@DB@", "addTarget": {"documents": {"documents": [name(x) for x in docs]}, "targetId": tid}}
    d["addTarget"].update(kw)
    return d


def untarget(tid):
    return {"database": "@DB@", "removeTarget": tid}


# Listen: the INITIAL snapshot protocol is identical to the emulator's; what follows a change differs on purpose (the emulator re-sends
# the whole result after a RESET, real Firestore -- and Warp -- send incremental DocumentChange/DocumentDelete/DocumentRemove), so the
# change feed is tested against Warp directly in test_firestore_conformance.py
case("listen", [
    seed("l", {"a": {"n": I(1)}, "b": {"n": I(2)}, "c": {"n": I(3)}}),                                                       # 0
    LISTEN([{"send": qtarget(1, sq("l", order_by=[order("n")]))}, {"recv": 6, "timeout": 8}]),                               # 1
    LISTEN([{"send": qtarget(2, sq("l", where=fil("n", "GREATER_THAN", I(1)), order_by=[order("n", "DESCENDING")]))}, {"recv": 5, "timeout": 8}]),  # 2
    LISTEN([{"send": dtarget(3, ["l/a", "l/zz", "l/c"])}, {"recv": 6, "timeout": 8}]),                                       # 3 documents target incl. a missing one
    LISTEN([{"send": qtarget(4, sq("nothing"))}, {"recv": 4, "timeout": 8}, {"send": untarget(4)}, {"recv": 1, "timeout": 3}]),  # 4 empty + remove
    LISTEN([{"send": qtarget(5, sq("l", where=fil("n", "IN", A())))}, {"recv": 1, "timeout": 5}]),                          # 5 invalid query
    LISTEN([{"send": qtarget(6, sq("l", where=fil("n", "EQUAL", I(1))))}, {"recv": 4, "timeout": 8},
            {"send": qtarget(7, sq("l", where=fil("n", "EQUAL", I(2))))}, {"recv": 4, "timeout": 8}]),                       # 6 two targets
    LISTEN([{"send": qtarget(0, sq("l", limit=2))}, {"recv": 5, "timeout": 8}]),                                             # 7 server-assigned target id
    LISTEN([{"send": qtarget(8, sq("l", group=True))}, {"recv": 6, "timeout": 8}]),                                          # 8 collection group
    LISTEN([{"send": qtarget(9, sq("l"), once=True)}, {"recv": 6, "timeout": 8}]),                                           # 9 once
])

case("write_stream", [
    WSTREAM([{"database": "@DB@"},
             {"streamToken": "$last", "writes": [write_update("w/a", {"n": I(1)})]},
             {"streamToken": "$last", "writes": [write_update("w/b", {"n": I(2)}), write_update("w/c", {"n": I(3)}, transforms=[{"fieldPath": "t", "setToServerValue": "REQUEST_TIME"}])]}]),  # 0
    get("w/a"), get("w/b"), get("w/c"),                                                                                      # 1-3
    WSTREAM([{"database": "@DB@"}, {"streamToken": "$last", "writes": [write_update("w/d", {"n": I(1)}), write_update("w/zz", {"n": I(1)}, exists=True)]}]),  # 4 failed write closes the stream
    get("w/d"),                                                                                                              # 5 atomic: not written
    WSTREAM([{"database": "@DB@", "writes": [write_update("w/e", {"n": I(1)})]}]),                                           # 6 writes on the handshake
    WSTREAM([{"database": "@DB@"}, {"streamToken": "$last", "writes": [{}]}]),                                               # 7 empty write
    WSTREAM([{"database": "@DB@"}, {"streamToken": "$last", "writes": [write_delete("w/a")]}]),                              # 8
    get("w/a"),                                                                                                              # 9
])
