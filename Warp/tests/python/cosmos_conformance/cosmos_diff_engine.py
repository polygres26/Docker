"""An INDEPENDENT Python implementation of the Cosmos SQL semantics used for randomized differential testing of Warp's (Java) engine:
undefined is the UNDEF sentinel, three-valued logic, type ordering undefined < null < boolean < number < string < array < object.
Written separately from the Java code but by the same author from the same documentation: it finds implementation bugs and
divergences between two codings, NOT misreadings of the documentation shared by both."""
import json
import random

UNDEF = object()


def rank(v):
    if v is UNDEF:
        return 0
    if v is None:
        return 1
    if isinstance(v, bool):
        return 2
    if isinstance(v, (int, float)):
        return 3
    if isinstance(v, str):
        return 4
    if isinstance(v, list):
        return 5
    return 6


def sort_key(v):
    r = rank(v)
    if r == 2:
        return (r, int(v))
    if r == 3:
        return (r, float(v))
    if r == 4:
        return (r, [ord(c) for c in v])
    if r in (5, 6):
        return (r, json.dumps(v, sort_keys=True))
    return (r, 0)


def eq(a, b):
    if a is UNDEF or b is UNDEF:
        return UNDEF
    return rank(a) == rank(b) and (sort_key(a) == sort_key(b) if rank(a) < 5 else json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True))


def cmp_op(op, a, b):
    if a is UNDEF or b is UNDEF or rank(a) != rank(b) or rank(a) > 4:
        return UNDEF
    ka, kb = sort_key(a), sort_key(b)
    return {"<": ka < kb, "<=": ka <= kb, ">": ka > kb, ">=": ka >= kb}[op]


def t_and(a, b):
    if a is False or b is False:
        return False
    if a is True and b is True:
        return True
    return UNDEF


def t_or(a, b):
    if a is True or b is True:
        return True
    if a is False and b is False:
        return False
    return UNDEF


def t_not(a):
    return (not a) if isinstance(a, bool) else UNDEF


def get(doc, path):
    cur = doc
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return UNDEF
        cur = cur[p]
    return cur


# ---- random data and predicate trees -----------------------------------------------------------------------------------

def rand_doc(rnd, i):
    d = {"id": "d%04d" % i, "pk": "p%d" % rnd.randrange(6)}
    if rnd.random() < 0.85:
        d["n"] = rnd.choice([rnd.randrange(-5, 15), rnd.randrange(-5, 15), rnd.random() * 10, None, "7", True, False])
    if rnd.random() < 0.8:
        d["s"] = rnd.choice(["apple", "apricot", "banana", "Berry", "cherry", "", "app"])
    if rnd.random() < 0.7:
        d["g"] = rnd.choice([0, 1, 2, 3, "1", None])
    if rnd.random() < 0.6:
        d["tags"] = [rnd.choice([1, 2, 3, "a", "b"]) for _ in range(rnd.randrange(0, 4))]
    if rnd.random() < 0.5:
        d["o"] = {"x": rnd.randrange(0, 10)} if rnd.random() < 0.8 else {}
    return d


def rand_atom(rnd):
    k = rnd.randrange(10)
    n = rnd.randrange(-3, 12)
    if k == 0:
        return ("c.n > %d" % n, lambda d: cmp_op(">", get(d, ["n"]), n))
    if k == 1:
        return ("c.n = %d" % n, lambda d: eq(get(d, ["n"]), n))
    if k == 2:
        vals = [rnd.randrange(0, 4) for _ in range(3)]
        return ("c.g IN (%s)" % ", ".join(map(str, vals)), lambda d: UNDEF if get(d, ["g"]) is UNDEF else any(eq(get(d, ["g"]), v) is True for v in vals))
    if k == 3:
        s = rnd.choice(["apple", "banana", "cherry", "app"])
        return ("c.s = '%s'" % s, lambda d: eq(get(d, ["s"]), s))
    if k == 4:
        p = rnd.choice(["a", "ap", "b", "c"])
        return ("STARTSWITH(c.s, '%s')" % p, lambda d: get(d, ["s"]).startswith(p) if isinstance(get(d, ["s"]), str) else UNDEF)
    if k == 5:
        return ("IS_DEFINED(c.n)", lambda d: get(d, ["n"]) is not UNDEF)
    if k == 6:
        return ("IS_NUMBER(c.n)", lambda d: rank(get(d, ["n"])) == 3)
    if k == 7:
        lo, hi = sorted([n, rnd.randrange(-3, 12)])
        return ("c.n BETWEEN %d AND %d" % (lo, hi), lambda d: t_and(cmp_op(">=", get(d, ["n"]), lo), cmp_op("<=", get(d, ["n"]), hi)))
    if k == 8:
        t = rnd.choice([1, 2, "a"])
        tj = json.dumps(t)
        return ("ARRAY_CONTAINS(c.tags, %s)" % tj, lambda d: UNDEF if not isinstance(get(d, ["tags"]), list) else any(eq(x, t) is True for x in get(d, ["tags"])))
    return ("c.o.x < %d" % n, lambda d: cmp_op("<", get(d, ["o", "x"]), n))


def rand_pred(rnd, depth=0):
    if depth > 2 or rnd.random() < 0.4:
        return rand_atom(rnd)
    k = rnd.randrange(3)
    a, b = rand_pred(rnd, depth + 1), rand_pred(rnd, depth + 1)
    if k == 0:
        return ("(%s AND %s)" % (a[0], b[0]), lambda d: t_and(a[1](d), b[1](d)))
    if k == 1:
        return ("(%s OR %s)" % (a[0], b[0]), lambda d: t_or(a[1](d), b[1](d)))
    return ("NOT (%s)" % a[0], lambda d: t_not(a[1](d)))


def expected(docs, pred, shape, rnd):
    """Returns (sql tail after SELECT, expected python result, ordered?)."""
    sql_where, f = pred
    sel = [d for d in docs if f(d) is True]
    if shape == "ids":
        return "VALUE c.id FROM c WHERE %s" % sql_where, sorted(d["id"] for d in sel), False
    if shape == "order":
        rev = rnd.random() < 0.5
        out = sorted(sel, key=lambda d: (sort_key(get(d, ["n"])), d["id"]) if not rev else 0)
        if rev:
            out = sorted(sel, key=lambda d: d["id"])
            out = sorted(out, key=lambda d: sort_key(get(d, ["n"])), reverse=True)
        return "VALUE c.id FROM c WHERE %s ORDER BY c.n %s, c.id %s" % (sql_where, "DESC" if rev else "ASC", "ASC"), [d["id"] for d in out], True
    if shape == "top":
        t = rnd.randrange(1, 6)
        out = sorted(sel, key=lambda d: d["id"])[:t]
        return "TOP %d VALUE c.id FROM c WHERE %s ORDER BY c.id" % (t, sql_where), [d["id"] for d in out], True
    if shape == "offset":
        o, l = rnd.randrange(0, 4), rnd.randrange(1, 5)
        out = sorted(sel, key=lambda d: d["id"])[o:o + l]
        return "VALUE c.id FROM c WHERE %s ORDER BY c.id OFFSET %d LIMIT %d" % (sql_where, o, l), [d["id"] for d in out], True
    if shape == "count":
        return "VALUE COUNT(1) FROM c WHERE %s" % sql_where, [len(sel)], True
    if shape == "agg":
        nums = [get(d, ["n"]) for d in sel if rank(get(d, ["n"])) == 3]
        allv = [get(d, ["n"]) for d in sel if get(d, ["n"]) is not UNDEF]
        res = {"cnt": len(allv)}
        if allv and len(nums) == len(allv):
            res["s"] = sum(nums)
            res["a"] = sum(nums) / len(nums)
        if allv:
            res["mn"] = min(allv, key=sort_key)
            res["mx"] = max(allv, key=sort_key)
        return ("COUNT(c.n) AS cnt, SUM(c.n) AS s, AVG(c.n) AS a, MIN(c.n) AS mn, MAX(c.n) AS mx FROM c WHERE %s" % sql_where), [res], True
    if shape == "distinct":
        seen = []
        for d in sel:
            v = get(d, ["g"])
            if v is not UNDEF and not any(json.dumps(v) == json.dumps(x) and rank(v) == rank(x) for x in seen):
                seen.append(v)
        return "DISTINCT VALUE c.g FROM c WHERE %s" % sql_where, sorted(seen, key=sort_key), False
    if shape == "group":
        groups = {}
        for d in sel:
            g = get(d, ["g"])
            k = "undef" if g is UNDEF else json.dumps(g)
            groups.setdefault(k, []).append(d)
        out = []
        for k, ds in groups.items():
            g = get(ds[0], ["g"])
            row = {"c": len(ds)}
            if g is not UNDEF:
                row["g"] = g
            out.append(row)
        return "c.g, COUNT(1) AS c FROM c WHERE %s GROUP BY c.g" % sql_where, out, False
    raise ValueError(shape)


SHAPES = ["ids", "order", "top", "offset", "count", "agg", "distinct", "group"]


def norm(v):
    """Comparable form: ints and floats that are equal compare equal; rows sorted for unordered comparisons."""
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, (int, float)):
        return round(float(v), 9)
    if isinstance(v, list):
        return [norm(x) for x in v]
    return {k: norm(x) for k, x in v.items()}
