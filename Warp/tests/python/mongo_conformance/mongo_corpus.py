"""Differential corpus for Warp's MongoDB frontend: the same pymongo operation sequences are run against a real
mongod (the oracle) and against Warp; every step's answer (result shape, type-exact documents, or the error
type/code/codeName/message) is normalised into a JSON tree and compared.

A case is `@case("name") def _(e): ...` using the Env helpers (op / cmd / find / agg / state). Each case gets a
fresh database. The corpus is split over mongo_corpus_*.py modules that register into CASES.
"""
import copy
import datetime
import random
import re

import bson
from bson import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import errors as pe

from mongo_typed import OidMap, mask as tmask, tdecode, tpy

CASES = {}
RAW = CodecOptions(document_class=RawBSONDocument)
DROP_REPLY_KEYS = {"$clusterTime", "operationTime", "$configTime", "$topologyTime", "electionId", "$replData", "$gleStats"}


def case(name):
    def deco(fn):
        assert name not in CASES, name
        CASES[name] = fn
        return fn
    return deco


def norm_msg(m):
    if m is None:
        return None
    m = re.sub(r"[0-9a-f]{24}", "<oid>", str(m))
    m = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>", m)
    m = re.sub(r"wc\d+_[0-9a-z]+", "<db>", m)
    m = re.sub(r"cursor id -?\d+", "cursor id <n>", m)
    return m


def err_outcome(ex):
    out = {"type": type(ex).__name__}
    if isinstance(ex, pe.BulkWriteError):
        d = ex.details or {}
        we = []
        for w in d.get("writeErrors", []):
            item = {"index": w.get("index"), "code": w.get("code"), "msg": norm_msg(w.get("errmsg"))}
            for k in ("keyPattern", "keyValue", "errInfo"):
                if k in w:
                    item[k] = tpy(w[k])
            we.append(item)
        out.update({"code": 65, "writeErrors": we, "nInserted": d.get("nInserted"), "nMatched": d.get("nMatched"),
                    "nModified": d.get("nModified"), "nRemoved": d.get("nRemoved"), "nUpserted": d.get("nUpserted"),
                    "upserted": tpy(d.get("upserted", [])),
                    "writeConcernErrors": [{"code": w.get("code")} for w in d.get("writeConcernErrors", [])]})
        return out
    if isinstance(ex, pe.OperationFailure):
        out["code"] = ex.code
        det = ex.details or {}
        out["codeName"] = det.get("codeName")
        out["msg"] = norm_msg(det.get("errmsg") or str(ex))
        for k in ("keyPattern", "keyValue"):
            if k in det:
                out[k] = tpy(det[k])
        return out
    if isinstance(ex, pe.PyMongoError):
        out["msg"] = norm_msg(str(ex))
        return out
    out["msg"] = norm_msg(str(ex))
    return out


class Env:
    def __init__(self, client, dbname):
        self.client = client
        self.dbname = dbname
        self.db = client[dbname]
        self.steps = []
        self.oids = OidMap()
        self.replies = {}

    # -- collections
    def coll(self, name="c"):
        return self.db[name]

    @property
    def c(self):
        return self.db["c"]

    def rawc(self, coll):
        return coll.with_options(codec_options=RAW)

    # -- recording
    def _rec(self, label, outcome, nat=False, masks=(), un=False):
        if "ok" in outcome:
            v = outcome["ok"]
            if masks:
                v = tmask(v, masks)
            outcome = {"ok": self.oids.apply(v)}
        else:
            e = outcome["err"]
            for k in ("keyValue", "keyPattern", "writeErrors", "upserted"):
                if k in e:
                    e[k] = self.oids.apply(e[k])
        self.steps.append({"l": label, "o": outcome, "nat": nat, "un": un})

    def _run(self, label, fn, conv, nat=False, masks=(), un=False):
        try:
            v = conv(fn())
            self._rec(label, {"ok": v}, nat, masks, un)
        except Exception as ex:  # noqa: BLE001 - every failure is an answer to compare
            self._rec(label, {"err": err_outcome(ex)}, nat, masks, un)

    @staticmethod
    def conv(v):
        from pymongo import results as r
        if isinstance(v, r.InsertOneResult):
            return {"inserted_id": tpy(v.inserted_id)}
        if isinstance(v, r.InsertManyResult):
            return {"inserted_ids": tpy(v.inserted_ids)}
        if isinstance(v, r.UpdateResult):
            return {"matched": v.matched_count, "modified": v.modified_count, "upserted_id": tpy(v.upserted_id)}
        if isinstance(v, r.DeleteResult):
            return {"deleted": v.deleted_count}
        if isinstance(v, r.BulkWriteResult):
            d = v.bulk_api_result
            return {k: tpy(x) for k, x in d.items() if k not in ("writeErrors", "writeConcernErrors")}
        if isinstance(v, (list, tuple)) and v and isinstance(v[0], RawBSONDocument):
            return [tdecode(x.raw) for x in v]
        if isinstance(v, RawBSONDocument):
            return tdecode(v.raw)
        return tpy(v)

    def op(self, label, fn, nat=False, mask=()):
        self._run(label, fn, self.conv, nat, mask)

    def cmd(self, label, doc, db=None, nat=False, mask=()):
        target = self.client[db or self.dbname]

        def run():
            r = target.command(doc if not isinstance(doc, dict) else doc, codec_options=RAW)
            self.replies[label] = r
            return {k: v for k, v in tdecode(r.raw).items() if k not in DROP_REPLY_KEYS}
        self._run(label, run, lambda x: self._normalize_cursor(x), nat, mask)

    @staticmethod
    def _normalize_cursor(t):
        # cursor ids are server-chosen: keep only "zero / non-zero"
        if isinstance(t, dict) and isinstance(t.get("cursor"), dict) and "id" in t["cursor"]:
            cid = t["cursor"]["id"]
            v = int(cid["$long"]) if isinstance(cid, dict) else cid
            t = dict(t)
            t["cursor"] = dict(t["cursor"])
            t["cursor"]["id"] = "<nonzero>" if v else 0
        if isinstance(t, dict) and "cursorsNotFound" in t:
            t = dict(t)
            for k in ("cursorsKilled", "cursorsNotFound", "cursorsAlive", "cursorsUnknown"):
                if k in t:
                    t[k] = ["<cursor>" for _ in t[k]]
        return t

    def find(self, label, coll, flt=None, mask=(), ordered=None, **kw):
        nat = ("sort" not in kw) if ordered is None else not ordered
        self._run(label, lambda: [tdecode(d.raw) for d in self.rawc(coll).find(flt, **kw)], lambda x: x, nat, mask)

    def agg(self, label, coll, pipeline, ordered=False, mask=(), **kw):
        self._run(label, lambda: [tdecode(d.raw) for d in self.rawc(coll).aggregate(pipeline, **kw)], lambda x: x, not ordered, mask, not ordered)

    def state(self, label, coll=None, mask=()):
        coll = coll if coll is not None else self.c
        self.find(label, coll, {}, mask=mask, sort=[("_id", 1)])

    def insert(self, label, coll, docs, **kw):
        self.op(label, lambda: coll.insert_many(copy.deepcopy(docs), **kw))

    def note(self, label, value):
        self._rec(label, {"ok": tpy(value)})


def run_case(name, fn, client, dbname):
    random.seed(name)
    e = Env(client, dbname)
    try:
        client.drop_database(dbname)
    except Exception:  # noqa: BLE001
        pass
    try:
        fn(e)
    except Exception as ex:  # noqa: BLE001
        e._rec("__case_exception__", {"err": err_outcome(ex)})
    finally:
        try:
            client.drop_database(dbname)
        except Exception:  # noqa: BLE001
            pass
    return e.steps


def canon_ok(o):
    return o


def step_key(s):
    import json
    return json.dumps(s["o"], sort_keys=False, default=str)


def compare_steps(gold, got, sharded=False, case=None):
    """Returns a list of (label, verdict, detail): verdict in same / msg / diff."""
    import json
    out = []
    gm = {s["l"]: s for s in gold}
    for s in got:
        g = gm.get(s["l"])
        if g is None:
            continue  # dropped from the golden file as non-deterministic on the oracle
        go, so = g["o"], s["o"]
        if (sharded and g.get("nat") or g.get("un")) and "ok" in go and "ok" in so:
            deep = True
            go = {"ok": _sorted_if_list(go["ok"], deep)}
            so = {"ok": _sorted_if_list(so["ok"], deep)}
        if go == so:
            out.append((s["l"], "same", None))
        elif "err" in go and "err" in so:
            a = dict(go["err"])
            b = dict(so["err"])
            am, bm = a.pop("msg", None), b.pop("msg", None)
            aw, bw = a.get("writeErrors"), b.get("writeErrors")
            if a == b or _strip_msgs(a) == _strip_msgs(b):
                out.append((s["l"], "msg", "%r != %r" % (am if am else aw, bm if bm else bw)))
            else:
                out.append((s["l"], "diff", "%s != %s" % (json.dumps(go)[:400], json.dumps(so)[:400])))
        else:
            out.append((s["l"], "diff", "%s != %s" % (json.dumps(go)[:600], json.dumps(so)[:600])))
    if case is not None:
        try:
            import mongo_known as known
        except ImportError:
            known = None
        if known:
            gm2 = {s["l"]: s for s in gold}
            sm2 = {s["l"]: s for s in got}
            for i, (label, verdict, detail) in enumerate(out):
                if verdict == "diff":
                    reason = known.KNOWN.get((case, label))
                    if reason is None and sharded and (case, label) in known.SHARDED_ORDER_DEPENDENT:
                        reason = "depends on natural document order, which differs across shards"
                    if reason is None and label in gm2 and label in sm2:
                        reason = known.accept(case, label, gm2[label]["o"], sm2[label]["o"])
                    if reason:
                        out[i] = (label, "accepted", reason + " | " + str(detail)[:160])
    seen = {s["l"] for s in got}
    for s in gold:
        if s["l"] not in seen:
            out.append((s["l"], "diff", "step missing"))
    return out


def _strip_msgs(e):
    e = dict(e)
    e.pop("msg", None)
    e.pop("codeName", None)
    if "writeErrors" in e:
        e["writeErrors"] = [{k: v for k, v in w.items() if k != "msg"} for w in e["writeErrors"]]
    return e


def _deep_sorted(v):
    import json
    if isinstance(v, list):
        return sorted((_deep_sorted(x) for x in v), key=lambda x: json.dumps(x, sort_keys=True, default=str))
    if isinstance(v, dict):
        return {k: _deep_sorted(x) for k, x in v.items()}
    return v


def _sorted_if_list(v, deep=False):
    import json
    if deep:
        return _deep_sorted(v)
    if isinstance(v, list):
        return sorted(v, key=lambda x: json.dumps(x, sort_keys=True, default=str))
    return v


# register the corpus modules
import mongo_corpus_query  # noqa: E402,F401
import mongo_corpus_write  # noqa: E402,F401
import mongo_corpus_agg  # noqa: E402,F401
import mongo_corpus_expr  # noqa: E402,F401
import mongo_corpus_admin  # noqa: E402,F401
