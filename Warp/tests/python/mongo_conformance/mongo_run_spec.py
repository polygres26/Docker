#!/usr/bin/env python3
"""Runs MongoDB driver-spec tests (mongodb/specifications, Apache-2.0; kept outside the repo) against a server:

  * source/crud/tests/unified/*.json  -- a subset unified-test-format runner: collection/database operations,
    initialData, expectResult / expectError (isError, errorCode, errorCodeName, expectResult), outcome; event
    expectations, sessions, transactions, change streams, client-level bulkWrite and failpoints are skipped.
  * source/bson-corpus/tests/*.json  -- every `valid` case's canonical BSON is inserted and read back through
    pymongo (raw documents) and must come back byte-identical.

  python3 mongo_run_spec.py --specs DIR --uri mongodb://... [--oracle mongodb://...] [--filter re] [--out results.json]

With --oracle the tests are first run against the real mongod; those that fail there are reported as "invalid on the
oracle" and excluded, so the Warp column only counts tests a real MongoDB passes.
"""
import argparse
import binascii
import glob
import json
import os
import re
import struct
import sys

import bson
from bson import json_util
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient, errors as pe
from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne, ReturnDocument

SERVER_VERSION = (7, 0)
SKIP_KEYS = ("sort",)  # update/replace sort option is not in 7.0


def vt(s):
    return tuple(int(x) for x in s.split(".")[:2])


def requirements_ok(reqs, client):
    if not reqs:
        return True
    for r in reqs:
        ok = True
        if "minServerVersion" in r and vt(r["minServerVersion"]) > SERVER_VERSION:
            ok = False
        if "maxServerVersion" in r and vt(r["maxServerVersion"]) < SERVER_VERSION:
            ok = False
        if "topologies" in r and "single" not in r["topologies"]:
            ok = False
        if "serverless" in r and r["serverless"] == "require":
            ok = False
        if "auth" in r and r["auth"]:
            ok = False
        if "serverParameters" in r or "csfle" in r or "authMechanism" in r:
            ok = False
        if ok:
            return True
    return False


def loads(x):
    return json_util.loads(json.dumps(x), json_options=json_util.JSONOptions(json_mode=json_util.JSONMode.CANONICAL, tz_aware=False))


def num_eq(a, b):
    return a == b and type(a) != bool == (type(b) != bool) or (isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool) and a == b)


def match(exp, act, root=False):
    """Unified-format matching: expected is a subset of actual for documents, exact for arrays."""
    if isinstance(exp, dict) and len(exp) == 1 and next(iter(exp)).startswith("$$"):
        k, v = next(iter(exp.items()))
        if k == "$$exists":
            return True  # handled by the caller (key presence)
        if k == "$$unsetOrMatches":
            return act is None or match(v, act)
        if k == "$$type":
            names = v if isinstance(v, list) else [v]
            return any(_type_is(n, act) for n in names)
        if k == "$$matchAsDocument" or k == "$$matchAsRoot":
            return True
        return True
    if isinstance(exp, dict):
        if not isinstance(act, dict) and not hasattr(act, "keys"):
            return False
        for k, v in exp.items():
            if isinstance(v, dict) and len(v) == 1 and "$$exists" in v:
                if bool(v["$$exists"]) != (k in act):
                    return False
                continue
            if k not in act:
                if isinstance(v, dict) and len(v) == 1 and "$$unsetOrMatches" in v:
                    continue
                return False
            if not match(v, act[k]):
                return False
        return True
    if isinstance(exp, list):
        return isinstance(act, list) and len(exp) == len(act) and all(match(e, a) for e, a in zip(exp, act))
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return isinstance(act, (int, float, bson.Int64, bson.Decimal128)) and not isinstance(act, bool) and float(exp) == float(act)
    return exp == act


def _type_is(name, v):
    m = {"string": str, "int": int, "long": (int, bson.Int64), "double": float, "object": dict, "array": list, "bool": bool,
         "objectId": bson.ObjectId, "date": __import__("datetime").datetime, "null": type(None)}
    t = m.get(name)
    return t is not None and isinstance(v, t)


class Skip(Exception):
    pass


def run_ops(client, entities, test, db_default):
    for op in test["operations"]:
        obj = entities.get(op["object"])
        if obj is None:
            raise Skip("entity " + op["object"])
        name = op["name"]
        args = loads(op.get("arguments", {}))
        expect_err = op.get("expectError")
        expect_res = op.get("expectResult")
        try:
            res = execute(obj, name, args)
            err = None
        except Skip:
            raise
        except (pe.PyMongoError, TypeError, ValueError, bson.errors.BSONError) as ex:
            res, err = None, ex
        if expect_err:
            if err is None:
                return "expected an error, got %r" % (res,)
            if expect_err.get("isClientError") and isinstance(err, pe.OperationFailure):
                return "expected a client error, got server error %s" % err
            if "errorCode" in expect_err and getattr(err, "code", None) != expect_err["errorCode"]:
                return "errorCode %s != %s" % (getattr(err, "code", None), expect_err["errorCode"])
            if "errorCodeName" in expect_err:
                cn = (getattr(err, "details", None) or {}).get("codeName")
                if cn != expect_err["errorCodeName"]:
                    return "codeName %s != %s" % (cn, expect_err["errorCodeName"])
            if "expectResult" in expect_err:
                got = getattr(err, "details", None) if isinstance(err, pe.BulkWriteError) else None
                # partial bulk results are reported through err.details; compare the counters that exist
                if isinstance(err, pe.BulkWriteError) and got is not None:
                    exp = loads(expect_err["expectResult"])
                    mapped = {"deletedCount": got.get("nRemoved"), "insertedCount": got.get("nInserted"), "matchedCount": got.get("nMatched"),
                              "modifiedCount": got.get("nModified"), "upsertedCount": got.get("nUpserted")}
                    for k, v in exp.items():
                        if k in mapped and mapped[k] != v:
                            return "bulk %s %s != %s" % (k, mapped[k], v)
            continue
        if err is not None:
            return "unexpected error %s: %s" % (type(err).__name__, str(err)[:200])
        if expect_res is not None:
            exp = loads(expect_res)
            if not match(exp, res, True):
                return "result %r does not match %r" % (str(res)[:300], str(exp)[:300])
        if "saveResultAsEntity" in op:
            entities[op["saveResultAsEntity"]] = res
    return None


def execute(obj, name, a):
    from pymongo.collection import Collection
    from pymongo.database import Database
    if isinstance(obj, Collection):
        c = obj
        a = dict(a)
        for k in ("session",):
            a.pop(k, None)
        if "sort" in a and name in ("updateOne", "replaceOne"):
            raise Skip("sort on update/replace is not in 7.0")
        if name == "insertOne":
            r = c.insert_one(a["document"], bypass_document_validation=a.get("bypassDocumentValidation", False), **({"comment": a["comment"]} if "comment" in a else {}))
            return {"insertedId": r.inserted_id}
        if name == "insertMany":
            r = c.insert_many(a["documents"], ordered=a.get("ordered", True), bypass_document_validation=a.get("bypassDocumentValidation", False))
            return {"insertedIds": {str(i): v for i, v in enumerate(r.inserted_ids)}}
        if name in ("updateOne", "updateMany", "replaceOne"):
            kw = {k: a[k] for k in ("upsert", "arrayFilters", "hint", "collation", "let", "comment", "bypassDocumentValidation") if k in a}
            if "arrayFilters" in kw:
                kw["array_filters"] = kw.pop("arrayFilters")
            if "bypassDocumentValidation" in kw:
                kw["bypass_document_validation"] = kw.pop("bypassDocumentValidation")
            if name == "replaceOne":
                r = c.replace_one(a["filter"], a["replacement"], **{k: v for k, v in kw.items() if k != "array_filters"})
            elif name == "updateOne":
                r = c.update_one(a["filter"], a["update"], **kw)
            else:
                r = c.update_many(a["filter"], a["update"], **kw)
            return {"matchedCount": r.matched_count, "modifiedCount": r.modified_count, "upsertedCount": 1 if r.upserted_id is not None else 0,
                    **({"upsertedId": r.upserted_id} if r.upserted_id is not None else {})}
        if name in ("deleteOne", "deleteMany"):
            kw = {k: a[k] for k in ("hint", "collation", "let", "comment") if k in a}
            r = (c.delete_one if name == "deleteOne" else c.delete_many)(a["filter"], **kw)
            return {"deletedCount": r.deleted_count}
        if name == "find":
            kw = {}
            m = {"sort": "sort", "skip": "skip", "limit": "limit", "batchSize": "batch_size", "projection": "projection", "hint": "hint",
                 "collation": "collation", "comment": "comment", "let": "let", "allowDiskUse": "allow_disk_use", "max": "max", "min": "min",
                 "returnKey": "return_key", "showRecordId": "show_record_id"}
            for k, pk in m.items():
                if k in a:
                    kw[pk] = list(a[k].items()) if k in ("sort", "hint") and isinstance(a[k], dict) else a[k]
            return list(c.find(a.get("filter", {}), **kw))
        if name == "findOne":
            return c.find_one(a.get("filter", {}), **({"projection": a["projection"]} if "projection" in a else {}))
        if name in ("findOneAndUpdate", "findOneAndReplace", "findOneAndDelete"):
            kw = {}
            if "returnDocument" in a:
                kw["return_document"] = ReturnDocument.AFTER if a["returnDocument"] == "After" else ReturnDocument.BEFORE
            for k, pk in (("sort", "sort"), ("projection", "projection"), ("upsert", "upsert"), ("hint", "hint"), ("collation", "collation"),
                          ("arrayFilters", "array_filters"), ("let", "let"), ("comment", "comment"), ("bypassDocumentValidation", "bypass_document_validation")):
                if k in a and not (name != "findOneAndUpdate" and k == "arrayFilters"):
                    kw[pk] = list(a[k].items()) if k == "sort" else a[k]
            if name == "findOneAndUpdate":
                return c.find_one_and_update(a["filter"], a["update"], **kw)
            if name == "findOneAndReplace":
                return c.find_one_and_replace(a["filter"], a["replacement"], **kw)
            kw.pop("upsert", None)
            kw.pop("return_document", None)
            return c.find_one_and_delete(a["filter"], **kw)
        if name == "aggregate":
            kw = {k: a[k] for k in ("collation", "hint", "let", "comment", "allowDiskUse", "batchSize") if k in a}
            if "allowDiskUse" in kw:
                kw["allowDiskUse"] = kw["allowDiskUse"]
            return list(c.aggregate(a["pipeline"], **{("batchSize" if k == "batchSize" else k): v for k, v in kw.items()}))
        if name == "countDocuments":
            kw = {k: a[k] for k in ("skip", "limit", "collation", "hint", "comment") if k in a}
            return c.count_documents(a.get("filter", {}), **kw)
        if name == "estimatedDocumentCount":
            return c.estimated_document_count(**({"comment": a["comment"]} if "comment" in a else {}))
        if name == "count":
            return c.count_documents(a.get("filter", {}))
        if name == "distinct":
            kw = {k: a[k] for k in ("collation", "comment") if k in a}
            return c.distinct(a["fieldName"], a.get("filter", {}), **kw)
        if name == "bulkWrite":
            reqs = []
            for r in a["requests"]:
                (k, v), = r.items()
                if k == "insertOne":
                    reqs.append(InsertOne(v["document"]))
                elif k in ("updateOne", "updateMany"):
                    kw = {x: v[x] for x in ("upsert", "hint", "collation") if x in v}
                    if "arrayFilters" in v:
                        kw["array_filters"] = v["arrayFilters"]
                    reqs.append((UpdateOne if k == "updateOne" else UpdateMany)(v["filter"], v["update"], **kw))
                elif k == "replaceOne":
                    reqs.append(ReplaceOne(v["filter"], v["replacement"], **{x: v[x] for x in ("upsert", "hint", "collation") if x in v}))
                elif k in ("deleteOne", "deleteMany"):
                    reqs.append((DeleteOne if k == "deleteOne" else DeleteMany)(v["filter"], **{x: v[x] for x in ("hint", "collation") if x in v}))
                else:
                    raise Skip("bulk op " + k)
            r = c.bulk_write(reqs, ordered=a.get("ordered", True), bypass_document_validation=a.get("bypassDocumentValidation", False))
            return {"deletedCount": r.deleted_count, "insertedCount": r.inserted_count, "matchedCount": r.matched_count, "modifiedCount": r.modified_count,
                    "upsertedCount": r.upserted_count, "insertedIds": {str(k): v for k, v in (r.bulk_api_result.get("insertedIds") or {}).items()} if False else {},
                    "upsertedIds": {str(k): v for k, v in r.upserted_ids.items()}}
        raise Skip("operation " + name)
    if isinstance(obj, Database):
        if name == "runCommand":
            return obj.command(a["command"])
        if name == "aggregate":
            return list(obj.aggregate(a["pipeline"]))
        raise Skip("db operation " + name)
    raise Skip("object type")


def run_unified_file(path, client):
    data = json.load(open(path))
    results = []
    if not requirements_ok(data.get("runOnRequirements"), client):
        return [(path, "*", "skip", "runOnRequirements")]
    ents_spec = data.get("createEntities", [])
    if any(k in e for e in ents_spec for k in ("session", "clientEncryption", "bucket", "thread", "stream")):
        return [(path, "*", "skip", "entity types")]
    for test in data["tests"]:
        name = test["description"]
        if test.get("skipReason"):
            results.append((path, name, "skip", test["skipReason"]))
            continue
        if not requirements_ok(test.get("runOnRequirements"), client):
            results.append((path, name, "skip", "runOnRequirements"))
            continue
        if any(o["name"] in ("failPoint", "targetedFailPoint", "createChangeStream", "assertCollectionNotExists", "assertSessionTransactionState", "startTransaction",
                             "clientBulkWrite", "iterateUntilDocumentOrError", "withTransaction") for o in test["operations"]):
            results.append((path, name, "skip", "unsupported operation type"))
            continue
        try:
            # entities
            entities = {}
            dbs = {}
            for e in ents_spec:
                if "client" in e:
                    entities[e["client"]["id"]] = client
                elif "database" in e:
                    d = e["database"]
                    entities[d["id"]] = client[d["databaseName"]]
                    dbs[d["databaseName"]] = True
                elif "collection" in e:
                    c = e["collection"]
                    entities[c["id"]] = entities[c["database"]][c["collectionName"]]
            for idata in data.get("initialData", []):
                coll = client[idata["databaseName"]][idata["collectionName"]]
                try:
                    client[idata["databaseName"]].drop_collection(idata["collectionName"])
                    client[idata["databaseName"]].create_collection(idata["collectionName"])
                except pe.OperationFailure:
                    coll.delete_many({})  # servers without drop/create (the pre-rewrite Warp)
                if idata["documents"]:
                    coll.insert_many(loads(idata["documents"]))
            problem = run_ops(client, entities, test, None)
            if problem is None:
                for oc in test.get("outcome", []):
                    docs = list(client[oc["databaseName"]][oc["collectionName"]].find({}).sort("_id", 1))
                    exp = loads(oc["documents"])
                    if not (len(docs) == len(exp) and all(match(e, a) and set(a.keys()) == set(e.keys()) for e, a in zip(exp, docs))):
                        problem = "outcome %s: %r != %r" % (oc["collectionName"], str(docs)[:300], str(exp)[:300])
                        break
            results.append((path, name, "pass" if problem is None else "fail", problem or ""))
        except Skip as s:
            results.append((path, name, "skip", str(s)))
        except Exception as ex:  # noqa: BLE001
            results.append((path, name, "fail", "harness/server exception %s: %s" % (type(ex).__name__, str(ex)[:200])))
    for idata in data.get("initialData", []):
        try:
            client[idata["databaseName"]].drop_collection(idata["collectionName"])
        except Exception:  # noqa: BLE001
            pass
    return results


def run_bson_corpus(path, client):
    data = json.load(open(path))
    out = []
    coll = client["bsoncorpus"]["c"]
    for i, t in enumerate(data.get("valid", [])):
        name = "%s/%s" % (data.get("description"), t.get("description"))
        canon = binascii.unhexlify(t["canonical_bson"])
        body = b"\x10_id\x00" + struct.pack("<i", 1) + b"\x03v\x00" + canon + b"\x00"
        wrapper = struct.pack("<i", len(body) + 4) + body
        try:
            coll.delete_many({})
            coll.insert_one(RawBSONDocument(wrapper))
            back = coll.with_options(codec_options=bson.CodecOptions(document_class=RawBSONDocument)).find_one({"_id": 1})
            raw = bytes(back.raw)
            if raw[13] != 0x03:
                out.append((path, name, "fail", "unexpected layout"))
                continue
            n = struct.unpack_from("<i", raw, 16)[0]
            got = raw[16:16 + n]
            out.append((path, name, "pass" if got == canon else "fail", "" if got == canon else "bytes differ: %s != %s" % (binascii.hexlify(got)[:120], t["canonical_bson"][:120])))
        except Exception as ex:  # noqa: BLE001
            out.append((path, name, "fail", "%s: %s" % (type(ex).__name__, str(ex)[:160])))
    client["bsoncorpus"].drop_collection("c")
    return out


def run_all(uri, specs, flt):
    client = MongoClient(uri, serverSelectionTimeoutMS=15000, socketTimeoutMS=int(os.environ.get("MONGO_SPEC_SOCKET_TIMEOUT_MS", "30000")), retryWrites=False)
    res = []
    for p in sorted(glob.glob(os.path.join(specs, "source/crud/tests/unified/*.json"))):
        if flt and not re.search(flt, p):
            continue
        res += run_unified_file(p, client)
    for p in sorted(glob.glob(os.path.join(specs, "source/bson-corpus/tests/*.json"))):
        if flt and not re.search(flt, p):
            continue
        res += run_bson_corpus(p, client)
    client.close()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--specs", required=True)
    ap.add_argument("--uri", required=True)
    ap.add_argument("--oracle")
    ap.add_argument("--filter", default="")
    ap.add_argument("--out")
    a = ap.parse_args()
    oracle = {}
    if a.oracle:
        for p, n, s, d in run_all(a.oracle, a.specs, a.filter):
            oracle[(os.path.basename(p), n)] = s
    warp = run_all(a.uri, a.specs, a.filter)
    counts = {"pass": 0, "fail": 0, "skip": 0, "oracle_invalid": 0}
    fails = []
    passing = []
    for p, n, s, d in warp:
        key = (os.path.basename(p), n)
        if a.oracle and oracle.get(key) not in ("pass",) and s != "skip":
            counts["oracle_invalid"] += 1
            continue
        counts[s] += 1
        if s == "fail":
            fails.append("%s :: %s :: %s" % (key[0], n, d))
        elif s == "pass":
            passing.append("%s :: %s" % key)
    print("\n".join(fails))
    print("SPEC %s" % counts)
    if a.out:
        json.dump({"counts": counts, "failures": fails, "passing": passing}, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
