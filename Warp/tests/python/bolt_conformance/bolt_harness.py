#!/usr/bin/env python3
"""Differential harness: the bolt_corpus.py cases against a REAL Neo4j and against Warp (boltwire), normalised comparison.

  bolt_harness.py --oracle bolt://localhost:7687 --warp bolt://localhost:<port> --record golden/oracle_neo4j_5.json.gz
  bolt_harness.py --golden golden/oracle_neo4j_5.json.gz --warp bolt://localhost:<port>       # no Neo4j needed
"""
import argparse, json, math, os, sys, time

from neo4j import GraphDatabase
import neo4j.time as ntime
from neo4j.graph import Node, Relationship, Path
import neo4j.spatial as nspatial

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bolt_corpus as corpus  # noqa: E402
import bolt_known as known  # noqa: E402


def norm(v):
    if v is None or isinstance(v, (bool, str)):
        return v
    if isinstance(v, int):
        return {"i": v}
    if isinstance(v, float):
        return {"f": "nan" if math.isnan(v) else ("inf" if v == math.inf else "-inf" if v == -math.inf else repr(v))}
    if isinstance(v, (bytes, bytearray)):
        return {"b": bytes(v).hex()}
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    if isinstance(v, dict):
        return {"m": {k: norm(x) for k, x in sorted(v.items())}}
    if isinstance(v, Node):
        return {"node": sorted(v.labels), "p": {k: norm(x) for k, x in sorted(v.items())}}
    if isinstance(v, Relationship):
        return {"rel": v.type, "p": {k: norm(x) for k, x in sorted(v.items())}}
    if isinstance(v, Path):
        out = []
        for i, r in enumerate(v.relationships):
            fwd = r.start_node.element_id == v.nodes[i].element_id
            out.append([norm(v.nodes[i]), "->" if fwd else "<-", norm(r)])
        return {"path": out, "end": norm(v.nodes[-1])}
    if isinstance(v, nspatial.Point):
        return {"pt": [getattr(v, "srid", None)] + [repr(c) for c in v]}
    if isinstance(v, ntime.Duration):
        return {"dur": [v.months, v.days, v.seconds, v.nanoseconds]}
    if isinstance(v, ntime.DateTime):
        z = getattr(v.tzinfo, "zone", None) or getattr(v.tzinfo, "key", None) if v.tzinfo else None
        return {"dt": v.iso_format(), "z": z if z != "UTC" else None}
    if isinstance(v, (ntime.Date, ntime.Time)):
        return {"t": v.iso_format()}
    return {"?": str(v)}


def rowkey(r):
    return json.dumps(r, sort_keys=True)


def run_query(session_or_tx, step, fetch=None):
    q, p = step["q"], step.get("p") or {}
    try:
        res = session_or_tx.run(q, p) if fetch is None else session_or_tx.run(q, p)
        cols = list(res.keys())
        if step.get("no_iterate"):
            summ = res.consume()
            return {"cols": cols, "rows": None, "counters": counters(summ)}
        if step.get("take") is not None:
            rows = []
            for r in res:
                rows.append([norm(x) for x in r.values()])
                if len(rows) >= step["take"]:
                    break
            res.consume()
            return {"cols": cols, "rows": rows}
        rows = [[norm(x) for x in r.values()] for r in res]
        summ = res.consume()
        if step.get("count_only"):
            return {"cols": len(cols), "n": len(rows), "sample": rows[:1] if len(rows[0:1]) and len(json.dumps(rows[0])) < 2000 else None, "counters": counters(summ)}
        if step.get("cols_only"):
            return {"cols": cols, "n": len(rows)}
        ordered = step.get("ordered") or "ORDER BY" in q.upper()
        if not ordered:
            rows.sort(key=rowkey)
        out = {"cols": cols, "rows": rows, "counters": counters(summ)}
        if step.get("meta") == "summary":
            out["type"] = summ.query_type
            out["db_named"] = summ.database is not None
        return out
    except Exception as e:  # noqa: BLE001
        return {"err": getattr(e, "code", None) or type(e).__name__}


def counters(summ):
    c = summ.counters
    d = {k: getattr(c, k) for k in ("nodes_created", "nodes_deleted", "relationships_created", "relationships_deleted",
                                    "properties_set", "labels_added", "labels_removed", "indexes_added", "indexes_removed",
                                    "constraints_added", "constraints_removed")}
    return {k: v for k, v in d.items() if v}


def reset(drv):
    with drv.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
        for r in list(s.run("SHOW CONSTRAINTS YIELD name")):
            s.run("DROP CONSTRAINT `%s`" % r["name"]).consume()
        for r in list(s.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'")):
            s.run("DROP INDEX `%s`" % r["name"]).consume()


def run_meta(drv, uri, step):
    m = step["meta"]
    try:
        if m == "server_agent":
            return {"agent": drv.get_server_info().agent.split("/")[0]}
        if m == "verify":
            drv.verify_connectivity()
            return {"ok": True}
        if m == "routing_driver":
            d = GraphDatabase.driver(uri.replace("bolt://", "neo4j://"), auth=None)
            try:
                with d.session() as s:
                    return {"v": s.run("RETURN 1 AS x").single()["x"]}
            finally:
                d.close()
        if m == "two_sessions":
            with drv.session() as a, drv.session() as b:
                return {"a": a.run("RETURN 1 AS x").single()["x"], "b": b.run("RETURN 2 AS x").single()["x"]}
        if m == "bookmarks":
            with drv.session() as s:
                s.run("CREATE (:BM)").consume()
                bm = s.last_bookmarks()
            with drv.session(bookmarks=bm) as s2:
                return {"n": s2.run("MATCH (n:BM) RETURN count(n) AS c").single()["c"], "has_bm": bool(list(bm))}
        if m == "database":
            with drv.session(database=step["database"]) as s:
                return {"v": s.run("RETURN 1 AS x").single()["x"]}
        if m == "auth":
            d = GraphDatabase.driver(uri, auth=(step["user"], step["password"]))
            try:
                d.verify_connectivity()
                return {"ok": True}
            finally:
                d.close()
        if m == "reset_recovery":
            with drv.session() as s:
                try:
                    s.run("RETRUN 1").consume()
                except Exception as e:  # noqa: BLE001
                    err = getattr(e, "code", None)
                return {"err": err, "after": s.run("RETURN 1 AS x").single()["x"]}
        if m == "concurrent_isolation":
            with drv.session() as a, drv.session() as b:
                tx = a.begin_transaction()
                tx.run("CREATE (:ISO)").consume()
                seen = b.run("MATCH (n:ISO) RETURN count(n) AS c").single()["c"]
                tx.rollback()
                return {"seen_before_commit": seen, "after_rollback": b.run("MATCH (n:ISO) RETURN count(n) AS c").single()["c"]}
        if m == "pipelined":
            with drv.session() as s:
                rs = [s.run("RETURN %d AS x" % i) for i in range(5)]
                return {"v": [r.single()["x"] for r in rs]}
        if m == "close_mid_stream":
            with drv.session(default_access_mode="read") as s:
                r = s.run("UNWIND range(1, 100000) AS i RETURN i")
                next(iter(r))
            with drv.session() as s:
                return {"after": s.run("RETURN 1 AS x").single()["x"]}
        if m == "element_ids":
            with drv.session() as s:
                s.run("CREATE (:EID)").consume()
                a = s.run("MATCH (n:EID) RETURN n").single()["n"].element_id
                b = s.run("MATCH (n:EID) RETURN elementId(n) AS e").single()["e"]
                return {"same": a == b, "is_str": isinstance(a, str)}
        if m == "protocol":
            return {"skipped": True}
    except Exception as e:  # noqa: BLE001
        return {"err": getattr(e, "code", None) or type(e).__name__}
    return {"skipped": m}


def run_case(drv, uri, case):
    reset(drv)
    out = []
    for st in case["steps"]:
        try:
            if "tx" in st:
                with drv.session(default_access_mode="read" if st.get("access") == "read" else "write") as s:
                    tx = s.begin_transaction(metadata=st.get("metadata"), timeout=st.get("timeout"))
                    res = []
                    for inner in st["tx"]:
                        res.append(run_query(tx, inner))
                    try:
                        (tx.commit if st["end"] == "commit" else tx.rollback)()
                        res.append({"end": st["end"]})
                    except Exception as e:  # noqa: BLE001
                        res.append({"err": getattr(e, "code", None) or type(e).__name__})
                    out.append({"tx": res})
            elif "stream" in st:
                with drv.session() as s:
                    tx = s.begin_transaction()
                    rs = [tx.run(q) for q in st["stream"]]
                    out.append({"stream": [[norm(x) for x in r.values()] for a in rs for r in a]})
                    tx.commit()
            elif "managed" in st:
                with drv.session() as s:
                    fn = s.execute_write if st["managed"] == "write" else s.execute_read
                    try:
                        out.append(fn(lambda tx: [[norm(x) for x in r.values()] for r in tx.run(st["q"])]))
                    except Exception as e:  # noqa: BLE001
                        out.append({"err": getattr(e, "code", None) or type(e).__name__})
            elif "fetch" in st:
                with drv.session(fetch_size=st["fetch"]) as s:
                    out.append(run_query(s, st))
            elif "meta" in st and "q" not in st:
                out.append(run_meta(drv, uri, st))
            else:
                with drv.session() as s:
                    out.append(run_query(s, st))
        except Exception as e:  # noqa: BLE001
            out.append({"harness_err": type(e).__name__})
    return out


def compare(a, b):
    return a == b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp")
    ap.add_argument("--record")
    ap.add_argument("--golden")
    ap.add_argument("--filter", default="")
    ap.add_argument("--out")
    a = ap.parse_args()
    cases = [c for c in corpus.all_cases() if a.filter in c["name"]]
    golden = {}
    if a.oracle:
        d = GraphDatabase.driver(a.oracle, auth=None)
        for c in cases:
            golden[c["name"]] = run_case(d, a.oracle, c)
        d.close()
        if a.record:
            import gzip
            json.dump(golden, gzip.open(a.record, "wt"), sort_keys=True, separators=(",", ":"))
    elif a.golden:
        import gzip
        golden = json.load(gzip.open(a.golden, "rt")) if a.golden.endswith(".gz") else json.load(open(a.golden))
    bad, kn, ok = {}, 0, 0
    if a.warp:
        d = GraphDatabase.driver(a.warp, auth=None)
        for c in cases:
            got = run_case(d, a.warp, c)
            if got == golden.get(c["name"]):
                ok += 1
            elif c.get("known") or c["name"] in known.KNOWN:
                kn += 1
            else:
                bad[c["name"]] = {"oracle": golden.get(c["name"]), "warp": got}
        d.close()
    print(json.dumps({"cases": len(cases), "match": ok, "known": kn, "differ": len(bad)}))
    for k, v in bad.items():
        print("DIFF", k)
        print("   oracle", json.dumps(v["oracle"])[:400])
        print("   warp  ", json.dumps(v["warp"])[:400])
    if a.out:
        json.dump(bad, open(a.out, "w"), indent=1)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
