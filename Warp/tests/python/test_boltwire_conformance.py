"""boltwire against real Neo4j's behaviour, with real Warp + real Postgres, no Docker for Neo4j.

1. Replays the differential corpus (bolt_conformance/bolt_corpus.py, ~950 cases) against Warp and compares with real
   Neo4j 5.26's recorded, normalised answers (golden/oracle_neo4j_5.json.gz). Differences must be listed in
   bolt_conformance/bolt_known.py (each with its reason).
2. The openCypher TCK (needs a checkout of github.com/opencypher/openCypher, set BOLT_TCK_DIR to its `tck` directory;
   skipped otherwise): every scenario that passed on real Neo4j (golden/tck_oracle_neo4j_5.json.gz) must pass on Warp.
3. neo4j-driver end-to-end checks: parameters, explicit transactions (commit / rollback / error), managed transactions,
   PULL n, RESET recovery, routing scheme, constraints.
"""
import json
import gzip
import os
import subprocess
import sys

import pytest
from neo4j import GraphDatabase

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "bolt_conformance"))
import bolt_corpus as corpus  # noqa: E402
import bolt_harness  # noqa: E402
import bolt_known as known  # noqa: E402
from bolt_launch_warp import Stack  # noqa: E402

GOLDEN = os.path.join(HERE, "bolt_conformance", "golden", "oracle_neo4j_5.json.gz")
TCK_GOLDEN = os.path.join(HERE, "bolt_conformance", "golden", "tck_oracle_neo4j_5.json.gz")


@pytest.fixture(scope="module")
def stack():
    with Stack() as s:
        yield s


@pytest.fixture(scope="module")
def drv(stack):
    d = GraphDatabase.driver(stack.uri, auth=None)
    yield d
    d.close()


def test_golden_replay(stack):
    golden = json.load(gzip.open(GOLDEN, "rt"))
    d = GraphDatabase.driver(stack.uri, auth=None)
    failures, ok, kn = {}, 0, 0
    try:
        for c in corpus.all_cases():
            got = bolt_harness.run_case(d, stack.uri, c)
            if got == golden[c["name"]]:
                ok += 1
            elif c.get("known") or c["name"] in known.KNOWN:
                kn += 1
            else:
                failures[c["name"]] = (golden[c["name"]], got)
    finally:
        d.close()
    print("\n[boltwire conformance] cases=%d match=%d documented-differences=%d unexpected=%d"
          % (ok + kn + len(failures), ok, kn, len(failures)))
    assert not failures, {k: (json.dumps(v[0])[:300], json.dumps(v[1])[:300]) for k, v in list(failures.items())[:5]}
    assert ok >= 900


@pytest.mark.skipif(not os.environ.get("BOLT_TCK_DIR"), reason="set BOLT_TCK_DIR to a checkout of opencypher/openCypher's tck/")
def test_tck_scenarios_valid_on_neo4j_pass_on_warp(stack):
    out = os.path.join(os.environ.get("TMPDIR", "/tmp"), "warp-bolt-tck.json")
    r = subprocess.run([sys.executable, os.path.join(HERE, "bolt_conformance", "bolt_tck.py"), "--uri", stack.uri, "--features",
                        os.environ["BOLT_TCK_DIR"], "--only-valid", TCK_GOLDEN, "--out", out], capture_output=True, text=True)
    res = json.load(open(out))
    bad = {k: v["detail"][:200] for k, v in res["scenarios"].items() if v["status"] == "fail"}
    print("\n[boltwire TCK]", res["counts"])
    assert not bad, list(bad.items())[:5]
    assert res["counts"]["pass"] >= 3800


def test_parameters_of_every_kind(drv):
    with drv.session() as s:
        rec = s.run("RETURN $i AS i, $f AS f, $s AS s, $b AS b, $l AS l, $m AS m, $n AS n",
                    i=2 ** 62, f=1.5, s="h\u00e9llo", b=True, l=[1, [2]], m={"a": {"b": 1}}, n=None).single()
        assert rec["i"] == 2 ** 62 and rec["f"] == 1.5 and rec["s"] == "h\u00e9llo" and rec["b"] is True
        assert rec["l"] == [1, [2]] and rec["m"] == {"a": {"b": 1}} and rec["n"] is None


def test_explicit_transaction_commit_rollback_and_isolation(drv):
    with drv.session() as a, drv.session() as b:
        a.run("MATCH (n:Tx) DETACH DELETE n").consume()
        tx = a.begin_transaction()
        tx.run("CREATE (:Tx {v: 1})").consume()
        assert tx.run("MATCH (n:Tx) RETURN count(n) AS c").single()["c"] == 1   # reads its own writes
        assert b.run("MATCH (n:Tx) RETURN count(n) AS c").single()["c"] == 0    # not visible to others
        tx.rollback()
        assert b.run("MATCH (n:Tx) RETURN count(n) AS c").single()["c"] == 0
        with a.begin_transaction() as tx2:
            tx2.run("CREATE (:Tx {v: 2})").consume()
            tx2.commit()
        assert b.run("MATCH (n:Tx) RETURN count(n) AS c").single()["c"] == 1


def test_failed_statement_in_transaction_rolls_back_and_session_recovers(drv):
    with drv.session() as s:
        s.run("MATCH (n:Tx2) DETACH DELETE n").consume()
        tx = s.begin_transaction()
        tx.run("CREATE (:Tx2)").consume()
        with pytest.raises(Exception) as ei:
            tx.run("RETURN 1/0").consume()
        assert "ArithmeticError" in ei.value.code
        with pytest.raises(Exception):
            tx.commit()
        assert s.run("MATCH (n:Tx2) RETURN count(n) AS c").single()["c"] == 0


def test_managed_transactions(drv):
    with drv.session() as s:
        assert s.execute_write(lambda tx: tx.run("CREATE (n:Mgd {v: $v}) RETURN n.v AS v", v=7).single()["v"]) == 7
        assert s.execute_read(lambda tx: tx.run("MATCH (n:Mgd) RETURN count(n) AS c").single()["c"]) >= 1


def test_pull_n_and_partial_consumption(drv):
    with drv.session(fetch_size=3) as s:
        r = s.run("UNWIND range(1, 10) AS i RETURN i")
        assert [x["i"] for x in r] == list(range(1, 11))
        r2 = s.run("UNWIND range(1, 10) AS i RETURN i")
        assert next(iter(r2))["i"] == 1
        r2.consume()
        assert s.run("RETURN 1 AS x").single()["x"] == 1


def test_syntax_error_then_recovery_and_error_codes(drv):
    with drv.session() as s:
        with pytest.raises(Exception) as ei:
            s.run("RETRUN 1").consume()
        assert ei.value.code == "Neo.ClientError.Statement.SyntaxError"
        with pytest.raises(Exception) as ei:
            s.run("RETURN $missing").consume()
        assert ei.value.code == "Neo.ClientError.Statement.ParameterMissing"
        assert s.run("RETURN 1 AS x").single()["x"] == 1


def test_uniqueness_constraint_violation(drv):
    with drv.session() as s:
        s.run("MATCH (n:Uq) DETACH DELETE n").consume()
        s.run("DROP CONSTRAINT uq_email IF EXISTS").consume()
        s.run("CREATE CONSTRAINT uq_email FOR (n:Uq) REQUIRE n.email IS UNIQUE").consume()
        try:
            s.run("CREATE (:Uq {email: 'a'})").consume()
            with pytest.raises(Exception) as ei:
                s.run("CREATE (:Uq {email: 'a'})").consume()
            assert ei.value.code == "Neo.ClientError.Schema.ConstraintValidationFailed"
        finally:
            s.run("DROP CONSTRAINT uq_email IF EXISTS").consume()


def test_routing_scheme_and_server_agent(stack):
    d = GraphDatabase.driver(stack.uri.replace("bolt://", "neo4j://"), auth=None)
    try:
        d.verify_connectivity()
        assert d.get_server_info().agent.startswith("Neo4j/")
        with d.session() as s:
            assert s.run("RETURN 1 AS x").single()["x"] == 1
    finally:
        d.close()


def test_graph_values_and_paths(drv):
    with drv.session() as s:
        s.run("MATCH (n:Pth) DETACH DELETE n").consume()
        p = s.run("CREATE p = (a:Pth {n: 1})-[:R {w: 2}]->(b:Pth {n: 2}) RETURN p").single()["p"]
        assert [n["n"] for n in p.nodes] == [1, 2] and p.relationships[0]["w"] == 2
        n = s.run("MATCH (n:Pth {n: 1}) RETURN n").single()["n"]
        assert n.labels == {"Pth"} and isinstance(n.element_id, str)
