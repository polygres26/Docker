"""oswire against real OpenSearch's behaviour, with real Warp + real Postgres.

1. Replays the differential corpus (tests/python/os_conformance/corpus.py, ~320 request sequences) against Warp and
   compares with OpenSearch 2.19.6's recorded, normalised answers (golden/oracle_2.19.6.json) -- once with one Postgres
   backend and once with the index sharded over TWO Postgres backends. Differences must be listed in
   os_conformance/known.py (each with its reason).
2. opensearch-py (the real client) doing what applications do: connect probes, bulk helper, scan/scroll, search with
   aggregations/highlight, msearch, update/delete_by_query, templates.
3. Sharding: documents spread over both hosts, and search/sort/from-size/search_after/aggregations/count/bulk/mget/
   k-NN/hybrid come back correct and complete.

Needs Docker-less Postgres (WARP_TEST_PG_LOCAL=1) or Docker, and WARP_TEST_JAR (see warp_test_support.py).
"""
import json
import os
import sys

import psycopg2
import pytest
import requests
from opensearchpy import OpenSearch, helpers

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "os_conformance"))
import corpus  # noqa: E402
import diff_harness  # noqa: E402
import known  # noqa: E402
from launch_warp import Stack  # noqa: E402

GOLDEN = os.path.join(HERE, "os_conformance", "golden", "oracle_2.19.6.json")


@pytest.fixture(scope="module")
def one():
    with Stack(shards=1) as s:
        yield s


@pytest.fixture(scope="module")
def two():
    with Stack(shards=2) as s:
        yield s


def client(stack):
    return OpenSearch(hosts=[stack.url], use_ssl=False, verify_certs=False, timeout=60)


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


# ---------------------------------------------------------------------------------------------
# 1. golden replay
# ---------------------------------------------------------------------------------------------

def _replay(stack, sharded):
    golden = json.load(open(GOLDEN))
    warp = diff_harness.Client(stack.url)
    failures, passed, known_diffs = {}, 0, 0
    for c in corpus.all_cases():
        name = c["name"]
        diffs = diff_harness.run_case(c, golden[name], warp, sharded=sharded)
        reason = c.get("known") or known.KNOWN.get(name) or (known.KNOWN_SHARDED.get(name) if sharded else None)
        if not diffs:
            passed += 1
        elif reason:
            known_diffs += 1
        else:
            failures[name] = diffs[:4]
    print("\n[oswire conformance %s] cases=%d pass=%d documented-differences=%d unexpected=%d"
          % ("sharded x2" if sharded else "single", passed + known_diffs + len(failures), passed, known_diffs, len(failures)))
    return failures, passed


def test_golden_replay_single_backend(one):
    failures, passed = _replay(one, sharded=False)
    assert not failures, json.dumps(failures, indent=1)[:6000]
    assert passed >= 300


def test_golden_replay_two_sharded_backends(two):
    failures, passed = _replay(two, sharded=True)
    assert not failures, json.dumps(failures, indent=1)[:6000]
    assert passed >= 295


# ---------------------------------------------------------------------------------------------
# 2. opensearch-py
# ---------------------------------------------------------------------------------------------

def test_client_connect_probes(one):
    c = client(one)
    assert c.ping()
    info = c.info()
    assert info["version"]["distribution"] == "opensearch" and info["version"]["number"].startswith("2.")
    assert c.cluster.health()["status"] in ("green", "yellow")
    assert isinstance(c.cat.indices(format="json"), list)
    assert "cluster_name" in c.cluster.stats() or "nodes" in c.cluster.stats()
    assert c.indices.exists(index="probe_missing") is False


def test_client_index_get_update_delete_and_errors(one):
    c = client(one)
    c.indices.create(index="cli", body={"settings": {"index": {"number_of_replicas": 0}},
                                        "mappings": {"properties": {"n": {"type": "long"}, "t": {"type": "text"}}}})
    r = c.index(index="cli", id="1", body={"n": 1, "t": "hello world"}, refresh=True)
    assert r["result"] == "created" and r["_version"] == 1 and r["_seq_no"] == 0
    r = c.index(index="cli", id="1", body={"n": 2, "t": "hello again"})
    assert r["result"] == "updated" and r["_version"] == 2
    assert c.get(index="cli", id="1")["_source"]["n"] == 2
    with pytest.raises(Exception) as e:
        c.create(index="cli", id="1", body={"n": 3})
    assert getattr(e.value, "status_code", None) == 409
    with pytest.raises(Exception) as e:
        c.index(index="cli", id="1", body={"n": 3}, if_seq_no=0, if_primary_term=1)
    assert getattr(e.value, "status_code", None) == 409
    c.update(index="cli", id="1", body={"doc": {"n": 10}})
    c.update(index="cli", id="1", body={"script": {"source": "ctx._source.n += params.d", "params": {"d": 5}}})
    assert c.get(index="cli", id="1")["_source"]["n"] == 15
    with pytest.raises(Exception) as e:
        c.index(index="cli", id="2", body={"n": "not a number"})
    assert getattr(e.value, "status_code", None) == 400 and "mapper_parsing_exception" in str(e.value)
    assert c.delete(index="cli", id="1")["result"] == "deleted"
    with pytest.raises(Exception) as e:
        c.get(index="cli", id="1")
    assert getattr(e.value, "status_code", None) == 404
    c.indices.delete(index="cli")


def test_client_bulk_helper_scan_and_search(one):
    c = client(one)
    c.indices.create(index="bk", body={"mappings": {"properties": {"cat": {"type": "keyword"}, "n": {"type": "long"},
                                                                    "body": {"type": "text"}}}})
    docs = [{"_index": "bk", "_id": str(i), "_source": {"cat": "even" if i % 2 == 0 else "odd", "n": i,
                                                          "body": "document number %d about %s" % (i, "cats" if i % 3 == 0 else "dogs")}}
            for i in range(53)]
    ok, errs = helpers.bulk(c, docs, refresh=True)
    assert ok == 53 and not errs
    seen = [h["_id"] for h in helpers.scan(c, index="bk", query={"query": {"match_all": {}}}, size=10)]
    assert sorted(seen, key=int) == [str(i) for i in range(53)]
    r = c.search(index="bk", body={"query": {"match": {"body": "cats"}}, "size": 3, "sort": [{"n": "desc"}],
                                   "aggs": {"by_cat": {"terms": {"field": "cat"}, "aggs": {"mx": {"max": {"field": "n"}}}},
                                            "hist": {"histogram": {"field": "n", "interval": 20}}},
                                   "highlight": {"fields": {"body": {}}}})
    assert r["hits"]["total"] == {"value": 18, "relation": "eq"}
    assert [h["_source"]["n"] for h in r["hits"]["hits"]] == [51, 48, 45]
    assert "<em>cats</em>" in r["hits"]["hits"][0]["highlight"]["body"][0]
    assert {b["key"]: b["doc_count"] for b in r["aggregations"]["by_cat"]["buckets"]} == {"even": 9, "odd": 9}
    assert [b["key"] for b in r["aggregations"]["hist"]["buckets"]] == [0.0, 20.0, 40.0]
    assert c.count(index="bk", body={"query": {"term": {"cat": "odd"}}})["count"] == 26
    ms = c.msearch(body=[{"index": "bk"}, {"query": {"term": {"cat": "even"}}, "size": 0}, {"index": "nope"}, {"query": {"match_all": {}}}])
    assert ms["responses"][0]["hits"]["total"]["value"] == 27 and ms["responses"][1]["status"] == 404
    assert [d["found"] for d in c.mget(index="bk", body={"ids": ["1", "999"]})["docs"]] == [True, False]
    dq = c.delete_by_query(index="bk", body={"query": {"range": {"n": {"gte": 50}}}}, refresh=True)
    assert dq["deleted"] == 3
    uq = c.update_by_query(index="bk", body={"query": {"term": {"cat": "even"}}, "script": {"source": "ctx._source.flag = true"}}, refresh=True)
    assert uq["updated"] == 25
    assert c.count(index="bk", body={"query": {"term": {"flag": True}}})["count"] == 25
    c.indices.delete(index="bk")


def test_client_templates_alias_and_mapping_api(one):
    c = client(one)
    c.indices.put_template(name="tpl", body={"index_patterns": ["logs-*"], "settings": {"number_of_replicas": 0},
                                              "mappings": {"properties": {"level": {"type": "keyword"}, "@timestamp": {"type": "date"}}}})
    assert c.indices.exists_template(name="tpl")
    c.index(index="logs-1", body={"level": "INFO", "@timestamp": "2024-01-01T00:00:00Z", "msg": "hi"}, refresh=True)
    m = c.indices.get_mapping(index="logs-1")["logs-1"]["mappings"]["properties"]
    assert m["level"]["type"] == "keyword" and m["msg"]["type"] == "text" and m["msg"]["fields"]["keyword"]["ignore_above"] == 256
    c.indices.put_alias(index="logs-1", name="logs")
    assert c.search(index="logs", body={"query": {"term": {"level": "INFO"}}})["hits"]["total"]["value"] == 1
    c.indices.update_aliases(body={"actions": [{"remove": {"index": "logs-1", "alias": "logs"}}]})
    assert not c.indices.exists_alias(name="logs")
    c.indices.delete(index="logs-1")
    c.indices.delete_template(name="tpl")


def test_unsupported_features_fail_with_a_clear_opensearch_error(one):
    c = client(one)
    c.index(index="unsup", id="1", body={"a": 1}, refresh=True)
    for body in ({"query": {"span_term": {"a": 1}}}, {"query": {"more_like_this": {"fields": ["a"], "like": "x"}}},
                 {"query": {"match_all": {}}, "rescore": {"query": {"rescore_query": {"match_all": {}}}}},
                 {"aggs": {"x": {"significant_terms": {"field": "a"}}}}):
        with pytest.raises(Exception) as e:
            c.search(index="unsup", body=body)
        assert getattr(e.value, "status_code", None) == 400 and "not supported" in str(e.value)
    c.indices.delete(index="unsup")


def test_refresh_interval_minus_one_gates_search_visibility(one):
    c = client(one)
    c.indices.create(index="gated", body={"settings": {"index": {"refresh_interval": "-1"}}})
    c.index(index="gated", id="1", body={"a": 1})
    assert c.get(index="gated", id="1")["found"]
    assert c.search(index="gated", body={"query": {"match_all": {}}})["hits"]["total"]["value"] == 0
    c.indices.refresh(index="gated")
    assert c.search(index="gated", body={"query": {"match_all": {}}})["hits"]["total"]["value"] == 1
    c.index(index="gated", id="2", body={"a": 2}, refresh="wait_for")
    assert c.search(index="gated", body={"query": {"match_all": {}}})["hits"]["total"]["value"] == 2
    c.indices.delete(index="gated")


def test_bm25_scores_match_lucene_for_a_simple_index(one):
    """Scores of simple relevance queries are Lucene's BM25 (k1=1.2, b=0.75, lossy norms): the recorded OpenSearch score is
    reproduced to 6 digits on a single host."""
    c = client(one)
    for i, t in enumerate(["the quick brown fox", "the lazy dog", "quick quick fox jumps", "brown dog"]):
        c.index(index="bm", id=str(i), body={"t": t})
    c.indices.refresh(index="bm")
    r = c.search(index="bm", body={"query": {"match": {"t": "quick fox"}}})
    scores = {h["_id"]: h["_score"] for h in r["hits"]["hits"]}
    # values recorded from OpenSearch 2.19.6 for exactly this index
    assert scores == pytest.approx({"2": 1.528344, "0": 1.2667098}, rel=1e-5)
    c.indices.delete(index="bm")


# ---------------------------------------------------------------------------------------------
# 3. sharded over two Postgres backends
# ---------------------------------------------------------------------------------------------

def test_sharded_documents_land_on_both_hosts_and_reads_merge(two):
    c = client(two)
    c.indices.create(index="sh", body={"mappings": {"properties": {"n": {"type": "long"}, "g": {"type": "keyword"}, "t": {"type": "text"}}}})
    n = 120
    ok, errs = helpers.bulk(c, [{"_index": "sh", "_id": "d%d" % i, "_source": {"n": i, "g": "g%d" % (i % 4), "t": "item %d of the set" % i}}
                                for i in range(n)], refresh=True)
    assert ok == n and not errs
    on0 = sql(two.pgs[0], "SELECT count(*) FROM warp_search_sh")[0][0]
    on1 = sql(two.pgs[1], "SELECT count(*) FROM warp_search_sh")[0][0]
    assert on0 + on1 == n and on0 > 30 and on1 > 30, (on0, on1)
    # point ops route to one host
    assert c.get(index="sh", id="d57")["_source"]["n"] == 57
    # sort + from/size merge globally
    r = c.search(index="sh", body={"query": {"match_all": {}}, "sort": [{"n": "asc"}], "from": 30, "size": 10})
    assert [h["_source"]["n"] for h in r["hits"]["hits"]] == list(range(30, 40)) and r["hits"]["total"]["value"] == n
    # search_after pages through every document exactly once
    seen, after = [], None
    while True:
        body = {"query": {"match_all": {}}, "sort": [{"n": "desc"}, {"_id": "asc"}], "size": 17}
        if after:
            body["search_after"] = after
        hits = c.search(index="sh", body=body)["hits"]["hits"]
        if not hits:
            break
        seen += [h["_source"]["n"] for h in hits]
        after = hits[-1]["sort"]
    assert seen == list(range(n - 1, -1, -1))
    # aggregations are exact across hosts (avg is a true weighted average, cardinality/percentiles over all docs)
    a = c.search(index="sh", body={"size": 0, "aggs": {"avg": {"avg": {"field": "n"}}, "g": {"terms": {"field": "g"}, "aggs": {"s": {"sum": {"field": "n"}}}},
                                                     "card": {"cardinality": {"field": "g"}}, "st": {"stats": {"field": "n"}},
                                                     "p": {"percentiles": {"field": "n", "percents": [50]}}}})["aggregations"]
    assert a["avg"]["value"] == pytest.approx((n - 1) / 2) and a["card"]["value"] == 4 and a["st"]["count"] == n
    assert {b["key"]: b["doc_count"] for b in a["g"]["buckets"]} == {"g0": 30, "g1": 30, "g2": 30, "g3": 30}
    assert a["p"]["values"]["50.0"] == pytest.approx(59.5, abs=1.0)
    assert c.count(index="sh", body={"query": {"range": {"n": {"gte": 100}}}})["count"] == 20
    # mget spans hosts
    docs = c.mget(index="sh", body={"ids": ["d1", "d2", "d3", "d100", "nope"]})["docs"]
    assert [d["found"] for d in docs] == [True, True, True, True, False]
    # update/delete route to the owning host; delete_by_query reaches both
    c.update(index="sh", id="d5", body={"doc": {"n": 5000}})
    assert c.get(index="sh", id="d5")["_source"]["n"] == 5000
    assert c.delete(index="sh", id="d6")["result"] == "deleted"
    dq = c.delete_by_query(index="sh", body={"query": {"term": {"g": "g1"}}}, refresh=True)
    assert dq["deleted"] == 30 or dq["deleted"] == 29  # d5 was updated (n=5000) but keeps g; d6 deleted before (g2)
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_search_sh WHERE source->>'g'='g1'")[0][0] == 0
    c.indices.delete(index="sh")


def test_sharded_knn_and_hybrid_search_work_across_hosts(two):
    c = client(two)
    c.indices.create(index="vec", body={"settings": {"index": {"knn": True}}, "mappings": {"properties": {
        "v": {"type": "knn_vector", "dimension": 2, "method": {"name": "hnsw", "space_type": "l2", "engine": "lucene"}}, "t": {"type": "text"}}}})
    helpers.bulk(c, [{"_index": "vec", "_id": str(i), "_source": {"v": [float(i), float(i)], "t": "point %d" % i}} for i in range(40)], refresh=True)
    r = c.search(index="vec", body={"size": 3, "query": {"knn": {"v": {"vector": [10.2, 10.2], "k": 3}}}})
    assert [h["_id"] for h in r["hits"]["hits"]] == ["10", "11", "9"]
    assert r["hits"]["hits"][0]["_score"] > r["hits"]["hits"][1]["_score"]
    h = c.search(index="vec", body={"size": 3, "query": {"hybrid": {"queries": [{"match": {"t": "point 7"}}, {"knn": {"v": {"vector": [7, 7], "k": 5}}}]}}})
    assert h["hits"]["hits"][0]["_id"] == "7"
    c.indices.delete(index="vec")


def test_sharded_refresh_gating_and_index_lifecycle(two):
    c = client(two)
    c.indices.create(index="lc", body={"mappings": {"properties": {"a": {"type": "keyword"}}}})
    assert sql(two.pgs[0], "SELECT to_regclass('warp_search_lc') IS NOT NULL")[0][0] and sql(two.pgs[1], "SELECT to_regclass('warp_search_lc') IS NOT NULL")[0][0]
    c.indices.delete(index="lc")
    assert not sql(two.pgs[0], "SELECT to_regclass('warp_search_lc') IS NOT NULL")[0][0] and not sql(two.pgs[1], "SELECT to_regclass('warp_search_lc') IS NOT NULL")[0][0]


def test_index_names_that_are_not_sql_identifiers_work(one):
    c = client(one)
    for name in ("my-logs-2024.01.01", "日本語-index"):
        c.index(index=name, id="1", body={"a": 1}, refresh=True)
        assert c.search(index=name, body={"query": {"match_all": {}}})["hits"]["total"]["value"] == 1
        c.indices.delete(index=name)


def test_dropped_table_is_index_not_found_and_legacy_tables_are_adopted(one):
    c = client(one)
    c.index(index="orph", id="1", body={"f": "v"})
    sql(one.pgs[0], "DROP TABLE warp_search_orph")
    with pytest.raises(Exception) as e:
        c.search(index="orph", body={"query": {"match_all": {}}})
    assert getattr(e.value, "status_code", None) == 404 and "index_not_found_exception" in str(e.value)
    # a table written by an older oswire (no catalog row, no sequence numbers) is adopted: mapping inferred, docs numbered
    sql(one.pgs[0], "CREATE TABLE warp_search_legacy (doc_id TEXT PRIMARY KEY, source JSONB NOT NULL, embedding JSONB, "
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())")
    sql(one.pgs[0], "INSERT INTO warp_search_legacy (doc_id, source) VALUES ('a', '{\"title\": \"old doc\", \"n\": 5}'), ('b', '{\"title\": \"other\", \"n\": 7}')")
    assert c.search(index="legacy", body={"query": {"match": {"title": "old"}}})["hits"]["total"]["value"] == 1
    assert c.indices.get_mapping(index="legacy")["legacy"]["mappings"]["properties"]["n"]["type"] == "long"
    assert c.index(index="legacy", id="c", body={"title": "new", "n": 9})["_seq_no"] == 2
    assert {c.get(index="legacy", id=i)["_seq_no"] for i in ("a", "b", "c")} == {0, 1, 2}
    c.indices.delete(index="legacy")
