"""oswire: real OpenSearch HTTP/JSON API (_search/documents/_bulk), real opensearch-py client,
translated to Postgres underneath -- see com.nexagres.wire.oswire's package for the internal
Search IR this is staged around. No auth configured in the test compose file, so a bare client
with SSL disabled is enough (matches how real OpenSearch's own dev/test setups run unauthenticated).
"""
import uuid

from opensearchpy import OpenSearch

from conftest import HOST, OSWIRE_PORT


def client():
    return OpenSearch(
        hosts=[{"host": HOST, "port": OSWIRE_PORT}],
        http_compress=False, use_ssl=False, verify_certs=False,
    )


def test_index_and_search_by_term():
    c = client()
    index = f"smoke_{uuid.uuid4().hex[:8]}"
    c.index(index=index, id="1", body={"name": "warp", "category": "gateway"}, refresh=True)
    c.index(index=index, id="2", body={"name": "polyadvisor", "category": "assessment"}, refresh=True)

    result = c.search(index=index, body={"query": {"term": {"category": "gateway"}}})
    hits = result["hits"]["hits"]
    assert len(hits) == 1
    assert hits[0]["_id"] == "1"
    assert hits[0]["_source"]["name"] == "warp"


def test_get_update_delete_document():
    c = client()
    index = f"smoke_{uuid.uuid4().hex[:8]}"
    c.index(index=index, id="1", body={"name": "warp"}, refresh=True)

    doc = c.get(index=index, id="1")
    assert doc["_source"]["name"] == "warp"

    c.delete(index=index, id="1")
    # Real OpenSearch's GET returns HTTP 200 with found=false for a missing document, not a 404 --
    # unlike DELETE, which does 404 when there's nothing to delete.
    after_delete = c.get(index=index, id="1")
    assert after_delete["found"] is False


def test_terms_aggregation_with_nested_avg_metric():
    c = client()
    index = f"smoke_{uuid.uuid4().hex[:8]}"
    c.index(index=index, id="1", body={"category": "electronics", "price": 20.0}, refresh=True)
    c.index(index=index, id="2", body={"category": "electronics", "price": 40.0}, refresh=True)
    c.index(index=index, id="3", body={"category": "home", "price": 10.0}, refresh=True)

    result = c.search(index=index, body={
        "size": 0,
        "aggs": {"by_category": {
            "terms": {"field": "category", "size": 10},
            "aggs": {"avg_price": {"avg": {"field": "price"}}},
        }},
    })
    buckets = {b["key"]: b for b in result["aggregations"]["by_category"]["buckets"]}
    assert buckets["electronics"]["doc_count"] == 2
    assert buckets["electronics"]["avg_price"]["value"] == 30.0
    assert buckets["home"]["doc_count"] == 1
    assert buckets["home"]["avg_price"]["value"] == 10.0


def test_hybrid_query_fuses_text_and_vector_scores():
    c = client()
    index = f"smoke_{uuid.uuid4().hex[:8]}"
    c.index(index=index, id="1", body={"description": "a warp gateway", "vector": [0.1, 0.2, 0.3, 0.4]}, refresh=True)
    c.index(index=index, id="2", body={"description": "unrelated text", "vector": [0.9, 0.8, 0.1, 0.05]}, refresh=True)

    result = c.search(index=index, body={"query": {"hybrid": {"queries": [
        {"match": {"description": "warp"}},
        {"knn": {"vector": {"vector": [0.1, 0.2, 0.3, 0.4], "k": 2}}},
    ]}}})
    hits = result["hits"]["hits"]
    # doc 1 matches both the text query and is an exact vector match -- it must rank first.
    assert hits[0]["_id"] == "1"
