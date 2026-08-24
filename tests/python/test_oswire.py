"""oswire: real OpenSearch HTTP/JSON API (_search/documents/_bulk), real opensearch-py client,
translated to Postgres underneath -- see com.polygres.wire.oswire's package for the internal
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
    c.index(index=index, id="1", body={"name": "polywire", "category": "gateway"}, refresh=True)
    c.index(index=index, id="2", body={"name": "polyadvisor", "category": "assessment"}, refresh=True)

    result = c.search(index=index, body={"query": {"term": {"category": "gateway"}}})
    hits = result["hits"]["hits"]
    assert len(hits) == 1
    assert hits[0]["_id"] == "1"
    assert hits[0]["_source"]["name"] == "polywire"


def test_get_update_delete_document():
    c = client()
    index = f"smoke_{uuid.uuid4().hex[:8]}"
    c.index(index=index, id="1", body={"name": "polywire"}, refresh=True)

    doc = c.get(index=index, id="1")
    assert doc["_source"]["name"] == "polywire"

    c.delete(index=index, id="1")
    # Real OpenSearch's GET returns HTTP 200 with found=false for a missing document, not a 404 --
    # unlike DELETE, which does 404 when there's nothing to delete.
    after_delete = c.get(index=index, id="1")
    assert after_delete["found"] is False
