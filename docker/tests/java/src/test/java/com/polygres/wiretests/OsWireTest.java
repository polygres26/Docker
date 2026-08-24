package com.polygres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

/** oswire: real OpenSearch HTTP/JSON API (_search/documents/_bulk), the real official
 * opensearch-java client, translated to Postgres underneath -- see
 * com.polygres.wire.oswire's package for the internal Search IR this is staged around. No auth
 * configured in the test compose file. */
class OsWireTest {

    private static OpenSearchClient client() {
        ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder
                .builder(new org.apache.hc.core5.http.HttpHost("http", TestConfig.HOST, TestConfig.OSWIRE_PORT))
                .build();
        return new OpenSearchClient(transport);
    }

    @Test
    void indexAndSearchByTerm() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("name", "polywire");
        doc1.put("category", "gateway");
        c.index(r -> r.index(index).id("1").document(doc1).refresh(
                org.opensearch.client.opensearch._types.Refresh.True));

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("name", "polyadvisor");
        doc2.put("category", "assessment");
        c.index(r -> r.index(index).id("2").document(doc2).refresh(
                org.opensearch.client.opensearch._types.Refresh.True));

        SearchResponse<Map> response = c.search(s -> s.index(index)
                .query(q -> q.term(t -> t.field("category").value(v -> v.stringValue("gateway")))),
                Map.class);

        assertEquals(1, response.hits().hits().size());
        assertEquals("1", response.hits().hits().get(0).id());
        assertEquals("polywire", response.hits().hits().get(0).source().get("name"));
    }

    @Test
    void getAndDeleteDocument() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "polywire");
        c.index(r -> r.index(index).id("1").document(doc));

        var got = c.get(g -> g.index(index).id("1"), Map.class);
        assertTrue(got.found());
        assertEquals("polywire", got.source().get("name"));

        c.delete(d -> d.index(index).id("1"));
        var afterDelete = c.get(g -> g.index(index).id("1"), Map.class);
        assertFalse(afterDelete.found());
    }
}
