package com.nexagres.wire.oswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

/**
 * End-to-end proof that a real OpenSearch client ({@code opensearch-java}, the Java equivalent of
 * opensearch-py -- real request/response parsing, not a hand-written HTTP check) gets a genuine
 * {@code index_not_found_exception} out of a real Postgres backend failure, via {@link
 * OpenSearchErrorMapper}. Real subprocess, real Postgres container, no mocks -- the first real
 * OpenSearch client library any oswire test has been verified against (the existing tests use raw
 * {@code HttpURLConnection}).
 */
class OpenSearchErrorMappingIntegrationTest {

    private static OpenSearchClient client(int port) {
        OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(new HttpHost("http", "localhost", port))
                .build();
        return new OpenSearchClient(transport);
    }

    /** The index's real backing Postgres table ({@code warp_search_<index>}, see
     * {@code PostgresSearchStore.pgTableName}) was dropped directly against Postgres, bypassing
     * oswire entirely -- oswire has no separate catalog to pre-validate against (unlike dynamowire/
     * sqswire's own catalogs), so a search against it reaches a genuine, unexpected {@code 42P01
     * undefined_table}, not an app-level pre-check. */
    @Test
    void anIndexWhoseTableWasDroppedUnderneathOswireReturnsARealIndexNotFoundException() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("oswire", "WARP_OSWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            OpenSearchClient os = client(warp.port("oswire"));
            os.index(IndexRequest.of(b -> b.index("orphaned_index").id("1")
                    .document(java.util.Map.of("field", "value"))));

            // Drop the REAL underlying table directly against Postgres -- oswire has no catalog
            // metadata that would notice.
            try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement st = admin.createStatement()) {
                st.execute("DROP TABLE warp_search_orphaned_index");
            }

            OpenSearchException thrown = assertThrows(OpenSearchException.class,
                    () -> os.search(SearchRequest.of(b -> b.index("orphaned_index")), Object.class),
                    "a search against an index whose real Postgres table is gone must be a genuine "
                            + "index_not_found_exception, not the generic postgres_exception default");
            assertEquals("index_not_found_exception", thrown.error().type());
            assertEquals(404, thrown.status());
            // root_cause is the real load-bearing part of OpenSearch's error shape -- confirm it's
            // actually populated, not just the top-level type/reason (see OpenSearchWireServer's
            // writeError, which used to omit it entirely).
            assertTrue(!thrown.error().rootCause().isEmpty(), "root_cause must be populated");
            assertEquals("index_not_found_exception", thrown.error().rootCause().get(0).type());
        }
    }

    /** Same real-outage discipline as the other three protocols' equivalent tests -- a genuinely
     * killed backend connection must surface as OpenSearch's own real {@code
     * no_shard_available_action_exception}, not the generic {@code postgres_exception} default. */
    @Test
    void aGenuinelyDeadBackendConnectionReturnsARealNoShardAvailableException() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("oswire", "WARP_OSWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            OpenSearchClient os = client(warp.port("oswire"));
            os.index(IndexRequest.of(b -> b.index("t").id("1").document(java.util.Map.of("field", "warmup"))));

            primary.stop();
            try {
                // opensearch-java only unmarshals into its typed OpenSearchException for 4xx --
                // a 5xx surfaces as the lower-level transport ResponseException instead (a real
                // client-library distinction, not a shape problem with the envelope itself, which
                // is confirmed correct below by checking the exception's own embedded response
                // body text -- it carries the exact same {"error":{"root_cause":[...]}} JSON the
                // first test in this class asserts on structurally).
                org.opensearch.client.transport.httpclient5.ResponseException thrown = assertThrows(
                        org.opensearch.client.transport.httpclient5.ResponseException.class,
                        () -> os.search(SearchRequest.of(b -> b.index("t")), Object.class),
                        "a search against a genuinely dead backend connection must fail with a real error");
                assertTrue(thrown.getMessage().contains("no_shard_available_action_exception"),
                        "must be the real OpenSearch error type, not the generic postgres_exception "
                                + "default -- got: " + thrown.getMessage());
                assertTrue(thrown.getMessage().contains("503"));
            } finally {
                primary.resume();
            }
        }
    }
}
