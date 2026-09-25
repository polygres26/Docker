package com.sayonora.wire.boltwire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

/**
 * There is no existing test for boltwire at all -- the source (BoltWireSessionHandler, PackStream,
 * GraphNode) has zero coverage in this project's test suite. This is the first: a real official
 * {@code neo4j-java-driver} (real Bolt 4.4 handshake, real PackStream framing, real HELLO/RUN/
 * PULL/RECORD/SUCCESS/GOODBYE) issuing a real Cypher query against Warp -- no mocks.
 *
 * <p>Scoped to exactly what {@link BoltWireSessionHandler}'s own javadoc documents as implemented
 * (Phase 1): {@code RETURN <literal> [AS <alias>]}, round-tripped through a real {@code SELECT
 * <literal> AS <alias>} against Postgres. Any other Cypher shape is explicitly out of scope for
 * this phase and would get a real Bolt FAILURE, not a wrong answer -- not this test's concern.
 */
class BoltWireRealDriverIntegrationTest {

    @Test
    void aReturnLiteralQueryRoundTripsThroughRealBoltAndRealPostgres() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("boltwire", "WARP_BOLTWIRE_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Driver driver = GraphDatabase.driver("bolt://localhost:" + warp.port("boltwire"),
                    AuthTokens.basic(postgres.username(), postgres.password()))) {
                try (Session session = driver.session()) {
                    Result result = session.run("RETURN 42 AS answer");
                    long answer = result.single().get("answer").asLong();
                    assertEquals(42L, answer,
                            "the literal must round-trip through a real SELECT 42 AS answer against Postgres, not just be echoed back in Java");
                }
            }
        }
    }
}
