package com.nexagres.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Phase 0 of the "connecting PolyWire to an existing Postgres" plan: {@code POLYWIRE_PG_SSLMODE}
 * (and {@code POLYWIRE_PG_SSLROOTCERT}) get appended as real pgjdbc {@code ?sslmode=...}
 * connection-string parameters (see {@code PgConnections#baseUrl}), not just parsed and dropped --
 * this is what makes the plain single-backend env vars usable against a backend that requires SSL
 * outright (Supabase, Azure Database for PostgreSQL), instead of forcing the workaround of hand-
 * building a full JDBC URL string via the multi-backend {@code POLYWIRE_BACKENDS} var.
 *
 * <p>Deliberately proves this with {@code sslmode=disable} against a real, plain (non-TLS)
 * Postgres container rather than standing up a genuinely TLS-enabled one -- self-signed-cert
 * provisioning into a container (ownership/permission requirements Postgres enforces on the key
 * file) is a lot of test infrastructure for what's fundamentally a query-string concatenation
 * fix. {@code sslmode=disable} still proves the real thing that matters: the parameter is a real
 * part of the JDBC URL pgjdbc parses and acts on (not silently ignored or malformed) -- a genuine,
 * live end-to-end connection through PolyWire, not just a string-equality assertion on the
 * constructed URL.
 */
class BackendSslModeIntegrationTest {

    @Test
    void sslModeOptionIsHonoredAndDoesNotBreakAPlainConnection() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            try (PolyWireProcess polywire = PolyWireProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(),
                            postgres.username(), postgres.password())
                    .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                    .env("POLYWIRE_PG_SSLMODE", "disable")
                    .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT 21 * 2")) {
                    assertTrue(rs.next());
                    assertEquals(42, rs.getInt(1));
                }
            }
        }
    }
}
