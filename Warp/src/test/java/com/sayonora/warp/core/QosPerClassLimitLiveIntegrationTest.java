package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real, live, end-to-end proof that {@code WARP_QOS_CLASS_LIMITS} (per-workload-class admission
 * control) actually enforces a DIFFERENT rate for one class than another, through a real pgwire
 * connection -- not just `QosControlStage` driven directly in-process the way every existing test
 * (`QosControlStageTest`, `SessionConnectionLeaseIntegrationTest`) does.
 *
 * <p>Real, confirmed gap this closes (2026-09-29, found while surveying test coverage for "every
 * feature needs to be tested and documented"): {@code WARP_QOS_CLASS_LIMITS}/{@code
 * parseClassLimitsSpec}'s class-limit map was never populated with more than one entry in any
 * existing test -- nothing proved a "write" class limit is enforced differently from "query"/
 * default, despite {@code RouterStage#classifyWorkload} classifying every statement into "query"
 * (SELECT), "write" (INSERT/UPDATE/DELETE/MERGE), "txn" (COMMIT/ROLLBACK), "ddl" (CREATE/ALTER/
 * DROP/TRUNCATE), or "other", and {@code QosControlStage}'s own rejection error message explicitly
 * naming the workload class ("rate limit exceeded for workload class \"write\"").
 *
 * <p>Configures a generous default/"query" limit (effectively unlimited for this test's purposes)
 * and a deliberately tight "write" limit (burst=1), then proves: many rapid SELECTs all succeed
 * (the "query" class is unaffected), while a second rapid INSERT is rejected with SQLState 57014
 * and a message naming the "write" class specifically -- proving the per-class token buckets are
 * genuinely independent, not a single shared bucket the class name is cosmetic for.
 */
class QosPerClassLimitLiveIntegrationTest {

    @Test
    @Timeout(60)
    void writeClassLimitIsEnforcedIndependentlyOfTheQueryClass() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            try (Connection setup = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE qos_class_it (id INT PRIMARY KEY, val TEXT)");
                stmt.execute("INSERT INTO qos_class_it VALUES (1, 'seed')");
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    // Default ("query" and everything else not named below) is generous: this
                    // proves the "write" limit below is a REAL, independent per-class bucket, not
                    // just a globally tight limit that happens to also reject writes.
                    .env("WARP_QOS_RATE_PER_SEC", "1000")
                    .env("WARP_QOS_BURST", "1000")
                    // "write" gets its own deliberately tiny bucket: rate=1/s, burst=1 -- the
                    // FIRST write in any window succeeds, the very next one (no time to
                    // replenish a token) must be rejected.
                    .env("WARP_QOS_CLASS_LIMITS", "write:1:1:0")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/" + postgres.database();
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {

                    // The "query" class must be unaffected by the tight "write" limit: many rapid
                    // SELECTs in a row, well beyond the write class's burst=1, must all succeed.
                    for (int i = 0; i < 20; i++) {
                        st.executeQuery("SELECT val FROM qos_class_it WHERE id = 1").close();
                    }

                    // The FIRST write consumes the "write" class's only token (burst=1) and must
                    // succeed.
                    st.executeUpdate("UPDATE qos_class_it SET val = 'first-write' WHERE id = 1");

                    // The SECOND write, immediately after with no time for the 1/s bucket to
                    // refill, must be rejected by QosControlStage specifically for the "write"
                    // class -- proving the per-class bucket is real and independent of the
                    // still-unaffected "query" class above.
                    SQLException e = assertThrows(SQLException.class,
                            () -> st.executeUpdate("UPDATE qos_class_it SET val = 'second-write' WHERE id = 1"),
                            "the second write in the same burst window must be rejected by the "
                                    + "deliberately tight write:1:1:0 class limit");
                    assertEquals("57014", e.getSQLState(),
                            "QosControlStage's rate-limit rejection must use SQLState 57014");
                    assertTrue(e.getMessage().contains("write"),
                            "the rejection message must name the specific workload class that was "
                                    + "rate-limited (\"write\"), not a generic message: " + e.getMessage());

                    // The "query" class must STILL be unaffected after the write class was
                    // rejected -- confirms the buckets are genuinely independent, not that the
                    // whole connection got globally throttled by the write rejection.
                    for (int i = 0; i < 5; i++) {
                        st.executeQuery("SELECT val FROM qos_class_it WHERE id = 1").close();
                    }
                }
            }
        }
    }
}
