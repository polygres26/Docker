package com.sayonora.wire.xa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real regression guard for the one claim in {@link XaBackendFactory}'s own javadoc that no
 * existing test independently confirmed: that a real MySQL backend (via the real, Oracle-published
 * Connector/J {@code MysqlXADataSource} -- explicitly NOT the community MariaDB driver, which has
 * no usable {@code XADataSource}) genuinely participates as a real XA branch, not just a
 * best-effort/degraded path. Same infra style as {@link XaRecoveryIntegrationTest} (real
 * containers, a real {@code Main} subprocess coordinator, no mocks) but with one branch on
 * Postgres and the other on a real MySQL container, proving cross-ENGINE two-phase commit, not
 * just cross-INSTANCE (two Postgres backends, already covered).
 */
class MySqlXaIntegrationTest {

    @Test
    void twoPhaseCommitAcrossARealPostgresAndARealMySqlBackendAppliesBothBranches() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres pgBackend = RealPostgres.start(java.util.List.of("max_prepared_transactions=10"));
                RealMySql mysqlBackend = RealMySql.start()) {

            try (Connection c = DriverManager.getConnection(pgBackend.jdbcUrl(), pgBackend.username(), pgBackend.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA shopA");
                st.execute("CREATE TABLE shopA.orders (id int)");
            }
            // RouterStage.SchemaRule (unlike the shard-scatter path's own
            // stripShardSchemaQualifiers) forwards a schema-qualified statement UNCHANGED to
            // whichever backend its schema prefix resolves to -- it never strips or rewrites the
            // qualifier. So the schema-rule name must be a REAL schema/database on its own
            // backend, exactly as XaRecoveryIntegrationTest's "shopA"/"shopB" are real Postgres
            // schemas on their own backends. MySQL's database IS its schema-equivalent, so the
            // rule name here is the real MySQL container's own database name, not an arbitrary
            // label.
            String mysqlDb = mysqlBackend.database();
            try (Connection c = DriverManager.getConnection(mysqlBackend.jdbcUrl(), mysqlBackend.username(), mysqlBackend.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE orders (id int)");
            }

            // Same WARP_BACKENDS/WARP_ROUTER_SCHEMA_RULES shape XaRecoveryIntegrationTest already
            // establishes -- "default" (Postgres, for the control-plane/catalog home) plus one
            // named entry per real branch, routed by schema prefix.
            String backends = "default=" + pgBackend.jdbcUrl() + "|" + pgBackend.username() + "|" + pgBackend.password()
                    + ";backendA=" + pgBackend.jdbcUrl() + "|" + pgBackend.username() + "|" + pgBackend.password()
                    + ";backendB=" + mysqlBackend.jdbcUrl().replace(";", "%3B") + "|" + mysqlBackend.username()
                    + "|" + mysqlBackend.password();

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", backends)
                    .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("WARP_ROUTER_SCHEMA_RULES", "shopA:backendA," + mysqlDb + ":backendB")
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                try (Connection conn = DriverManager.getConnection(url, pgBackend.username(), pgBackend.password())) {
                    conn.setAutoCommit(false);
                    try (Statement st = conn.createStatement()) {
                        st.execute("INSERT INTO shopA.orders (id) VALUES (1)");
                        st.execute("INSERT INTO " + mysqlDb + ".orders (id) VALUES (2)");
                    }
                    conn.commit();
                }
            }

            try (Connection c = DriverManager.getConnection(pgBackend.jdbcUrl(), pgBackend.username(), pgBackend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM shopA.orders")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "the Postgres branch must have committed for real");
            }
            try (Connection c = DriverManager.getConnection(mysqlBackend.jdbcUrl(), mysqlBackend.username(), mysqlBackend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM orders")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "the real MySQL branch must ALSO have committed -- "
                        + "proving XaBackendFactory's real MysqlXADataSource genuinely participated "
                        + "in two-phase commit, not a degraded/best-effort path");
            }
        }
    }
}
