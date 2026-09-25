package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the fix documented in {@link RoutingBackendExecutor#stripShardSchemaQualifiers}: the
 * schema-qualified {@code WARP_ROUTER_SHARD_TABLES} mechanism (a client types {@code
 * public.orders} to trigger scatter-gather) now works against real Oracle/MySQL shards, not just
 * Postgres -- the "public." qualifier is stripped before the SQL reaches any shard, instead of
 * being forwarded as literal (and, on Oracle/MySQL, invalid) SQL text.
 *
 * <p>Same DDL/seeding shape as {@link ShardingAcrossBackendEnginesIntegrationTest}, but routed via
 * the SCHEMA-qualified mechanism ({@code WARP_ROUTER_SHARD_TABLES=public}, a client query
 * literally reading {@code public.orders}) rather than {@code WARP_TABLE_SHARDS}'s unqualified,
 * hash-based routing -- this is specifically a regression test for the schema-qualifier bug, not
 * a second proof of general cross-dialect sharding (that's already covered).
 */
class SchemaQualifiedShardingAcrossBackendEnginesIntegrationTest {

    private RealPostgres shard1;
    private AutoCloseable shard2;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (shard2 != null) {
            try {
                shard2.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        if (shard1 != null) shard1.close();
    }

    private static final String DDL = "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)";

    private WarpProcess startWarp(String shard2Url, String shard2User, String shard2Password) throws Exception {
        String backends = "shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard2=" + shard2Url.replace(";", "%3B") + "|" + shard2User + "|" + shard2Password;
        return WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_SHARD_BACKENDS", "shard1,shard2")
                .env("WARP_ROUTER_SHARD_TABLES", "public")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private void verifySchemaQualifiedScatterSum(int expectedTotal) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", shard1.username(), shard1.password());
                Statement st = conn.createStatement();
                // The literal "public." qualifier IS the point of this test -- it's what triggers
                // ShardRule's own match in RouterStage, same as any real client relying on this
                // documented mechanism would type.
                ResultSet rs = st.executeQuery("SELECT SUM(amount) FROM public.orders")) {
            assertEquals(true, rs.next());
            assertEquals(expectedTotal, rs.getInt(1),
                    "grand total across both shards via the SCHEMA-qualified scatter path -- only "
                            + "correct if the \"public.\" qualifier was stripped before reaching "
                            + "the real non-Postgres shard, not forwarded as literal (invalid) SQL");
        }
    }

    @Test
    void schemaQualifiedScatterAcrossAPostgresAndARealOracleShardStripsThePublicQualifier() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
            st.execute("INSERT INTO orders (id, amount) VALUES (1, 100)");
        }

        RealOracle oracle = RealOracle.start();
        shard2 = oracle;
        try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement()) {
            st.execute(DDL);
            st.execute("INSERT INTO orders (id, amount) VALUES (2, 200)");
        }

        warp = startWarp(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
        verifySchemaQualifiedScatterSum(300);
    }

    @Test
    void schemaQualifiedScatterAcrossAPostgresAndARealMySqlShardStripsThePublicQualifier() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
            st.execute("INSERT INTO orders (id, amount) VALUES (1, 100)");
        }

        RealMySql mysql = RealMySql.start();
        shard2 = mysql;
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
            st.execute("INSERT INTO orders (id, amount) VALUES (2, 200)");
        }

        warp = startWarp(mysql.jdbcUrl(), mysql.username(), mysql.password());
        verifySchemaQualifiedScatterSum(300);
    }
}
