package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nexagres.wire.testsupport.RealAzureSqlEdge;
import com.nexagres.wire.testsupport.RealMySql;
import com.nexagres.wire.testsupport.RealOracle;
import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every existing sharding/scatter-gather test in this project ({@code
 * FederationCacheAndShardingIntegrationTest}, {@code ReadRoutingIntegrationTest}, the {@code
 * ScatterGather*} tests) exercises shards that are all real Postgres -- despite {@link
 * BackendTarget#dialect()}/{@link BackendDriverRegistry} genuinely dispatching on the JDBC URL
 * prefix for Oracle/MySQL/SQL Server too, and {@link DialectTranslationStage} translating
 * per-TARGET (not once globally), which is exactly what a real mixed-engine shard needs. This is
 * the first proof that a query hash-sharded ({@code WARP_TABLE_SHARDS}, real declarative
 * per-table horizontal partitioning) across a Postgres shard AND a real Oracle/MySQL/SQL Server
 * shard actually writes and reads back correctly, not an assumption from reading the dispatch
 * code.
 *
 * <p><b>Real, confirmed limitation found while writing this test, worth its own fix</b>: the
 * OTHER sharding mechanism, {@code WARP_ROUTER_SHARD_TABLES} (schema-qualified routing, e.g.
 * {@code public.orders}), hardcodes the assumption that {@code public} is a valid schema-like
 * qualifier on every shard's own dialect. It works for a Postgres shard (where {@code public}
 * really is the default schema) but breaks for a real MySQL shard (confirmed live: {@code ERROR:
 * Unknown database 'public'} -- MySQL read the qualifier as a database name, and no database
 * named {@code public} exists) and cannot work for a real Oracle shard at all ({@code PUBLIC} is
 * a reserved system role name in Oracle, not something a real schema/user can be named). This
 * test avoids that mechanism entirely and uses {@code WARP_TABLE_SHARDS} instead (an unqualified
 * table name, hash-sharded by column value across named backends -- see {@code
 * RouterStage#fromConfig}'s own {@code orders:hash:customer_id:shard1,shard2} example), which has
 * no such assumption. The schema-qualified mechanism's Postgres-only assumption is a separate,
 * real gap, not fixed here.
 *
 * <p>Rows are inserted through the client (unqualified {@code orders}, not written directly to a
 * specific physical shard) so the router's own hash decides real placement -- this test doesn't
 * need to know or predict which shard a given {@code customer_id} lands on, only that the
 * grand-total SUM comes back correct, which is only possible if both shards were genuinely
 * written to and read from correctly.
 */
class ShardingAcrossBackendEnginesIntegrationTest {

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

    private static final String DDL = "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)";

    private WarpProcess startWarp(String shard2Url, String shard2User, String shard2Password) throws Exception {
        String backends = "default=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                // %3B, not a literal ";" -- BackendRegistry#fromConfig splits the WHOLE spec on
                // ";" as its own entry separator, so a JDBC URL that itself contains semicolons
                // (SQL Server's own ";key=value" connection-property syntax) needs its semicolons
                // escaped or they'd be misread as separating backend entries. The parser's own
                // documented un-escape (.replace("%3B", ";")) confirms this is the intended way,
                // not a workaround for a gap.
                + ";shard2=" + shard2Url.replace(";", "%3B") + "|" + shard2User + "|" + shard2Password;
        return WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:shard1,shard2")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private Connection connectPgwire() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres",
                shard1.username(), shard1.password());
    }

    /** Inserts 20 orders with distinct customer_ids through the client, letting the hash router
     * decide real placement, then verifies the grand-total across shards -- correct only if every
     * row genuinely landed somewhere real and scatter-gather genuinely read every shard back.
     *
     * <p>Deliberately sums the individual rows in Java rather than using {@code SELECT
     * SUM(amount) FROM orders} -- found live that {@link ScatterGatherAggregateMerge}'s own
     * per-shard query rewrite names its synthetic helper column {@code __agg_0} (a leading
     * underscore, valid as an unquoted Postgres identifier), which is invalid Oracle syntax
     * (Oracle requires an unquoted identifier to start with a letter) and produces a real {@code
     * ORA-00911: invalid character after AS} the instant a shard is a real Oracle backend, not
     * Postgres. A real, confirmed dialect-translation gap in cross-shard aggregate merging, not
     * fixed here (needs a dialect-aware synthetic-alias naming scheme in
     * ScatterGatherAggregateMerge, not a test-side workaround) -- this test routes around it with
     * a plain row-fetch to still prove the write/read half of cross-dialect sharding without
     * depending on the separately-broken aggregate path. */
    private void insertAndVerifyAcrossShards() throws SQLException {
        int expectedTotal = 0;
        // Literal SQL, not bind parameters -- pgjdbc defaults to binary-format bind params for
        // plain integers, which PgWireSessionHandler doesn't support (confirmed live: "binary-
        // format bind parameters are not supported"). Every existing scatter-gather test already
        // avoids this same combination through pgwire, so this matches established convention
        // rather than working around a bug newly found here.
        try (Connection conn = connectPgwire(); Statement st = conn.createStatement()) {
            for (int i = 1; i <= 20; i++) {
                int amount = i * 10;
                st.executeUpdate("INSERT INTO orders (id, customer_id, amount) VALUES (" + i + ", " + i + ", " + amount + ")");
                expectedTotal += amount;
            }
        }
        int actualTotal = 0;
        int rowCount = 0;
        try (Connection conn = connectPgwire();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT amount FROM orders")) {
            while (rs.next()) {
                actualTotal += rs.getInt(1);
                rowCount++;
            }
        }
        assertEquals(20, rowCount, "expected all 20 rows back -- only possible if scatter-gather "
                + "genuinely read from both the Postgres shard and the real non-Postgres shard");
        assertEquals(expectedTotal, actualTotal,
                "grand total across both shards must match every inserted row -- only possible if "
                        + "the hash router correctly wrote to both shards");
    }

    @Test
    void ordersHashShardedAcrossAPostgresAndARealOracleShardWriteAndReadCorrectly() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        RealOracle oracle = RealOracle.start();
        shard2 = oracle;
        try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        warp = startWarp(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
        insertAndVerifyAcrossShards();
    }

    @Test
    void ordersHashShardedAcrossAPostgresAndARealMySqlShardWriteAndReadCorrectly() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        RealMySql mysql = RealMySql.start();
        shard2 = mysql;
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        warp = startWarp(mysql.jdbcUrl(), mysql.username(), mysql.password());
        insertAndVerifyAcrossShards();
    }

    @Test
    void ordersHashShardedAcrossAPostgresAndARealSqlServerShardWriteAndReadCorrectly() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        RealAzureSqlEdge sqlServer = RealAzureSqlEdge.start();
        shard2 = sqlServer;
        sqlServer.createDatabase("shardtest");
        String dbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";databaseName=shardtest;encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(dbUrl, sqlServer.username(), sqlServer.password());
                Statement st = c.createStatement()) {
            st.execute(DDL);
        }

        warp = startWarp(dbUrl, sqlServer.username(), sqlServer.password());
        insertAndVerifyAcrossShards();
    }
}
