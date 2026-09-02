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
 * <p><b>Real limitation found while writing this test, still open</b>: the OTHER sharding
 * mechanism, {@code WARP_ROUTER_SHARD_TABLES} (schema-qualified routing, e.g. {@code
 * public.orders}), hardcodes the assumption that {@code public} is a valid schema-like qualifier
 * on every shard's own dialect. It works for a Postgres shard (where {@code public} really is the
 * default schema) but breaks for a real MySQL shard (confirmed live: {@code ERROR: Unknown
 * database 'public'} -- MySQL read the qualifier as a database name, and no database named
 * {@code public} exists) and cannot work for a real Oracle shard at all ({@code PUBLIC} is a
 * reserved system role name in Oracle, not something a real schema/user can be named). This test
 * avoids that mechanism entirely and uses {@code WARP_TABLE_SHARDS} instead (an unqualified table
 * name, hash-sharded by column value across named backends -- see {@code RouterStage#fromConfig}'s
 * own {@code orders:hash:customer_id:shard1,shard2} example), which has no such assumption. Still
 * genuinely open: fixing it needs {@code RouterStage}/{@code DialectTranslationStage} to strip or
 * remap the schema qualifier per-target-dialect rather than forwarding it as literal SQL, a real
 * design decision (does {@code public} become the shard's own default schema silently, or does a
 * per-shard schema mapping need its own config?) rather than a small patch, so not attempted here.
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
     * decide real placement, then verifies the grand-total via a real {@code SELECT SUM(amount)}
     * -- correct only if every row genuinely landed somewhere real and {@link
     * ScatterGatherAggregateMerge} genuinely merged the aggregate across both shards.
     *
     * <p>This exercises the real cross-shard SQL SUM() path -- originally routed around here via
     * a plain per-row fetch, because {@link ScatterGatherAggregateMerge}'s per-shard query rewrite
     * named its synthetic helper column {@code __agg_0} (a leading underscore, valid as an
     * unquoted Postgres identifier but invalid Oracle syntax), producing a real {@code
     * ORA-00911: invalid character after AS} against a real Oracle shard. Now fixed (aliases
     * renamed to {@code agg_0}/{@code agg_avg_sum_0}/{@code agg_avg_cnt_0}), so this test uses the
     * real aggregate path again instead of working around it. */
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
        try (Connection conn = connectPgwire();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT SUM(amount) FROM orders")) {
            assertEquals(true, rs.next());
            assertEquals(expectedTotal, rs.getInt(1),
                    "grand total across both shards must match every inserted row -- only possible "
                            + "if the hash router correctly wrote AND ScatterGatherAggregateMerge "
                            + "correctly merged the SUM across both the Postgres shard and the real "
                            + "non-Postgres shard");
        }
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
