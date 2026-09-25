package com.sayonora.wire.xa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real, end-to-end proof of two things together: (1) {@code WARP_TABLE_SHARDS} shard-key pruning
 * genuinely resolves a query to exactly ONE real shard backend -- both when the shard key is a SQL
 * literal AND when it is a real JDBC bind parameter (the exact gap {@link
 * com.sayonora.wire.core.ValueShardLiteralMatcher#findBindValue} closed this session -- see its own
 * javadoc and {@code RouterStageTest#tableShardRuleMatchesABoundParameterNotJustALiteral} for the
 * unit-level coverage this test proves end to end with a real wire-protocol client instead), and
 * (2) a real two-phase-commit transaction whose two branches are discovered via SHARD routing
 * (rather than {@code WARP_ROUTER_SCHEMA_RULES}, the shape {@link XaRecoveryIntegrationTest}/{@link
 * MySqlXaIntegrationTest} already cover) commits and rolls back correctly across both real Postgres
 * shard backends.
 *
 * <p><b>Mechanically confirmed before writing this test, not assumed</b>: {@link
 * com.sayonora.wire.core.RoutingBackendExecutor#execute} resolves a fresh {@code targetName} for
 * EVERY statement (via {@code RouterStage.handle} on the way in, one call per statement, never
 * cached across a transaction), and {@code executeOnTransactionConnection} keys {@code
 * transactionConnections} by {@code target.name()}, lazily opening (and XA-enlisting via {@code
 * XaTransaction#addBranch}) a new branch connection the first time a given backend name is seen
 * inside the current transaction. So a transaction whose two statements resolve to two DIFFERENT
 * shard-routed backends (one row's shard key routes to shardA, the next row's routes to shardB)
 * genuinely opens two distinct backend connections and gets coordinated by the SAME {@link
 * XaTransaction} as schema-rule routing -- nothing pins the whole transaction to one connection
 * chosen at {@code BEGIN} time. This is real, already-shipped behavior; no fix was needed, and no
 * separate "Test 5" gap exists for this codebase.
 *
 * <p><b>Row-count design, not "does the right row exist somewhere"</b> -- {@code WARP_TABLE_SHARDS}
 * config below deterministically maps {@code customer_id=100} to {@code shardA} and {@code
 * customer_id=200} to {@code shardB} (the {@code list} strategy). Tests 1/2 insert a row with {@code
 * customer_id=100} directly into shardA (the CORRECT shard) and ALSO directly into shardB (a shard
 * that correct routing should never reach for that value) -- bypassing Warp entirely for setup, so
 * the router's own behavior is the only thing under test. A query through Warp for {@code
 * customer_id=100} must then return EXACTLY the one row that lives on shardA: if pruning ever
 * regressed back to scatter-gather (the pre-fix behavior for a bind parameter), the query would
 * additionally pick up shardB's row and the count would silently become 2 instead of 1 -- an exact,
 * mechanical regression signal, not merely "the right rows are somewhere in the result".
 */
class ShardRoutingAndTwoPhaseCommitIntegrationTest {

    private RealPostgres shardA;
    private RealPostgres shardB;
    private WarpProcess warp;

    private static final String DDL = "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, marker TEXT)";

    // customer_id=100 -> shardA, customer_id=200 -> shardB (WARP_TABLE_SHARDS "list" strategy).
    private static final String TABLE_SHARDS = "orders:list:customer_id:shardA=100;shardB=200";

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (shardB != null) shardB.close();
        if (shardA != null) shardA.close();
    }

    private void startShardsAndWarp(java.util.List<String> extraPgConf) throws Exception {
        shardA = RealPostgres.start(extraPgConf);
        shardB = RealPostgres.start(extraPgConf);
        for (RealPostgres shard : new RealPostgres[] { shardA, shardB }) {
            try (Connection c = DriverManager.getConnection(shard.jdbcUrl(), shard.username(), shard.password());
                    Statement st = c.createStatement()) {
                st.execute(DDL);
            }
        }

        String backends = "default=" + shardA.jdbcUrl() + "|" + shardA.username() + "|" + shardA.password()
                + ";shardA=" + shardA.jdbcUrl() + "|" + shardA.username() + "|" + shardA.password()
                + ";shardB=" + shardB.jdbcUrl() + "|" + shardB.username() + "|" + shardB.password();

        warp = WarpProcess.builder()
                .pgBackend(shardA.host(), shardA.port(), shardA.database(), shardA.username(), shardA.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_TABLE_SHARDS", TABLE_SHARDS)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                // This dev machine has an unrelated process that can hold the default gRPC port
                // (7070) -- always pin an ephemeral one instead of relying on the default.
                .env("WARP_GRPC_PORT", String.valueOf(findFreePort()))
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    private Connection connectPgwire() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres",
                shardA.username(), shardA.password());
    }

    private void insertDirectly(RealPostgres shard, int id, int customerId, String marker) throws SQLException {
        try (Connection c = DriverManager.getConnection(shard.jdbcUrl(), shard.username(), shard.password());
                PreparedStatement ps = c.prepareStatement("INSERT INTO orders (id, customer_id, marker) VALUES (?, ?, ?)")) {
            ps.setInt(1, id);
            ps.setInt(2, customerId);
            ps.setString(3, marker);
            ps.executeUpdate();
        }
    }

    @Test
    void literalShardKeyQueryPrunesToTheOneCorrectShardNotBoth() throws Exception {
        startShardsAndWarp(java.util.List.of());

        // customer_id=100 belongs on shardA per WARP_TABLE_SHARDS. Also plant a customer_id=100
        // row directly on shardB (the WRONG shard for that value) so a scatter-gather regression
        // would visibly add a second row to the result.
        insertDirectly(shardA, 1, 100, "onA-correct");
        insertDirectly(shardB, 2, 100, "onB-shouldNeverBeSeen");

        try (Connection conn = connectPgwire();
                Statement st = conn.createStatement();
                // Literal SQL, not a bind parameter -- this is the pre-existing, already-working
                // path (ValueShardLiteralMatcher#findLiteralValue), proven here through a real
                // wire-protocol client rather than RouterStage's own unit-level Statement.
                ResultSet rs = st.executeQuery("SELECT id, marker FROM orders WHERE customer_id = 100")) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                assertEquals(1, rs.getInt("id"), "the only row returned must be shardA's own row");
                assertEquals("onA-correct", rs.getString("marker"));
            }
            assertEquals(1, rowCount, "a literal-valued shard key must prune to exactly shardA -- "
                    + "seeing shardB's row too would mean the query scatter-gathered instead of pruning");
        }
    }

    @Test
    void bindParameterShardKeyQueryAlsoPrunesToTheOneCorrectShardNotBoth() throws Exception {
        startShardsAndWarp(java.util.List.of());

        insertDirectly(shardA, 1, 100, "onA-correct");
        insertDirectly(shardB, 2, 100, "onB-shouldNeverBeSeen");

        try (Connection conn = connectPgwire();
                // A genuine bind parameter, not text substitution -- this is exactly the shape
                // that used to scatter-gather across both shards before ValueShardLiteralMatcher
                // #findBindValue was added (TableShardRule's pruning previously only recognized a
                // literal inlined in the SQL text). Before that fix, this test would have seen
                // BOTH rows (count=2) instead of pruning to shardA alone.
                PreparedStatement ps = conn.prepareStatement("SELECT id, marker FROM orders WHERE customer_id = ?")) {
            ps.setInt(1, 100);
            try (ResultSet rs = ps.executeQuery()) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    assertEquals(1, rs.getInt("id"), "the only row returned must be shardA's own row");
                    assertEquals("onA-correct", rs.getString("marker"));
                }
                assertEquals(1, rowCount, "a bind-parameter-valued shard key must ALSO prune to exactly "
                        + "shardA -- seeing shardB's row too (count=2) is precisely the regression the "
                        + "findBindValue fix closed");
            }
        }
    }

    private String markerOn(RealPostgres shard, int customerId) throws SQLException {
        try (Connection c = DriverManager.getConnection(shard.jdbcUrl(), shard.username(), shard.password());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT marker FROM orders WHERE customer_id = " + customerId)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void twoPhaseCommitTransactionSpanningBothShardsCommitsBothBranches() throws Exception {
        // Real 2PC against real Postgres needs PREPARE TRANSACTION support, which stock Postgres
        // ships disabled (max_prepared_transactions=0) -- same requirement as XaRecoveryIntegrationTest.
        startShardsAndWarp(java.util.List.of("max_prepared_transactions=10"));

        // Pre-seed one row directly on each real shard, bypassing Warp -- WARP_TABLE_SHARDS's
        // literal-value matcher (ValueShardLiteralMatcher#findLiteralValue) only recognizes a
        // "column = value" EQUALITY-COMPARISON shape (a WHERE-clause predicate), not an INSERT's
        // positional "VALUES (...)" list (the same real, documented gap
        // ThreeOracleBackendsHashShardedByCustomerIdIntegrationTest's javadoc calls out for
        // WARP_TABLE_SHARDS specifically). An UPDATE ... WHERE customer_id = <value> is the
        // natural, common real-world SQL shape that DOES route correctly, and is exactly what
        // Tests 1/2 already proved end to end -- so the 2PC transaction below uses that shape.
        insertDirectly(shardA, 1, 100, "before");
        insertDirectly(shardB, 2, 200, "before");

        try (Connection conn = connectPgwire()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                // customer_id=100 routes to shardA, customer_id=200 routes to shardB -- two
                // DIFFERENT shard-routed backends touched inside the SAME explicit transaction.
                st.execute("UPDATE orders SET marker = 'committed' WHERE customer_id = 100");
                st.execute("UPDATE orders SET marker = 'committed' WHERE customer_id = 200");
            }
            conn.commit();
        }

        assertEquals("committed", markerOn(shardA, 100), "shardA's own branch must have committed for real");
        assertEquals("committed", markerOn(shardB, 200), "shardB's own branch must have committed for real");
    }

    @Test
    void twoPhaseCommitTransactionSpanningBothShardsRolledBackAppliesNeitherBranch() throws Exception {
        startShardsAndWarp(java.util.List.of("max_prepared_transactions=10"));

        insertDirectly(shardA, 1, 100, "before");
        insertDirectly(shardB, 2, 200, "before");

        try (Connection conn = connectPgwire()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("UPDATE orders SET marker = 'shouldNotStick' WHERE customer_id = 100");
                st.execute("UPDATE orders SET marker = 'shouldNotStick' WHERE customer_id = 200");
            }
            conn.rollback();
        }

        assertEquals("before", markerOn(shardA, 100), "shardA's branch must NOT have been applied after rollback");
        assertEquals("before", markerOn(shardB, 200), "shardB's branch must NOT have been applied after rollback");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
