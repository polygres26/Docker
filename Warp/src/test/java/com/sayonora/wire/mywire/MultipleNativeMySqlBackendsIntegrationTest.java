package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the headline case this turn's work was for: TWO real MySQL backends live at the same
 * time (not just one, per the earlier {@code MySqlNativeBackendIntegrationTest}), reached via a
 * real mywire (native-mode) client, with a real router rule genuinely choosing which one a given
 * row lands on -- proving {@link com.sayonora.wire.core.RouterStage}'s rule-matching runs for
 * native-mode statements exactly like it always has for pgwire/translated ones (see
 * {@code ShardingAcrossBackendEnginesIntegrationTest}, which proves the same kind of routing but
 * only ever via a pgwire client against one Postgres + one other-engine shard).
 *
 * <p><b>Bind-parameter routing (WARP_ROUTER_VALUE_SHARD_RULES), not WARP_TABLE_SHARDS, and why</b>:
 * found live while writing this test -- {@code WARP_TABLE_SHARDS}'s per-row routing goes through
 * {@link com.sayonora.wire.core.ValueShardLiteralMatcher}, which only recognizes {@code column =
 * value} equality syntax (a WHERE-clause shape). An {@code INSERT INTO t (col) VALUES (val)}
 * statement has no such substring at all -- the column name and its value are related
 * positionally, not by an {@code =} -- so it never matches, and every insert silently falls
 * through to {@link com.sayonora.wire.core.RouterStage}'s no-rule-matched fallback instead of the
 * intended shard (a real, separate, pre-existing limitation, not something this turn's work
 * introduced or is scoped to fix). {@code WARP_ROUTER_VALUE_SHARD_RULES} instead reads
 * {@code Statement.bindParams()} by position, which is populated identically for every statement
 * shape a client sends as a bound parameter -- so this test issues the insert as a real
 * {@code PreparedStatement} with {@code customer_id} as its own bind parameter, letting the value-
 * shard rule route it correctly.
 *
 * <p>{@code WARP_MYWIRE_BACKEND=mysql} is on, but neither MySQL target here is named
 * {@code "mysql-native"} (the reserved single-target default -- see
 * {@link com.sayonora.wire.core.BackendRegistry#MYSQL_NATIVE_DEFAULT_NAME}): with two named,
 * non-reserved targets and an explicit rule, the rule decides, not the reserved-name fallback --
 * exactly the point {@code RouterStageDialectDefaultFallbackTest} covers at the unit level.
 */
class MultipleNativeMySqlBackendsIntegrationTest {

    private RealPostgres postgres;
    private RealMySql mysqlA;
    private RealMySql mysqlB;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (mysqlB != null) mysqlB.close();
        if (mysqlA != null) mysqlA.close();
        if (postgres != null) postgres.close();
    }

    private static final String DDL = "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)";

    /** {@code WarpProcess}'s own readiness check only confirms the TCP port accepts connections,
     * not that the auth backend behind it (roleAuthCache, for {@code WARP_AUTH_MODE=postgres_roles})
     * has finished its own warmup -- a real, pre-existing race also seen intermittently in the
     * already-shipped {@code MySqlJdbcIntegrationTest} (unrelated to this test's own changes), not
     * something introduced here. A short connect-retry is the pragmatic fix at the test level;
     * fixing the race in {@code WarpProcess}'s readiness check itself is a separate, broader change
     * out of scope for this test. */
    private Connection connectWithRetry(String url) throws SQLException, InterruptedException {
        SQLException last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                return DriverManager.getConnection(url, postgres.username(), postgres.password());
            } catch (SQLException e) {
                last = e;
                Thread.sleep(500);
            }
        }
        throw last;
    }

    @Test
    void ordersHashShardedAcrossTwoRealMySqlBackendsViaANativeMywireClientWriteAndReadCorrectly() throws Exception {
        postgres = RealPostgres.start();

        mysqlA = RealMySql.start();
        mysqlB = RealMySql.start();
        for (RealMySql mysql : new RealMySql[] { mysqlA, mysqlB }) {
            try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = c.createStatement()) {
                st.execute(DDL);
            }
        }

        // An explicit "default=" entry is required here: BackendRegistry.fromConfig only falls
        // back to auto-registering the implicit WARP_* Postgres target as "default" when
        // WARP_BACKENDS is completely UNSET -- a non-blank spec (this one) is taken as the whole
        // backend list, so without this entry nothing is ever named "default" and sqswire (and
        // anything else that resolves the "default" name directly) fails at startup with
        // "no \"default\" backend registered". Same convention ShardingAcrossBackendEnginesIntegrationTest
        // already follows.
        String backends = "default=" + postgres.jdbcUrl() + "|" + postgres.username() + "|" + postgres.password()
                + ";mysql-a=" + mysqlA.jdbcUrl() + "|" + mysqlA.username() + "|" + mysqlA.password()
                + ";mysql-b=" + mysqlB.jdbcUrl() + "|" + mysqlB.username() + "|" + mysqlB.password();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_MYWIRE_BACKEND", "mysql")
                // Still required: the native-mode session handler's own connection details, used
                // only if neither a rule nor the reserved "mysql-native" name resolves a target --
                // not exercised by this test's INSERT path (the value-shard rule below always
                // matches), but Main.java reads these unconditionally whenever native mode is on.
                .env("WARP_MYSQL_HOST", mysqlA.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysqlA.port()))
                .env("WARP_MYSQL_DATABASE", mysqlA.database())
                .env("WARP_MYSQL_USER", mysqlA.username())
                .env("WARP_MYSQL_PASSWORD", mysqlA.password())
                .env("WARP_BACKENDS", backends)
                // Bind index 1 (0-based) is customer_id in "INSERT INTO orders (id, customer_id,
                // amount) VALUES (?, ?, ?)" below.
                .env("WARP_ROUTER_VALUE_SHARD_RULES", "1:hash:mysql-a,mysql-b")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        // useServerPrepStmts=true is required: MySQL Connector/J's DEFAULT for a PreparedStatement
        // is to inline bind values into literal SQL text client-side and send a plain COM_QUERY --
        // confirmed live as the real cause of an earlier version of this test always landing every
        // row on the same backend (Statement.bindParams() arrived empty, so the value-shard rule
        // below never had anything to match on bind index 1, and every insert silently fell
        // through to the reserved "mysql-native" default). This forces a genuine COM_STMT_PREPARE/
        // COM_STMT_EXECUTE round trip, so mywire's own bindParams decoding (MySqlWireSessionHandler
        // #handleExecute) has real values to hand RouterStage.
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        int expectedTotal = 0;
        try (Connection conn = connectWithRetry(url);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= 20; i++) {
                int amount = i * 10;
                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.setInt(3, amount);
                ps.executeUpdate();
                expectedTotal += amount;
            }
        }

        // Confirm the router genuinely split rows across BOTH real backends, not all onto one --
        // otherwise this wouldn't be proving multi-backend routing at all, just single-target
        // native mode again under a different name.
        int countA;
        int countB;
        int sumA;
        int sumB;
        try (Connection c = DriverManager.getConnection(mysqlA.jdbcUrl(), mysqlA.username(), mysqlA.password());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM orders")) {
            rs.next();
            countA = rs.getInt(1);
            sumA = rs.getInt(2);
        }
        try (Connection c = DriverManager.getConnection(mysqlB.jdbcUrl(), mysqlB.username(), mysqlB.password());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM orders")) {
            rs.next();
            countB = rs.getInt(1);
            sumB = rs.getInt(2);
        }
        assertEquals(20, countA + countB, "every row must land on exactly one of the two real MySQL backends");
        assertTrue(countA > 0 && countB > 0,
                "the hash rule must genuinely split rows across BOTH real backends, not collapse onto one "
                        + "(countA=" + countA + ", countB=" + countB + ")");
        assertEquals(expectedTotal, sumA + sumB,
                "every inserted amount must be present, undamaged, on whichever real backend it landed on");
    }
}
