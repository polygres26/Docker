package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.license.LicenseKeyGenTool;
import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link ShardingAcrossBackendEnginesIntegrationTest} proves ONE real Oracle shard alongside a
 * Postgres shard, via literal SQL through pgwire; this proves the case that's never been exercised
 * end to end: THREE real Oracle backends, no Postgres shard among them at all, hash-sharded by
 * {@code customer_id} -- and it exercises a genuinely different, previously unit-test-only path:
 * the rule's backend list names a {@code WARP_BACKEND_SETS} SET ({@code oracle-shards}) instead of
 * listing {@code ora1,ora2,ora3} directly. {@link RouterStage#expandBackendSets}, until now, was
 * only proven against an in-memory {@link BackendRegistry} built by hand
 * ({@code RouterStageBackendSetExpansionTest}), never against a real {@code WarpProcess} routing
 * real statements to real backends started from a real {@code WARP_BACKEND_SETS} env var.
 *
 * <p><b>Why bind-parameter routing (WARP_ROUTER_VALUE_SHARD_RULES), not WARP_TABLE_SHARDS, and
 * why mywire, not pgwire</b> -- both found live writing this test:
 * <ul>
 * <li>{@code WARP_TABLE_SHARDS}'s per-row routing goes through
 * {@link ValueShardLiteralMatcher}, which only recognizes {@code column = value} equality syntax
 * (a WHERE-clause shape, per its own javadoc). An {@code INSERT INTO t (col) VALUES (val)}
 * statement has no such substring -- the column and its value are related positionally, not by an
 * {@code =} -- so it never matches, and {@link RouterStage#resolveBackend} falls through to
 * {@code resolveUnambiguousDefault} for every insert, same real, pre-existing gap
 * {@code MultipleNativeMySqlBackendsIntegrationTest} documents for the identical reason. (A prior
 * version of this test used {@code WARP_TABLE_SHARDS} with literal-SQL inserts over pgwire and
 * every row silently landed on the "default" backend instead of being sharded at all -- caught
 * only because this test asserts each real backend independently received rows, not just a
 * grand-total SUM, which a single-backend landing would still get right by accident.)
 * {@code WARP_ROUTER_VALUE_SHARD_RULES} instead reads {@code Statement.bindParams()} by position,
 * populated identically regardless of statement shape, so this test issues the insert as a real
 * {@code PreparedStatement} with {@code customer_id} as its own bind parameter.
 * <li>pgjdbc defaults to binary-format bind parameters for plain integers, which
 * {@code PgWireSessionHandler} doesn't support -- confirmed live via a genuine "binary-format bind
 * parameters are not supported" failure the first time this test tried a real
 * {@code PreparedStatement} over pgwire. mywire has no such restriction (see
 * {@code MultipleNativeMySqlBackendsIntegrationTest}'s own real bind-param routing over mywire),
 * so this test's client is mywire even though every registered backend is Oracle -- exactly the
 * "different frontend protocol than every backend's own dialect" case {@code DialectTranslationStage}
 * translates per-target for. {@code WARP_MYWIRE_BACKEND} is deliberately left UNSET: this proves
 * default/translating mode, not native-backend mode -- a different, narrower code path already
 * covered by {@code MultipleNativeMySqlBackendsIntegrationTest} itself.
 * </ul>
 *
 * <p>Rows are inserted through the client (unqualified {@code orders}, not written directly to a
 * specific physical shard) so the hash router's own decision decides real placement -- this test
 * doesn't predict which shard a given {@code customer_id} lands on, only that (a) the grand total
 * across all three, summed from each real backend's own table directly, matches every inserted
 * row, and (b) all three real Oracle instances genuinely received at least one row apiece --
 * otherwise a bug that silently collapsed the set to one member would still pass a total-only
 * check even though it isn't sharding at all.
 */
class ThreeOracleBackendsHashShardedByCustomerIdIntegrationTest {

    private RealPostgres postgres;
    private RealOracle ora1;
    private RealOracle ora2;
    private RealOracle ora3;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (ora3 != null) ora3.close();
        if (ora2 != null) ora2.close();
        if (ora1 != null) ora1.close();
        if (postgres != null) postgres.close();
    }

    private static final String DDL = "CREATE TABLE orders (id NUMBER PRIMARY KEY, customer_id NUMBER, amount NUMBER)";

    // Same real, deliberately-offline Ed25519 signing key LicenseIntegrationTest itself commits
    // for exactly this reason -- four real backends (default + 3 Oracle shards) exceeds the
    // Developer-edition 3-backend cap (see BackendRegistry.fromConfig's own license check), and
    // this test's whole point is proving genuine 3-way sharding, not working around the cap by
    // shrinking to 2 real shards.
    private static final String LICENSE_PRIVATE_KEY_B64 =
            "MC4CAQAwBQYDK2VwBCIEIL6Icy/4IPbMRpzHSxFIQqyLwDXKgcP7T/Y2UWJjvfE6";

    private static String generateEnterpriseLicenseKey() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        try {
            LicenseKeyGenTool.main(List.of(
                    "--private-key", LICENSE_PRIVATE_KEY_B64, "--tier", "ENTERPRISE", "--licensed-to", "Test Suite"
            ).toArray(new String[0]));
        } finally {
            System.setOut(original);
        }
        return out.toString(StandardCharsets.UTF_8).strip();
    }

    private int rowCountOn(RealOracle oracle) throws SQLException {
        try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM orders")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private int totalAmountOn(RealOracle oracle) throws SQLException {
        try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT COALESCE(SUM(amount), 0) FROM orders")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void ordersHashShardedAcrossThreeRealOracleBackendsViaANamedBackendSetWriteAndReadCorrectly() throws Exception {
        // Postgres here is Warp's own control-plane database only (warp_config, XA recovery,
        // etc.) plus the reserved "default" WARP_BACKENDS entry BackendRegistry.fromConfig
        // requires whenever the spec isn't left unset (see MultipleNativeMySqlBackendsIntegrationTest's
        // own javadoc on this) -- it holds none of the orders data, all three real shards do, and
        // every insert below carries a real customer_id bind value so none should ever fall
        // through to it at all.
        postgres = RealPostgres.start();

        ora1 = RealOracle.start();
        ora2 = RealOracle.start();
        ora3 = RealOracle.start();
        for (RealOracle oracle : new RealOracle[] { ora1, ora2, ora3 }) {
            try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement st = c.createStatement()) {
                st.execute(DDL);
            }
        }

        String backends = "default=" + postgres.jdbcUrl() + "|" + postgres.username() + "|" + postgres.password()
                + ";ora1=" + ora1.jdbcUrl() + "|" + ora1.sysUsername() + "|" + ora1.sysPassword()
                + ";ora2=" + ora2.jdbcUrl() + "|" + ora2.sysUsername() + "|" + ora2.sysPassword()
                + ";ora3=" + ora3.jdbcUrl() + "|" + ora3.sysUsername() + "|" + ora3.sysPassword();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_LICENSE_KEY", generateEnterpriseLicenseKey())
                .env("WARP_BACKENDS", backends)
                .env("WARP_BACKEND_SETS", "oracle-shards=ora1,ora2,ora3")
                // The set name, not the raw "ora1,ora2,ora3" list -- the whole point of this test
                // is proving RouterStage#expandBackendSets resolves it correctly end to end. Bind
                // index 1 (0-based) is customer_id in "INSERT INTO orders (id, customer_id,
                // amount) VALUES (?, ?, ?)" below.
                .env("WARP_ROUTER_VALUE_SHARD_RULES", "1:hash:oracle-shards")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        int expectedTotal = 0;
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        // useServerPrepStmts=true forces a genuine COM_STMT_PREPARE/COM_STMT_EXECUTE round trip --
        // MySQL Connector/J's default inlines bind values into literal SQL client-side instead,
        // which would arrive at mywire with an empty bindParams() and never match the rule at all
        // (same real gap MultipleNativeMySqlBackendsIntegrationTest found and documents).
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= 30; i++) {
                int amount = i * 10;
                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.setInt(3, amount);
                ps.executeUpdate();
                expectedTotal += amount;
            }
        }

        int countA = rowCountOn(ora1);
        int countB = rowCountOn(ora2);
        int countC = rowCountOn(ora3);
        int totalA = totalAmountOn(ora1);
        int totalB = totalAmountOn(ora2);
        int totalC = totalAmountOn(ora3);

        assertEquals(30, countA + countB + countC,
                "every row must land on exactly one of the three real Oracle backends the "
                        + "\"oracle-shards\" set resolved to");
        assertTrue(countA > 0 && countB > 0 && countC > 0,
                "the hash rule must genuinely split rows across all THREE real backends the "
                        + "\"oracle-shards\" set names, not collapse onto one or two of them "
                        + "(countA=" + countA + ", countB=" + countB + ", countC=" + countC + ")");
        assertEquals(expectedTotal, totalA + totalB + totalC,
                "every inserted amount must be present, undamaged, on whichever real Oracle "
                        + "backend it landed on");
    }
}
