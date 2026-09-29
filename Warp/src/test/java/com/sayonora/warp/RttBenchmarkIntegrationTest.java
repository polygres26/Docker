package com.sayonora.warp;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealAzureSqlEdge;
import com.sayonora.warp.testsupport.RealMySql;
import com.sayonora.warp.testsupport.RealOracle;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

/**
 * Real, live round-trip-time (RTT) measurement across every one of Warp's frontend/backend-mode
 * combinations -- requested directly by the user ("measure the RTT for Warp with different
 * backends including Adapt Postgres ... document that. Use a 250 byte result RTT").
 *
 * <h2>Methodology</h2>
 * Every combination selects the identical payload shape: a single row, one {@code VARCHAR(250)}
 * column holding exactly 250 ASCII 'x' characters (the "250 byte result" the request asked for --
 * this is the application-level payload size, not a claim about exact wire bytes, since orawire's
 * binary TTC framing, MySQL/SQL Server/Postgres's text wire formats, and each JDBC driver's own
 * per-row overhead all differ -- a like-for-like comparison across engines has to fix the thing
 * that's actually comparable, the row's own data size, not the wire encoding of it).
 *
 * <p>Each measurement: one real client connection to Warp (or, for the "direct" baselines, straight
 * to the real backend, no Warp in the path), a single literal key-value lookup ({@code SELECT
 * payload FROM rtt_bench WHERE id = 1}) via a plain {@link Statement} re-executed each iteration
 * (not a {@code PreparedStatement} reused across executions -- see {@link #measure}'s own javadoc
 * for the real, live-discovered mssqlwire bug that methodology choice sidesteps), 30 warmup
 * executions (discarded -- JIT warmup, connection/plan caching settle), then 300 measured
 * executions of {@code executeQuery()} + {@code next()} + {@code getString()}, timed with {@code
 * System.nanoTime()} per iteration on the SAME thread (sequential, not concurrent -- this measures
 * latency, not throughput, matching what "RTT" means). Reports min/p50/p90/p99/avg in milliseconds.
 *
 * <p>Real, disclosed limitation: this runs on one shared development machine, not an isolated
 * perf-lab environment, and each mode's real backend (Oracle/MySQL/SQL Server containers, a
 * locally-run Postgres) sits at a different network distance (all loopback/localhost here, so the
 * differences below are driven by protocol/pipeline overhead, not network latency) -- absolute
 * numbers will vary machine to machine; the RELATIVE ordering between modes on the same run is the
 * meaningful result.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RttBenchmarkIntegrationTest {

    private static final int WARMUP_ITERATIONS = 30;
    private static final int MEASURED_ITERATIONS = 300;
    private static final String PAYLOAD_250 = "x".repeat(250);
    private static final List<String> RESULTS = new ArrayList<>();

    private static RealOracle oracle;
    private static RealMySql mysql;
    private static RealAzureSqlEdge sqlServer;
    private static RealPostgres postgres;

    @BeforeAll
    static void startBackends() throws Exception {
        postgres = RealPostgres.start();
        oracle = RealOracle.start();
        mysql = RealMySql.start();
        sqlServer = RealAzureSqlEdge.start();

        // Postgres: seeded once, used by every Adapt-mode combination (orawire/mywire/mssqlwire
        // Adapt all dialect-translate onto this same backend) plus the plain pgwire case.
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE rtt_bench (id INT PRIMARY KEY, payload VARCHAR(250))");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO rtt_bench VALUES (1, ?)")) {
                ins.setString(1, PAYLOAD_250);
                ins.executeUpdate();
            }
        }

        // Real Oracle: used by orawire Relay/Bridge (verbatim execution against this instance).
        try (Connection c = DriverManager.getConnection(oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE rtt_bench (id NUMBER PRIMARY KEY, payload VARCHAR2(250))");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO rtt_bench VALUES (1, ?)")) {
                ins.setString(1, PAYLOAD_250);
                ins.executeUpdate();
            }
        }

        // Real MySQL: used by mywire Relay/Bridge.
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE rtt_bench (id INT PRIMARY KEY, payload VARCHAR(250))");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO rtt_bench VALUES (1, ?)")) {
                ins.setString(1, PAYLOAD_250);
                ins.executeUpdate();
            }
        }

        // Real SQL Server: used by mssqlwire Relay/Bridge.
        sqlServer.createDatabase("rttdb");
        String sqlServerDbUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";databaseName=rttdb;encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(sqlServerDbUrl, sqlServer.username(), sqlServer.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE rtt_bench (id INT PRIMARY KEY, payload VARCHAR(250))");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO rtt_bench VALUES (1, ?)")) {
                ins.setString(1, PAYLOAD_250);
                ins.executeUpdate();
            }
        }
    }

    @AfterAll
    static void printSummaryAndStop() {
        System.out.println();
        System.out.println("=".repeat(100));
        System.out.println("RTT BENCHMARK SUMMARY (250-byte VARCHAR payload, " + MEASURED_ITERATIONS
                + " sequential iterations after " + WARMUP_ITERATIONS + " warmup, prepared statement reused)");
        System.out.println("=".repeat(100));
        for (String line : RESULTS) {
            System.out.println(line);
        }
        System.out.println("=".repeat(100));

        if (oracle != null) oracle.close();
        if (mysql != null) mysql.close();
        if (sqlServer != null) sqlServer.close();
        if (postgres != null) postgres.close();
    }

    // ---- measurement helper --------------------------------------------------------------

    private record Stats(double minMs, double p50Ms, double p90Ms, double p99Ms, double avgMs) {
    }

    /** A single literal key-value lookup, timed round-trip -- deliberately a plain {@link
     * Statement} re-executing the same literal SQL text each iteration, NOT a {@link
     * PreparedStatement} reused across executions. A reused server-side prepared statement with
     * zero bind parameters hits a real, live-discovered mssqlwire bug (a repeated {@code
     * sp_executesql} call for a parameterless statement fails with "sp_executesql call missing a
     * string @stmt parameter" -- see the follow-up task filed for it) -- sidestepped here by using
     * the simpler, more universal methodology every engine supports identically: a literal
     * single-key lookup, exactly what "RTT" means for this benchmark's purpose, without leaning on
     * any driver's specific prepared-statement/RPC caching behavior. */
    private static Stats measure(Connection conn, String sql) throws Exception {
        try (Statement st = conn.createStatement()) {
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    assertTrue(rs.next());
                    rs.getString(1);
                }
            }
            long[] samples = new long[MEASURED_ITERATIONS];
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                long start = System.nanoTime();
                try (ResultSet rs = st.executeQuery(sql)) {
                    assertTrue(rs.next());
                    String payload = rs.getString(1);
                    assertTrue(payload.length() >= 250, "expected the full 250-char payload back, got length "
                            + payload.length());
                }
                samples[i] = System.nanoTime() - start;
            }
            java.util.Arrays.sort(samples);
            double min = samples[0] / 1_000_000.0;
            double p50 = samples[MEASURED_ITERATIONS / 2] / 1_000_000.0;
            double p90 = samples[(int) (MEASURED_ITERATIONS * 0.90)] / 1_000_000.0;
            double p99 = samples[(int) (MEASURED_ITERATIONS * 0.99)] / 1_000_000.0;
            double avg = java.util.Arrays.stream(samples).average().orElse(0) / 1_000_000.0;
            return new Stats(min, p50, p90, p99, avg);
        }
    }

    private static void record(String label, Stats s) {
        String line = String.format(java.util.Locale.ROOT,
                "%-38s min=%6.3fms  p50=%6.3fms  p90=%6.3fms  p99=%6.3fms  avg=%6.3fms",
                label, s.minMs(), s.p50Ms(), s.p90Ms(), s.p99Ms(), s.avgMs());
        RESULTS.add(line);
        System.out.println(line);
    }

    // ---- baselines: direct to the real backend, no Warp in the path ----------------------

    @Test
    @Order(1)
    @Timeout(120)
    void directPostgresBaseline() throws Exception {
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password())) {
            record("Direct Postgres (no Warp)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
        }
    }

    @Test
    @Order(2)
    @Timeout(120)
    void directOracleBaseline() throws Exception {
        try (Connection c = DriverManager.getConnection(oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword())) {
            record("Direct Oracle (no Warp)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
        }
    }

    @Test
    @Order(3)
    @Timeout(120)
    void directMySqlBaseline() throws Exception {
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password())) {
            record("Direct MySQL (no Warp)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
        }
    }

    @Test
    @Order(4)
    @Timeout(120)
    void directSqlServerBaseline() throws Exception {
        String url = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";databaseName=rttdb;encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(url, sqlServer.username(), sqlServer.password())) {
            record("Direct SQL Server (no Warp)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
        }
    }

    // ---- pgwire: Postgres client -> Warp -> Postgres (the "Adapt Postgres" case) ----------

    @Test
    @Order(5)
    @Timeout(120)
    void pgwireAdaptPostgres() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/" + postgres.database();
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("pgwire Adapt (Postgres->Warp->Postgres)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    // ---- orawire: Relay / Bridge / Adapt ---------------------------------------------------

    @Test
    @Order(6)
    @Timeout(180)
    void orawireRelay() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_ORACLE_BACKEND_MODE", "relay")
                .env("WARP_ORACLE_HOST", oracle.host())
                .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/" + oracle.serviceName();
            try (Connection c = DriverManager.getConnection(url, oracle.sysUsername(), oracle.sysPassword())) {
                record("orawire Relay (Oracle->Warp->real Oracle)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(7)
    @Timeout(180)
    void orawireBridge() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_ORACLE_BACKEND_MODE", "bridge")
                .env("WARP_ORACLE_HOST", oracle.host())
                .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                .env("WARP_ORACLE_USER", oracle.sysUsername())
                .env("WARP_ORACLE_PASSWORD", oracle.sysPassword())
                .env("WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS", "app_user1=" + oracle.sysPassword())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
            try (Connection c = DriverManager.getConnection(url, "app_user1", oracle.sysPassword())) {
                record("orawire Bridge (Oracle->Warp->real Oracle, pooled)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(8)
    @Timeout(120)
    void orawireAdapt() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("orawire Adapt (Oracle->Warp->Postgres, translated)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    // ---- mywire: Relay / Bridge / Adapt -----------------------------------------------------

    @Test
    @Order(9)
    @Timeout(180)
    void mywireRelay() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_MYWIRE_BACKEND_MODE", "relay")
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/" + mysql.database()
                    + "?useSSL=false&allowPublicKeyRetrieval=true";
            try (Connection c = DriverManager.getConnection(url, mysql.username(), mysql.password())) {
                record("mywire Relay (MySQL->Warp->real MySQL)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(10)
    @Timeout(180)
    void mywireBridge() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_MYWIRE_BACKEND_MODE", "bridge")
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/" + mysql.database()
                    + "?useSSL=false&allowPublicKeyRetrieval=true";
            // Bridge's client-facing login is Warp's own CredentialStore (WARP_AUTH_USER/PASSWORD,
            // seeded by pgBackend() from the Postgres creds), NOT the real MySQL credentials --
            // see MySqlBridgePool's own javadoc.
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("mywire Bridge (MySQL->Warp->real MySQL, pooled)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(11)
    @Timeout(120)
    void mywireAdapt() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/" + postgres.database()
                    + "?useSSL=false&allowPublicKeyRetrieval=true";
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("mywire Adapt (MySQL->Warp->Postgres, translated)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    // ---- mssqlwire: Relay / Bridge / Adapt --------------------------------------------------

    @Test
    @Order(12)
    @Timeout(180)
    void mssqlwireRelay() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_MSSQLWIRE_BACKEND_MODE", "relay")
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", "rttdb")
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                    + ";databaseName=rttdb;encrypt=false;trustServerCertificate=true";
            try (Connection c = DriverManager.getConnection(url, sqlServer.username(), sqlServer.password())) {
                record("mssqlwire Relay (SQLServer->Warp->real SQLServer)", measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(13)
    @Timeout(180)
    void mssqlwireBridge() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_MSSQLWIRE_BACKEND_MODE", "bridge")
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", "rttdb")
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                    + ";encrypt=false;trustServerCertificate=true";
            // Bridge's client-facing login is Warp's own CredentialStore, not the real SQL Server
            // credentials -- see MssqlBridgePool's own javadoc.
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("mssqlwire Bridge (SQLServer->Warp->real SQLServer, pooled)",
                        measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }

    @Test
    @Order(14)
    @Timeout(120)
    void mssqlwireAdapt() throws Exception {
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;trustServerCertificate=true";
            try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                record("mssqlwire Adapt (SQLServer->Warp->Postgres, translated)",
                        measure(c, "SELECT payload FROM rtt_bench WHERE id = 1"));
            }
        }
    }
}
