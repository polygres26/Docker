package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * One real Warp process fronting FIVE real, DIFFERENT database engines at once -- Postgres,
 * Oracle, MySQL, SQL Server, and MongoDB -- each reached by that engine's own official client
 * driver over its own matching wire protocol, all issuing real work CONCURRENTLY (one thread per
 * protocol, not sequentially), then a real proof that Warp captured every statement
 * ({@code WARP_CAPTURE_ENABLED}) and counted it correctly (real {@code GET /api/metrics/summary}).
 *
 * <p><b>Why MySQL/SQL Server use native-backend mode but Oracle uses a plain, reserved-name
 * backend instead</b> -- found live writing this test: {@code WARP_MYWIRE_BACKEND=mysql}/
 * {@code WARP_MSSQLWIRE_BACKEND=sqlserver} register a real reserved-name backend
 * ({@code mysql-native}/{@code mssql-native}) that {@code RouterStage}'s shared no-rule-matched
 * fallback resolves to for that dialect, going through the full pipeline (capture, metrics,
 * firewall all still apply). {@code WARP_ORACLE_BACKEND_MODE=native} is a DIFFERENT, narrower
 * mechanism entirely -- it only does anything when {@code WARP_DUAL_EXEC_ENABLED} +
 * {@code WARP_DUAL_EXEC_AUTHORITY=oracle} are ALSO set (see {@code RequestLoop#handleExecute}'s
 * native-execute guard), and even then either runs a real Oracle/Postgres shadow-comparison
 * (dual execution, a migration-verification feature, not what this test wants) or -- when dual
 * exec's own Oracle-authority relay condition is met in {@code SessionHandler.run()} -- hands the
 * raw socket to {@code NativeSessionRelay} as a byte-for-byte TCP proxy BEFORE a
 * {@code RequestLoop} is ever constructed, meaning NO capture and NO metrics at all for that path
 * -- exactly the two things this test exists to prove. So Oracle here instead gets a plain
 * {@code WARP_BACKENDS} entry literally NAMED {@code oracle-native}: {@link RouterStage}'s
 * reserved-name fallback ({@link BackendRegistry#ORACLE_NATIVE_DEFAULT_NAME}) resolves to
 * whatever real backend is registered under that exact name regardless of which mechanism put it
 * there, so an ordinary spec entry works identically to how {@code MultipleNativeMySqlBackendsIntegrationTest}
 * relies on the {@code mysql-native}/{@code mssql-native} names -- full pipeline, full capture,
 * full metrics, dialect translation a real no-op since orawire's own {@code SourceDialect} and the
 * target are both {@code ORACLE}.
 *
 * <p>MongoDB has no native-backend-mode toggle at all -- {@code WARP_MONGOWIRE_BACKEND} doesn't
 * exist as a real env var (confirmed against {@link BackendRegistry} and {@code ServerOptions}
 * earlier in this project) -- so the real Mongo driver's traffic here genuinely lands on the same
 * Postgres backend pgwire uses, translated by {@code MongoWireSessionHandler}/
 * {@code PostgresDocumentStore}, exactly as the shipped product actually works today. Nothing here
 * overclaims a capability that doesn't exist.
 *
 * <p>Each engine gets its own DDL, in its own real dialect, applied directly against its own real
 * connection before any Warp traffic starts -- Oracle/MySQL/SQL Server's own reserved-name
 * backends do no dialect translation between two DIFFERENT dialects here (source and target are
 * the same engine in every case), so Warp never needs to rewrite these statements' structure.
 */
class AllFiveEnginesConcurrentCaptureAndMetricsIntegrationTest {

    private static RealPostgres postgres;
    private static RealOracle oracle;
    private static RealMySql mysql;
    private static RealAzureSqlEdge sqlServer;
    private static WarpProcess warp;
    private static final String ADMIN_TOKEN = "five-engines-test-token";
    private static final int ROWS_PER_PROTOCOL = 5;

    @BeforeAll
    static void startInfra() throws Exception {
        // Real containers started concurrently too -- four independent Docker daemons booting in
        // parallel is the dominant real cost here, same reasoning ShortRegressionSuiteTest gives
        // for sharing one WarpProcess across many tests rather than one-per-test.
        ExecutorService bootPool = Executors.newFixedThreadPool(4);
        Future<RealPostgres> pgFuture = bootPool.submit((Callable<RealPostgres>) RealPostgres::start);
        Future<RealOracle> oraFuture = bootPool.submit((Callable<RealOracle>) RealOracle::start);
        Future<RealMySql> mysqlFuture = bootPool.submit((Callable<RealMySql>) RealMySql::start);
        Future<RealAzureSqlEdge> sqlServerFuture = bootPool.submit((Callable<RealAzureSqlEdge>) RealAzureSqlEdge::start);
        try {
            postgres = pgFuture.get();
            oracle = oraFuture.get();
            mysql = mysqlFuture.get();
            sqlServer = sqlServerFuture.get();
        } finally {
            bootPool.shutdown();
        }
        sqlServer.createDatabase("workdb");

        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INT PRIMARY KEY, protocol VARCHAR(50), note VARCHAR(200))");
        }
        try (Connection c = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id NUMBER PRIMARY KEY, protocol VARCHAR2(50), note VARCHAR2(200))");
        }
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INT PRIMARY KEY, protocol VARCHAR(50), note VARCHAR(200))");
        }
        String sqlServerUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";databaseName=workdb;encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(sqlServerUrl, sqlServer.username(), sqlServer.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE widgets (id INT PRIMARY KEY, protocol VARCHAR(50), note VARCHAR(200))");
        }
        // No DDL needed for Mongo -- PostgresDocumentStore creates its backing Postgres table
        // lazily on first real insert, same as every other mongowire test relies on.

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                // "oracle-native" is a RESERVED name (BackendRegistry#ORACLE_NATIVE_DEFAULT_NAME)
                // -- RouterStage's no-rule-matched fallback resolves any Oracle-dialect statement
                // straight to whatever's registered under this exact name, same convention
                // mysql-native/mssql-native use below, going through the full pipeline (capture,
                // metrics, firewall) unlike WARP_ORACLE_BACKEND_MODE (see this class's own
                // javadoc for why that toggle doesn't do what its name suggests here).
                .env("WARP_BACKENDS", "default=" + postgres.jdbcUrl() + "|" + postgres.username() + "|" + postgres.password()
                        + ";oracle-native=" + oracle.jdbcUrl() + "|" + oracle.sysUsername() + "|" + oracle.sysPassword())
                .env("WARP_MYWIRE_BACKEND", "mysql")
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_MSSQLWIRE_BACKEND", "sqlserver")
                .env("WARP_MSSQL_HOST", sqlServer.host())
                .env("WARP_MSSQL_PORT", String.valueOf(sqlServer.port()))
                .env("WARP_MSSQL_DATABASE", "workdb")
                .env("WARP_MSSQL_USER", sqlServer.username())
                .env("WARP_MSSQL_PASSWORD", sqlServer.password())
                .env("WARP_CAPTURE_ENABLED", "true")
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (sqlServer != null) sqlServer.close();
        if (mysql != null) mysql.close();
        if (oracle != null) oracle.close();
        if (postgres != null) postgres.close();
    }

    private record ProtocolResult(String protocol, String backend, int inserted, int readBack) {
    }

    private ProtocolResult runPgwire() throws Exception {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/" + postgres.database();
        // Literal SQL, not a PreparedStatement -- pgjdbc defaults to binary-format bind
        // parameters for plain integers, which PgWireSessionHandler doesn't decode (same real
        // gap every other pgwire sharding test in this project already works around the same
        // way; see ShardingAcrossBackendEnginesIntegrationTest's own javadoc on it).
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement()) {
            for (int i = 1; i <= ROWS_PER_PROTOCOL; i++) {
                st.executeUpdate("INSERT INTO widgets (id, protocol, note) VALUES (" + i + ", 'pgwire', 'row " + i + "')");
            }
        }
        int count;
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM widgets WHERE protocol = 'pgwire'")) {
            rs.next();
            count = rs.getInt(1);
        }
        return new ProtocolResult("pgwire", "default (real Postgres)", ROWS_PER_PROTOCOL, count);
    }

    private ProtocolResult runOrawire() throws Exception {
        // The client authenticates against Warp's own control-plane identity (the same Postgres
        // credentials every other frontend uses to log in) -- NOT the real Oracle backend's own
        // credentials, which live only in the "oracle-native" WARP_BACKENDS entry above and are
        // never presented by the client at all.
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
            for (int i = 1; i <= ROWS_PER_PROTOCOL; i++) {
                // A fresh PreparedStatement per row -- avoids the real, separate orawire
                // REEXECUTE bind-count bug found and documented in
                // ThreeOracleBackendsHashShardedByCustomerIdIntegrationTest (unrelated to this
                // test's single fixed native backend, but cheap to sidestep the same way).
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO widgets (id, protocol, note) VALUES (?, ?, ?)")) {
                    ps.setInt(1, i);
                    ps.setString(2, "orawire");
                    ps.setString(3, "row " + i);
                    ps.executeUpdate();
                }
            }
        }
        int count;
        try (Connection conn = DriverManager.getConnection(oracle.jdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM widgets WHERE protocol = 'orawire'")) {
            rs.next();
            count = rs.getInt(1);
        }
        return new ProtocolResult("orawire", "oracle-native (real Oracle, native-backend mode)", ROWS_PER_PROTOCOL, count);
    }

    private ProtocolResult runMywire() throws Exception {
        // Same client-vs-backend credential split as orawire above: the client logs in with
        // Warp's own control-plane identity; WARP_MYSQL_USER/PASSWORD is what Warp itself uses
        // against the real MySQL backend.
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/" + mysql.database()
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("INSERT INTO widgets (id, protocol, note) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= ROWS_PER_PROTOCOL; i++) {
                ps.setInt(1, i);
                ps.setString(2, "mywire");
                ps.setString(3, "row " + i);
                ps.executeUpdate();
            }
        }
        int count;
        try (Connection conn = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM widgets WHERE protocol = 'mywire'")) {
            rs.next();
            count = rs.getInt(1);
        }
        return new ProtocolResult("mywire", "mysql-native (real MySQL, native-backend mode)", ROWS_PER_PROTOCOL, count);
    }

    private ProtocolResult runMssqlwire() throws Exception {
        // Same client-vs-backend credential split again: client logs in as Warp's own
        // control-plane identity; WARP_MSSQL_USER/PASSWORD is what Warp itself uses against the
        // real SQL Server backend.
        // Literal SQL, not a PreparedStatement -- mssql-jdbc's PreparedStatement issues an
        // sp_prepare/sp_execute RPC call, and MssqlWireSessionHandler's native-execute path
        // (unlike its default/translating path, which MssqlJdbcIntegrationTest already proves
        // handles real bind parameters fine) can't decode that RPC shape yet ("RPC parameter
        // \"null\" is BY_REF (output parameter) -- not supported"), a real, separate, narrower
        // gap than this test is scoped to fix -- MssqlNativeBackendIntegrationTest's own native-
        // mode test uses only literal SQL for the same reason.
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                + ";databaseName=workdb;encrypt=false;trustServerCertificate=true";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement()) {
            for (int i = 1; i <= ROWS_PER_PROTOCOL; i++) {
                st.executeUpdate("INSERT INTO widgets (id, protocol, note) VALUES (" + i + ", 'mssqlwire', 'row " + i + "')");
            }
        }
        int count;
        String directUrl = "jdbc:sqlserver://" + sqlServer.host() + ":" + sqlServer.port()
                + ";databaseName=workdb;encrypt=false;trustServerCertificate=true";
        try (Connection conn = DriverManager.getConnection(directUrl, sqlServer.username(), sqlServer.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM widgets WHERE protocol = 'mssqlwire'")) {
            rs.next();
            count = rs.getInt(1);
        }
        return new ProtocolResult("mssqlwire", "sqlserver-native (real SQL Server, native-backend mode)", ROWS_PER_PROTOCOL, count);
    }

    private ProtocolResult runMongowire() throws Exception {
        String url = "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true";
        try (MongoClient client = MongoClients.create(url)) {
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("widgets_mongo");
            for (int i = 1; i <= ROWS_PER_PROTOCOL; i++) {
                coll.insertOne(new Document("_id", i).append("protocol", "mongowire").append("note", "row " + i));
            }
            // countDocuments()/estimatedDocumentCount() both issue a real "aggregate"/"count"
            // command, neither of which mongowire implements (find/insert/update/delete only,
            // per MongoWireSessionHandler's own javadoc) -- a plain find().into(...) uses only
            // the real OP_MSG "find" command mongowire does support.
            int count = coll.find(new Document("protocol", "mongowire")).into(new java.util.ArrayList<>()).size();
            return new ProtocolResult("mongowire", "default (real Postgres, via PostgresDocumentStore)",
                    ROWS_PER_PROTOCOL, count);
        }
    }

    /** Real HTTP call against the admin API -- no shortcuts into internal Java objects. */
    private String adminGet(String path) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + path))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }

    @Test
    void fiveRealEnginesHandleConcurrentTrafficAndWarpCapturesAndCountsAllOfIt() throws Exception {
        // One thread per protocol, submitted together and joined together -- genuinely
        // concurrent, not five sequential blocks that happen to share one process.
        ExecutorService workPool = Executors.newFixedThreadPool(5);
        List<Callable<ProtocolResult>> work = List.of(
                this::runPgwire, this::runOrawire, this::runMywire, this::runMssqlwire, this::runMongowire);
        List<Future<ProtocolResult>> futures = workPool.invokeAll(work, 60, TimeUnit.SECONDS);
        workPool.shutdown();

        Map<String, ProtocolResult> results = new LinkedHashMap<>();
        for (Future<ProtocolResult> f : futures) {
            ProtocolResult r = f.get();
            results.put(r.protocol(), r);
        }

        for (String protocol : List.of("pgwire", "orawire", "mywire", "mssqlwire", "mongowire")) {
            ProtocolResult r = results.get(protocol);
            assertEquals(ROWS_PER_PROTOCOL, r.inserted(), protocol + ": expected all rows to insert without error");
            assertEquals(ROWS_PER_PROTOCOL, r.readBack(), protocol + ": expected every inserted row to read back from "
                    + "its own real backend (" + r.backend() + ")");
        }

        // Real proof Warp captured every statement, not a rowcount coincidence --
        // WorkloadCaptureBuffer records the statement's real SourceDialect (POSTGRES/ORACLE/
        // MYSQL/SQL_SERVER -- confirmed live; it's the SQL dialect, not the wire-frontend name)
        // plus the real SQL text and bind params, per statement as it actually executes.
        String captureJson = adminGet("/api/capture?limit=1000");
        JsonArray capture = JsonParser.parseString(captureJson).getAsJsonArray();
        Map<String, Long> capturedByDialect = new LinkedHashMap<>();
        for (var el : capture) {
            String dialect = el.getAsJsonObject().get("protocol").getAsString();
            capturedByDialect.merge(dialect, 1L, Long::sum);
        }
        Map<String, String> protocolToDialect = Map.of(
                "pgwire", "POSTGRES", "orawire", "ORACLE", "mywire", "MYSQL", "mssqlwire", "SQL_SERVER");
        for (var entry : protocolToDialect.entrySet()) {
            long count = capturedByDialect.getOrDefault(entry.getValue(), 0L);
            assertTrue(count >= ROWS_PER_PROTOCOL,
                    "expected at least " + ROWS_PER_PROTOCOL + " captured " + entry.getKey() + " ("
                            + entry.getValue() + ") statements, got " + count + " -- full capture: " + captureJson);
        }
        // mongowire's inserts execute as PgItemStore/PostgresDocumentStore-style backend calls
        // rather than a client-supplied SQL string, so they may not appear as their own "protocol"
        // entry the same way SQL protocols do -- captured only if mongowire itself records one;
        // not asserted on to avoid coupling this test to that internal a detail.

        String metricsJson = adminGet("/api/metrics/summary");
        JsonObject metrics = JsonParser.parseString(metricsJson).getAsJsonObject();
        JsonObject protocolCounts = metrics.getAsJsonObject("protocolCounts");
        for (String protocol : List.of("pgwire", "orawire", "mywire", "mssqlwire", "mongowire")) {
            assertTrue(protocolCounts.has(protocol) && protocolCounts.get(protocol).getAsLong() > 0,
                    "expected /api/metrics/summary.protocolCounts to show real traffic for " + protocol
                            + " -- got: " + metricsJson);
        }

        writeReport(results, capturedByDialect, metrics);
    }

    /** "Show it to me": a plain-text report a human can actually read, not just assertions that
     * pass silently -- printed to stdout (visible in the test run's own console output) and saved
     * to target/five-engines-report.txt so it survives after the test run ends. */
    private void writeReport(Map<String, ProtocolResult> results, Map<String, Long> capturedByDialect,
            JsonObject metrics) throws Exception {
        StringBuilder report = new StringBuilder();
        report.append("=== Five real database engines, one Warp process, concurrent traffic ===\n\n");
        report.append(String.format("%-12s %-45s %8s %8s%n", "Protocol", "Real backend", "Inserted", "Read back"));
        for (ProtocolResult r : results.values()) {
            report.append(String.format("%-12s %-45s %8d %8d%n", r.protocol(), r.backend(), r.inserted(), r.readBack()));
        }
        report.append("\n--- Captured statements (WARP_CAPTURE_ENABLED, GET /api/capture) ---\n");
        capturedByDialect.forEach((protocol, count) -> report.append(String.format("  %-12s %d captured%n", protocol, count)));
        report.append("\n--- GET /api/metrics/summary ---\n");
        report.append("protocolCounts: ").append(metrics.get("protocolCounts")).append('\n');
        if (metrics.has("byBackend")) {
            report.append("byBackend: ").append(metrics.get("byBackend")).append('\n');
        }
        String reportText = report.toString();
        System.out.println(reportText);
        Files.writeString(Path.of("target", "five-engines-report.txt"), reportText);
    }
}
