package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Proves {@code WARP_MCP_BACKEND=oracle/mysql/sqlserver} -- closing the gap this project's own
 * orawire/mywire/mssqlwire native-backend modes already closed for their own wire protocols, but
 * the MCP frontend never had: before this, {@code WarpMcpServer} always opened a hardcoded
 * {@code PgConnections.open(options)} connection no matter what, so an MCP tool call could only
 * ever reach the configured Postgres backend. {@code execute_sql}, {@code list_tables}, and
 * {@code describe_table} all now dispatch to a real Oracle/MySQL/SQL Server connection instead,
 * bypassing the shared pipeline exactly the way the wire protocols' own native modes do (see
 * {@code WarpMcpServer#runSql}'s own javadoc). {@code document_schema}/{@code explain_query}/
 * {@code query_natural_language} stay Postgres-only (they hardcode Postgres EXPLAIN syntax and an
 * LLM schema-drafting prompt written for Postgres) -- refused with a clear error in native mode,
 * proved by {@link #postgresOnlyToolsAreClearlyRefusedInNativeMode}, not silently run.
 */
class McpNativeBackendIntegrationTest {

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private static HttpResponse<String> mcpCall(int mcpPort, String toolName, JsonObject arguments) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "tools/call");
        req.add("params", params);
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return HttpClient.newHttpClient().send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void executeSqlInOracleModeReadsFromTheRealOracleBackendNotPostgres() throws Exception {
        postgres = RealPostgres.start();
        try (RealOracle oracle = RealOracle.start()) {
            try (Connection c = DriverManager.getConnection(oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE native_test (id NUMBER PRIMARY KEY, label VARCHAR2(50))");
                st.execute("INSERT INTO native_test (id, label) VALUES (1, 'real-oracle-row')");
            }

            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mcp", "WARP_MCP_PORT")
                    .env("WARP_MCP_BACKEND", "oracle")
                    .env("WARP_ORACLE_HOST", oracle.host())
                    .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                    .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                    .env("WARP_ORACLE_USER", oracle.sysUsername())
                    .env("WARP_ORACLE_PASSWORD", oracle.sysPassword())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();

            JsonObject args = new JsonObject();
            args.addProperty("sql", "SELECT label FROM native_test WHERE id = 1");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), "execute_sql", args);
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("real-oracle-row"),
                    "expected the row written directly to real Oracle -- got: " + resp.body());

            HttpResponse<String> listResp = mcpCall(warp.port("mcp"), "list_tables", new JsonObject());
            assertTrue(listResp.body().toUpperCase(java.util.Locale.ROOT).contains("NATIVE_TEST"),
                    "list_tables should see the real Oracle table -- got: " + listResp.body());

            JsonObject describeArgs = new JsonObject();
            describeArgs.addProperty("table", "native_test");
            HttpResponse<String> describeResp = mcpCall(warp.port("mcp"), "describe_table", describeArgs);
            assertTrue(describeResp.body().toUpperCase(java.util.Locale.ROOT).contains("LABEL"),
                    "describe_table should see the real Oracle column -- got: " + describeResp.body());
        }
    }

    @Test
    void executeSqlInMySqlModeReadsFromTheRealMySqlBackendNotPostgres() throws Exception {
        postgres = RealPostgres.start();
        try (RealMySql mysql = RealMySql.start()) {
            try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE native_test (id INT PRIMARY KEY, label VARCHAR(50))");
                st.execute("INSERT INTO native_test (id, label) VALUES (1, 'real-mysql-row')");
            }

            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mcp", "WARP_MCP_PORT")
                    .env("WARP_MCP_BACKEND", "mysql")
                    .env("WARP_MYSQL_HOST", mysql.host())
                    .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                    .env("WARP_MYSQL_DATABASE", mysql.database())
                    .env("WARP_MYSQL_USER", mysql.username())
                    .env("WARP_MYSQL_PASSWORD", mysql.password())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();

            JsonObject args = new JsonObject();
            args.addProperty("sql", "SELECT label FROM native_test WHERE id = 1");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), "execute_sql", args);
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("real-mysql-row"),
                    "expected the row written directly to real MySQL -- got: " + resp.body());
        }
    }

    @Test
    void executeSqlInSqlServerModeReadsFromTheRealSqlServerBackendNotPostgres() throws Exception {
        postgres = RealPostgres.start();
        try (RealAzureSqlEdge mssql = RealAzureSqlEdge.start()) {
            try (Connection c = DriverManager.getConnection(mssql.masterJdbcUrl(), mssql.username(), mssql.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE native_test (id INT PRIMARY KEY, label VARCHAR(50))");
                st.execute("INSERT INTO native_test (id, label) VALUES (1, 'real-sqlserver-row')");
            }

            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mcp", "WARP_MCP_PORT")
                    .env("WARP_MCP_BACKEND", "sqlserver")
                    .env("WARP_MSSQL_HOST", mssql.host())
                    .env("WARP_MSSQL_PORT", String.valueOf(mssql.port()))
                    .env("WARP_MSSQL_USER", mssql.username())
                    .env("WARP_MSSQL_PASSWORD", mssql.password())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();

            JsonObject args = new JsonObject();
            args.addProperty("sql", "SELECT label FROM native_test WHERE id = 1");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), "execute_sql", args);
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("real-sqlserver-row"),
                    "expected the row written directly to real SQL Server -- got: " + resp.body());
        }
    }

    @Test
    void postgresOnlyToolsAreClearlyRefusedInNativeMode() throws Exception {
        postgres = RealPostgres.start();
        try (RealMySql mysql = RealMySql.start()) {
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mcp", "WARP_MCP_PORT")
                    .env("WARP_MCP_BACKEND", "mysql")
                    .env("WARP_MYSQL_HOST", mysql.host())
                    .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                    .env("WARP_MYSQL_DATABASE", mysql.database())
                    .env("WARP_MYSQL_USER", mysql.username())
                    .env("WARP_MYSQL_PASSWORD", mysql.password())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();

            JsonObject args = new JsonObject();
            args.addProperty("sql", "SELECT 1");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), "explain_query", args);
            assertEquals(200, resp.statusCode());
            // Gson HTML-escapes ' and = in JSON string literals (', =) -- check the
            // parsed text, not the raw JSON, so this assertion isn't tied to that encoding detail.
            String text = com.google.gson.JsonParser.parseString(resp.body()).getAsJsonObject()
                    .getAsJsonObject("result").getAsJsonArray("content").get(0).getAsJsonObject().get("text").getAsString();
            assertTrue(text.contains("isn't supported with WARP_MCP_BACKEND=mysql"),
                    "expected a clear refusal, not a broken Postgres EXPLAIN sent to real MySQL -- got: " + text);

            // tools/list must not even advertise the Postgres-only tools in native mode.
            JsonObject listReq = new JsonObject();
            listReq.addProperty("jsonrpc", "2.0");
            listReq.addProperty("id", 1);
            listReq.addProperty("method", "tools/list");
            HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.port("mcp") + "/"))
                    .header("content-type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(listReq.toString()))
                    .build();
            HttpResponse<String> listResp = HttpClient.newHttpClient().send(httpReq, HttpResponse.BodyHandlers.ofString());
            assertTrue(listResp.body().contains("execute_sql"), "execute_sql must still be listed");
            assertTrue(!listResp.body().contains("explain_query") && !listResp.body().contains("document_schema")
                    && !listResp.body().contains("query_natural_language"),
                    "Postgres-only tools must not be advertised in native mode -- got: " + listResp.body());
        }
    }
}
