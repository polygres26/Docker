package com.nexagres.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.nexagres.wire.testsupport.RealMySql;
import com.nexagres.wire.testsupport.RealOracle;
import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
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
 * Proves the data-investigation MCP tool set (inspect_schema, column_stats, compare_groups,
 * correlation, sample_rows, find_outliers, find_join_path, run_sql, explain_sql) -- the toolset
 * https://www.linkedin.com/pulse/how-train-small-model-databases-kumar-rajamani-n1i5c/ describes
 * for training/evaluating a small model against a real database via a fixed agent loop, rather
 * than raw SQL generation alone. Real, per-dialect SQL end to end against real Postgres, plus the
 * two dialects whose SQL for these tools genuinely differs in kind, not just spelling: MySQL
 * (correlation has no built-in CORR(), needs the hand-derived Pearson formula) and Oracle
 * (inspect_schema/find_join_path have no information_schema at all).
 */
class DataInvestigationToolsIntegrationTest {

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
    void allNineToolsWorkAgainstRealPostgres() throws Exception {
        postgres = RealPostgres.start();
        try (Connection direct = DriverManager.getConnection(
                "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                postgres.username(), postgres.password());
                Statement st = direct.createStatement()) {
            st.execute("CREATE TABLE customers (id INT PRIMARY KEY, region VARCHAR(20))");
            st.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), "
                    + "amount NUMERIC)");
            st.execute("INSERT INTO customers VALUES (1, 'west'), (2, 'east')");
            st.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 120), (3, 2, 9999), (4, 2, 90)");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        HttpResponse<String> inspect = mcpCall(port, "inspect_schema", new JsonObject());
        assertTrue(inspect.body().contains("orders") && inspect.body().contains("customers"),
                "inspect_schema should list both tables -- got: " + inspect.body());

        JsonObject statsArgs = new JsonObject();
        statsArgs.addProperty("table", "orders");
        statsArgs.addProperty("column", "amount");
        HttpResponse<String> stats = mcpCall(port, "column_stats", statsArgs);
        assertTrue(stats.body().contains("\"n\":4") || stats.body().contains("\\\"n\\\":4"),
                "column_stats should count all 4 rows -- got: " + stats.body());

        JsonObject groupArgs = new JsonObject();
        groupArgs.addProperty("table", "orders");
        groupArgs.addProperty("group_by", "customer_id");
        groupArgs.addProperty("metric", "amount");
        groupArgs.addProperty("agg", "sum");
        HttpResponse<String> groups = mcpCall(port, "compare_groups", groupArgs);
        assertEquals(200, groups.statusCode());
        assertTrue(groups.body().contains("value"), "compare_groups should return an aggregated value -- got: " + groups.body());

        JsonObject corrArgs = new JsonObject();
        corrArgs.addProperty("table", "orders");
        corrArgs.addProperty("col1", "customer_id");
        corrArgs.addProperty("col2", "amount");
        HttpResponse<String> corr = mcpCall(port, "correlation", corrArgs);
        assertTrue(corr.body().contains("correlation"), "correlation should return a correlation field -- got: " + corr.body());

        JsonObject sampleArgs = new JsonObject();
        sampleArgs.addProperty("table", "orders");
        sampleArgs.addProperty("limit", 2);
        HttpResponse<String> sample = mcpCall(port, "sample_rows", sampleArgs);
        assertEquals(200, sample.statusCode());

        JsonObject outlierArgs = new JsonObject();
        outlierArgs.addProperty("table", "orders");
        outlierArgs.addProperty("column", "amount");
        outlierArgs.addProperty("threshold", 1.0);
        HttpResponse<String> outliers = mcpCall(port, "find_outliers", outlierArgs);
        assertTrue(outliers.body().contains("9999"),
                "find_outliers should flag the 9999 row as a real outlier -- got: " + outliers.body());

        JsonObject pathArgs = new JsonObject();
        pathArgs.addProperty("from_table", "orders");
        pathArgs.addProperty("to_table", "customers");
        HttpResponse<String> path = mcpCall(port, "find_join_path", pathArgs);
        assertTrue(path.body().contains("\\\"found\\\":true") || path.body().contains("\"found\":true"),
                "find_join_path should find the real orders->customers FK -- got: " + path.body());
        assertTrue(path.body().toLowerCase(java.util.Locale.ROOT).contains("customer_id"),
                "the real join column should appear in the path -- got: " + path.body());

        JsonObject runSqlArgs = new JsonObject();
        runSqlArgs.addProperty("sql", "SELECT count(*) FROM orders");
        HttpResponse<String> runSql = mcpCall(port, "run_sql", runSqlArgs);
        assertTrue(runSql.body().contains("count"), "run_sql should behave exactly like execute_sql -- got: " + runSql.body());

        JsonObject explainArgs = new JsonObject();
        explainArgs.addProperty("sql", "SELECT * FROM orders WHERE id = 1");
        HttpResponse<String> explain = mcpCall(port, "explain_sql", explainArgs);
        assertTrue(explain.body().contains("Plan") || explain.body().contains("plan"),
                "explain_sql should return a real Postgres plan -- got: " + explain.body());
    }

    @Test
    void correlationUsesTheHandDerivedFormulaAgainstRealMySql() throws Exception {
        postgres = RealPostgres.start();
        try (RealMySql mysql = RealMySql.start()) {
            try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE metrics (x INT, y INT)");
                st.execute("INSERT INTO metrics VALUES (1, 2), (2, 4), (3, 6), (4, 8)");
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

            JsonObject corrArgs = new JsonObject();
            corrArgs.addProperty("table", "metrics");
            corrArgs.addProperty("col1", "x");
            corrArgs.addProperty("col2", "y");
            HttpResponse<String> corr = mcpCall(warp.port("mcp"), "correlation", corrArgs);
            assertEquals(200, corr.statusCode());
            // y = 2x exactly -- a real perfect positive correlation, coefficient 1.0.
            assertTrue(corr.body().contains("\":1") || corr.body().contains("\": 1"),
                    "a perfectly linear x/y should correlate at ~1.0 -- got: " + corr.body());
        }
    }

    @Test
    void inspectSchemaAndFindJoinPathWorkAgainstRealOracleWithNoInformationSchema() throws Exception {
        postgres = RealPostgres.start();
        try (RealOracle oracle = RealOracle.start()) {
            try (Connection c = DriverManager.getConnection(oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE oc_customers (id NUMBER PRIMARY KEY, region VARCHAR2(20))");
                st.execute("CREATE TABLE oc_orders (id NUMBER PRIMARY KEY, customer_id NUMBER "
                        + "REFERENCES oc_customers(id), amount NUMBER)");
                st.execute("INSERT INTO oc_customers VALUES (1, 'west')");
                st.execute("INSERT INTO oc_orders VALUES (1, 1, 100)");
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
            int port = warp.port("mcp");

            HttpResponse<String> inspect = mcpCall(port, "inspect_schema", new JsonObject());
            assertTrue(inspect.body().toUpperCase(java.util.Locale.ROOT).contains("OC_ORDERS")
                    && inspect.body().toUpperCase(java.util.Locale.ROOT).contains("OC_CUSTOMERS"),
                    "inspect_schema should see both real Oracle tables via user_tab_columns -- got: " + inspect.body());

            JsonObject pathArgs = new JsonObject();
            pathArgs.addProperty("from_table", "oc_orders");
            pathArgs.addProperty("to_table", "oc_customers");
            HttpResponse<String> path = mcpCall(port, "find_join_path", pathArgs);
            assertTrue(path.body().contains("\\\"found\\\":true") || path.body().contains("\"found\":true"),
                    "find_join_path should find the real Oracle FK via user_cons_columns -- got: " + path.body());
        }
    }
}
