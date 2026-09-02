package com.sayonora.wire.influxwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * influxwire -- InfluxDB-compatible HTTP/JSON, the same "speak the real client's wire protocol,
 * translate to plain SQL underneath" shape as oswire/dynamowire/sqswire, so a real InfluxDB v1
 * client (line-protocol writers, {@code influxdb-client}/{@code influxdb} SDKs, {@code curl},
 * Telegraf's InfluxDB output plugin) can point at Warp directly.
 *
 * <p>V1 surface, scoped deliberately narrow and honest rather than silently approximating InfluxDB
 * v2's query language:
 * <ul>
 *   <li>{@code POST /write?db=&lt;db&gt;&amp;precision=&lt;ns|us|ms|s&gt;} -- real line-protocol
 *       body, one or more points, any number of measurements per request</li>
 *   <li>{@code GET/POST /query?q=&lt;InfluxQL&gt;} -- {@code SHOW MEASUREMENTS}, and a real, bounded
 *       {@code SELECT} subset via {@link InfluxQlParser}: {@code SELECT * | field[,field...] |
 *       agg(field)[,...] FROM &lt;measurement&gt; [WHERE cond [AND cond...]] [GROUP BY
 *       time(&lt;duration&gt;) [, tag...]] [LIMIT n]}, {@code agg} being
 *       {@code mean|sum|count|min|max} and {@code cond} an {@code =/!=/&gt;/&lt;/&gt;=/&lt;=}
 *       comparison against a tag, field, or {@code time} (including {@code now() - &lt;duration&gt;}).
 *       Everything past that -- {@code OR}, parenthesized conditions, {@code fill()},
 *       {@code ORDER BY}, regex tag matching, subqueries, Flux -- returns a clear 400, not a wrong
 *       or partial answer, matching {@code OpenSearchAdapter}'s "unrecognized clause fails loudly"
 *       policy for this codebase's other protocols; see {@link InfluxQlParser}'s own javadoc for
 *       the exact grammar.</li>
 *   <li>{@code GET /ping} -- real InfluxDB client SDKs call this as a liveness/version check
 *       before doing anything else; a 204 with an {@code X-Influxdb-Version} header is real
 *       InfluxDB's own shape</li>
 * </ul>
 * Flux (InfluxDB v2's real query language), continuous queries, retention policies, and downsampling
 * are V2 -- not implemented here. See {@link PgTimeSeriesStore}'s javadoc for the TimescaleDB
 * detection this store does underneath both routes.
 */
public final class InfluxWireServer {

    private static final Logger log = LoggerFactory.getLogger(InfluxWireServer.class);

    private static final Pattern SHOW_MEASUREMENTS = Pattern.compile("(?i)^\\s*SHOW\\s+MEASUREMENTS\\s*;?\\s*$");

    private final Server server;
    private final PgTimeSeriesStore store;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    public InfluxWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this(port, backendRegistry, com.sayonora.wire.acl.ConnectionGate.DISABLED,
                com.sayonora.wire.http.auth.AccessContextResolver.DISABLED, null);
    }

    public InfluxWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.http.auth.AccessContextResolver oauth,
            com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = new PgTimeSeriesStore(backendRegistry);
        this.sqlMetrics = sqlMetrics;
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                if (!connectionGate.acceptHttp(request)) {
                    writeError(response, 403, "forbidden");
                    return;
                }
                if (oauth.enforce(request, response) == null) {
                    return;
                }
                route(request, response, target);
            }
        });
    }

    private void route(HttpServletRequest request, HttpServletResponse response, String target) throws IOException {
        String method = request.getMethod();
        if (target.equals("/write") && method.equals("POST")) {
            handleWrite(request, response);
            return;
        }
        if (target.equals("/query") && (method.equals("GET") || method.equals("POST"))) {
            handleQuery(request, response);
            return;
        }
        if (target.equals("/ping")) {
            // Real InfluxDB's own liveness endpoint -- no body, a version header, 204. Client SDKs
            // (confirmed against the real influxdb-client Python/Java SDKs' own connection-check
            // calls) call this before anything else; refusing it with a 404 breaks first contact.
            response.setHeader("X-Influxdb-Version", "warp-influxwire-v1");
            response.setStatus(204);
            return;
        }
        writeError(response, 404, "no influxwire route for " + method + " " + target);
    }

    private void handleWrite(HttpServletRequest request, HttpServletResponse response) throws IOException {
        long start = System.nanoTime();
        // queryParam, not request.getParameter -- see that method's own javadoc: calling
        // getParameter on a POST whose Content-Type is (or defaults to, as every real line-
        // protocol-writing client and curl both do) application/x-www-form-urlencoded makes Jetty
        // parse the request BODY as form params, silently consuming the input stream before
        // readBody() below ever reads it. Found live: /write returned a real 204 with every point
        // silently discarded (0 rows written, no error) until this was diagnosed and fixed.
        String db = queryParam(request, "db");
        try {
            String body = readBody(request);
            List<InfluxPoint> points = LineProtocolParser.parse(body, queryParam(request, "precision"));
            store.write(points);
            // Real InfluxDB's own /write success response: 204, empty body.
            response.setStatus(204);
            recordMetric("write", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE, db, start);
        } catch (InfluxException e) {
            writeError(response, 400, e.getMessage());
        } catch (SQLException e) {
            log.warn("influxwire: Postgres error servicing /write (db={}): {}", db, e.getMessage());
            writeError(response, InfluxErrorMapper.status(e.getSQLState()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("influxwire: /write (db={}) failed", db, e);
            writeError(response, 400, String.valueOf(e.getMessage()));
        }
    }

    private void handleQuery(HttpServletRequest request, HttpServletResponse response) throws IOException {
        long start = System.nanoTime();
        String q = queryParam(request, "q");
        String db = queryParam(request, "db");
        if (q == null || q.isBlank()) {
            writeError(response, 400, "missing required \"q\" query parameter");
            return;
        }
        try {
            if (SHOW_MEASUREMENTS.matcher(q).matches()) {
                writeJson(response, 200, renderShowMeasurements(store.listMeasurements()));
            } else if (q.strip().regionMatches(true, 0, "SELECT", 0, 6)) {
                InfluxQlParser.SelectStatement stmt = InfluxQlParser.parse(q);
                PgTimeSeriesStore.QueryResult result = store.select(stmt);
                writeJson(response, 200, renderQueryResult(stmt.measurement(), result));
            } else {
                writeError(response, 400, "influxwire V1 only recognizes \"SHOW MEASUREMENTS\" and a "
                        + "bounded SELECT subset (WHERE/GROUP BY time()/mean|sum|count|min|max) -- got: " + q);
                return;
            }
            recordMetric("query", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ, db, start);
        } catch (SQLException e) {
            log.warn("influxwire: Postgres error servicing /query (db={}): {}", db, e.getMessage());
            writeError(response, InfluxErrorMapper.status(e.getSQLState()), e.getMessage());
        } catch (InfluxException e) {
            writeError(response, 400, e.getMessage());
        } catch (RuntimeException e) {
            // Same "never let an unexpected failure fall through to an empty/broken response" as
            // handleWrite's own catch-all -- found live: a config problem elsewhere (an unrelated
            // IllegalStateException) previously reached the client as a truncated, unparseable
            // response instead of a real error, because this method had no catch-all yet.
            log.error("influxwire: /query (db={}, q={}) failed", db, q, e);
            writeError(response, 500, String.valueOf(e.getMessage()));
        }
    }

    /** Real InfluxDB's own {@code /query} response shape: {@code results[0].series[0]} carrying
     * {@code name}/{@code columns}/{@code values} (an array of arrays, not an array of objects --
     * confirmed against real InfluxDB v1's documented response format). Every real client SDK's
     * query-result parser expects exactly this nesting. {@link PgTimeSeriesStore.QueryResult}'s
     * own column list already matches this shape directly -- this just wraps it and converts each
     * Java value (String/Number/Boolean/null/a parsed {@code JsonElement} for a jsonb column) to
     * its JSON form. */
    private static JsonObject renderQueryResult(String measurement, PgTimeSeriesStore.QueryResult result) {
        JsonObject series = new JsonObject();
        series.addProperty("name", measurement);
        JsonArray columns = new JsonArray();
        result.columns().forEach(columns::add);
        series.add("columns", columns);
        JsonArray values = new JsonArray();
        for (List<Object> row : result.rows()) {
            JsonArray value = new JsonArray();
            for (Object cell : row) {
                value.add(toJsonElement(cell));
            }
            values.add(value);
        }
        series.add("values", values);
        JsonArray seriesArray = new JsonArray();
        seriesArray.add(series);
        JsonObject statement = new JsonObject();
        statement.addProperty("statement_id", 0);
        statement.add("series", seriesArray);
        JsonArray results = new JsonArray();
        results.add(statement);
        JsonObject resp = new JsonObject();
        resp.add("results", results);
        return resp;
    }

    private static com.google.gson.JsonElement toJsonElement(Object cell) {
        if (cell == null) {
            return com.google.gson.JsonNull.INSTANCE;
        }
        if (cell instanceof com.google.gson.JsonElement je) {
            return je;
        }
        if (cell instanceof Number n) {
            return new com.google.gson.JsonPrimitive(n);
        }
        if (cell instanceof Boolean b) {
            return new com.google.gson.JsonPrimitive(b);
        }
        return new com.google.gson.JsonPrimitive(String.valueOf(cell));
    }

    private static JsonObject renderShowMeasurements(List<String> measurements) {
        JsonObject series = new JsonObject();
        series.addProperty("name", "measurements");
        JsonArray columns = new JsonArray();
        columns.add("name");
        series.add("columns", columns);
        JsonArray values = new JsonArray();
        for (String m : measurements) {
            JsonArray value = new JsonArray();
            value.add(m);
            values.add(value);
        }
        series.add("values", values);
        JsonArray seriesArray = new JsonArray();
        seriesArray.add(series);
        JsonObject statement = new JsonObject();
        statement.addProperty("statement_id", 0);
        statement.add("series", seriesArray);
        JsonArray results = new JsonArray();
        results.add(statement);
        JsonObject resp = new JsonObject();
        resp.add("results", results);
        return resp;
    }

    /**
     * Reads a parameter straight out of the raw query string, deliberately NOT via
     * {@code HttpServletRequest#getParameter} -- see {@link #handleWrite}'s own comment for why:
     * {@code getParameter} on a POST request makes the servlet container eagerly parse (and
     * consume) the request body as {@code application/x-www-form-urlencoded} form data whenever
     * that's the request's Content-Type, which is exactly what curl and every real line-protocol
     * client sends by default -- silently emptying the body before this class's own
     * {@link #readBody} gets to read it as line protocol. Every influxwire param this class reads
     * (db, precision, q) is always sent as a real URL query-string param by real InfluxDB clients,
     * never as a form-body field, so parsing the query string directly is both correct and safe
     * regardless of read order.
     */
    private static String queryParam(HttpServletRequest request, String name) {
        String qs = request.getQueryString();
        if (qs == null) {
            return null;
        }
        for (String pair : qs.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            if (key.equals(name)) {
                String value = eq < 0 ? "" : pair.substring(eq + 1);
                return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void recordMetric(String operation, com.sayonora.wire.core.SqlMetricsCollector.StatementKind kind,
            String db, long startNanos) {
        if (sqlMetrics != null) {
            long elapsedNanos = System.nanoTime() - startNanos;
            sqlMetrics.recordOperation("influxwire", db == null ? "default" : db, kind, operation, elapsedNanos, elapsedNanos);
            String outcome = kind == com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ
                    ? com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ
                    : com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE;
            sqlMetrics.recordRttOutcome("influxwire", outcome, elapsedNanos);
        }
    }

    private static String readBody(HttpServletRequest request) throws IOException {
        return new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void writeJson(HttpServletResponse response, int status, JsonObject body) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(body.toString());
    }

    /** Real InfluxDB's own error shape: a flat {@code {"error": "message"}}, not OpenSearch's
     * nested {@code root_cause} or DynamoDB's {@code __type} envelope. */
    private static void writeError(HttpServletResponse response, int status, String message) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("error", message);
        writeJson(response, status, err);
    }

    public void start() throws Exception {
        server.start();
        log.info("warp listening for InfluxDB HTTP/JSON (influxwire) on port {}",
                ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
    }
}
