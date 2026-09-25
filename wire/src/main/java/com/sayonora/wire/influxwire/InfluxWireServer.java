package com.sayonora.wire.influxwire;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.influxwire.InfluxFmt.Series;
import com.sayonora.wire.influxwire.InfluxFmt.StmtResult;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * influxwire -- an InfluxDB 1.x compatible HTTP API (plus the 2.x write endpoint) over Postgres, so real InfluxDB
 * clients (Telegraf, {@code influxdb}/{@code influxdb-client} SDKs, {@code curl}) can point at Warp directly.
 *
 * <ul>
 *   <li>{@code POST /write?db=&rp=&precision=} -- line protocol (gzip accepted), InfluxDB's error/partial-write texts</li>
 *   <li>{@code GET|POST /query?q=&db=&epoch=&chunked=&chunk_size=&pretty=&params=} -- InfluxQL: SELECT (functions,
 *       GROUP BY time/fill, subqueries, INTO), SHOW ..., CREATE/DROP DATABASE/RETENTION POLICY/MEASUREMENT/SERIES,
 *       DELETE; JSON (default), CSV ({@code Accept: application/csv}), pretty and chunked responses</li>
 *   <li>{@code POST /api/v2/write?org=&bucket=&precision=} -- 2.x write (bucket = database, {@code db/rp} allowed)</li>
 *   <li>{@code POST /api/v2/query} -- Flux: answered with a clear 501 (not supported)</li>
 *   <li>{@code GET|HEAD /ping}, {@code GET /health}</li>
 * </ul>
 * Optional native credentials: {@code WARP_INFLUXWIRE_USER}/{@code WARP_INFLUXWIRE_PASSWORD} (basic auth or
 * {@code u=}/{@code p=}) and {@code WARP_INFLUXWIRE_TOKEN} ({@code Authorization: Token ...}, also accepted as the
 * password of the v1-compat endpoints). {@code WARP_INFLUXWIRE_STRICT_DB=true} makes writes to an unknown database a
 * 404 like real InfluxDB (default: the database is created on first write, the historical Warp behaviour).
 */
public final class InfluxWireServer {

    private static final Logger log = LoggerFactory.getLogger(InfluxWireServer.class);
    private static final String VERSION = "1.8.10";
    private static final long MAX_BODY = 25L * 1024 * 1024;

    private final Server server;
    private final PgTimeSeriesStore store;
    private final InfluxEngine engine;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;
    private final boolean strictDb;
    private final String authUser;
    private final String authPassword;
    private final String authToken;

    public InfluxWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this(port, backendRegistry, com.sayonora.wire.acl.ConnectionGate.DISABLED,
                com.sayonora.wire.http.auth.AccessContextResolver.DISABLED, null);
    }

    public InfluxWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.http.auth.AccessContextResolver oauth,
            com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = new PgTimeSeriesStore(backendRegistry);
        this.strictDb = "true".equalsIgnoreCase(System.getenv("WARP_INFLUXWIRE_STRICT_DB"));
        this.engine = new InfluxEngine(store, !strictDb);
        this.authUser = blankToNull(System.getenv("WARP_INFLUXWIRE_USER"));
        this.authPassword = blankToNull(System.getenv("WARP_INFLUXWIRE_PASSWORD"));
        this.authToken = blankToNull(System.getenv("WARP_INFLUXWIRE_TOKEN"));
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
                try {
                    route(request, response, target);
                } catch (IOException e) {
                    throw e;
                } catch (RuntimeException e) {
                    log.error("influxwire: unhandled failure on {} {}", request.getMethod(), target, e);
                    if (!response.isCommitted()) {
                        writeError(response, 500, String.valueOf(e.getMessage()));
                    }
                }
            }
        });
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private boolean authRequired() {
        return authUser != null || authToken != null;
    }

    // ------------------------------------------------------------------ routing

    private void route(HttpServletRequest request, HttpServletResponse response, String target) throws IOException {
        String method = request.getMethod();
        commonHeaders(response);
        switch (target) {
            case "/ping" -> {
                if (!method.equals("GET") && !method.equals("HEAD")) {
                    methodNotAllowed(response);
                    return;
                }
                response.setHeader("X-Influxdb-Version", VERSION);
                if ("true".equals(param(request, "verbose"))) {
                    writeText(response, 200, "application/json", "{\"version\":\"" + VERSION + "\"}\n");
                } else {
                    response.setContentType("application/json");
                    response.setStatus(204);
                }
                return;
            }
            case "/health" -> {
                if (!method.equals("GET") && !method.equals("HEAD")) {
                    methodNotAllowed(response);
                    return;
                }
                writeText(response, 200, "application/json",
                        "{\"checks\":[],\"message\":\"ready for queries and writes\",\"name\":\"influxdb\",\"status\":\"pass\",\"version\":\"" + VERSION + "\"}\n");
                return;
            }
            case "/write" -> {
                if (!method.equals("POST")) {
                    methodNotAllowed(response);
                    return;
                }
                handleWrite(request, response, false);
                return;
            }
            case "/api/v2/write" -> {
                if (!method.equals("POST")) {
                    methodNotAllowed(response);
                    return;
                }
                handleWrite(request, response, true);
                return;
            }
            case "/query" -> {
                if (!method.equals("GET") && !method.equals("POST")) {
                    methodNotAllowed(response);
                    return;
                }
                handleQuery(request, response);
                return;
            }
            case "/api/v2/query" -> {
                if (!method.equals("POST")) {
                    methodNotAllowed(response);
                    return;
                }
                writeText(response, 501, "application/json", "{\"code\":\"unimplemented\",\"message\":\"the Flux query language is not supported "
                        + "by Warp's influxwire; use InfluxQL through /query (GET or POST) instead\"}\n");
                return;
            }
            default -> writeText(response, 404, "text/plain; charset=utf-8", "404 page not found\n");
        }
    }

    private static void commonHeaders(HttpServletResponse response) {
        response.setHeader("X-Influxdb-Build", "OSS");
        response.setHeader("X-Influxdb-Version", VERSION);
        response.setHeader("Request-Id", java.util.UUID.randomUUID().toString());
    }

    private static void methodNotAllowed(HttpServletResponse response) throws IOException {
        writeText(response, 405, "text/plain; charset=utf-8", "Method Not Allowed\n");
    }

    // ------------------------------------------------------------------ auth

    /** @return true when the request may proceed (else a 401 was written). */
    private boolean authorised(HttpServletRequest request, HttpServletResponse response, Map<String, String> qp, boolean v2) throws IOException {
        if (!authRequired()) {
            return true;
        }
        String header = request.getHeader("Authorization");
        String user = null;
        String pass = null;
        String token = null;
        if (header != null) {
            if (header.regionMatches(true, 0, "Token ", 0, 6)) {
                token = header.substring(6).trim();
            } else if (header.regionMatches(true, 0, "Basic ", 0, 6)) {
                try {
                    String dec = new String(Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
                    int i = dec.indexOf(':');
                    user = i < 0 ? dec : dec.substring(0, i);
                    pass = i < 0 ? "" : dec.substring(i + 1);
                } catch (IllegalArgumentException e) {
                    user = null;
                }
            }
        }
        if (user == null && qp.get("u") != null) {
            user = qp.get("u");
            pass = qp.getOrDefault("p", "");
        }
        boolean present = token != null || user != null;
        if (!present) {
            unauthorised(response, v2, v2 ? "unauthorized access" : "unable to parse authentication credentials");
            return false;
        }
        boolean ok = (token != null && authToken != null && constantEq(token, authToken))
                || (user != null && authUser != null && constantEq(user, authUser) && authPassword != null && constantEq(pass, authPassword))
                || (user != null && authToken != null && pass != null && constantEq(pass, authToken));
        if (!ok) {
            unauthorised(response, v2, v2 ? "unauthorized access" : "authorization failed");
            return false;
        }
        return true;
    }

    private static boolean constantEq(String a, String b) {
        return java.security.MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static void unauthorised(HttpServletResponse response, boolean v2, String msg) throws IOException {
        response.setHeader("WWW-Authenticate", "Basic realm=\"InfluxDB\"");
        if (v2) {
            writeText(response, 401, "application/json", "{\"code\":\"unauthorized\",\"message\":" + jsonStr(msg) + "}\n");
        } else {
            writeError(response, 401, msg);
        }
    }

    // ------------------------------------------------------------------ /write

    private void handleWrite(HttpServletRequest request, HttpServletResponse response, boolean v2) throws IOException {
        long start = System.nanoTime();
        Map<String, String> qp = queryParams(request);
        if (!authorised(request, response, qp, v2)) {
            return;
        }
        String db;
        String rp = qp.get("rp");
        String precision = qp.get("precision");
        if (v2) {
            db = qp.get("bucket");
            if (db == null || db.isEmpty()) {
                writeV2Error(response, 400, "invalid", "bucket is required");
                return;
            }
            int slash = db.indexOf('/');
            if (slash >= 0) {
                rp = db.substring(slash + 1);
                db = db.substring(0, slash);
            }
            if (precision != null && precision.equals("us")) {
                precision = "u";
            } else if (precision != null && !precision.isEmpty() && !precision.equals("ns") && !precision.equals("ms") && !precision.equals("s")) {
                writeV2Error(response, 400, "invalid", "invalid precision");
                return;
            }
        } else {
            db = qp.get("db");
            if (db == null || db.isEmpty()) {
                writeError(response, 400, "database is required");
                return;
            }
        }
        try {
            String cons = qp.get("consistency");
            if (cons != null && !cons.isEmpty() && !java.util.Set.of("any", "one", "quorum", "all").contains(cons.toLowerCase())) {
                writeError(response, 400, "invalid consistency level");
                return;
            }
            // InfluxDB writes the points with nanosecond precision and only then reports an unknown precision
            boolean badPrecision = precision != null && !LineProtocolParser.validPrecision(precision);
            String badPrecisionMsg = "invalid precision \"" + precision + "\" (use n, u, ms, s, m or h)";
            if (badPrecision) {
                precision = null;
            }
            String body;
            try {
                body = readBody(request);
            } catch (EOFException e) {
                writeError(response, 400, "unexpected EOF");
                return;
            } catch (ZipException e) {
                writeError(response, 400, "gzip: invalid header");
                return;
            } catch (BodyTooLarge e) {
                writeError(response, 413, "Request Entity Too Large");
                return;
            }
            LineProtocolParser.Parsed parsed = LineProtocolParser.parseLenient(body, precision);
            if (parsed.points().isEmpty() && !parsed.errors().isEmpty()) {
                String msg = String.join("\n", parsed.errors());
                if (v2) {
                    writeV2Error(response, 400, "invalid", msg);
                } else {
                    writeError(response, 400, msg);
                }
                return;
            }
            InfluxBackend.WriteOutcome outcome;
            try {
                outcome = store.write(db, rp, parsed.points(), !strictDb);
            } catch (InfluxException e) {
                if (v2) {
                    writeV2Error(response, e.status(), e.status() == 404 ? "not found" : "invalid",
                            e.status() == 404 ? "bucket \"" + db + "\" not found" : e.getMessage());
                } else {
                    writeError(response, e.status(), e.getMessage());
                }
                return;
            }
            if (!parsed.errors().isEmpty() || outcome.dropped() > 0) {
                StringBuilder msg = new StringBuilder("partial write: ");
                if (!parsed.errors().isEmpty()) {
                    msg.append(String.join("\n", parsed.errors()));
                    if (outcome.conflictMessage() != null) {
                        msg.append('\n');
                    }
                }
                if (outcome.conflictMessage() != null) {
                    msg.append(outcome.conflictMessage());
                }
                msg.append(" dropped=").append(outcome.dropped());
                if (v2) {
                    writeV2Error(response, 422, "unprocessable entity", msg.toString());
                } else {
                    writeError(response, 400, msg.toString());
                }
                return;
            }
            if (badPrecision) {
                writeError(response, 400, badPrecisionMsg);
                return;
            }
            response.setContentType("application/json");
            response.setStatus(204);
            recordMetric("write", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE, db, start);
        } catch (SQLException e) {
            log.warn("influxwire: Postgres error servicing /write (db={}): {}", db, e.getMessage());
            writeError(response, InfluxErrorMapper.status(e.getSQLState()), e.getMessage());
        } catch (InfluxException e) {
            writeError(response, e.status(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("influxwire: /write (db={}) failed", db, e);
            writeError(response, 500, String.valueOf(e.getMessage()));
        }
    }

    // ------------------------------------------------------------------ /query

    private void handleQuery(HttpServletRequest request, HttpServletResponse response) throws IOException {
        long start = System.nanoTime();
        Map<String, String> qp = queryParams(request);
        boolean post = request.getMethod().equals("POST");
        String ctype = request.getContentType();
        if (post && ctype != null && ctype.toLowerCase().startsWith("application/x-www-form-urlencoded")) {
            try {
                for (var e : parseForm(readBody(request)).entrySet()) {
                    qp.putIfAbsent(e.getKey(), e.getValue());
                }
            } catch (BodyTooLarge e) {
                writeError(response, 413, "Request Entity Too Large");
                return;
            }
        }
        if (!authorised(request, response, qp, false)) {
            return;
        }
        String q = qp.get("q");
        if (q == null || q.isBlank()) {
            writeError(response, 400, "missing required parameter \"q\"");
            return;
        }
        String db = qp.get("db");
        String epoch = qp.get("epoch");
        if (epoch != null && epoch.isEmpty()) {
            epoch = null;
        }
        boolean pretty = "true".equals(qp.get("pretty"));
        boolean chunked = "true".equalsIgnoreCase(qp.get("chunked"));
        int chunkSize = 10000;
        try {
            if (qp.get("chunk_size") != null) {
                int c = Integer.parseInt(qp.get("chunk_size"));
                if (c > 0) {
                    chunkSize = c;
                }
            }
        } catch (NumberFormatException ignored) {
            // default
        }
        Map<String, Object> params = null;
        if (qp.get("params") != null) {
            try {
                params = new LinkedHashMap<>();
                JsonObject o = JsonParser.parseString(qp.get("params")).getAsJsonObject();
                for (var e : o.entrySet()) {
                    var v = e.getValue();
                    if (v.isJsonPrimitive()) {
                        var pr = v.getAsJsonPrimitive();
                        if (pr.isBoolean()) {
                            params.put(e.getKey(), pr.getAsBoolean());
                        } else if (pr.isString()) {
                            params.put(e.getKey(), pr.getAsString());
                        } else {
                            String txt = pr.getAsString();
                            if (txt.indexOf('.') >= 0 || txt.indexOf('e') >= 0 || txt.indexOf('E') >= 0) {
                                params.put(e.getKey(), pr.getAsDouble());
                            } else {
                                params.put(e.getKey(), pr.getAsLong());
                            }
                        }
                    } else {
                        params.put(e.getKey(), v.isJsonNull() ? null : new Object());
                    }
                }
            } catch (RuntimeException e) {
                writeError(response, 400, "error parsing query parameters: " + e.getMessage());
                return;
            }
        }
        String accept = request.getHeader("Accept");
        boolean csv = accept != null && (accept.contains("application/csv") || accept.contains("text/csv"));
        List<StmtResult> results;
        try {
            results = engine.execute(q, db, qp.get("rp"), post, params);
        } catch (InfluxQl.ParseError e) {
            if (csv) {
                writeText(response, 400, accept.contains("application/csv") ? "application/csv" : "text/csv",
                        "error\n" + csvQuote(e.getMessage()) + "\n");
            } else {
                writeError(response, 400, e.getMessage());
            }
            return;
        } catch (SQLException e) {
            log.warn("influxwire: Postgres error servicing /query (db={}): {}", db, e.getMessage());
            writeError(response, InfluxErrorMapper.status(e.getSQLState()), e.getMessage());
            return;
        } catch (InfluxException e) {
            writeError(response, e.status(), e.getMessage());
            return;
        } catch (RuntimeException e) {
            log.error("influxwire: /query (db={}, q={}) failed", db, q, e);
            writeError(response, 500, String.valueOf(e.getMessage()));
            return;
        }
        if (csv) {
            writeText(response, 200, accept.contains("application/csv") ? "application/csv" : "text/csv",
                    InfluxFmt.csv(results, epoch == null ? "ns" : epoch));
        } else if (chunked) {
            writeChunked(response, results, epoch, pretty, chunkSize);
        } else {
            writeText(response, 200, "application/json", InfluxFmt.json(results, epoch, pretty) + "\n");
        }
        recordMetric("query", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ, db, start);
    }

    private static String csvQuote(String s) {
        return s.indexOf(',') >= 0 || s.indexOf('"') >= 0 ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }

    private static void writeChunked(HttpServletResponse response, List<StmtResult> results, String epoch, boolean pretty, int chunkSize)
            throws IOException {
        StringBuilder out = new StringBuilder();
        for (int ri = 0; ri < results.size(); ri++) {
            StmtResult r = results.get(ri);
            boolean lastResult = ri == results.size() - 1;
            if (r.series.isEmpty()) {
                out.append(doc(r, new ArrayList<>(), epoch, pretty));
                continue;
            }
            // one JSON document per series slice of at most chunkSize rows
            List<Object[]> slices = new ArrayList<>();
            for (Series s : r.series) {
                int n = s.values.size();
                int pos = 0;
                do {
                    int end = Math.min(n, pos + chunkSize);
                    Series part = new Series(s.name, s.tags, s.columns);
                    part.values = new ArrayList<>(s.values.subList(pos, end));
                    part.partial = end < n;
                    slices.add(new Object[] {part});
                    pos = end;
                } while (pos < n);
            }
            for (int i = 0; i < slices.size(); i++) {
                StmtResult one = new StmtResult();
                one.id = r.id;
                one.partial = i < slices.size() - 1;
                if (i == slices.size() - 1) {
                    one.error = r.error;
                    one.messages = r.messages;
                }
                List<Series> l = new ArrayList<>();
                l.add((Series) slices.get(i)[0]);
                out.append(doc(one, l, epoch, pretty));
            }
        }
        writeText(response, 200, "application/json", out.toString());
    }

    private static String doc(StmtResult r, List<Series> series, String epoch, boolean pretty) {
        StmtResult copy = new StmtResult();
        copy.id = r.id;
        copy.error = r.error;
        copy.messages = r.messages;
        copy.partial = r.partial;
        copy.series = series.isEmpty() ? r.series : series;
        if (series.isEmpty() && r.series.isEmpty()) {
            copy.series = new ArrayList<>();
        }
        return InfluxFmt.json(List.of(copy), epoch, pretty) + "\n";
    }

    // ------------------------------------------------------------------ helpers

    private static Map<String, String> queryParams(HttpServletRequest request) {
        Map<String, String> out = new LinkedHashMap<>();
        String qs = request.getQueryString();
        if (qs != null) {
            for (var e : parseForm(qs).entrySet()) {
                out.putIfAbsent(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private static String param(HttpServletRequest request, String name) {
        return queryParams(request).get(name);
    }

    private static Map<String, String> parseForm(String s) {
        Map<String, String> out = new LinkedHashMap<>();
        if (s == null || s.isEmpty()) {
            return out;
        }
        for (String pair : s.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String k = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.putIfAbsent(k, v);
        }
        return out;
    }

    private static final class BodyTooLarge extends IOException {
        BodyTooLarge() {
            super("body too large", null);
        }
    }

    private static String readBody(HttpServletRequest request) throws IOException {
        InputStream in = request.getInputStream();
        String enc = request.getHeader("Content-Encoding");
        if (enc != null && enc.equalsIgnoreCase("gzip")) {
            byte[] raw = in.readAllBytes();
            if (raw.length < 10) {
                throw new EOFException("unexpected EOF"); // shorter than a gzip header, as Go's reader reports it
            }
            in = new GZIPInputStream(new java.io.ByteArrayInputStream(raw));
        }
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[16384];
        long total = 0;
        int n;
        while ((n = in.read(buf)) > 0) {
            total += n;
            if (total > MAX_BODY) {
                throw new BodyTooLarge();
            }
            bos.write(buf, 0, n);
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

    private void recordMetric(String operation, com.sayonora.wire.core.SqlMetricsCollector.StatementKind kind, String db, long startNanos) {
        if (sqlMetrics != null) {
            long elapsedNanos = System.nanoTime() - startNanos;
            sqlMetrics.recordOperation("influxwire", db == null ? "default" : db, kind, operation, elapsedNanos, elapsedNanos);
            String outcome = kind == com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ
                    ? com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ
                    : com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE;
            sqlMetrics.recordRttOutcome("influxwire", outcome, elapsedNanos);
        }
    }

    private static String jsonStr(String s) {
        StringBuilder sb = new StringBuilder();
        InfluxFmt.jsonString(sb, s);
        return sb.toString();
    }

    private static void writeText(HttpServletResponse response, int status, String contentType, String body) throws IOException {
        response.setStatus(status);
        response.setContentType(contentType);
        response.setCharacterEncoding("UTF-8");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
    }

    /** Real InfluxDB's error shape: a flat {@code {"error": "message"}} plus an X-Influxdb-Error header. */
    private static void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setHeader("X-Influxdb-Error", message.replace('\n', ' ').replace('\r', ' '));
        writeText(response, status, "application/json", "{\"error\":" + jsonStr(message) + "}\n");
    }

    private static void writeV2Error(HttpServletResponse response, int status, String code, String message) throws IOException {
        response.setHeader("X-Platform-Error-Code", code);
        writeText(response, status, "application/json", "{\"code\":" + jsonStr(code) + ",\"message\":" + jsonStr(message) + "}\n");
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
