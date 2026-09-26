package com.sayonora.warp.oswire;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * oswire -- an OpenSearch-compatible REST/JSON frontend over Postgres (see {@link PostgresSearchStore}): the same
 * "speak the real client's wire protocol, keep the data in plain Postgres" shape as dynamowire/sqswire, so a real
 * OpenSearch client ({@code opensearch-py}, {@code opensearch-java}, Logstash/Fluent Bit outputs, curl) can point
 * at Warp directly. Routing and handlers are in {@link RestApi}; query DSL, aggregations, scoring in
 * {@link SearchEngine}/{@link Query}/{@link Aggs}. Unsupported features fail with an OpenSearch-shaped error (see
 * docs/WARP_GUIDE.md for the supported/unsupported lists).
 */
public final class OpenSearchWireServer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchWireServer.class);
    private static final com.google.gson.Gson PRETTY = new GsonBuilder().setPrettyPrinting().serializeNulls().create();

    private final Server server;
    private final PostgresSearchStore store;
    private final RestApi api;
    private final com.sayonora.warp.core.SqlMetricsCollector sqlMetrics;

    public OpenSearchWireServer(int port, com.sayonora.warp.core.BackendRegistry backendRegistry) {
        this(port, backendRegistry, com.sayonora.warp.acl.ConnectionGate.DISABLED,
                com.sayonora.warp.http.auth.AccessContextResolver.DISABLED, null);
    }

    public OpenSearchWireServer(int port, com.sayonora.warp.core.BackendRegistry backendRegistry,
            com.sayonora.warp.acl.ConnectionGate connectionGate,
            com.sayonora.warp.http.auth.AccessContextResolver oauth,
            com.sayonora.warp.core.SqlMetricsCollector sqlMetrics) {
        this.store = new PostgresSearchStore(backendRegistry);
        this.api = new RestApi(store);
        this.sqlMetrics = sqlMetrics;
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                response.setCharacterEncoding("UTF-8");
                if (!connectionGate.acceptHttp(request)) {
                    writeJson(response, RestApi.errorJson(new OpenSearchException("security_exception", "forbidden", 403)), 403, null, false);
                    return;
                }
                if (oauth.enforce(request, response) == null) {
                    return;
                }
                serve(request, response, target);
            }
        });
    }

    private static Map<String, String> queryParams(String qs) {
        Map<String, String> out = new LinkedHashMap<>();
        if (qs == null || qs.isEmpty()) {
            return out;
        }
        for (String part : qs.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int i = part.indexOf('=');
            String k = URLDecoder.decode(i < 0 ? part : part.substring(0, i), StandardCharsets.UTF_8);
            String v = i < 0 ? "" : URLDecoder.decode(part.substring(i + 1), StandardCharsets.UTF_8);
            out.put(k, v);
        }
        return out;
    }

    private static String readBody(HttpServletRequest request) throws IOException {
        var in = request.getInputStream();
        String enc = request.getHeader("Content-Encoding");
        byte[] bytes = (enc != null && enc.toLowerCase().contains("gzip") ? new GZIPInputStream(in) : in).readAllBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void serve(HttpServletRequest request, HttpServletResponse response, String target) throws IOException {
        long start = System.nanoTime();
        String method = request.getMethod();
        RestApi.Request req = new RestApi.Request();
        req.method = method;
        req.path = target;
        req.q = queryParams(request.getQueryString());
        String rawPath = request.getRequestURI();
        if (rawPath.length() > 1 && rawPath.endsWith("/")) {
            rawPath = rawPath.substring(0, rawPath.length() - 1);
        }
        Map<String, String> vars = new LinkedHashMap<>();
        Set<String> allowed = new LinkedHashSet<>();
        String[] segs = rawPath.equals("/") ? new String[] {""} : rawPath.substring(1).split("/", -1);
        for (int i = 0; i < segs.length; i++) {
            segs[i] = URLDecoder.decode(segs[i].replace("+", "%2B"), StandardCharsets.UTF_8);
        }
        if (segs.length > 0 && segs[0].startsWith("<") && segs[0].endsWith(">")) {
            try {
                segs[0] = Dates.indexNameMath(segs[0]);
            } catch (OpenSearchException e) {
                // reported below like any other error
                segs[0] = segs[0];
            }
        }
        RestApi.Route route = null;
        boolean pretty = false;
        String filterPath = req.q.get("filter_path");
        try {
            route = api.match(method, segs, vars, allowed);
            pretty = req.q.containsKey("pretty") && !"false".equals(req.q.get("pretty"));
            if (route == null) {
                drain(request);
                if (!allowed.isEmpty()) {
                    JsonObject o = new JsonObject();
                    o.addProperty("error", "Incorrect HTTP method for uri [" + target + (request.getQueryString() != null ? "?" + request.getQueryString() : "")
                            + "] and method [" + method + "], allowed: " + allowed.stream().sorted().toList());
                    o.addProperty("status", 405);
                    writeJson(response, o, 405, null, pretty);
                } else {
                    JsonObject o = new JsonObject();
                    o.addProperty("error", "no handler found for uri [" + target + (request.getQueryString() != null ? "?" + request.getQueryString() : "")
                            + "] and method [" + method + "]");
                    writeJson(response, o, 400, method, pretty);
                }
                return;
            }
            req.vars = vars;
            req.body = readBody(request);
            RestApi.Response resp = route.h().handle(req);
            long elapsed = System.nanoTime() - start;
            if (resp.text != null) {
                writeText(response, resp.status, resp.text);
            } else {
                JsonElement json = resp.json;
                if (filterPath != null && json != null && !method.equals("HEAD")) {
                    json = FilterPath.apply(json, filterPath);
                }
                writeJson(response, json, resp.status, method, pretty);
            }
            recordMetric(route.op(), route.write(), vars.get("index"), start);
        } catch (OpenSearchException e) {
            writeJson(response, RestApi.errorJson(e), e.status, method, pretty);
        } catch (SQLException e) {
            log.warn("oswire: Postgres error on {} {}: {}", method, target, e.getMessage());
            String type = OpenSearchErrorMapper.errorType(e.getSQLState());
            int status = OpenSearchErrorMapper.status(e.getSQLState());
            OpenSearchException oe = type.equals("index_not_found_exception")
                    ? OpenSearchException.indexNotFound(vars.getOrDefault("index", "_all")) : new OpenSearchException(type, e.getMessage(), status);
            writeJson(response, RestApi.errorJson(oe), status, method, pretty);
        } catch (com.google.gson.JsonParseException e) {
            writeJson(response, RestApi.errorJson(new OpenSearchException("json_parse_exception", "Failed to parse content: " + e.getMessage())), 400, method, pretty);
        } catch (NumberFormatException | ClassCastException | IllegalStateException | UnsupportedOperationException
                | IndexOutOfBoundsException | NullPointerException e) {
            // a request body of the wrong shape (a string where a number belongs, a missing required key, ...)
            log.debug("oswire: {} {} rejected: {}", method, target, e.toString());
            writeJson(response, RestApi.errorJson(new OpenSearchException("x_content_parse_exception",
                    "failed to parse the request: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()))), 400, method, pretty);
        } catch (RuntimeException e) {
            log.error("oswire: {} {} failed", method, target, e);
            writeJson(response, RestApi.errorJson(new OpenSearchException("exception", String.valueOf(e.getMessage()), 500)), 500, method, pretty);
        }
    }

    private static void drain(HttpServletRequest request) throws IOException {
        request.getInputStream().readAllBytes();
    }

    private void recordMetric(String operation, boolean write, String index, long startNanos) {
        if (sqlMetrics != null) {
            long elapsedNanos = System.nanoTime() - startNanos;
            var kind = write ? com.sayonora.warp.core.SqlMetricsCollector.StatementKind.WRITE : com.sayonora.warp.core.SqlMetricsCollector.StatementKind.READ;
            sqlMetrics.recordOperation("oswire", index == null ? "default" : index, kind, operation, elapsedNanos, elapsedNanos);
            sqlMetrics.recordRttOutcome("oswire", write ? com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_PG_WRITE
                    : com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_PG_READ, elapsedNanos);
        }
    }

    private static void writeText(HttpServletResponse response, int status, String text) throws IOException {
        response.setStatus(status);
        response.setContentType("text/plain; charset=UTF-8");
        response.getWriter().write(text);
    }

    private static void writeJson(HttpServletResponse response, JsonElement body, int status, String method, boolean pretty) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json; charset=UTF-8");
        if ("HEAD".equals(method) || body == null) {
            return;
        }
        response.getWriter().write(pretty ? PRETTY.toJson(body) + "\n" : body.toString());
    }

    public void start() throws Exception {
        server.start();
        log.info("warp listening for OpenSearch HTTP/JSON (oswire) on port {}",
                ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
    }
}
