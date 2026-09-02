package com.sayonora.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * oswire -- OpenSearch-compatible HTTP/JSON, the same "speak the real client's wire protocol,
 * translate to plain SQL underneath" shape as dynamowire/sqswire, so a real OpenSearch client
 * (the {@code opensearch-py} SDK, curl, Kibana's dev tools) can point at Warp directly.
 *
 * <p>V1 surface, per this feature's staged plan (see {@link SearchRequest}'s and
 * {@link OpenSearchAdapter}'s javadoc for the query-DSL coverage and the Qdrant-adapter plan this
 * architecture is staged for):
 * <ul>
 *   <li>{@code POST /&lt;index&gt;/_search} -- bool/term/range/match/match_all/knn queries, sort,
 *       pagination</li>
 *   <li>{@code PUT /&lt;index&gt;/_doc/&lt;id&gt;} -- index (upsert) one document</li>
 *   <li>{@code GET /&lt;index&gt;/_doc/&lt;id&gt;} -- fetch one document</li>
 *   <li>{@code DELETE /&lt;index&gt;/_doc/&lt;id&gt;} -- delete one document</li>
 *   <li>{@code PUT /&lt;index&gt;} -- create the index (no mapping semantics in V1 -- just
 *       ensures the backing table exists)</li>
 *   <li>{@code POST /_bulk} and {@code POST /&lt;index&gt;/_bulk} -- NDJSON bulk index/delete</li>
 * </ul>
 * Aggregations, stronger hybrid/full-text/vector scoring, and update-by-query are explicitly V2 --
 * not implemented here, not silently approximated either (see
 * {@link OpenSearchAdapter#parseQuery} for the "unrecognized clause fails loudly" policy).
 */
public final class OpenSearchWireServer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchWireServer.class);

    private static final Pattern SEARCH_PATH = Pattern.compile("^/([^/]+)/_search/?$");
    private static final Pattern DOC_PATH = Pattern.compile("^/([^/]+)/_doc/([^/]+)/?$");
    private static final Pattern INDEX_PATH = Pattern.compile("^/([^/]+)/?$");
    private static final Pattern BULK_PATH = Pattern.compile("^(?:/([^/]+))?/_bulk/?$");
    private static final Pattern REFRESH_PATH = Pattern.compile("^(?:/([^/]+))?/_refresh/?$");

    private final Server server;
    private final PostgresSearchStore store;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    public OpenSearchWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry) {
        this(port, backendRegistry, com.sayonora.wire.acl.ConnectionGate.DISABLED,
                com.sayonora.wire.http.auth.AccessContextResolver.DISABLED, null);
    }

    public OpenSearchWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.http.auth.AccessContextResolver oauth,
            com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = new PostgresSearchStore(backendRegistry);
        this.sqlMetrics = sqlMetrics;
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                response.setContentType("application/json");
                if (!connectionGate.acceptHttp(request)) {
                    writeError(response, 403, "security_exception", "forbidden");
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
        Matcher m;

        if ((m = SEARCH_PATH.matcher(target)).matches() && (method.equals("POST") || method.equals("GET"))) {
            handleSearch(request, response, m.group(1));
            return;
        }
        if ((m = DOC_PATH.matcher(target)).matches()) {
            String index = m.group(1);
            String id = m.group(2);
            switch (method) {
                case "PUT", "POST" -> handleIndexDoc(request, response, index, id);
                case "GET" -> handleGetDoc(response, index, id);
                case "DELETE" -> handleDeleteDoc(response, index, id);
                default -> writeError(response, 405, "method_not_allowed", method + " not supported on " + target);
            }
            return;
        }
        if ((m = BULK_PATH.matcher(target)).matches() && method.equals("POST")) {
            handleBulk(request, response, m.group(1));
            return;
        }
        if ((m = REFRESH_PATH.matcher(target)).matches() && method.equals("POST")) {
            handleRefresh(response, m.group(1));
            return;
        }
        if ((m = INDEX_PATH.matcher(target)).matches() && method.equals("PUT")) {
            handleCreateIndex(response, m.group(1));
            return;
        }
        writeError(response, 404, "index_not_found_exception", "no oswire route for " + method + " " + target);
    }

    private void handleSearch(HttpServletRequest request, HttpServletResponse response, String index) throws IOException {
        long start = System.nanoTime();
        try {
            String body = readBody(request);
            JsonObject requestJson = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
            SearchRequest searchRequest = OpenSearchAdapter.parseSearch(index, requestJson);
            SearchResult result = store.search(searchRequest);
            long tookMillis = (System.nanoTime() - start) / 1_000_000;
            // Real OpenSearch only prefixes aggregation response keys with their type
            // ("sterms#name" instead of "name") when the request asks via ?typed_keys=true --
            // not unconditionally. Official high-level clients (opensearch-java) always send that
            // param themselves because their strict typed deserializer needs it; opensearch-py's
            // low-level client (confirmed live: it never sends the param) returns bare names, and
            // a caller reading the response as a plain dict expects that. Defaulting to real
            // OpenSearch's own default (untyped) keeps that path's response shape correct instead
            // of unconditionally breaking it to satisfy the strict client.
            boolean typedKeys = "true".equalsIgnoreCase(request.getParameter("typed_keys"));
            writeJson(response, 200, OpenSearchAdapter.renderSearchResponse(result, tookMillis, typedKeys));
            recordMetric("_search", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ, index, start);
        } catch (OpenSearchException e) {
            writeError(response, 400, e.errorType, e.getMessage());
        } catch (SQLException e) {
            log.warn("oswire: Postgres error servicing _search on \"{}\": {}", index, e.getMessage());
            writeError(response, OpenSearchErrorMapper.status(e.getSQLState()), OpenSearchErrorMapper.errorType(e.getSQLState()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("oswire: _search on \"{}\" failed", index, e);
            writeError(response, 400, "parsing_exception", String.valueOf(e.getMessage()));
        }
    }

    private void handleIndexDoc(HttpServletRequest request, HttpServletResponse response, String index, String id)
            throws IOException {
        long start = System.nanoTime();
        try {
            String body = readBody(request);
            JsonObject doc = JsonParser.parseString(body).getAsJsonObject();
            float[] vector = OpenSearchAdapter.extractVector(doc);
            store.indexDocument(index, id, doc, vector);
            JsonObject resp = new JsonObject();
            resp.addProperty("_index", index);
            resp.addProperty("_id", id);
            resp.addProperty("result", "created");
            addVersionFields(resp);
            writeJson(response, 201, resp);
            recordMetric("_doc", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE, index, start);
        } catch (SQLException e) {
            writeError(response, OpenSearchErrorMapper.status(e.getSQLState()), OpenSearchErrorMapper.errorType(e.getSQLState()), e.getMessage());
        } catch (RuntimeException e) {
            writeError(response, 400, "parsing_exception", String.valueOf(e.getMessage()));
        }
    }

    /**
     * {@code _version}/{@code _seq_no}/{@code _primary_term} are real OpenSearch's optimistic-
     * concurrency-control fields -- V1 doesn't implement versioning (every write just overwrites,
     * see {@code PostgresSearchStore#indexDocument}'s upsert), but official clients (confirmed
     * live with {@code opensearch-java}: it throws a deserialization error without them --
     * {@code Missing required property 'IndexResponse.primaryTerm'}) require these fields to be
     * present in the response schema regardless. Reporting fixed placeholder values here is
     * honest about what V1 actually does (no real versioning) while still being a response real
     * clients can parse; a real optimistic-concurrency implementation is out of V1's scope.
     */
    private static void addVersionFields(JsonObject resp) {
        resp.addProperty("_version", 1);
        resp.addProperty("_seq_no", 0);
        resp.addProperty("_primary_term", 1);
        // Real OpenSearch's per-request shard-acknowledgement summary -- V1 has no shards to
        // report against (one plain Postgres table, not a sharded index), but official clients
        // require the field to be present in the response schema regardless (confirmed live:
        // opensearch-java also throws without this one, after _version/_seq_no/_primary_term were
        // added -- same "satisfy the schema honestly" reasoning as those).
        JsonObject shards = new JsonObject();
        shards.addProperty("total", 1);
        shards.addProperty("successful", 1);
        shards.addProperty("failed", 0);
        resp.add("_shards", shards);
    }

    /**
     * Real OpenSearch's {@code GET /<index>/_doc/<id>} returns HTTP 200 for a missing document
     * too -- {@code {"found": false}}, no {@code _source} -- not a 404. Confirmed live: the
     * official {@code opensearch-java} client's high-level {@code get()} throws on a non-2xx
     * response, so returning 404 here (this endpoint's first version did) broke a real client
     * that otherwise handles "not found" via the response body, exactly as real OpenSearch
     * expects it to.
     */
    private void handleGetDoc(HttpServletResponse response, String index, String id) throws IOException {
        long start = System.nanoTime();
        try {
            JsonObject doc = store.getDocument(index, id);
            JsonObject resp = new JsonObject();
            resp.addProperty("_index", index);
            resp.addProperty("_id", id);
            resp.addProperty("found", doc != null);
            if (doc != null) {
                addVersionFields(resp);
                resp.add("_source", doc);
            }
            writeJson(response, 200, resp);
            recordMetric("_doc", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ, index, start);
        } catch (SQLException e) {
            writeError(response, OpenSearchErrorMapper.status(e.getSQLState()), OpenSearchErrorMapper.errorType(e.getSQLState()), e.getMessage());
        }
    }

    private void handleDeleteDoc(HttpServletResponse response, String index, String id) throws IOException {
        long start = System.nanoTime();
        try {
            boolean deleted = store.deleteDocument(index, id);
            JsonObject resp = new JsonObject();
            resp.addProperty("_index", index);
            resp.addProperty("_id", id);
            resp.addProperty("result", deleted ? "deleted" : "not_found");
            addVersionFields(resp);
            writeJson(response, deleted ? 200 : 404, resp);
            recordMetric("_doc", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE, index, start);
        } catch (SQLException e) {
            writeError(response, OpenSearchErrorMapper.status(e.getSQLState()), OpenSearchErrorMapper.errorType(e.getSQLState()), e.getMessage());
        }
    }

    /**
     * Real OpenSearch's {@code _refresh} makes recent writes visible to search (Lucene segments
     * are flushed on a delay by default). oswire has nothing to flush -- every write already
     * commits directly to Postgres and is immediately visible to the very next {@code _search} --
     * so this is a genuine, honest no-op, not a stub pretending to do work: there's no staleness
     * window here for it to close. Still a real route, not a 404, because real clients call it
     * routinely (the official {@code opensearch-java} client's {@code indices().refresh()}, and
     * every example in OpenSearch's own docs after a write-then-immediately-search sequence) and
     * would otherwise fail against oswire for no reason a caller could act on.
     */
    private void handleRefresh(HttpServletResponse response, String index) throws IOException {
        JsonObject resp = new JsonObject();
        JsonObject shards = new JsonObject();
        shards.addProperty("total", 1);
        shards.addProperty("successful", 1);
        shards.addProperty("failed", 0);
        resp.add("_shards", shards);
        writeJson(response, 200, resp);
    }

    private void handleCreateIndex(HttpServletResponse response, String index) throws IOException {
        try {
            store.ensureCollection(index);
            JsonObject resp = new JsonObject();
            resp.addProperty("acknowledged", true);
            resp.addProperty("index", index);
            writeJson(response, 200, resp);
        } catch (SQLException e) {
            writeError(response, OpenSearchErrorMapper.status(e.getSQLState()), OpenSearchErrorMapper.errorType(e.getSQLState()), e.getMessage());
        }
    }

    /**
     * NDJSON bulk body: each action is two lines -- {@code {"index":{"_index":"i","_id":"1"}}}
     * (or {@code "delete"}) followed by the document body (index/create actions only; delete has
     * no second line). {@code defaultIndex} is used when a URL-path index was given
     * ({@code POST /<index>/_bulk}); an action's own {@code _index} always overrides it, matching
     * real OpenSearch/Elasticsearch bulk semantics.
     */
    private void handleBulk(HttpServletRequest request, HttpServletResponse response, String defaultIndex) throws IOException {
        String body = readBody(request);
        List<String> lines = body.lines().filter(l -> !l.isBlank()).toList();
        com.google.gson.JsonArray items = new com.google.gson.JsonArray();
        boolean anyErrors = false;
        long start = System.nanoTime();
        int i = 0;
        while (i < lines.size()) {
            JsonObject action = JsonParser.parseString(lines.get(i)).getAsJsonObject();
            String actionType = action.keySet().iterator().next();
            JsonObject meta = action.getAsJsonObject(actionType);
            String index = meta.has("_index") ? meta.get("_index").getAsString() : defaultIndex;
            String id = meta.has("_id") ? meta.get("_id").getAsString() : null;
            JsonObject itemResult = new JsonObject();
            try {
                switch (actionType) {
                    case "index", "create" -> {
                        i++;
                        JsonObject doc = JsonParser.parseString(lines.get(i)).getAsJsonObject();
                        if (id == null) {
                            throw new OpenSearchException("action_request_validation_exception",
                                    "oswire V1 requires an explicit _id for bulk index/create (no auto-generated ids yet)");
                        }
                        store.indexDocument(index, id, doc, OpenSearchAdapter.extractVector(doc));
                        itemResult.addProperty("_index", index);
                        itemResult.addProperty("_id", id);
                        itemResult.addProperty("status", 201);
                        addVersionFields(itemResult);
                    }
                    case "delete" -> {
                        boolean deleted = store.deleteDocument(index, id);
                        itemResult.addProperty("_index", index);
                        itemResult.addProperty("_id", id);
                        itemResult.addProperty("status", deleted ? 200 : 404);
                        addVersionFields(itemResult);
                    }
                    default -> throw new OpenSearchException("action_request_validation_exception",
                            "oswire V1 bulk supports index/create/delete, not \"" + actionType + "\"");
                }
            } catch (SQLException | RuntimeException e) {
                anyErrors = true;
                JsonObject err = new JsonObject();
                String errorType;
                int status;
                if (e instanceof OpenSearchException ose) {
                    errorType = ose.errorType;
                    status = 400;
                } else if (e instanceof SQLException sqlEx) {
                    errorType = OpenSearchErrorMapper.errorType(sqlEx.getSQLState());
                    status = OpenSearchErrorMapper.status(sqlEx.getSQLState());
                } else {
                    errorType = OpenSearchErrorMapper.DEFAULT_ERROR_TYPE;
                    status = OpenSearchErrorMapper.DEFAULT_STATUS;
                }
                err.addProperty("type", errorType);
                err.addProperty("reason", e.getMessage());
                itemResult.add("error", err);
                itemResult.addProperty("status", status);
            }
            com.google.gson.JsonObject wrapper = new com.google.gson.JsonObject();
            wrapper.add(actionType, itemResult);
            items.add(wrapper);
            i++;
        }
        JsonObject resp = new JsonObject();
        resp.addProperty("took", (System.nanoTime() - start) / 1_000_000);
        resp.addProperty("errors", anyErrors);
        resp.add("items", items);
        writeJson(response, 200, resp);
        recordMetric("_bulk", com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE,
                defaultIndex == null ? "default" : defaultIndex, start);
    }

    /**
     * Also feeds the admin console's "RTT by outcome" card -- oswire has no result cache (see
     * {@code PostgresSearchStore}'s javadoc: every {@code _search}/{@code _doc} call hits Postgres
     * directly, there's no cache_hit case to report), so unlike pgwire/mongowire/dynamowire this
     * only ever reports {@code pg_read}/{@code pg_write}, never {@code cache_hit} -- an honest
     * absence, not a missing wire-up. (Found live: oswire showed up in "Wire protocol traffic" and
     * "Top 10 SQL by cost" but was silently missing from "RTT by outcome" entirely, because this
     * method only ever called {@code recordOperation}, never {@code recordRttOutcome} -- the two
     * are separate calls into {@code SqlMetricsCollector}, and only the latter feeds that card.)
     */
    private void recordMetric(String operation, com.sayonora.wire.core.SqlMetricsCollector.StatementKind kind,
            String index, long startNanos) {
        if (sqlMetrics != null) {
            long elapsedNanos = System.nanoTime() - startNanos;
            sqlMetrics.recordOperation("oswire", index, kind, operation, elapsedNanos, elapsedNanos);
            String outcome = switch (kind) {
                case READ -> com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ;
                case WRITE -> com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE;
                case OTHER -> null;
            };
            if (outcome != null) {
                sqlMetrics.recordRttOutcome("oswire", outcome, elapsedNanos);
            }
        }
    }

    private static String readBody(HttpServletRequest request) throws IOException {
        return new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void writeJson(HttpServletResponse response, int status, JsonObject body) throws IOException {
        response.setStatus(status);
        response.getWriter().write(body.toString());
    }

    private static void writeError(HttpServletResponse response, int status, String errorType, String message) throws IOException {
        JsonObject err = new JsonObject();
        JsonObject inner = new JsonObject();
        // root_cause is a real, load-bearing part of OpenSearch's/Elasticsearch's actual error
        // shape, not decorative -- opensearch-py and other real clients read type/reason from
        // root_cause[0], not (only) the top-level type/reason, for the exception it actually
        // raises client-side. This was missing entirely before -- a client got a top-level
        // type/reason that looked plausible but wasn't the real shape a genuine OpenSearch server
        // sends. One cause per error here (this server never produces a multi-cause failure),
        // matching real OpenSearch's own shape for the common single-cause case.
        JsonObject cause = new JsonObject();
        cause.addProperty("type", errorType);
        cause.addProperty("reason", message);
        JsonArray rootCause = new JsonArray();
        rootCause.add(cause);
        inner.add("root_cause", rootCause);
        inner.addProperty("type", errorType);
        inner.addProperty("reason", message);
        err.add("error", inner);
        err.addProperty("status", status);
        response.setStatus(status);
        response.getWriter().write(err.toString());
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
