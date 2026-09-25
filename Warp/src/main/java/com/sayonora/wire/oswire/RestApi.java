package com.sayonora.wire.oswire;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The OpenSearch REST surface of oswire: routing plus every handler. Document APIs (index/create/get/source/delete/
 * update/bulk/mget/delete_by_query/update_by_query/reindex), search APIs (search/msearch/count/scroll/PIT/explain/
 * validate/field_caps), index management (create/delete/get/exists, mappings, settings, aliases, templates, analyze,
 * refresh/flush/... no-ops), and cluster/cat/info endpoints that clients probe on connect.
 */
final class RestApi {

    static final String VERSION = "2.19.6";
    static final String CLUSTER_NAME = "warp-oswire";
    static final String NODE_NAME = "warp-node-1";
    static final String CLUSTER_UUID = "WarpOsWireClusterUuid01";
    private static final Gson PRETTY = new GsonBuilder().setPrettyPrinting().serializeNulls().create();

    final PostgresSearchStore store;
    final SearchEngine engine;
    final List<Route> routes = new ArrayList<>();

    static final class Request {
        String method;
        String path;
        Map<String, String> q = new LinkedHashMap<>();
        String body = "";
        Map<String, String> vars = new LinkedHashMap<>();

        String var(String k) {
            return vars.get(k);
        }

        boolean flag(String k, boolean dflt) {
            String v = q.get(k);
            return v == null ? dflt : !v.equals("false");
        }
    }

    static final class Response {
        int status = 200;
        JsonElement json;
        String text;
        boolean head;

        static Response json(int status, JsonElement j) {
            Response r = new Response();
            r.status = status;
            r.json = j;
            return r;
        }

        static Response text(int status, String t) {
            Response r = new Response();
            r.status = status;
            r.text = t;
            return r;
        }
    }

    interface Handler {
        Response handle(Request r) throws SQLException;
    }

    record Route(Set<String> methods, String[] segs, Handler h, String op, boolean write) {
    }

    RestApi(PostgresSearchStore store) {
        this.store = store;
        this.engine = new SearchEngine(store);
        registerRoutes();
    }

    // ------------------------------------------------------------------ routing

    private void add(String methods, String path, String op, boolean write, Handler h) {
        routes.add(new Route(Set.of(methods.split(",")), path.substring(1).split("/", -1), h, op, write));
    }

    private void registerRoutes() {
        // ---- root / cluster / nodes / cat ----
        add("GET,HEAD", "/", "info", false, this::info);
        add("GET", "/_cluster/health", "_cluster", false, this::clusterHealth);
        add("GET", "/_cluster/health/{index}", "_cluster", false, this::clusterHealth);
        add("GET", "/_cluster/settings", "_cluster", false, r -> Response.json(200, clusterSettings(r)));
        add("PUT", "/_cluster/settings", "_cluster", true, this::putClusterSettings);
        add("GET", "/_cluster/stats", "_cluster", false, this::clusterStats);
        add("GET", "/_cluster/state", "_cluster", false, this::clusterState);
        add("GET", "/_cluster/state/{metric}", "_cluster", false, this::clusterState);
        add("GET", "/_cluster/state/{metric}/{index}", "_cluster", false, this::clusterState);
        add("GET", "/_cluster/pending_tasks", "_cluster", false, r -> obj("tasks", new JsonArray()));
        add("GET", "/_nodes", "_nodes", false, this::nodesInfo);
        add("GET", "/_nodes/stats", "_nodes", false, this::nodesStats);
        add("GET", "/_nodes/stats/{metric}", "_nodes", false, this::nodesStats);
        add("GET", "/_nodes/{node}", "_nodes", false, this::nodesInfo);
        add("GET", "/_nodes/{node}/{metric}", "_nodes", false, this::nodesInfo);
        add("GET", "/_cat", "_cat", false, r -> Response.text(200, "=^.^=\n/_cat/aliases\n/_cat/allocation\n/_cat/cluster_manager\n/_cat/count\n"
                + "/_cat/health\n/_cat/indices\n/_cat/master\n/_cat/nodes\n/_cat/plugins\n/_cat/shards\n/_cat/templates\n"));
        add("GET", "/_cat/indices", "_cat", false, r -> Cat.indices(this, r));
        add("GET", "/_cat/indices/{index}", "_cat", false, r -> Cat.indices(this, r));
        add("GET", "/_cat/health", "_cat", false, r -> Cat.health(this, r));
        add("GET", "/_cat/count", "_cat", false, r -> Cat.count(this, r));
        add("GET", "/_cat/count/{index}", "_cat", false, r -> Cat.count(this, r));
        add("GET", "/_cat/shards", "_cat", false, r -> Cat.shards(this, r));
        add("GET", "/_cat/shards/{index}", "_cat", false, r -> Cat.shards(this, r));
        add("GET", "/_cat/aliases", "_cat", false, r -> Cat.aliases(this, r));
        add("GET", "/_cat/aliases/{name}", "_cat", false, r -> Cat.aliases(this, r));
        add("GET", "/_cat/nodes", "_cat", false, r -> Cat.nodes(this, r));
        add("GET", "/_cat/master", "_cat", false, r -> Cat.master(this, r));
        add("GET", "/_cat/cluster_manager", "_cat", false, r -> Cat.master(this, r));
        add("GET", "/_cat/templates", "_cat", false, r -> Cat.templates(this, r));
        add("GET", "/_cat/templates/{name}", "_cat", false, r -> Cat.templates(this, r));
        add("GET", "/_cat/plugins", "_cat", false, r -> Cat.plugins(this, r));
        add("GET", "/_cat/allocation", "_cat", false, r -> Cat.allocation(this, r));

        // ---- global (index-less) APIs ----
        add("GET,POST", "/_search", "_search", false, this::search);
        add("GET,POST", "/_search/scroll", "_search", false, this::scroll);
        add("GET,POST", "/_search/scroll/{scroll_id}", "_search", false, this::scroll);
        add("DELETE", "/_search/scroll", "_search", true, this::clearScroll);
        add("DELETE", "/_search/scroll/{scroll_id}", "_search", true, this::clearScroll);
        add("GET,POST,PUT", "/_search/point_in_time/_all", "_search", false, this::pitAll);
        add("DELETE", "/_search/point_in_time/_all", "_search", true, this::deleteAllPits);
        add("DELETE", "/_search/point_in_time", "_search", true, this::deletePit);
        add("GET,POST", "/_msearch", "_msearch", false, this::msearch);
        add("GET,POST", "/_count", "_count", false, this::count);
        add("GET,POST", "/_mget", "_mget", false, this::mget);
        add("POST,PUT", "/_bulk", "_bulk", true, this::bulk);
        add("POST", "/_reindex", "_reindex", true, this::reindex);
        add("POST,GET", "/_delete_by_query", "_delete_by_query", true, r -> Response.json(400, error("illegal_argument_exception", "index is required")));
        add("GET,POST", "/_validate/query", "_validate", false, this::validateQuery);
        add("GET,POST", "/_field_caps", "_field_caps", false, this::fieldCaps);
        add("POST,GET", "/_analyze", "_analyze", false, this::analyze);
        add("POST", "/_refresh", "_refresh", true, this::refresh);
        add("GET", "/_refresh", "_refresh", true, this::refresh);
        add("POST", "/_flush", "_flush", true, this::refresh);
        add("POST", "/_forcemerge", "_forcemerge", true, this::refresh);
        add("POST", "/_cache/clear", "_cache", true, this::refresh);
        add("GET", "/_stats", "_stats", false, this::indicesStats);
        add("GET", "/_stats/{metric}", "_stats", false, this::indicesStats);
        add("GET", "/_mapping", "_mapping", false, this::getMapping);
        add("GET", "/_mapping/field/{fields}", "_mapping", false, this::getFieldMapping);
        add("GET", "/_settings", "_settings", false, this::getSettings);
        add("GET", "/_settings/{name}", "_settings", false, this::getSettings);
        add("GET", "/_alias", "_alias", false, this::getAlias);
        add("GET", "/_alias/{name}", "_alias", false, this::getAlias);
        add("HEAD", "/_alias/{name}", "_alias", false, this::getAlias);
        add("GET", "/_aliases", "_alias", false, this::getAlias);
        add("POST", "/_aliases", "_alias", true, this::updateAliases);
        add("GET,HEAD", "/_template", "_template", false, r -> getTemplate(r, "template"));
        add("GET,HEAD", "/_template/{name}", "_template", false, r -> getTemplate(r, "template"));
        add("PUT,POST", "/_template/{name}", "_template", true, r -> putTemplate(r, "template"));
        add("DELETE", "/_template/{name}", "_template", true, r -> deleteTemplate(r, "template"));
        add("GET,HEAD", "/_index_template", "_index_template", false, r -> getTemplate(r, "index_template"));
        add("GET,HEAD", "/_index_template/{name}", "_index_template", false, r -> getTemplate(r, "index_template"));
        add("PUT,POST", "/_index_template/{name}", "_index_template", true, r -> putTemplate(r, "index_template"));
        add("DELETE", "/_index_template/{name}", "_index_template", true, r -> deleteTemplate(r, "index_template"));
        add("GET,HEAD", "/_component_template", "_component_template", false, r -> getTemplate(r, "component_template"));
        add("GET,HEAD", "/_component_template/{name}", "_component_template", false, r -> getTemplate(r, "component_template"));
        add("PUT,POST", "/_component_template/{name}", "_component_template", true, r -> putTemplate(r, "component_template"));
        add("DELETE", "/_component_template/{name}", "_component_template", true, r -> deleteTemplate(r, "component_template"));
        add("GET", "/_resolve/index/{name}", "_resolve", false, this::resolveIndex);
        add("GET", "/_tasks", "_tasks", false, r -> obj("nodes", new JsonObject()));

        // ---- index scoped ----
        add("POST", "/{index}/_search/point_in_time", "_search", true, this::createPit);
        add("GET,POST", "/{index}/_search", "_search", false, this::search);
        add("GET,POST", "/{index}/_msearch", "_msearch", false, this::msearch);
        add("GET,POST", "/{index}/_count", "_count", false, this::count);
        add("GET,POST", "/{index}/_mget", "_mget", false, this::mget);
        add("POST,PUT", "/{index}/_bulk", "_bulk", true, this::bulk);
        add("POST", "/{index}/_delete_by_query", "_delete_by_query", true, this::deleteByQuery);
        add("POST", "/{index}/_update_by_query", "_update_by_query", true, this::updateByQuery);
        add("GET,POST", "/{index}/_validate/query", "_validate", false, this::validateQuery);
        add("GET,POST", "/{index}/_field_caps", "_field_caps", false, this::fieldCaps);
        add("GET,POST", "/{index}/_analyze", "_analyze", false, this::analyze);
        add("POST,GET", "/{index}/_refresh", "_refresh", true, this::refresh);
        add("POST,GET", "/{index}/_flush", "_flush", true, this::refresh);
        add("POST", "/{index}/_forcemerge", "_forcemerge", true, this::refresh);
        add("POST", "/{index}/_cache/clear", "_cache", true, this::refresh);
        add("POST", "/{index}/_close", "_close", true, r -> openClose(r, true));
        add("POST", "/{index}/_open", "_open", true, r -> openClose(r, false));
        add("GET", "/{index}/_stats", "_stats", false, this::indicesStats);
        add("GET", "/{index}/_stats/{metric}", "_stats", false, this::indicesStats);
        add("GET", "/{index}/_mapping", "_mapping", false, this::getMapping);
        add("PUT,POST", "/{index}/_mapping", "_mapping", true, this::putMapping);
        add("GET", "/{index}/_mapping/field/{fields}", "_mapping", false, this::getFieldMapping);
        add("GET", "/{index}/_settings", "_settings", false, this::getSettings);
        add("GET", "/{index}/_settings/{name}", "_settings", false, this::getSettings);
        add("PUT", "/{index}/_settings", "_settings", true, this::putSettings);
        add("GET", "/{index}/_alias", "_alias", false, this::getAlias);
        add("GET,HEAD", "/{index}/_alias/{name}", "_alias", false, this::getAlias);
        add("GET", "/{index}/_aliases", "_alias", false, this::getAlias);
        add("PUT,POST", "/_alias/{name}", "_alias", true, this::putAlias);
        add("PUT,POST", "/_aliases/{name}", "_alias", true, this::putAlias);
        add("PUT,POST", "/_alias", "_alias", true, this::putAlias);
        add("DELETE", "/_alias/{name}", "_alias", true, this::deleteAlias);
        add("PUT,POST", "/{index}/_alias/{name}", "_alias", true, this::putAlias);
        add("PUT,POST", "/{index}/_aliases/{name}", "_alias", true, this::putAlias);
        add("PUT,POST", "/{index}/_alias", "_alias", true, this::putAlias);
        add("DELETE", "/{index}/_alias/{name}", "_alias", true, this::deleteAlias);
        add("DELETE", "/{index}/_aliases/{name}", "_alias", true, this::deleteAlias);
        add("PUT,POST", "/{index}/_doc/{id}", "_doc", true, this::indexDoc);
        add("POST", "/{index}/_doc", "_doc", true, this::indexDoc);
        add("PUT,POST", "/{index}/_create/{id}", "_doc", true, this::indexDoc);
        add("GET,HEAD", "/{index}/_doc/{id}", "_doc", false, this::getDoc);
        add("DELETE", "/{index}/_doc/{id}", "_doc", true, this::deleteDoc);
        add("GET,HEAD", "/{index}/_source/{id}", "_doc", false, this::getSource);
        add("POST", "/{index}/_update/{id}", "_doc", true, this::update);
        add("GET,POST", "/{index}/_explain/{id}", "_explain", false, this::explain);
        add("GET,POST", "/{index}/_termvectors/{id}", "_termvectors", false, r -> unsupported("_termvectors"));
        add("POST", "/{index}/_rollover", "_rollover", true, r -> unsupported("_rollover"));
        add("POST", "/{index}/_rollover/{new_index}", "_rollover", true, r -> unsupported("_rollover"));
        add("PUT,POST,GET,DELETE", "/_data_stream/{name}", "_data_stream", true, r -> unsupported("data streams"));
        add("PUT,POST", "/{index}", "index", true, this::createIndex);
        add("DELETE", "/{index}", "index", true, this::deleteIndex);
        add("GET", "/{index}", "index", false, this::getIndex);
        add("HEAD", "/{index}", "index", false, this::existsIndex);
        // legacy typed paths: /{index}/{type}/{id} -> handled as _doc
    }

    Response unsupported(String what) {
        return Response.json(400, error("illegal_argument_exception", what + " is not supported by Warp's OpenSearch frontend"));
    }

    /** Finds the route; returns null when none (or the allowed methods via {@code allowedOut}). */
    Route match(String method, String[] segs, Map<String, String> varsOut, Set<String> allowedOut) {
        for (Route r : routes) {
            if (r.segs().length != segs.length) {
                continue;
            }
            Map<String, String> vars = new LinkedHashMap<>();
            boolean ok = true;
            for (int i = 0; i < segs.length && ok; i++) {
                String rs = r.segs()[i];
                if (rs.startsWith("{")) {
                    if (segs[i].isEmpty()) {
                        ok = false;
                    } else {
                        vars.put(rs.substring(1, rs.length() - 1), segs[i]);
                    }
                } else if (!rs.equals(segs[i])) {
                    ok = false;
                }
            }
            if (!ok) {
                continue;
            }
            if (r.methods().contains(method) || (method.equals("HEAD") && r.methods().contains("GET"))) {
                varsOut.putAll(vars);
                return r;
            }
            allowedOut.addAll(r.methods());
        }
        return null;
    }

    // ------------------------------------------------------------------ helpers

    static JsonObject error(String type, String reason) {
        return errorJson(new OpenSearchException(type, reason));
    }

    /** The full {"error":..., "status":...} envelope for an exception. */
    static JsonObject errorJson(OpenSearchException e) {
        JsonObject root = new JsonObject();
        JsonObject cause = causeJson(e);
        JsonObject inner;
        if (e.shardLevel) {
            inner = new JsonObject();
            JsonArray rc = new JsonArray();
            rc.add(cause);
            inner.add("root_cause", rc);
            inner.addProperty("type", "search_phase_execution_exception");
            inner.addProperty("reason", "all shards failed");
            inner.addProperty("phase", "query");
            inner.addProperty("grouped", true);
            JsonArray failed = new JsonArray();
            JsonObject f = new JsonObject();
            f.addProperty("shard", 0);
            if (e.extra.has("index")) {
                f.add("index", e.extra.get("index"));
            }
            f.addProperty("node", "warp");
            f.add("reason", causeJson(e));
            failed.add(f);
            inner.add("failed_shards", failed);
        } else {
            inner = causeJson(e);
            JsonArray rc = new JsonArray();
            rc.add(causeJson(e));
            JsonObject ordered = new JsonObject();
            ordered.add("root_cause", rc);
            for (var en : inner.entrySet()) {
                ordered.add(en.getKey(), en.getValue());
            }
            inner = ordered;
        }
        root.add("error", inner);
        root.addProperty("status", e.status);
        return root;
    }

    private static JsonObject causeJson(OpenSearchException e) {
        JsonObject c = new JsonObject();
        c.addProperty("type", e.errorType);
        c.addProperty("reason", e.getMessage());
        for (var en : e.extra.entrySet()) {
            if (!en.getKey().equals("caused")) {
                c.add(en.getKey(), en.getValue());
            }
        }
        return c;
    }

    private static Response obj(String k, JsonElement v) {
        JsonObject o = new JsonObject();
        o.add(k, v);
        return Response.json(200, o);
    }

    static JsonObject ack() {
        JsonObject o = new JsonObject();
        o.addProperty("acknowledged", true);
        return o;
    }

    static JsonObject parseBody(String body, boolean required) {
        if (body == null || body.isBlank()) {
            if (required) {
                throw new OpenSearchException("parse_exception", "request body is required");
            }
            return new JsonObject();
        }
        try {
            JsonElement e = JsonParser.parseString(body);
            if (!e.isJsonObject()) {
                throw new OpenSearchException("parsing_exception", "Unexpected token [" + (e.isJsonArray() ? "START_ARRAY" : "VALUE") + "], expected START_OBJECT");
            }
            return e.getAsJsonObject();
        } catch (com.google.gson.JsonParseException ex) {
            OpenSearchException oe = new OpenSearchException("json_parse_exception", "Failed to parse content to map");
            throw oe;
        }
    }

    SearchEngine engine() {
        return engine;
    }

    PostgresSearchStore.ResolveOpts resolveOpts(Request r) {
        PostgresSearchStore.ResolveOpts o = new PostgresSearchStore.ResolveOpts();
        o.ignoreUnavailable = "true".equals(r.q.get("ignore_unavailable"));
        o.allowNoIndices = !"false".equals(r.q.get("allow_no_indices"));
        String ew = r.q.get("expand_wildcards");
        if (ew != null) {
            o.expandClosed = ew.contains("closed") || ew.contains("all");
            o.expandOpen = !ew.equals("none") && !ew.equals("closed");
        }
        return o;
    }

    List<PostgresSearchStore.Resolved> targets(Request r) throws SQLException {
        String expr = r.var("index");
        return store.resolve(expr, resolveOpts(r));
    }

    JsonObject shards(int total, int ok) {
        JsonObject s = new JsonObject();
        s.addProperty("total", total);
        s.addProperty("successful", ok);
        s.addProperty("failed", 0);
        return s;
    }

    private static int replicas(PostgresSearchStore.IndexMeta m) {
        JsonElement idx = m.settings.get("index");
        JsonElement r = idx != null && idx.isJsonObject() ? idx.getAsJsonObject().get("number_of_replicas") : null;
        if (r == null || !r.isJsonPrimitive()) {
            return 1;
        }
        try {
            return Integer.parseInt(r.getAsString());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    // ------------------------------------------------------------------ info / cluster

    Response info(Request r) {
        if (r.method.equals("HEAD")) {
            return Response.json(200, new JsonObject());
        }
        JsonObject o = new JsonObject();
        o.addProperty("name", NODE_NAME);
        o.addProperty("cluster_name", CLUSTER_NAME);
        o.addProperty("cluster_uuid", CLUSTER_UUID);
        JsonObject v = new JsonObject();
        v.addProperty("distribution", "opensearch");
        v.addProperty("number", VERSION);
        v.addProperty("build_type", "tar");
        v.addProperty("build_hash", "warp");
        v.addProperty("build_date", "2026-07-01T04:24:12.873558153Z");
        v.addProperty("build_snapshot", false);
        v.addProperty("lucene_version", "9.12.3");
        v.addProperty("minimum_wire_compatibility_version", "7.10.0");
        v.addProperty("minimum_index_compatibility_version", "7.0.0");
        o.add("version", v);
        o.addProperty("tagline", "The OpenSearch Project: https://opensearch.org/");
        return Response.json(200, o);
    }

    Response clusterHealth(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, new PostgresSearchStore.ResolveOpts())
                : store.resolve(r.var("index"), resolveOpts(r));
        int primaries = 0;
        for (var t : ts) {
            primaries += SearchEngine.shardsOf(t.index());
        }
        JsonObject o = new JsonObject();
        o.addProperty("cluster_name", CLUSTER_NAME);
        o.addProperty("status", "green");
        o.addProperty("timed_out", false);
        o.addProperty("number_of_nodes", 1);
        o.addProperty("number_of_data_nodes", 1);
        o.addProperty("discovered_master", true);
        o.addProperty("discovered_cluster_manager", true);
        o.addProperty("active_primary_shards", primaries);
        o.addProperty("active_shards", primaries);
        o.addProperty("relocating_shards", 0);
        o.addProperty("initializing_shards", 0);
        o.addProperty("unassigned_shards", 0);
        o.addProperty("delayed_unassigned_shards", 0);
        o.addProperty("number_of_pending_tasks", 0);
        o.addProperty("number_of_in_flight_fetch", 0);
        o.addProperty("task_max_waiting_in_queue_millis", 0);
        o.addProperty("active_shards_percent_as_number", 100.0);
        String level = r.q.get("level");
        if ("indices".equals(level) || "shards".equals(level)) {
            JsonObject idx = new JsonObject();
            for (var t : ts) {
                JsonObject i = new JsonObject();
                i.addProperty("status", "green");
                i.addProperty("number_of_shards", SearchEngine.shardsOf(t.index()));
                i.addProperty("number_of_replicas", replicas(t.index()));
                i.addProperty("active_primary_shards", SearchEngine.shardsOf(t.index()));
                i.addProperty("active_shards", SearchEngine.shardsOf(t.index()));
                i.addProperty("relocating_shards", 0);
                i.addProperty("initializing_shards", 0);
                i.addProperty("unassigned_shards", 0);
                idx.add(t.index().name, i);
            }
            o.add("indices", idx);
        }
        return Response.json(200, o);
    }

    JsonObject clusterSettings(Request r) {
        JsonObject o = new JsonObject();
        o.add("persistent", new JsonObject());
        o.add("transient", new JsonObject());
        if ("true".equals(r.q.get("include_defaults"))) {
            o.add("defaults", new JsonObject());
        }
        return o;
    }

    Response putClusterSettings(Request r) {
        JsonObject body = parseBody(r.body, false);
        JsonObject o = new JsonObject();
        o.addProperty("acknowledged", true);
        o.add("persistent", body.has("persistent") ? body.get("persistent") : new JsonObject());
        o.add("transient", body.has("transient") ? body.get("transient") : new JsonObject());
        return Response.json(200, o);
    }

    Response clusterStats(Request r) throws SQLException {
        JsonObject o = new JsonObject();
        o.addProperty("_nodes_total", 1);
        JsonObject n = new JsonObject();
        n.addProperty("total", 1);
        n.addProperty("successful", 1);
        n.addProperty("failed", 0);
        o.add("_nodes", n);
        o.addProperty("cluster_name", CLUSTER_NAME);
        o.addProperty("cluster_uuid", CLUSTER_UUID);
        o.addProperty("timestamp", System.currentTimeMillis());
        o.addProperty("status", "green");
        JsonObject idx = new JsonObject();
        var all = store.allMetas();
        idx.addProperty("count", all.size());
        JsonObject docs = new JsonObject();
        long total = 0;
        for (var m : all) {
            total += store.count(m);
        }
        docs.addProperty("count", total);
        docs.addProperty("deleted", 0);
        idx.add("docs", docs);
        JsonObject sh = new JsonObject();
        sh.addProperty("total", all.size());
        sh.addProperty("primaries", all.size());
        idx.add("shards", sh);
        o.add("indices", idx);
        JsonObject nodes = new JsonObject();
        JsonObject count = new JsonObject();
        count.addProperty("total", 1);
        count.addProperty("data", 1);
        count.addProperty("cluster_manager", 1);
        nodes.add("count", count);
        JsonObject versions = new JsonObject();
        versions.add("0", new JsonPrimitive(VERSION));
        nodes.add("versions", new JsonArray());
        nodes.getAsJsonArray("versions").add(VERSION);
        o.add("nodes", nodes);
        return Response.json(200, o);
    }

    Response clusterState(Request r) throws SQLException {
        JsonObject o = new JsonObject();
        o.addProperty("cluster_name", CLUSTER_NAME);
        o.addProperty("cluster_uuid", CLUSTER_UUID);
        o.addProperty("version", 1);
        o.addProperty("state_uuid", "warp");
        o.addProperty("cluster_manager_node", "warp-node-1");
        o.addProperty("master_node", "warp-node-1");
        JsonObject metadata = new JsonObject();
        JsonObject indices = new JsonObject();
        for (var m : store.allMetas()) {
            JsonObject i = new JsonObject();
            i.addProperty("state", m.closed ? "close" : "open");
            i.add("settings", m.settings);
            i.add("mappings", m.mappings.raw);
            indices.add(m.name, i);
        }
        metadata.add("indices", indices);
        o.add("metadata", metadata);
        return Response.json(200, o);
    }

    JsonObject nodeInfo() {
        JsonObject n = new JsonObject();
        n.addProperty("name", NODE_NAME);
        n.addProperty("transport_address", "127.0.0.1:9300");
        n.addProperty("host", "127.0.0.1");
        n.addProperty("ip", "127.0.0.1");
        n.addProperty("version", VERSION);
        n.addProperty("build_type", "tar");
        n.addProperty("build_hash", "warp");
        JsonArray roles = new JsonArray();
        roles.add("cluster_manager");
        roles.add("data");
        roles.add("ingest");
        n.add("roles", roles);
        n.add("attributes", new JsonObject());
        JsonObject http = new JsonObject();
        http.addProperty("publish_address", "127.0.0.1:9200");
        n.add("http", http);
        n.add("plugins", new JsonArray());
        n.add("modules", new JsonArray());
        return n;
    }

    Response nodesInfo(Request r) {
        JsonObject o = new JsonObject();
        o.add("_nodes", shards(1, 1));
        o.getAsJsonObject("_nodes").remove("failed");
        o.getAsJsonObject("_nodes").addProperty("failed", 0);
        o.addProperty("cluster_name", CLUSTER_NAME);
        JsonObject nodes = new JsonObject();
        nodes.add("warp-node-1", nodeInfo());
        o.add("nodes", nodes);
        return Response.json(200, o);
    }

    Response nodesStats(Request r) {
        JsonObject o = new JsonObject();
        o.add("_nodes", shards(1, 1));
        o.addProperty("cluster_name", CLUSTER_NAME);
        JsonObject nodes = new JsonObject();
        JsonObject n = new JsonObject();
        n.addProperty("name", NODE_NAME);
        n.addProperty("timestamp", System.currentTimeMillis());
        n.addProperty("host", "127.0.0.1");
        n.addProperty("ip", "127.0.0.1");
        nodes.add("warp-node-1", n);
        o.add("nodes", nodes);
        return Response.json(200, o);
    }

    // ------------------------------------------------------------------ document APIs

    Response indexDoc(Request r) throws SQLException {
        String index = r.var("index");
        String id = r.var("id");
        boolean createPath = r.path.contains("/_create/");
        boolean autoId = id == null;
        if (autoId) {
            id = PostgresSearchStore.autoId();
        }
        String opType = r.q.get("op_type");
        if (opType != null && !opType.equals("index") && !opType.equals("create")) {
            throw OpenSearchException.validation("opType must be either 'create' or 'index'... got [" + opType + "]");
        }
        boolean create = createPath || autoId || "create".equals(opType);
        if (autoId && "index".equals(opType)) {
            throw OpenSearchException.validation("an id must be provided if version type or value are set");
        }
        JsonObject doc;
        try {
            doc = parseBody(r.body, true);
        } catch (OpenSearchException e) {
            if (e.errorType.equals("json_parse_exception") || e.errorType.equals("parsing_exception")) {
                OpenSearchException m = new OpenSearchException("mapper_parsing_exception", "failed to parse");
                JsonObject cb = new JsonObject();
                cb.addProperty("type", "json_parse_exception");
                cb.addProperty("reason", "Unrecognized token in document body");
                m.extra.add("caused_by", cb);
                throw m;
            }
            throw e;
        }
        if (id.isEmpty()) {
            throw OpenSearchException.validation("if _id is specified it must not be empty");
        }
        PostgresSearchStore.WriteOpts opts = writeOpts(r, create);
        checkRequireAlias(index, "true".equals(r.q.get("require_alias")));
        PostgresSearchStore.IndexMeta meta = store.resolveWriteTarget(index, autoCreateAllowed(index));
        PostgresSearchStore.WriteResult wr = store.write(meta, id, doc, opts);
        maybeRefresh(r, meta);
        return Response.json(wr.created() ? 201 : 200, writeResponse(meta, id, wr, wr.created() ? "created" : "updated", r));
    }

    /** refresh=true|wait_for|"" makes the write visible to search when the index gates visibility (refresh_interval -1). */
    void maybeRefresh(Request r, PostgresSearchStore.IndexMeta meta) throws SQLException {
        String rf = r.q.get("refresh");
        if (rf != null && !rf.equals("false") && meta.gated()) {
            store.refresh(meta);
        }
    }

    private boolean autoCreateAllowed(String index) {
        return true;
    }

    void checkRequireAlias(String index, boolean required) throws SQLException {
        if (!required) {
            return;
        }
        for (var m : store.allMetas()) {
            if (m.aliases.has(index)) {
                return;
            }
        }
        throw new OpenSearchException("index_not_found_exception", "no such index [" + index + "] and [require_alias] request flag is [true] and [" + index + "] is not an alias", 404)
                .with("resource.type", "index_or_alias").with("resource.id", index).with("index_uuid", "_na_").with("index", index);
    }

    PostgresSearchStore.WriteOpts writeOpts(Request r, boolean create) {
        return writeOpts(r, create, false);
    }

    PostgresSearchStore.WriteOpts writeOpts(Request r, boolean create, boolean isDeleteContext) {
        PostgresSearchStore.WriteOpts o = new PostgresSearchStore.WriteOpts();
        o.create = create;
        try {
            if (r.q.containsKey("if_seq_no")) {
                o.ifSeqNo = Long.parseLong(r.q.get("if_seq_no"));
            }
            if (r.q.containsKey("if_primary_term")) {
                o.ifPrimaryTerm = Long.parseLong(r.q.get("if_primary_term"));
            }
            if (r.q.containsKey("version")) {
                o.version = Long.parseLong(r.q.get("version"));
            }
        } catch (NumberFormatException e) {
            throw new OpenSearchException("illegal_argument_exception", "Failed to parse long parameter with value [" + e.getMessage() + "]");
        }
        if (r.q.containsKey("version_type")) {
            o.versionType = r.q.get("version_type");
            if (!Set.of("internal", "external", "external_gte", "force").contains(o.versionType)) {
                throw new OpenSearchException("illegal_argument_exception", "No version type match [" + o.versionType + "]");
            }
        }
        if ((o.ifSeqNo != null) != (o.ifPrimaryTerm != null)) {
            throw OpenSearchException.validation(o.ifSeqNo != null ? "ifSeqNo is set, but primary term is [0]" : "ifPrimaryTerm is set, but sequence number is [-2]");
        }
        if (o.ifSeqNo != null && o.version != null) {
            throw OpenSearchException.validation("compare and write operations can not use versioning");
        }
        if (o.version != null && o.versionType.equals("internal") && !isDeleteContext) {
            throw OpenSearchException.validation("internal versioning can not be used for optimistic concurrency control. Please use `if_seq_no` and `if_primary_term` instead");
        }
        if (o.create && o.version != null && o.versionType.equals("internal")) {
            throw OpenSearchException.validation("create operations do not support explicit versions. use index instead");
        }
        if (o.create && !o.versionType.equals("internal")) {
            throw OpenSearchException.validation("create operations only support internal versioning. use index instead");
        }
        if (o.create && o.ifSeqNo != null) {
            throw OpenSearchException.validation("create operations do not support compare and set. use index instead");
        }
        if (o.version != null && o.versionType.equals("force")) {
            throw OpenSearchException.validation("version type [force] may no longer be used");
        }
        return o;
    }

    JsonObject writeResponse(PostgresSearchStore.IndexMeta meta, String id, PostgresSearchStore.WriteResult wr, String result, Request r) {
        JsonObject o = new JsonObject();
        o.addProperty("_index", meta.name);
        o.addProperty("_id", id);
        o.addProperty("_version", wr.version());
        o.addProperty("result", result);
        if ("true".equals(r.q.get("refresh")) || "".equals(r.q.get("refresh"))) {
            o.addProperty("forced_refresh", true);
        }
        o.add("_shards", shards(1 + replicas(meta), 1));
        o.addProperty("_seq_no", wr.seqNo());
        o.addProperty("_primary_term", 1);
        return o;
    }

    Response getDoc(Request r) throws SQLException {
        PostgresSearchStore.IndexMeta meta = requireIndex(r.var("index"));
        PostgresSearchStore.Doc d = store.get(meta, r.var("id"));
        if (d != null && r.q.containsKey("version")) {
            checkGetVersion(d, r);
        }
        if (r.method.equals("HEAD")) {
            return Response.json(d != null ? 200 : 404, new JsonObject());
        }
        JsonObject o = new JsonObject();
        o.addProperty("_index", meta.name);
        o.addProperty("_id", r.var("id"));
        if (d == null) {
            o.addProperty("found", false);
            return Response.json(404, o);
        }
        o.addProperty("_version", d.version);
        o.addProperty("_seq_no", d.seqNo);
        o.addProperty("_primary_term", 1);
        o.addProperty("found", true);
        SearchEngine.Req sr = new SearchEngine.Req();
        SearchEngine.applySource(sr, r.q.containsKey("_source") ? paramSource(r.q.get("_source")) : null, r.q);
        String sf = r.q.get("stored_fields");
        boolean srcExplicit = r.q.containsKey("_source") || sr.includes != null;
        if (!sr.sourceOff && (sf == null || srcExplicit)) {
            o.add("_source", SearchEngine.filterSource(d.source, sr.includes, sr.excludes));
        }
        if (sf != null && !sf.equals("_none_")) {
            JsonObject fields = new JsonObject();
            for (String f : sf.split(",")) {
                Mappings.Field mf = meta.mappings.get(f);
                if (mf != null && mf.def.has("store") && mf.def.get("store").getAsBoolean()) {
                    JsonArray a = new JsonArray();
                    Mappings.values(d.source, f).forEach(a::add);
                    if (a.size() > 0) {
                        fields.add(f, a);
                    }
                }
            }
            if (fields.size() > 0) {
                o.add("fields", fields);
            }
        }
        return Response.json(200, o);
    }

    private static void checkGetVersion(PostgresSearchStore.Doc d, Request r) {
        long v = Long.parseLong(r.q.get("version"));
        String vt = r.q.getOrDefault("version_type", "internal");
        boolean conflict = d.version != v;
        if (conflict) {
            throw new OpenSearchException("version_conflict_engine_exception", "[" + r.var("id") + "]: version conflict, current version ["
                    + d.version + "] is different than the one provided [" + v + "]");
        }
    }

    private static JsonElement paramSource(String v) {
        if (v.equals("true") || v.equals("false")) {
            return new JsonPrimitive(Boolean.parseBoolean(v));
        }
        JsonArray a = new JsonArray();
        for (String p : v.split(",")) {
            a.add(p);
        }
        return a;
    }

    Response getSource(Request r) throws SQLException {
        PostgresSearchStore.IndexMeta meta = requireIndex(r.var("index"));
        PostgresSearchStore.Doc d = store.get(meta, r.var("id"));
        if (d == null) {
            if (r.method.equals("HEAD")) {
                return Response.json(404, new JsonObject());
            }
            OpenSearchException e = new OpenSearchException("resource_not_found_exception",
                    "Document not found [" + meta.name + "]/[" + r.var("id") + "]");
            return Response.json(404, errorJson(e));
        }
        if (r.method.equals("HEAD")) {
            return Response.json(200, new JsonObject());
        }
        SearchEngine.Req sr = new SearchEngine.Req();
        SearchEngine.applySource(sr, r.q.containsKey("_source") ? paramSource(r.q.get("_source")) : null, r.q);
        return Response.json(200, SearchEngine.filterSource(d.source, sr.includes, sr.excludes));
    }

    PostgresSearchStore.IndexMeta requireIndex(String name) throws SQLException {
        List<PostgresSearchStore.Resolved> rs = store.resolve(name, new PostgresSearchStore.ResolveOpts());
        if (rs.isEmpty()) {
            throw OpenSearchException.indexNotFound(name);
        }
        if (rs.size() > 1) {
            throw OpenSearchException.illegalArgument("Alias [" + name + "] has more than one index associated with it ["
                    + String.join(", ", rs.stream().map(x -> x.index().name).toList()) + "], can't execute a single index op");
        }
        return rs.get(0).index();
    }

    Response deleteDoc(Request r) throws SQLException {
        PostgresSearchStore.IndexMeta meta = requireIndex(r.var("index"));
        PostgresSearchStore.WriteOpts opts = writeOpts(r, false, true);
        PostgresSearchStore.WriteResult wr = store.delete(meta, r.var("id"), opts);
        maybeRefresh(r, meta);
        boolean found = wr != null;
        JsonObject o = new JsonObject();
        o.addProperty("_index", meta.name);
        o.addProperty("_id", r.var("id"));
        o.addProperty("_version", found ? wr.version() : 1);
        o.addProperty("result", found ? "deleted" : "not_found");
        if ("true".equals(r.q.get("refresh")) || "".equals(r.q.get("refresh"))) {
            o.addProperty("forced_refresh", true);
        }
        o.add("_shards", shards(1 + replicas(meta), 1));
        o.addProperty("_seq_no", found ? wr.seqNo() : 0);
        o.addProperty("_primary_term", 1);
        return Response.json(found ? 200 : 404, o);
    }

    // ---- update ----

    Response update(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        String index = r.var("index");
        String id = r.var("id");
        for (String k : body.keySet()) {
            if (!Set.of("doc", "upsert", "doc_as_upsert", "script", "scripted_upsert", "detect_noop", "_source", "if_seq_no", "if_primary_term").contains(k)) {
                String hint = "";
                int best = 3;
                for (String cand : new String[] {"doc", "upsert", "doc_as_upsert", "script", "scripted_upsert", "detect_noop", "_source"}) {
                    int dd = Query.editDistance(k, cand, 2, true);
                    if (dd < best) {
                        best = dd;
                        hint = " did you mean [" + cand + "]?";
                    }
                }
                throw new OpenSearchException("x_content_parse_exception", "[UpdateRequest] unknown field [" + k + "]" + hint);
            }
        }
        if (!body.has("doc") && !body.has("script") && !body.has("upsert")) {
            throw OpenSearchException.validation("script or doc is missing");
        }
        if (body.has("doc") && body.has("script")) {
            throw OpenSearchException.validation("can't provide both script and doc");
        }
        checkRequireAlias(index, "true".equals(r.q.get("require_alias")));
        PostgresSearchStore.IndexMeta meta;
        boolean docAsUpsert = body.has("doc_as_upsert") && body.get("doc_as_upsert").getAsBoolean();
        boolean hasUpsert = body.has("upsert") || docAsUpsert;
        try {
            meta = store.resolveWriteTarget(index, hasUpsert);
        } catch (OpenSearchException e) {
            if (e.errorType.equals("index_not_found_exception") && !hasUpsert && !(body.has("scripted_upsert") && body.get("scripted_upsert").getAsBoolean())) {
                throw new OpenSearchException("document_missing_exception", "[" + id + "]: document missing").with("index", index).with("shard", "0")
                        .with("index_uuid", "_na_");
            }
            throw e;
        }
        int retriesInit = r.q.containsKey("retry_on_conflict") ? Integer.parseInt(r.q.get("retry_on_conflict")) : 3;
        int retries = retriesInit;
        for (int attempt = 0; ; attempt++) {
            PostgresSearchStore.Doc cur = store.get(meta, id);
            JsonObject newSource;
            String result = "updated";
            PostgresSearchStore.WriteOpts opts = new PostgresSearchStore.WriteOpts();
            if (cur == null) {
                if (body.has("script") && !(body.has("scripted_upsert") && body.get("scripted_upsert").getAsBoolean()) && !docAsUpsert
                        || (!hasUpsert && !(body.has("scripted_upsert") && body.get("scripted_upsert").getAsBoolean()))) {
                    if (!hasUpsert) {
                        throw new OpenSearchException("document_missing_exception", "[" + id + "]: document missing")
                                .with("index", meta.name).with("shard", "0").with("index_uuid", meta.uuid);
                    }
                }
                if (docAsUpsert) {
                    newSource = body.getAsJsonObject("doc").deepCopy();
                } else if (body.has("scripted_upsert") && body.get("scripted_upsert").getAsBoolean() && body.has("script")) {
                    JsonObject base = body.has("upsert") ? body.getAsJsonObject("upsert").deepCopy() : new JsonObject();
                    JsonObject ctx = new JsonObject();
                    ctx.add("_source", base);
                    new Script(body.getAsJsonObject("script")).run(ctx);
                    newSource = ctx.getAsJsonObject("_source");
                } else if (body.has("upsert")) {
                    newSource = body.getAsJsonObject("upsert").deepCopy();
                } else {
                    throw new OpenSearchException("document_missing_exception", "[" + id + "]: document missing")
                            .with("index", meta.name).with("shard", "0").with("index_uuid", meta.uuid);
                }
                opts.create = true;
                result = "created";
            } else {
                if (r.q.containsKey("if_seq_no") || body.has("if_seq_no")) {
                    long want = r.q.containsKey("if_seq_no") ? Long.parseLong(r.q.get("if_seq_no")) : body.get("if_seq_no").getAsLong();
                    if (want != cur.seqNo) {
                        throw new OpenSearchException("version_conflict_engine_exception", "[" + id + "]: version conflict, required seqNo [" + want
                                + "], primary term [1]. current document has seqNo [" + cur.seqNo + "] and primary term [1]");
                    }
                    retries = 0;
                }
                newSource = cur.source.deepCopy();
                if (body.has("script")) {
                    JsonObject ctx = new JsonObject();
                    ctx.add("_source", newSource);
                    ctx.addProperty("op", "index");
                    new Script(body.getAsJsonObject("script")).run(ctx);
                    String op = ctx.has("op") ? ctx.get("op").getAsString() : "index";
                    newSource = ctx.getAsJsonObject("_source");
                    if (op.equals("delete")) {
                        PostgresSearchStore.WriteResult dr = store.delete(meta, id, null);
                        JsonObject o = writeResponse(meta, id, dr == null ? new PostgresSearchStore.WriteResult(0, 1, false, false) : dr, "deleted", r);
                        return Response.json(200, o);
                    }
                    if (op.equals("none") || op.equals("noop")) {
                        return Response.json(200, noopResponse(meta, id, cur));
                    }
                } else if (body.has("doc")) {
                    boolean detect = !body.has("detect_noop") || body.get("detect_noop").getAsBoolean();
                    JsonObject merged = cur.source.deepCopy();
                    mergeDoc(merged, body.getAsJsonObject("doc"));
                    if (detect && merged.equals(cur.source)) {
                        JsonObject nr = noopResponse(meta, id, cur);
                        addUpdateGet(nr, body, r, cur.source, cur.seqNo);
                        return Response.json(200, nr);
                    }
                    newSource = merged;
                }
                opts.ifSeqNo = cur.seqNo;
                opts.ifPrimaryTerm = 1L;
            }
            try {
                PostgresSearchStore.WriteResult wr = store.write(meta, id, newSource, opts);
                maybeRefresh(r, meta);
                JsonObject resp = writeResponse(meta, id, wr, result, r);
                addUpdateGet(resp, body, r, newSource, wr.seqNo());
                return Response.json(wr.created() ? 201 : 200, resp);
            } catch (OpenSearchException e) {
                if (e.errorType.equals("version_conflict_engine_exception") && attempt < retries) {
                    continue;
                }
                throw e;
            }
        }
    }

    /** The {@code get} block of an update response when {@code _source} was requested (body, URL or bulk metadata). */
    private void addUpdateGet(JsonObject resp, JsonObject body, Request r, JsonObject source, long seqNo) {
        if (!(body.has("_source") || r.q.containsKey("_source") || r.q.containsKey("_source_includes") || r.q.containsKey("_source_excludes"))) {
            return;
        }
        SearchEngine.Req sr = new SearchEngine.Req();
        SearchEngine.applySource(sr, body.has("_source") ? body.get("_source") : r.q.containsKey("_source") ? paramSource(r.q.get("_source")) : null, r.q);
        JsonObject get = new JsonObject();
        get.addProperty("_seq_no", seqNo);
        get.addProperty("_primary_term", 1);
        get.addProperty("found", true);
        if (!sr.sourceOff) {
            get.add("_source", SearchEngine.filterSource(source, sr.includes, sr.excludes));
        }
        resp.add("get", get);
    }

    private JsonObject noopResponse(PostgresSearchStore.IndexMeta meta, String id, PostgresSearchStore.Doc cur) {
        JsonObject o = new JsonObject();
        o.addProperty("_index", meta.name);
        o.addProperty("_id", id);
        o.addProperty("_version", cur.version);
        o.addProperty("result", "noop");
        JsonObject s = new JsonObject();
        s.addProperty("total", 0);
        s.addProperty("successful", 0);
        s.addProperty("failed", 0);
        o.add("_shards", s);
        o.addProperty("_seq_no", cur.seqNo);
        o.addProperty("_primary_term", 1);
        return o;
    }

    static void mergeDoc(JsonObject into, JsonObject from) {
        for (var e : from.entrySet()) {
            JsonElement cur = into.get(e.getKey());
            if (cur != null && cur.isJsonObject() && e.getValue().isJsonObject()) {
                mergeDoc(cur.getAsJsonObject(), e.getValue().getAsJsonObject());
            } else {
                into.add(e.getKey(), e.getValue().deepCopy());
            }
        }
    }

    // ---- bulk ----

    Response bulk(Request r) throws SQLException {
        long start = System.nanoTime();
        String[] rawLines = r.body.split("\n");
        List<String> lines = new ArrayList<>();
        for (String l : rawLines) {
            if (!l.isBlank()) {
                lines.add(l);
            }
        }
        if (lines.isEmpty()) {
            throw new OpenSearchException("parse_exception", "request body is required");
        }
        String defaultIndex = r.var("index");
        JsonArray items = new JsonArray();
        boolean errors = false;
        int i = 0;
        int lineNo = 0;
        List<Object[]> parsed = new ArrayList<>();
        // parse everything first: a malformed line fails the whole request (like OpenSearch)
        while (i < lines.size()) {
            JsonObject action;
            try {
                JsonElement ae = JsonParser.parseString(lines.get(i));
                if (!ae.isJsonObject()) {
                    throw new OpenSearchException("illegal_argument_exception", "Malformed action/metadata line [" + (i + 1)
                            + "], expected START_OBJECT or END_OBJECT but found [" + (ae.isJsonArray() ? "START_ARRAY" : "VALUE_STRING") + "]");
                }
                action = ae.getAsJsonObject();
            } catch (com.google.gson.JsonParseException e) {
                throw new OpenSearchException("json_parse_exception", "Unrecognized token: was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");
            }
            if (action.size() != 1) {
                throw new OpenSearchException("illegal_argument_exception", "Malformed action/metadata line [" + (i + 1)
                        + "], expected FIELD_NAME but found [" + (action.size() == 0 ? "END_OBJECT" : "FIELD_NAME") + "]");
            }
            String type = action.keySet().iterator().next();
            if (!Set.of("index", "create", "update", "delete").contains(type)) {
                throw new OpenSearchException("illegal_argument_exception", "Malformed action/metadata line [" + (i + 1)
                        + "], expected one of [create, delete, index, update] but found [" + type + "]");
            }
            if (!action.get(type).isJsonObject()) {
                throw new OpenSearchException("illegal_argument_exception", "Malformed action/metadata line [" + (i + 1)
                        + "], expected START_OBJECT but found [VALUE_STRING]");
            }
            JsonObject meta = action.getAsJsonObject(type);
            String src = null;
            if (!type.equals("delete")) {
                i++;
                if (i >= lines.size()) {
                    throw new OpenSearchException("illegal_argument_exception", "Validation Failed: 1: no requests added;");
                }
                src = lines.get(i);
            }
            parsed.add(new Object[] {type, meta, src});
            i++;
        }
        for (Object[] p : parsed) {
            String type = (String) p[0];
            JsonObject meta = (JsonObject) p[1];
            String src = (String) p[2];
            String index = meta.has("_index") ? meta.get("_index").getAsString() : defaultIndex;
            String id = meta.has("_id") && !meta.get("_id").isJsonNull() ? meta.get("_id").getAsString() : null;
            JsonObject item = new JsonObject();
            int status;
            PostgresSearchStore.IndexMeta im = null;
            try {
                if (index == null) {
                    throw OpenSearchException.validation("index is missing");
                }
                if (id == null && (type.equals("delete") || type.equals("update"))) {
                    throw OpenSearchException.validation("id is missing");
                }
                if (id != null && id.isEmpty()) {
                    throw OpenSearchException.illegalArgument("if _id is specified it must not be empty");
                }
                String pipeline = meta.has("pipeline") ? meta.get("pipeline").getAsString() : r.q.get("pipeline");
                if (pipeline != null && !pipeline.equals("_none") && !type.equals("delete")) {
                    throw new OpenSearchException("illegal_argument_exception", "pipeline with id [" + pipeline + "] does not exist");
                }
                checkRequireAlias(index, meta.has("require_alias") ? meta.get("require_alias").getAsBoolean() : "true".equals(r.q.get("require_alias")));
                PostgresSearchStore.WriteOpts opts = new PostgresSearchStore.WriteOpts();
                opts.create = type.equals("create") || (type.equals("index") && (id == null || "create".equals(meta.has("op_type") ? meta.get("op_type").getAsString() : null)));
                if (meta.has("if_seq_no")) {
                    opts.ifSeqNo = meta.get("if_seq_no").getAsLong();
                }
                if (meta.has("if_primary_term")) {
                    opts.ifPrimaryTerm = meta.get("if_primary_term").getAsLong();
                }
                if (meta.has("version")) {
                    opts.version = meta.get("version").getAsLong();
                }
                if (meta.has("version_type")) {
                    opts.versionType = meta.get("version_type").getAsString();
                }
                switch (type) {
                    case "index", "create" -> {
                        if (id == null) {
                            id = PostgresSearchStore.autoId();
                        }
                        JsonObject doc = parseDocLine(src);
                        Request fake = new Request();
                        fake.q = r.q;
                        im = store.resolveWriteTarget(index, true);
                        PostgresSearchStore.WriteResult wr = store.write(im, id, doc, opts);
                        item = itemResult(im, id, wr, wr.created() ? "created" : "updated", wr.created() ? 201 : 200, r);
                    }
                    case "delete" -> {
                        im = store.resolveWriteTarget(index, false);
                        PostgresSearchStore.WriteResult wr = store.delete(im, id, opts);
                        boolean found = wr != null;
                        item = itemResult(im, id, found ? wr : new PostgresSearchStore.WriteResult(0, 1, false, false),
                                found ? "deleted" : "not_found", found ? 200 : 404, r);
                    }
                    default -> {
                        JsonObject upd = parseDocLine(src);
                        Request fake = new Request();
                        fake.method = "POST";
                        fake.vars.put("index", index);
                        fake.vars.put("id", id);
                        fake.q = new LinkedHashMap<>(r.q);
                        fake.q.remove("require_alias");
                        if (meta.has("_source")) {
                            JsonElement se = meta.get("_source");
                            upd.add("_source", se);
                        }
                        if (meta.has("retry_on_conflict")) {
                            fake.q.put("retry_on_conflict", meta.get("retry_on_conflict").getAsString());
                        }
                        fake.q.remove("refresh");
                        if (meta.has("if_seq_no")) {
                            fake.q.put("if_seq_no", meta.get("if_seq_no").getAsString());
                        }
                        fake.body = upd.toString();
                        Response ur = update(fake);
                        item = ur.json.getAsJsonObject();
                        status = ur.status;
                        item.addProperty("status", status);
                        if (!item.has("_shards")) {
                            item.add("_shards", shards(1, 1));
                        }
                        if (!item.has("_seq_no")) {
                            // noop responses carry no seq_no
                        }
                    }
                }
            } catch (OpenSearchException e) {
                errors = true;
                item = new JsonObject();
                item.addProperty("_index", index == null ? "" : index);
                item.addProperty("_id", id == null ? "" : id);
                item.addProperty("status", e.status);
                JsonObject err = causeJson(e);
                if (e.errorType.equals("version_conflict_engine_exception") || e.errorType.equals("document_missing_exception")) {
                    if (!err.has("index")) {
                        err.addProperty("index", index);
                    }
                    if (!err.has("index_uuid")) {
                        err.addProperty("index_uuid", im != null ? im.uuid : "_na_");
                    }
                    if (!err.has("shard")) {
                        err.addProperty("shard", "0");
                    }
                }
                item.add("error", err);
            }
            JsonObject wrapper = new JsonObject();
            wrapper.add(type.equals("index") && "create".equals(meta.has("op_type") ? meta.get("op_type").getAsString() : null) ? "create" : type, item);
            items.add(wrapper);
            if (item.has("status") && item.get("status").getAsInt() >= 400 && !item.has("error") && !type.equals("delete")) {
                errors = true;
            }
        }
        String rfb = r.q.get("refresh");
        if (rfb != null && !rfb.equals("false")) {
            for (Object[] p : parsed) {
                JsonObject m = (JsonObject) p[1];
                String ix = m.has("_index") ? m.get("_index").getAsString() : defaultIndex;
                PostgresSearchStore.IndexMeta im2 = ix == null ? null : store.meta(ix);
                if (im2 != null) {
                    maybeRefresh(r, im2);
                }
            }
        }
        JsonObject out = new JsonObject();
        out.addProperty("took", (System.nanoTime() - start) / 1_000_000);
        out.addProperty("errors", errors);
        out.add("items", items);
        return Response.json(200, out);
    }

    private JsonObject parseDocLine(String src) {
        try {
            JsonElement e = JsonParser.parseString(src);
            if (!e.isJsonObject()) {
                throw new OpenSearchException("mapper_parsing_exception", "failed to parse");
            }
            return e.getAsJsonObject();
        } catch (com.google.gson.JsonParseException ex) {
            OpenSearchException m = new OpenSearchException("mapper_parsing_exception", "failed to parse");
            JsonObject cb = new JsonObject();
            cb.addProperty("type", "json_parse_exception");
            cb.addProperty("reason", "Unrecognized token in document body");
            m.extra.add("caused_by", cb);
            throw m;
        }
    }

    private JsonObject itemResult(PostgresSearchStore.IndexMeta im, String id, PostgresSearchStore.WriteResult wr, String result, int status, Request r) {
        JsonObject o = new JsonObject();
        o.addProperty("_index", im.name);
        o.addProperty("_id", id);
        o.addProperty("_version", wr.version());
        o.addProperty("result", result);
        if ("true".equals(r.q.get("refresh")) || "".equals(r.q.get("refresh"))) {
            o.addProperty("forced_refresh", true);
        }
        o.add("_shards", shards(1 + replicas(im), 1));
        o.addProperty("_seq_no", wr.seqNo());
        o.addProperty("_primary_term", 1);
        o.addProperty("status", status);
        return o;
    }

    // ---- mget ----

    Response mget(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        String defaultIndex = r.var("index");
        List<JsonObject> reqs = new ArrayList<>();
        if (body.has("docs")) {
            if (body.has("ids")) {
                throw OpenSearchException.validation("Unknown key for a VALUE_STRING in [ids].");
            }
            for (JsonElement e : body.getAsJsonArray("docs")) {
                reqs.add(e.getAsJsonObject());
            }
        } else if (body.has("ids")) {
            for (JsonElement e : body.getAsJsonArray("ids")) {
                JsonObject o = new JsonObject();
                o.add("_id", e);
                reqs.add(o);
            }
        } else {
            throw OpenSearchException.validation("no documents to get");
        }
        if (reqs.isEmpty()) {
            throw OpenSearchException.validation("no documents to get");
        }
        JsonArray docs = new JsonArray();
        for (JsonObject q : reqs) {
            String index = q.has("_index") ? q.get("_index").getAsString() : defaultIndex;
            if (index == null) {
                throw OpenSearchException.validation("index is missing");
            }
            if (!q.has("_id")) {
                throw OpenSearchException.validation("id is missing");
            }
            String id = q.get("_id").getAsString();
            JsonObject d = new JsonObject();
            try {
                List<PostgresSearchStore.Resolved> rs = store.resolve(index, new PostgresSearchStore.ResolveOpts());
                if (rs.isEmpty()) {
                    throw OpenSearchException.indexNotFound(index);
                }
                if (rs.size() > 1) {
                    throw OpenSearchException.illegalArgument("Alias [" + index + "] has more than one index associated with it ["
                            + String.join(", ", rs.stream().map(x -> x.index().name).toList()) + "], can't execute a single index op");
                }
                PostgresSearchStore.IndexMeta meta = rs.get(0).index();
                PostgresSearchStore.Doc doc = store.get(meta, id);
                d.addProperty("_index", meta.name);
                d.addProperty("_id", id);
                if (doc == null) {
                    d.addProperty("found", false);
                } else {
                    d.addProperty("_version", doc.version);
                    d.addProperty("_seq_no", doc.seqNo);
                    d.addProperty("_primary_term", 1);
                    d.addProperty("found", true);
                    SearchEngine.Req sr = new SearchEngine.Req();
                    JsonElement srcSpec = q.has("_source") ? q.get("_source") : body.has("_source") ? body.get("_source") : r.q.containsKey("_source") ? paramSource(r.q.get("_source")) : null;
                    SearchEngine.applySource(sr, srcSpec, r.q);
                    JsonElement sf = q.has("stored_fields") ? q.get("stored_fields") : null;
                    if (!sr.sourceOff && sf == null) {
                        d.add("_source", SearchEngine.filterSource(doc.source, sr.includes, sr.excludes));
                    }
                }
            } catch (OpenSearchException e) {
                d = new JsonObject();
                d.addProperty("_index", index);
                d.addProperty("_id", id);
                if (e.errorType.equals("index_not_found_exception")) {
                    e.extra.addProperty("resource.type", "index_expression");
                }
                d.add("error", errorJson(e).getAsJsonObject("error"));
            }
            docs.add(d);
        }
        JsonObject out = new JsonObject();
        out.add("docs", docs);
        return Response.json(200, out);
    }

    // ---- by-query / reindex ----

    private JsonObject byQueryResult(long took, long total, long deleted, long updated, long noops, long conflicts) {
        JsonObject o = new JsonObject();
        o.addProperty("took", took);
        o.addProperty("timed_out", false);
        o.addProperty("total", total);
        if (deleted >= 0) {
            o.addProperty("deleted", deleted);
        }
        if (updated >= 0) {
            o.addProperty("updated", updated);
        }
        o.addProperty("batches", total == 0 ? 0 : 1);
        o.addProperty("version_conflicts", conflicts);
        o.addProperty("noops", noops);
        JsonObject retries = new JsonObject();
        retries.addProperty("bulk", 0);
        retries.addProperty("search", 0);
        o.add("retries", retries);
        o.addProperty("throttled_millis", 0);
        o.addProperty("requests_per_second", -1.0);
        o.addProperty("throttled_until_millis", 0);
        o.add("failures", new JsonArray());
        return o;
    }

    private Query.Node bodyQuery(Request r, JsonObject body) {
        if (r.q.containsKey("q") && !body.has("query")) {
            return LuceneSyntax.fromUri(r.q.get("q"), r.q.get("df"), "and".equalsIgnoreCase(r.q.get("default_operator")), false, r.q.get("analyzer"));
        }
        return body.has("query") ? QueryParser.parse(body.get("query")) : new Query.MatchAll();
    }

    Response deleteByQuery(Request r) throws SQLException {
        long t0 = System.nanoTime();
        JsonObject body = parseBody(r.body, false);
        if (!body.has("query") && !r.q.containsKey("q")) {
            throw new OpenSearchException("action_request_validation_exception", "Validation Failed: 1: query is missing;");
        }
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty() && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        List<PostgresSearchStore.Doc> docs = engine.matching(ts, bodyQuery(r, body));
        long max = body.has("max_docs") ? body.get("max_docs").getAsLong() : r.q.containsKey("max_docs") ? Long.parseLong(r.q.get("max_docs")) : Long.MAX_VALUE;
        boolean proceed = "proceed".equals(r.q.get("conflicts"));
        long deleted = 0, conflicts = 0;
        Map<String, PostgresSearchStore.IndexMeta> metas = new LinkedHashMap<>();
        for (var t : ts) {
            metas.put(t.index().name, t.index());
        }
        long total = Math.min(max, docs.size());
        for (PostgresSearchStore.Doc d : docs) {
            if (deleted + conflicts >= max) {
                break;
            }
            PostgresSearchStore.WriteOpts o = new PostgresSearchStore.WriteOpts();
            o.ifSeqNo = d.seqNo;
            o.ifPrimaryTerm = 1L;
            try {
                if (store.delete(metas.get(d.index), d.id, o) != null) {
                    deleted++;
                }
            } catch (OpenSearchException e) {
                if (!e.errorType.equals("version_conflict_engine_exception")) {
                    throw e;
                }
                conflicts++;
                if (!proceed) {
                    JsonObject res = byQueryResult((System.nanoTime() - t0) / 1_000_000, total, deleted, -1, 0, conflicts);
                    JsonArray f = new JsonArray();
                    JsonObject fo = new JsonObject();
                    fo.addProperty("index", d.index);
                    fo.addProperty("id", d.id);
                    fo.addProperty("status", 409);
                    fo.add("cause", causeJson(e));
                    f.add(fo);
                    res.add("failures", f);
                    return Response.json(409, res);
                }
            }
        }
        return Response.json(200, byQueryResult((System.nanoTime() - t0) / 1_000_000, total, deleted, -1, 0, conflicts));
    }

    Response updateByQuery(Request r) throws SQLException {
        long t0 = System.nanoTime();
        JsonObject body = parseBody(r.body, false);
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty() && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        List<PostgresSearchStore.Doc> docs = engine.matching(ts, bodyQuery(r, body));
        long max = body.has("max_docs") ? body.get("max_docs").getAsLong() : r.q.containsKey("max_docs") ? Long.parseLong(r.q.get("max_docs")) : Long.MAX_VALUE;
        boolean proceed = "proceed".equals(r.q.get("conflicts"));
        Script script = body.has("script") ? new Script(body.getAsJsonObject("script")) : null;
        Map<String, PostgresSearchStore.IndexMeta> metas = new LinkedHashMap<>();
        for (var t : ts) {
            metas.put(t.index().name, t.index());
        }
        long updated = 0, noops = 0, conflicts = 0, deleted = 0;
        long total = Math.min(max, docs.size());
        int n = 0;
        for (PostgresSearchStore.Doc d : docs) {
            if (n++ >= max) {
                break;
            }
            JsonObject src = d.source.deepCopy();
            String op = "index";
            if (script != null) {
                JsonObject ctx = new JsonObject();
                ctx.add("_source", src);
                ctx.addProperty("op", "index");
                script.run(ctx);
                op = ctx.has("op") ? ctx.get("op").getAsString() : "index";
                src = ctx.getAsJsonObject("_source");
            }
            try {
                if (op.equals("noop") || op.equals("none")) {
                    noops++;
                } else if (op.equals("delete")) {
                    PostgresSearchStore.WriteOpts o = new PostgresSearchStore.WriteOpts();
                    o.ifSeqNo = d.seqNo;
                    o.ifPrimaryTerm = 1L;
                    if (store.delete(metas.get(d.index), d.id, o) != null) {
                        deleted++;
                    }
                } else {
                    PostgresSearchStore.WriteOpts o = new PostgresSearchStore.WriteOpts();
                    o.ifSeqNo = d.seqNo;
                    o.ifPrimaryTerm = 1L;
                    store.write(metas.get(d.index), d.id, src, o);
                    updated++;
                }
            } catch (OpenSearchException e) {
                if (!e.errorType.equals("version_conflict_engine_exception")) {
                    throw e;
                }
                conflicts++;
                if (!proceed) {
                    JsonObject res = byQueryResult((System.nanoTime() - t0) / 1_000_000, total, -1, updated, noops, conflicts);
                    return Response.json(409, res);
                }
            }
        }
        JsonObject res = byQueryResult((System.nanoTime() - t0) / 1_000_000, total, deleted, updated, noops, conflicts);
        return Response.json(200, res);
    }

    Response reindex(Request r) throws SQLException {
        long t0 = System.nanoTime();
        JsonObject body = parseBody(r.body, true);
        if (!body.has("source") || !body.has("dest")) {
            throw OpenSearchException.validation("source and dest are required");
        }
        JsonObject srcSpec = body.getAsJsonObject("source");
        JsonObject dest = body.getAsJsonObject("dest");
        String srcIndex = srcSpec.get("index").isJsonArray() ? String.join(",", srcSpec.getAsJsonArray("index").asList().stream().map(JsonElement::getAsString).toList())
                : srcSpec.get("index").getAsString();
        List<PostgresSearchStore.Resolved> ts = store.resolve(srcIndex, new PostgresSearchStore.ResolveOpts());
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(srcIndex);
        }
        Query.Node q = srcSpec.has("query") ? QueryParser.parse(srcSpec.get("query")) : new Query.MatchAll();
        List<PostgresSearchStore.Doc> docs = engine.matching(ts, q);
        Script script = body.has("script") ? new Script(body.getAsJsonObject("script")) : null;
        String destIndex = dest.get("index").getAsString();
        boolean create = dest.has("op_type") && dest.get("op_type").getAsString().equals("create");
        long created = 0, updated = 0, conflicts = 0, noops = 0;
        long max = body.has("max_docs") ? body.get("max_docs").getAsLong() : (body.has("size") ? body.get("size").getAsLong() : Long.MAX_VALUE);
        PostgresSearchStore.IndexMeta dm = store.resolveWriteTarget(destIndex, true);
        long total = Math.min(max, docs.size());
        int n = 0;
        for (PostgresSearchStore.Doc d : docs) {
            if (n++ >= max) {
                break;
            }
            JsonObject s = d.source.deepCopy();
            String id = d.id;
            if (script != null) {
                JsonObject ctx = new JsonObject();
                ctx.add("_source", s);
                ctx.addProperty("op", "index");
                ctx.addProperty("_id", id);
                script.run(ctx);
                String op = ctx.has("op") ? ctx.get("op").getAsString() : "index";
                if (op.equals("noop") || op.equals("none")) {
                    noops++;
                    continue;
                }
                s = ctx.getAsJsonObject("_source");
                if (ctx.has("_id")) {
                    id = ctx.get("_id").getAsString();
                }
            }
            PostgresSearchStore.WriteOpts o = new PostgresSearchStore.WriteOpts();
            o.create = create;
            try {
                PostgresSearchStore.WriteResult wr = store.write(dm, id, s, o);
                if (wr.created()) {
                    created++;
                } else {
                    updated++;
                }
            } catch (OpenSearchException e) {
                if (!e.errorType.equals("version_conflict_engine_exception")) {
                    throw e;
                }
                conflicts++;
                if (!"proceed".equals(body.has("conflicts") ? body.get("conflicts").getAsString() : r.q.get("conflicts"))) {
                    return Response.json(409, byQueryResult((System.nanoTime() - t0) / 1_000_000, total, -1, updated, noops, conflicts));
                }
            }
        }
        JsonObject res = byQueryResult((System.nanoTime() - t0) / 1_000_000, total, 0, updated, noops, conflicts);
        res.addProperty("created", created);
        return Response.json(200, res);
    }

    // ------------------------------------------------------------------ search APIs

    Response search(Request r) throws SQLException {
        long start = System.nanoTime();
        JsonObject body = searchBody(r);
        SearchEngine.Req req = SearchEngine.parse(body, r.q);
        List<PostgresSearchStore.Resolved> ts;
        if (req.pit != null) {
            SearchEngine.Pit pit = engine.pit(req.pit.get("id").getAsString());
            ts = store.resolve(String.join(",", pit.indices), new PostgresSearchStore.ResolveOpts());
            if (req.pit.has("keep_alive")) {
                pit.expiresAt = System.currentTimeMillis() + SearchEngine.parseKeepAlive(req.pit.get("keep_alive").getAsString());
            }
        } else {
            ts = targets(r);
        }
        SearchEngine.Result res = engine.execute(ts, req);
        JsonObject out = engine.renderSearch(res, req, ts, start);
        if (req.pit != null) {
            out.addProperty("pit_id", req.pit.get("id").getAsString());
        }
        if (req.scroll != null) {
            engine.startScroll(out, res, req, ts);
            JsonArray hits = out.getAsJsonObject("hits").getAsJsonArray("hits");
            // startScroll stored everything; the first page is what renderSearch produced
        }
        return Response.json(200, out);
    }

    private JsonObject searchBody(Request r) {
        String b = r.body;
        if ((b == null || b.isBlank()) && r.q.containsKey("source")) {
            b = r.q.get("source");
        }
        return parseBody(b, false);
    }

    Response msearch(Request r) throws SQLException {
        long start = System.nanoTime();
        List<String> lines = new ArrayList<>();
        for (String l : r.body.split("\n")) {
            if (!l.isBlank()) {
                lines.add(l);
            }
        }
        if (lines.isEmpty()) {
            throw OpenSearchException.validation("no requests added");
        }
        JsonArray responses = new JsonArray();
        for (int i = 0; i < lines.size(); i += 2) {
            JsonObject header;
            JsonObject body;
            try {
                header = JsonParser.parseString(lines.get(i)).getAsJsonObject();
                body = i + 1 < lines.size() ? JsonParser.parseString(lines.get(i + 1)).getAsJsonObject() : new JsonObject();
            } catch (RuntimeException e) {
                throw new OpenSearchException("parsing_exception", "Malformed msearch request");
            }
            try {
                Request sub = new Request();
                sub.method = "POST";
                sub.q = new LinkedHashMap<>();
                for (String k : new String[] {"typed_keys", "rest_total_hits_as_int", "search_type", "ignore_unavailable", "allow_no_indices", "expand_wildcards"}) {
                    if (r.q.containsKey(k)) {
                        sub.q.put(k, r.q.get(k));
                    }
                }
                for (var e : header.entrySet()) {
                    if (!e.getKey().equals("index") && e.getValue().isJsonPrimitive()) {
                        sub.q.put(e.getKey(), e.getValue().getAsString());
                    }
                }
                String index = r.var("index");
                if (header.has("index")) {
                    index = header.get("index").isJsonArray()
                            ? String.join(",", header.getAsJsonArray("index").asList().stream().map(JsonElement::getAsString).toList())
                            : header.get("index").getAsString();
                }
                if (index != null) {
                    sub.vars.put("index", index);
                }
                sub.body = body.toString();
                Response resp = search(sub);
                JsonObject out = resp.json.getAsJsonObject();
                out.addProperty("status", 200);
                responses.add(out);
            } catch (OpenSearchException e) {
                JsonObject err = errorJson(e);
                responses.add(err);
            }
        }
        JsonObject out = new JsonObject();
        out.addProperty("took", (System.nanoTime() - start) / 1_000_000);
        out.add("responses", responses);
        return Response.json(200, out);
    }

    Response count(Request r) throws SQLException {
        JsonObject body = searchBody(r);
        for (String k : body.keySet()) {
            if (!k.equals("query")) {
                throw new OpenSearchException("parsing_exception", "request does not support [" + k + "]");
            }
        }
        List<PostgresSearchStore.Resolved> ts = targets(r);
        Query.Node q = bodyQuery(r, body);
        long n = engine.count(ts, q, r.q.containsKey("min_score") ? Double.parseDouble(r.q.get("min_score")) : null,
                r.q.containsKey("terminate_after") ? Integer.parseInt(r.q.get("terminate_after")) : 0);
        JsonObject o = new JsonObject();
        o.addProperty("count", n);
        o.add("_shards", shards(Math.max(1, ts.stream().mapToInt(t -> SearchEngine.shardsOf(t.index())).sum()), Math.max(1, ts.stream().mapToInt(t -> SearchEngine.shardsOf(t.index())).sum())));
        o.getAsJsonObject("_shards").addProperty("skipped", 0);
        return Response.json(200, o);
    }

    Response scroll(Request r) {
        JsonObject body = parseBody(r.body, false);
        String id = body.has("scroll_id") ? body.get("scroll_id").getAsString() : r.var("scroll_id");
        if (id == null) {
            id = r.q.get("scroll_id");
        }
        if (id == null) {
            throw OpenSearchException.validation("scroll id is missing");
        }
        String keep = body.has("scroll") ? body.get("scroll").getAsString() : r.q.get("scroll");
        return Response.json(200, engine.continueScroll(id, keep, "true".equals(r.q.get("rest_total_hits_as_int"))));
    }

    Response clearScroll(Request r) {
        JsonObject body = parseBody(r.body, false);
        List<String> ids = new ArrayList<>();
        if (body.has("scroll_id")) {
            if (body.get("scroll_id").isJsonArray()) {
                body.getAsJsonArray("scroll_id").forEach(e -> ids.add(e.getAsString()));
            } else {
                ids.add(body.get("scroll_id").getAsString());
            }
        } else if (r.var("scroll_id") != null) {
            ids.addAll(List.of(r.var("scroll_id").split(",")));
        }
        int n = engine.clearScrolls(ids.isEmpty() ? null : ids);
        JsonObject o = new JsonObject();
        o.addProperty("succeeded", true);
        o.addProperty("num_freed", n);
        return Response.json(n == 0 && !ids.isEmpty() && !ids.contains("_all") ? 404 : 200, o);
    }

    Response createPit(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        if (!r.q.containsKey("keep_alive")) {
            throw new OpenSearchException("illegal_argument_exception", "Missing keep_alive parameter");
        }
        String id = engine.createPit(ts.stream().map(t -> t.index().name).toList(), r.q.get("keep_alive"));
        JsonObject o = new JsonObject();
        o.addProperty("pit_id", id);
        o.add("_shards", shards(ts.size(), ts.size()));
        o.addProperty("creation_time", System.currentTimeMillis());
        return Response.json(200, o);
    }

    Response deletePit(Request r) {
        JsonObject body = parseBody(r.body, true);
        JsonArray ids = body.getAsJsonArray("pit_id");
        JsonArray pits = new JsonArray();
        for (JsonElement e : ids) {
            boolean ok = engine.deletePit(e.getAsString());
            JsonObject p = new JsonObject();
            p.addProperty("successful", ok);
            p.addProperty("pit_id", e.getAsString());
            pits.add(p);
        }
        return obj("pits", pits);
    }

    Response deleteAllPits(Request r) {
        JsonArray pits = new JsonArray();
        for (String id : engine.pitIds()) {
            engine.deletePit(id);
            JsonObject p = new JsonObject();
            p.addProperty("successful", true);
            p.addProperty("pit_id", id);
            pits.add(p);
        }
        return obj("pits", pits);
    }

    Response pitAll(Request r) {
        JsonArray pits = new JsonArray();
        for (var e : engine.pits().entrySet()) {
            JsonObject p = new JsonObject();
            p.addProperty("pit_id", e.getKey());
            p.addProperty("creation_time", e.getValue().created);
            p.addProperty("keep_alive", e.getValue().expiresAt - System.currentTimeMillis());
            pits.add(p);
        }
        return obj("pits", pits);
    }

    Response validateQuery(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, false);
        targets(r);
        boolean valid = true;
        JsonObject o = new JsonObject();
        JsonArray explanations = new JsonArray();
        try {
            bodyQuery(r, body);
        } catch (OpenSearchException e) {
            valid = false;
            JsonObject ex = new JsonObject();
            ex.addProperty("valid", false);
            ex.addProperty("error", e.getMessage());
            explanations.add(ex);
        }
        o.add("_shards", shards(1, 1));
        o.addProperty("valid", valid);
        if ("true".equals(r.q.get("explain"))) {
            o.add("explanations", explanations);
        }
        return Response.json(200, o);
    }

    Response explain(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, false);
        PostgresSearchStore.IndexMeta meta = requireIndex(r.var("index"));
        PostgresSearchStore.Doc d = store.get(meta, r.var("id"));
        JsonObject o = new JsonObject();
        o.addProperty("_index", meta.name);
        o.addProperty("_id", r.var("id"));
        if (d == null) {
            o.addProperty("matched", false);
            return Response.json(404, o);
        }
        if (!body.has("query") && !r.q.containsKey("q")) {
            throw OpenSearchException.validation("query is missing");
        }
        Query.Node q = bodyQuery(r, body);
        var ix = new IndexCtx(meta.name, meta.mappings, meta.settings, System.currentTimeMillis(), store.scan(meta, null));
        q.prepare(new Vector.Prep(List.of(ix)));
        double s = q.score(new Query.DocCtx(ix, d));
        boolean matched = Query.matched(s);
        o.addProperty("matched", matched);
        JsonObject ex = new JsonObject();
        ex.addProperty("value", matched ? (float) s : 0.0f);
        ex.addProperty("description", matched ? "sum of:" : "no matching term");
        ex.add("details", new JsonArray());
        o.add("explanation", ex);
        if (r.q.containsKey("_source") || r.q.containsKey("_source_includes") || r.q.containsKey("_source_excludes")) {
            addUpdateGet(o, new JsonObject(), r, d.source, d.seqNo);
            o.getAsJsonObject("get").remove("_seq_no");
            o.getAsJsonObject("get").remove("_primary_term");
        }
        return Response.json(200, o);
    }

    private static JsonArray allIndexNames(List<PostgresSearchStore.Resolved> ts) {
        JsonArray a = new JsonArray();
        ts.forEach(t -> a.add(t.index().name));
        return a;
    }

    private static final String[][] META_FIELDS = {
        {"_data_stream_timestamp", "_data_stream_timestamp", "false", "false"}, {"_doc_count", "long", "false", "false"},
        {"_feature", "_feature", "false", "false"}, {"_field_names", "_field_names", "true", "false"}, {"_id", "_id", "true", "true"},
        {"_ignored", "_ignored", "true", "false"}, {"_index", "_index", "true", "true"}, {"_nested_path", "_nested_path", "true", "false"},
        {"_routing", "_routing", "true", "false"}, {"_seq_no", "_seq_no", "true", "true"}, {"_source", "_source", "false", "false"},
        {"_version", "_version", "false", "false"}};

    Response fieldCaps(Request r) throws SQLException {
        String fieldsParam = r.q.get("fields");
        JsonObject body = parseBody(r.body, false);
        if (fieldsParam == null && body.has("fields")) {
            fieldsParam = body.get("fields").isJsonArray() ? String.join(",", body.getAsJsonArray("fields").asList().stream().map(JsonElement::getAsString).toList())
                    : body.get("fields").getAsString();
        }
        if (fieldsParam == null) {
            throw OpenSearchException.illegalArgument("specify at least one field");
        }
        List<PostgresSearchStore.Resolved> ts = targets(r);
        TreeMap<String, TreeMap<String, JsonObject>> fields = new TreeMap<>();
        JsonArray indices = new JsonArray();
        for (var t : ts) {
            indices.add(t.index().name);
            for (Mappings.Field f : t.index().mappings.fields.values()) {
                boolean m = false;
                for (String pat : fieldsParam.split(",")) {
                    m |= Mappings.wildcardMatch(pat, f.path);
                }
                if (!m) {
                    continue;
                }
                if (f.type.equals("object") && !f.def.has("properties")) {
                    continue;
                }
                JsonObject caps = new JsonObject();
                caps.addProperty("type", f.type);
                caps.addProperty("searchable", f.indexed && !f.type.equals("object") && !f.type.equals("nested"));
                caps.addProperty("aggregatable", !f.isText() && !f.type.equals("object") && !f.type.equals("nested"));
                if (f.def != null && f.def.has("meta") && f.def.get("meta").isJsonObject()) {
                    JsonObject meta = new JsonObject();
                    for (var me : f.def.getAsJsonObject("meta").entrySet()) {
                        JsonArray vals = new JsonArray();
                        vals.add(me.getValue());
                        meta.add(me.getKey(), vals);
                    }
                    caps.add("meta", meta);
                }
                JsonObject existing = fields.computeIfAbsent(f.path, k -> new TreeMap<>()).get(f.type);
                if (existing == null) {
                    fields.get(f.path).put(f.type, caps);
                    existing = caps;
                    existing.add("_idx", new JsonArray());
                    existing.add("_nonsearch", new JsonArray());
                    existing.add("_nonagg", new JsonArray());
                } else if (caps.has("meta")) {
                    JsonObject em = existing.has("meta") ? existing.getAsJsonObject("meta") : new JsonObject();
                    for (var me : caps.getAsJsonObject("meta").entrySet()) {
                        JsonArray have = em.has(me.getKey()) ? em.getAsJsonArray(me.getKey()) : new JsonArray();
                        for (JsonElement v : me.getValue().getAsJsonArray()) {
                            if (!have.contains(v)) {
                                have.add(v);
                            }
                        }
                        em.add(me.getKey(), have);
                    }
                    existing.add("meta", em);
                }
                existing.getAsJsonArray("_idx").add(t.index().name);
                if (!caps.get("searchable").getAsBoolean()) {
                    existing.getAsJsonArray("_nonsearch").add(t.index().name);
                }
                if (!caps.get("aggregatable").getAsBoolean()) {
                    existing.getAsJsonArray("_nonagg").add(t.index().name);
                }
            }
        }
        for (String[] mf : META_FIELDS) {
            boolean wanted = false;
            for (String pat : fieldsParam.split(",")) {
                wanted |= Mappings.wildcardMatch(pat, mf[0]);
            }
            if (wanted && !fields.containsKey(mf[0])) {
                JsonObject caps = new JsonObject();
                caps.addProperty("type", mf[1]);
                caps.addProperty("searchable", Boolean.parseBoolean(mf[2]));
                caps.addProperty("aggregatable", Boolean.parseBoolean(mf[3]));
                fields.computeIfAbsent(mf[0], k -> new TreeMap<>()).put(mf[1], caps);
            }
        }
        boolean includeUnmapped = "true".equals(r.q.get("include_unmapped"));
        // per (field, type): which indices carry it (only when it is not the same everywhere), and where it is not
        // searchable / aggregatable
        for (var fe : fields.entrySet()) {
            boolean multiType = fe.getValue().size() > 1;
            for (var te : fe.getValue().entrySet()) {
                JsonObject c = te.getValue();
                JsonArray idx = c.has("_idx") ? c.getAsJsonArray("_idx") : allIndexNames(ts);
                JsonArray nonS = c.has("_nonsearch") ? c.getAsJsonArray("_nonsearch") : new JsonArray();
                JsonArray nonA = c.has("_nonagg") ? c.getAsJsonArray("_nonagg") : new JsonArray();
                c.remove("_idx");
                c.remove("_nonsearch");
                c.remove("_nonagg");
                if (multiType) {
                    c.add("indices", idx);
                }
                if (nonS.size() > 0 && nonS.size() < idx.size()) {
                    c.addProperty("searchable", false);
                    c.add("non_searchable_indices", nonS);
                }
                if (nonA.size() > 0 && nonA.size() < idx.size()) {
                    c.addProperty("aggregatable", false);
                    c.add("non_aggregatable_indices", nonA);
                }
                if (includeUnmapped && idx.size() < ts.size() && !c.has("indices")) {
                    // reported below through the "unmapped" pseudo type
                }
            }
            if (includeUnmapped) {
                JsonArray mappedIn = new JsonArray();
                for (var t : ts) {
                    if (t.index().mappings.fields.containsKey(fe.getKey())) {
                        mappedIn.add(t.index().name);
                    }
                }
                if (mappedIn.size() < ts.size()) {
                    JsonObject un = new JsonObject();
                    un.addProperty("type", "unmapped");
                    un.addProperty("searchable", false);
                    un.addProperty("aggregatable", false);
                    JsonArray unIdx = new JsonArray();
                    for (var t : ts) {
                        if (!t.index().mappings.fields.containsKey(fe.getKey())) {
                            unIdx.add(t.index().name);
                        }
                    }
                    un.add("indices", unIdx);
                    fe.getValue().put("unmapped", un);
                    for (var te : fe.getValue().entrySet()) {
                        if (!te.getKey().equals("unmapped") && !te.getValue().has("indices")) {
                            te.getValue().add("indices", mappedIn);
                        }
                    }
                }
            }
        }
        JsonObject fo = new JsonObject();
        for (var e : fields.entrySet()) {
            JsonObject types = new JsonObject();
            for (var te : e.getValue().entrySet()) {
                types.add(te.getKey(), te.getValue());
            }
            fo.add(e.getKey(), types);
        }
        JsonObject o = new JsonObject();
        o.add("indices", indices);
        o.add("fields", fo);
        return Response.json(200, o);
    }

    Response analyze(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, false);
        for (String k : r.q.keySet()) {
            if (k.equals("text") || k.equals("analyzer")) {
                body.addProperty(k, r.q.get(k));
            }
        }
        if (!body.has("text")) {
            throw OpenSearchException.illegalArgument("text is missing");
        }
        JsonObject settings = new JsonObject();
        Mappings mappings = Mappings.empty();
        if (r.var("index") != null) {
            PostgresSearchStore.IndexMeta m = requireIndex(r.var("index"));
            settings = m.settings;
            mappings = m.mappings;
        }
        List<String> texts = new ArrayList<>();
        if (body.get("text").isJsonArray()) {
            body.getAsJsonArray("text").forEach(e -> texts.add(e.getAsString()));
        } else {
            texts.add(body.get("text").getAsString());
        }
        Analysis.Analyzer an;
        boolean stdTok = true;
        if (body.has("analyzer")) {
            String an0 = body.get("analyzer").getAsString();
            stdTok = an0.equals("standard") || an0.equals("default");
            if (!stdTok) {
                JsonObject custom = Analysis.analysisSettings(settings);
                if (custom != null && custom.has("analyzer") && custom.getAsJsonObject("analyzer").has(an0)) {
                    JsonElement tk0 = custom.getAsJsonObject("analyzer").getAsJsonObject(an0).get("tokenizer");
                    stdTok = tk0 == null || tk0.getAsString().equals("standard");
                }
            }
        } else if (body.has("tokenizer")) {
            stdTok = body.get("tokenizer").isJsonPrimitive() && body.get("tokenizer").getAsString().equals("standard");
        } else if (body.has("field")) {
            Mappings.Field f0 = mappings.get(body.get("field").getAsString());
            stdTok = f0 != null && f0.isText();
        }
        if (body.has("field")) {
            Mappings.Field f = mappings.get(body.get("field").getAsString());
            IndexCtx ix = new IndexCtx("", mappings, settings, 0, List.of());
            an = f != null && f.isString() ? ix.indexAnalyzer(f) : Analysis.STANDARD;
        } else if (body.has("analyzer")) {
            an = Analysis.resolve(body.get("analyzer").getAsString(), settings);
        } else if (body.has("tokenizer") || body.has("filter") || body.has("char_filter")) {
            JsonObject inline = new JsonObject();
            JsonObject settings2 = new JsonObject();
            JsonObject analysis = new JsonObject();
            JsonObject analyzers = new JsonObject();
            JsonObject def = new JsonObject();
            def.addProperty("type", "custom");
            def.add("tokenizer", body.has("tokenizer") ? body.get("tokenizer") : new JsonPrimitive("keyword"));
            if (body.has("filter")) {
                JsonArray names = new JsonArray();
                JsonObject filters = new JsonObject();
                int i = 0;
                for (JsonElement f : body.getAsJsonArray("filter")) {
                    if (f.isJsonObject()) {
                        String n = "_inline_filter_" + i++;
                        filters.add(n, f);
                        names.add(n);
                    } else {
                        names.add(f);
                    }
                }
                def.add("filter", names);
                analysis.add("filter", filters);
            }
            if (body.has("char_filter")) {
                JsonArray names = new JsonArray();
                JsonObject cfs = new JsonObject();
                int i = 0;
                for (JsonElement f : body.getAsJsonArray("char_filter")) {
                    if (f.isJsonObject()) {
                        String n = "_inline_cf_" + i++;
                        cfs.add(n, f);
                        names.add(n);
                    } else {
                        names.add(f);
                    }
                }
                def.add("char_filter", names);
                analysis.add("char_filter", cfs);
            }
            if (body.has("tokenizer") && body.get("tokenizer").isJsonObject()) {
                JsonObject tk = new JsonObject();
                tk.add("_inline_tokenizer", body.get("tokenizer"));
                analysis.add("tokenizer", tk);
                def.addProperty("tokenizer", "_inline_tokenizer");
            }
            analyzers.add("_inline", def);
            analysis.add("analyzer", analyzers);
            settings2.add("analysis", analysis);
            an = Analysis.resolve("_inline", settings2);
        } else {
            an = Analysis.resolve("default", settings);
        }
        JsonArray tokens = new JsonArray();
        int posBase = 0;
        int maxTokens = 10_000;
        try {
            var idxSettings = settings.has("index") ? settings.getAsJsonObject("index") : settings;
            if (idxSettings.has("analyze") && idxSettings.getAsJsonObject("analyze").has("max_token_count")) {
                maxTokens = Integer.parseInt(idxSettings.getAsJsonObject("analyze").get("max_token_count").getAsString());
            }
        } catch (RuntimeException ignored) {
            // keep the default
        }
        for (String t : texts) {
            int maxPos = -1;
            int type = 0;
            for (Analysis.Token tk : an.analyze(t)) {
                if (tokens.size() >= maxTokens) {
                    throw OpenSearchException.illegalArgument("The number of tokens produced by calling _analyze has exceeded the allowed maximum of ["
                            + maxTokens + "]. This limit can be set by changing the [index.analyze.max_token_count] index level setting.");
                }
                JsonObject o = new JsonObject();
                o.addProperty("token", tk.term());
                o.addProperty("start_offset", tk.start());
                o.addProperty("end_offset", tk.end());
                o.addProperty("type", Analysis.tokenType(stdTok, tk.term()));
                o.addProperty("position", posBase + tk.pos());
                tokens.add(o);
                maxPos = Math.max(maxPos, tk.pos());
            }
            posBase += maxPos + 1 + 100;
        }
        JsonObject out = new JsonObject();
        if (body.has("explain") && body.get("explain").getAsBoolean()) {
            JsonObject detail = new JsonObject();
            detail.addProperty("custom_analyzer", body.has("tokenizer") || body.has("filter") || body.has("char_filter"));
            JsonArray detailed = new JsonArray();
            for (JsonElement te : tokens) {
                JsonObject t = te.getAsJsonObject().deepCopy();
                t.addProperty("bytes", "[" + String.join(" ", t.get("token").getAsString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length == 0 ? new String[0]
                        : hexBytes(t.get("token").getAsString())) + "]");
                t.addProperty("positionLength", 1);
                t.addProperty("termFrequency", 1);
                detailed.add(t);
            }
            if (body.has("tokenizer") || body.has("filter")) {
                JsonObject tk = new JsonObject();
                tk.addProperty("name", body.has("tokenizer") && body.get("tokenizer").isJsonPrimitive() ? body.get("tokenizer").getAsString() : "_anonymous_tokenizer");
                tk.add("tokens", detailed);
                detail.add("tokenizer", tk);
            } else {
                JsonObject an2 = new JsonObject();
                an2.addProperty("name", body.has("analyzer") ? body.get("analyzer").getAsString() : "standard");
                an2.add("tokens", detailed);
                detail.add("analyzer", an2);
            }
            out.add("detail", detail);
        } else {
            out.add("tokens", tokens);
        }
        return Response.json(200, out);
    }

    private static String[] hexBytes(String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String[] out = new String[b.length];
        for (int i = 0; i < b.length; i++) {
            out[i] = String.format("%x", b[i] & 0xff);
        }
        return out;
    }

    // ------------------------------------------------------------------ index management

    Response createIndex(Request r) throws SQLException {
        String name = r.var("index");
        JsonObject body = parseBody(r.body, false);
        for (String k : body.keySet()) {
            if (!Set.of("settings", "mappings", "aliases").contains(k)) {
                throw new OpenSearchException("parse_exception", "unknown key [" + k + "] for create index");
            }
        }
        PostgresSearchStore.validateIndexName(name);
        Templates.Applied t = Templates.forNewIndex(store, name);
        JsonObject settings = t.settings;
        if (body.has("settings")) {
            Templates.deepMerge(settings, PostgresSearchStore.normalizeSettings(body.getAsJsonObject("settings")));
        }
        JsonObject mappings = t.mappings;
        if (body.has("mappings")) {
            JsonObject m = body.getAsJsonObject("mappings");
            if (m.has("_doc") && m.size() == 1 && m.get("_doc").isJsonObject()) {
                m = m.getAsJsonObject("_doc");
            }
            Templates.deepMerge(mappings, m);
        }
        mappings = Mappings.expandDots(mappings);
        normalizeDynamic(mappings);
        validateMappings(mappings);
        JsonObject aliases = t.aliases;
        if (body.has("aliases")) {
            for (var e : body.getAsJsonObject("aliases").entrySet()) {
                aliases.add(e.getKey(), e.getValue());
            }
        }
        int shardsN = 1;
        try {
            var idx = settings.has("index") ? settings.getAsJsonObject("index") : new JsonObject();
            if (idx.has("number_of_shards")) {
                shardsN = Integer.parseInt(idx.get("number_of_shards").getAsString());
                if (shardsN < 1) {
                    throw new OpenSearchException("illegal_argument_exception", "Failed to parse value [" + shardsN + "] for setting [index.number_of_shards] must be >= 1");
                }
            }
        } catch (NumberFormatException e) {
            throw new OpenSearchException("illegal_argument_exception", "Failed to parse value [" + e.getMessage() + "] for setting [index.number_of_shards]");
        }
        PostgresSearchStore.IndexMeta m = store.createIndex(name, settings, mappings, aliases);
        JsonObject o = new JsonObject();
        o.addProperty("acknowledged", true);
        o.addProperty("shards_acknowledged", true);
        o.addProperty("index", m.name);
        return Response.json(200, o);
    }

    /** OpenSearch reports {@code "dynamic": "false"} (a string) wherever the request used a boolean. */
    static void normalizeDynamic(JsonObject node) {
        if (node.has("dynamic") && node.get("dynamic").isJsonPrimitive()) {
            node.addProperty("dynamic", node.get("dynamic").getAsString());
        }
        if (node.has("properties") && node.get("properties").isJsonObject()) {
            for (var e : node.getAsJsonObject("properties").entrySet()) {
                if (e.getValue().isJsonObject()) {
                    normalizeDynamic(e.getValue().getAsJsonObject());
                }
            }
        }
    }

    void validateMappings(JsonObject mappings) {
        if (mappings.has("properties")) {
            for (var e : mappings.getAsJsonObject("properties").entrySet()) {
                validateField(e.getKey(), e.getValue());
            }
        }
    }

    private static final Set<String> KNOWN_TYPES = Set.of("text", "keyword", "long", "integer", "short", "byte", "double", "float", "half_float",
            "scaled_float", "unsigned_long", "boolean", "date", "date_nanos", "ip", "object", "nested", "geo_point", "geo_shape", "binary",
            "flat_object", "knn_vector", "alias", "wildcard", "constant_keyword", "match_only_text", "completion", "join", "percolator",
            "rank_feature", "rank_features", "integer_range", "long_range", "float_range", "double_range", "date_range", "ip_range",
            "search_as_you_type", "token_count", "murmur3", "xy_point", "xy_shape", "semantic", "star_tree");

    private void validateField(String name, JsonElement def) {
        if (!def.isJsonObject()) {
            throw new OpenSearchException("mapper_parsing_exception", "Expected map for property [fields] on field [" + name + "] but got a class java.lang.String");
        }
        JsonObject d = def.getAsJsonObject();
        String type = d.has("type") ? d.get("type").getAsString() : d.has("properties") ? "object" : null;
        if (type == null) {
            throw new OpenSearchException("mapper_parsing_exception", "No type specified for field [" + name + "]");
        }
        if (!KNOWN_TYPES.contains(type)) {
            throw new OpenSearchException("mapper_parsing_exception", "No handler for type [" + type + "] declared on field [" + name + "]");
        }
        if (d.has("properties") && d.get("properties").isJsonObject()) {
            for (var e : d.getAsJsonObject("properties").entrySet()) {
                validateField(e.getKey(), e.getValue());
            }
        }
        if (d.has("fields") && d.get("fields").isJsonObject()) {
            for (var e : d.getAsJsonObject("fields").entrySet()) {
                validateField(e.getKey(), e.getValue());
            }
        }
    }

    Response deleteIndex(Request r) throws SQLException {
        String expr = r.var("index");
        if (expr.equals("_all") || expr.equals("*")) {
            // allowed (action.destructive_requires_name defaults to false in OpenSearch 2.x)
        }
        List<PostgresSearchStore.Resolved> ts = new ArrayList<>();
        PostgresSearchStore.ResolveOpts o = resolveOpts(r);
        o.expandClosed = true;
        // aliases are not valid delete targets, only concrete names / patterns
        for (String p : expr.split(",")) {
            boolean wildcard = p.contains("*") || p.equals("_all");
            for (PostgresSearchStore.IndexMeta m : store.allMetas()) {
                if (p.equals("_all") || (wildcard ? Mappings.wildcardMatch(p, m.name) : m.name.equals(p))) {
                    ts.add(new PostgresSearchStore.Resolved(m, List.of()));
                }
            }
            if (!wildcard && ts.stream().noneMatch(x -> x.index().name.equals(p))) {
                PostgresSearchStore.IndexMeta legacy = store.meta(p);
                if (legacy != null) {
                    ts.add(new PostgresSearchStore.Resolved(legacy, List.of()));
                } else if (store.allMetas().stream().anyMatch(m -> m.hasAlias(p))) {
                    if ("true".equals(r.q.get("ignore_unavailable"))) {
                        continue;
                    }
                    throw OpenSearchException.illegalArgument("The provided expression [" + p
                            + "] matches an alias, specify the corresponding concrete indices instead.");
                } else if (!"true".equals(r.q.get("ignore_unavailable"))) {
                    throw OpenSearchException.indexNotFound(p);
                }
            }
        }
        if (ts.isEmpty() && "false".equals(r.q.get("allow_no_indices"))) {
            throw OpenSearchException.indexNotFound(expr);
        }
        for (var t : ts) {
            store.deleteIndex(t.index());
        }
        return Response.json(200, ack());
    }

    Response existsIndex(Request r) throws SQLException {
        PostgresSearchStore.ResolveOpts o = resolveOpts(r);
        o.expandClosed = true;
        List<PostgresSearchStore.Resolved> ts;
        try {
            ts = store.resolve(r.var("index"), o);
        } catch (OpenSearchException e) {
            return Response.json(404, new JsonObject());
        }
        return Response.json(ts.isEmpty() ? 404 : 200, new JsonObject());
    }

    JsonObject settingsForOutput(PostgresSearchStore.IndexMeta m, boolean flat) {
        return settingsForOutput(m, flat, false);
    }

    JsonObject settingsForOutput(PostgresSearchStore.IndexMeta m, boolean flat, boolean humanSettings) {
        JsonObject s = m.settings.deepCopy();
        JsonObject idx = s.getAsJsonObject("index");
        JsonObject repl = new JsonObject();
        repl.addProperty("type", "DOCUMENT");
        if (!idx.has("replication")) {
            idx.add("replication", repl);
        }
        if (humanSettings && idx.has("creation_date")) {
            idx.addProperty("creation_date_string", Dates.format(Long.parseLong(idx.get("creation_date").getAsString()), null, java.time.ZoneOffset.UTC));
        }
        if (flat) {
            JsonObject flatOut = new JsonObject();
            flatten("index", idx, flatOut);
            return flatOut;
        }
        return s;
    }

    private static void flatten(String prefix, JsonElement e, JsonObject out) {
        if (e.isJsonObject()) {
            for (var en : e.getAsJsonObject().entrySet()) {
                flatten(prefix + "." + en.getKey(), en.getValue(), out);
            }
        } else {
            out.add(prefix, e);
        }
    }

    JsonObject aliasesOutput(PostgresSearchStore.IndexMeta m) {
        JsonObject a = new JsonObject();
        for (var e : m.aliases.entrySet()) {
            a.add(e.getKey(), e.getValue().isJsonObject() ? e.getValue() : new JsonObject());
        }
        return a;
    }

    Response getIndex(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty() && !r.var("index").contains("*") && !r.var("index").equals("_all") && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        boolean flat = "true".equals(r.q.get("flat_settings"));
        JsonObject out = new JsonObject();
        for (var t : ts) {
            PostgresSearchStore.IndexMeta m = t.index();
            JsonObject o = new JsonObject();
            o.add("aliases", aliasesOutput(m));
            o.add("mappings", m.mappings.raw);
            o.add("settings", settingsForOutput(m, flat, "true".equals(r.q.get("human"))));
            if ("true".equals(r.q.get("include_defaults"))) {
                o.add("defaults", new JsonObject());
            }
            out.add(m.name, o);
        }
        return Response.json(200, out);
    }

    Response getMapping(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        if (ts.isEmpty() && r.var("index") != null && !r.var("index").contains("*") && !r.var("index").equals("_all") && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        JsonObject out = new JsonObject();
        for (var t : ts) {
            JsonObject o = new JsonObject();
            o.add("mappings", t.index().mappings.raw);
            out.add(t.index().name, o);
        }
        return Response.json(200, out);
    }

    Response getFieldMapping(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        JsonObject out = new JsonObject();
        for (var t : ts) {
            JsonObject fm = new JsonObject();
            for (String pat : r.var("fields").split(",")) {
                for (Mappings.Field f : t.index().mappings.fields.values()) {
                    boolean match = pat.contains("*") ? Mappings.wildcardMatch(pat, f.path) : f.path.equals(pat);
                    if (match && !f.subField && !f.type.equals("object") && !f.type.equals("nested")) {
                        JsonObject entry = new JsonObject();
                        entry.addProperty("full_name", f.path);
                        JsonObject mapping = new JsonObject();
                        mapping.add(f.path.substring(f.path.lastIndexOf('.') + 1), f.def);
                        entry.add("mapping", mapping);
                        fm.add(f.path, entry);
                    }
                }
            }
            for (String[] mf : META_FIELDS) {
                for (String pat : r.var("fields").split(",")) {
                    if (Mappings.wildcardMatch(pat, mf[0]) && !fm.has(mf[0])) {
                        JsonObject entry = new JsonObject();
                        entry.addProperty("full_name", mf[0]);
                        entry.add("mapping", new JsonObject());
                        fm.add(mf[0], entry);
                    }
                }
            }
            if (fm.size() > 0 || !r.var("fields").contains("*")) {
                JsonObject o = new JsonObject();
                o.add("mappings", fm);
                out.add(t.index().name, o);
            }
        }
        return Response.json(200, out);
    }

    Response putMapping(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        if (body.has("_doc") && body.size() == 1 && body.get("_doc").isJsonObject()) {
            throw OpenSearchException.illegalArgument("Types cannot be provided in put mapping requests");
        }
        body = Mappings.expandDots(body);
        normalizeDynamic(body);
        validateMappings(body);
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        for (var t : ts) {
            JsonObject merged = t.index().mappings.raw.deepCopy();
            Mappings.merge(merged, body, "");
            store.persistMappings(t.index(), merged);
        }
        return Response.json(200, ack());
    }

    Response getSettings(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        if (ts.isEmpty() && r.var("index") != null && !r.var("index").contains("*") && !r.var("index").equals("_all") && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        boolean flat = "true".equals(r.q.get("flat_settings"));
        JsonObject out = new JsonObject();
        for (var t : ts) {
            JsonObject settings = settingsForOutput(t.index(), flat, "true".equals(r.q.get("human")));
            String name = r.var("name");
            if (name != null && !name.equals("_all")) {
                JsonObject filtered = new JsonObject();
                JsonObject flatAll = flat ? settings : settingsForOutput(t.index(), true);
                JsonObject f2 = new JsonObject();
                for (var e : flatAll.entrySet()) {
                    for (String pat : name.split(",")) {
                        if (Mappings.wildcardMatch(pat, e.getKey())) {
                            f2.add(e.getKey(), e.getValue());
                        }
                    }
                }
                if (f2.size() == 0) {
                    continue;
                }
                if (flat) {
                    settings = f2;
                } else {
                    JsonObject nested = new JsonObject();
                    for (var e : f2.entrySet()) {
                        putNested(nested, e.getKey(), e.getValue());
                    }
                    settings = nested;
                }
            }
            JsonObject o = new JsonObject();
            o.add("settings", settings);
            out.add(t.index().name, o);
        }
        return Response.json(200, out);
    }

    private static void putNested(JsonObject root, String key, JsonElement v) {
        String[] parts = key.split("\\.");
        JsonObject cur = root;
        for (int i = 0; i < parts.length - 1; i++) {
            if (!cur.has(parts[i])) {
                cur.add(parts[i], new JsonObject());
            }
            cur = cur.getAsJsonObject(parts[i]);
        }
        cur.add(parts[parts.length - 1], v);
    }

    Response putSettings(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        if (ts.isEmpty() && r.var("index") != null && !r.var("index").contains("*") && !r.var("index").equals("_all") && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        JsonObject norm = PostgresSearchStore.normalizeSettings(body);
        for (String immutable : new String[] {"number_of_shards", "uuid", "creation_date", "provided_name"}) {
            if (norm.getAsJsonObject("index").has(immutable)) {
                throw new OpenSearchException("illegal_argument_exception", "Can't update non dynamic settings [[index." + immutable
                        + "]] for open indices [[" + ts.get(0).index().name + "/" + ts.get(0).index().uuid + "]]");
            }
        }
        for (var t : ts) {
            JsonObject cur = t.index().settings.deepCopy();
            Templates.deepMerge(cur, norm);
            store.updateCatalogColumn(t.index(), "settings", cur);
        }
        return Response.json(200, ack());
    }

    Response openClose(Request r, boolean close) throws SQLException {
        PostgresSearchStore.ResolveOpts o = resolveOpts(r);
        o.expandClosed = true;
        List<PostgresSearchStore.Resolved> ts = store.resolve(r.var("index"), o);
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        for (var t : ts) {
            store.setClosed(t.index(), close);
        }
        JsonObject out = ack();
        out.addProperty("shards_acknowledged", true);
        if (close) {
            JsonObject idx = new JsonObject();
            for (var t : ts) {
                JsonObject i = new JsonObject();
                i.addProperty("closed", true);
                idx.add(t.index().name, i);
            }
            out.add("indices", idx);
        }
        return Response.json(200, out);
    }

    Response refresh(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        if (ts.isEmpty() && r.var("index") != null && !r.var("index").contains("*") && !r.var("index").equals("_all") && !"true".equals(r.q.get("ignore_unavailable"))) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        for (var t : ts) {
            store.refresh(t.index());
        }
        int n = ts.stream().mapToInt(t -> SearchEngine.shardsOf(t.index()) * (1 + replicas(t.index()))).sum();
        int ok = ts.stream().mapToInt(t -> SearchEngine.shardsOf(t.index())).sum();
        JsonObject o = new JsonObject();
        o.add("_shards", shards(n, ok));
        return Response.json(200, o);
    }

    Response indicesStats(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = r.var("index") == null ? store.resolve(null, resolveOpts(r)) : targets(r);
        JsonObject o = new JsonObject();
        o.add("_shards", shards(ts.size(), ts.size()));
        JsonObject all = new JsonObject();
        JsonObject indices = new JsonObject();
        long total = 0;
        for (var t : ts) {
            long c = store.count(t.index());
            total += c;
            JsonObject docs = new JsonObject();
            docs.addProperty("count", c);
            docs.addProperty("deleted", 0);
            JsonObject prim = new JsonObject();
            prim.add("docs", docs);
            JsonObject i = new JsonObject();
            i.addProperty("uuid", t.index().uuid);
            i.add("primaries", prim);
            i.add("total", prim);
            indices.add(t.index().name, i);
        }
        JsonObject docs = new JsonObject();
        docs.addProperty("count", total);
        docs.addProperty("deleted", 0);
        JsonObject prim = new JsonObject();
        prim.add("docs", docs);
        all.add("primaries", prim);
        all.add("total", prim);
        o.add("_all", all);
        o.add("indices", indices);
        return Response.json(200, o);
    }

    Response resolveIndex(Request r) throws SQLException {
        JsonArray indices = new JsonArray();
        JsonArray aliases = new JsonArray();
        String pat = r.var("name");
        for (var m : store.allMetas()) {
            if (Mappings.wildcardMatch(pat, m.name)) {
                JsonObject i = new JsonObject();
                i.addProperty("name", m.name);
                JsonArray al = new JsonArray();
                m.aliases.keySet().forEach(al::add);
                if (al.size() > 0) {
                    i.add("aliases", al);
                }
                JsonArray attrs = new JsonArray();
                attrs.add(m.closed ? "closed" : "open");
                i.add("attributes", attrs);
                indices.add(i);
            }
            for (String a : m.aliases.keySet()) {
                if (Mappings.wildcardMatch(pat, a)) {
                    JsonObject ao = new JsonObject();
                    ao.addProperty("name", a);
                    JsonArray is = new JsonArray();
                    is.add(m.name);
                    ao.add("indices", is);
                    aliases.add(ao);
                }
            }
        }
        JsonObject out = new JsonObject();
        out.add("indices", indices);
        out.add("aliases", aliases);
        out.add("data_streams", new JsonArray());
        return Response.json(200, out);
    }

    // ---- aliases ----

    Response getAlias(Request r) throws SQLException {
        String name = r.var("name");
        List<PostgresSearchStore.IndexMeta> metas = new ArrayList<>();
        if (r.var("index") != null) {
            for (var t : targets(r)) {
                metas.add(t.index());
            }
            if (metas.isEmpty() && !r.var("index").contains("*") && !r.var("index").equals("_all")) {
                throw OpenSearchException.indexNotFound(r.var("index"));
            }
        } else {
            metas.addAll(store.allMetas());
        }
        JsonObject out = new JsonObject();
        Set<String> found = new LinkedHashSet<>();
        for (var m : metas) {
            JsonObject al = new JsonObject();
            for (var e : m.aliases.entrySet()) {
                boolean match = name == null || name.equals("_all");
                if (!match) {
                    for (String p : name.split(",")) {
                        if (p.startsWith("-")) {
                            if (Mappings.wildcardMatch(p.substring(1), e.getKey())) {
                                match = false;
                            }
                        } else if (Mappings.wildcardMatch(p, e.getKey())) {
                            match = true;
                        }
                    }
                }
                if (match) {
                    al.add(e.getKey(), e.getValue());
                    found.add(e.getKey());
                }
            }
            if (al.size() > 0 || name == null || name.contains("*")) {
                if (al.size() > 0 || r.var("index") != null || name == null) {
                    JsonObject o = new JsonObject();
                    o.add("aliases", al);
                    if (al.size() > 0 || name == null) {
                        out.add(m.name, o);
                    }
                }
            }
        }
        if (name != null && !name.equals("_all") && !name.contains("*")) {
            List<String> missing = new ArrayList<>();
            for (String p : name.split(",")) {
                if (!found.contains(p)) {
                    missing.add(p);
                }
            }
            if (!missing.isEmpty()) {
                if (r.method.equals("HEAD")) {
                    return Response.json(404, new JsonObject());
                }
                JsonObject err = new JsonObject();
                java.util.Collections.sort(missing);
                err.addProperty("error", (missing.size() > 1 ? "aliases [" : "alias [") + String.join(",", missing) + "] missing");
                err.addProperty("status", 404);
                if (out.size() > 0) {
                    for (var e : err.entrySet()) {
                        out.add(e.getKey(), e.getValue());
                    }
                    return Response.json(404, out);
                }
                return Response.json(404, err);
            }
        }
        if (r.method.equals("HEAD")) {
            return Response.json(out.size() > 0 ? 200 : 404, new JsonObject());
        }
        return Response.json(200, out);
    }

    Response putAlias(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, false);
        for (String k : body.keySet()) {
            if (!Set.of("filter", "routing", "index_routing", "search_routing", "is_write_index", "is_hidden", "must_exist", "alias", "index", "indices", "aliases").contains(k)) {
                throw new OpenSearchException("x_content_parse_exception", "[alias_action] unknown field [" + k + "]");
            }
        }
        String name = r.var("name") != null ? r.var("name") : body.has("alias") ? body.get("alias").getAsString() : null;
        if (name == null) {
            throw OpenSearchException.validation("alias is missing");
        }
        String idxExpr = r.var("index");
        if (idxExpr == null) {
            if (body.has("index")) {
                idxExpr = body.get("index").getAsString();
            } else if (body.has("indices")) {
                idxExpr = String.join(",", body.getAsJsonArray("indices").asList().stream().map(JsonElement::getAsString).toList());
            } else {
                throw OpenSearchException.validation("index is missing");
            }
        } else if (body.has("index") && !idxExpr.contains("*")) {
            idxExpr = body.get("index").getAsString();
        }
        List<PostgresSearchStore.Resolved> ts = store.resolve(idxExpr, resolveOpts(r));
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(idxExpr);
        }
        for (var t : ts) {
            addAlias(t.index(), name, body);
        }
        return Response.json(200, ack());
    }

    private void addAlias(PostgresSearchStore.IndexMeta m, String alias, JsonObject def) throws SQLException {
        PostgresSearchStore.validateIndexName(alias);
        if (store.meta(alias) != null) {
            throw new OpenSearchException("invalid_alias_name_exception", "Invalid alias name [" + alias + "]: an index or data stream exists with the same name as the alias")
                    .with("index", alias);
        }
        JsonObject aliases = m.aliases.deepCopy();
        JsonObject d = new JsonObject();
        for (var e : def.entrySet()) {
            if (!Set.of("alias", "index", "must_exist").contains(e.getKey())) {
                d.add(e.getKey(), e.getValue());
            }
        }
        if (d.has("routing")) {
            String rt = d.get("routing").getAsString();
            if (!d.has("index_routing")) {
                d.addProperty("index_routing", rt);
            }
            if (!d.has("search_routing")) {
                d.addProperty("search_routing", rt);
            }
            d.remove("routing");
        }
        for (String k : new String[] {"index_routing", "search_routing"}) {
            if (d.has(k) && d.get(k).isJsonPrimitive()) {
                d.addProperty(k, d.get(k).getAsString());
            }
        }
        if (d.has("filter")) {
            QueryParser.parse(d.get("filter"));
        }
        aliases.add(alias, d);
        store.updateCatalogColumn(m, "aliases", aliases);
    }

    Response deleteAlias(Request r) throws SQLException {
        List<PostgresSearchStore.Resolved> ts = targets(r);
        if (ts.isEmpty()) {
            throw OpenSearchException.indexNotFound(r.var("index"));
        }
        boolean any = false;
        for (var t : ts) {
            JsonObject aliases = t.index().aliases.deepCopy();
            for (String p : r.var("name").split(",")) {
                for (String a : new ArrayList<>(aliases.keySet())) {
                    if (p.equals("_all") || (p.contains("*") ? Mappings.wildcardMatch(p, a) : p.equals(a))) {
                        aliases.remove(a);
                        any = true;
                    }
                }
            }
            store.updateCatalogColumn(t.index(), "aliases", aliases);
        }
        if (!any) {
            OpenSearchException e = new OpenSearchException("aliases_not_found_exception", "aliases [" + r.var("name") + "] missing");
            e.extra.addProperty("resource.type", "aliases");
            e.extra.addProperty("resource.id", r.var("name"));
            throw e;
        }
        return Response.json(200, ack());
    }

    Response updateAliases(Request r) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        if (!body.has("actions") || !body.get("actions").isJsonArray()) {
            throw OpenSearchException.validation("Must specify at least one alias action");
        }
        for (JsonElement ae : body.getAsJsonArray("actions")) {
            JsonObject action = ae.getAsJsonObject();
            String type = action.keySet().iterator().next();
            JsonObject spec = action.getAsJsonObject(type);
            if (!Set.of("add", "remove", "remove_index").contains(type)) {
                throw new OpenSearchException("illegal_argument_exception", "Unknown action [" + type + "]");
            }
            List<String> idx = new ArrayList<>();
            if (spec.has("index")) {
                idx.add(spec.get("index").getAsString());
            }
            if (spec.has("indices")) {
                spec.getAsJsonArray("indices").forEach(e -> idx.add(e.getAsString()));
            }
            List<String> als = new ArrayList<>();
            if (spec.has("alias")) {
                als.add(spec.get("alias").getAsString());
            }
            if (spec.has("aliases")) {
                if (spec.get("aliases").isJsonArray()) {
                    spec.getAsJsonArray("aliases").forEach(e -> als.add(e.getAsString()));
                } else {
                    als.add(spec.get("aliases").getAsString());
                }
            }
            if (idx.isEmpty()) {
                throw OpenSearchException.validation("One of [index] or [indices] is required");
            }
            if (!type.equals("remove_index") && als.isEmpty()) {
                throw OpenSearchException.validation("One of [alias] or [aliases] is required");
            }
            for (String ix : idx) {
                List<PostgresSearchStore.Resolved> ts = store.resolve(ix, new PostgresSearchStore.ResolveOpts());
                if (ts.isEmpty()) {
                    throw OpenSearchException.indexNotFound(ix);
                }
                for (var t : ts) {
                    switch (type) {
                        case "add" -> {
                            for (String a : als) {
                                addAlias(t.index(), a, spec);
                            }
                        }
                        case "remove" -> {
                            JsonObject aliases = t.index().aliases.deepCopy();
                            boolean any = false;
                            for (String a : als) {
                                for (String have : new ArrayList<>(aliases.keySet())) {
                                    if (a.contains("*") ? Mappings.wildcardMatch(a, have) : a.equals(have)) {
                                        aliases.remove(have);
                                        any = true;
                                    }
                                }
                            }
                            if (!any && (!spec.has("must_exist") || spec.get("must_exist").getAsBoolean())) {
                                OpenSearchException e = new OpenSearchException("aliases_not_found_exception", "aliases [" + String.join(",", als) + "] missing");
                                throw e;
                            }
                            store.updateCatalogColumn(t.index(), "aliases", aliases);
                        }
                        default -> store.deleteIndex(t.index());
                    }
                }
            }
        }
        return Response.json(200, ack());
    }

    // ---- templates ----

    Response getTemplate(Request r, String kind) throws SQLException {
        Map<String, JsonObject> all = store.templates(kind);
        String name = r.var("name");
        JsonObject legacy = new JsonObject();
        JsonArray listed = new JsonArray();
        boolean any = false;
        for (var e : all.entrySet()) {
            if (name == null || (name.contains("*") ? Mappings.wildcardMatch(name, e.getKey()) : name.equals(e.getKey()))) {
                any = true;
                if (kind.equals("template")) {
                    legacy.add(e.getKey(), e.getValue());
                } else {
                    JsonObject item = new JsonObject();
                    item.addProperty("name", e.getKey());
                    item.add(kind.equals("index_template") ? "index_template" : "component_template", e.getValue());
                    listed.add(item);
                }
            }
        }
        if (!any && name != null && !name.contains("*")) {
            if (r.method.equals("HEAD")) {
                return Response.json(404, new JsonObject());
            }
            if (kind.equals("template")) {
                return Response.json(404, new JsonObject());
            }
            return Response.json(404, errorJson(new OpenSearchException("resource_not_found_exception", (kind.equals("index_template") ? "index template" : "component template")
                    + " matching [" + name + "] not found")));
        }
        if (r.method.equals("HEAD")) {
            return Response.json(200, new JsonObject());
        }
        if (kind.equals("template")) {
            return Response.json(200, legacy);
        }
        JsonObject out = new JsonObject();
        out.add(kind.equals("index_template") ? "index_templates" : "component_templates", listed);
        return Response.json(200, out);
    }

    Response putTemplate(Request r, String kind) throws SQLException {
        JsonObject body = parseBody(r.body, true);
        String name = r.var("name");
        if (kind.equals("index_template")) {
            if (!body.has("index_patterns")) {
                throw OpenSearchException.validation("index patterns are missing");
            }
            if ("true".equals(r.q.get("create")) && store.templates(kind).containsKey(name)) {
                throw new OpenSearchException("illegal_argument_exception", "index template [" + name + "] already exists");
            }
            if (body.has("composed_of")) {
                Map<String, JsonObject> comps = store.templates("component_template");
                for (JsonElement c : body.getAsJsonArray("composed_of")) {
                    if (!comps.containsKey(c.getAsString())) {
                        throw new OpenSearchException("invalid_index_template_exception", "index_template [" + name + "] invalid, cause [index template ["
                                + name + "] specifies a missing component templates [" + c.getAsString() + "] that does not exist]");
                    }
                }
            }
        } else if (kind.equals("template")) {
            if (!body.has("index_patterns") && !body.has("template")) {
                throw OpenSearchException.validation("index patterns are missing");
            }
            if ("true".equals(r.q.get("create")) && store.templates(kind).containsKey(name)) {
                throw new OpenSearchException("illegal_argument_exception", "index_template [" + name + "] already exists");
            }
        } else if (kind.equals("component_template") && !body.has("template")) {
            throw new OpenSearchException("x_content_parse_exception", "[component_template] Required [template]");
        }
        JsonObject norm = body.deepCopy();
        if (kind.equals("template")) {
            norm.remove("template");
            if (!norm.has("index_patterns") && body.has("template")) {
                JsonArray pats = new JsonArray();
                pats.add(body.get("template"));
                norm.add("index_patterns", pats);
            } else if (norm.has("index_patterns") && norm.get("index_patterns").isJsonPrimitive()) {
                JsonArray pats = new JsonArray();
                pats.add(norm.get("index_patterns"));
                norm.add("index_patterns", pats);
            }
            if (!norm.has("order")) {
                norm.addProperty("order", 0);
            }
            norm.add("settings", PostgresSearchStore.normalizeSettings(norm.has("settings") ? norm.getAsJsonObject("settings") : new JsonObject()));
            if (!norm.has("mappings")) {
                norm.add("mappings", new JsonObject());
            }
            if (!norm.has("aliases")) {
                norm.add("aliases", new JsonObject());
            }
        } else if (norm.has("template") && norm.get("template").isJsonObject()) {
            JsonObject t = norm.getAsJsonObject("template");
            if (t.has("settings")) {
                t.add("settings", PostgresSearchStore.normalizeSettings(t.getAsJsonObject("settings")));
            }
            if (t.has("mappings")) {
                t.add("mappings", Mappings.expandDots(t.getAsJsonObject("mappings")));
            }
        }
        store.putTemplate(kind, name, norm);
        return Response.json(200, ack());
    }

    Response deleteTemplate(Request r, String kind) throws SQLException {
        if (!store.deleteTemplate(kind, r.var("name"))) {
            return Response.json(404, errorJson(new OpenSearchException("index_template_missing_exception", "index_template [" + r.var("name") + "] missing")));
        }
        return Response.json(200, ack());
    }
}
