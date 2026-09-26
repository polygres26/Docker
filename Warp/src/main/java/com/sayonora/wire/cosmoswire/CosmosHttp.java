package com.sayonora.wire.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.wire.cosmoswire.CosmosStore.Coll;
import com.sayonora.wire.cosmoswire.CosmosStore.DbRow;
import com.sayonora.wire.cosmoswire.CosmosStore.Doc;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** HTTP mapping of the Cosmos DB for NoSQL REST API: routing, headers, bodies and errors. See {@link CosmosService}. */
final class CosmosHttp {

    private static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US).withZone(ZoneOffset.UTC);

    private final CosmosService svc;
    private final CosmosStore store;
    private final CosmosConfig cfg;
    private final CosmosAuth auth;
    private final AtomicLong sessionLsn = new AtomicLong(1);
    private final boolean planHandshake = "true".equalsIgnoreCase(System.getenv("WARP_COSMOSWIRE_QUERY_PLAN_HANDSHAKE"));

    /** The operation name and read/write flag of the request just handled (for metrics). */
    static final class Outcome {
        String op = "Unknown";
        boolean write;
    }

    CosmosHttp(CosmosService svc, CosmosConfig cfg) {
        this.svc = svc;
        this.store = svc.store();
        this.cfg = cfg;
        this.auth = new CosmosAuth(cfg);
    }

    // ------------------------------------------------------------------------------------------ request/response plumbing

    private static final class Req {
        final HttpServletRequest r;
        final String method;
        final String rawPath;
        final List<String> segs = new ArrayList<>();
        final String decodedPath;
        byte[] body;

        Req(HttpServletRequest r) {
            this.r = r;
            this.method = r.getMethod().toUpperCase(Locale.ROOT);
            String uri = r.getRequestURI();
            this.rawPath = uri == null ? "/" : uri;
            this.decodedPath = URLDecoder.decode(rawPath.replace("+", "%2B"), StandardCharsets.UTF_8);
            for (String s : rawPath.split("/")) {
                if (!s.isEmpty()) {
                    segs.add(URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8));
                }
            }
        }

        String h(String n) {
            return r.getHeader(n);
        }

        boolean flag(String n) {
            String v = h(n);
            return v != null && v.trim().equalsIgnoreCase("true");
        }

        byte[] body() throws IOException {
            if (body == null) {
                body = r.getInputStream().readNBytes(16 * 1024 * 1024);
            }
            return body;
        }

        JsonObject json() throws IOException {
            byte[] b = body();
            if (b.length == 0) {
                throw CosmosException.badRequest("The request body is empty; a JSON document is required.");
            }
            try {
                JsonElement e = JsonParser.parseString(new String(b, StandardCharsets.UTF_8));
                if (!e.isJsonObject()) {
                    throw CosmosException.badRequest("The request body must be a JSON object.");
                }
                return e.getAsJsonObject();
            } catch (JsonParseException ex) {
                throw CosmosException.badRequest("The request body is not valid JSON: " + ex.getMessage());
            }
        }

        JsonElement jsonAny() throws IOException {
            try {
                return JsonParser.parseString(new String(body(), StandardCharsets.UTF_8));
            } catch (JsonParseException ex) {
                throw CosmosException.badRequest("The request body is not valid JSON: " + ex.getMessage());
            }
        }
    }

    private static final class Res {
        int status = 200;
        String body;
        final java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        double charge = 1.0;
        long lsn;
    }

    private Res ok(JsonElement body) {
        Res r = new Res();
        r.body = body.toString();
        return r;
    }

    private Res status(int code, JsonElement body) {
        Res r = ok(body);
        r.status = code;
        return r;
    }

    /** Handles one request end to end. */
    void handle(HttpServletRequest request, HttpServletResponse response, Outcome outcome) throws IOException {
        long t0 = System.nanoTime();
        String activity = request.getHeader("x-ms-activity-id");
        if (activity == null || activity.isBlank()) {
            activity = UUID.randomUUID().toString();
        }
        Req req = new Req(request);
        Res res;
        try {
            String date = req.h("x-ms-date");
            if (date == null) {
                date = req.h("date");
            }
            auth.check(req.method, req.rawPath, req.decodedPath, date, req.h("authorization"));
            res = route(req, outcome);
        } catch (CosmosException e) {
            res = error(e, activity);
        } catch (RuntimeException e) {
            res = error(CosmosException.internal("An unexpected error occurred: " + e), activity);
        }
        long lsn = res.lsn > 0 ? sessionLsn.accumulateAndGet(res.lsn, Math::max) : sessionLsn.get();
        response.setStatus(res.status);
        response.setHeader("Server", "Warp-cosmoswire/1.0 Microsoft-HTTPAPI/2.0");
        response.setHeader("Date", HTTP_DATE.format(Instant.now()));
        response.setHeader("x-ms-activity-id", activity);
        response.setHeader("x-ms-request-charge", String.format(Locale.ROOT, "%.2f", res.charge));
        response.setHeader("x-ms-session-token", "0:1#" + lsn + "#1=" + lsn);
        response.setHeader("x-ms-request-duration-ms", String.format(Locale.ROOT, "%.3f", (System.nanoTime() - t0) / 1e6));
        response.setHeader("x-ms-serviceversion", "version=2.14.0");
        response.setHeader("x-ms-gatewayversion", "version=2.14.0");
        response.setHeader("x-ms-schemaversion", "1.14");
        response.setHeader("lsn", String.valueOf(lsn));
        response.setHeader("x-ms-cosmos-llsn", String.valueOf(lsn));
        response.setHeader("x-ms-global-committed-lsn", String.valueOf(lsn));
        response.setHeader("x-ms-number-of-read-regions", "0");
        res.headers.forEach(response::setHeader);
        if (res.status == 304 || res.status == 204 || res.body == null) {
            response.setContentLength(0);
            return;
        }
        byte[] b = res.body.getBytes(StandardCharsets.UTF_8);
        response.setContentType("application/json");
        response.setContentLength(b.length);
        response.getOutputStream().write(b);
    }

    private Res error(CosmosException e, String activity) {
        JsonObject o = new JsonObject();
        o.addProperty("code", e.code);
        o.addProperty("message", e.wireMessage(activity));
        Res r = status(e.status, o);
        r.charge = 0;
        if (e.substatus != 0) {
            r.headers.put("x-ms-substatus", String.valueOf(e.substatus));
        }
        r.headers.putAll(e.headers);
        return r;
    }

    // ------------------------------------------------------------------------------------------ routing

    private Res route(Req q, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        if (s.isEmpty()) {
            if (m.equals("GET")) {
                out.op = "GetDatabaseAccount";
                return ok(account(q));
            }
            throw notAllowed(m, q);
        }
        switch (s.get(0)) {
            case "dbs":
                return dbs(q, out);
            case "offers":
                return offers(q, out);
            default:
                throw CosmosException.notFound("{\"Errors\":[\"Resource Not Found. Learn more: https://aka.ms/cosmosdb-tsg-not-found\"]}");
        }
    }

    private static CosmosException notAllowed(String m, Req q) {
        return new CosmosException(405, "MethodNotAllowed", "The HTTP method " + m + " is not allowed for " + q.decodedPath);
    }

    private static boolean isQuery(Req q) {
        String ct = q.h("content-type");
        return q.flag("x-ms-documentdb-isquery") || ct != null && ct.toLowerCase(Locale.ROOT).contains("query+json");
    }

    private JsonObject account(Req q) {
        String host = q.h("host");
        String url = cfg.advertisedUrl != null ? cfg.advertisedUrl : "http://" + (host == null ? "localhost" : host) + "/";
        if (!url.endsWith("/")) {
            url += "/";
        }
        JsonObject o = new JsonObject();
        o.addProperty("_self", "");
        o.addProperty("id", "warp-cosmoswire");
        o.addProperty("_rid", "warp-cosmoswire.warp");
        o.addProperty("media", "//media/");
        o.addProperty("addresses", "//addresses/");
        o.addProperty("_dbs", "//dbs/");
        JsonArray loc = new JsonArray();
        JsonObject l = new JsonObject();
        l.addProperty("name", "Local");
        l.addProperty("databaseAccountEndpoint", url);
        loc.add(l);
        o.add("writableLocations", loc.deepCopy());
        o.add("readableLocations", loc);
        o.addProperty("enableMultipleWriteLocations", false);
        JsonObject urp = new JsonObject();
        urp.addProperty("asyncReplication", false);
        urp.addProperty("minReplicaSetSize", 1);
        urp.addProperty("maxReplicasetSize", 4);
        o.add("userReplicationPolicy", urp);
        JsonObject srp = new JsonObject();
        srp.addProperty("minReplicaSetSize", 1);
        srp.addProperty("maxReplicasetSize", 4);
        o.add("systemReplicationPolicy", srp);
        JsonObject ucp = new JsonObject();
        ucp.addProperty("defaultConsistencyLevel", "Session");
        o.add("userConsistencyPolicy", ucp);
        JsonObject rp = new JsonObject();
        rp.addProperty("primaryReadCoefficient", 1);
        rp.addProperty("secondaryReadCoefficient", 1);
        o.add("readPolicy", rp);
        o.addProperty("queryEngineConfiguration", "{\"maxSqlQueryInputLength\":262144,\"maxJoinsPerSqlQuery\":5,\"maxLogicalAndPerSqlQuery\":500,"
                + "\"maxLogicalOrPerSqlQuery\":500,\"maxUdfRefPerSqlQuery\":10,\"maxInExpressionItemsCount\":16000,\"queryMaxInMemorySortDocumentCount\":"
                + "500,\"maxQueryRequestTimeoutFraction\":0.9,\"sqlAllowNonFiniteNumbers\":false,\"sqlAllowAggregateFunctions\":true,"
                + "\"sqlAllowSubQuery\":true,\"sqlAllowScalarSubQuery\":true,\"allowNewKeywords\":true,\"sqlAllowLike\":true,"
                + "\"sqlAllowGroupByClause\":true,\"maxSpatialQueryCells\":12,\"spatialMaxGeometryPointCount\":256,\"sqlDisableOptimizationFlags\":0,"
                + "\"sqlAllowTop\":true,\"enableSpatialIndexing\":true}");
        return o;
    }

    private Res list(String key, String rid, List<JsonElement> items) {
        JsonObject o = new JsonObject();
        o.addProperty("_rid", rid);
        JsonArray a = new JsonArray();
        items.forEach(a::add);
        o.add(key, a);
        o.addProperty("_count", items.size());
        Res r = ok(o);
        r.headers.put("x-ms-item-count", String.valueOf(items.size()));
        return r;
    }

    /** A query over the in-memory list of catalog resources (databases, containers, offers, scripts...). */
    private Res metaQuery(Req q, String key, String rid, List<JsonObject> items) throws IOException {
        JsonObject b = q.json();
        CosmosQuery cq = CosmosQuery.compile(b.has("query") ? b.get("query").getAsString() : null,
                b.has("parameters") && b.get("parameters").isJsonArray() ? b.getAsJsonArray("parameters") : null);
        List<JsonElement> docs = new ArrayList<>(items);
        return list(key, rid, cq.runAll(docs));
    }

    private DbRow db(String seg) {
        DbRow d = store.getDb(seg);
        if (d == null) {
            for (DbRow x : store.listDbs()) {
                if (x.rid().equals(seg)) {
                    return x;
                }
            }
            throw CosmosStore.notFound();
        }
        return d;
    }

    private Coll coll(DbRow d, String seg) {
        Coll c = store.getColl(d.id(), seg);
        if (c == null) {
            for (Coll x : store.listColls(d.id())) {
                if (x.rid().equals(seg)) {
                    return x;
                }
            }
            throw CosmosStore.notFound();
        }
        return c;
    }

    // ------------------------------------------------------------------------------------------ databases

    private Res dbs(Req q, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        if (s.size() == 1) {
            if (m.equals("GET")) {
                out.op = "ListDatabases";
                List<JsonElement> l = new ArrayList<>();
                store.listDbs().forEach(d -> l.add(d.toJson()));
                return list("Databases", "", l);
            }
            if (m.equals("POST") && isQuery(q)) {
                out.op = "QueryDatabases";
                List<JsonObject> l = new ArrayList<>();
                store.listDbs().forEach(d -> l.add(d.toJson()));
                return metaQuery(q, "Databases", "", l);
            }
            if (m.equals("POST")) {
                out.op = "CreateDatabase";
                out.write = true;
                JsonObject b = q.json();
                String id = idOf(b);
                DbRow d = store.createDb(id, throughputHeader(q));
                Res r = status(201, d.toJson());
                r.charge = 5.71;
                r.headers.put("etag", d.etag());
                return r;
            }
            throw notAllowed(m, q);
        }
        DbRow d = db(s.get(1));
        if (s.size() == 2) {
            if (m.equals("GET")) {
                out.op = "ReadDatabase";
                Res r = ok(d.toJson());
                r.headers.put("etag", d.etag());
                return r;
            }
            if (m.equals("DELETE")) {
                out.op = "DeleteDatabase";
                out.write = true;
                store.deleteDb(d.id());
                Res r = new Res();
                r.status = 204;
                return r;
            }
            throw notAllowed(m, q);
        }
        switch (s.get(2)) {
            case "colls":
                return colls(q, d, out);
            case "users":
            case "permissions":
            case "clientencryptionkeys":
                throw CosmosException.unsupported("Users, permissions and client encryption keys are not supported by Warp cosmoswire; use master-key "
                        + "authentication (or configure WARP_COSMOSWIRE_RESOURCE_TOKENS).");
            default:
                throw CosmosStore.notFound();
        }
    }

    private static String idOf(JsonObject b) {
        if (!b.has("id") || !CosmosJson.isStr(b.get("id")) || b.get("id").getAsString().isEmpty()) {
            throw CosmosException.badRequest("The input content is invalid because the required properties - 'id; ' - are missing");
        }
        String id = b.get("id").getAsString();
        if (id.length() > 255 || id.indexOf('/') >= 0 || id.indexOf('\\') >= 0 || id.indexOf('?') >= 0 || id.indexOf('#') >= 0) {
            throw CosmosException.badRequest("The specified resource name contains invalid characters or is too long.");
        }
        return id;
    }

    private static Integer throughputHeader(Req q) {
        String v = q.h("x-ms-offer-throughput");
        if (v == null || v.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw CosmosException.badRequest("x-ms-offer-throughput must be an integer.");
        }
    }

    // ------------------------------------------------------------------------------------------ containers

    private static final String DEFAULT_INDEXING = "{\"indexingMode\":\"consistent\",\"automatic\":true,\"includedPaths\":[{\"path\":\"/*\"}],"
            + "\"excludedPaths\":[{\"path\":\"/\\\"_etag\\\"/?\"}]}";

    private JsonObject collDef(JsonObject b, Coll old) {
        JsonObject def = new JsonObject();
        JsonObject pk = b.has("partitionKey") && b.get("partitionKey").isJsonObject() ? b.getAsJsonObject("partitionKey") : null;
        if (pk == null || !pk.has("paths") || !pk.get("paths").isJsonArray() || pk.getAsJsonArray("paths").isEmpty()) {
            throw CosmosException.badRequest("The partition key definition is required: partitionKey.paths must list 1 to 3 paths.");
        }
        JsonArray paths = pk.getAsJsonArray("paths");
        if (paths.size() > 3) {
            throw CosmosException.badRequest("Hierarchical partition keys support at most 3 paths.");
        }
        for (JsonElement p : paths) {
            String ps = p.getAsString();
            if (!ps.startsWith("/") || ps.length() < 2) {
                throw CosmosException.badRequest("Partition key path '" + ps + "' is invalid: it must start with '/'.");
            }
        }
        JsonObject npk = new JsonObject();
        npk.add("paths", paths.deepCopy());
        npk.addProperty("kind", paths.size() > 1 ? "MultiHash" : pk.has("kind") ? pk.get("kind").getAsString() : "Hash");
        npk.addProperty("version", pk.has("version") ? pk.get("version").getAsInt() : 2);
        if (old != null && !CosmosJson.equal(old.def().get("partitionKey").getAsJsonObject().get("paths"), paths)) {
            throw CosmosException.badRequest("The partition key of a container cannot be changed.");
        }
        def.add("partitionKey", npk);
        def.add("indexingPolicy", b.has("indexingPolicy") && b.get("indexingPolicy").isJsonObject() ? b.get("indexingPolicy")
                : JsonParser.parseString(DEFAULT_INDEXING));
        if (b.has("uniqueKeyPolicy") && b.get("uniqueKeyPolicy").isJsonObject()) {
            if (old != null && !CosmosJson.equal(old.def().get("uniqueKeyPolicy"), b.get("uniqueKeyPolicy"))) {
                throw CosmosException.badRequest("The unique key policy of a container cannot be changed.");
            }
            for (JsonElement k : b.getAsJsonObject("uniqueKeyPolicy").getAsJsonArray("uniqueKeys")) {
                for (JsonElement p : k.getAsJsonObject().getAsJsonArray("paths")) {
                    if (!p.getAsString().startsWith("/")) {
                        throw CosmosException.badRequest("Unique key path '" + p.getAsString() + "' is invalid: it must start with '/'.");
                    }
                }
            }
            def.add("uniqueKeyPolicy", b.get("uniqueKeyPolicy"));
        } else if (old != null && old.def().has("uniqueKeyPolicy")) {
            def.add("uniqueKeyPolicy", old.def().get("uniqueKeyPolicy"));
        }
        if (b.has("defaultTtl") && !b.get("defaultTtl").isJsonNull()) {
            JsonElement t = b.get("defaultTtl");
            if (!CosmosJson.isInteger(t) || CosmosJson.dbl(t) != -1 && (CosmosJson.dbl(t) <= 0 || CosmosJson.dbl(t) > Integer.MAX_VALUE)) {
                throw CosmosException.badRequest("defaultTtl must be -1 or a positive integer number of seconds.");
            }
            def.add("defaultTtl", t);
        }
        def.add("conflictResolutionPolicy", b.has("conflictResolutionPolicy") ? b.get("conflictResolutionPolicy")
                : JsonParser.parseString("{\"mode\":\"LastWriterWins\",\"conflictResolutionPath\":\"/_ts\",\"conflictResolutionProcedure\":\"\"}"));
        def.add("geospatialConfig", b.has("geospatialConfig") ? b.get("geospatialConfig") : JsonParser.parseString("{\"type\":\"Geography\"}"));
        for (String k : new String[] {"computedProperties", "vectorEmbeddingPolicy", "fullTextPolicy", "changeFeedPolicy", "analyticalStorageTtl",
                "materializedViewDefinition"}) {
            if (b.has(k)) {
                def.add(k, b.get(k));
            }
        }
        return def;
    }

    private Res colls(Req q, DbRow d, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        if (s.size() == 3) {
            if (m.equals("GET")) {
                out.op = "ListCollections";
                List<JsonElement> l = new ArrayList<>();
                store.listColls(d.id()).forEach(c -> l.add(c.toJson()));
                return list("DocumentCollections", d.rid(), l);
            }
            if (m.equals("POST") && isQuery(q)) {
                out.op = "QueryCollections";
                List<JsonObject> l = new ArrayList<>();
                store.listColls(d.id()).forEach(c -> l.add(c.toJson()));
                return metaQuery(q, "DocumentCollections", d.rid(), l);
            }
            if (m.equals("POST")) {
                out.op = "CreateCollection";
                out.write = true;
                JsonObject b = q.json();
                String id = idOf(b);
                Coll c = store.createColl(d.id(), id, collDef(b, null), throughputHeader(q));
                Res r = status(201, c.toJson());
                r.charge = 5.71;
                r.headers.put("etag", c.etag());
                return r;
            }
            throw notAllowed(m, q);
        }
        Coll c = coll(d, s.get(3));
        if (s.size() == 4) {
            switch (m) {
                case "GET": {
                    out.op = "ReadCollection";
                    Res r = ok(c.toJson());
                    r.headers.put("etag", c.etag());
                    r.headers.put("x-ms-resource-quota", "documentSize=10240;documentsSize=10485760;documentsCount=-1;collectionSize=10485760;");
                    r.headers.put("x-ms-resource-usage", "documentSize=0;documentsSize=0;documentsCount=" + (q.flag("x-ms-documentdb-populatequotainfo")
                            ? totalCount(c) : 0) + ";collectionSize=0;");
                    r.headers.put("x-ms-alt-content-path", "dbs/" + c.db() + "/colls/" + c.id());
                    r.headers.put("x-ms-content-path", c.rid());
                    return r;
                }
                case "PUT": {
                    out.op = "ReplaceCollection";
                    out.write = true;
                    JsonObject b = q.json();
                    Coll n = store.replaceColl(c, collDef(b, c));
                    Res r = ok(n.toJson());
                    r.headers.put("etag", n.etag());
                    return r;
                }
                case "DELETE": {
                    out.op = "DeleteCollection";
                    out.write = true;
                    store.deleteColl(d.id(), c.id());
                    Res r = new Res();
                    r.status = 204;
                    return r;
                }
                default:
                    throw notAllowed(m, q);
            }
        }
        switch (s.get(4)) {
            case "docs":
                return docs(q, c, out);
            case "pkranges":
                out.op = "ReadPartitionKeyRanges";
                return pkranges(q, c);
            case "sprocs":
            case "triggers":
            case "udfs":
                return scripts(q, c, s.get(4), out);
            case "conflicts":
                out.op = "ListConflicts";
                if (!m.equals("GET")) {
                    throw notAllowed(m, q);
                }
                return list("Conflicts", c.rid(), List.of());
            default:
                throw CosmosStore.notFound();
        }
    }

    private long totalCount(Coll c) {
        long n = 0;
        for (String h : store.hosts()) {
            n += store.count(h, c);
        }
        return n;
    }

    private Res pkranges(Req q, Coll c) {
        if (!q.method.equals("GET")) {
            throw notAllowed(q.method, q);
        }
        JsonObject r = new JsonObject();
        r.addProperty("id", "0");
        r.addProperty("_rid", c.rid() + "AAAA");
        r.addProperty("_self", c.selfLink() + "pkranges/0/");
        r.addProperty("_etag", "\"00000000-0000-0000-0000-000000000000\"");
        r.addProperty("_ts", c.ts());
        r.addProperty("maxExclusive", "FF");
        r.addProperty("minInclusive", "");
        r.addProperty("ridPrefix", 0);
        r.addProperty("throughputFraction", 1);
        r.addProperty("status", "online");
        r.add("parents", new JsonArray());
        Res res = list("PartitionKeyRanges", c.rid(), List.of(r));
        res.headers.put("etag", "\"00000000-0000-0000-0000-000000000000\"");
        String inm = q.h("if-none-match");
        if (inm != null && inm.equals("\"00000000-0000-0000-0000-000000000000\"")) {
            Res nm = new Res();
            nm.status = 304;
            nm.headers.put("etag", inm);
            return nm;
        }
        return res;
    }

    // ------------------------------------------------------------------------------------------ scripts (stored only)

    private Res scripts(Req q, Coll c, String kind, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        String key = kind.equals("sprocs") ? "StoredProcedures" : kind.equals("triggers") ? "Triggers" : "UserDefinedFunctions";
        if (s.size() == 5) {
            if (m.equals("GET")) {
                out.op = "List" + key;
                List<JsonElement> l = new ArrayList<>();
                store.listScripts(c, kind).forEach(x -> l.add(x.toJson(c)));
                return list(key, c.rid(), l);
            }
            if (m.equals("POST") && isQuery(q)) {
                out.op = "Query" + key;
                List<JsonObject> l = new ArrayList<>();
                store.listScripts(c, kind).forEach(x -> l.add(x.toJson(c)));
                return metaQuery(q, key, c.rid(), l);
            }
            if (m.equals("POST")) {
                out.op = "Create" + key;
                out.write = true;
                JsonObject b = q.json();
                String id = idOf(b);
                CosmosStore.Script sc = store.putScript(c, kind, id, scriptDef(b), true);
                return status(201, sc.toJson(c));
            }
            throw notAllowed(m, q);
        }
        String id = s.get(5);
        switch (m) {
            case "GET": {
                out.op = "Read" + key;
                CosmosStore.Script sc = store.getScript(c, kind, id);
                if (sc == null) {
                    throw CosmosStore.notFound();
                }
                Res r = ok(sc.toJson(c));
                r.headers.put("etag", sc.etag());
                return r;
            }
            case "PUT": {
                out.op = "Replace" + key;
                out.write = true;
                JsonObject b = q.json();
                if (!id.equals(idOf(b))) {
                    throw CosmosException.badRequest("The id in the body does not match the id in the URL.");
                }
                return ok(store.putScript(c, kind, id, scriptDef(b), false).toJson(c));
            }
            case "DELETE": {
                out.op = "Delete" + key;
                out.write = true;
                store.deleteScript(c, kind, id);
                Res r = new Res();
                r.status = 204;
                return r;
            }
            case "POST":
                if (kind.equals("sprocs")) {
                    out.op = "ExecuteStoredProcedure";
                    throw CosmosException.unsupported("Stored procedure execution is not supported by Warp cosmoswire: JavaScript is not executed. "
                            + "Stored procedures can be created, listed, read, replaced and deleted, but never run.");
                }
                throw notAllowed(m, q);
            default:
                throw notAllowed(m, q);
        }
    }

    private static JsonObject scriptDef(JsonObject b) {
        JsonObject d = b.deepCopy();
        for (String sys : new String[] {"_rid", "_self", "_etag", "_ts", "id"}) {
            d.remove(sys);
        }
        return d;
    }

    // ------------------------------------------------------------------------------------------ offers (throughput: stored and reported only)

    private List<JsonObject> offerList() {
        List<JsonObject> out = new ArrayList<>();
        for (DbRow d : store.listDbs()) {
            if (d.throughput() != null) {
                out.add(offer("OF" + d.rid(), "dbs/" + d.rid() + "/", d.rid(), d.throughput(), d.ts()));
            }
            for (Coll c : store.listColls(d.id())) {
                out.add(offer("OF" + c.rid(), c.selfLink(), c.rid(), c.throughput() != null ? c.throughput() : 400, c.ts()));
            }
        }
        return out;
    }

    private static JsonObject offer(String rid, String resource, String resourceRid, int throughput, long ts) {
        JsonObject o = new JsonObject();
        o.addProperty("offerVersion", "V2");
        o.addProperty("offerType", "Invalid");
        JsonObject content = new JsonObject();
        content.addProperty("offerThroughput", throughput);
        content.addProperty("offerIsRUPerMinuteThroughputEnabled", false);
        o.add("content", content);
        o.addProperty("resource", resource);
        o.addProperty("offerResourceId", resourceRid);
        o.addProperty("id", rid);
        o.addProperty("_rid", rid);
        o.addProperty("_self", "offers/" + rid + "/");
        o.addProperty("_etag", "\"00000000-0000-0000-0000-000000000000\"");
        o.addProperty("_ts", ts);
        return o;
    }

    private Res offers(Req q, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        if (s.size() == 1) {
            if (m.equals("GET")) {
                out.op = "ListOffers";
                return list("Offers", "", new ArrayList<>(offerList()));
            }
            if (m.equals("POST") && isQuery(q)) {
                out.op = "QueryOffers";
                return metaQuery(q, "Offers", "", offerList());
            }
            throw notAllowed(m, q);
        }
        JsonObject found = offerList().stream().filter(o -> o.get("_rid").getAsString().equals(s.get(1))).findFirst().orElse(null);
        if (found == null) {
            throw CosmosStore.notFound();
        }
        if (m.equals("GET")) {
            out.op = "ReadOffer";
            return ok(found);
        }
        if (m.equals("PUT")) {
            out.op = "ReplaceOffer";
            out.write = true;
            JsonObject b = q.json();
            JsonElement t = CosmosJson.path(b, "/content/offerThroughput");
            if (!CosmosJson.isInteger(t)) {
                throw CosmosException.badRequest("content.offerThroughput is required.");
            }
            int tp = (int) CosmosJson.dbl(t);
            String resRid = found.get("offerResourceId").getAsString();
            String res = found.get("resource").getAsString();
            if (res.contains("/colls/")) {
                for (DbRow d : store.listDbs()) {
                    for (Coll c : store.listColls(d.id())) {
                        if (c.rid().equals(resRid)) {
                            store.setCollThroughput(c.db(), c.id(), tp);
                        }
                    }
                }
            } else {
                for (DbRow d : store.listDbs()) {
                    if (d.rid().equals(resRid)) {
                        store.setDbThroughput(d.id(), tp);
                    }
                }
            }
            return ok(offer(found.get("_rid").getAsString(), res, resRid, tp, System.currentTimeMillis() / 1000));
        }
        throw notAllowed(m, q);
    }

    // ------------------------------------------------------------------------------------------ documents

    private static int maxItems(Req q, int dflt) {
        String v = q.h("x-ms-max-item-count");
        if (v == null || v.isBlank()) {
            return dflt;
        }
        try {
            int n = Integer.parseInt(v.trim());
            return n <= 0 ? 1000 : Math.min(n, 10000);
        } catch (NumberFormatException e) {
            throw CosmosException.badRequest("x-ms-max-item-count must be an integer.");
        }
    }

    private void noTriggers(Req q) {
        if (q.h("x-ms-documentdb-pre-trigger-include") != null || q.h("x-ms-documentdb-post-trigger-include") != null) {
            throw CosmosException.unsupported("Triggers are stored but never executed by Warp cosmoswire (JavaScript is not run); remove the "
                    + "pre/post trigger include headers.");
        }
    }

    private Res itemResponse(CosmosService.ItemResult r, Coll c) {
        Res res = r.doc() == null ? new Res() : ok(r.json());
        res.status = r.status();
        res.charge = r.charge();
        res.lsn = r.lsn();
        if (r.doc() != null) {
            res.headers.put("etag", r.doc().etag());
            res.headers.put("x-ms-alt-content-path", "dbs/" + c.db() + "/colls/" + c.id());
            res.headers.put("x-ms-content-path", c.rid());
        }
        return res;
    }

    private Res docs(Req q, Coll c, Outcome out) throws IOException {
        List<String> s = q.segs;
        String m = q.method;
        List<JsonElement> pk = CosmosPk.parseHeader(q.h("x-ms-documentdb-partitionkey"));
        String ifMatch = q.h("if-match");
        if (s.size() == 5 && (q.h("x-ms-start-epk") != null || q.h("x-ms-end-epk") != null) && !q.flag("x-ms-cosmos-is-batch-request")) {
            throw CosmosException.unsupported("Effective-partition-key range requests (feed ranges, hierarchical partition key prefix queries issued by "
                    + "the SDKs) are not supported by Warp cosmoswire; pass the partition key (or its prefix) in x-ms-documentdb-partitionkey instead.");
        }
        if (s.size() == 5) {
            if (m.equals("GET")) {
                String aim = q.h("a-im");
                if (aim != null && aim.toLowerCase(Locale.ROOT).contains("incremental feed")) {
                    out.op = "ReadChangeFeed";
                    return changeFeed(q, c, pk);
                }
                out.op = "ReadDocumentFeed";
                CosmosService.QueryPage p = svc.readFeed(c, pk, maxItems(q, 100), q.h("x-ms-continuation"));
                return queryResponse(p, c);
            }
            if (!m.equals("POST")) {
                throw notAllowed(m, q);
            }
            if (q.flag("x-ms-cosmos-is-batch-request")) {
                out.op = "ExecuteBatch";
                out.write = true;
                return batch(q, c, pk);
            }
            if (q.flag("x-ms-cosmos-is-query-plan-request")) {
                out.op = "QueryPlan";
                JsonObject b = q.json();
                CosmosQuery cq = compile(b);
                Res r = ok(plan(cq));
                r.charge = 0;
                return r;
            }
            if (isQuery(q)) {
                out.op = "QueryDocuments";
                JsonObject b = q.json();
                CosmosQuery cq = compile(b);
                if (planHandshake && pk == null && q.h("x-ms-documentdb-partitionkeyrangeid") == null && q.flag("x-ms-documentdb-query-enablecrosspartition")
                        && (cq.grouped || cq.ordered || cq.ast.distinct || cq.ast.top != null || cq.ast.offset != null || cq.equalities().isEmpty())) {
                    // what the real gateway does: hand the client a "first chance" error carrying the query plan
                    CosmosException e = new CosmosException(400, "BadRequest", 1004, "The provided cross partition query can not be directly served by "
                            + "the gateway. This is a first chance (internal) exception that all newer clients will know how to handle gracefully.");
                    Res r = error(e, "plan");
                    JsonObject body = JsonParser.parseString(r.body).getAsJsonObject();
                    body.addProperty("additionalErrorInfo", plan(cq).toString());
                    r.body = body.toString();
                    return r;
                }
                CosmosService.QueryPage p = svc.query(c, cq, pk, maxItems(q, 100), q.h("x-ms-continuation"));
                return queryResponse(p, c);
            }
            noTriggers(q);
            boolean upsert = q.flag("x-ms-documentdb-is-upsert");
            out.op = upsert ? "UpsertDocument" : "CreateDocument";
            out.write = true;
            return itemResponse(svc.create(c, pk, q.json(), upsert, ifMatch), c);
        }
        String id = s.get(5);
        switch (m) {
            case "GET": {
                out.op = "ReadDocument";
                Doc d = svc.read(c, pk, id);
                if (d == null) {
                    throw CosmosStore.notFound();
                }
                String inm = q.h("if-none-match");
                if (inm != null && (inm.equals(d.etag()) || inm.equals("*"))) {
                    Res r = new Res();
                    r.status = 304;
                    r.charge = 1.0;
                    r.headers.put("etag", d.etag());
                    return r;
                }
                return itemResponse(new CosmosService.ItemResult(200, d, d.toJson(c, false), svc.charge("read", d.body().toString().length(), 0)), c);
            }
            case "PUT":
                noTriggers(q);
                out.op = "ReplaceDocument";
                out.write = true;
                return itemResponse(svc.replace(c, pk, id, q.json(), ifMatch), c);
            case "DELETE":
                noTriggers(q);
                out.op = "DeleteDocument";
                out.write = true;
                return itemResponse(svc.delete(c, pk, id, ifMatch), c);
            case "PATCH":
                noTriggers(q);
                out.op = "PatchDocument";
                out.write = true;
                return itemResponse(svc.patch(c, pk, id, q.json(), ifMatch), c);
            default:
                throw notAllowed(m, q);
        }
    }

    private static CosmosQuery compile(JsonObject b) {
        return CosmosQuery.compile(b.has("query") && CosmosJson.isStr(b.get("query")) ? b.get("query").getAsString() : null,
                b.has("parameters") && b.get("parameters").isJsonArray() ? b.getAsJsonArray("parameters") : null);
    }

    private Res queryResponse(CosmosService.QueryPage p, Coll c) {
        JsonObject o = new JsonObject();
        o.addProperty("_rid", c.rid());
        JsonArray a = new JsonArray();
        p.rows().forEach(a::add);
        o.add("Documents", a);
        o.addProperty("_count", p.rows().size());
        Res r = ok(o);
        r.charge = p.charge();
        r.headers.put("x-ms-item-count", String.valueOf(p.rows().size()));
        if (p.continuation() != null) {
            r.headers.put("x-ms-continuation", p.continuation());
        }
        return r;
    }

    private Res changeFeed(Req q, Coll c, List<JsonElement> pk) {
        long since = 0;
        String ims = q.h("if-modified-since");
        if (ims != null) {
            try {
                since = java.time.ZonedDateTime.parse(ims, DateTimeFormatter.RFC_1123_DATE_TIME).toEpochSecond();
            } catch (RuntimeException e) {
                throw CosmosException.badRequest("If-Modified-Since is not a valid HTTP date.");
            }
        }
        CosmosService.FeedPage p = svc.changeFeed(c, pk, q.h("if-none-match"), since, maxItems(q, 100));
        if (!p.modified()) {
            Res r = new Res();
            r.status = 304;
            r.charge = p.charge();
            r.headers.put("etag", p.etag());
            return r;
        }
        JsonObject o = new JsonObject();
        o.addProperty("_rid", c.rid());
        JsonArray a = new JsonArray();
        p.docs().forEach(a::add);
        o.add("Documents", a);
        o.addProperty("_count", p.docs().size());
        Res r = ok(o);
        r.charge = p.charge();
        r.headers.put("etag", p.etag());
        r.headers.put("x-ms-item-count", String.valueOf(p.docs().size()));
        return r;
    }

    private Res batch(Req q, Coll c, List<JsonElement> pk) throws IOException {
        JsonElement b = q.jsonAny();
        if (!b.isJsonArray()) {
            throw CosmosException.badRequest("A transactional batch request body must be a JSON array of operations.");
        }
        CosmosService.BatchResult br = svc.batch(c, pk, b.getAsJsonArray());
        JsonArray arr = new JsonArray();
        double total = 0;
        for (CosmosService.BatchOp op : br.ops()) {
            JsonObject o = new JsonObject();
            o.addProperty("statusCode", op.status());
            o.addProperty("requestCharge", op.charge());
            if (op.etag() != null) {
                o.addProperty("eTag", op.etag());
            }
            if (op.body() != null) {
                o.add("resourceBody", op.body());
            }
            if (op.message() != null) {
                o.addProperty("message", op.message());
            }
            total += op.charge();
            arr.add(o);
        }
        Res r = status(br.ok() ? 200 : 207, arr);
        r.charge = total;
        return r;
    }

    /** The query plan answer: every clause is already handled on the server (one partition key range), so queryInfo is empty. */
    static JsonObject plan(CosmosQuery cq) {
        JsonObject qi = new JsonObject();
        qi.addProperty("distinctType", "None");
        qi.add("top", com.google.gson.JsonNull.INSTANCE);
        qi.add("offset", com.google.gson.JsonNull.INSTANCE);
        qi.add("limit", com.google.gson.JsonNull.INSTANCE);
        qi.add("orderBy", new JsonArray());
        qi.add("orderByExpressions", new JsonArray());
        qi.add("groupByExpressions", new JsonArray());
        qi.add("groupByAliases", new JsonArray());
        qi.add("aggregates", new JsonArray());
        qi.add("groupByAliasToAggregateType", new JsonObject());
        qi.addProperty("rewrittenQuery", "");
        qi.addProperty("hasSelectValue", cq.ast.value);
        qi.add("dCountInfo", com.google.gson.JsonNull.INSTANCE);
        qi.addProperty("hasNonStreamingOrderBy", false);
        JsonObject range = new JsonObject();
        range.addProperty("min", "");
        range.addProperty("max", "FF");
        range.addProperty("isMinInclusive", true);
        range.addProperty("isMaxInclusive", false);
        JsonArray ranges = new JsonArray();
        ranges.add(range);
        JsonObject o = new JsonObject();
        o.addProperty("partitionedQueryExecutionInfoVersion", 2);
        o.add("queryInfo", qi);
        o.add("queryRanges", ranges);
        return o;
    }
}
