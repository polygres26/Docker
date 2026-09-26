package com.sayonora.warp.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.cosmoswire.CosmosStore.Coll;
import com.sayonora.warp.cosmoswire.CosmosStore.Doc;
import com.sayonora.warp.cosmoswire.CosmosStore.Filter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Document operations of cosmoswire on top of {@link CosmosStore}: item CRUD/patch, transactional batch, SQL queries with
 * scatter-gather over the shards and continuation tokens, and the change feed. Shared by the HTTP frontend and {@link CosmosEmbedded}.
 */
final class CosmosService {

    static final int MAX_DOC_BYTES = 2 * 1024 * 1024;
    static final int MAX_BATCH_OPS = 100;

    private final CosmosStore store;
    private final CosmosConfig cfg;
    private final long maxMaterialize;

    CosmosService(CosmosStore store, CosmosConfig cfg) {
        this.store = store;
        this.cfg = cfg;
        String v = System.getenv("WARP_COSMOSWIRE_MAX_MATERIALIZE_DOCS");
        this.maxMaterialize = v == null || v.isBlank() ? 1_000_000L : Long.parseLong(v.trim());
    }

    CosmosStore store() {
        return store;
    }

    // ------------------------------------------------------------------------------------------ request charge (synthetic)

    double charge(String kind, long bytes, long scanned) {
        if (cfg.fixedRu != null) {
            return cfg.fixedRu;
        }
        double kb = Math.max(1.0, bytes / 1024.0);
        double ru = switch (kind) {
            case "read" -> 1.0 * kb;
            case "create", "upsert" -> 5.71 * kb;
            case "replace", "patch" -> 10.67 * kb;
            case "delete" -> 5.71 * kb;
            case "query" -> 2.83 + 0.05 * scanned + 0.5 * (bytes / 1024.0);
            default -> 1.0;
        };
        return Math.round(ru * 100.0) / 100.0;
    }

    // ------------------------------------------------------------------------------------------ helpers

    static Integer ttlOf(JsonObject body) {
        JsonElement t = body.get("ttl");
        if (t == null || !CosmosJson.isInteger(t)) {
            return null;
        }
        long v = (long) CosmosJson.dbl(t);
        return v == -1 || v > 0 && v <= Integer.MAX_VALUE ? (int) v : null;
    }

    /** Validates an incoming document and strips system properties. */
    static JsonObject prepare(JsonObject in) {
        JsonElement id = in.get("id");
        if (id == null || !CosmosJson.isStr(id)) {
            throw CosmosException.badRequest("The input content is invalid because the required properties - 'id; ' - are missing");
        }
        String s = id.getAsString();
        if (s.isEmpty() || s.length() > 255 || s.indexOf('/') >= 0 || s.indexOf('\\') >= 0 || s.indexOf('?') >= 0 || s.indexOf('#') >= 0) {
            throw CosmosException.badRequest("The specified document id is invalid: it must be 1 to 255 characters and must not contain '/', '\\', '?' or '#'.");
        }
        JsonObject body = in.deepCopy();
        for (String sys : new String[] {"_rid", "_self", "_etag", "_attachments", "_ts", "_lsn"}) {
            body.remove(sys);
        }
        JsonElement ttl = body.get("ttl");
        if (ttl != null && ttlOf(body) == null) {
            throw CosmosException.badRequest("The 'ttl' property must be -1 or a positive integer number of seconds.");
        }
        if (body.toString().getBytes(StandardCharsets.UTF_8).length > MAX_DOC_BYTES) {
            throw new CosmosException(413, "RequestEntityTooLarge", "Request size is too large. Documents are limited to 2 MB.");
        }
        return body;
    }

    /** The partition key of a document write: extracted from the body and checked against the header when one is given. */
    List<JsonElement> writeKey(Coll cl, List<JsonElement> header, JsonObject body) {
        List<JsonElement> pk = CosmosPk.extract(cl.pkPaths(), body);
        if (header != null) {
            if (header.size() != cl.pkPaths().size()) {
                throw CosmosException.badRequest("The partition key supplied in x-ms-partitionkey header has fewer components than defined in the "
                        + "the collection.");
            }
            if (!CosmosPk.same(header, pk)) {
                throw CosmosException.badRequest("The partition key supplied in x-ms-partitionkey header does not match the value extracted from the "
                        + "document: header " + CosmosPk.describe(header) + ", document " + CosmosPk.describe(pk) + ".");
            }
        }
        return pk;
    }

    List<JsonElement> requireKey(Coll cl, List<JsonElement> header) {
        if (header == null || header.size() != cl.pkPaths().size()) {
            throw CosmosException.badRequest("The partition key supplied in x-ms-partitionkey header has fewer components than defined in the the "
                    + "collection.");
        }
        return header;
    }

    // ------------------------------------------------------------------------------------------ item operations

    record ItemResult(int status, Doc doc, JsonObject json, double charge) {

        String etag() {
            return doc == null ? null : doc.etag();
        }

        long lsn() {
            return doc == null ? 0 : doc.lsn();
        }
    }

    ItemResult create(Coll cl, List<JsonElement> hdr, JsonObject in, boolean upsert, String ifMatch) {
        JsonObject body = prepare(in);
        List<JsonElement> pk = writeKey(cl, hdr, body);
        String pkh = CosmosPk.canon(pk);
        int hi = store.ownerIndex(cl.db(), cl.id(), pk);
        String host = store.hosts().get(hi);
        Integer ttl = ttlOf(body);
        if (upsert) {
            CosmosStore.Upserted u = store.tx(host, c -> store.upsert(c, cl, hi, pkh, body, ttl, ifMatch));
            return new ItemResult(u.created() ? 201 : 200, u.doc(), u.doc().toJson(cl, false), charge("upsert", size(body), 0));
        }
        Doc d = store.tx(host, c -> store.create(c, cl, hi, pkh, body, ttl));
        return new ItemResult(201, d, d.toJson(cl, false), charge("create", size(body), 0));
    }

    private static long size(JsonObject o) {
        return o.toString().length();
    }

    /** Point read; null when absent. */
    Doc read(Coll cl, List<JsonElement> hdr, String id) {
        List<JsonElement> pk = requireKey(cl, hdr);
        String pkh = CosmosPk.canon(pk);
        String host = store.hosts().get(store.ownerIndex(cl.db(), cl.id(), pk));
        return store.conn(host, c -> CosmosStore.read(c, cl, pkh, id, false));
    }

    ItemResult replace(Coll cl, List<JsonElement> hdr, String id, JsonObject in, String ifMatch) {
        JsonObject body = prepare(in);
        if (!id.equals(body.get("id").getAsString())) {
            throw CosmosException.badRequest("The document id in the request URL does not match the id in the document body.");
        }
        List<JsonElement> pk = writeKey(cl, requireKey(cl, hdr), body);
        String pkh = CosmosPk.canon(pk);
        String host = store.hosts().get(store.ownerIndex(cl.db(), cl.id(), pk));
        Integer ttl = ttlOf(body);
        Doc d = store.tx(host, c -> store.replace(c, cl, pkh, body, ttl, ifMatch));
        return new ItemResult(200, d, d.toJson(cl, false), charge("replace", size(body), 0));
    }

    ItemResult delete(Coll cl, List<JsonElement> hdr, String id, String ifMatch) {
        List<JsonElement> pk = requireKey(cl, hdr);
        String pkh = CosmosPk.canon(pk);
        String host = store.hosts().get(store.ownerIndex(cl.db(), cl.id(), pk));
        store.tx(host, c -> {
            store.delete(c, cl, pkh, id, ifMatch);
            return 0;
        });
        return new ItemResult(204, null, null, charge("delete", 0, 0));
    }

    ItemResult patch(Coll cl, List<JsonElement> hdr, String id, JsonObject req, String ifMatch) {
        List<JsonElement> pk = requireKey(cl, hdr);
        String pkh = CosmosPk.canon(pk);
        int hi = store.ownerIndex(cl.db(), cl.id(), pk);
        String host = store.hosts().get(hi);
        Doc d = store.tx(host, c -> store.patch(c, cl, pkh, id, ifMatch, cur -> patched(cl, pk, cur, req)));
        return new ItemResult(200, d, d.toJson(cl, false), charge("patch", size(d.body()), 0));
    }

    private JsonObject patched(Coll cl, List<JsonElement> pk, Doc cur, JsonObject req) {
        JsonElement ops = req.get("operations");
        if (ops == null || !ops.isJsonArray()) {
            throw CosmosException.badRequest("The patch request body must contain an 'operations' array.");
        }
        if (req.has("condition") && CosmosJson.isStr(req.get("condition"))) {
            String cond = req.get("condition").getAsString().trim();
            String sql = cond.regionMatches(true, 0, "from", 0, 4) ? "SELECT * " + cond : "SELECT * FROM c WHERE " + cond;
            CosmosQuery q = CosmosQuery.compile(sql, null);
            List<JsonElement> r = q.runAll(List.of(cur.toJson(cl, false)));
            if (r.isEmpty()) {
                throw CosmosException.precondition("The patch condition was not met.");
            }
        }
        for (JsonElement o : ops.getAsJsonArray()) {
            if (o.isJsonObject() && o.getAsJsonObject().has("path")) {
                String p = o.getAsJsonObject().get("path").getAsString();
                String top = p.startsWith("/") ? p.substring(1).split("/", -1)[0] : p;
                if (top.equals("id") || top.startsWith("_") && !top.equals("_") && isSystem(top)) {
                    throw CosmosException.badRequest("Patch cannot modify the system property or id at path '" + p + "'.");
                }
                for (String pp : cl.pkPaths()) {
                    if (pp.equals(p) || p.startsWith(pp + "/")) {
                        throw CosmosException.badRequest("Patch cannot modify the partition key path '" + pp + "'.");
                    }
                }
            }
        }
        JsonObject nb = CosmosPatch.apply(cur.body(), ops.getAsJsonArray());
        return prepare(nb);
    }

    private static boolean isSystem(String p) {
        return p.equals("_rid") || p.equals("_self") || p.equals("_etag") || p.equals("_ts") || p.equals("_attachments") || p.equals("_lsn");
    }

    // ------------------------------------------------------------------------------------------ transactional batch

    record BatchOp(int status, JsonObject body, String etag, double charge, String message) {
    }

    /** Thrown inside the transaction to roll it back at operation {@code index}. */
    private static final class BatchFail extends RuntimeException {
        final int index;
        final CosmosException cause;

        BatchFail(int index, CosmosException cause) {
            super(cause.getMessage(), null, false, false);
            this.index = index;
            this.cause = cause;
        }
    }

    /** Runs every operation in one transaction on the partition's host. Returns the per-operation results and whether all succeeded. */
    record BatchResult(List<BatchOp> ops, boolean ok) {
    }

    BatchResult batch(Coll cl, List<JsonElement> hdr, JsonArray ops) {
        if (ops.isEmpty()) {
            throw CosmosException.badRequest("The transactional batch request must contain at least one operation.");
        }
        if (ops.size() > MAX_BATCH_OPS) {
            throw CosmosException.badRequest("The transactional batch request has too many operations: at most " + MAX_BATCH_OPS + " are allowed.");
        }
        List<JsonElement> pk = requireKey(cl, hdr);
        String pkh = CosmosPk.canon(pk);
        int hi = store.ownerIndex(cl.db(), cl.id(), pk);
        String host = store.hosts().get(hi);
        List<BatchOp> results = new ArrayList<>();
        try {
            store.tx(host, c -> {
                for (int i = 0; i < ops.size(); i++) {
                    try {
                        results.add(batchOne(c, cl, hi, pk, pkh, ops.get(i)));
                    } catch (CosmosException e) {
                        throw new BatchFail(i, e);
                    } catch (SQLException e) {
                        throw e;
                    }
                }
                return 0;
            });
            return new BatchResult(results, true);
        } catch (BatchFail f) {
            List<BatchOp> out = new ArrayList<>();
            for (int i = 0; i < ops.size(); i++) {
                if (i == f.index) {
                    out.add(new BatchOp(f.cause.status, null, null, 0, f.cause.getMessage()));
                } else {
                    out.add(new BatchOp(424, null, null, 0, null)); // Failed Dependency
                }
            }
            return new BatchResult(out, false);
        }
    }

    private BatchOp batchOne(java.sql.Connection c, Coll cl, int hi, List<JsonElement> pk, String pkh, JsonElement opEl) throws SQLException {
        if (!opEl.isJsonObject()) {
            throw CosmosException.badRequest("Each batch operation must be an object.");
        }
        JsonObject op = opEl.getAsJsonObject();
        String type = op.has("operationType") ? op.get("operationType").getAsString().toLowerCase(java.util.Locale.ROOT) : "";
        String id = op.has("id") ? op.get("id").getAsString() : null;
        String ifMatch = op.has("ifMatch") ? op.get("ifMatch").getAsString() : null;
        JsonObject rb = op.has("resourceBody") && op.get("resourceBody").isJsonObject() ? op.getAsJsonObject("resourceBody") : null;
        switch (type) {
            case "create", "upsert": {
                if (rb == null) {
                    throw CosmosException.badRequest("The " + type + " operation needs a resourceBody.");
                }
                JsonObject body = prepare(rb);
                writeKey(cl, pk, body);
                Integer ttl = ttlOf(body);
                if (type.equals("create")) {
                    Doc d = store.create(c, cl, hi, pkh, body, ttl);
                    return new BatchOp(201, d.toJson(cl, false), d.etag(), charge("create", size(body), 0), null);
                }
                CosmosStore.Upserted u = store.upsert(c, cl, hi, pkh, body, ttl, ifMatch);
                return new BatchOp(u.created() ? 201 : 200, u.doc().toJson(cl, false), u.doc().etag(), charge("upsert", size(body), 0), null);
            }
            case "replace": {
                if (rb == null || id == null) {
                    throw CosmosException.badRequest("The replace operation needs an id and a resourceBody.");
                }
                JsonObject body = prepare(rb);
                if (!id.equals(body.get("id").getAsString())) {
                    throw CosmosException.badRequest("The operation id does not match the id in the document body.");
                }
                writeKey(cl, pk, body);
                Doc d = store.replace(c, cl, pkh, body, ttlOf(body), ifMatch);
                return new BatchOp(200, d.toJson(cl, false), d.etag(), charge("replace", size(body), 0), null);
            }
            case "read": {
                if (id == null) {
                    throw CosmosException.badRequest("The read operation needs an id.");
                }
                Doc d = CosmosStore.read(c, cl, pkh, id, false);
                if (d == null) {
                    throw CosmosStore.notFound();
                }
                return new BatchOp(200, d.toJson(cl, false), d.etag(), charge("read", size(d.body()), 0), null);
            }
            case "delete": {
                if (id == null) {
                    throw CosmosException.badRequest("The delete operation needs an id.");
                }
                store.delete(c, cl, pkh, id, ifMatch);
                return new BatchOp(204, null, null, charge("delete", 0, 0), null);
            }
            case "patch": {
                if (id == null || rb == null) {
                    throw CosmosException.badRequest("The patch operation needs an id and a resourceBody.");
                }
                Doc d = store.patch(c, cl, pkh, id, ifMatch, cur -> patched(cl, pk, cur, rb));
                return new BatchOp(200, d.toJson(cl, false), d.etag(), charge("patch", size(d.body()), 0), null);
            }
            default:
                throw CosmosException.badRequest("Unknown batch operationType '" + type + "'.");
        }
    }

    // ------------------------------------------------------------------------------------------ queries

    record QueryPage(List<JsonElement> rows, String continuation, double charge, long scanned) {
    }

    static String encode(JsonObject o) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(o.toString().getBytes(StandardCharsets.UTF_8));
    }

    static JsonObject decode(String tok) {
        try {
            return JsonParser.parseString(new String(Base64.getUrlDecoder().decode(tok), StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (RuntimeException e) {
            throw CosmosException.badRequest("The continuation token is not valid.");
        }
    }

    /** Results of materialized queries kept for the pages that follow (a snapshot; a miss just re-executes). */
    private final Map<String, CachedResult> resultCache = new ConcurrentHashMap<>();

    private record CachedResult(List<JsonElement> rows, long at, long scanned) {
    }

    private static final long RESULT_TTL_MS = 60_000;

    private static final class Scope {
        List<String> hosts = new ArrayList<>();
        List<Integer> hostIdx = new ArrayList<>();
        Filter filter = Filter.NONE;
    }

    /** Which hosts and which partition/id filter a query needs. */
    private Scope scopeOf(Coll cl, CosmosQuery q, List<JsonElement> hdr) {
        Scope s = new Scope();
        List<String> all = store.hosts();
        List<String> paths = cl.pkPaths();
        String pkh = null;
        String prefix = null;
        Integer only = null;
        if (hdr != null) {
            if (hdr.size() == paths.size()) {
                pkh = CosmosPk.canon(hdr);
                only = store.ownerIndex(cl.db(), cl.id(), hdr);
            } else if (hdr.size() < paths.size() && cl.hierarchical()) {
                prefix = CosmosPk.prefix(hdr);
                only = store.ownerIndex(cl.db(), cl.id(), hdr);
            } else {
                throw CosmosException.badRequest("The partition key supplied in x-ms-partitionkey header has more components than defined in the "
                        + "collection.");
            }
        } else {
            Map<String, JsonElement> eq = q.equalities();
            List<JsonElement> vals = new ArrayList<>();
            for (String p : paths) {
                if (eq.containsKey(p)) {
                    vals.add(eq.get(p));
                } else {
                    break;
                }
            }
            if (vals.size() == paths.size()) {
                pkh = CosmosPk.canon(vals);
                only = store.ownerIndex(cl.db(), cl.id(), vals);
            } else if (!vals.isEmpty()) {
                only = store.ownerIndex(cl.db(), cl.id(), vals.subList(0, 1));
            }
        }
        String idEq = null;
        if (hdr == null || true) {
            JsonElement idv = q.equalities().get("/id");
            if (idv != null && CosmosJson.isStr(idv)) {
                idEq = idv.getAsString();
            }
        }
        s.filter = new Filter(pkh, prefix, idEq);
        if (only != null) {
            s.hosts.add(all.get(only));
            s.hostIdx.add(only);
        } else {
            for (int i = 0; i < all.size(); i++) {
                s.hosts.add(all.get(i));
                s.hostIdx.add(i);
            }
        }
        return s;
    }

    QueryPage query(Coll cl, CosmosQuery q, List<JsonElement> hdr, int maxItems, String continuation) {
        Scope sc = scopeOf(cl, q, hdr);
        JsonObject tok = continuation == null || continuation.isBlank() ? null : decode(continuation);
        if (q.streamable) {
            return streaming(cl, q, sc, maxItems, tok);
        }
        return materialized(cl, q, sc, hdr, maxItems, tok);
    }

    private QueryPage streaming(Coll cl, CosmosQuery q, Scope sc, int maxItems, JsonObject tok) {
        long off = tok != null && tok.has("off") ? tok.get("off").getAsLong() : q.offsetValue();
        long lim = tok != null && tok.has("lim") ? tok.get("lim").getAsLong()
                : q.topValue() >= 0 ? q.topValue() : q.limitValue();
        List<JsonElement> out = new ArrayList<>();
        long scanned = 0;
        long bytes = 0;
        int startHost = tok != null && tok.has("h") ? tok.get("h").getAsInt() : 0;
        String afterPkh = tok != null && tok.has("p") ? tok.get("p").getAsString() : null;
        String afterId = tok != null && tok.has("i") ? tok.get("i").getAsString() : null;
        int skipSub = tok != null && tok.has("s") ? tok.get("s").getAsInt() : 0;
        boolean inclusive = tok != null && tok.has("inc") && tok.get("inc").getAsBoolean();
        if (lim == 0) {
            return new QueryPage(out, null, charge("query", 0, 0), 0);
        }
        for (int hpos = Math.min(startHost, sc.hosts.size()); hpos < sc.hosts.size(); hpos++) {
            String host = sc.hosts.get(hpos);
            boolean first = true;
            while (true) {
                List<Doc> page = store.scan(host, cl, sc.filter, afterPkh, afterId, first && inclusive, CosmosStore.PAGE);
                first = false;
                for (Doc d : page) {
                    afterPkh = d.pkh();
                    afterId = d.id();
                    if (d.body() == null) {
                        continue;
                    }
                    scanned++;
                    List<JsonElement> rows = new ArrayList<>(1);
                    q.streamDoc(d.toJson(cl, false), rows::add);
                    int startJ = resumeMatches(tok, d) ? skipSub : 0;
                    for (int j = startJ; j < rows.size(); j++) {
                        if (off > 0) {
                            off--;
                            continue;
                        }
                        JsonElement row = rows.get(j);
                        out.add(row);
                        bytes += row.toString().length();
                        if (lim > 0) {
                            lim--;
                        }
                        if (lim == 0) {
                            return new QueryPage(out, null, charge("query", bytes, scanned), scanned);
                        }
                        if (out.size() >= maxItems || bytes > 3_500_000) {
                            JsonObject t = new JsonObject();
                            t.addProperty("h", hpos);
                            t.addProperty("p", d.pkh());
                            t.addProperty("i", d.id());
                            t.addProperty("s", j + 1);
                            t.addProperty("inc", true);
                            t.addProperty("off", off);
                            t.addProperty("lim", lim);
                            if (j + 1 >= rows.size()) {
                                t.addProperty("s", 0);
                                t.addProperty("inc", false);
                            }
                            return new QueryPage(out, encode(t), charge("query", bytes, scanned), scanned);
                        }
                    }
                }
                if (page.size() < CosmosStore.PAGE) {
                    break;
                }
            }
            afterPkh = null;
            afterId = null;
            skipSub = 0;
            inclusive = false;
        }
        return new QueryPage(out, null, charge("query", bytes, scanned), scanned);
    }

    private static boolean resumeMatches(JsonObject tok, Doc d) {
        return tok != null && tok.has("inc") && tok.get("inc").getAsBoolean() && tok.has("p") && tok.get("p").getAsString().equals(d.pkh())
                && tok.get("i").getAsString().equals(d.id());
    }

    private QueryPage materialized(Coll cl, CosmosQuery q, Scope sc, List<JsonElement> hdr, int maxItems, JsonObject tok) {
        String cacheKey = tok != null && tok.has("k") ? tok.get("k").getAsString() : null;
        long off = tok != null && tok.has("o") ? tok.get("o").getAsLong() : 0;
        CachedResult cached = cacheKey == null ? null : resultCache.get(cacheKey);
        List<JsonElement> all;
        long scanned;
        if (cached != null && System.currentTimeMillis() - cached.at() < RESULT_TTL_MS) {
            all = cached.rows();
            scanned = cached.scanned();
        } else {
            resultCache.values().removeIf(r -> System.currentTimeMillis() - r.at() > RESULT_TTL_MS);
            if (q.isPlainCount() && !cl.ttlEnabled() && sc.filter.pkh() == null && sc.filter.pkhPrefix() == null && sc.filter.id() == null) {
                long n = 0;
                for (String h : sc.hosts) {
                    n += store.count(h, cl);
                }
                all = List.of(CosmosJson.num(n));
                scanned = n;
            } else {
                CosmosQuery.Pipeline p = q.pipeline();
                long[] n = {0};
                for (String host : sc.hosts) {
                    String ap = null;
                    String ai = null;
                    while (true) {
                        List<Doc> page = store.scan(host, cl, sc.filter, ap, ai, false, CosmosStore.PAGE);
                        for (Doc d : page) {
                            ap = d.pkh();
                            ai = d.id();
                            if (d.body() == null) {
                                continue;
                            }
                            if (++n[0] > maxMaterialize) {
                                throw CosmosException.badRequest("The query needs more than " + maxMaterialize
                                        + " documents in memory (ORDER BY / GROUP BY / aggregate / DISTINCT across partitions); add a partition key "
                                        + "filter or raise WARP_COSMOSWIRE_MAX_MATERIALIZE_DOCS.");
                            }
                            p.add(d.toJson(cl, false));
                        }
                        if (page.size() < CosmosStore.PAGE) {
                            break;
                        }
                    }
                }
                all = p.finish();
                scanned = n[0];
            }
            if (all.size() > maxItems) {
                cacheKey = java.util.UUID.randomUUID().toString();
                resultCache.put(cacheKey, new CachedResult(all, System.currentTimeMillis(), scanned));
                if (resultCache.size() > 64) {
                    resultCache.entrySet().stream().min(Comparator.comparingLong(e -> e.getValue().at())).ifPresent(e -> resultCache.remove(e.getKey()));
                }
            }
        }
        int from = (int) Math.min(off, all.size());
        int to = (int) Math.min((long) all.size(), from + (long) maxItems);
        List<JsonElement> page = new ArrayList<>(all.subList(from, to));
        String cont = null;
        if (to < all.size()) {
            JsonObject t = new JsonObject();
            t.addProperty("o", to);
            if (cacheKey != null) {
                t.addProperty("k", cacheKey);
            }
            cont = encode(t);
        }
        long bytes = 0;
        for (JsonElement e : page) {
            bytes += e.toString().length();
        }
        return new QueryPage(page, cont, charge("query", bytes, scanned), scanned);
    }

    /** Lists documents of a container (the read feed): pages of a full scan in (partition, id) order. */
    QueryPage readFeed(Coll cl, List<JsonElement> hdr, int maxItems, String continuation) {
        CosmosQuery q = CosmosQuery.compile("SELECT * FROM c", null);
        return query(cl, q, hdr, maxItems, continuation);
    }

    // ------------------------------------------------------------------------------------------ change feed

    record FeedPage(List<JsonObject> docs, String etag, boolean modified, double charge) {
    }

    /**
     * Incremental change feed: the latest version of every document changed after the continuation, per host in _lsn order (order across
     * partition keys of different hosts is by _lsn, then host). {@code ifNoneMatch} is the continuation ({@code *} = from now).
     */
    FeedPage changeFeed(Coll cl, List<JsonElement> hdr, String ifNoneMatch, long modifiedSinceSec, int maxItems) {
        List<String> hosts = store.hosts();
        JsonObject positions = new JsonObject();
        boolean fromNow = "*".equals(ifNoneMatch != null ? ifNoneMatch.trim() : null);
        if (ifNoneMatch != null && !fromNow) {
            String t = ifNoneMatch.trim();
            if (t.startsWith("W/")) {
                t = t.substring(2);
            }
            if (t.startsWith("\"") && t.endsWith("\"") && t.length() >= 2) {
                t = t.substring(1, t.length() - 1);
            }
            JsonObject o = decode(t);
            positions = o.has("l") ? o.getAsJsonObject("l") : new JsonObject();
        }
        if (fromNow) {
            for (String h : hosts) {
                positions.addProperty(h, store.maxLsn(h, cl));
            }
            return new FeedPage(List.of(), etagOf(positions), false, charge("read", 0, 0));
        }
        String pkh = null;
        List<String> targets = hosts;
        if (hdr != null && hdr.size() == cl.pkPaths().size()) {
            pkh = CosmosPk.canon(hdr);
            targets = List.of(hosts.get(store.ownerIndex(cl.db(), cl.id(), hdr)));
        }
        record Ch(int host, Doc d) {
        }
        List<Ch> merged = new ArrayList<>();
        for (String h : targets) {
            long after = positions.has(h) ? positions.get(h).getAsLong() : 0;
            for (Doc d : store.feed(h, cl, after, pkh, modifiedSinceSec, maxItems)) {
                merged.add(new Ch(hosts.indexOf(h), d));
            }
        }
        merged.sort(Comparator.comparingLong((Ch c) -> c.d().lsn()).thenComparingInt(Ch::host));
        List<Ch> take = merged.size() > maxItems ? merged.subList(0, maxItems) : merged;
        JsonObject np = positions.deepCopy();
        List<JsonObject> docs = new ArrayList<>();
        for (Ch c : take) {
            String h = hosts.get(c.host());
            long cur = np.has(h) ? np.get(h).getAsLong() : 0;
            np.addProperty(h, Math.max(cur, c.d().lsn()));
            docs.add(c.d().toJson(cl, true));
        }
        if (docs.isEmpty()) {
            return new FeedPage(docs, etagOf(positions), false, charge("read", 0, 0));
        }
        return new FeedPage(docs, etagOf(np), true, charge("read", docs.size() * 1024L, docs.size()));
    }

    private static String etagOf(JsonObject positions) {
        JsonObject o = new JsonObject();
        o.add("l", positions);
        return "\"" + encode(o) + "\"";
    }

    /** Sorted by name for deterministic output. */
    static Map<String, JsonElement> sorted(JsonObject o) {
        Map<String, JsonElement> m = new LinkedHashMap<>();
        o.keySet().stream().sorted().forEach(k -> m.put(k, o.get(k)));
        return m;
    }
}
