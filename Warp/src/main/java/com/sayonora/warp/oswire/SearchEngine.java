package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes {@code _search} / {@code _count} / {@code _msearch} / scroll / point-in-time over the documents of every
 * host of the addressed indices: loads the documents, evaluates the query tree per document (BM25 scores per host,
 * like per-shard scoring), sorts, applies {@code search_after}/{@code from}/{@code size}, runs aggregations over the
 * union of all matches (exact across hosts), and renders the OpenSearch response.
 */
final class SearchEngine {

    private static final int DEFAULT_TRACK_TOTAL = 10_000;

    /** True when the query result cannot depend on corpus statistics or on documents the query itself does not match. */
    private static boolean hasSetLevelQuery(Query.Node n) {
        if (n instanceof Vector.Knn || n instanceof Vector.Hybrid) {
            return true;
        }
        for (Query.Node c : n.children()) {
            if (hasSetLevelQuery(c)) {
                return true;
            }
        }
        return false;
    }

    private static boolean prefilterable(Req req) {
        if (req.minScore != null || hasSetLevelQuery(req.query) || (req.postFilter != null && hasSetLevelQuery(req.postFilter))) {
            return false;
        }
        for (Aggs.Spec a : req.aggs) {
            if (usesGlobal(a)) {
                return false;
            }
        }
        boolean scoresOutput = req.size > 0 && (!req.hasSort || req.trackScores || sortsOnScore(req));
        return !scoresOutput;
    }

    private static boolean usesGlobal(Aggs.Spec a) {
        if (a.type.equals("global")) {
            return true;
        }
        for (Aggs.Spec s : a.subs) {
            if (usesGlobal(s)) {
                return true;
            }
        }
        return false;
    }

    private final PostgresSearchStore store;
    private final ConcurrentHashMap<String, Scroll> scrolls = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Pit> pits = new ConcurrentHashMap<>();

    SearchEngine(PostgresSearchStore store) {
        this.store = store;
    }

    static final class Scroll {
        final List<JsonObject> hits;
        final long total;
        final int size;
        int next;
        long expiresAt;
        final JsonObject shards;

        Scroll(List<JsonObject> hits, long total, int size, long expiresAt, JsonObject shards) {
            this.hits = hits;
            this.total = total;
            this.size = size;
            this.expiresAt = expiresAt;
            this.shards = shards;
        }
    }

    static final class Pit {
        final List<String> indices;
        final long created;
        long expiresAt;

        Pit(List<String> indices, long expiresAt) {
            this.indices = indices;
            this.created = System.currentTimeMillis();
            this.expiresAt = expiresAt;
        }
    }

    // ------------------------------------------------------------------ request model

    static final class SortSpec {
        String field;
        boolean asc = true;
        String mode;
        Object missing; // "_last" / "_first" / literal
        String unmappedType;
        String nestedPath;
        Query.Node nestedFilter;
        String format;
        boolean numeric;
        boolean intLike;
    }

    static final class Req {
        Query.Node query = new Query.MatchAll();
        Query.Node postFilter;
        int size = 10;
        int from = 0;
        List<SortSpec> sort = new ArrayList<>();
        JsonElement source; // true/false/string/array/object
        List<String> includes = null, excludes = null;
        boolean sourceOff;
        JsonElement storedFields, docvalueFields, fields;
        long trackTotal = DEFAULT_TRACK_TOTAL; // -1 = exact, 0 = disabled (value omitted)
        boolean trackTotalExact;
        boolean trackTotalOff;
        boolean trackScores;
        Double minScore;
        int terminateAfter;
        boolean version, seqNoPrimaryTerm, explain;
        JsonObject highlight;
        JsonArray searchAfter;
        List<Aggs.Spec> aggs = List.of();
        JsonObject collapse;
        String scroll;
        boolean typedKeys;
        boolean totalAsInt;
        boolean namedScores;
        String searchType;
        JsonObject pit;
        List<Map.Entry<String, Double>> indicesBoost;
        boolean hasSort;
        List<String> suggest;
    }

    private static final Set<String> KNOWN_KEYS = Set.of("query", "post_filter", "size", "from", "sort", "_source", "stored_fields",
            "docvalue_fields", "fields", "track_total_hits", "track_scores", "min_score", "terminate_after", "version", "seq_no_primary_term",
            "explain", "highlight", "search_after", "aggs", "aggregations", "collapse", "timeout", "pit", "indices_boost", "slice", "profile",
            "stats", "ext", "_name", "runtime_mappings", "script_fields", "rescore", "suggest", "knn", "search_type", "verbose", "batched_reduce_size",
            "allow_partial_search_results", "request_cache", "preference", "routing", "search_pipeline", "derived", "max_concurrent_shard_requests");

    static Req parse(JsonObject body, Map<String, String> params) {
        Req r = new Req();
        for (String k : body.keySet()) {
            if (!KNOWN_KEYS.contains(k)) {
                throw new OpenSearchException("parsing_exception", "Unknown key for a " + kind(body.get(k)) + " in [" + k + "].");
            }
        }
        if (body.has("rescore")) {
            throw new OpenSearchException("parsing_exception", "[rescore] is not supported by Warp's OpenSearch frontend");
        }
        if (body.has("script_fields")) {
            throw new OpenSearchException("parsing_exception", "[script_fields] is not supported by Warp's OpenSearch frontend");
        }
        if (body.has("suggest")) {
            throw new OpenSearchException("parsing_exception", "[suggest] is not supported by Warp's OpenSearch frontend");
        }
        if (body.has("runtime_mappings") || body.has("derived")) {
            throw new OpenSearchException("parsing_exception", "[runtime_mappings] is not supported by Warp's OpenSearch frontend");
        }
        if (body.has("profile") && body.get("profile").getAsBoolean()) {
            throw new OpenSearchException("parsing_exception", "[profile] is not supported by Warp's OpenSearch frontend");
        }
        // URL parameters (a body value wins, like OpenSearch)
        String q = params.get("q");
        if (q != null && !body.has("query")) {
            r.query = LuceneSyntax.fromUri(q, params.get("df"), "and".equalsIgnoreCase(params.get("default_operator")),
                    "true".equals(params.get("lenient")), params.get("analyzer"));
        } else if (body.has("query")) {
            r.query = QueryParser.parse(body.get("query"));
        }
        if (body.has("post_filter")) {
            r.postFilter = QueryParser.parse(body.get("post_filter"));
        }
        r.size = intOf(body.get("size"), params.get("size"), 10, "size");
        r.from = intOf(body.get("from"), params.get("from"), 0, "from");
        if (r.from < 0) {
            throw new OpenSearchException("illegal_argument_exception", "[from] parameter cannot be negative, found [" + r.from + "]");
        }
        if (r.size < 0) {
            throw new OpenSearchException("illegal_argument_exception", "[size] parameter cannot be negative, found [" + r.size + "]");
        }
        if (body.has("sort")) {
            r.sort = parseSort(body.get("sort"));
        } else if (params.containsKey("sort")) {
            for (String part : params.get("sort").split(",")) {
                SortSpec s = new SortSpec();
                int i = part.lastIndexOf(':');
                if (i > 0) {
                    s.field = part.substring(0, i);
                    s.asc = !part.substring(i + 1).equalsIgnoreCase("desc");
                } else {
                    s.field = part;
                    s.asc = !part.equals("_score");
                }
                r.sort.add(s);
            }
        }
        r.hasSort = !r.sort.isEmpty();
        // _source
        JsonElement src = body.has("_source") ? body.get("_source") : params.containsKey("_source") ? paramSource(params.get("_source")) : null;
        applySource(r, src, params);
        r.storedFields = body.has("stored_fields") ? body.get("stored_fields") : params.containsKey("stored_fields") ? csv(params.get("stored_fields")) : null;
        r.docvalueFields = body.has("docvalue_fields") ? body.get("docvalue_fields") : params.containsKey("docvalue_fields") ? csv(params.get("docvalue_fields")) : null;
        r.fields = body.get("fields");
        // total hits
        JsonElement tth = body.has("track_total_hits") ? body.get("track_total_hits") : params.containsKey("track_total_hits") ? new JsonPrimitive(params.get("track_total_hits")) : null;
        if (tth != null) {
            String t = tth.getAsString();
            if (t.equals("true")) {
                r.trackTotalExact = true;
            } else if (t.equals("false")) {
                r.trackTotalOff = true;
            } else {
                r.trackTotal = Long.parseLong(t);
                if (r.trackTotal < 0) {
                    throw new OpenSearchException("illegal_argument_exception", "[track_total_hits] parameter must be positive or equals to -1, got " + t);
                }
            }
        }
        r.totalAsInt = "true".equals(params.get("rest_total_hits_as_int"));
        if (r.totalAsInt && tth != null && !r.trackTotalExact && !r.trackTotalOff) {
            throw new OpenSearchException("illegal_argument_exception",
                    "[rest_total_hits_as_int] cannot be used if the tracking of total hits is not accurate, got " + r.trackTotal);
        }
        if (r.totalAsInt && tth == null) {
            r.trackTotalExact = true;
        }
        r.trackScores = flag(body.get("track_scores"), params.get("track_scores"));
        r.minScore = body.has("min_score") ? body.get("min_score").getAsDouble() : null;
        r.terminateAfter = intOf(body.get("terminate_after"), params.get("terminate_after"), 0, "terminate_after");
        r.version = flag(body.get("version"), params.get("version"));
        r.seqNoPrimaryTerm = flag(body.get("seq_no_primary_term"), params.get("seq_no_primary_term"));
        r.explain = flag(body.get("explain"), params.get("explain"));
        if (body.has("highlight")) {
            r.highlight = body.getAsJsonObject("highlight");
        }
        if (body.has("search_after")) {
            r.searchAfter = body.getAsJsonArray("search_after");
            if (r.hasSort && r.searchAfter.size() != r.sort.size()) {
                throw new OpenSearchException("illegal_argument_exception", "search_after has " + r.searchAfter.size()
                        + " value(s) but sort has " + r.sort.size() + ".");
            }
        }
        JsonObject aggBody = body.has("aggs") ? body.getAsJsonObject("aggs") : body.has("aggregations") ? body.getAsJsonObject("aggregations") : null;
        if (aggBody != null) {
            r.aggs = Aggs.parse(aggBody);
        }
        if (body.has("collapse")) {
            r.collapse = body.getAsJsonObject("collapse");
        }
        r.scroll = params.get("scroll");
        r.typedKeys = "true".equals(params.get("typed_keys"));
        r.namedScores = "true".equals(params.get("include_named_queries_score"));
        r.searchType = params.getOrDefault("search_type", body.has("search_type") ? body.get("search_type").getAsString() : "query_then_fetch");
        if (!r.searchType.equals("query_then_fetch") && !r.searchType.equals("dfs_query_then_fetch")) {
            throw new OpenSearchException("illegal_argument_exception", "Unsupported search type [" + r.searchType + "]");
        }
        if (body.has("pit")) {
            r.pit = body.getAsJsonObject("pit");
        }
        if (body.has("indices_boost")) {
            r.indicesBoost = new ArrayList<>();
            JsonElement ib = body.get("indices_boost");
            if (ib.isJsonArray()) {
                for (JsonElement e : ib.getAsJsonArray()) {
                    for (var en : e.getAsJsonObject().entrySet()) {
                        r.indicesBoost.add(Map.entry(en.getKey(), en.getValue().getAsDouble()));
                    }
                }
            } else {
                for (var en : ib.getAsJsonObject().entrySet()) {
                    r.indicesBoost.add(Map.entry(en.getKey(), en.getValue().getAsDouble()));
                }
            }
        }
        if (r.scroll != null && r.collapse != null) {
            throw OpenSearchException.validation("cannot use `collapse` in a scroll context");
        }
        if (r.scroll != null && r.size == 0 && !(body.has("size") == false && params.get("size") == null)) {
            throw new OpenSearchException("action_request_validation_exception", "Validation Failed: 1: [size] cannot be [0] in a scroll context;");
        }
        return r;
    }

    private static String kind(JsonElement e) {
        return e.isJsonObject() ? "START_OBJECT" : e.isJsonArray() ? "START_ARRAY" : e.getAsJsonPrimitive().isString() ? "VALUE_STRING"
                : e.getAsJsonPrimitive().isBoolean() ? "VALUE_BOOLEAN" : "VALUE_NUMBER";
    }

    private static JsonElement csv(String s) {
        JsonArray a = new JsonArray();
        for (String p : s.split(",")) {
            a.add(p);
        }
        return a;
    }

    private static JsonElement paramSource(String v) {
        if (v.equals("true") || v.equals("false")) {
            return new JsonPrimitive(Boolean.parseBoolean(v));
        }
        return csv(v);
    }

    private static boolean flag(JsonElement b, String p) {
        if (b != null && !b.isJsonNull()) {
            return b.getAsBoolean();
        }
        return "true".equals(p);
    }

    private static int intOf(JsonElement b, String p, int dflt, String name) {
        try {
            if (b != null && !b.isJsonNull()) {
                return b.getAsInt();
            }
            if (p != null) {
                return Integer.parseInt(p);
            }
        } catch (NumberFormatException e) {
            throw new OpenSearchException("illegal_argument_exception", "Failed to parse int parameter [" + name + "] with value [" + (p != null ? p : b) + "]");
        }
        return dflt;
    }

    static void applySource(Req r, JsonElement src, Map<String, String> params) {
        List<String> inc = new ArrayList<>(), exc = new ArrayList<>();
        boolean any = false;
        if (src != null && !src.isJsonNull()) {
            if (src.isJsonPrimitive() && src.getAsJsonPrimitive().isBoolean()) {
                if (!src.getAsBoolean()) {
                    r.sourceOff = true;
                }
            } else if (src.isJsonPrimitive()) {
                inc.add(src.getAsString());
                any = true;
            } else if (src.isJsonArray()) {
                src.getAsJsonArray().forEach(e -> inc.add(e.getAsString()));
                any = true;
            } else if (src.isJsonObject()) {
                JsonObject o = src.getAsJsonObject();
                for (String k : new String[] {"includes", "include"}) {
                    if (o.has(k)) {
                        listInto(o.get(k), inc);
                    }
                }
                for (String k : new String[] {"excludes", "exclude"}) {
                    if (o.has(k)) {
                        listInto(o.get(k), exc);
                    }
                }
                any = true;
            }
        }
        for (String k : new String[] {"_source_includes", "_source_include"}) {
            if (params.containsKey(k)) {
                inc.addAll(List.of(params.get(k).split(",")));
                any = true;
            }
        }
        for (String k : new String[] {"_source_excludes", "_source_exclude"}) {
            if (params.containsKey(k)) {
                exc.addAll(List.of(params.get(k).split(",")));
                any = true;
            }
        }
        if (any) {
            r.includes = inc;
            r.excludes = exc;
        }
    }

    private static void listInto(JsonElement e, List<String> out) {
        if (e.isJsonArray()) {
            e.getAsJsonArray().forEach(x -> out.add(x.getAsString()));
        } else {
            out.add(e.getAsString());
        }
    }

    private static List<SortSpec> parseSort(JsonElement el) {
        List<SortSpec> out = new ArrayList<>();
        List<JsonElement> items = new ArrayList<>();
        if (el.isJsonArray()) {
            el.getAsJsonArray().forEach(items::add);
        } else {
            items.add(el);
        }
        for (JsonElement e : items) {
            if (e.isJsonPrimitive()) {
                SortSpec s = new SortSpec();
                s.field = e.getAsString();
                s.asc = !s.field.equals("_score");
                out.add(s);
                continue;
            }
            if (!e.isJsonObject()) {
                throw new OpenSearchException("parsing_exception", "Expected [START_OBJECT] or [VALUE_STRING] in sort");
            }
            for (var entry : e.getAsJsonObject().entrySet()) {
                SortSpec s = new SortSpec();
                s.field = entry.getKey();
                JsonElement spec = entry.getValue();
                s.asc = !s.field.equals("_score");
                if (spec.isJsonPrimitive()) {
                    String o = spec.getAsString();
                    if (!o.equalsIgnoreCase("asc") && !o.equalsIgnoreCase("desc")) {
                        throw new OpenSearchException("illegal_argument_exception", "sort order must be either [asc] or [desc], got [" + o + "]");
                    }
                    s.asc = o.equalsIgnoreCase("asc");
                } else if (spec.isJsonObject()) {
                    JsonObject so = spec.getAsJsonObject();
                    if (so.has("order")) {
                        String o = so.get("order").getAsString();
                        if (!o.equalsIgnoreCase("asc") && !o.equalsIgnoreCase("desc")) {
                            throw new OpenSearchException("illegal_argument_exception", "Unknown SortOrder [" + o + "]");
                        }
                        s.asc = o.equalsIgnoreCase("asc");
                    }
                    if (so.has("mode")) {
                        s.mode = so.get("mode").getAsString();
                    }
                    if (so.has("missing")) {
                        JsonElement m = so.get("missing");
                        s.missing = m.getAsJsonPrimitive().isNumber() ? (Object) m.getAsDouble() : m.getAsString();
                    }
                    if (so.has("unmapped_type")) {
                        s.unmappedType = so.get("unmapped_type").getAsString();
                    }
                    if (so.has("format")) {
                        s.format = so.get("format").getAsString();
                    }
                    if (so.has("nested")) {
                        JsonObject n = so.getAsJsonObject("nested");
                        s.nestedPath = n.get("path").getAsString();
                        if (n.has("filter")) {
                            s.nestedFilter = QueryParser.parse(n.get("filter"));
                        }
                    }
                    if (so.has("nested_path")) {
                        s.nestedPath = so.get("nested_path").getAsString();
                    }
                }
                if (s.field.equals("_geo_distance") || s.field.equals("_script")) {
                    throw new OpenSearchException("parsing_exception", "sort by [" + s.field + "] is not supported by Warp's OpenSearch frontend");
                }
                out.add(s);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ execution

    static final class Hit {
        Query.DocCtx ctx;
        PostgresSearchStore.Doc doc;
        float score;
        boolean hasScore;
        List<Object> sortValues;
        int order;
        List<String> matchedQueries;
        JsonObject innerHits;
        JsonObject highlight;
        List<Hit> group;
    }

    static final class Result {
        List<Hit> hits = new ArrayList<>();
        long total;
        boolean totalExact = true;
        JsonObject aggs;
        int shardsTotal;
        boolean terminatedEarly;
        Map<String, JsonObject> hlCache;
    }

    JsonObject search(List<PostgresSearchStore.Resolved> targets, Req req, long startNanos) throws SQLException {
        Result res = execute(targets, req);
        return renderSearch(res, req, targets, startNanos);
    }

    private static final class Partition {
        final IndexCtx ix;
        final List<JsonObject> aliasFilters;

        Partition(IndexCtx ix, List<JsonObject> aliasFilters) {
            this.ix = ix;
            this.aliasFilters = aliasFilters;
        }
    }

    private List<Partition> partitions(List<PostgresSearchStore.Resolved> targets, boolean dfs, Query.Node query)
            throws SQLException {
        List<Partition> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (PostgresSearchStore.Resolved r : targets) {
            PostgresSearchStore.IndexMeta m = r.index();
            // scoring needs corpus statistics (every document of the partition), so the SQL pre-filter is only used when
            // the query cannot depend on them: filters/aggregation-only requests and sorts that ignore _score
            PostgresSearchStore.SqlFilter filter = query == null ? null : Prefilter.of(query, m.mappings);
            List<PostgresSearchStore.Doc> docs = store.scan(m, filter);
            if (dfs || store.shardCount() == 1) {
                out.add(new Partition(new IndexCtx(m.name, m.mappings, m.settings, now, docs), r.aliasFilters()));
            } else {
                Map<Integer, List<PostgresSearchStore.Doc>> byShard = new java.util.TreeMap<>();
                for (int s = 0; s < store.shardCount(); s++) {
                    byShard.put(s, new ArrayList<>());
                }
                for (PostgresSearchStore.Doc d : docs) {
                    byShard.get(d.shard).add(d);
                }
                for (var e : byShard.entrySet()) {
                    out.add(new Partition(new IndexCtx(m.name, m.mappings, m.settings, now, e.getValue()), r.aliasFilters()));
                }
            }
        }
        return out;
    }

    Result execute(List<PostgresSearchStore.Resolved> targets, Req req) throws SQLException {
        Result res = new Result();
        int window = 10_000;
        for (PostgresSearchStore.Resolved t : targets) {
            JsonObject idx = t.index().settings.getAsJsonObject("index");
            if (idx.has("max_result_window")) {
                window = Math.min(window, Integer.parseInt(idx.get("max_result_window").getAsString()));
            }
        }
        if (req.scroll != null && req.size > window) {
            throw new OpenSearchException("illegal_argument_exception", "Batch size is too large, size must be less than or equal to: [" + window
                    + "] but was [" + req.size + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the [index.max_result_window] index level setting.").asShardLevel();
        }
        if (req.scroll == null && (long) req.from + req.size > window) {
            throw new OpenSearchException("illegal_argument_exception", "Result window is too large, from + size must be less than or equal to: [" + window
                    + "] but was [" + ((long) req.from + req.size) + "]. See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting.").asShardLevel();
        }
        Map<String, Double> boostByIndex = new HashMap<>();
        if (req.indicesBoost != null) {
            for (var e : req.indicesBoost) {
                String pat = e.getKey();
                boolean any = pat.contains("*");
                for (PostgresSearchStore.Resolved t : targets) {
                    any |= t.index().name.equals(pat) || t.index().hasAlias(pat);
                }
                if (!any) {
                    throw OpenSearchException.indexNotFound(pat);
                }
            }
            for (PostgresSearchStore.Resolved t : targets) {
                for (var e : req.indicesBoost) {
                    String pat = e.getKey();
                    if (t.index().name.equals(pat) || (pat.contains("*") && Mappings.wildcardMatch(pat, t.index().name)) || t.index().hasAlias(pat)) {
                        boostByIndex.putIfAbsent(t.index().name, e.getValue());
                    }
                }
            }
        }
        boolean dfs = "dfs_query_then_fetch".equals(req.searchType);
        List<Partition> parts = partitions(targets, dfs, prefilterable(req) ? req.query : null);
        res.shardsTotal = targets.stream().mapToInt(t -> shardsOf(t.index())).sum();
        req.query.prepare(new Vector.Prep(parts.stream().map(p -> p.ix).toList()));
        if (req.postFilter != null) {
            req.postFilter.prepare(new Vector.Prep(parts.stream().map(p -> p.ix).toList()));
        }
        List<Query.DocCtx> matchedForAggs = new ArrayList<>();
        List<Query.DocCtx> allDocs = new ArrayList<>();
        List<Hit> hits = new ArrayList<>();
        long total = 0;
        int order = 0;
        boolean needScore = !req.hasSort || req.trackScores || sortsOnScore(req);
        for (Partition p : parts) {
            List<Query.Node> aliasNodes = new ArrayList<>();
            for (JsonObject f : p.aliasFilters) {
                aliasNodes.add(QueryParser.parse(f));
            }
            int perPartition = 0;
            for (PostgresSearchStore.Doc d : p.ix.corpus) {
                Query.DocCtx ctx = new Query.DocCtx(p.ix, d);
                allDocs.add(ctx);
                double s = req.query.score(ctx);
                if (!Query.matched(s)) {
                    continue;
                }
                boolean aliasOk = true;
                for (Query.Node an : aliasNodes) {
                    if (!Query.matched(an.score(ctx))) {
                        aliasOk = false;
                    }
                }
                if (!aliasOk) {
                    continue;
                }
                if (req.minScore != null && s < req.minScore) {
                    continue;
                }
                if (req.terminateAfter > 0 && perPartition >= req.terminateAfter) {
                    res.terminatedEarly = true;
                    break;
                }
                perPartition++;
                if (!boostByIndex.isEmpty()) {
                    s *= boostByIndex.getOrDefault(d.index, 1.0);
                }
                ctx.score = s;
                matchedForAggs.add(ctx);
                if (req.postFilter != null && !Query.matched(req.postFilter.score(ctx))) {
                    continue;
                }
                total++;
                Hit h = new Hit();
                h.ctx = ctx;
                h.doc = d;
                h.score = (float) s;
                h.hasScore = true;
                h.order = order++;
                hits.add(h);
            }
        }
        res.total = total;
        if (!allDocs.isEmpty() || !req.aggs.isEmpty()) {
            // aggregations run over every document matching the query (post_filter excluded)
            if (!req.aggs.isEmpty()) {
                final List<Query.DocCtx> all = allDocs;
                Aggs aggs = new Aggs(new Aggs.Hooks() {
                    public JsonObject topHits(JsonObject params, List<Query.DocCtx> docs) {
                        return topHitsAgg(params, docs);
                    }

                    public List<Query.DocCtx> allDocs() {
                        return all;
                    }
                });
                res.aggs = aggs.run(req.aggs, matchedForAggs, req.typedKeys);
            }
        }
        // sorting
        if (req.hasSort) {
            computeSortValues(hits, req);
            hits.sort(hitComparator(req));
        } else {
            hits.sort((a, b) -> {
                int c = Float.compare(b.score, a.score);
                return c != 0 ? c : Integer.compare(a.order, b.order);
            });
        }
        if (req.searchAfter != null) {
            hits = applySearchAfter(hits, req);
        }
        res.hits = hits;
        res.total = hits.size() == 0 && req.searchAfter == null ? total : (req.searchAfter != null ? total : total);
        return res;
    }

    static int shardsOf(PostgresSearchStore.IndexMeta m) {
        try {
            return Integer.parseInt(m.settings.getAsJsonObject("index").get("number_of_shards").getAsString());
        } catch (RuntimeException e) {
            return 1;
        }
    }

    private static boolean sortsOnScore(Req r) {
        for (SortSpec s : r.sort) {
            if (s.field.equals("_score")) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ sorting

    private static final Object MISSING_LAST = new Object();

    private void computeSortValues(List<Hit> hits, Req req) {
        for (Hit h : hits) {
            List<Object> vals = new ArrayList<>();
            for (SortSpec s : req.sort) {
                vals.add(sortValue(h, s));
            }
            h.sortValues = vals;
        }
    }

    private Object sortValue(Hit h, SortSpec s) {
        switch (s.field) {
            case "_score":
                return (double) h.score;
            case "_doc":
                return (long) h.order;
            case "_shard_doc":
                return (long) h.order;
            case "_id":
                return h.doc.id;
            case "_index":
                return h.doc.index;
            case "_seq_no":
                return h.doc.seqNo;
            case "_version":
                return h.doc.version;
            default:
                break;
        }
        Query.DocCtx ctx = h.ctx;
        Mappings.Field f = ctx.ix.mappings.get(s.field);
        if (f == null || (f.nestedPath != null && s.nestedPath == null)) {
            if (s.unmappedType == null) {
                throw new OpenSearchException("query_shard_exception", "No mapping found for [" + s.field + "] in order to sort on")
                        .with("index", ctx.ix.indexName).asShardLevel();
            }
            return missingValue(s, null);
        }
        if (f.isText()) {
            throw new OpenSearchException("illegal_argument_exception",
                    "Text fields are not optimised for operations that require per-document field data like aggregations and sorting, so these "
                            + "operations are disabled by default. Please use a keyword field instead. Alternatively, set fielddata=true on ["
                            + s.field + "] in order to load field data by uninverting the inverted index. Note that this can use significant memory.")
                    .asShardLevel();
        }
        List<Object> vals = new ArrayList<>();
        if (s.nestedPath != null) {
            String rel = s.nestedPath;
            for (JsonElement e : Mappings.values(ctx.obj, rel)) {
                if (e.isJsonObject()) {
                    Query.DocCtx nd = ctx.nested(e.getAsJsonObject(), s.nestedPath + ".");
                    if (s.nestedFilter == null || Query.matched(s.nestedFilter.score(nd))) {
                        vals.addAll(nd.values(f));
                    }
                }
            }
        } else {
            vals = ctx.values(f);
        }
        if (vals.isEmpty()) {
            return missingValue(s, f);
        }
        String mode = s.mode != null ? s.mode : (s.asc ? "min" : "max");
        Object v;
        switch (mode) {
            case "max" -> v = vals.stream().max(Values::compare).get();
            case "sum" -> v = vals.stream().mapToDouble(Values::toDouble).sum();
            case "avg" -> v = vals.stream().mapToDouble(Values::toDouble).average().getAsDouble();
            case "median" -> {
                List<Object> sorted = new ArrayList<>(vals);
                sorted.sort(Values::compare);
                v = sorted.get(sorted.size() / 2);
            }
            default -> v = vals.stream().min(Values::compare).get();
        }
        if (v instanceof Boolean b) {
            return b ? 1L : 0L;
        }
        return v;
    }

    private Object missingValue(SortSpec s, Mappings.Field f) {
        s.numeric = f != null ? !f.isString() : (s.unmappedType != null && !s.unmappedType.equals("keyword") && !s.unmappedType.equals("text"));
        s.intLike = f != null && (f.type.equals("integer") || f.type.equals("short") || f.type.equals("byte"));
        if (s.missing != null && !s.missing.equals("_last") && !s.missing.equals("_first")) {
            if (f != null && s.missing instanceof String str) {
                Object p = Values.parse(f, new JsonPrimitive(str), null, java.time.ZoneOffset.UTC);
                return p == null ? str : p;
            }
            return s.missing instanceof Double d && f != null && f.isInteger() ? (Object) d.longValue() : s.missing;
        }
        boolean first = "_first".equals(s.missing);
        return first ? (s.asc ? MissingSentinel.FIRST : MissingSentinel.LAST) : (s.asc ? MissingSentinel.LAST : MissingSentinel.FIRST);
    }

    private enum MissingSentinel { FIRST, LAST }

    private Comparator<Hit> hitComparator(Req req) {
        return (a, b) -> {
            for (int i = 0; i < req.sort.size(); i++) {
                int c = compareSortValue(a.sortValues.get(i), b.sortValues.get(i));
                if (c != 0) {
                    return req.sort.get(i).asc ? c : -c;
                }
            }
            // tie: host then document order (like docid order inside a shard)
            int c = Integer.compare(a.doc.shard, b.doc.shard);
            if (c != 0) {
                return c;
            }
            return Long.compare(a.doc.seqNo, b.doc.seqNo);
        };
    }

    /** Missing values sort last/first REGARDLESS of order: the caller inverts for desc, so pre-invert sentinels. */
    private static int compareSortValue(Object a, Object b) {
        boolean am = a instanceof MissingSentinel, bm = b instanceof MissingSentinel;
        if (am || bm) {
            if (am && bm) {
                return a == b ? 0 : (a == MissingSentinel.FIRST ? -1 : 1);
            }
            if (am) {
                return a == MissingSentinel.FIRST ? -1 : 1;
            }
            return b == MissingSentinel.FIRST ? 1 : -1;
        }
        return Values.compare(a, b);
    }

    private List<Hit> applySearchAfter(List<Hit> hits, Req req) {
        if (!req.hasSort) {
            throw new OpenSearchException("illegal_argument_exception", "Sort must contain at least one field.");
        }
        List<Object> after = new ArrayList<>();
        for (int i = 0; i < req.sort.size(); i++) {
            JsonElement e = req.searchAfter.get(i);
            SortSpec s = req.sort.get(i);
            if (e.isJsonNull()) {
                after.add(s.asc ? MissingSentinel.LAST : MissingSentinel.FIRST);
            } else if (e.getAsJsonPrimitive().isNumber()) {
                java.math.BigDecimal bd = e.getAsBigDecimal();
                after.add(bd.scale() <= 0 && bd.abs().compareTo(new java.math.BigDecimal(Long.MAX_VALUE)) <= 0 ? (Object) bd.longValue() : (Object) bd.doubleValue());
            } else if (e.getAsJsonPrimitive().isBoolean()) {
                after.add(e.getAsBoolean() ? 1L : 0L);
            } else {
                String str = e.getAsString();
                Mappings.Field f = null;
                for (Hit h : hits) {
                    f = h.ctx.ix.mappings.get(s.field);
                    if (f != null) {
                        break;
                    }
                }
                Object parsed = f != null && !f.isString() ? Values.parse(f, e, null, java.time.ZoneOffset.UTC) : null;
                after.add(parsed != null ? parsed : str);
            }
        }
        List<Hit> out = new ArrayList<>();
        for (Hit h : hits) {
            int c = 0;
            for (int i = 0; i < req.sort.size() && c == 0; i++) {
                c = compareSortValue(h.sortValues.get(i), after.get(i));
                if (!req.sort.get(i).asc) {
                    c = -c;
                }
            }
            if (c > 0) {
                out.add(h);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ rendering

    private JsonObject shardsObj(int total) {
        JsonObject s = new JsonObject();
        s.addProperty("total", total);
        s.addProperty("successful", total);
        s.addProperty("skipped", 0);
        s.addProperty("failed", 0);
        return s;
    }

    JsonObject renderSearch(Result res, Req req, List<PostgresSearchStore.Resolved> targets, long startNanos) throws SQLException {
        List<Hit> hits = res.hits;
        if (req.collapse != null) {
            hits = collapse(hits, req);
        }
        List<Hit> page = hits.subList(Math.min(req.from, hits.size()), Math.min(hits.size(), req.from + req.size));
        JsonObject out = new JsonObject();
        out.addProperty("took", Math.max(0, (System.nanoTime() - startNanos) / 1_000_000));
        out.addProperty("timed_out", false);
        if (res.terminatedEarly) {
            out.addProperty("terminated_early", true);
        }
        out.add("_shards", shardsObj(Math.max(1, res.shardsTotal)));
        JsonObject hw = new JsonObject();
        long total = req.collapse != null ? res.total : res.total;
        if (req.trackTotalOff && req.totalAsInt) {
            hw.addProperty("total", -1);
        } else if (!req.trackTotalOff) {
            boolean gte = !req.trackTotalExact && total > req.trackTotal;
            long value = gte ? req.trackTotal : total;
            if (req.totalAsInt) {
                if (gte) {
                    throw new OpenSearchException("illegal_argument_exception",
                            "[rest_total_hits_as_int] cannot be used if the tracking of total hits is not accurate, got " + req.trackTotal);
                }
                hw.addProperty("total", total);
            } else {
                JsonObject t = new JsonObject();
                t.addProperty("value", value);
                t.addProperty("relation", gte ? "gte" : "eq");
                hw.add("total", t);
            }
        }
        Double maxScore = null;
        if (req.size + req.from == 0) {
            // no documents collected
        } else if (!req.hasSort || req.trackScores) {
            for (Hit h : hits) {
                if (maxScore == null || h.score > maxScore) {
                    maxScore = (double) h.score;
                }
            }
        } else if (sortsOnScore(req) && !hits.isEmpty()) {
            for (Hit h : hits) {
                if (maxScore == null || h.score > maxScore) {
                    maxScore = (double) h.score;
                }
            }
        }
        if (maxScore == null) {
            hw.add("max_score", JsonNull.INSTANCE);
        } else {
            hw.addProperty("max_score", (float) (double) maxScore);
        }
        JsonArray arr = new JsonArray();
        Query.Hl hl = null;
        if (req.highlight != null) {
            hl = new Query.Hl();
            req.query.collect(hl);
        }
        for (Hit h : page) {
            JsonObject ho = renderHit(h, req, hl);
            if (req.collapse != null) {
                collapseFields(ho, h, req);
            }
            arr.add(ho);
        }
        hw.add("hits", arr);
        out.add("hits", hw);
        if (res.aggs != null) {
            out.add("aggregations", res.aggs);
        }
        return out;
    }

    JsonObject renderHit(Hit h, Req req, Query.Hl hl) {
        JsonObject o = new JsonObject();
        o.addProperty("_index", h.doc.index);
        boolean storedNoneOnly = req.storedFields != null && req.storedFields.isJsonPrimitive() && req.storedFields.getAsString().equals("_none_");
        if (!storedNoneOnly) {
            o.addProperty("_id", h.doc.id);
        }
        if (req.version) {
            o.addProperty("_version", h.doc.version);
        }
        if (req.seqNoPrimaryTerm) {
            o.addProperty("_seq_no", h.doc.seqNo);
            o.addProperty("_primary_term", 1);
        }
        if (!req.hasSort || req.trackScores) {
            o.addProperty("_score", h.score);
        } else if (sortsOnScore(req)) {
            o.addProperty("_score", h.score);
        } else {
            o.add("_score", JsonNull.INSTANCE);
        }
        if (req.hasSort) {
            JsonArray sv = new JsonArray();
            for (int i = 0; i < h.sortValues.size(); i++) {
                Object v = h.sortValues.get(i);
                sv.add(sortJson(v, req.sort.get(i)));
            }
            o.add("sort", sv);
        }
        boolean storedNone = req.storedFields != null && !hasSourceInStored(req.storedFields);
        if (!req.sourceOff && !storedNone) {
            o.add("_source", filterSource(h.doc.source, req.includes, req.excludes));
        }
        JsonObject fields = new JsonObject();
        if (req.docvalueFields != null) {
            addDocvalueFields(fields, h, req.docvalueFields);
        }
        if (req.fields != null) {
            addFields(fields, h, req.fields);
        }
        if (fields.size() > 0) {
            o.add("fields", fields);
        }
        if (req.highlight != null && hl != null) {
            JsonObject hj = Highlighter.highlight(h.ctx, req.highlight, hl);
            if (hj != null && hj.size() > 0) {
                o.add("highlight", hj);
            }
        }
        List<Query.Node> namedNodes = new ArrayList<>();
        nodesOf(req.query, namedNodes);
        if (req.namedScores) {
            JsonObject mq = new JsonObject();
            for (Query.Node n : namedNodes) {
                if (n.name != null) {
                    double sc = n.score(h.ctx);
                    if (Query.matched(sc)) {
                        mq.addProperty(n.name, (float) sc);
                    }
                }
            }
            if (mq.size() > 0) {
                o.add("matched_queries", mq);
            }
        } else {
            List<String> named = matchedQueries(req.query, h.ctx);
            if (!named.isEmpty()) {
                JsonArray mq = new JsonArray();
                named.forEach(mq::add);
                o.add("matched_queries", mq);
            }
        }
        JsonObject inner = innerHits(req.query, h.ctx);
        if (inner != null && inner.size() > 0) {
            o.add("inner_hits", inner);
        }
        return o;
    }

    private static boolean hasSourceInStored(JsonElement sf) {
        if (sf.isJsonArray()) {
            for (JsonElement e : sf.getAsJsonArray()) {
                if (e.getAsString().equals("_source") || e.getAsString().equals("*")) {
                    return true;
                }
            }
            return false;
        }
        return sf.getAsString().equals("_source") || sf.getAsString().equals("*");
    }

    private JsonElement sortJson(Object v, SortSpec s) {
        if (v instanceof MissingSentinel m) {
            return missingJson(m, s);
        }
        if (v instanceof Long l) {
            return new JsonPrimitive(l);
        }
        if (v instanceof Double d) {
            return new JsonPrimitive(s.field.equals("_score") ? (Number) (float) (double) d : (Number) d);
        }
        if (v instanceof Boolean b) {
            return new JsonPrimitive(b ? 1 : 0);
        }
        return new JsonPrimitive(String.valueOf(v));
    }

    private JsonElement missingJson(MissingSentinel m, SortSpec s) {
        if (!s.numeric) {
            return JsonNull.INSTANCE;
        }
        if (s.intLike) {
            return new JsonPrimitive(m == MissingSentinel.LAST ? (long) Integer.MAX_VALUE : (long) Integer.MIN_VALUE);
        }
        return new JsonPrimitive(m == MissingSentinel.LAST ? Long.MAX_VALUE : Long.MIN_VALUE);
    }

    // ---- _source filtering ----

    static JsonObject filterSource(JsonObject src, List<String> inc, List<String> exc) {
        if ((inc == null || inc.isEmpty()) && (exc == null || exc.isEmpty())) {
            return src;
        }
        JsonObject out = inc == null || inc.isEmpty() ? src.deepCopy() : includeOnly(src, inc, "");
        if (exc != null) {
            for (String e : exc) {
                excludePath(out, e, "");
            }
        }
        return out;
    }

    private static boolean pathMatches(String pattern, String path) {
        return Mappings.wildcardMatch(pattern, path);
    }

    private static JsonObject includeOnly(JsonObject src, List<String> inc, String prefix) {
        JsonObject out = new JsonObject();
        for (var e : src.entrySet()) {
            String path = prefix + e.getKey();
            boolean full = false, partial = false;
            for (String p : inc) {
                if (pathMatches(p, path) || (p.endsWith(".*") && pathMatches(p.substring(0, p.length() - 2) + ".*", path))) {
                    full = true;
                } else if (p.startsWith(path + ".") || (p.contains("*") && pathMatches(p.substring(0, Math.max(0, p.lastIndexOf('.'))).isEmpty() ? "*" : p.substring(0, p.lastIndexOf('.')), path) && p.lastIndexOf('.') > 0)) {
                    partial = true;
                } else if (path.startsWith(p + ".")) {
                    full = true;
                }
            }
            if (full) {
                out.add(e.getKey(), e.getValue().deepCopy());
            } else if (partial) {
                JsonElement v = e.getValue();
                if (v.isJsonObject()) {
                    JsonObject sub = includeOnly(v.getAsJsonObject(), inc, path + ".");
                    if (sub.size() > 0) {
                        out.add(e.getKey(), sub);
                    }
                } else if (v.isJsonArray()) {
                    JsonArray arr = new JsonArray();
                    for (JsonElement x : v.getAsJsonArray()) {
                        if (x.isJsonObject()) {
                            JsonObject sub = includeOnly(x.getAsJsonObject(), inc, path + ".");
                            if (sub.size() > 0) {
                                arr.add(sub);
                            }
                        }
                    }
                    if (arr.size() > 0) {
                        out.add(e.getKey(), arr);
                    }
                }
            }
        }
        return out;
    }

    private static void excludePath(JsonObject obj, String pattern, String prefix) {
        List<String> keys = new ArrayList<>(obj.keySet());
        for (String k : keys) {
            String path = prefix + k;
            if (pathMatches(pattern, path)) {
                obj.remove(k);
                continue;
            }
            JsonElement v = obj.get(k);
            if (v.isJsonObject()) {
                excludePath(v.getAsJsonObject(), pattern, path + ".");
            } else if (v.isJsonArray()) {
                for (JsonElement x : v.getAsJsonArray()) {
                    if (x.isJsonObject()) {
                        excludePath(x.getAsJsonObject(), pattern, path + ".");
                    }
                }
            }
        }
    }

    // ---- fields / docvalue_fields ----

    private void addDocvalueFields(JsonObject fields, Hit h, JsonElement dv) {
        List<JsonElement> items = new ArrayList<>();
        if (dv.isJsonArray()) {
            dv.getAsJsonArray().forEach(items::add);
        } else {
            items.add(dv);
        }
        int maxDv = 100;
        try {
            var idxs = h.ctx.ix.settings.getAsJsonObject("index");
            if (idxs.has("max_docvalue_fields_search")) {
                maxDv = Integer.parseInt(idxs.get("max_docvalue_fields_search").getAsString());
            }
        } catch (RuntimeException ignored) {
            // default
        }
        if (items.size() > maxDv) {
            throw OpenSearchException.illegalArgument("Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxDv
                    + "] but was [" + items.size() + "]. This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.")
                    .asShardLevel();
        }
        for (JsonElement it : items) {
            String field = it.isJsonObject() ? it.getAsJsonObject().get("field").getAsString() : it.getAsString();
            String format = it.isJsonObject() && it.getAsJsonObject().has("format") ? it.getAsJsonObject().get("format").getAsString() : null;
            for (Mappings.Field f : expandFields(h.ctx.ix.mappings, field)) {
                if (f.isText() || f.type.equals("object") || f.type.equals("nested") || f.nestedPath != null) {
                    continue;
                }
                List<Object> vals = h.ctx.values(f);
                if (vals.isEmpty()) {
                    continue;
                }
                JsonArray a = new JsonArray();
                List<Object> sorted = new ArrayList<>(vals);
                if (!f.type.equals("boolean")) {
                    sorted.sort(Values::compare);
                }
                for (Object v : sorted) {
                    a.add(docvalueJson(v, f, format));
                }
                fields.add(f.path, a);
            }
        }
    }

    private JsonElement docvalueJson(Object v, Mappings.Field f, String format) {
        if (v instanceof Long l && f.type.equals("date")) {
            if (format != null && format.equals("epoch_millis")) {
                return new JsonPrimitive(Long.toString(l));
            }
            return new JsonPrimitive(Dates.format(l, format != null ? format : f.format, java.time.ZoneOffset.UTC));
        }
        if (format != null && v instanceof Number n && f.isNumeric()) {
            return new JsonPrimitive(new java.text.DecimalFormat(format, java.text.DecimalFormatSymbols.getInstance(java.util.Locale.ROOT)).format(n.doubleValue()));
        }
        if (v instanceof Long l) {
            return new JsonPrimitive(l);
        }
        if (v instanceof Double d) {
            return new JsonPrimitive(d);
        }
        if (v instanceof Boolean b) {
            return new JsonPrimitive(b);
        }
        return new JsonPrimitive(String.valueOf(v));
    }

    private List<Mappings.Field> expandFields(Mappings m, String pattern) {
        List<Mappings.Field> out = new ArrayList<>();
        if (!pattern.contains("*")) {
            Mappings.Field f = m.get(pattern);
            if (f != null) {
                out.add(f);
            }
            return out;
        }
        for (Mappings.Field f : m.fields.values()) {
            if (Mappings.wildcardMatch(pattern, f.path)) {
                out.add(f);
            }
        }
        return out;
    }

    private void addFields(JsonObject fields, Hit h, JsonElement spec) {
        List<JsonElement> items = new ArrayList<>();
        if (spec.isJsonArray()) {
            spec.getAsJsonArray().forEach(items::add);
        } else {
            items.add(spec);
        }
        for (JsonElement it : items) {
            String field = it.isJsonObject() ? it.getAsJsonObject().get("field").getAsString() : it.getAsString();
            String format = it.isJsonObject() && it.getAsJsonObject().has("format") ? it.getAsJsonObject().get("format").getAsString() : null;
            for (Mappings.Field f : expandFields(h.ctx.ix.mappings, field)) {
                if (f.type.equals("object") || f.type.equals("nested") || f.nestedPath != null || f.subField) {
                    continue;
                }
                List<JsonElement> raw = Mappings.values(h.ctx.obj, f.path);
                if (raw.isEmpty()) {
                    continue;
                }
                JsonArray a = new JsonArray();
                for (JsonElement e : raw) {
                    if (f.type.equals("date")) {
                        Object v = Values.parse(f, e, h.ctx.ix.settings, java.time.ZoneOffset.UTC);
                        a.add(v instanceof Long l ? new JsonPrimitive(Dates.format(l, format != null ? format : f.format, java.time.ZoneOffset.UTC)) : e);
                    } else {
                        a.add(e);
                    }
                }
                fields.add(f.path, a);
            }
        }
    }

    // ---- named queries / inner hits ----

    private void nodesOf(Query.Node n, List<Query.Node> out) {
        out.add(n);
        for (Query.Node c : n.children()) {
            nodesOf(c, out);
        }
    }

    private List<String> matchedQueries(Query.Node root, Query.DocCtx ctx) {
        List<Query.Node> all = new ArrayList<>();
        nodesOf(root, all);
        List<String> out = new ArrayList<>();
        for (Query.Node n : all) {
            if (n.name != null && Query.matched(n.score(ctx))) {
                out.add(n.name);
            }
        }
        return out;
    }

    private JsonObject innerHits(Query.Node root, Query.DocCtx ctx) {
        List<Query.Node> all = new ArrayList<>();
        nodesOf(root, all);
        JsonObject out = new JsonObject();
        for (Query.Node n : all) {
            if (n instanceof Query.NestedQ nq && nq.innerHitsName != null) {
                List<Object[]> ms = nq.matches(ctx);
                ms.sort((a, b) -> Double.compare((Double) b[1], (Double) a[1]));
                JsonArray arr = new JsonArray();
                for (Object[] m : ms) {
                    JsonObject hit = new JsonObject();
                    hit.addProperty("_index", ctx.doc.index);
                    hit.addProperty("_id", ctx.doc.id);
                    JsonObject nest = new JsonObject();
                    nest.addProperty("field", nq.path);
                    nest.addProperty("offset", (Integer) m[0]);
                    hit.add("_nested", nest);
                    hit.addProperty("_score", (float) (double) (Double) m[1]);
                    hit.add("_source", (JsonObject) m[2]);
                    arr.add(hit);
                }
                JsonObject wrapper = new JsonObject();
                JsonObject hits = new JsonObject();
                JsonObject total = new JsonObject();
                total.addProperty("value", ms.size());
                total.addProperty("relation", "eq");
                hits.add("total", total);
                hits.add("max_score", ms.isEmpty() ? JsonNull.INSTANCE : new JsonPrimitive((float) (double) (Double) ms.get(0)[1]));
                hits.add("hits", arr);
                wrapper.add("hits", hits);
                out.add(nq.innerHitsName, wrapper);
            }
        }
        return out;
    }

    // ---- collapse ----

    private List<Hit> collapse(List<Hit> hits, Req req) {
        String field = req.collapse.get("field").getAsString();
        List<Hit> out = new ArrayList<>();
        Map<Object, Hit> first = new java.util.LinkedHashMap<>();
        for (Hit h : hits) {
            Mappings.Field f = h.ctx.ix.mappings.get(field);
            if (f == null) {
                throw new OpenSearchException("illegal_argument_exception", "no mapping found for `" + field + "` in order to collapse on").asShardLevel();
            }
            List<Object> v = h.ctx.values(f);
            Object key = v.isEmpty() ? "\u0000null" : v.get(0);
            Hit head = first.get(key);
            if (head == null) {
                first.put(key, h);
                h.group = new ArrayList<>();
                h.group.add(h);
                out.add(h);
            } else {
                head.group.add(h);
            }
        }
        return out;
    }

    private void collapseFields(JsonObject o, Hit h, Req req) {
        String field = req.collapse.get("field").getAsString();
        Mappings.Field f = h.ctx.ix.mappings.get(field);
        JsonObject fields = o.has("fields") ? o.getAsJsonObject("fields") : new JsonObject();
        JsonArray a = new JsonArray();
        if (f != null) {
            for (Object v : h.ctx.values(f)) {
                a.add(docvalueJson(v, f, null));
                break;
            }
        }
        fields.add(field, a);
        o.add("fields", fields);
        if (req.collapse.has("inner_hits") && h.group != null) {
            JsonObject inner = new JsonObject();
            List<JsonObject> specs = new ArrayList<>();
            JsonElement ih = req.collapse.get("inner_hits");
            if (ih.isJsonArray()) {
                ih.getAsJsonArray().forEach(e -> specs.add(e.getAsJsonObject()));
            } else {
                specs.add(ih.getAsJsonObject());
            }
            for (JsonObject spec : specs) {
                Req sub = new Req();
                sub.size = spec.has("size") ? spec.get("size").getAsInt() : 3;
                sub.from = spec.has("from") ? spec.get("from").getAsInt() : 0;
                if (spec.has("sort")) {
                    sub.sort = parseSort(spec.get("sort"));
                    sub.hasSort = !sub.sort.isEmpty();
                }
                applySource(sub, spec.get("_source"), Map.of());
                sub.version = spec.has("version") && spec.get("version").getAsBoolean();
                sub.seqNoPrimaryTerm = spec.has("seq_no_primary_term") && spec.get("seq_no_primary_term").getAsBoolean();
                sub.fields = spec.get("fields");
                sub.docvalueFields = spec.get("docvalue_fields");
                List<Hit> members = new ArrayList<>(h.group);
                if (sub.hasSort) {
                    for (Hit m : members) {
                        m.sortValues = null;
                    }
                    computeSortValues(members, sub);
                    members.sort(hitComparator(sub));
                }
                JsonObject wrapper = new JsonObject();
                JsonObject hw = new JsonObject();
                if (req.totalAsInt) {
                    hw.addProperty("total", members.size());
                } else {
                    JsonObject total = new JsonObject();
                    total.addProperty("value", members.size());
                    total.addProperty("relation", "eq");
                    hw.add("total", total);
                }
                Float max = null;
                if (!sub.hasSort) {
                    for (Hit m : members) {
                        max = max == null ? m.score : Math.max(max, m.score);
                    }
                }
                hw.add("max_score", max == null ? JsonNull.INSTANCE : new JsonPrimitive(max));
                JsonArray arr = new JsonArray();
                for (Hit m : members.subList(Math.min(sub.from, members.size()), Math.min(members.size(), sub.from + sub.size))) {
                    arr.add(renderHit(m, sub, null));
                }
                hw.add("hits", arr);
                wrapper.add("hits", hw);
                inner.add(spec.has("name") ? spec.get("name").getAsString() : "inner_hits", wrapper);
            }
            o.add("inner_hits", inner);
        }
    }

    // ---- top_hits aggregation ----

    private JsonObject topHitsAgg(JsonObject params, List<Query.DocCtx> docs) {
        Req r = new Req();
        r.size = params.has("size") ? params.get("size").getAsInt() : 3;
        r.from = params.has("from") ? params.get("from").getAsInt() : 0;
        if (params.has("sort")) {
            r.sort = parseSort(params.get("sort"));
            r.hasSort = !r.sort.isEmpty();
        }
        applySource(r, params.get("_source"), Map.of());
        r.version = params.has("version") && params.get("version").getAsBoolean();
        r.seqNoPrimaryTerm = params.has("seq_no_primary_term") && params.get("seq_no_primary_term").getAsBoolean();
        r.docvalueFields = params.get("docvalue_fields");
        r.fields = params.get("fields");
        r.trackScores = params.has("track_scores") && params.get("track_scores").getAsBoolean();
        List<Hit> hits = new ArrayList<>();
        int i = 0;
        for (Query.DocCtx c : docs) {
            Hit h = new Hit();
            h.ctx = c;
            h.doc = c.doc;
            h.score = Double.isNaN(c.score) ? 1f : (float) c.score;
            h.order = i++;
            hits.add(h);
        }
        if (r.hasSort) {
            computeSortValues(hits, r);
            hits.sort(hitComparator(r));
        } else {
            hits.sort((a, b) -> {
                int c = Float.compare(b.score, a.score);
                return c != 0 ? c : Integer.compare(a.order, b.order);
            });
        }
        JsonObject out = new JsonObject();
        JsonObject hw = new JsonObject();
        JsonObject total = new JsonObject();
        total.addProperty("value", hits.size());
        total.addProperty("relation", "eq");
        hw.add("total", total);
        Float max = null;
        if (!r.hasSort) {
            for (Hit h : hits) {
                max = max == null ? h.score : Math.max(max, h.score);
            }
        }
        hw.add("max_score", max == null ? JsonNull.INSTANCE : new JsonPrimitive(max));
        JsonArray arr = new JsonArray();
        Query.Hl hl = null;
        if (params.has("highlight")) {
            r.highlight = params.getAsJsonObject("highlight");
            hl = new Query.Hl();
        }
        for (Hit h : hits.subList(Math.min(r.from, hits.size()), Math.min(hits.size(), r.from + r.size))) {
            arr.add(renderHit(h, r, hl));
        }
        hw.add("hits", arr);
        out.add("hits", hw);
        return out;
    }

    // ------------------------------------------------------------------ count

    long count(List<PostgresSearchStore.Resolved> targets, Query.Node query, Double minScore, int terminateAfter) throws SQLException {
        List<Partition> parts = partitions(targets, false, minScore == null && !hasSetLevelQuery(query) ? query : null);
        query.prepare(new Vector.Prep(parts.stream().map(p -> p.ix).toList()));
        long n = 0;
        for (Partition p : parts) {
            List<Query.Node> aliasNodes = new ArrayList<>();
            for (JsonObject f : p.aliasFilters) {
                aliasNodes.add(QueryParser.parse(f));
            }
            long local = 0;
            for (PostgresSearchStore.Doc d : p.ix.corpus) {
                Query.DocCtx ctx = new Query.DocCtx(p.ix, d);
                double s = query.score(ctx);
                if (!Query.matched(s) || (minScore != null && s < minScore)) {
                    continue;
                }
                boolean ok = true;
                for (Query.Node an : aliasNodes) {
                    ok &= Query.matched(an.score(ctx));
                }
                if (ok) {
                    local++;
                    if (terminateAfter > 0 && local >= terminateAfter) {
                        break;
                    }
                }
            }
            n += local;
        }
        return n;
    }

    /** All documents matching {@code query} (delete_by_query / update_by_query / reindex). */
    List<PostgresSearchStore.Doc> matching(List<PostgresSearchStore.Resolved> targets, Query.Node query) throws SQLException {
        List<Partition> parts = partitions(targets, false, hasSetLevelQuery(query) ? null : query);
        query.prepare(new Vector.Prep(parts.stream().map(p -> p.ix).toList()));
        List<PostgresSearchStore.Doc> out = new ArrayList<>();
        for (Partition p : parts) {
            List<Query.Node> aliasNodes = new ArrayList<>();
            for (JsonObject f : p.aliasFilters) {
                aliasNodes.add(QueryParser.parse(f));
            }
            for (PostgresSearchStore.Doc d : p.ix.corpus) {
                Query.DocCtx ctx = new Query.DocCtx(p.ix, d);
                if (Query.matched(query.score(ctx))) {
                    boolean ok = true;
                    for (Query.Node an : aliasNodes) {
                        ok &= Query.matched(an.score(ctx));
                    }
                    if (ok) {
                        out.add(d);
                    }
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ scroll / pit

    static String newId() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString((UUID.randomUUID().toString()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    static long parseKeepAlive(String s) {
        if (s == null) {
            return 60_000;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d+)(ms|s|m|h|d|micros|nanos)?").matcher(s);
        if (!m.matches()) {
            throw new OpenSearchException("illegal_argument_exception", "failed to parse setting [keep_alive] with value [" + s + "] as a time value: unit is missing or unrecognized");
        }
        long n = Long.parseLong(m.group(1));
        String u = m.group(2) == null ? "ms" : m.group(2);
        return switch (u) {
            case "s" -> n * 1000;
            case "m" -> n * 60_000;
            case "h" -> n * 3_600_000;
            case "d" -> n * 86_400_000;
            default -> n;
        };
    }

    JsonObject startScroll(JsonObject firstPage, Result res, Req req, List<PostgresSearchStore.Resolved> targets) {
        long keep = parseKeepAlive(req.scroll);
        if (keep > 86_400_000L) {
            throw new OpenSearchException("illegal_argument_exception", "Keep alive for request (" + req.scroll + ") is too large. It must be less than (24h). This limit can be set by changing the [search.max_keep_alive] cluster level setting.");
        }
        List<JsonObject> rendered = new ArrayList<>();
        Query.Hl hl = null;
        if (req.highlight != null) {
            hl = new Query.Hl();
            req.query.collect(hl);
        }
        for (Hit h : res.hits) {
            rendered.add(renderHit(h, req, hl));
        }
        sweep();
        String id = newId();
        Scroll sc = new Scroll(rendered, res.total, req.size, System.currentTimeMillis() + keep, firstPage.getAsJsonObject("_shards"));
        sc.next = Math.min(rendered.size(), req.from + req.size);
        scrolls.put(id, sc);
        firstPage.addProperty("_scroll_id", id);
        return firstPage;
    }

    static void checkScrollId(String id) {
        try {
            Base64.getUrlDecoder().decode(id);
        } catch (IllegalArgumentException e) {
            OpenSearchException oe = new OpenSearchException("illegal_argument_exception", "Cannot parse scroll id");
            oe.extra.add("caused_by", causedBy("illegal_argument_exception", e.getMessage()));
            throw oe;
        }
    }

    private static JsonObject causedBy(String type, String reason) {
        JsonObject cb = new JsonObject();
        cb.addProperty("type", type);
        cb.addProperty("reason", reason);
        return cb;
    }

    JsonObject continueScroll(String id, String keepAlive, boolean totalAsInt) {
        checkScrollId(id);
        sweep();
        Scroll sc = scrolls.get(id);
        if (sc == null || sc.expiresAt < System.currentTimeMillis()) {
            scrolls.remove(id);
            throw new OpenSearchException("search_context_missing_exception", "No search context found for id [" + id + "]", 404).asShardLevel();
        }
        if (keepAlive != null) {
            sc.expiresAt = System.currentTimeMillis() + parseKeepAlive(keepAlive);
        }
        int end = Math.min(sc.hits.size(), sc.next + sc.size);
        JsonArray arr = new JsonArray();
        for (int i = sc.next; i < end; i++) {
            arr.add(sc.hits.get(i));
        }
        sc.next = end;
        JsonObject out = new JsonObject();
        out.addProperty("_scroll_id", id);
        out.addProperty("took", 1);
        out.addProperty("timed_out", false);
        out.add("_shards", sc.shards);
        JsonObject hw = new JsonObject();
        if (totalAsInt) {
            hw.addProperty("total", sc.total);
        } else {
            JsonObject t = new JsonObject();
            t.addProperty("value", sc.total);
            t.addProperty("relation", "eq");
            hw.add("total", t);
        }
        Float max = null;
        for (JsonObject h : sc.hits) {
            if (h.has("_score") && !h.get("_score").isJsonNull()) {
                max = max == null ? h.get("_score").getAsFloat() : Math.max(max, h.get("_score").getAsFloat());
            }
        }
        hw.add("max_score", max == null ? JsonNull.INSTANCE : new JsonPrimitive(max));
        hw.add("hits", arr);
        out.add("hits", hw);
        return out;
    }

    int clearScrolls(List<String> ids) {
        int n = 0;
        if (ids == null || ids.contains("_all")) {
            n = scrolls.size();
            scrolls.clear();
            return n;
        }
        for (String id : ids) {
            checkScrollId(id);
            if (scrolls.remove(id) != null) {
                n++;
            }
        }
        return n;
    }

    private void sweep() {
        long now = System.currentTimeMillis();
        scrolls.values().removeIf(s -> s.expiresAt < now);
        pits.values().removeIf(p -> p.expiresAt < now);
    }

    String createPit(List<String> indices, String keepAlive) {
        sweep();
        String id = newId();
        pits.put(id, new Pit(indices, System.currentTimeMillis() + parseKeepAlive(keepAlive)));
        return id;
    }

    Pit pit(String id) {
        Pit p = pits.get(id);
        if (p == null || p.expiresAt < System.currentTimeMillis()) {
            throw new OpenSearchException("search_context_missing_exception", "No search context found for id [" + id + "]", 404);
        }
        return p;
    }

    boolean deletePit(String id) {
        return pits.remove(id) != null;
    }

    List<String> pitIds() {
        sweep();
        return new ArrayList<>(pits.keySet());
    }

    Map<String, Pit> pits() {
        return pits;
    }

    static Map<String, Object> asMap() {
        return new LinkedHashMap<>();
    }

    static Map<String, Integer> counters() {
        return new HashMap<>();
    }
}
