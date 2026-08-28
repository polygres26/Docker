package com.nexagres.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates OpenSearch's real {@code _search} request JSON into {@link SearchRequest}, and
 * {@link SearchResult} back into OpenSearch's real response JSON -- the only place that knows
 * OpenSearch's specific wire shape. A future Qdrant adapter (see {@link SearchRequest}'s javadoc)
 * would be this same shape aimed at Qdrant's REST JSON instead, sharing everything downstream.
 *
 * <p>V1 query DSL coverage: {@code bool} (must/filter/should/must_not), {@code term},
 * {@code range}, {@code match}, {@code match_all}, and the OpenSearch k-NN plugin's {@code knn}
 * query clause (<a href="https://opensearch.org/docs/latest/search-plugins/knn/">
 * {@code {"query": {"knn": {"&lt;field&gt;": {"vector": [...], "k": N}}}}}</a>), sort, pagination.
 *
 * <p>V2 adds: {@code aggs}/{@code aggregations} ({@code terms} buckets with nested
 * {@code avg}/{@code sum}/{@code min}/{@code max}/{@code value_count} metrics, or a bare metric --
 * see {@link #parseAggregations}), and the neural-search plugin's {@code hybrid} compound query
 * ({@code {"query": {"hybrid": {"queries": [...]}}}}) -- see {@link #parseHybridQuery} and
 * {@code PostgresSearchStore#searchHybrid}'s javadoc for the score-fusion algorithm. Real
 * relevance/similarity scoring (not V1's flat {@code 1.0}/raw distance) is implemented in
 * {@code PostgresSearchStore}, not here -- this class only ever translates the request shape.
 */
public final class OpenSearchAdapter {

    private OpenSearchAdapter() {
    }

    public static SearchRequest parseSearch(String index, JsonObject body) {
        int size = body.has("size") ? body.get("size").getAsInt() : 10;
        int from = body.has("from") ? body.get("from").getAsInt() : 0;
        List<String> projection = parseSource(body);
        List<SortField> sort = parseSort(body);
        List<Aggregation> aggregations = parseAggregations(body);

        if (body.has("query") && body.getAsJsonObject("query").has("hybrid")) {
            return parseHybridQuery(index, body, projection, sort, size, from);
        }
        if (body.has("query") && body.getAsJsonObject("query").has("knn")) {
            return parseKnnQuery(index, body, projection, sort, size, from, aggregations);
        }

        SearchFilter filter = body.has("query") ? parseQuery(body.getAsJsonObject("query")) : new SearchFilter.MatchAll();
        return new SearchRequest(index, projection, filter, null, null, null, null, size, from, sort,
                aggregations, List.of());
    }

    private static SearchRequest parseKnnQuery(String index, JsonObject body, List<String> projection,
            List<SortField> sort, int size, int from, List<Aggregation> aggregations) {
        JsonObject knn = body.getAsJsonObject("query").getAsJsonObject("knn");
        String field = knn.keySet().iterator().next();
        JsonObject spec = knn.getAsJsonObject(field);
        JsonArray vectorArr = spec.getAsJsonArray("vector");
        float[] vector = new float[vectorArr.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = vectorArr.get(i).getAsFloat();
        }
        int k = spec.has("k") ? spec.get("k").getAsInt() : size;
        SearchFilter filter = spec.has("filter") ? parseQuery(spec.getAsJsonObject("filter")) : new SearchFilter.MatchAll();
        return new SearchRequest(index, projection, filter, null, vector, field,
                SearchRequest.DistanceMetric.COSINE, k, from, sort, aggregations, List.of());
    }

    /**
     * {@code {"query": {"hybrid": {"queries": [<clause>, <clause>, ...]}}}} -- real OpenSearch's
     * neural-search hybrid query. Each element of {@code queries} is parsed exactly like a
     * top-level {@code query} clause would be (including {@code knn}), becoming one complete,
     * independent {@link SearchRequest} in {@link SearchRequest#hybridSubRequests()}; outer
     * {@code sort}/pagination apply only to the fused result, not to any individual sub-query --
     * see {@code PostgresSearchStore#searchHybrid}.
     */
    private static SearchRequest parseHybridQuery(String index, JsonObject body, List<String> projection,
            List<SortField> sort, int size, int from) {
        JsonArray subQueries = body.getAsJsonObject("query").getAsJsonObject("hybrid").getAsJsonArray("queries");
        if (subQueries.isEmpty()) {
            throw new OpenSearchException("parsing_exception", "hybrid query requires at least one entry in \"queries\"");
        }
        List<SearchRequest> subRequests = new ArrayList<>();
        for (JsonElement e : subQueries) {
            JsonObject clause = e.getAsJsonObject();
            if (clause.has("knn")) {
                JsonObject wrapped = new JsonObject();
                wrapped.add("query", clause);
                subRequests.add(parseKnnQuery(index, wrapped, projection, List.of(), size, 0, List.of()));
            } else {
                subRequests.add(new SearchRequest(index, projection, parseQuery(clause), null, null, null, null,
                        size, 0, List.of(), List.of(), List.of()));
            }
        }
        return new SearchRequest(index, projection, new SearchFilter.MatchAll(), null, null, null, null,
                size, from, sort, List.of(), subRequests);
    }

    private static List<String> parseSource(JsonObject body) {
        if (!body.has("_source")) {
            return null;
        }
        JsonElement src = body.get("_source");
        if (src.isJsonPrimitive() && !src.getAsBoolean()) {
            return List.of();
        }
        if (src.isJsonArray()) {
            List<String> fields = new ArrayList<>();
            for (JsonElement e : src.getAsJsonArray()) {
                fields.add(e.getAsString());
            }
            return fields;
        }
        return null;
    }

    private static List<SortField> parseSort(JsonObject body) {
        if (!body.has("sort")) {
            return List.of();
        }
        List<SortField> sort = new ArrayList<>();
        for (JsonElement e : body.getAsJsonArray("sort")) {
            if (e.isJsonPrimitive()) {
                sort.add(new SortField(e.getAsString(), true));
                continue;
            }
            JsonObject o = e.getAsJsonObject();
            String field = o.keySet().iterator().next();
            JsonElement spec = o.get(field);
            boolean asc = !spec.isJsonObject() || !spec.getAsJsonObject().has("order")
                    || !"desc".equalsIgnoreCase(spec.getAsJsonObject().get("order").getAsString());
            sort.add(new SortField(field, asc));
        }
        return sort;
    }

    /** Recursively compiles one OpenSearch query-DSL clause into {@link SearchFilter}. Only the
     * clause names V1/V2 document are recognized -- an unrecognized clause name is rejected loudly
     * (not silently ignored, which would make a filtered search quietly return unfiltered
     * results) via {@link com.nexagres.wire.oswire.OpenSearchException}. */
    static SearchFilter parseQuery(JsonObject query) {
        if (query.has("match_all")) {
            return new SearchFilter.MatchAll();
        }
        if (query.has("term")) {
            JsonObject term = query.getAsJsonObject("term");
            String field = term.keySet().iterator().next();
            JsonElement value = term.get(field);
            return new SearchFilter.Term(field, scalar(value));
        }
        if (query.has("range")) {
            JsonObject range = query.getAsJsonObject("range");
            String field = range.keySet().iterator().next();
            JsonObject bounds = range.getAsJsonObject(field);
            return new SearchFilter.Range(field,
                    bounds.has("gte") ? scalar(bounds.get("gte")) : null,
                    bounds.has("lte") ? scalar(bounds.get("lte")) : null,
                    bounds.has("gt") ? scalar(bounds.get("gt")) : null,
                    bounds.has("lt") ? scalar(bounds.get("lt")) : null);
        }
        if (query.has("match")) {
            JsonObject match = query.getAsJsonObject("match");
            String field = match.keySet().iterator().next();
            JsonElement value = match.get(field);
            String text = value.isJsonObject() ? value.getAsJsonObject().get("query").getAsString() : value.getAsString();
            return new SearchFilter.Match(field, text);
        }
        if (query.has("bool")) {
            JsonObject bool = query.getAsJsonObject("bool");
            return new SearchFilter.Bool(
                    parseClauseList(bool, "must"), parseClauseList(bool, "filter"),
                    parseClauseList(bool, "should"), parseClauseList(bool, "must_not"));
        }
        throw new OpenSearchException("parsing_exception",
                "oswire supports match_all/term/range/match/bool/knn/hybrid queries -- got: " + query.keySet());
    }

    private static List<SearchFilter> parseClauseList(JsonObject bool, String key) {
        if (!bool.has(key)) {
            return List.of();
        }
        List<SearchFilter> clauses = new ArrayList<>();
        JsonElement el = bool.get(key);
        if (el.isJsonArray()) {
            for (JsonElement e : el.getAsJsonArray()) {
                clauses.add(parseQuery(e.getAsJsonObject()));
            }
        } else {
            clauses.add(parseQuery(el.getAsJsonObject()));
        }
        return clauses;
    }

    /**
     * A term/match value can arrive two ways depending on the client: the short form
     * {@code {"term": {"field": "value"}}} sends the scalar directly; the "long form"
     * {@code {"term": {"field": {"value": "value", "boost": 1.0}}}} wraps it in an object --
     * real OpenSearch accepts both, and the official {@code opensearch-java} client always sends
     * the long form (confirmed live: parsing the short-form assumption alone threw
     * {@code Not a JSON Primitive} against a real client's request). Unwraps one level of
     * {@code {"value": ...}} before falling through to plain-scalar handling either way.
     */
    private static Object scalar(JsonElement e) {
        JsonElement value = (e.isJsonObject() && e.getAsJsonObject().has("value"))
                ? e.getAsJsonObject().get("value") : e;
        if (value.getAsJsonPrimitive().isNumber()) {
            return value.getAsDouble();
        }
        if (value.getAsJsonPrimitive().isBoolean()) {
            return value.getAsBoolean();
        }
        return value.getAsString();
    }

    /**
     * V1 supports exactly one vector field per collection, extracted automatically instead of
     * requiring a separate mapping API: when indexing a document, this scans its top-level fields
     * (JSON key order) for the first one whose value is a plain JSON array of numbers and treats
     * it as the row's embedding -- real OpenSearch k-NN documents already store their vector as
     * an ordinary top-level field (typed {@code knn_vector} in the index mapping), so this reads
     * the same document shape a real OpenSearch client sends, just without consulting a mapping
     * to know which field name to expect. A document with more than one numeric-array field only
     * gets the first one indexed for k-NN -- everything is still preserved verbatim in
     * {@code _source} either way. {@code null} if no such field exists.
     */
    static float[] extractVector(JsonObject doc) {
        for (String key : doc.keySet()) {
            JsonElement value = doc.get(key);
            if (!value.isJsonArray() || value.getAsJsonArray().isEmpty()) {
                continue;
            }
            JsonArray arr = value.getAsJsonArray();
            boolean allNumeric = true;
            for (JsonElement e : arr) {
                if (!e.isJsonPrimitive() || !e.getAsJsonPrimitive().isNumber()) {
                    allNumeric = false;
                    break;
                }
            }
            if (allNumeric) {
                float[] vector = new float[arr.size()];
                for (int i = 0; i < vector.length; i++) {
                    vector[i] = arr.get(i).getAsFloat();
                }
                return vector;
            }
        }
        return null;
    }

    /**
     * Parses OpenSearch's {@code aggs} (or its long-form alias {@code aggregations}) into
     * {@link Aggregation}s. Recognizes {@code terms} (with {@code field}, {@code size}, and
     * nested {@code aggs}/{@code aggregations}) and the five metric aggregations
     * ({@code avg}/{@code sum}/{@code min}/{@code max}/{@code value_count}, each shaped
     * {@code {"<name>": {"<type>": {"field": "<field>"}}}}). Same "unrecognized clause fails
     * loudly" policy as {@link #parseQuery} -- an aggregation type this adapter doesn't recognize
     * throws rather than silently omitting it from the response.
     */
    static List<Aggregation> parseAggregations(JsonObject body) {
        JsonObject aggs = body.has("aggs") ? body.getAsJsonObject("aggs")
                : body.has("aggregations") ? body.getAsJsonObject("aggregations") : null;
        if (aggs == null) {
            return List.of();
        }
        List<Aggregation> result = new ArrayList<>();
        for (String name : aggs.keySet()) {
            result.add(parseOneAggregation(name, aggs.getAsJsonObject(name)));
        }
        return result;
    }

    private static Aggregation parseOneAggregation(String name, JsonObject spec) {
        if (spec.has("terms")) {
            JsonObject terms = spec.getAsJsonObject("terms");
            String field = terms.get("field").getAsString();
            int size = terms.has("size") ? terms.get("size").getAsInt() : 10;
            List<Aggregation> subAggs = parseAggregations(spec);
            return new Aggregation.Terms(name, field, size, subAggs);
        }
        for (var entry : java.util.Map.of(
                "avg", Aggregation.MetricType.AVG, "sum", Aggregation.MetricType.SUM,
                "min", Aggregation.MetricType.MIN, "max", Aggregation.MetricType.MAX,
                "value_count", Aggregation.MetricType.COUNT).entrySet()) {
            if (spec.has(entry.getKey())) {
                String field = spec.getAsJsonObject(entry.getKey()).get("field").getAsString();
                return new Aggregation.Metric(name, entry.getValue(), field);
            }
        }
        throw new OpenSearchException("parsing_exception",
                "oswire V2 supports terms/avg/sum/min/max/value_count aggregations -- got: " + spec.keySet());
    }

    /** Renders a {@link SearchResult} into OpenSearch's real {@code _search} response shape.
     * {@code typedKeys} controls whether aggregation result keys get real OpenSearch's
     * {@code typed_keys} type prefix -- see {@link #typedKey}'s javadoc. */
    public static JsonObject renderSearchResponse(SearchResult result, long tookMillis, boolean typedKeys) {
        JsonObject response = new JsonObject();
        response.addProperty("took", tookMillis);
        response.addProperty("timed_out", false);
        // Real OpenSearch's per-request shard-acknowledgement summary -- V1 has no shards to
        // report against, but official clients require the field regardless (confirmed live with
        // opensearch-java, same as OpenSearchWireServer#addVersionFields's _shards).
        JsonObject shards = new JsonObject();
        shards.addProperty("total", 1);
        shards.addProperty("successful", 1);
        shards.addProperty("failed", 0);
        response.add("_shards", shards);

        JsonObject hitsWrapper = new JsonObject();
        JsonObject total = new JsonObject();
        total.addProperty("value", result.total());
        total.addProperty("relation", "eq");
        hitsWrapper.add("total", total);
        // A ternary with one branch `(Double) null` and the other a primitive `double` unboxes the
        // null branch and throws NPE at runtime -- Java picks a common numeric type for the
        // conditional expression and unboxes both sides to get it, it doesn't stay boxed just
        // because one literal branch was cast to Double. Found live: this had been latent since
        // V1 (never exercised by a test with zero hits) until a size:0 aggregation-only request
        // hit it. An explicit if/else avoids the unboxing entirely.
        if (result.hits().isEmpty()) {
            hitsWrapper.add("max_score", com.google.gson.JsonNull.INSTANCE);
        } else {
            double maxScore = result.hits().stream().mapToDouble(SearchHit::score).max().orElseThrow();
            hitsWrapper.addProperty("max_score", maxScore);
        }

        JsonArray hitsArray = new JsonArray();
        for (SearchHit hit : result.hits()) {
            JsonObject h = new JsonObject();
            h.addProperty("_id", hit.id());
            h.addProperty("_score", hit.score());
            h.add("_source", hit.source());
            hitsArray.add(h);
        }
        hitsWrapper.add("hits", hitsArray);
        response.add("hits", hitsWrapper);

        if (!result.aggregations().isEmpty()) {
            JsonObject aggsWrapper = new JsonObject();
            for (AggregationResult agg : result.aggregations()) {
                aggsWrapper.add(key(agg, typedKeys), renderAggregationResult(agg, typedKeys));
            }
            response.add("aggregations", aggsWrapper);
        }
        return response;
    }

    /**
     * Real OpenSearch's {@code typed_keys} response format ({@code "<type>#<name>"} instead of a
     * bare {@code "<name>"}) is opt-in per request via {@code ?typed_keys=true} -- NOT
     * unconditional. Official high-level clients (confirmed live: {@code opensearch-java}) send
     * that query param themselves because their strict typed deserializer requires it; low-level
     * clients that hand back a plain response body (confirmed live: {@code opensearch-py} never
     * sends the param) get real OpenSearch's own default, bare names. {@code typedKeys} is
     * threaded down from the actual incoming request's query string
     * ({@code OpenSearchWireServer#handleSearch}), not a fixed choice made here.
     */
    private static String key(AggregationResult agg, boolean typedKeys) {
        return typedKeys ? agg.openSearchType() + "#" + agg.name() : agg.name();
    }

    private static JsonObject renderAggregationResult(AggregationResult agg, boolean typedKeys) {
        JsonObject rendered = new JsonObject();
        switch (agg) {
            case AggregationResult.SingleValue(String ignoredName, Double value, String ignoredType) ->
                    rendered.addProperty("value", value);
            case AggregationResult.Buckets(String ignoredName, List<AggregationResult.Bucket> buckets, long sumOtherDocCount) -> {
                JsonArray bucketsArray = new JsonArray();
                for (AggregationResult.Bucket bucket : buckets) {
                    JsonObject b = new JsonObject();
                    b.addProperty("key", bucket.key());
                    b.addProperty("doc_count", bucket.docCount());
                    for (AggregationResult sub : bucket.subResults()) {
                        b.add(key(sub, typedKeys), renderAggregationResult(sub, typedKeys));
                    }
                    bucketsArray.add(b);
                }
                rendered.addProperty("doc_count_error_upper_bound", 0);
                rendered.addProperty("sum_other_doc_count", sumOtherDocCount);
                rendered.add("buckets", bucketsArray);
            }
        }
        return rendered;
    }
}
