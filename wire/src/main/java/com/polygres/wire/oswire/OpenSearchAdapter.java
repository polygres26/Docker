package com.polygres.wire.oswire;

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
 * <p>V1 query DSL coverage, per this feature's staged plan: {@code bool} (must/filter/should/
 * must_not), {@code term}, {@code range}, {@code match}, {@code match_all}, and the OpenSearch
 * k-NN plugin's {@code knn} query clause (<a href="https://opensearch.org/docs/latest/search-plugins/knn/">
 * {@code {"query": {"knn": {"&lt;field&gt;": {"vector": [...], "k": N}}}}}</a>). Aggregations and
 * hybrid (hybrid-query/multi-clause-with-knn) scoring are explicitly V2 -- see the class's
 * top-level javadoc in {@code SearchRequest} for why that's a deliberate staging choice, not an
 * oversight.
 */
public final class OpenSearchAdapter {

    private OpenSearchAdapter() {
    }

    public static SearchRequest parseSearch(String index, JsonObject body) {
        int size = body.has("size") ? body.get("size").getAsInt() : 10;
        int from = body.has("from") ? body.get("from").getAsInt() : 0;
        List<String> projection = parseSource(body);
        List<SortField> sort = parseSort(body);

        if (body.has("query") && body.getAsJsonObject("query").has("knn")) {
            return parseKnnQuery(index, body, projection, sort, size, from);
        }

        SearchFilter filter = body.has("query") ? parseQuery(body.getAsJsonObject("query")) : new SearchFilter.MatchAll();
        return new SearchRequest(index, projection, filter, null, null, null, null, size, from, sort, List.of());
    }

    private static SearchRequest parseKnnQuery(String index, JsonObject body, List<String> projection,
            List<SortField> sort, int size, int from) {
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
                SearchRequest.DistanceMetric.COSINE, k, from, sort, List.of());
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
     * clause names V1 documents are recognized -- an unrecognized clause name is rejected loudly
     * (not silently ignored, which would make a filtered search quietly return unfiltered
     * results) via {@link com.polygres.wire.oswire.OpenSearchException}. */
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
                "oswire V1 supports match_all/term/range/match/bool queries (aggregations and other clause "
                        + "types are not yet implemented) -- got: " + query.keySet());
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

    private static Object scalar(JsonElement e) {
        if (e.getAsJsonPrimitive().isNumber()) {
            return e.getAsDouble();
        }
        if (e.getAsJsonPrimitive().isBoolean()) {
            return e.getAsBoolean();
        }
        return e.getAsString();
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

    /** Renders a {@link SearchResult} into OpenSearch's real {@code _search} response shape. */
    public static JsonObject renderSearchResponse(SearchResult result, long tookMillis) {
        JsonObject response = new JsonObject();
        response.addProperty("took", tookMillis);
        response.addProperty("timed_out", false);

        JsonObject hitsWrapper = new JsonObject();
        JsonObject total = new JsonObject();
        total.addProperty("value", result.total());
        total.addProperty("relation", "eq");
        hitsWrapper.add("total", total);
        double maxScore = result.hits().stream().mapToDouble(SearchHit::score).max().orElse(0.0);
        hitsWrapper.addProperty("max_score", result.hits().isEmpty() ? (Double) null : maxScore);

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
        return response;
    }
}
