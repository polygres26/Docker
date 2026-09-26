package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * OpenSearch aggregations executed over the matched documents of every host (so results are exact across shards):
 * buckets (terms, rare_terms, multi_terms, histogram, date_histogram, range, date_range, filter, filters, global,
 * missing, nested, reverse_nested, sampler, adjacency_matrix, composite), metrics (min, max, sum, avg, value_count,
 * stats, extended_stats, cardinality (exact), percentiles, percentile_ranks, top_hits, weighted_avg, string_stats,
 * geo_bounds, geo_centroid) and pipelines (sum/avg/min/max/stats/percentiles_bucket, cumulative_sum, derivative,
 * serial_diff, bucket_sort). Scripted aggregations and significant_terms / geo grids are not supported.
 */
final class Aggs {

    interface Hooks {
        JsonObject topHits(JsonObject params, List<Query.DocCtx> docs);

        List<Query.DocCtx> allDocs();
    }

    static final class Spec {
        String name;
        String type;
        JsonObject params;
        final List<Spec> subs = new ArrayList<>();
        JsonObject meta;
    }

    private static final Set<String> BUCKET = Set.of("terms", "rare_terms", "multi_terms", "histogram", "date_histogram", "range", "date_range",
            "filter", "filters", "global", "missing", "nested", "reverse_nested", "sampler", "adjacency_matrix", "composite", "diversified_sampler");
    private static final Set<String> METRIC = Set.of("min", "max", "sum", "avg", "value_count", "stats", "extended_stats", "cardinality",
            "percentiles", "percentile_ranks", "top_hits", "weighted_avg", "string_stats", "geo_bounds", "geo_centroid", "median_absolute_deviation");
    private static final Set<String> PIPELINE = Set.of("sum_bucket", "avg_bucket", "min_bucket", "max_bucket", "stats_bucket",
            "extended_stats_bucket", "percentiles_bucket", "cumulative_sum", "derivative", "serial_diff", "bucket_sort", "bucket_script",
            "bucket_selector", "moving_avg", "moving_fn");
    private static final Set<String> UNSUPPORTED = Set.of("significant_terms", "significant_text", "geohash_grid", "geotile_grid", "geo_distance",
            "ip_range", "auto_date_histogram", "variable_width_histogram", "scripted_metric", "matrix_stats", "boxplot", "t_test", "rate",
            "geo_line", "min_max_bucket", "categorize_text", "children", "parent");

    private final Hooks hooks;

    Aggs(Hooks hooks) {
        this.hooks = hooks;
    }

    // ------------------------------------------------------------ parsing

    static List<Spec> parse(JsonObject body) {
        List<Spec> out = new ArrayList<>();
        for (Map.Entry<String, JsonElement> e : body.entrySet()) {
            if (!e.getValue().isJsonObject()) {
                throw QueryParser.err("Expected [START_OBJECT] under [" + e.getKey() + "], but got a [" + (e.getValue().isJsonArray() ? "START_ARRAY" : "VALUE") + "] in [aggs]");
            }
            out.add(parseOne(e.getKey(), e.getValue().getAsJsonObject()));
        }
        return out;
    }

    private static Spec parseOne(String name, JsonObject o) {
        Spec s = new Spec();
        s.name = name;
        for (Map.Entry<String, JsonElement> e : o.entrySet()) {
            String k = e.getKey();
            if (k.equals("aggs") || k.equals("aggregations")) {
                s.subs.addAll(parse(e.getValue().getAsJsonObject()));
            } else if (k.equals("meta")) {
                s.meta = e.getValue().getAsJsonObject();
            } else if (s.type != null) {
                throw QueryParser.err("Found two aggregation type definitions in [" + name + "]: [" + s.type + "] and [" + k + "]");
            } else {
                s.type = k;
                if (!e.getValue().isJsonObject()) {
                    throw QueryParser.err("Expected [START_OBJECT] under [" + k + "], but got a [VALUE_STRING] in [" + name + "]");
                }
                s.params = e.getValue().getAsJsonObject();
            }
        }
        if (s.type == null) {
            throw new OpenSearchException("illegal_argument_exception", "Invalid aggregation [" + name + "]: expected one aggregation type");
        }
        if (!BUCKET.contains(s.type) && !METRIC.contains(s.type) && !PIPELINE.contains(s.type)) {
            if (UNSUPPORTED.contains(s.type)) {
                throw new OpenSearchException("parsing_exception", "[" + s.type + "] aggregation is not supported by Warp's OpenSearch frontend");
            }
            throw new OpenSearchException("parsing_exception", "Unknown aggregation type [" + s.type + "]").with("line", "1");
        }
        if (s.params.has("script")) {
            throw new OpenSearchException("illegal_argument_exception", "scripted aggregations are not supported by Warp's OpenSearch frontend");
        }
        validateParams(s);
        if (METRIC.contains(s.type) && !s.subs.isEmpty()) {
            throw new OpenSearchException("aggregation_initialization_exception",
                    "Aggregator [" + name + "] of type [" + s.type + "] cannot accept sub-aggregations");
        }
        return s;
    }

    private static void validateParams(Spec s) {
        JsonObject p = s.params;
        switch (s.type) {
            case "extended_stats" -> {
                if (p.has("sigma") && p.get("sigma").getAsDouble() < 0) {
                    throw new OpenSearchException("illegal_argument_exception", "[sigma] must be greater than or equal to 0. Found ["
                            + p.get("sigma").getAsDouble() + "] in [" + s.name + "]");
                }
            }
            case "cardinality" -> {
                if (p.has("precision_threshold") && p.get("precision_threshold").getAsLong() < 0) {
                    throw new OpenSearchException("illegal_argument_exception", "[precisionThreshold] must be greater than or equal to 0. Found ["
                            + p.get("precision_threshold").getAsLong() + "] in [" + s.name + "]");
                }
            }
            case "percentiles", "percentile_ranks" -> {
                if (p.has("tdigest") && p.getAsJsonObject("tdigest").has("compression") && p.getAsJsonObject("tdigest").get("compression").getAsDouble() < 0) {
                    throw new OpenSearchException("illegal_argument_exception", "[compression] must be greater than or equal to 0. Found ["
                            + p.getAsJsonObject("tdigest").get("compression").getAsDouble() + "] in [" + s.name + "]");
                }
                if (p.has("hdr") && p.getAsJsonObject("hdr").has("number_of_significant_value_digits")) {
                    int d = p.getAsJsonObject("hdr").get("number_of_significant_value_digits").getAsInt();
                    if (d < 0 || d > 5) {
                        throw new OpenSearchException("illegal_argument_exception", "[numberOfSignificantValueDigits] must be between 0 and 5");
                    }
                }
            }
            case "median_absolute_deviation" -> {
                if (p.has("compression") && p.get("compression").getAsDouble() <= 0) {
                    throw new OpenSearchException("illegal_argument_exception", "[compression] must be greater than 0. Found ["
                            + p.get("compression").getAsDouble() + "] in [" + s.name + "]");
                }
            }
            default -> {
            }
        }
    }

    static final int MAX_BUCKETS = 65535;

    // ------------------------------------------------------------ execution

    /** Runs all specs (siblings) over {@code docs}; returns the {@code aggregations} object. */
    JsonObject run(List<Spec> specs, List<Query.DocCtx> docs, boolean typedKeys) {
        return run(specs, docs, typedKeys, false);
    }

    private static final Set<String> PARENT_PIPELINE = Set.of("cumulative_sum", "derivative", "serial_diff", "bucket_sort", "bucket_script",
            "bucket_selector", "moving_avg", "moving_fn");

    JsonObject run(List<Spec> specs, List<Query.DocCtx> docs, boolean typedKeys, boolean nested) {
        JsonObject out = new JsonObject();
        List<Spec> pipelines = new ArrayList<>();
        for (Spec s : specs) {
            if (PIPELINE.contains(s.type)) {
                if (!(nested && PARENT_PIPELINE.contains(s.type))) {
                    pipelines.add(s);
                }
                continue;
            }
            JsonObject r = runOne(s, docs, typedKeys);
            if (s.meta != null) {
                r.add("meta", s.meta);
            }
            String tn = typeName(s, r);
            out.add(typedKeys ? tn + "#" + s.name : s.name, r);
        }
        for (Spec s : pipelines) {
            if (s.type.equals("bucket_sort") || s.type.equals("cumulative_sum") || s.type.equals("derivative") || s.type.equals("serial_diff")
                    || s.type.equals("bucket_script") || s.type.equals("bucket_selector")) {
                throw new OpenSearchException("action_request_validation_exception", "Validation Failed: 1: " + s.type + " aggregation ["
                        + s.name + "] must be declared inside of another aggregation;");
            }
            JsonObject r = siblingPipeline(s, out);
            out.add(typedKeys ? pipelineTypeName(s) + "#" + s.name : s.name, r);
        }
        return out;
    }

    private String pipelineTypeName(Spec s) {
        return switch (s.type) {
            case "stats_bucket" -> "stats_bucket";
            case "extended_stats_bucket" -> "extended_stats_bucket";
            case "percentiles_bucket" -> "percentiles_bucket";
            case "sum_bucket", "avg_bucket" -> "simple_value";
            case "min_bucket", "max_bucket" -> "bucket_metric_value";
            default -> "simple_value";
        };
    }

    private static final Map<String, String> TYPE_NAMES = Map.ofEntries(Map.entry("percentiles", "tdigest_percentiles"),
            Map.entry("percentile_ranks", "tdigest_percentile_ranks"), Map.entry("cardinality", "cardinality"),
            Map.entry("value_count", "value_count"), Map.entry("filter", "filter"), Map.entry("filters", "filters"),
            Map.entry("string_stats", "string_stats"), Map.entry("weighted_avg", "weighted_avg"),
            Map.entry("median_absolute_deviation", "median_absolute_deviation"));

    private String typeName(Spec s, JsonObject result) {
        if (s.type.equals("terms") || s.type.equals("rare_terms")) {
            String prefix = s.type.equals("terms") ? "" : "";
            String kind = result.has("_ktype") ? result.get("_ktype").getAsString() : "s";
            result.remove("_ktype");
            return kind + (s.type.equals("terms") ? "terms" : "rareterms");
        }
        result.remove("_ktype");
        return TYPE_NAMES.getOrDefault(s.type, s.type);
    }

    private JsonObject runOne(Spec s, List<Query.DocCtx> docs, boolean tk) {
        return switch (s.type) {
            case "terms" -> terms(s, docs, tk);
            case "rare_terms" -> rareTerms(s, docs, tk);
            case "multi_terms" -> multiTerms(s, docs, tk);
            case "histogram" -> histogram(s, docs, tk, false);
            case "date_histogram" -> histogram(s, docs, tk, true);
            case "range" -> ranges(s, docs, tk, false);
            case "date_range" -> ranges(s, docs, tk, true);
            case "filter" -> filter(s, docs, tk);
            case "filters" -> filters(s, docs, tk);
            case "global" -> subBucket(s, hooks.allDocs(), tk);
            case "missing" -> missing(s, docs, tk);
            case "nested" -> nested(s, docs, tk);
            case "reverse_nested" -> reverseNested(s, docs, tk);
            case "sampler", "diversified_sampler" -> sampler(s, docs, tk);
            case "adjacency_matrix" -> adjacency(s, docs, tk);
            case "composite" -> composite(s, docs, tk);
            case "top_hits" -> hooks.topHits(s.params, docs);
            default -> metric(s, docs);
        };
    }

    private JsonObject subBucket(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject o = new JsonObject();
        o.addProperty("doc_count", docs.size());
        addSubs(o, s, docs, tk);
        return o;
    }

    private void addSubs(JsonObject bucket, Spec s, List<Query.DocCtx> docs, boolean tk) {
        if (s.subs.isEmpty()) {
            return;
        }
        JsonObject subs = run(s.subs, docs, tk, true);
        for (var e : subs.entrySet()) {
            bucket.add(e.getKey(), e.getValue());
        }
    }

    // ------------------------------------------------------------ value access

    private static String field(Spec s) {
        JsonObject p = s.params;
        if (!p.has("field")) {
            throw new OpenSearchException("parsing_exception", "Required one of fields [field, script], but none were specified. ");
        }
        return p.get("field").getAsString();
    }

    /** Typed values (Long / Double / Boolean / String) of a doc for a field, honouring {@code missing}. */
    private List<Object> vals(Query.DocCtx d, String field, JsonElement missing, boolean forAgg) {
        Mappings.Field f = d.ix.mappings.get(field);
        if (f != null && f.nestedPath != null && d.prefix.isEmpty()) {
            f = null;
        } else if (f != null && !d.prefix.isEmpty() && !f.path.startsWith(d.prefix)) {
            f = null;
        }
        List<Object> out = new ArrayList<>();
        if (f != null) {
            if (f.isText() && forAgg) {
                throw new OpenSearchException("illegal_argument_exception",
                        "Text fields are not optimised for operations that require per-document field data like aggregations and sorting, "
                                + "so these operations are disabled by default. Please use a keyword field instead. Alternatively, set "
                                + "fielddata=true on [" + field + "] in order to load field data by uninverting the inverted index. "
                                + "Note that this can use significant memory.").asShardLevel();
            }
            out = d.values(f);
            if (f.type.equals("geo_point")) {
                out = new ArrayList<>();
            }
        }
        if (out.isEmpty() && missing != null && !missing.isJsonNull()) {
            if (f != null) {
                Object v = Values.parse(f, missing, d.ix.settings, d.ix.zone());
                if (v != null) {
                    out.add(v);
                }
            } else {
                out.add(missing.isJsonPrimitive() && missing.getAsJsonPrimitive().isNumber() ? (Object) missing.getAsDouble() : missing.getAsString());
            }
        }
        return out;
    }

    private Mappings.Field fieldOf(List<Query.DocCtx> docs, String field) {
        for (Query.DocCtx d : docs) {
            Mappings.Field f = d.ix.mappings.get(field);
            if (f != null) {
                return f;
            }
        }
        return null;
    }

    private static Mappings.Field anyField(Aggs a, String field, List<Query.DocCtx> docs) {
        return a.fieldOf(docs, field);
    }

    private static double num(Object o) {
        return Values.toDouble(o);
    }

    private static JsonElement jnum(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return JsonNull.INSTANCE;
        }
        return new JsonPrimitive(v);
    }

    private static void addKey(JsonObject b, Object key, Mappings.Field f, String format) {
        if (key instanceof String s) {
            b.addProperty("key", s);
        } else if (key instanceof Boolean bo) {
            b.addProperty("key", bo ? 1 : 0);
            b.addProperty("key_as_string", bo ? "true" : "false");
        } else if (key instanceof Long l) {
            b.addProperty("key", l);
            if (f != null && (f.type.equals("date") || f.type.equals("date_nanos"))) {
                b.addProperty("key_as_string", Dates.format(l, format != null ? format : f.format, java.time.ZoneOffset.UTC));
            }
        } else if (key instanceof Double dv) {
            if (dv == Math.rint(dv) && Math.abs(dv) < 1e15 && f != null && f.isInteger()) {
                b.addProperty("key", dv.longValue());
            } else {
                b.addProperty("key", dv);
            }
        } else {
            b.addProperty("key", String.valueOf(key));
        }
    }

    // ------------------------------------------------------------ terms

    private static final class Bucket {
        Object key;
        List<Query.DocCtx> docs = new ArrayList<>();
        JsonObject subs;
    }

    private JsonObject terms(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject p = s.params;
        String field = field(s);
        int size = p.has("size") ? p.get("size").getAsInt() : 10;
        if (size <= 0 && p.has("size")) {
            throw new OpenSearchException("illegal_argument_exception", "[size] must be greater than 0. Found [" + size + "] in [" + s.name + "]");
        }
        long minDoc = p.has("min_doc_count") ? p.get("min_doc_count").getAsLong() : 1;
        JsonElement missing = p.get("missing");
        Map<Object, Bucket> buckets = new LinkedHashMap<>();
        for (Query.DocCtx d : docs) {
            List<Object> vs = vals(d, field, missing, true);
            Set<Object> seen = new HashSet<>();
            for (Object v : vs) {
                if (seen.add(v)) {
                    Bucket b = buckets.computeIfAbsent(v, k -> {
                        Bucket nb = new Bucket();
                        nb.key = k;
                        return nb;
                    });
                    b.docs.add(d);
                }
            }
        }
        Mappings.Field f = fieldOf(docs, field);
        // longs and doubles of the same value (an integral double and a long across indices) are one bucket
        boolean anyDouble = buckets.keySet().stream().anyMatch(k -> k instanceof Double);
        if (anyDouble && buckets.keySet().stream().anyMatch(k -> k instanceof Long)) {
            Map<Object, Bucket> unified = new LinkedHashMap<>();
            for (Bucket b : buckets.values()) {
                Object k = b.key instanceof Long l ? (Object) l.doubleValue() : b.key;
                Bucket t = unified.computeIfAbsent(k, kk -> {
                    Bucket nb = new Bucket();
                    nb.key = kk;
                    return nb;
                });
                for (Query.DocCtx dd : b.docs) {
                    if (!t.docs.contains(dd)) {
                        t.docs.add(dd);
                    }
                }
            }
            buckets = unified;
        }
        List<Bucket> list = new ArrayList<>(buckets.values());
        list = includeExclude(list, p, f);
        if (minDoc > 1) {
            list.removeIf(b -> b.docs.size() < minDoc);
        }
        // compute sub-aggregations first when ordering by them
        boolean needSubs = !s.subs.isEmpty();
        if (needSubs) {
            for (Bucket b : list) {
                b.subs = run(s.subs, b.docs, tk, true);
            }
        }
        Comparator<Bucket> cmp = orderComparator(p.get("order"), false, s);
        list.sort(cmp);
        long total = list.stream().mapToLong(b -> b.docs.size()).sum();
        List<Bucket> shown = list.size() > size ? list.subList(0, size) : list;
        long shownCount = shown.stream().mapToLong(b -> b.docs.size()).sum();
        JsonObject out = new JsonObject();
        out.addProperty("doc_count_error_upper_bound", 0);
        out.addProperty("sum_other_doc_count", totalDocsCounted(list) - shownCount);
        JsonArray arr = new JsonArray();
        boolean showErr = p.has("show_term_doc_count_error") && p.get("show_term_doc_count_error").getAsBoolean();
        for (Bucket b : shown) {
            JsonObject bo = new JsonObject();
            addKey(bo, b.key, f, null);
            bo.addProperty("doc_count", b.docs.size());
            if (showErr) {
                bo.addProperty("doc_count_error_upper_bound", 0);
            }
            if (b.subs != null) {
                for (var e : b.subs.entrySet()) {
                    bo.add(e.getKey(), e.getValue());
                }
            }
            arr.add(bo);
        }
        applyBucketPipelines(s, arr);
        out.add("buckets", arr);
        out.addProperty("_ktype", f == null || f.isString() ? "s" : (f.isInteger() || f.type.equals("date") || f.type.equals("boolean") ? "l" : "d"));
        return out;
    }

    private static long totalDocsCounted(List<Bucket> l) {
        return l.stream().mapToLong(b -> b.docs.size()).sum();
    }

    private List<Bucket> includeExclude(List<Bucket> list, JsonObject p, Mappings.Field f) {
        List<Bucket> out = new ArrayList<>(list);
        if (p.has("include")) {
            JsonElement inc = p.get("include");
            out.removeIf(b -> !matchesFilter(inc, b.key, f));
        }
        if (p.has("exclude")) {
            JsonElement exc = p.get("exclude");
            out.removeIf(b -> matchesFilter(exc, b.key, f));
        }
        return out;
    }

    private static boolean matchesFilter(JsonElement flt, Object bucketKey, Mappings.Field f) {
        String key = String.valueOf(bucketKey);
        if (flt.isJsonArray()) {
            for (JsonElement e : flt.getAsJsonArray()) {
                if (f != null && !f.isString()) {
                    Object parsed = Values.parse(f, e, null, java.time.ZoneOffset.UTC);
                    if (parsed != null && Values.compare(parsed, bucketKey) == 0) {
                        return true;
                    }
                } else if (e.getAsString().equals(key)) {
                    return true;
                }
            }
            return false;
        }
        JsonElement f2 = flt;
        if (f2.isJsonPrimitive()) {
            return Pattern.matches(f2.getAsString(), key);
        }
        return false;
    }

    /** Orders per the agg's {@code order} (default: count desc, key asc; histograms: key asc). */
    private Comparator<Bucket> orderComparator(JsonElement order, boolean keyDefault, Spec s) {
        List<Comparator<Bucket>> cs = new ArrayList<>();
        List<JsonObject> orders = new ArrayList<>();
        if (order != null) {
            if (order.isJsonArray()) {
                order.getAsJsonArray().forEach(e -> orders.add(e.getAsJsonObject()));
            } else {
                orders.add(order.getAsJsonObject());
            }
        }
        for (JsonObject o : orders) {
            for (var e : o.entrySet()) {
                boolean asc = e.getValue().getAsString().equalsIgnoreCase("asc");
                String k = e.getKey();
                Comparator<Bucket> c;
                if (k.equals("_count")) {
                    c = Comparator.comparingLong(b -> b.docs.size());
                } else if (k.equals("_key") || k.equals("_term") || k.equals("_time")) {
                    c = (a, b) -> a.key instanceof List<?> && b.key instanceof List<?> ? compareLists((List<Object>) a.key, (List<Object>) b.key) : Values.compare(a.key, b.key);
                } else {
                    String path = k;
                    c = (a, b) -> Double.compare(orderValue(a, path), orderValue(b, path));
                }
                cs.add(asc ? c : c.reversed());
            }
        }
        if (cs.isEmpty()) {
            if (keyDefault) {
                cs.add((a, b) -> Values.compare(a.key, b.key));
            } else {
                cs.add(Comparator.<Bucket>comparingLong(b -> b.docs.size()).reversed());
            }
        }
        if (!keyDefault) {
            cs.add((a, b) -> a.key instanceof List<?> && b.key instanceof List<?> ? compareLists((List<Object>) a.key, (List<Object>) b.key) : Values.compare(a.key, b.key));
        }
        return (a, b) -> {
            for (Comparator<Bucket> c : cs) {
                int r = c.compare(a, b);
                if (r != 0) {
                    return r;
                }
            }
            return 0;
        };
    }

    private double orderValue(Bucket b, String path) {
        if (b.subs == null) {
            return Double.NaN;
        }
        String[] parts = path.split(">", 2);
        JsonElement e = b.subs.get(parts[0].contains(".") ? parts[0].substring(0, parts[0].indexOf('.')) : parts[0]);
        if (e == null) {
            return Double.NaN;
        }
        String metric = parts[0].contains(".") ? parts[0].substring(parts[0].indexOf('.') + 1) : "value";
        JsonElement v = e.getAsJsonObject().get(metric);
        return v == null || v.isJsonNull() ? Double.NaN : v.getAsDouble();
    }

    private JsonObject rareTerms(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject p = s.params;
        String field = field(s);
        long maxDoc = p.has("max_doc_count") ? p.get("max_doc_count").getAsLong() : 1;
        Map<Object, Bucket> buckets = new LinkedHashMap<>();
        for (Query.DocCtx d : docs) {
            for (Object v : new HashSet<>(vals(d, field, p.get("missing"), true))) {
                buckets.computeIfAbsent(v, k -> {
                    Bucket nb = new Bucket();
                    nb.key = k;
                    return nb;
                }).docs.add(d);
            }
        }
        Mappings.Field f = fieldOf(docs, field);
        List<Bucket> list = new ArrayList<>(buckets.values());
        list.removeIf(b -> b.docs.size() > maxDoc);
        list = includeExclude(list, p, f);
        list.sort((a, b) -> a.docs.size() != b.docs.size() ? Integer.compare(a.docs.size(), b.docs.size()) : Values.compare(a.key, b.key));
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        for (Bucket b : list) {
            JsonObject bo = new JsonObject();
            addKey(bo, b.key, f, null);
            bo.addProperty("doc_count", b.docs.size());
            addSubs(bo, s, b.docs, tk);
            arr.add(bo);
        }
        out.add("buckets", arr);
        out.addProperty("_ktype", f == null || f.isString() ? "s" : "l");
        return out;
    }

    private JsonObject multiTerms(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject p = s.params;
        JsonArray terms = p.getAsJsonArray("terms");
        int size = p.has("size") ? p.get("size").getAsInt() : 10;
        long minDoc = p.has("min_doc_count") ? p.get("min_doc_count").getAsLong() : 1;
        List<Mappings.Field> fields = new ArrayList<>();
        for (JsonElement t : terms) {
            fields.add(fieldOf(docs, t.getAsJsonObject().get("field").getAsString()));
        }
        Map<List<Object>, Bucket> buckets = new LinkedHashMap<>();
        for (Query.DocCtx d : docs) {
            List<List<Object>> per = new ArrayList<>();
            boolean ok = true;
            for (JsonElement t : terms) {
                List<Object> v = vals(d, t.getAsJsonObject().get("field").getAsString(), t.getAsJsonObject().get("missing"), true);
                if (v.isEmpty()) {
                    ok = false;
                    break;
                }
                per.add(new ArrayList<>(new java.util.LinkedHashSet<>(v)));
            }
            if (!ok) {
                continue;
            }
            for (List<Object> combo : cartesian(per)) {
                buckets.computeIfAbsent(combo, k -> {
                    Bucket b = new Bucket();
                    b.key = k;
                    return b;
                }).docs.add(d);
            }
        }
        List<Bucket> list = new ArrayList<>(buckets.values());
        list.removeIf(b -> b.docs.size() < minDoc);
        if (!s.subs.isEmpty()) {
            for (Bucket b : list) {
                b.subs = run(s.subs, b.docs, tk, true);
            }
        }
        list.sort(orderComparator(p.get("order"), false, s));
        JsonObject out = new JsonObject();
        out.addProperty("doc_count_error_upper_bound", 0);
        List<Bucket> shown = list.size() > size ? list.subList(0, size) : list;
        out.addProperty("sum_other_doc_count", totalDocsCounted(list) - totalDocsCounted(shown));
        JsonArray arr = new JsonArray();
        for (Bucket b : shown) {
            JsonObject bo = new JsonObject();
            JsonArray keys = new JsonArray();
            List<String> strs = new ArrayList<>();
            List<Object> ks = (List<Object>) b.key;
            for (int i = 0; i < ks.size(); i++) {
                Object k = ks.get(i);
                Mappings.Field f = fields.get(i);
                if (k instanceof Boolean bo2) {
                    keys.add(bo2);
                    strs.add(String.valueOf(bo2));
                } else if (k instanceof Long l && f != null && f.type.equals("date")) {
                    String fmt = Dates.format(l, f.format, java.time.ZoneOffset.UTC);
                    keys.add(fmt);
                    strs.add(fmt);
                } else if (k instanceof Number n) {
                    keys.add(new JsonPrimitive(n));
                    strs.add(String.valueOf(n));
                } else {
                    keys.add(String.valueOf(k));
                    strs.add(String.valueOf(k));
                }
            }
            bo.add("key", keys);
            bo.addProperty("key_as_string", String.join("|", strs));
            bo.addProperty("doc_count", b.docs.size());
            if (b.subs != null) {
                for (var e : b.subs.entrySet()) {
                    bo.add(e.getKey(), e.getValue());
                }
            }
            arr.add(bo);
        }
        out.add("buckets", arr);
        return out;
    }

    private static int compareLists(List<Object> a, List<Object> b) {
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            int c = Values.compare(a.get(i), b.get(i));
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private static List<List<Object>> cartesian(List<List<Object>> per) {
        List<List<Object>> out = new ArrayList<>();
        out.add(new ArrayList<>());
        for (List<Object> vs : per) {
            List<List<Object>> next = new ArrayList<>();
            for (List<Object> pre : out) {
                for (Object v : vs) {
                    List<Object> n = new ArrayList<>(pre);
                    n.add(v);
                    next.add(n);
                }
            }
            out = next;
        }
        return out;
    }

    // ------------------------------------------------------------ histogram

    private JsonObject histogram(Spec s, List<Query.DocCtx> docs, boolean tk, boolean date) {
        JsonObject p = s.params;
        String field = field(s);
        Mappings.Field f = fieldOf(docs, field);
        long minDoc = p.has("min_doc_count") ? p.get("min_doc_count").getAsLong() : 0;
        boolean keyed = p.has("keyed") && p.get("keyed").getAsBoolean();
        TreeMap<Double, Bucket> buckets = new TreeMap<>();
        Rounding r = date ? Rounding.of(p, f) : Rounding.numeric(p);
        for (Query.DocCtx d : docs) {
            Set<Double> seen = new HashSet<>();
            for (Object v : vals(d, field, p.get("missing"), true)) {
                double key = r.round(num(v));
                if (seen.add(key)) {
                    buckets.computeIfAbsent(key, k -> {
                        Bucket b = new Bucket();
                        b.key = k;
                        return b;
                    }).docs.add(d);
                }
            }
        }
        // fill gaps / extended bounds
        double lo = Double.NaN, hi = Double.NaN;
        if (!buckets.isEmpty()) {
            lo = buckets.firstKey();
            hi = buckets.lastKey();
        }
        if (p.has("extended_bounds")) {
            JsonObject eb = p.getAsJsonObject("extended_bounds");
            if (eb.has("min")) {
                double m = r.round(boundValue(eb.get("min"), f, date, r));
                lo = Double.isNaN(lo) ? m : Math.min(lo, m);
            }
            if (eb.has("max")) {
                double m = r.round(boundValue(eb.get("max"), f, date, r));
                hi = Double.isNaN(hi) ? m : Math.max(hi, m);
            }
        }
        if (minDoc == 0 && !Double.isNaN(lo)) {
            for (double k = lo; k <= hi; k = r.next(k)) {
                if (buckets.size() >= MAX_BUCKETS) {
                    throw new OpenSearchException("too_many_buckets_exception", "Trying to create too many buckets. Must be less than or equal to: ["
                            + MAX_BUCKETS + "] but was [" + (buckets.size() + 1) + "]. This limit can be set by changing the [search.max_buckets] cluster level setting.")
                            .with("max_buckets", String.valueOf(MAX_BUCKETS)).asShardLevel();
                }
                double kk = k;
                buckets.computeIfAbsent(kk, x -> {
                    Bucket b = new Bucket();
                    b.key = x;
                    return b;
                });
            }
        }
        if (p.has("hard_bounds")) {
            JsonObject hb = p.getAsJsonObject("hard_bounds");
            double hmin = hb.has("min") ? boundValue(hb.get("min"), f, date, r) : -Double.MAX_VALUE;
            double hmax = hb.has("max") ? boundValue(hb.get("max"), f, date, r) : Double.MAX_VALUE;
            buckets.keySet().removeIf(k -> k < hmin || k > hmax);
        }
        List<Bucket> list = new ArrayList<>(buckets.values());
        list.removeIf(b -> b.docs.size() < minDoc);
        if (!s.subs.isEmpty() || p.has("order")) {
            for (Bucket b : list) {
                b.subs = run(s.subs, b.docs, tk, true);
            }
        }
        if (p.has("order")) {
            list.sort(orderComparator(p.get("order"), true, s));
        }
        String format = p.has("format") ? p.get("format").getAsString() : null;
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        JsonObject keyedObj = new JsonObject();
        for (Bucket b : list) {
            JsonObject bo = new JsonObject();
            double k = (Double) b.key;
            if (date) {
                bo.addProperty("key_as_string", Dates.format((long) k, format != null ? format : f != null ? f.format : null, r.zone));
                bo.addProperty("key", (long) k);
            } else if (format != null) {
                bo.addProperty("key_as_string", new java.text.DecimalFormat(format).format(k));
                bo.addProperty("key", k);
            } else {
                bo.addProperty("key", k);
            }
            bo.addProperty("doc_count", b.docs.size());
            if (b.subs != null) {
                for (var e : b.subs.entrySet()) {
                    bo.add(e.getKey(), e.getValue());
                }
            }
            if (keyed) {
                keyedObj.add(bo.has("key_as_string") ? bo.get("key_as_string").getAsString() : String.valueOf(k), bo);
            } else {
                arr.add(bo);
            }
        }
        if (keyed) {
            out.add("buckets", keyedObj);
        } else {
            applyBucketPipelines(s, arr);
            out.add("buckets", arr);
        }
        return out;
    }

    private double boundValue(JsonElement e, Mappings.Field f, boolean date, Rounding r) {
        if (date) {
            if (e.getAsJsonPrimitive().isNumber()) {
                return e.getAsDouble();
            }
            return Dates.parseMath(e.getAsString(), f != null ? f.format : null, r.zone, false, System.currentTimeMillis());
        }
        return e.getAsDouble();
    }

    /** Bucket key rounding for histogram / date_histogram. */
    static final class Rounding {
        double interval;
        double offset;
        String calendar;
        int calendarN = 1;
        ZoneId zone = java.time.ZoneOffset.UTC;

        static Rounding numeric(JsonObject p) {
            Rounding r = new Rounding();
            if (!p.has("interval")) {
                throw new OpenSearchException("parsing_exception", "Required [interval]");
            }
            r.interval = p.get("interval").getAsDouble();
            if (r.interval <= 0) {
                throw new OpenSearchException("illegal_argument_exception", "[interval] must be >0 for histogram aggregation [" + "histogram" + "]");
            }
            r.offset = p.has("offset") ? p.get("offset").getAsDouble() : 0;
            return r;
        }

        static Rounding of(JsonObject p, Mappings.Field f) {
            Rounding r = new Rounding();
            r.zone = Dates.zone(p.has("time_zone") ? p.get("time_zone").getAsString() : null);
            String spec = null;
            boolean fixed = false;
            if (p.has("calendar_interval")) {
                spec = p.get("calendar_interval").getAsString();
            } else if (p.has("fixed_interval")) {
                spec = p.get("fixed_interval").getAsString();
                fixed = true;
            } else if (p.has("interval")) {
                spec = p.get("interval").getAsString();
                fixed = !spec.matches("(1)?(s|m|h|d|w|M|q|y|second|minute|hour|day|week|month|quarter|year)");
            } else {
                throw new OpenSearchException("illegal_argument_exception", "[date_histogram] requires either [calendar_interval] or [fixed_interval]");
            }
            if (fixed) {
                java.util.regex.Matcher m = Pattern.compile("(\\d+)(ms|s|m|h|d)").matcher(spec);
                if (!m.matches()) {
                    throw new OpenSearchException("illegal_argument_exception", "failed to parse setting [date_histogram.fixedInterval] with value ["
                            + spec + "] as a time value: unit is missing or unrecognized");
                }
                r.interval = Long.parseLong(m.group(1)) * switch (m.group(2)) {
                    case "s" -> 1000L;
                    case "m" -> 60000L;
                    case "h" -> 3600000L;
                    case "d" -> 86400000L;
                    default -> 1L;
                };
            } else {
                String u = switch (spec) {
                    case "1s", "second", "s" -> "s";
                    case "1m", "minute", "m" -> "m";
                    case "1h", "hour", "h" -> "h";
                    case "1d", "day", "d" -> "d";
                    case "1w", "week", "w" -> "w";
                    case "1M", "month", "M" -> "M";
                    case "1q", "quarter", "q" -> "q";
                    case "1y", "year", "y" -> "y";
                    default -> throw new OpenSearchException("illegal_argument_exception", "The supplied interval [" + spec
                            + "] could not be parsed as a calendar interval.");
                };
                r.calendar = u;
            }
            if (p.has("offset")) {
                String o = p.get("offset").getAsString();
                boolean neg = o.startsWith("-");
                String body = o.replaceFirst("^[+-]", "");
                java.util.regex.Matcher m = Pattern.compile("(\\d+)(ms|s|m|h|d)").matcher(body);
                if (m.matches()) {
                    long ms = Long.parseLong(m.group(1)) * switch (m.group(2)) {
                        case "s" -> 1000L;
                        case "m" -> 60000L;
                        case "h" -> 3600000L;
                        case "d" -> 86400000L;
                        default -> 1L;
                    };
                    r.offset = neg ? -ms : ms;
                }
            }
            return r;
        }

        double round(double v) {
            if (calendar == null) {
                return Math.floor((v - offset) / interval) * interval + offset;
            }
            ZonedDateTime t = Instant.ofEpochMilli((long) (v - offset)).atZone(zone);
            ZonedDateTime f = switch (calendar) {
                case "s" -> t.truncatedTo(ChronoUnit.SECONDS);
                case "m" -> t.truncatedTo(ChronoUnit.MINUTES);
                case "h" -> t.truncatedTo(ChronoUnit.HOURS);
                case "d" -> t.truncatedTo(ChronoUnit.DAYS);
                case "w" -> t.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).truncatedTo(ChronoUnit.DAYS);
                case "M" -> t.with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS);
                case "q" -> t.withMonth(((t.getMonthValue() - 1) / 3) * 3 + 1).with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS);
                default -> t.with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS);
            };
            return f.toInstant().toEpochMilli() + offset;
        }

        double next(double key) {
            if (calendar == null) {
                return key + interval;
            }
            ZonedDateTime t = Instant.ofEpochMilli((long) (key - offset)).atZone(zone);
            ZonedDateTime n = switch (calendar) {
                case "s" -> t.plusSeconds(1);
                case "m" -> t.plusMinutes(1);
                case "h" -> t.plusHours(1);
                case "d" -> t.plusDays(1);
                case "w" -> t.plusWeeks(1);
                case "M" -> t.plusMonths(1);
                case "q" -> t.plusMonths(3);
                default -> t.plusYears(1);
            };
            return n.toInstant().toEpochMilli() + offset;
        }
    }

    // ------------------------------------------------------------ range / filters / misc buckets

    private JsonObject ranges(Spec s, List<Query.DocCtx> docs, boolean tk, boolean date) {
        JsonObject p = s.params;
        String field = field(s);
        Mappings.Field f = fieldOf(docs, field);
        boolean keyed = p.has("keyed") && p.get("keyed").getAsBoolean();
        String format = p.has("format") ? p.get("format").getAsString() : f != null ? f.format : null;
        if (!p.has("ranges") || !p.get("ranges").isJsonArray()) {
            throw new OpenSearchException("illegal_argument_exception", "No [ranges] specified for the [" + s.name + "] aggregation");
        }
        JsonArray arr = new JsonArray();
        JsonObject keyedOut = new JsonObject();
        for (JsonElement re : p.getAsJsonArray("ranges")) {
            JsonObject r = re.getAsJsonObject();
            Double from = r.has("from") && !r.get("from").isJsonNull() ? bound(r.get("from"), date, format, f) : null;
            Double to = r.has("to") && !r.get("to").isJsonNull() ? bound(r.get("to"), date, format, f) : null;
            List<Query.DocCtx> in = new ArrayList<>();
            for (Query.DocCtx d : docs) {
                for (Object v : vals(d, field, p.get("missing"), true)) {
                    double x = num(v);
                    if ((from == null || x >= from) && (to == null || x < to)) {
                        in.add(d);
                        break;
                    }
                }
            }
            JsonObject bo = new JsonObject();
            String key;
            if (r.has("key")) {
                key = r.get("key").getAsString();
            } else if (date) {
                key = (from == null ? "*" : Dates.format(from.longValue(), format, java.time.ZoneOffset.UTC)) + "-"
                        + (to == null ? "*" : Dates.format(to.longValue(), format, java.time.ZoneOffset.UTC));
            } else {
                key = (from == null ? "*" : String.valueOf(from)) + "-" + (to == null ? "*" : String.valueOf(to));
            }
            bo.addProperty("key", key);
            if (from != null) {
                bo.addProperty("from", from);
                if (date) {
                    bo.addProperty("from_as_string", Dates.format(from.longValue(), format, java.time.ZoneOffset.UTC));
                }
            }
            if (to != null) {
                bo.addProperty("to", to);
                if (date) {
                    bo.addProperty("to_as_string", Dates.format(to.longValue(), format, java.time.ZoneOffset.UTC));
                }
            }
            bo.addProperty("doc_count", in.size());
            addSubs(bo, s, in, tk);
            if (keyed) {
                bo.remove("key");
                keyedOut.add(key, bo);
            } else {
                arr.add(bo);
            }
        }
        JsonObject out = new JsonObject();
        out.add("buckets", keyed ? keyedOut : arr);
        return out;
    }

    private double bound(JsonElement e, boolean date, String format, Mappings.Field f) {
        if (date && !(e.getAsJsonPrimitive().isNumber())) {
            return Dates.parseMath(e.getAsString(), format, java.time.ZoneOffset.UTC, false, System.currentTimeMillis());
        }
        return e.getAsDouble();
    }

    private List<Query.DocCtx> matching(List<Query.DocCtx> docs, JsonElement query) {
        Query.Node q = QueryParser.parse(query);
        List<Query.DocCtx> in = new ArrayList<>();
        for (Query.DocCtx d : docs) {
            if (Query.matched(q.score(d))) {
                in.add(d);
            }
        }
        return in;
    }

    private JsonObject filter(Spec s, List<Query.DocCtx> docs, boolean tk) {
        return subBucket(s, matching(docs, s.params), tk);
    }

    private JsonObject filters(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonElement fe = s.params.get("filters");
        if (fe == null) {
            throw new OpenSearchException("illegal_argument_exception", "[filters] cannot be empty.");
        }
        boolean isArray = fe.isJsonArray();
        boolean keyed = !isArray;
        if (s.params.has("keyed")) {
            keyed = s.params.get("keyed").getAsBoolean();
        }
        LinkedHashMap<String, JsonElement> fs = new LinkedHashMap<>();
        if (isArray) {
            int i = 0;
            for (JsonElement e : fe.getAsJsonArray()) {
                fs.put(String.valueOf(i++), e);
            }
        } else {
            for (var e : fe.getAsJsonObject().entrySet()) {
                fs.put(e.getKey(), e.getValue());
            }
        }
        if (fs.isEmpty()) {
            throw new OpenSearchException("illegal_argument_exception", "[filters] cannot be empty.");
        }
        JsonObject keyedOut = new JsonObject();
        JsonArray arr = new JsonArray();
        Set<Query.DocCtx> matchedAny = new HashSet<>();
        for (var e : fs.entrySet()) {
            List<Query.DocCtx> in = matching(docs, e.getValue());
            matchedAny.addAll(in);
            JsonObject bo = new JsonObject();
            bo.addProperty("doc_count", in.size());
            addSubs(bo, s, in, tk);
            if (keyed) {
                keyedOut.add(e.getKey(), bo);
            } else {
                arr.add(bo);
            }
        }
        boolean other = s.params.has("other_bucket") && s.params.get("other_bucket").getAsBoolean() || s.params.has("other_bucket_key");
        if (other) {
            List<Query.DocCtx> rest = new ArrayList<>();
            for (Query.DocCtx d : docs) {
                if (!matchedAny.contains(d)) {
                    rest.add(d);
                }
            }
            JsonObject bo = new JsonObject();
            bo.addProperty("doc_count", rest.size());
            addSubs(bo, s, rest, tk);
            String key = s.params.has("other_bucket_key") ? s.params.get("other_bucket_key").getAsString() : "_other_";
            if (keyed) {
                keyedOut.add(key, bo);
            } else {
                arr.add(bo);
            }
        }
        JsonObject out = new JsonObject();
        out.add("buckets", keyed ? keyedOut : arr);
        return out;
    }

    private JsonObject missing(Spec s, List<Query.DocCtx> docs, boolean tk) {
        String field = field(s);
        List<Query.DocCtx> in = new ArrayList<>();
        for (Query.DocCtx d : docs) {
            if (s.params.has("missing")) {
                continue;
            }
            if (vals(d, field, null, false).isEmpty() && !Query.Exists.exists(d, field)) {
                in.add(d);
            }
        }
        return subBucket(s, in, tk);
    }

    private JsonObject nested(Spec s, List<Query.DocCtx> docs, boolean tk) {
        String path = s.params.get("path").getAsString();
        List<Query.DocCtx> out = new ArrayList<>();
        for (Query.DocCtx d : docs) {
            String rel = path.startsWith(d.prefix) ? path.substring(d.prefix.length()) : path;
            for (JsonElement e : Mappings.values(d.obj, rel)) {
                if (e.isJsonObject()) {
                    out.add(d.nested(e.getAsJsonObject(), path + "."));
                }
            }
        }
        return subBucket(s, out, tk);
    }

    private JsonObject reverseNested(Spec s, List<Query.DocCtx> docs, boolean tk) {
        List<Query.DocCtx> out = new ArrayList<>();
        Set<Object> seen = new HashSet<>();
        for (Query.DocCtx d : docs) {
            Query.DocCtx root = d;
            if (s.params.has("path")) {
                String path = s.params.get("path").getAsString();
                while (root.parent != null && !root.prefix.equals(path + ".")) {
                    root = root.parent;
                }
            } else {
                while (root.parent != null) {
                    root = root.parent;
                }
            }
            if (seen.add(System.identityHashCode(root.obj))) {
                out.add(root);
            }
        }
        return subBucket(s, out, tk);
    }

    private JsonObject sampler(Spec s, List<Query.DocCtx> docs, boolean tk) {
        int shard = s.params.has("shard_size") ? s.params.get("shard_size").getAsInt() : 100;
        List<Query.DocCtx> sorted = new ArrayList<>(docs);
        sorted.sort((a, b) -> Double.compare(Double.isNaN(b.score) ? 0 : b.score, Double.isNaN(a.score) ? 0 : a.score));
        return subBucket(s, sorted.size() > shard ? sorted.subList(0, shard) : sorted, tk);
    }

    private JsonObject adjacency(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject fs = s.params.getAsJsonObject("filters");
        String sep = s.params.has("separator") ? s.params.get("separator").getAsString() : "&";
        List<String> names = new ArrayList<>(new java.util.TreeSet<>(fs.keySet()));
        Map<String, Set<Query.DocCtx>> m = new HashMap<>();
        for (String n : names) {
            m.put(n, new HashSet<>(matching(docs, fs.get(n))));
        }
        JsonArray arr = new JsonArray();
        for (int i = 0; i < names.size(); i++) {
            for (int j = i; j < names.size(); j++) {
                List<Query.DocCtx> in = new ArrayList<>();
                for (Query.DocCtx d : docs) {
                    if (m.get(names.get(i)).contains(d) && m.get(names.get(j)).contains(d)) {
                        in.add(d);
                    }
                }
                if (!in.isEmpty()) {
                    JsonObject bo = new JsonObject();
                    bo.addProperty("key", i == j ? names.get(i) : names.get(i) + sep + names.get(j));
                    bo.addProperty("doc_count", in.size());
                    addSubs(bo, s, in, tk);
                    arr.add(bo);
                }
            }
        }
        JsonObject out = new JsonObject();
        out.add("buckets", arr);
        return out;
    }

    private JsonObject composite(Spec s, List<Query.DocCtx> docs, boolean tk) {
        JsonObject p = s.params;
        int size = p.has("size") ? p.get("size").getAsInt() : 10;
        JsonArray sources = p.getAsJsonArray("sources");
        if (sources == null || sources.isEmpty()) {
            throw new OpenSearchException("parsing_exception", "Required [sources]");
        }
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        List<JsonObject> confs = new ArrayList<>();
        List<Boolean> asc = new ArrayList<>();
        for (JsonElement se : sources) {
            var e = se.getAsJsonObject().entrySet().iterator().next();
            String n = e.getKey();
            if (names.contains(n)) {
                throw new OpenSearchException("illegal_argument_exception", "Composite source names must be unique, found duplicates: [" + n + "]");
            }
            names.add(n);
            var inner = e.getValue().getAsJsonObject().entrySet().iterator().next();
            types.add(inner.getKey());
            confs.add(inner.getValue().getAsJsonObject());
            asc.add(!inner.getValue().getAsJsonObject().has("order") || inner.getValue().getAsJsonObject().get("order").getAsString().equalsIgnoreCase("asc"));
        }
        Map<List<Object>, Bucket> buckets = new HashMap<>();
        for (Query.DocCtx d : docs) {
            List<List<Object>> per = new ArrayList<>();
            boolean ok = true;
            for (int i = 0; i < names.size(); i++) {
                JsonObject c = confs.get(i);
                List<Object> vs = vals(d, c.get("field").getAsString(), null, true);
                List<Object> keys = new ArrayList<>();
                if (types.get(i).equals("histogram")) {
                    Rounding r = Rounding.numeric(c);
                    for (Object v : vs) {
                        keys.add(r.round(num(v)));
                    }
                } else if (types.get(i).equals("date_histogram")) {
                    Rounding r = Rounding.of(c, fieldOf(docs, c.get("field").getAsString()));
                    for (Object v : vs) {
                        keys.add((long) r.round(num(v)));
                    }
                } else {
                    keys.addAll(vs);
                }
                if (keys.isEmpty()) {
                    if (c.has("missing_bucket") && c.get("missing_bucket").getAsBoolean()) {
                        keys.add("\u0000null");
                    } else {
                        ok = false;
                        break;
                    }
                }
                per.add(new ArrayList<>(new java.util.LinkedHashSet<>(keys)));
            }
            if (!ok) {
                continue;
            }
            for (List<Object> combo : cartesian(per)) {
                Bucket b = buckets.computeIfAbsent(combo, k -> {
                    Bucket nb = new Bucket();
                    nb.key = k;
                    return nb;
                });
                b.docs.add(d);
            }
        }
        List<Bucket> list = new ArrayList<>(buckets.values());
        Comparator<Bucket> cmp = (a, b) -> {
            for (int i = 0; i < names.size(); i++) {
                int c = compareNullable(((List<Object>) a.key).get(i), ((List<Object>) b.key).get(i));
                if (c != 0) {
                    return asc.get(i) ? c : -c;
                }
            }
            return 0;
        };
        list.sort(cmp);
        if (p.has("after")) {
            JsonObject after = p.getAsJsonObject("after");
            List<Object> ak = new ArrayList<>();
            for (int i = 0; i < names.size(); i++) {
                JsonElement e = after.get(names.get(i));
                ak.add(e == null || e.isJsonNull() ? "\u0000null" : e.getAsJsonPrimitive().isNumber()
                        ? (types.get(i).equals("date_histogram") ? (Object) e.getAsLong() : (Object) e.getAsDouble()) : e.getAsString());
            }
            Bucket probe = new Bucket();
            probe.key = ak;
            list.removeIf(b -> cmp.compare(b, probe) <= 0);
        }
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        List<Bucket> shown = list.size() > size ? list.subList(0, size) : list;
        for (Bucket b : shown) {
            JsonObject bo = new JsonObject();
            JsonObject key = new JsonObject();
            for (int i = 0; i < names.size(); i++) {
                Object k = ((List<Object>) b.key).get(i);
                if (k instanceof Long l && confs.get(i).has("format") && types.get(i).equals("date_histogram")) {
                    key.addProperty(names.get(i), Dates.format(l, confs.get(i).get("format").getAsString(),
                            Dates.zone(confs.get(i).has("time_zone") ? confs.get(i).get("time_zone").getAsString() : null)));
                } else if (k instanceof Long l) {
                    key.addProperty(names.get(i), l);
                } else if (k instanceof Double dd) {
                    key.addProperty(names.get(i), dd);
                } else if (k instanceof Boolean bb) {
                    key.addProperty(names.get(i), bb);
                } else if ("\u0000null".equals(k)) {
                    key.add(names.get(i), JsonNull.INSTANCE);
                } else {
                    key.addProperty(names.get(i), String.valueOf(k));
                }
            }
            bo.add("key", key);
            bo.addProperty("doc_count", b.docs.size());
            addSubs(bo, s, b.docs, tk);
            arr.add(bo);
        }
        if (!shown.isEmpty()) {
            out.add("after_key", arr.get(arr.size() - 1).getAsJsonObject().get("key"));
        }
        out.add("buckets", arr);
        return out;
    }

    private static int compareNullable(Object a, Object b) {
        boolean an = "\u0000null".equals(a), bn = "\u0000null".equals(b);
        if (an || bn) {
            return an && bn ? 0 : an ? -1 : 1;
        }
        return Values.compare(a, b);
    }

    // ------------------------------------------------------------ metrics

    private JsonObject metric(Spec s, List<Query.DocCtx> docs) {
        JsonObject p = s.params;
        JsonObject out = new JsonObject();
        if (s.type.equals("weighted_avg")) {
            String vf = p.getAsJsonObject("value").get("field").getAsString();
            String wf = p.getAsJsonObject("weight").get("field").getAsString();
            double num = 0, den = 0;
            for (Query.DocCtx d : docs) {
                List<Object> v = vals(d, vf, p.getAsJsonObject("value").get("missing"), true);
                List<Object> w = vals(d, wf, p.getAsJsonObject("weight").get("missing"), true);
                if (!v.isEmpty() && !w.isEmpty()) {
                    num += num(v.get(0)) * num(w.get(0));
                    den += num(w.get(0));
                }
            }
            out.add("value", den == 0 ? JsonNull.INSTANCE : new JsonPrimitive(num / den));
            return out;
        }
        if (s.type.equals("geo_bounds") || s.type.equals("geo_centroid")) {
            return geoMetric(s, docs);
        }
        String field = field(s);
        Mappings.Field f = fieldOf(docs, field);
        if (s.type.equals("string_stats")) {
            return stringStats(s, docs, field);
        }
        if (s.type.equals("cardinality")) {
            Set<Object> distinct = new HashSet<>();
            for (Query.DocCtx d : docs) {
                Mappings.Field df = d.field(field);
                if (df != null && df.isText()) {
                    throw new OpenSearchException("illegal_argument_exception", "Text fields are not optimised for operations that require per-document field data like aggregations and sorting, so these operations are disabled by default. Please use a keyword field instead. Alternatively, set fielddata=true on [" + field + "] in order to load field data by uninverting the inverted index. Note that this can use significant memory.").asShardLevel();
                }
                for (Object v : vals(d, field, p.get("missing"), true)) {
                    distinct.add(v instanceof Double dv && dv == Math.rint(dv) ? (Object) dv.longValue() : v);
                }
            }
            out.addProperty("value", distinct.size());
            return out;
        }
        if (s.type.equals("value_count")) {
            long n = 0;
            for (Query.DocCtx d : docs) {
                Mappings.Field df = d.field(field);
                if (df != null && df.isText()) {
                    n += d.tokens(df).isEmpty() ? 0 : 0;
                    throw new OpenSearchException("illegal_argument_exception", "Text fields are not optimised for operations that require per-document field data like aggregations and sorting, so these operations are disabled by default. Please use a keyword field instead. Alternatively, set fielddata=true on [" + field + "] in order to load field data by uninverting the inverted index. Note that this can use significant memory.").asShardLevel();
                }
                n += vals(d, field, p.get("missing"), true).size();
            }
            out.addProperty("value", n);
            return out;
        }
        List<Double> nums = new ArrayList<>();
        for (Query.DocCtx d : docs) {
            for (Object v : vals(d, field, p.get("missing"), true)) {
                if (v instanceof String str) {
                    if (f != null && f.isString()) {
                        throw new OpenSearchException("illegal_argument_exception", "Field [" + field + "] of type [" + f.type
                                + "] is not supported for aggregation [" + s.type + "]").asShardLevel();
                    }
                    continue;
                }
                nums.add(num(v));
            }
        }
        boolean isDate = f != null && f.type.equals("date");
        switch (s.type) {
            case "min", "max", "sum", "avg" -> {
                double r;
                if (nums.isEmpty()) {
                    r = s.type.equals("sum") ? 0 : Double.NaN;
                } else {
                    r = switch (s.type) {
                        case "min" -> nums.stream().mapToDouble(x -> x).min().getAsDouble();
                        case "max" -> nums.stream().mapToDouble(x -> x).max().getAsDouble();
                        case "sum" -> kahan(nums);
                        default -> kahan(nums) / nums.size();
                    };
                }
                out.add("value", jnum(r));
                if (isDate && !Double.isNaN(r) && !s.type.equals("sum")) {
                    out.addProperty("value_as_string", Dates.format((long) r, f.format, java.time.ZoneOffset.UTC));
                }
            }
            case "stats", "extended_stats" -> stats(out, nums, s.type.equals("extended_stats"), p.has("sigma") ? p.get("sigma").getAsDouble() : 2.0);
            case "percentiles" -> {
                double[] pcts = p.has("percents") ? doubles(p.getAsJsonArray("percents")) : new double[] {1, 5, 25, 50, 75, 95, 99};
                for (double pc : pcts) {
                    if (pc < 0 || pc > 100) {
                        throw new OpenSearchException("illegal_argument_exception", "[percents] must be in the [0, 100] range");
                    }
                }
                List<Double> sorted = new ArrayList<>(nums);
                sorted.sort(null);
                boolean keyed = !p.has("keyed") || p.get("keyed").getAsBoolean();
                JsonObject vs = new JsonObject();
                JsonArray va = new JsonArray();
                boolean hdr = p.has("hdr");
                for (double pc : pcts) {
                    double v = hdr ? (sorted.isEmpty() ? Double.NaN : sorted.get((int) Math.min(sorted.size() - 1, Math.max(0, Math.ceil(pc / 100.0 * sorted.size()) - 1))))
                            : TDigest.quantile(sorted, pc / 100.0);
                    if (keyed) {
                        vs.add(String.valueOf(pc), jnum(v));
                    } else {
                        JsonObject o = new JsonObject();
                        o.addProperty("key", pc);
                        o.add("value", jnum(v));
                        va.add(o);
                    }
                }
                out.add("values", keyed ? vs : va);
            }
            case "percentile_ranks" -> {
                double[] vals = doubles(p.getAsJsonArray("values"));
                List<Double> sorted = new ArrayList<>(nums);
                sorted.sort(null);
                boolean keyed = !p.has("keyed") || p.get("keyed").getAsBoolean();
                JsonObject vs = new JsonObject();
                JsonArray va = new JsonArray();
                for (double v : vals) {
                    double r = TDigest.cdf(sorted, v) * 100.0;
                    if (keyed) {
                        vs.add(String.valueOf(v), jnum(r));
                    } else {
                        JsonObject o = new JsonObject();
                        o.addProperty("key", v);
                        o.add("value", jnum(r));
                        va.add(o);
                    }
                }
                out.add("values", keyed ? vs : va);
            }
            case "median_absolute_deviation" -> {
                List<Double> sorted = new ArrayList<>(nums);
                sorted.sort(null);
                if (sorted.isEmpty()) {
                    out.add("value", JsonNull.INSTANCE);
                } else {
                    double med = TDigest.quantile(sorted, 0.5);
                    List<Double> dev = new ArrayList<>();
                    for (double v : sorted) {
                        dev.add(Math.abs(v - med));
                    }
                    dev.sort(null);
                    out.add("value", jnum(TDigest.quantile(dev, 0.5)));
                }
            }
            default -> throw new OpenSearchException("parsing_exception", "unsupported metric [" + s.type + "]");
        }
        return out;
    }

    private static double kahan(List<Double> xs) {
        double sum = 0, c = 0;
        for (double x : xs) {
            double y = x - c;
            double t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }
        return sum;
    }

    private static double[] doubles(JsonArray a) {
        double[] d = new double[a.size()];
        for (int i = 0; i < d.length; i++) {
            d[i] = a.get(i).getAsDouble();
        }
        return d;
    }

    private static void stats(JsonObject out, List<Double> nums, boolean extended, double sigma) {
        int n = nums.size();
        double sum = kahan(nums);
        out.addProperty("count", n);
        out.add("min", n == 0 ? JsonNull.INSTANCE : new JsonPrimitive(nums.stream().mapToDouble(x -> x).min().getAsDouble()));
        out.add("max", n == 0 ? JsonNull.INSTANCE : new JsonPrimitive(nums.stream().mapToDouble(x -> x).max().getAsDouble()));
        out.add("avg", n == 0 ? JsonNull.INSTANCE : new JsonPrimitive(sum / n));
        out.addProperty("sum", sum);
        if (!extended) {
            return;
        }
        double sq = 0;
        for (double x : nums) {
            sq += x * x;
        }
        double mean = n == 0 ? Double.NaN : sum / n;
        double varPop = n == 0 ? Double.NaN : Math.max(0, (sq - (sum * sum) / n) / n);
        double varSamp = n > 1 ? Math.max(0, (sq - (sum * sum) / n) / (n - 1)) : Double.NaN;
        out.add("sum_of_squares", n == 0 ? JsonNull.INSTANCE : new JsonPrimitive(sq));
        out.add("variance", jnum(varPop));
        out.add("variance_population", jnum(varPop));
        out.add("variance_sampling", jnum(varSamp));
        double sd = Math.sqrt(varPop), sdp = sd, sds = Math.sqrt(varSamp);
        out.add("std_deviation", jnum(sd));
        out.add("std_deviation_population", jnum(sdp));
        out.add("std_deviation_sampling", jnum(sds));
        JsonObject b = new JsonObject();
        b.add("upper", jnum(mean + sigma * sd));
        b.add("lower", jnum(mean - sigma * sd));
        b.add("upper_population", jnum(mean + sigma * sdp));
        b.add("lower_population", jnum(mean - sigma * sdp));
        b.add("upper_sampling", jnum(mean + sigma * sds));
        b.add("lower_sampling", jnum(mean - sigma * sds));
        out.add("std_deviation_bounds", b);
    }

    private JsonObject stringStats(Spec s, List<Query.DocCtx> docs, String field) {
        long count = 0, minLen = Long.MAX_VALUE, maxLen = 0, total = 0;
        Map<Integer, Long> chars = new HashMap<>();
        for (Query.DocCtx d : docs) {
            for (Object v : vals(d, field, s.params.get("missing"), true)) {
                String str = String.valueOf(v);
                count++;
                minLen = Math.min(minLen, str.length());
                maxLen = Math.max(maxLen, str.length());
                total += str.length();
                str.codePoints().forEach(cp -> chars.merge(cp, 1L, Long::sum));
            }
        }
        JsonObject out = new JsonObject();
        out.addProperty("count", count);
        out.add("min_length", count == 0 ? JsonNull.INSTANCE : new JsonPrimitive(minLen));
        out.add("max_length", count == 0 ? JsonNull.INSTANCE : new JsonPrimitive(maxLen));
        out.add("avg_length", count == 0 ? JsonNull.INSTANCE : new JsonPrimitive((double) total / count));
        double ent = 0;
        for (long c : chars.values()) {
            double pr = (double) c / total;
            ent += -pr * (Math.log(pr) / Math.log(2));
        }
        out.add("entropy", count == 0 ? JsonNull.INSTANCE : new JsonPrimitive(ent));
        return out;
    }

    private JsonObject geoMetric(Spec s, List<Query.DocCtx> docs) {
        String field = field(s);
        double top = -90, bottom = 90, left = 180, right = -180, slat = 0, slon = 0;
        long n = 0;
        for (Query.DocCtx d : docs) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                continue;
            }
            for (JsonElement e : Mappings.values(d.obj, d.rel(f))) {
                double[] pt = Geo.point(e);
                if (pt == null) {
                    continue;
                }
                n++;
                top = Math.max(top, pt[0]);
                bottom = Math.min(bottom, pt[0]);
                left = Math.min(left, pt[1]);
                right = Math.max(right, pt[1]);
                slat += pt[0];
                slon += pt[1];
            }
        }
        JsonObject out = new JsonObject();
        if (s.type.equals("geo_bounds")) {
            if (n > 0) {
                JsonObject b = new JsonObject();
                JsonObject tl = new JsonObject();
                tl.addProperty("lat", top);
                tl.addProperty("lon", left);
                JsonObject br = new JsonObject();
                br.addProperty("lat", bottom);
                br.addProperty("lon", right);
                b.add("top_left", tl);
                b.add("bottom_right", br);
                out.add("bounds", b);
            }
        } else {
            out.addProperty("count", n);
            if (n > 0) {
                JsonObject l = new JsonObject();
                l.addProperty("lat", slat / n);
                l.addProperty("lon", slon / n);
                out.add("location", l);
            }
        }
        return out;
    }

    // ------------------------------------------------------------ pipelines

    private static List<Double> bucketValues(JsonElement buckets, String path) {
        List<Double> out = new ArrayList<>();
        List<JsonElement> items = new ArrayList<>();
        if (buckets.isJsonArray()) {
            buckets.getAsJsonArray().forEach(items::add);
        } else {
            buckets.getAsJsonObject().entrySet().forEach(e -> items.add(e.getValue()));
        }
        for (JsonElement b : items) {
            Double v = resolvePath(b.getAsJsonObject(), path);
            out.add(v);
        }
        return out;
    }

    static Double resolvePath(JsonObject bucket, String path) {
        if (path.equals("_count")) {
            return bucket.get("doc_count").getAsDouble();
        }
        if (path.equals("_key")) {
            return bucket.get("key").isJsonPrimitive() && bucket.get("key").getAsJsonPrimitive().isNumber() ? bucket.get("key").getAsDouble() : null;
        }
        JsonElement cur = bucket;
        String[] parts = path.split(">");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String metric = null;
            int dot = part.indexOf('.');
            if (dot > 0 && !(cur.getAsJsonObject().has(part))) {
                metric = part.substring(dot + 1);
                part = part.substring(0, dot);
            }
            if (!cur.isJsonObject() || !cur.getAsJsonObject().has(part)) {
                return null;
            }
            cur = cur.getAsJsonObject().get(part);
            if (i == parts.length - 1) {
                if (!cur.isJsonObject()) {
                    return cur.isJsonNull() ? null : cur.getAsDouble();
                }
                String k = metric == null ? "value" : metric;
                JsonElement v = cur.getAsJsonObject().get(k);
                if (v == null && cur.getAsJsonObject().has("values") && metric != null) {
                    v = cur.getAsJsonObject().getAsJsonObject("values").get(metric);
                }
                return v == null || v.isJsonNull() ? null : v.getAsDouble();
            }
        }
        return null;
    }

    private JsonObject siblingPipeline(Spec s, JsonObject siblings) {
        String path = s.params.has("buckets_path") ? s.params.get("buckets_path").getAsString() : null;
        if (path == null) {
            throw new OpenSearchException("action_request_validation_exception",
                    "Validation Failed: 1: buckets_path must be set for aggregation [" + s.name + "];");
        }
        int gt = path.indexOf('>');
        if (gt < 0) {
            throw new OpenSearchException("illegal_argument_exception", "No aggregation found for path [" + path + "]");
        }
        String first = path.substring(0, gt);
        JsonElement agg = null;
        for (var e : siblings.entrySet()) {
            String k = e.getKey().contains("#") ? e.getKey().substring(e.getKey().indexOf('#') + 1) : e.getKey();
            if (k.equals(first)) {
                agg = e.getValue();
            }
        }
        if (agg == null || !agg.getAsJsonObject().has("buckets")) {
            throw new OpenSearchException("illegal_argument_exception", "No aggregation found for path [" + path + "]");
        }
        JsonElement buckets = agg.getAsJsonObject().get("buckets");
        List<Double> vals = bucketValues(buckets, path.substring(gt + 1));
        List<Double> present = new ArrayList<>();
        for (Double v : vals) {
            if (v != null && !Double.isNaN(v)) {
                present.add(v);
            }
        }
        JsonObject out = new JsonObject();
        switch (s.type) {
            case "sum_bucket" -> out.addProperty("value", present.stream().mapToDouble(x -> x).sum());
            case "avg_bucket" -> out.add("value", present.isEmpty() ? JsonNull.INSTANCE : new JsonPrimitive(present.stream().mapToDouble(x -> x).average().getAsDouble()));
            case "min_bucket", "max_bucket" -> {
                boolean min = s.type.equals("min_bucket");
                double best = Double.NaN;
                List<String> keys = new ArrayList<>();
                List<JsonElement> items = new ArrayList<>();
                if (buckets.isJsonArray()) {
                    buckets.getAsJsonArray().forEach(items::add);
                } else {
                    buckets.getAsJsonObject().entrySet().forEach(e -> items.add(e.getValue()));
                }
                for (int i = 0; i < vals.size(); i++) {
                    Double v = vals.get(i);
                    if (v == null) {
                        continue;
                    }
                    String key = items.get(i).getAsJsonObject().has("key_as_string") ? items.get(i).getAsJsonObject().get("key_as_string").getAsString()
                            : items.get(i).getAsJsonObject().get("key").getAsString();
                    if (Double.isNaN(best) || (min ? v < best : v > best)) {
                        best = v;
                        keys.clear();
                        keys.add(key);
                    } else if (v == best) {
                        keys.add(key);
                    }
                }
                out.add("value", jnum(best));
                JsonArray ka = new JsonArray();
                keys.forEach(ka::add);
                out.add("keys", ka);
            }
            case "stats_bucket", "extended_stats_bucket" -> stats(out, present, s.type.equals("extended_stats_bucket"), 2.0);
            case "percentiles_bucket" -> {
                double[] pcts = s.params.has("percents") ? doubles(s.params.getAsJsonArray("percents")) : new double[] {1, 5, 25, 50, 75, 95, 99};
                List<Double> sorted = new ArrayList<>(present);
                sorted.sort(null);
                JsonObject vs = new JsonObject();
                for (double pc : pcts) {
                    double v = sorted.isEmpty() ? Double.NaN : sorted.get((int) Math.min(sorted.size() - 1, Math.max(0, Math.round(pc / 100.0 * (sorted.size() - 1)))));
                    vs.add(String.valueOf(pc), jnum(v));
                }
                out.add("values", vs);
            }
            default -> throw new OpenSearchException("parsing_exception", "unsupported pipeline [" + s.type + "]");
        }
        return out;
    }

    /** Parent pipelines (cumulative_sum, derivative, serial_diff, bucket_sort, bucket_script/selector) nested in a bucket agg. */
    private void applyBucketPipelines(Spec s, JsonArray buckets) {
        for (Spec sub : s.subs) {
            if (!PIPELINE.contains(sub.type)) {
                continue;
            }
            String path = sub.params.has("buckets_path") ? sub.params.get("buckets_path").isJsonPrimitive() ? sub.params.get("buckets_path").getAsString() : null : null;
            switch (sub.type) {
                case "cumulative_sum" -> {
                    double run = 0;
                    for (JsonElement be : buckets) {
                        Double v = resolvePath(be.getAsJsonObject(), path);
                        if (v != null && !Double.isNaN(v)) {
                            run += v;
                        }
                        JsonObject o = new JsonObject();
                        o.addProperty("value", run);
                        be.getAsJsonObject().add(sub.name, o);
                    }
                }
                case "derivative" -> {
                    Double prev = null;
                    for (JsonElement be : buckets) {
                        Double v = resolvePath(be.getAsJsonObject(), path);
                        if (prev != null && v != null) {
                            JsonObject o = new JsonObject();
                            o.addProperty("value", v - prev);
                            be.getAsJsonObject().add(sub.name, o);
                        }
                        prev = v;
                    }
                }
                case "serial_diff" -> {
                    int lag = sub.params.has("lag") ? sub.params.get("lag").getAsInt() : 1;
                    List<Double> vs = new ArrayList<>();
                    for (JsonElement be : buckets) {
                        vs.add(resolvePath(be.getAsJsonObject(), path));
                    }
                    for (int i = lag; i < vs.size(); i++) {
                        if (vs.get(i) != null && vs.get(i - lag) != null) {
                            JsonObject o = new JsonObject();
                            o.addProperty("value", vs.get(i) - vs.get(i - lag));
                            buckets.get(i).getAsJsonObject().add(sub.name, o);
                        }
                    }
                }
                case "bucket_sort" -> {
                    List<JsonElement> items = new ArrayList<>();
                    buckets.forEach(items::add);
                    if (sub.params.has("sort")) {
                        List<Comparator<JsonElement>> cs = new ArrayList<>();
                        for (JsonElement se : sub.params.getAsJsonArray("sort")) {
                            var e = se.isJsonObject() ? se.getAsJsonObject().entrySet().iterator().next() : null;
                            String sp = e.getKey();
                            String order = e.getValue().isJsonObject() ? e.getValue().getAsJsonObject().get("order").getAsString() : e.getValue().getAsString();
                            Comparator<JsonElement> c = Comparator.comparingDouble(b -> {
                                Double v = resolvePath(b.getAsJsonObject(), sp);
                                return v == null ? Double.NEGATIVE_INFINITY : v;
                            });
                            cs.add(order.equalsIgnoreCase("desc") ? c.reversed() : c);
                        }
                        items.sort((a, b) -> {
                            for (var c : cs) {
                                int r = c.compare(a, b);
                                if (r != 0) {
                                    return r;
                                }
                            }
                            return 0;
                        });
                    }
                    int from = sub.params.has("from") ? sub.params.get("from").getAsInt() : 0;
                    int size = sub.params.has("size") ? sub.params.get("size").getAsInt() : items.size();
                    List<JsonElement> cut = items.subList(Math.min(from, items.size()), Math.min(items.size(), from + size));
                    while (buckets.size() > 0) {
                        buckets.remove(0);
                    }
                    cut.forEach(buckets::add);
                }
                default -> throw new OpenSearchException("parsing_exception", "[" + sub.type + "] pipeline aggregation is not supported by Warp's OpenSearch frontend");
            }
        }
    }
}
