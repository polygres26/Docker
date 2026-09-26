package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Query DSL JSON to {@link Query.Node} tree, with OpenSearch's parse errors ({@code parsing_exception}, ...). */
final class QueryParser {

    private static final Set<String> UNSUPPORTED = Set.of("script", "script_score", "more_like_this", "percolate", "intervals",
            "span_term", "span_near", "span_or", "span_not", "span_first", "span_containing", "span_within", "span_multi",
            "span_field_masking", "geo_shape", "geo_polygon", "shape", "has_child", "has_parent", "parent_id", "terms_set",
            "rank_feature", "distance_feature", "combined_fields", "neural", "neural_sparse", "template", "wrapper_unsupported",
            "match_bool_prefix_unsupported");

    private QueryParser() {
    }

    static OpenSearchException err(String msg) {
        return new OpenSearchException("parsing_exception", msg);
    }

    static Query.Node parse(JsonElement el) {
        if (el == null || el.isJsonNull()) {
            return new Query.MatchAll();
        }
        if (!el.isJsonObject()) {
            throw err("[_na] query malformed, must start with start_object");
        }
        JsonObject q = el.getAsJsonObject();
        if (q.size() == 0) {
            return new Query.MatchAll();
        }
        if (q.size() > 1) {
            String first = q.keySet().iterator().next();
            throw err("[" + first + "] malformed query, expected [END_OBJECT] but found [FIELD_NAME]");
        }
        String type = q.keySet().iterator().next();
        JsonElement body = q.get(type);
        Query.Node n = parseNamed(type, body);
        return n;
    }

    private static double boostOf(JsonObject o) {
        if (o != null && o.has("boost")) {
            return o.get("boost").getAsDouble();
        }
        return 1.0;
    }

    private static Query.Node tag(Query.Node n, JsonObject o) {
        if (o != null && o.has("_name")) {
            return new Query.Named(n, o.get("_name").getAsString()).boost(1.0);
        }
        return n;
    }

    private static JsonObject obj(String type, JsonElement body) {
        if (!body.isJsonObject()) {
            throw err("[" + type + "] query malformed, no start_object after query name");
        }
        return body.getAsJsonObject();
    }

    private static void allow(String type, JsonObject o, String... allowed) {
        Set<String> ok = Set.of(allowed);
        for (String k : o.keySet()) {
            if (!ok.contains(k)) {
                throw err("[" + type + "] query does not support [" + k + "]");
            }
        }
    }

    private static Map.Entry<String, JsonElement> single(String type, JsonObject o) {
        if (o.size() == 0) {
            throw err("[" + type + "] query malformed, empty fieldname");
        }
        if (o.size() > 1) {
            List<String> ks = new ArrayList<>(o.keySet());
            throw err("[" + type + "] query doesn't support multiple fields, found [" + ks.get(0) + "] and [" + ks.get(1) + "]");
        }
        return o.entrySet().iterator().next();
    }

    private static String text(JsonElement e) {
        if (e.isJsonPrimitive()) {
            return e.getAsString();
        }
        throw err("[match] unexpected token [" + (e.isJsonObject() ? "START_OBJECT" : "START_ARRAY") + "] after [query]");
    }

    private static Query.Node parseNamed(String type, JsonElement body) {
        switch (type) {
            case "match_all": {
                JsonObject o = obj(type, body);
                return tag(new Query.MatchAll().boost(boostOf(o)), o);
            }
            case "match_none":
                return new Query.MatchNone();
            case "match", "match_phrase", "match_phrase_prefix", "match_bool_prefix":
                return parseMatch(type, obj(type, body));
            case "multi_match":
                return parseMultiMatch(obj(type, body));
            case "term": {
                var e = single(type, obj(type, body));
                JsonObject o = e.getValue().isJsonObject() ? e.getValue().getAsJsonObject() : null;
                if (o != null) {
                    allow(type, o, "value", "boost", "_name", "case_insensitive");
                    if (!o.has("value")) {
                        throw err("[term] query does not support [" + o.keySet().iterator().next() + "]");
                    }
                    Query.Term t = new Query.Term(e.getKey(), o.get("value"), o.has("case_insensitive") && o.get("case_insensitive").getAsBoolean());
                    t.boost = boostOf(o);
                    return tag(t, o);
                }
                if (e.getValue().isJsonArray()) {
                    throw err("[term] query does not support array of values");
                }
                return new Query.Term(e.getKey(), e.getValue(), false);
            }
            case "terms": {
                JsonObject o = obj(type, body);
                String field = null;
                JsonElement vals = null;
                double boost = 1.0;
                for (var e : o.entrySet()) {
                    if (e.getKey().equals("boost")) {
                        boost = e.getValue().getAsDouble();
                    } else if (e.getKey().equals("_name")) {
                        continue;
                    } else if (field != null) {
                        throw err("[" + type + "] query does not support multiple fields");
                    } else {
                        field = e.getKey();
                        vals = e.getValue();
                    }
                }
                if (field == null) {
                    throw err("[terms] query requires a field name, followed by array of terms or a document lookup specification");
                }
                if (!vals.isJsonArray()) {
                    throw err("[terms] query does not support terms lookup in oswire (index/id/path)");
                }
                List<JsonElement> l = new ArrayList<>();
                vals.getAsJsonArray().forEach(l::add);
                return tag(new Query.Terms(field, l).boost(boost), o);
            }
            case "range":
                return parseRange(obj(type, body));
            case "exists": {
                JsonObject o = obj(type, body);
                allow(type, o, "field", "boost", "_name");
                if (!o.has("field")) {
                    throw err("[exists] must be provided with a [field]");
                }
                return tag(new Query.Exists(o.get("field").getAsString()).boost(boostOf(o)), o);
            }
            case "ids": {
                JsonObject o = obj(type, body);
                allow(type, o, "values", "boost", "_name", "type");
                Set<String> ids = new LinkedHashSet<>();
                if (o.has("values")) {
                    if (!o.get("values").isJsonArray()) {
                        throw err("[ids] failed to parse field [values]");
                    }
                    o.getAsJsonArray("values").forEach(x -> ids.add(x.getAsString()));
                }
                return tag(new Query.Ids(ids).boost(boostOf(o)), o);
            }
            case "prefix", "wildcard", "regexp": {
                var e = single(type, obj(type, body));
                if (type.equals("regexp")) {
                    String rx = e.getValue().isJsonObject() && e.getValue().getAsJsonObject().has("value")
                            ? e.getValue().getAsJsonObject().get("value").getAsString()
                            : e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : "";
                    if (rx.length() > 1000) {
                        throw new OpenSearchException("illegal_argument_exception", "The length of regex [" + rx.length()
                                + "] used in the Regexp Query request has exceeded the allowed maximum of [1000]. This maximum can be set by changing the "
                                + "[index.max_regex_length] index level setting.").asShardLevel();
                    }
                }
                JsonObject o = e.getValue().isJsonObject() ? e.getValue().getAsJsonObject() : null;
                String v;
                boolean ci = false;
                double boost = 1;
                if (o != null) {
                    allow(type, o, "value", "wildcard", "boost", "_name", "case_insensitive", "rewrite", "flags", "max_determinized_states");
                    JsonElement ve = o.has("value") ? o.get("value") : o.get("wildcard");
                    if (ve == null) {
                        throw err("[" + type + "] query malformed, no [value] field");
                    }
                    v = ve.getAsString();
                    ci = o.has("case_insensitive") && o.get("case_insensitive").getAsBoolean();
                    boost = boostOf(o);
                } else {
                    v = e.getValue().getAsString();
                }
                Query.Node n = new Query.TermPattern(e.getKey(), type, v, ci, null).boost(boost);
                return tag(n, o);
            }
            case "fuzzy": {
                var e = single(type, obj(type, body));
                JsonObject o = e.getValue().isJsonObject() ? e.getValue().getAsJsonObject() : null;
                if (o == null) {
                    return new Query.Fuzzy(e.getKey(), e.getValue().getAsString(), "AUTO", 0, true);
                }
                allow(type, o, "value", "fuzziness", "prefix_length", "max_expansions", "transpositions", "rewrite", "boost", "_name");
                Query.Fuzzy f = new Query.Fuzzy(e.getKey(), o.get("value").getAsString(),
                        o.has("fuzziness") ? o.get("fuzziness").getAsString() : "AUTO",
                        o.has("prefix_length") ? o.get("prefix_length").getAsInt() : 0,
                        !o.has("transpositions") || o.get("transpositions").getAsBoolean(),
                        o.has("max_expansions") ? o.get("max_expansions").getAsInt() : 50);
                f.boost = boostOf(o);
                return tag(f, o);
            }
            case "bool":
                return parseBool(obj(type, body));
            case "constant_score": {
                JsonObject o = obj(type, body);
                allow(type, o, "filter", "boost", "_name");
                if (!o.has("filter")) {
                    throw err("[constant_score] requires a 'filter' element");
                }
                return tag(new Query.ConstantScore(parse(o.get("filter"))).boost(boostOf(o)), o);
            }
            case "dis_max": {
                JsonObject o = obj(type, body);
                allow(type, o, "queries", "tie_breaker", "boost", "_name");
                List<Query.Node> qs = new ArrayList<>();
                if (!o.has("queries") || !o.get("queries").isJsonArray()) {
                    throw err("[dis_max] requires 'queries' field with array of queries");
                }
                o.getAsJsonArray("queries").forEach(x -> qs.add(parse(x)));
                return tag(new Query.DisMax(qs, o.has("tie_breaker") ? o.get("tie_breaker").getAsDouble() : 0).boost(boostOf(o)), o);
            }
            case "boosting": {
                JsonObject o = obj(type, body);
                allow(type, o, "positive", "negative", "negative_boost", "boost", "_name");
                if (!o.has("positive")) {
                    throw err("[boosting] query requires 'positive' query to be set'");
                }
                if (!o.has("negative")) {
                    throw err("[boosting] query requires 'negative' query to be set'");
                }
                if (!o.has("negative_boost")) {
                    throw err("[boosting] query requires 'negative_boost' to be set to be a positive value'");
                }
                return tag(new Query.Boosting(parse(o.get("positive")), parse(o.get("negative")), o.get("negative_boost").getAsDouble()).boost(boostOf(o)), o);
            }
            case "nested": {
                JsonObject o = obj(type, body);
                allow(type, o, "path", "query", "score_mode", "ignore_unmapped", "inner_hits", "boost", "_name");
                if (!o.has("path")) {
                    throw err("[nested] requires 'path' field");
                }
                if (!o.has("query")) {
                    throw err("[nested] requires 'query' field");
                }
                String sm = o.has("score_mode") ? o.get("score_mode").getAsString() : "avg";
                if (!Set.of("avg", "sum", "max", "min", "none").contains(sm)) {
                    throw new OpenSearchException("illegal_argument_exception", "No score mode for child query [" + sm + "] found");
                }
                Query.NestedQ n = new Query.NestedQ(o.get("path").getAsString(), parse(o.get("query")), sm,
                        o.has("ignore_unmapped") && o.get("ignore_unmapped").getAsBoolean());
                n.boost = boostOf(o);
                if (o.has("inner_hits")) {
                    n.innerHitsName = o.get("inner_hits").isJsonObject() && o.getAsJsonObject("inner_hits").has("name")
                            ? o.getAsJsonObject("inner_hits").get("name").getAsString() : o.get("path").getAsString();
                }
                return tag(n, o);
            }
            case "function_score":
                return parseFunctionScore(obj(type, body));
            case "query_string", "simple_query_string":
                return LuceneSyntax.fromRequest(type, obj(type, body));
            case "wrapper": {
                JsonObject o = obj(type, body);
                String dec = new String(Base64.getDecoder().decode(o.get("query").getAsString()), java.nio.charset.StandardCharsets.UTF_8);
                return parse(JsonParser.parseString(dec));
            }
            case "geo_distance": {
                JsonObject o = obj(type, body);
                String field = null;
                double[] pt = null;
                String dist = null;
                for (var e : o.entrySet()) {
                    switch (e.getKey()) {
                        case "distance" -> dist = e.getValue().getAsString();
                        case "distance_type", "validation_method", "boost", "_name", "ignore_unmapped" -> {
                        }
                        default -> {
                            field = e.getKey();
                            pt = Geo.point(e.getValue());
                        }
                    }
                }
                if (field == null || pt == null || dist == null) {
                    throw err("[geo_distance] requires a field with a point and a distance");
                }
                return tag(new Query.GeoDistance(field, pt[0], pt[1], Geo.meters(dist)).boost(boostOf(o)), o);
            }
            case "geo_bounding_box": {
                JsonObject o = obj(type, body);
                for (var e : o.entrySet()) {
                    if (e.getValue().isJsonObject() && !Set.of("boost", "_name").contains(e.getKey())) {
                        JsonObject b = e.getValue().getAsJsonObject();
                        double top, left, bottom, right;
                        if (b.has("top_left") && b.has("bottom_right")) {
                            double[] tl = Geo.point(b.get("top_left")), br = Geo.point(b.get("bottom_right"));
                            top = tl[0];
                            left = tl[1];
                            bottom = br[0];
                            right = br[1];
                        } else if (b.has("top") && b.has("left") && b.has("bottom") && b.has("right")) {
                            top = b.get("top").getAsDouble();
                            left = b.get("left").getAsDouble();
                            bottom = b.get("bottom").getAsDouble();
                            right = b.get("right").getAsDouble();
                        } else {
                            throw err("failed to parse geo_bounding_box");
                        }
                        return tag(new Query.GeoBox(e.getKey(), top, left, bottom, right).boost(boostOf(o)), o);
                    }
                }
                throw err("failed to parse geo_bounding_box");
            }
            case "knn":
                return Vector.parseKnn(obj(type, body));
            case "hybrid":
                return Vector.parseHybrid(obj(type, body));
            default:
                if (UNSUPPORTED.contains(type)) {
                    throw new OpenSearchException("parsing_exception", "[" + type + "] query is not supported by Warp's OpenSearch frontend");
                }
                throw err("unknown query [" + type + "]");
        }
    }

    private static Query.Node parseMatch(String type, JsonObject body) {
        var e = single(type, body);
        String field = e.getKey();
        JsonElement v = e.getValue();
        String q;
        boolean and = false;
        String msm = null, analyzer = null, fuzziness = null, zero = "none";
        int prefix = 0, slop = 0;
        boolean lenient = false, transp = true;
        double boost = 1;
        JsonObject o = null;
        if (v.isJsonObject()) {
            o = v.getAsJsonObject();
            if (type.equals("match")) {
                allow(type, o, "query", "operator", "minimum_should_match", "analyzer", "fuzziness", "prefix_length", "max_expansions",
                        "fuzzy_rewrite", "fuzzy_transpositions", "lenient", "zero_terms_query", "boost", "_name", "cutoff_frequency",
                        "auto_generate_synonyms_phrase_query", "synonyms");
            } else if (type.equals("match_bool_prefix")) {
                allow(type, o, "query", "operator", "minimum_should_match", "analyzer", "fuzziness", "prefix_length", "max_expansions",
                        "fuzzy_rewrite", "fuzzy_transpositions", "boost", "_name");
            } else {
                allow(type, o, "query", "analyzer", "slop", "zero_terms_query", "boost", "_name", "max_expansions");
            }
            if (!o.has("query")) {
                throw err("[" + type + "] requires query value");
            }
            q = text(o.get("query"));
            and = o.has("operator") && o.get("operator").getAsString().equalsIgnoreCase("and");
            if (o.has("operator") && !Set.of("and", "or").contains(o.get("operator").getAsString().toLowerCase())) {
                throw new OpenSearchException("illegal_argument_exception", "Unsupported defaultOperator [" + o.get("operator").getAsString()
                        + "], can either be [OR] or [AND]");
            }
            msm = o.has("minimum_should_match") ? o.get("minimum_should_match").getAsString() : null;
            analyzer = o.has("analyzer") ? o.get("analyzer").getAsString() : null;
            fuzziness = o.has("fuzziness") ? o.get("fuzziness").getAsString() : null;
            prefix = o.has("prefix_length") ? o.get("prefix_length").getAsInt() : 0;
            slop = o.has("slop") ? o.get("slop").getAsInt() : 0;
            lenient = o.has("lenient") && o.get("lenient").getAsBoolean();
            transp = !o.has("fuzzy_transpositions") || o.get("fuzzy_transpositions").getAsBoolean();
            zero = o.has("zero_terms_query") ? o.get("zero_terms_query").getAsString().toLowerCase() : "none";
            boost = boostOf(o);
        } else {
            q = text(v);
        }
        String mt = switch (type) {
            case "match_phrase" -> "phrase";
            case "match_phrase_prefix" -> "phrase_prefix";
            case "match_bool_prefix" -> "bool_prefix";
            default -> "bool";
        };
        Query.Match m = new Query.Match(field, q, and, msm, analyzer, fuzziness, prefix, zero, lenient, mt, slop, transp);
        m.boost = boost;
        return tag(m, o);
    }

    static List<Map.Entry<String, Double>> fieldList(JsonElement fields) {
        List<Map.Entry<String, Double>> out = new ArrayList<>();
        if (fields == null) {
            out.add(Map.entry("*", 1.0));
            return out;
        }
        List<JsonElement> l = new ArrayList<>();
        if (fields.isJsonArray()) {
            fields.getAsJsonArray().forEach(l::add);
        } else {
            l.add(fields);
        }
        for (JsonElement e : l) {
            String s = e.getAsString();
            int i = s.lastIndexOf('^');
            if (i > 0) {
                out.add(Map.entry(s.substring(0, i), Double.parseDouble(s.substring(i + 1))));
            } else {
                out.add(Map.entry(s, 1.0));
            }
        }
        return out;
    }

    private static Query.Node parseMultiMatch(JsonObject o) {
        allow("multi_match", o, "query", "fields", "type", "operator", "minimum_should_match", "analyzer", "fuzziness", "slop",
                "tie_breaker", "lenient", "zero_terms_query", "boost", "_name", "prefix_length", "max_expansions", "fuzzy_rewrite",
                "fuzzy_transpositions", "cutoff_frequency", "auto_generate_synonyms_phrase_query");
        if (!o.has("query")) {
            throw err("No text specified for multi_match query");
        }
        String type = o.has("type") ? o.get("type").getAsString() : "best_fields";
        if (!Set.of("best_fields", "most_fields", "cross_fields", "phrase", "phrase_prefix", "bool_prefix").contains(type)) {
            throw new OpenSearchException("illegal_argument_exception", "Unknown MultiMatchQueryBuilder.Type [" + type + "]");
        }
        if (type.equals("bool_prefix") && o.has("slop")) {
            throw err("[slop] not allowed for type [bool_prefix]");
        }
        if (type.equals("bool_prefix") && o.has("cutoff_frequency")) {
            throw err("[cutoff_frequency] not allowed for type [bool_prefix]");
        }
        Query.MultiMatch m = new Query.MultiMatch(fieldList(o.get("fields")), text(o.get("query")), type,
                o.has("operator") && o.get("operator").getAsString().equalsIgnoreCase("and"),
                o.has("minimum_should_match") ? o.get("minimum_should_match").getAsString() : null,
                o.has("analyzer") ? o.get("analyzer").getAsString() : null,
                o.has("fuzziness") ? o.get("fuzziness").getAsString() : null,
                o.has("slop") ? o.get("slop").getAsInt() : 0,
                o.has("tie_breaker") ? o.get("tie_breaker").getAsDouble() : (type.equals("best_fields") || type.equals("phrase") ? 0.0 : 1.0),
                o.has("lenient") && o.get("lenient").getAsBoolean(),
                o.has("zero_terms_query") ? o.get("zero_terms_query").getAsString().toLowerCase() : "none",
                o.has("prefix_length") ? o.get("prefix_length").getAsInt() : 0);
        m.boost = boostOf(o);
        return tag(m, o);
    }

    private static Query.Node parseRange(JsonObject body) {
        var e = single("range", body);
        if (!e.getValue().isJsonObject()) {
            throw err("[range] query malformed, no start_object after query name");
        }
        JsonObject o = e.getValue().getAsJsonObject();
        allow("range", o, "gt", "gte", "lt", "lte", "from", "to", "include_lower", "include_upper", "format", "time_zone", "boost", "_name",
                "relation");
        JsonElement gt = o.get("gt"), gte = o.get("gte"), lt = o.get("lt"), lte = o.get("lte");
        if (o.has("from") && !o.get("from").isJsonNull()) {
            if (!o.has("include_lower") || o.get("include_lower").getAsBoolean()) {
                gte = o.get("from");
            } else {
                gt = o.get("from");
            }
        }
        if (o.has("to") && !o.get("to").isJsonNull()) {
            if (!o.has("include_upper") || o.get("include_upper").getAsBoolean()) {
                lte = o.get("to");
            } else {
                lt = o.get("to");
            }
        }
        Query.Range r = new Query.Range(e.getKey(), gt, gte, lt, lte, o.has("format") ? o.get("format").getAsString() : null,
                o.has("time_zone") ? o.get("time_zone").getAsString() : null);
        r.boost = boostOf(o);
        return tag(r, o);
    }

    private static void addAll(List<Query.Node> into, JsonElement el) {
        if (el == null) {
            return;
        }
        if (el.isJsonArray()) {
            el.getAsJsonArray().forEach(x -> into.add(parse(x)));
        } else {
            into.add(parse(el));
        }
    }

    private static Query.Node parseBool(JsonObject o) {
        for (String k : o.keySet()) {
            if (!Set.of("must", "filter", "should", "must_not", "mustNot", "minimum_should_match", "minimumShouldMatch", "boost", "_name",
                    "adjust_pure_negative", "adjustPureNegative").contains(k)) {
                throw new OpenSearchException("x_content_parse_exception", "[1:1] [bool] unknown field [" + k + "] did you mean any of [must, mustNot]?");
            }
        }
        Query.Bool b = new Query.Bool();
        addAll(b.must, o.get("must"));
        addAll(b.filter, o.get("filter"));
        addAll(b.should, o.get("should"));
        addAll(b.mustNot, o.get("must_not"));
        if (o.has("minimum_should_match")) {
            b.msm = o.get("minimum_should_match").getAsString();
        }
        b.boost = boostOf(o);
        return tag(b, o);
    }

    private static Query.Node parseFunctionScore(JsonObject o) {
        allow("function_score", o, "query", "functions", "score_mode", "boost_mode", "max_boost", "min_score", "boost", "_name", "weight",
                "field_value_factor", "random_score", "gauss", "exp", "linear", "script_score", "filter");
        Query.FunctionScore fs = new Query.FunctionScore(o.has("query") ? parse(o.get("query")) : new Query.MatchAll());
        if (o.has("score_mode")) {
            fs.scoreMode = o.get("score_mode").getAsString();
        }
        if (o.has("boost_mode")) {
            fs.boostMode = o.get("boost_mode").getAsString();
        }
        if (o.has("max_boost")) {
            fs.maxBoost = o.get("max_boost").getAsDouble();
        }
        if (o.has("min_score")) {
            fs.minScore = o.get("min_score").getAsDouble();
        }
        fs.boost = boostOf(o);
        List<JsonObject> fns = new ArrayList<>();
        if (o.has("functions")) {
            o.getAsJsonArray("functions").forEach(x -> fns.add(x.getAsJsonObject()));
        } else {
            fns.add(o);
        }
        for (JsonObject f : fns) {
            Query.Node filter = f.has("filter") ? parse(f.get("filter")) : null;
            double weight = f.has("weight") ? f.get("weight").getAsDouble() : 1.0;
            boolean found = false;
            for (String kind : new String[] {"field_value_factor", "random_score", "gauss", "exp", "linear", "script_score"}) {
                if (f.has(kind)) {
                    if (kind.equals("script_score")) {
                        throw new OpenSearchException("parsing_exception", "[script_score] function is not supported by Warp's OpenSearch frontend");
                    }
                    fs.fns.add(new Query.FunctionScore.Fn(filter, weight, kind, f.get(kind).getAsJsonObject()));
                    found = true;
                    break;
                }
            }
            if (!found && f.has("weight")) {
                fs.fns.add(new Query.FunctionScore.Fn(filter, weight, "weight", new JsonObject()));
            }
        }
        return tag(fs, o);
    }
}
