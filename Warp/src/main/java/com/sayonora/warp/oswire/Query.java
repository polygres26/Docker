package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The oswire query tree, evaluated in the JVM against one document at a time. {@link Node#score} returns
 * {@link Double#NaN} for "no match" and otherwise a Lucene-BM25-compatible relevance score (see {@link IndexCtx}).
 * Built from the request's Query DSL by {@link QueryParser} and from Lucene query syntax by {@link LuceneSyntax}.
 */
final class Query {

    private Query() {
    }

    static final double NO = Double.NaN;

    static boolean matched(double s) {
        return !Double.isNaN(s);
    }

    /** Collects terms a query looked for, per field, for the highlighter. */
    static final class Hl {
        final Map<String, Set<String>> terms = new HashMap<>();
        final Map<String, Set<String>> prefixes = new HashMap<>();
        final Map<String, List<List<String>>> phrases = new HashMap<>();
        final List<Pattern> patterns = new ArrayList<>();

        void term(String field, String t) {
            terms.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(t);
        }

        void prefix(String field, String t) {
            prefixes.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(t);
        }

        void phrase(String field, List<String> t) {
            phrases.computeIfAbsent(field, k -> new ArrayList<>()).add(t);
        }
    }

    /** Per-document evaluation context (root document, or one nested object). */
    static final class DocCtx {
        final IndexCtx ix;
        final PostgresSearchStore.Doc doc;
        final JsonObject obj;
        final String prefix;
        final DocCtx parent;
        double score = Double.NaN;
        private final Map<String, List<Analysis.Token>> tokenCache = new HashMap<>();

        DocCtx(IndexCtx ix, PostgresSearchStore.Doc doc) {
            this(ix, doc, doc.source, "", null);
        }

        DocCtx(IndexCtx ix, PostgresSearchStore.Doc doc, JsonObject obj, String prefix, DocCtx parent) {
            this.ix = ix;
            this.doc = doc;
            this.obj = obj;
            this.prefix = prefix;
            this.parent = parent;
        }

        Mappings.Field field(String path) {
            Mappings.Field f = ix.mappings.get(path);
            if (f == null) {
                return null;
            }
            if (prefix.isEmpty()) {
                return f.nestedPath != null ? null : f;
            }
            return f.path.startsWith(prefix) ? f : null;
        }

        String rel(Mappings.Field f) {
            String p = IndexCtx.sourcePath(f);
            return p.startsWith(prefix) ? p.substring(prefix.length()) : p;
        }

        List<Analysis.Token> tokens(Mappings.Field f) {
            return tokenCache.computeIfAbsent(f.path, k -> ix.tokens(f, obj, rel(f)));
        }

        List<Object> values(Mappings.Field f) {
            return Values.of(f, obj, rel(f), ix.settings, ix.zone());
        }

        DocCtx nested(JsonObject o, String newPrefix) {
            return new DocCtx(ix, doc, o, newPrefix, this);
        }
    }

    abstract static class Node {
        double boost = 1.0;
        String name;

        abstract double score(DocCtx d);

        void collect(Hl h) {
        }

        List<Node> children() {
            return List.of();
        }

        /** Set-level preparation (k-NN top-k, hybrid normalisation) before per-document scoring. */
        void prepare(Vector.Prep p) {
            for (Node c : children()) {
                c.prepare(p);
            }
        }

        Node boost(double b) {
            this.boost = b;
            return this;
        }
    }

    // ---------------------------------------------------------------- leaf queries

    static final class MatchAll extends Node {
        double score(DocCtx d) {
            return boost;
        }
    }

    static final class MatchNone extends Node {
        double score(DocCtx d) {
            return NO;
        }
    }

    private static String str(JsonElement v) {
        if (v.isJsonPrimitive()) {
            JsonPrimitive p = v.getAsJsonPrimitive();
            if (p.isNumber()) {
                java.math.BigDecimal bd = p.getAsBigDecimal();
                String s = bd.stripTrailingZeros().scale() <= 0 ? bd.toBigInteger().toString() : p.getAsString();
                return p.getAsString().contains(".") && bd.stripTrailingZeros().scale() <= 0 ? p.getAsString() : s;
            }
            return p.getAsString();
        }
        return v.toString();
    }

    /** {@code term}: exact term (no analysis) -- on text fields the term must equal an analysed token. */
    static final class Term extends Node {
        final String field;
        final JsonElement value;
        final boolean ci;

        Term(String field, JsonElement value, boolean ci) {
            this.field = field;
            this.value = value;
            this.ci = ci;
        }

        double score(DocCtx d) {
            if (field.equals("_id")) {
                return d.doc.id.equals(str(value)) ? boost : NO;
            }
            if (field.equals("_index")) {
                return Mappings.wildcardMatch(str(value), d.doc.index) ? boost : NO;
            }
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            if (f.isString() || f.type.equals("boolean")) {
                String t = str(value);
                if (f.type.equals("boolean")) {
                    Object pb = Values.parse(f, value, d.ix.settings, d.ix.zone());
                    if (pb == null) {
                        return NO;
                    }
                    t = pb.toString();
                } else if (f.isKeyword() && !ci) {
                    t = Values.normalize(f, t, d.ix.settings);
                }
                List<Analysis.Token> toks = d.tokens(f);
                int tf = 0;
                for (Analysis.Token tk : toks) {
                    if (ci ? tk.term().equalsIgnoreCase(t) : tk.term().equals(t)) {
                        tf++;
                    }
                }
                if (tf == 0) {
                    return NO;
                }
                if (ci) {
                    return boost;
                }
                IndexCtx.FieldStats st = d.ix.stats(f);
                return IndexCtx.bm25(IndexCtx.idf(st, t), tf, toks.size(), f.isText(), st, boost);
            }
            Object q = Values.parse(f, value, d.ix.settings, d.ix.zone());
            if (q == null) {
                return NO;
            }
            for (Object v : d.values(f)) {
                if (Values.compare(v, q) == 0) {
                    return boost;
                }
            }
            return NO;
        }

        void collect(Hl h) {
            h.term(field, str(value));
        }
    }

    static final class Terms extends Node {
        final String field;
        final List<JsonElement> values;

        Terms(String field, List<JsonElement> values) {
            this.field = field;
            this.values = values;
        }

        double score(DocCtx d) {
            int max = 65536;
            try {
                var idxs = d.ix.settings.getAsJsonObject("index");
                if (idxs.has("max_terms_count")) {
                    max = Integer.parseInt(idxs.get("max_terms_count").getAsString());
                }
            } catch (RuntimeException ignored) {
                // default
            }
            if (values.size() > max) {
                throw OpenSearchException.illegalArgument("The number of terms [" + values.size() + "] used in the Terms Query request has exceeded the allowed maximum of ["
                        + max + "]. This maximum can be set by changing the [index.max_terms_count] index level setting.").asShardLevel();
            }
            for (JsonElement v : values) {
                if (new Term(field, v, false).score(d) >= 0) {
                    return boost;
                }
            }
            return NO;
        }

        void collect(Hl h) {
            for (JsonElement v : values) {
                h.term(field, str(v));
            }
        }
    }

    static final class Ids extends Node {
        final Set<String> ids;

        Ids(Set<String> ids) {
            this.ids = ids;
        }

        double score(DocCtx d) {
            return ids.contains(d.doc.id) ? boost : NO;
        }
    }

    static final class Exists extends Node {
        final String field;

        Exists(String field) {
            this.field = field;
        }

        double score(DocCtx d) {
            return exists(d, field) ? boost : NO;
        }

        static boolean exists(DocCtx d, String field) {
            if (field.equals("_id") || field.equals("_index") || field.equals("_seq_no") || field.equals("_version") || field.equals("_primary_term")) {
                return true;
            }
            if (field.equals("_source")) {
                throw new OpenSearchException("query_shard_exception", "Field [_source] of type [_source] does not support exists queries")
                        .with("index", d.ix.indexName).asShardLevel();
            }
            if (field.contains("*")) {
                for (Mappings.Field f : d.ix.mappings.fields.values()) {
                    if (Mappings.wildcardMatch(field, f.path) && !f.type.equals("object") && !f.type.equals("nested") && exists(d, f.path)) {
                        return true;
                    }
                }
                return false;
            }
            Mappings.Field f = d.field(field);
            if (f == null) {
                for (Mappings.Field c : d.ix.mappings.fields.values()) {
                    if (c.path.startsWith(field + ".") && !c.subField && d.field(c.path) != null && !c.type.equals("object") && !c.type.equals("nested")
                            && exists(d, c.path)) {
                        return true;
                    }
                }
                return false;
            }
            if (f.type.equals("object") || f.type.equals("nested")) {
                for (Mappings.Field c : d.ix.mappings.fields.values()) {
                    if (c.path.startsWith(f.path + ".") && !c.subField && !c.type.equals("object") && !c.type.equals("nested") && exists(d, c.path)) {
                        return true;
                    }
                }
                return false;
            }
            if (!f.indexed && !(f.def != null && f.def.has("doc_values"))) {
                return false;
            }
            if (f.isText()) {
                return !d.tokens(f).isEmpty();
            }
            if (f.isKeyword()) {
                return !d.tokens(f).isEmpty();
            }
            if (f.type.equals("knn_vector") || f.type.equals("geo_point") || f.type.equals("binary") || f.type.equals("flat_object")) {
                return !Mappings.values(d.obj, d.rel(f)).isEmpty();
            }
            return !d.values(f).isEmpty();
        }
    }

    /** prefix / wildcard / regexp over terms (constant score). */
    static final class TermPattern extends Node {
        final String field;
        final String kind;
        final String value;
        final boolean ci;
        private final Pattern pattern;

        TermPattern(String field, String kind, String value, boolean ci, String flags) {
            this.field = field;
            this.kind = kind;
            this.value = value;
            this.ci = ci;
            Pattern p = null;
            if (kind.equals("wildcard")) {
                p = Pattern.compile(wildcardToRegex(value), Pattern.DOTALL | (ci ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0));
            } else if (kind.equals("regexp")) {
                try {
                    p = Pattern.compile(luceneRegexToJava(value), Pattern.DOTALL | (ci ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0));
                } catch (java.util.regex.PatternSyntaxException e) {
                    throw new OpenSearchException("parsing_exception", "failed to create query: " + e.getDescription());
                }
            }
            this.pattern = p;
        }

        boolean test(String term) {
            return switch (kind) {
                case "prefix" -> ci ? term.toLowerCase(Locale.ROOT).startsWith(value.toLowerCase(Locale.ROOT)) : term.startsWith(value);
                default -> pattern.matcher(term).matches();
            };
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            if (!f.isString()) {
                if (f.type.equals("ip") || f.isNumeric() || f.type.equals("boolean") || f.type.equals("date")) {
                    throw OpenSearchException.illegalArgument("Can only use " + kind + " queries on keyword, text and wildcard fields - not on ["
                            + field + "] which is of type [" + f.type + "]").asShardLevel();
                }
                return NO;
            }
            for (Analysis.Token t : d.tokens(f)) {
                if (test(t.term())) {
                    return boost;
                }
            }
            return NO;
        }

        void collect(Hl h) {
            if (kind.equals("prefix")) {
                h.prefix(field, ci ? value.toLowerCase(Locale.ROOT) : value);
            } else {
                h.patterns.add(pattern);
            }
        }
    }

    static String wildcardToRegex(String w) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < w.length(); i++) {
            char c = w.charAt(i);
            if (c == '\\' && i + 1 < w.length()) {
                sb.append(Pattern.quote(String.valueOf(w.charAt(++i))));
            } else if (c == '*') {
                sb.append(".*");
            } else if (c == '?') {
                sb.append('.');
            } else {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return sb.toString();
    }

    /** Lucene RegExp syntax is close to Java's for the commonly used subset; '~', '&', '<>', '@', '#' are handled minimally. */
    static String luceneRegexToJava(String r) {
        return r.replace("@", ".*").replace("#", "(?!)");
    }

    static int fuzzyDistance(String fuzziness, int termLen) {
        if (fuzziness == null || fuzziness.equalsIgnoreCase("AUTO") || fuzziness.toUpperCase(Locale.ROOT).startsWith("AUTO:")) {
            int lo = 3, hi = 6;
            if (fuzziness != null && fuzziness.contains(":")) {
                String[] p = fuzziness.substring(5).split(",");
                lo = Integer.parseInt(p[0].trim());
                hi = Integer.parseInt(p[1].trim());
            }
            return termLen < lo ? 0 : termLen < hi ? 1 : 2;
        }
        return Math.min(2, Integer.parseInt(fuzziness));
    }

    /** Damerau-Levenshtein (adjacent transposition = 1) with an early cut-off. */
    static int editDistance(String a, String b, int max, boolean transpositions) {
        int n = a.length(), m = b.length();
        if (Math.abs(n - m) > max) {
            return max + 1;
        }
        int[][] dp = new int[n + 1][m + 1];
        for (int i = 0; i <= n; i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= m; j++) {
            dp[0][j] = j;
        }
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
                if (transpositions && i > 1 && j > 1 && a.charAt(i - 1) == b.charAt(j - 2) && a.charAt(i - 2) == b.charAt(j - 1)) {
                    dp[i][j] = Math.min(dp[i][j], dp[i - 2][j - 2] + 1);
                }
            }
        }
        return dp[n][m];
    }

    static final class Fuzzy extends Node {
        final String field;
        final String value;
        final String fuzziness;
        final int prefixLength;
        final boolean transpositions;
        final int maxExpansions;

        Fuzzy(String field, String value, String fuzziness, int prefixLength, boolean transpositions) {
            this(field, value, fuzziness, prefixLength, transpositions, 50);
        }

        Fuzzy(String field, String value, String fuzziness, int prefixLength, boolean transpositions, int maxExpansions) {
            this.field = field;
            this.value = value;
            this.fuzziness = fuzziness;
            this.prefixLength = prefixLength;
            this.transpositions = transpositions;
            this.maxExpansions = maxExpansions;
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null || !f.isString()) {
                return NO;
            }
            String v = f.isKeyword() ? Values.normalize(f, value, d.ix.settings) : value;
            return fuzzyScore(d, f, v, fuzzyDistance(fuzziness, v.length()), prefixLength, maxExpansions, transpositions, boost);
        }

        void collect(Hl h) {
            h.term(field, value);
            h.patterns.add(Pattern.compile(Pattern.quote(value)));
        }
    }

    /** Score of one fuzzy term in one document (sum over the expansions the document contains). */
    static double fuzzyScore(DocCtx d, Mappings.Field f, String term, int maxEdits, int prefixLength, int maxExpansions, boolean transpositions,
            double boost) {
        IndexCtx.Fuzzed fz = d.ix.fuzzyExpand(f, term, maxEdits, prefixLength, maxExpansions, transpositions);
        if (fz.terms().isEmpty()) {
            return NO;
        }
        IndexCtx.FieldStats st = d.ix.stats(f);
        List<Analysis.Token> toks = d.tokens(f);
        Map<String, Integer> tf = new HashMap<>();
        for (Analysis.Token t : toks) {
            tf.merge(t.term(), 1, Integer::sum);
        }
        double idf = IndexCtx.idfDf(st, fz.blendedDf());
        double sum = NO;
        for (int i = 0; i < fz.terms().size(); i++) {
            Integer c = tf.get(fz.terms().get(i));
            if (c != null) {
                double s = IndexCtx.bm25(idf, c, toks.size(), f.isText(), st, boost * fz.boosts().get(i));
                sum = Double.isNaN(sum) ? s : sum + s;
            }
        }
        return sum;
    }

    // ---------------------------------------------------------------- range

    static final class Range extends Node {
        final String field;
        final JsonElement gt, gte, lt, lte;
        final String format;
        final String timeZone;
        private final Map<Mappings.Field, Object[]> compiled = new IdentityHashMap<>();

        Range(String field, JsonElement gt, JsonElement gte, JsonElement lt, JsonElement lte, String format, String timeZone) {
            this.field = field;
            this.gt = gt;
            this.gte = gte;
            this.lt = lt;
            this.lte = lte;
            this.format = format;
            this.timeZone = timeZone;
        }

        private Object bound(Mappings.Field f, JsonElement e, boolean roundUp, IndexCtx ix) {
            if (e == null || e.isJsonNull()) {
                return null;
            }
            try {
                if (f.type.equals("date") || f.type.equals("date_nanos")) {
                    ZoneId z = Dates.zone(timeZone);
                    String fmt = format != null ? format : f.format;
                    if (e.getAsJsonPrimitive().isNumber() && (fmt == null || fmt.contains("epoch_millis"))) {
                        return e.getAsLong();
                    }
                    return Dates.parseMath(e.getAsString(), fmt, z, roundUp, ix.now);
                }
                if (f.isString()) {
                    return f.isKeyword() ? Values.normalize(f, str(e), ix.settings) : str(e);
                }
                Object v = Values.parse(f, e, ix.settings, ix.zone());
                if (v == null) {
                    throw new IllegalArgumentException("bad bound");
                }
                return v;
            } catch (IllegalArgumentException ex) {
                throw new OpenSearchException("parse_exception", ex.getMessage() + ": [" + ex.getMessage() + "]").asShardLevel();
            }
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            Object[] b = compiled.computeIfAbsent(f, k -> new Object[] {bound(f, gt, true, d.ix), bound(f, gte, false, d.ix),
                    bound(f, lt, false, d.ix), bound(f, lte, true, d.ix)});
            if (f.type.equals("boolean") || f.type.equals("geo_point") || f.type.equals("knn_vector")) {
                return NO;
            }
            List<?> vals = f.isString() ? d.tokens(f).stream().map(Analysis.Token::term).toList() : d.values(f);
            for (Object v : vals) {
                if (b[0] != null && Values.compare(v, b[0]) <= 0) {
                    continue;
                }
                if (b[1] != null && Values.compare(v, b[1]) < 0) {
                    continue;
                }
                if (b[2] != null && Values.compare(v, b[2]) >= 0) {
                    continue;
                }
                if (b[3] != null && Values.compare(v, b[3]) > 0) {
                    continue;
                }
                return boost;
            }
            return NO;
        }
    }

    // ---------------------------------------------------------------- full text

    /** minimum_should_match: "2", "-1", "75%", "-25%", "3&lt;90%", "2&lt;-25% 9&lt;-3". */
    static int msm(String spec, int optional) {
        if (spec == null) {
            return -1;
        }
        spec = spec.trim();
        int result = optional;
        if (spec.contains("<")) {
            String[] conds = spec.split("\\s+");
            result = optional;
            for (String c : conds) {
                String[] p = c.split("<");
                int threshold = Integer.parseInt(p[0]);
                if (optional <= threshold) {
                    return Math.max(0, Math.min(optional, result));
                }
                result = msm(p[1], optional);
            }
            return Math.max(0, Math.min(optional, result));
        }
        if (spec.endsWith("%")) {
            double pct = Double.parseDouble(spec.substring(0, spec.length() - 1));
            int v = (int) (optional * pct / 100.0);
            result = pct < 0 ? optional + v : v;
        } else {
            int v = Integer.parseInt(spec);
            result = v < 0 ? optional + v : v;
        }
        return Math.max(0, Math.min(optional, result));
    }

    static final class Match extends Node {
        final String field;
        final String text;
        final boolean and;
        final String msm;
        final String analyzer;
        final String fuzziness;
        final int prefixLength;
        final String zeroTerms;
        final boolean lenient;
        final String type; // bool, phrase, phrase_prefix, bool_prefix
        final int slop;
        final boolean transpositions;

        Match(String field, String text, boolean and, String msm, String analyzer, String fuzziness, int prefixLength,
                String zeroTerms, boolean lenient, String type, int slop, boolean transpositions) {
            this.field = field;
            this.text = text;
            this.and = and;
            this.msm = msm;
            this.analyzer = analyzer;
            this.fuzziness = fuzziness;
            this.prefixLength = prefixLength;
            this.zeroTerms = zeroTerms;
            this.lenient = lenient;
            this.type = type;
            this.slop = slop;
            this.transpositions = transpositions;
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            if (f.type.equals("boolean")) {
                Term tq = new Term(field, new JsonPrimitive(text), false);
                tq.boost = boost;
                return tq.score(d);
            }
            if (!f.isString()) {
                if (f.type.equals("knn_vector") || f.type.equals("geo_point")) {
                    return NO;
                }
                Object q = Values.parse(f, new JsonPrimitive(text), d.ix.settings, d.ix.zone());
                if (q == null) {
                    if (lenient) {
                        return NO;
                    }
                    throw new OpenSearchException("illegal_argument_exception", "failed to create query: For input string: \"" + text + "\"")
                            .asShardLevel();
                }
                for (Object v : d.values(f)) {
                    if (Values.compare(v, q) == 0) {
                        return boost;
                    }
                }
                return NO;
            }
            Analysis.Analyzer an = analyzer != null ? d.ix.analyzer(analyzer) : d.ix.searchAnalyzer(f);
            List<Analysis.Token> q = an.analyze(text);
            if (q.isEmpty()) {
                return zeroTerms.equals("all") ? boost : NO;
            }
            IndexCtx.FieldStats st = d.ix.stats(f);
            List<Analysis.Token> toks = d.tokens(f);
            boolean norms = f.isText();
            if (type.equals("phrase") || type.equals("phrase_prefix")) {
                if (q.size() == 1 && type.equals("phrase")) {
                    return termScore(d, f, q.get(0).term(), toks, st, norms, false, 0);
                }
                return phrase(d, f, q, toks, st, norms);
            }
            boolean lastPrefix = type.equals("bool_prefix");
            int n = q.size();
            int need = and ? n : (msm == null ? 1 : Math.max(1, msm(msm, n)));
            double sum = 0;
            int hits = 0;
            for (int i = 0; i < n; i++) {
                String t = q.get(i).term();
                double s = termScore(d, f, t, toks, st, norms, lastPrefix && i == n - 1, fuzziness == null ? -1 : fuzzyDistance(fuzziness, t.length()));
                if (matched(s)) {
                    sum += s;
                    hits++;
                }
            }
            return hits >= need ? sum : NO;
        }

        private double termScore(DocCtx dctx, Mappings.Field dfield, String t, List<Analysis.Token> toks, IndexCtx.FieldStats st, boolean norms, boolean prefix, int fuzz) {
            if (prefix) {
                boolean any = false;
                for (Analysis.Token k : toks) {
                    if (k.term().startsWith(t)) {
                        any = true;
                        break;
                    }
                }
                return any ? boost : NO;
            }
            if (fuzz > 0) {
                return fuzzyScore(dctx, dfield, t, fuzz, prefixLength, 50, transpositions, boost);
            }
            int tf = 0;
            for (Analysis.Token k : toks) {
                if (k.term().equals(t)) {
                    tf++;
                }
            }
            return tf == 0 ? NO : IndexCtx.bm25(IndexCtx.idf(st, t), tf, toks.size(), norms, st, boost);
        }

        private double phrase(DocCtx d, Mappings.Field f, List<Analysis.Token> q, List<Analysis.Token> toks, IndexCtx.FieldStats st, boolean norms) {
            Map<String, List<Integer>> positions = new HashMap<>();
            for (Analysis.Token t : toks) {
                positions.computeIfAbsent(t.term(), k -> new ArrayList<>()).add(t.pos());
            }
            List<String> terms = q.stream().map(Analysis.Token::term).toList();
            int[] offs = new int[q.size()];
            for (int i = 0; i < offs.length; i++) {
                offs[i] = q.get(i).pos() - q.get(0).pos();
            }
            double freq = 0;
            double idf = 0;
            if (type.equals("phrase_prefix")) {
                String last = terms.get(terms.size() - 1);
                for (int i = 0; i < terms.size() - 1; i++) {
                    idf += IndexCtx.idf(st, terms.get(i));
                }
                int count = 0;
                for (String cand : new java.util.TreeSet<>(st.docFreq.keySet())) {
                    if (cand.startsWith(last) && count++ < 50) {
                        idf += IndexCtx.idf(st, cand);
                        if (positions.containsKey(cand)) {
                            List<String> t2 = new ArrayList<>(terms);
                            t2.set(t2.size() - 1, cand);
                            freq += phraseFreq(positions, t2, offs, slop);
                        }
                    }
                }
            } else {
                freq = phraseFreq(positions, terms, offs, slop);
                for (String t : terms) {
                    idf += IndexCtx.idf(st, t);
                }
            }
            if (freq <= 0) {
                return NO;
            }
            return IndexCtx.bm25(idf, freq, toks.size(), norms, st, boost);
        }

        void collect(Hl h) {
            for (String t : new LinkedHashSet<>(termsOf(h))) {
                if (type.equals("phrase_prefix") || type.equals("bool_prefix")) {
                    h.prefix(field, t);
                }
                h.term(field, t);
            }
        }

        private List<String> termsOf(Hl h) {
            List<String> out = new ArrayList<>();
            for (Analysis.Token t : Analysis.STANDARD.analyze(text)) {
                out.add(t.term());
            }
            // the exact analyzer is index-specific; the highlighter re-analyses field text with the field analyzer and
            // compares lower-cased terms, so the standard tokenisation of the query text is the right approximation
            return out;
        }
    }

    static double phraseFreq(Map<String, List<Integer>> positions, List<String> terms, int[] offs, int slop) {
        for (String t : terms) {
            if (!positions.containsKey(t)) {
                return 0;
            }
        }
        if (slop == 0) {
            double freq = 0;
            for (int p : positions.get(terms.get(0))) {
                boolean ok = true;
                for (int i = 1; i < terms.size() && ok; i++) {
                    ok = positions.get(terms.get(i)).contains(p + offs[i]);
                }
                if (ok) {
                    freq += 1;
                }
            }
            return freq;
        }
        List<int[]> events = new ArrayList<>();
        for (int i = 0; i < terms.size(); i++) {
            for (int p : positions.get(terms.get(i))) {
                events.add(new int[] {p - offs[i], i});
            }
        }
        events.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(a[1], b[1]));
        int[] counts = new int[terms.size()];
        int covered = 0, l = 0;
        double freq = 0;
        for (int r = 0; r < events.size(); r++) {
            if (counts[events.get(r)[1]]++ == 0) {
                covered++;
            }
            while (covered == terms.size()) {
                int span = events.get(r)[0] - events.get(l)[0];
                if (span <= slop) {
                    freq += 1.0 / (1 + span);
                }
                if (--counts[events.get(l)[1]] == 0) {
                    covered--;
                }
                l++;
            }
        }
        return freq;
    }

    // ---------------------------------------------------------------- compound

    static final class Bool extends Node {
        final List<Node> must = new ArrayList<>(), filter = new ArrayList<>(), should = new ArrayList<>(), mustNot = new ArrayList<>();
        String msm;

        List<Node> children() {
            List<Node> l = new ArrayList<>(must);
            l.addAll(filter);
            l.addAll(should);
            l.addAll(mustNot);
            return l;
        }

        double score(DocCtx d) {
            double sum = 0;
            for (Node n : must) {
                double s = n.score(d);
                if (!matched(s)) {
                    return NO;
                }
                sum += s;
            }
            for (Node n : filter) {
                if (!matched(n.score(d))) {
                    return NO;
                }
            }
            for (Node n : mustNot) {
                if (matched(n.score(d))) {
                    return NO;
                }
            }
            int need = msm != null ? msm(msm, should.size()) : (must.isEmpty() && filter.isEmpty() ? (should.isEmpty() ? 0 : 1) : 0);
            int hits = 0;
            double shouldSum = 0;
            for (Node n : should) {
                double s = n.score(d);
                if (matched(s)) {
                    hits++;
                    shouldSum += s;
                }
            }
            if (hits < need) {
                return NO;
            }
            if (must.isEmpty() && filter.isEmpty() && should.isEmpty() && !mustNot.isEmpty()) {
                return 0.0;
            }
            if (must.isEmpty() && filter.isEmpty() && should.isEmpty() && mustNot.isEmpty()) {
                return boost;
            }
            return (sum + shouldSum) * boost;
        }

        void collect(Hl h) {
            for (Node n : must) {
                n.collect(h);
            }
            for (Node n : filter) {
                n.collect(h);
            }
            for (Node n : should) {
                n.collect(h);
            }
        }
    }

    static final class ConstantScore extends Node {
        final Node inner;

        ConstantScore(Node inner) {
            this.inner = inner;
        }

        List<Node> children() {
            return List.of(inner);
        }

        double score(DocCtx d) {
            return matched(inner.score(d)) ? boost : NO;
        }

        void collect(Hl h) {
            inner.collect(h);
        }
    }

    static final class DisMax extends Node {
        final List<Node> queries;
        final double tie;

        DisMax(List<Node> queries, double tie) {
            this.queries = queries;
            this.tie = tie;
        }

        List<Node> children() {
            return queries;
        }

        double score(DocCtx d) {
            double max = NO, sum = 0;
            boolean any = false;
            for (Node n : queries) {
                double s = n.score(d);
                if (matched(s)) {
                    any = true;
                    sum += s;
                    max = Double.isNaN(max) ? s : Math.max(max, s);
                }
            }
            return any ? (max + tie * (sum - max)) * boost : NO;
        }

        void collect(Hl h) {
            for (Node n : queries) {
                n.collect(h);
            }
        }
    }

    static final class Boosting extends Node {
        final Node positive, negative;
        final double negBoost;

        Boosting(Node positive, Node negative, double negBoost) {
            this.positive = positive;
            this.negative = negative;
            this.negBoost = negBoost;
        }

        List<Node> children() {
            return List.of(positive, negative);
        }

        double score(DocCtx d) {
            double s = positive.score(d);
            if (!matched(s)) {
                return NO;
            }
            return (matched(negative.score(d)) ? s * negBoost : s) * boost;
        }

        void collect(Hl h) {
            positive.collect(h);
        }
    }

    static final class NestedQ extends Node {
        final String path;
        final Node inner;
        final String scoreMode;
        final boolean ignoreUnmapped;
        String innerHitsName;

        NestedQ(String path, Node inner, String scoreMode, boolean ignoreUnmapped) {
            this.path = path;
            this.inner = inner;
            this.scoreMode = scoreMode;
            this.ignoreUnmapped = ignoreUnmapped;
        }

        List<Object[]> matches(DocCtx d) {
            if (!d.ix.mappings.nestedPaths.contains(path)) {
                if (ignoreUnmapped) {
                    return List.of();
                }
                throw new OpenSearchException("query_shard_exception", "[nested] failed to find nested object under path [" + path + "]")
                        .with("index", d.ix.indexName).asShardLevel();
            }
            List<Object[]> out = new ArrayList<>();
            String rel = path.startsWith(d.prefix) ? path.substring(d.prefix.length()) : path;
            int i = 0;
            for (JsonElement e : Mappings.values(d.obj, rel)) {
                if (e.isJsonObject()) {
                    DocCtx nd = d.nested(e.getAsJsonObject(), path + ".");
                    double s = inner.score(nd);
                    if (matched(s)) {
                        out.add(new Object[] {i, s, e.getAsJsonObject()});
                    }
                }
                i++;
            }
            return out;
        }

        List<Node> children() {
            return List.of(inner);
        }

        double score(DocCtx d) {
            List<Object[]> ms = matches(d);
            if (ms.isEmpty()) {
                return NO;
            }
            double sum = 0, max = 0, min = Double.MAX_VALUE;
            for (Object[] m : ms) {
                double s = (Double) m[1];
                sum += s;
                max = Math.max(max, s);
                min = Math.min(min, s);
            }
            double v = switch (scoreMode) {
                case "sum" -> sum;
                case "max" -> max;
                case "min" -> min;
                case "none" -> 0;
                default -> sum / ms.size();
            };
            return v * boost;
        }

        void collect(Hl h) {
            inner.collect(h);
        }
    }

    /** function_score -- weight, field_value_factor, random_score(seeded, deterministic), decay functions. */
    static final class FunctionScore extends Node {
        record Fn(Node filter, double weight, String kind, JsonObject spec) {
        }

        final Node query;
        final List<Fn> fns = new ArrayList<>();
        String scoreMode = "multiply", boostMode = "multiply";
        double maxBoost = Double.MAX_VALUE;
        Double minScore;

        FunctionScore(Node query) {
            this.query = query;
        }

        List<Node> children() {
            List<Node> l = new ArrayList<>();
            l.add(query);
            for (Fn f : fns) {
                if (f.filter != null) {
                    l.add(f.filter);
                }
            }
            return l;
        }

        double score(DocCtx d) {
            double qs = query.score(d);
            if (!matched(qs)) {
                return NO;
            }
            double fscore = 1.0;
            if (!fns.isEmpty()) {
                List<Double> vals = new ArrayList<>();
                for (Fn fn : fns) {
                    if (fn.filter != null && !matched(fn.filter.score(d))) {
                        continue;
                    }
                    double v = fnValue(fn, d);
                    if (!Double.isNaN(v)) {
                        vals.add(v);
                        if (scoreMode.equals("first")) {
                            break;
                        }
                    }
                }
                if (!vals.isEmpty()) {
                    fscore = switch (scoreMode) {
                        case "sum" -> vals.stream().mapToDouble(x -> x).sum();
                        case "avg" -> vals.stream().mapToDouble(x -> x).average().orElse(1);
                        case "max" -> vals.stream().mapToDouble(x -> x).max().orElse(1);
                        case "min" -> vals.stream().mapToDouble(x -> x).min().orElse(1);
                        case "first" -> vals.get(0);
                        default -> vals.stream().mapToDouble(x -> x).reduce(1, (a, b) -> a * b);
                    };
                }
            }
            fscore = Math.min(fscore, maxBoost);
            double out = switch (boostMode) {
                case "replace" -> fscore;
                case "sum" -> qs + fscore;
                case "avg" -> (qs + fscore) / 2;
                case "max" -> Math.max(qs, fscore);
                case "min" -> Math.min(qs, fscore);
                default -> qs * fscore;
            };
            if (minScore != null && out < minScore) {
                return NO;
            }
            return out * boost;
        }

        private double fnValue(Fn fn, DocCtx d) {
            switch (fn.kind) {
                case "weight":
                    return fn.weight;
                case "field_value_factor": {
                    JsonObject s = fn.spec;
                    String field = s.get("field").getAsString();
                    double factor = s.has("factor") ? s.get("factor").getAsDouble() : 1;
                    String mod = s.has("modifier") ? s.get("modifier").getAsString() : "none";
                    Mappings.Field f = d.field(field);
                    double v;
                    List<Object> vals = f == null ? List.of() : d.values(f);
                    if (vals.isEmpty()) {
                        if (!s.has("missing")) {
                            throw new OpenSearchException("illegal_argument_exception",
                                    "Missing value for field [" + field + "]").asShardLevel();
                        }
                        v = s.get("missing").getAsDouble();
                    } else {
                        v = Values.toDouble(vals.get(0));
                    }
                    v *= factor;
                    v = switch (mod) {
                        case "log" -> Math.log10(v);
                        case "log1p" -> Math.log10(v + 1);
                        case "log2p" -> Math.log10(v + 2);
                        case "ln" -> Math.log(v);
                        case "ln1p" -> Math.log1p(v);
                        case "ln2p" -> Math.log(v + 2);
                        case "square" -> v * v;
                        case "sqrt" -> Math.sqrt(v);
                        case "reciprocal" -> 1.0 / v;
                        default -> v;
                    };
                    return v * fn.weight;
                }
                case "random_score": {
                    long seed = fn.spec.has("seed") ? fn.spec.get("seed").getAsLong() : 0;
                    long h = (d.doc.id.hashCode() * 31L + seed) * 0x9E3779B97F4A7C15L;
                    return ((h >>> 11) / (double) (1L << 53)) * fn.weight;
                }
                default: {
                    JsonObject s = fn.spec;
                    String field = s.keySet().stream().filter(k -> s.get(k).isJsonObject()).findFirst().orElse(null);
                    if (field == null) {
                        return NO;
                    }
                    JsonObject p = s.getAsJsonObject(field);
                    Mappings.Field f = d.field(field);
                    List<Object> vals = f == null ? List.of() : d.values(f);
                    if (vals.isEmpty()) {
                        return 1.0 * fn.weight;
                    }
                    boolean isDate = f.type.equals("date");
                    double origin = isDate && p.has("origin") ? Dates.parseMath(p.get("origin").getAsString(), null, d.ix.zone(), false, d.ix.now)
                            : isDate ? d.ix.now : p.get("origin").getAsDouble();
                    double scale = numberWithUnit(p.get("scale").getAsString(), isDate);
                    double offset = p.has("offset") ? numberWithUnit(p.get("offset").getAsString(), isDate) : 0;
                    double decay = p.has("decay") ? p.get("decay").getAsDouble() : 0.5;
                    double dist = Math.max(0, Math.abs(Values.toDouble(vals.get(0)) - origin) - offset);
                    double v = switch (fn.kind) {
                        case "gauss" -> Math.exp(-(dist * dist) / (2 * (scale * scale / (-2 * Math.log(decay)))));
                        case "exp" -> Math.exp(dist * (Math.log(decay) / scale));
                        default -> Math.max(0.0, (scale / (1 - decay) - dist) / (scale / (1 - decay)));
                    };
                    return v * fn.weight;
                }
            }
        }

        private static double numberWithUnit(String s, boolean date) {
            if (!date) {
                return Double.parseDouble(s);
            }
            java.util.regex.Matcher m = Pattern.compile("(\\d+(?:\\.\\d+)?)(ms|s|m|h|d)?").matcher(s);
            if (!m.matches()) {
                return Double.parseDouble(s);
            }
            double n = Double.parseDouble(m.group(1));
            String u = m.group(2) == null ? "ms" : m.group(2);
            return n * switch (u) {
                case "s" -> 1000.0;
                case "m" -> 60000.0;
                case "h" -> 3600000.0;
                case "d" -> 86400000.0;
                default -> 1.0;
            };
        }

        void collect(Hl h) {
            query.collect(h);
        }
    }

    /** multi_match. Field entries are (name, boost); wildcard names are expanded against the mapping at match time. */
    static final class MultiMatch extends Node {
        final List<Map.Entry<String, Double>> fields;
        final String text;
        final String type;
        final boolean and;
        final String msm;
        final String analyzer;
        final String fuzziness;
        final int slop;
        final double tie;
        final boolean lenient;
        final String zeroTerms;
        final int prefixLength;

        MultiMatch(List<Map.Entry<String, Double>> fields, String text, String type, boolean and, String msm, String analyzer,
                String fuzziness, int slop, double tie, boolean lenient, String zeroTerms, int prefixLength) {
            this.fields = fields;
            this.text = text;
            this.type = type;
            this.and = and;
            this.msm = msm;
            this.analyzer = analyzer;
            this.fuzziness = fuzziness;
            this.slop = slop;
            this.tie = tie;
            this.lenient = lenient;
            this.zeroTerms = zeroTerms;
            this.prefixLength = prefixLength;
        }

        List<Map.Entry<String, Double>> expand(DocCtx d) {
            List<Map.Entry<String, Double>> out = new ArrayList<>();
            for (var e : fields) {
                String n = e.getKey();
                if (n.contains("*")) {
                    for (Mappings.Field f : d.ix.mappings.fields.values()) {
                        if (Mappings.wildcardMatch(n, f.path) && eligible(f) && d.field(f.path) != null) {
                            out.add(Map.entry(f.path, e.getValue()));
                        }
                    }
                } else {
                    out.add(e);
                }
            }
            return out;
        }

        static boolean eligible(Mappings.Field f) {
            return f.isString() || f.isNumeric() || f.type.equals("date") || f.type.equals("boolean") || f.type.equals("ip");
        }

        Match make(String field, double fieldBoost, String matchType) {
            Match m = new Match(field, text, and, msm, analyzer, fuzziness, prefixLength, zeroTerms, lenient, matchType, slop, true);
            m.boost = fieldBoost * boost;
            return m;
        }

        double score(DocCtx d) {
            List<Map.Entry<String, Double>> fs = expand(d);
            if (fs.isEmpty()) {
                return NO;
            }
            switch (type) {
                case "most_fields", "bool_prefix": {
                    double sum = 0;
                    boolean any = false;
                    for (var e : fs) {
                        double s = make(e.getKey(), e.getValue(), type.equals("bool_prefix") ? "bool_prefix" : "bool").score(d);
                        if (matched(s)) {
                            any = true;
                            sum += s;
                        }
                    }
                    return any ? sum : NO;
                }
                case "phrase", "phrase_prefix": {
                    List<Node> qs = new ArrayList<>();
                    for (var e : fs) {
                        qs.add(make(e.getKey(), e.getValue(), type));
                    }
                    return new DisMax(qs, tie).score(d);
                }
                case "cross_fields": {
                    // term-centric: every query term must be found in SOME field (AND) or any (OR); per term the best field wins
                    Mappings.Field f0 = d.field(fs.get(0).getKey());
                    Analysis.Analyzer an = analyzer != null ? d.ix.analyzer(analyzer)
                            : f0 != null && f0.isString() ? d.ix.searchAnalyzer(f0) : Analysis.STANDARD;
                    List<Analysis.Token> qt = an.analyze(text);
                    if (qt.isEmpty()) {
                        return zeroTerms.equals("all") ? boost : NO;
                    }
                    double sum = 0;
                    int found = 0;
                    for (Analysis.Token t : qt) {
                        double best = NO, tot = 0;
                        for (var e : fs) {
                            Match m = new Match(e.getKey(), t.term(), false, null, null, null, 0, "none", lenient,
                                    "bool", 0, true);
                            m.boost = e.getValue() * boost;
                            double s = m.score(d);
                            if (matched(s)) {
                                tot += s;
                                best = Double.isNaN(best) ? s : Math.max(best, s);
                            }
                        }
                        if (matched(best)) {
                            found++;
                            sum += best + tie * (tot - best);
                        }
                    }
                    int need = and ? qt.size() : (msm == null ? 1 : Math.max(1, msm(msm, qt.size())));
                    return found >= need ? sum : NO;
                }
                default: {
                    List<Node> qs = new ArrayList<>();
                    for (var e : fs) {
                        qs.add(make(e.getKey(), e.getValue(), "bool"));
                    }
                    return new DisMax(qs, tie).score(d);
                }
            }
        }

        void collect(Hl h) {
            for (var e : fields) {
                for (Analysis.Token t : Analysis.STANDARD.analyze(text)) {
                    h.term(e.getKey(), t.term());
                }
            }
        }
    }

    // ---------------------------------------------------------------- geo

    static final class GeoDistance extends Node {
        final String field;
        final double lat, lon, meters;

        GeoDistance(String field, double lat, double lon, double meters) {
            this.field = field;
            this.lat = lat;
            this.lon = lon;
            this.meters = meters;
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            for (JsonElement e : Mappings.values(d.obj, d.rel(f))) {
                double[] p = Geo.point(e);
                if (p != null && Geo.haversine(lat, lon, p[0], p[1]) <= meters) {
                    return boost;
                }
            }
            return NO;
        }
    }

    static final class GeoBox extends Node {
        final String field;
        final double top, left, bottom, right;

        GeoBox(String field, double top, double left, double bottom, double right) {
            this.field = field;
            this.top = top;
            this.left = left;
            this.bottom = bottom;
            this.right = right;
        }

        double score(DocCtx d) {
            Mappings.Field f = d.field(field);
            if (f == null) {
                return NO;
            }
            for (JsonElement e : Mappings.values(d.obj, d.rel(f))) {
                double[] p = Geo.point(e);
                if (p != null && p[0] <= top && p[0] >= bottom && (left <= right ? p[1] >= left && p[1] <= right : p[1] >= left || p[1] <= right)) {
                    return boost;
                }
            }
            return NO;
        }
    }

    /** A wrapper adding {@code _name} tracking is done by the engine; this node just delegates. */
    static final class Named extends Node {
        final Node inner;

        Named(Node inner, String name) {
            this.inner = inner;
            this.name = name;
        }

        List<Node> children() {
            return List.of(inner);
        }

        double score(DocCtx d) {
            return inner.score(d);
        }

        void collect(Hl h) {
            inner.collect(h);
        }
    }

    static List<String> sorted(Set<String> s) {
        List<String> l = new ArrayList<>(s);
        Collections.sort(l);
        return l;
    }

    static Map<String, Object> newOrdered() {
        return new LinkedHashMap<>();
    }

    static TreeMap<String, Object> newSorted() {
        return new TreeMap<>();
    }

    static Set<String> newSet() {
        return new HashSet<>();
    }
}
