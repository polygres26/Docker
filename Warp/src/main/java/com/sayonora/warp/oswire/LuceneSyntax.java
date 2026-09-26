package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code query_string} (Lucene classic syntax: {@code field:term}, {@code "phrase"~2}, {@code AND/OR/NOT}, {@code + -},
 * {@code *}/{@code ?} wildcards, {@code term~1} fuzzy, {@code [a TO b]} / {@code >=5} ranges, {@code /regex/},
 * {@code (groups)}, {@code ^boost}, {@code _exists_:f}) and {@code simple_query_string} ({@code + | - " * ( ) ~N}),
 * compiled to {@link Query} nodes. Multi-field terms use best_fields semantics like OpenSearch.
 */
final class LuceneSyntax {

    private final String s;
    private int p;
    private final List<Map.Entry<String, Double>> defaultFields;
    private final boolean and;
    private final String analyzer;
    private final String fuzziness;
    private final int phraseSlop;
    private final boolean lenient;
    private final double tie;
    private final boolean simple;
    private final boolean allowLeadingWildcard;

    private LuceneSyntax(String s, List<Map.Entry<String, Double>> fields, boolean and, String analyzer, String fuzziness, int slop,
            boolean lenient, double tie, boolean simple, boolean allowLeadingWildcard) {
        this.s = s;
        this.defaultFields = fields;
        this.and = and;
        this.analyzer = analyzer;
        this.fuzziness = fuzziness;
        this.phraseSlop = slop;
        this.lenient = lenient;
        this.tie = tie;
        this.simple = simple;
        this.allowLeadingWildcard = allowLeadingWildcard;
    }

    static Query.Node fromRequest(String type, JsonObject o) {
        boolean simple = type.equals("simple_query_string");
        if (!o.has("query")) {
            throw QueryParser.err("[" + type + "] requires a query");
        }
        String q = o.get("query").getAsString();
        List<Map.Entry<String, Double>> fields;
        if (o.has("fields")) {
            fields = QueryParser.fieldList(o.get("fields"));
        } else if (o.has("default_field")) {
            fields = QueryParser.fieldList(o.get("default_field"));
        } else {
            fields = QueryParser.fieldList(null);
        }
        boolean and = o.has("default_operator") && o.get("default_operator").getAsString().equalsIgnoreCase("and");
        boolean wildcardFields = fields.stream().anyMatch(e -> e.getKey().contains("*"));
        boolean lenient = (o.has("lenient") && o.get("lenient").getAsBoolean()) || wildcardFields || simple;
        LuceneSyntax ls = new LuceneSyntax(q, fields, and, o.has("analyzer") ? o.get("analyzer").getAsString() : null,
                o.has("fuzziness") ? o.get("fuzziness").getAsString() : "AUTO",
                o.has("phrase_slop") ? o.get("phrase_slop").getAsInt() : 0, lenient,
                o.has("tie_breaker") ? o.get("tie_breaker").getAsDouble() : 0.0, simple,
                !o.has("allow_leading_wildcard") || o.get("allow_leading_wildcard").getAsBoolean());
        Query.Node n = simple ? ls.parseSimple() : ls.parseTop();
        if (o.has("minimum_should_match") && n instanceof Query.Bool b) {
            b.msm = o.get("minimum_should_match").getAsString();
        }
        n.boost = o.has("boost") ? o.get("boost").getAsDouble() : 1.0;
        if (o.has("_name")) {
            return new Query.Named(n, o.get("_name").getAsString());
        }
        return n;
    }

    /** Parses a URI-search {@code q=} string (query_string with default operator/field). */
    static Query.Node fromUri(String q, String defaultField, boolean and, boolean lenient, String analyzer) {
        JsonObject o = new JsonObject();
        o.addProperty("query", q);
        if (defaultField != null) {
            o.addProperty("default_field", defaultField);
        }
        o.addProperty("default_operator", and ? "and" : "or");
        if (lenient) {
            o.addProperty("lenient", true);
        }
        if (analyzer != null) {
            o.addProperty("analyzer", analyzer);
        }
        return fromRequest("query_string", o);
    }

    // ------------------------------------------------------------ classic

    private OpenSearchException fail(String detail) {
        return new OpenSearchException("query_shard_exception", "Failed to parse query [" + s + "]").asShardLevel();
    }

    private boolean eof() {
        return p >= s.length();
    }

    private void ws() {
        while (!eof() && Character.isWhitespace(s.charAt(p))) {
            p++;
        }
    }

    private Query.Node parseTop() {
        if (s.isBlank()) {
            return new Query.MatchNone();
        }
        Query.Node n = parseGroup(null, false);
        if (!eof()) {
            throw fail("Cannot parse '" + s + "': unexpected '" + s.charAt(p) + "'");
        }
        return n;
    }

    private record Clause(Query.Node node, char occur) {
    }

    /** One level of clauses until end of input or {@code )}. */
    private Query.Node parseGroup(String field, boolean nestedGroup) {
        List<Clause> clauses = new ArrayList<>();
        String pendingConj = null;
        while (true) {
            ws();
            if (eof() || s.charAt(p) == ')') {
                break;
            }
            String conj = null;
            if (s.startsWith("&&", p)) {
                conj = "AND";
                p += 2;
                continue_ws();
            } else if (s.startsWith("||", p)) {
                conj = "OR";
                p += 2;
                continue_ws();
            } else if (word("AND")) {
                conj = "AND";
            } else if (word("OR")) {
                conj = "OR";
            }
            if (conj != null) {
                pendingConj = conj;
                if (conj.equals("AND") && !clauses.isEmpty() && clauses.get(clauses.size() - 1).occur == 'S') {
                    Clause last = clauses.remove(clauses.size() - 1);
                    clauses.add(new Clause(last.node, 'M'));
                }
                continue;
            }
            char mod = 0;
            boolean not = false;
            if (word("NOT")) {
                not = true;
                ws();
            } else if (s.charAt(p) == '+') {
                mod = '+';
                p++;
            } else if (s.charAt(p) == '-' || s.charAt(p) == '!') {
                mod = '-';
                p++;
            }
            Query.Node n = parseClause(field);
            if (n == null) {
                continue;
            }
            char occur;
            if (not || mod == '-') {
                occur = 'N';
            } else if (mod == '+') {
                occur = 'M';
            } else if ("AND".equals(pendingConj) || (pendingConj == null && and)) {
                occur = 'M';
            } else {
                occur = 'S';
            }
            clauses.add(new Clause(n, occur));
            pendingConj = null;
        }
        if (clauses.isEmpty()) {
            return new Query.MatchNone();
        }
        if (clauses.size() == 1 && clauses.get(0).occur != 'N') {
            return clauses.get(0).node;
        }
        Query.Bool b = new Query.Bool();
        for (Clause c : clauses) {
            switch (c.occur) {
                case 'M' -> b.must.add(c.node);
                case 'N' -> b.mustNot.add(c.node);
                default -> b.should.add(c.node);
            }
        }
        return b;
    }

    private void continue_ws() {
        ws();
    }

    private boolean word(String w) {
        if (s.startsWith(w, p) && p + w.length() < s.length() && Character.isWhitespace(s.charAt(p + w.length()))) {
            p += w.length();
            ws();
            return true;
        }
        if (s.startsWith(w, p) && p + w.length() == s.length()) {
            p += w.length();
            return true;
        }
        return false;
    }

    private Query.Node parseClause(String field) {
        ws();
        // field prefix
        String f = field;
        int save = p;
        String fname = readFieldName();
        if (fname != null) {
            f = fname;
            if (f.equals("_exists_")) {
                String target = readTermText();
                return new Query.Exists(target);
            }
        } else {
            p = save;
        }
        Query.Node n;
        ws();
        if (eof()) {
            throw fail("Cannot parse '" + s + "': Encountered \"<EOF>\"");
        }
        char c = s.charAt(p);
        if (c == '(') {
            p++;
            n = parseGroup(f, true);
            ws();
            if (eof() || s.charAt(p) != ')') {
                throw fail("Cannot parse '" + s + "': Encountered \"<EOF>\" expecting \")\"");
            }
            p++;
        } else if (c == '"') {
            n = parsePhrase(f);
        } else if (c == '[' || c == '{') {
            n = parseRange(f);
        } else if ((c == '>' || c == '<') && f != null) {
            n = parseComparison(f);
        } else if (c == '/') {
            int end = s.indexOf('/', p + 1);
            while (end > 0 && s.charAt(end - 1) == '\\') {
                end = s.indexOf('/', end + 1);
            }
            if (end < 0) {
                throw fail("Cannot parse '" + s + "': unterminated regexp");
            }
            String re = s.substring(p + 1, end);
            p = end + 1;
            n = perField(f, name -> new Query.TermPattern(name, "regexp", re, false, null));
        } else {
            n = parseTermClause(f);
        }
        return applyBoost(n);
    }

    private Query.Node applyBoost(Query.Node n) {
        if (!eof() && s.charAt(p) == '^') {
            int st = ++p;
            while (!eof() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.')) {
                p++;
            }
            if (p > st) {
                n.boost = n.boost * Double.parseDouble(s.substring(st, p));
            }
        }
        return n;
    }

    private String readFieldName() {
        int st = p;
        StringBuilder sb = new StringBuilder();
        while (!eof()) {
            char c = s.charAt(p);
            if (c == '\\' && p + 1 < s.length()) {
                sb.append(s.charAt(p + 1));
                p += 2;
                continue;
            }
            if (c == ':') {
                p++;
                return sb.length() == 0 ? null : sb.toString();
            }
            if (Character.isWhitespace(c) || c == '(' || c == ')' || c == '"' || c == '[' || c == ']' || c == '{' || c == '}' || c == '^'
                    || c == '~' || c == '/') {
                break;
            }
            sb.append(c);
            p++;
        }
        p = st;
        return null;
    }

    private String readTermText() {
        StringBuilder sb = new StringBuilder();
        while (!eof()) {
            char c = s.charAt(p);
            if (c == '\\' && p + 1 < s.length()) {
                sb.append('\\').append(s.charAt(p + 1));
                p += 2;
                continue;
            }
            if (Character.isWhitespace(c) || c == '(' || c == ')' || c == '^' || c == '"' || c == '[' || c == ']' || c == '{' || c == '}'
                    || (c == '~')) {
                break;
            }
            sb.append(c);
            p++;
        }
        return sb.toString();
    }

    private static String unescape(String t) {
        return t.replaceAll("\\\\(.)", "$1");
    }

    private interface FieldQuery {
        Query.Node make(String field);
    }

    private Query.Node perField(String f, FieldQuery fq) {
        if (f != null && !f.contains("*")) {
            return fq.make(f);
        }
        List<Query.Node> qs = new ArrayList<>();
        for (Map.Entry<String, Double> e : f != null ? List.of(Map.entry(f, 1.0)) : defaultFields) {
            qs.add(new FieldExpand(e.getKey(), fq).boost(e.getValue()));
        }
        return qs.size() == 1 ? qs.get(0) : new Query.DisMax(qs, tie);
    }

    /** Expands wildcard field names at evaluation time and takes the best matching field (dis_max). */
    private static final class FieldExpand extends Query.Node {
        final String pattern;
        final FieldQuery fq;
        final boolean sum;

        FieldExpand(String pattern, FieldQuery fq) {
            this(pattern, fq, false);
        }

        FieldExpand(String pattern, FieldQuery fq, boolean sum) {
            this.pattern = pattern;
            this.fq = fq;
            this.sum = sum;
        }

        double score(Query.DocCtx d) {
            if (!pattern.contains("*")) {
                return fq.make(pattern).boost(boost).score(d);
            }
            double max = Query.NO, sum = 0;
            for (Mappings.Field f : d.ix.mappings.fields.values()) {
                if (Mappings.wildcardMatch(pattern, f.path) && Query.MultiMatch.eligible(f) && d.field(f.path) != null) {
                    double s = fq.make(f.path).boost(boost).score(d);
                    if (Query.matched(s)) {
                        max = Double.isNaN(max) ? s : Math.max(max, s);
                        sum += s;
                    }
                }
            }
            return this.sum && !Double.isNaN(max) ? sum : max;
        }

        void collect(Query.Hl h) {
            fq.make(pattern).collect(h);
        }
    }

    private Query.Node parseTermClause(String f) {
        String raw = readTermText();
        if (raw.isEmpty()) {
            throw fail("Cannot parse '" + s + "': Encountered \"" + (eof() ? "<EOF>" : String.valueOf(s.charAt(p))) + "\"");
        }
        int tilde = -1;
        String fuzz = null;
        if (!eof() && s.charAt(p) == '~') {
            p++;
            int st = p;
            while (!eof() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.')) {
                p++;
            }
            fuzz = p > st ? s.substring(st, p) : "AUTO";
            tilde = 1;
        }
        boolean wildcard = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == '*' || c == '?') {
                wildcard = true;
            }
        }
        if (raw.equals("*") && f == null) {
            return new Query.MatchAll();
        }
        if (raw.equals("*") && f != null) {
            return perField(f, name -> new Query.Exists(name));
        }
        if (wildcard) {
            if (!allowLeadingWildcard && (raw.startsWith("*") || raw.startsWith("?"))) {
                throw fail("'*' or '?' not allowed as first character in WildcardQuery");
            }
            String w = raw;
            boolean prefixOnly = w.endsWith("*") && w.indexOf('*') == w.length() - 1 && w.indexOf('?') < 0;
            return perField(f, name -> {
                Query.TermPattern tp = prefixOnly ? new Query.TermPattern(name, "prefix", unescape(w.substring(0, w.length() - 1)).toLowerCase(Locale.ROOT), true, null)
                        : new Query.TermPattern(name, "wildcard", w.toLowerCase(Locale.ROOT), true, null);
                return tp;
            });
        }
        String text = unescape(raw);
        if (tilde == 1) {
            String fz = fuzz;
            return perField(f, name -> new Query.Fuzzy(name, text.toLowerCase(Locale.ROOT), fz, 0, true));
        }
        return perField(f, name -> {
            Query.Match m = new Query.Match(name, text, and, null, analyzer, null, 0, "none", lenient, "bool", 0, true);
            return m;
        });
    }

    private Query.Node parsePhrase(String f) {
        p++;
        StringBuilder sb = new StringBuilder();
        while (!eof() && s.charAt(p) != '"') {
            if (s.charAt(p) == '\\' && p + 1 < s.length()) {
                sb.append(s.charAt(p + 1));
                p += 2;
                continue;
            }
            sb.append(s.charAt(p++));
        }
        if (eof()) {
            throw fail("Cannot parse '" + s + "': Lexical error, unterminated quote");
        }
        p++;
        int slop = phraseSlop;
        if (!eof() && s.charAt(p) == '~') {
            p++;
            int st = p;
            while (!eof() && Character.isDigit(s.charAt(p))) {
                p++;
            }
            slop = p > st ? Integer.parseInt(s.substring(st, p)) : 2;
        }
        String text = sb.toString();
        int sl = slop;
        return perField(f, name -> new Query.Match(name, text, false, null, analyzer, null, 0, "none", lenient, "phrase", sl, true));
    }

    private Query.Node parseRange(String f) {
        boolean incLo = s.charAt(p) == '[';
        p++;
        ws();
        String lo = readBound();
        ws();
        if (!s.startsWith("TO", p)) {
            throw fail("Cannot parse '" + s + "': expected TO in range");
        }
        p += 2;
        ws();
        String hi = readBound();
        ws();
        if (eof() || (s.charAt(p) != ']' && s.charAt(p) != '}')) {
            throw fail("Cannot parse '" + s + "': unterminated range");
        }
        boolean incHi = s.charAt(p) == ']';
        p++;
        return rangeNode(f, lo, incLo, hi, incHi);
    }

    private String readBound() {
        StringBuilder sb = new StringBuilder();
        if (!eof() && s.charAt(p) == '"') {
            p++;
            while (!eof() && s.charAt(p) != '"') {
                sb.append(s.charAt(p++));
            }
            p++;
            return sb.toString();
        }
        while (!eof() && !Character.isWhitespace(s.charAt(p)) && s.charAt(p) != ']' && s.charAt(p) != '}') {
            sb.append(s.charAt(p++));
        }
        return sb.toString();
    }

    private Query.Node rangeNode(String f, String lo, boolean incLo, String hi, boolean incHi) {
        JsonElement l = lo.equals("*") ? null : new JsonPrimitive(lo);
        JsonElement h = hi.equals("*") ? null : new JsonPrimitive(hi);
        return perField(f, name -> new Query.Range(name, incLo ? null : l, incLo ? l : null, incHi ? null : h, incHi ? h : null, null, null));
    }

    private Query.Node parseComparison(String f) {
        String op = String.valueOf(s.charAt(p++));
        if (!eof() && s.charAt(p) == '=') {
            op += "=";
            p++;
        }
        String v = readBound();
        JsonElement val = new JsonPrimitive(v);
        String o = op;
        return perField(f, name -> new Query.Range(name, o.equals(">") ? val : null, o.equals(">=") ? val : null, o.equals("<") ? val : null,
                o.equals("<=") ? val : null, null, null));
    }

    // ------------------------------------------------------------ simple_query_string

    /** Lucene's SimpleQueryParser state machine: infix, left-associative; {@code +} = AND with what came before, {@code |} = OR,
     * {@code -x} = (MUST_NOT x, SHOULD match-all); adjacent terms join with the default operator; per-field queries are summed. */
    private Query.Node parseSimple() {
        int[] pos = {0};
        Query.Node top = simpleLevel(s, pos, false);
        return top == null ? new Query.MatchNone() : top;
    }

    private static final class SimpleState {
        Query.Node top;
        char currentOp;   // 0, 'M' (must) or 'S' (should)
        char previousOp;
        int not;
    }

    private Query.Node simpleLevel(String q, int[] pos, boolean nested) {
        SimpleState st = new SimpleState();
        int n = q.length();
        while (pos[0] < n) {
            char c = q.charAt(pos[0]);
            if (Character.isWhitespace(c)) {
                pos[0]++;
            } else if (c == '+') {
                pos[0]++;
                st.currentOp = 'M';
            } else if (c == '|') {
                pos[0]++;
                st.currentOp = 'S';
            } else if (c == '-' && (pos[0] + 1 < n && !Character.isWhitespace(q.charAt(pos[0] + 1)))) {
                pos[0]++;
                st.not++;
            } else if (c == '(') {
                pos[0]++;
                Query.Node sub = simpleLevel(q, pos, true);
                simpleJoin(st, sub);
            } else if (c == ')') {
                pos[0]++;
                if (nested) {
                    return st.top;
                }
            } else if (c == '"') {
                int end = q.indexOf('"', pos[0] + 1);
                String phrase = end < 0 ? q.substring(pos[0] + 1) : q.substring(pos[0] + 1, end);
                pos[0] = end < 0 ? n : end + 1;
                int slop = 0;
                if (pos[0] < n && q.charAt(pos[0]) == '~') {
                    int ds = ++pos[0];
                    while (pos[0] < n && Character.isDigit(q.charAt(pos[0]))) {
                        pos[0]++;
                    }
                    slop = pos[0] > ds ? Integer.parseInt(q.substring(ds, pos[0])) : 0;
                }
                int sl = slop;
                simpleJoin(st, perField(null, name -> new Query.Match(name, phrase, false, null, analyzer, null, 0, "none", true, "phrase", sl, true)));
            } else {
                int start = pos[0];
                while (pos[0] < n && !Character.isWhitespace(q.charAt(pos[0])) && "+|()\"".indexOf(q.charAt(pos[0])) < 0) {
                    pos[0]++;
                }
                String tok = q.substring(start, pos[0]);
                if (tok.isEmpty()) {
                    pos[0]++;
                    continue;
                }
                String fuzz = null;
                int t = tok.lastIndexOf('~');
                if (t > 0 && tok.substring(t + 1).matches("\\d*")) {
                    fuzz = tok.substring(t + 1).isEmpty() ? "2" : tok.substring(t + 1);
                    tok = tok.substring(0, t);
                }
                String text = tok;
                Query.Node branch;
                if (fuzz != null) {
                    String fz = fuzz;
                    branch = perFieldSum(name -> new Query.Fuzzy(name, text.toLowerCase(Locale.ROOT), fz, 0, true));
                } else if (tok.endsWith("*") && tok.length() > 1) {
                    String pre = tok.substring(0, tok.length() - 1).toLowerCase(Locale.ROOT);
                    branch = perFieldSum(name -> new Query.TermPattern(name, "prefix", pre, true, null));
                } else {
                    branch = perFieldSum(name -> new Query.Match(name, text, false, null, analyzer, null, 0, "none", true, "bool", 0, true));
                }
                simpleJoin(st, branch);
            }
        }
        return st.top;
    }

    private void simpleJoin(SimpleState st, Query.Node branch) {
        if (branch == null) {
            return;
        }
        if (st.not % 2 == 1) {
            Query.Bool nq = new Query.Bool();
            nq.mustNot.add(branch);
            nq.should.add(new Query.MatchAll());
            branch = nq;
        }
        if (st.top == null) {
            st.top = branch;
        } else {
            if (st.currentOp == 0) {
                st.currentOp = and ? 'M' : 'S';
            }
            if (st.previousOp != st.currentOp) {
                Query.Bool wrap = new Query.Bool();
                (st.currentOp == 'M' ? wrap.must : wrap.should).add(st.top);
                st.top = wrap;
            }
            Query.Bool b = (Query.Bool) st.top;
            (st.currentOp == 'M' ? b.must : b.should).add(branch);
            st.previousOp = st.currentOp;
        }
        st.not = 0;
        st.currentOp = 0;
    }

    /** A term's per-field queries combined by SUM (SHOULD clauses), like simple_query_string does. */
    private Query.Node perFieldSum(FieldQuery fq) {
        Query.Bool b = new Query.Bool();
        for (Map.Entry<String, Double> e : defaultFields) {
            FieldExpand fe = new FieldExpand(e.getKey(), fq, true);
            fe.boost = e.getValue();
            b.should.add(fe);
        }
        return b.should.size() == 1 ? b.should.get(0) : b;
    }
}
