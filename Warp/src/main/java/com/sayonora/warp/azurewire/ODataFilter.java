package com.sayonora.warp.azurewire;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * OData $filter of the Table service: {@code eq ne gt ge lt le and or not}, parentheses and the literal forms
 * (strings, Int32/Int64 {@code L}, doubles, booleans, {@code datetime'..'}, {@code guid'..'}, {@code X'..'/binary'..'}).
 * Comparisons need matching types (numbers compare across Int32/Int64/Double); a missing property makes a comparison false.
 */
final class ODataFilter {

    /** A typed value: type is one of Edm.String, Edm.Int32, Edm.Int64, Edm.Double, Edm.Boolean, Edm.DateTime, Edm.Guid, Edm.Binary. */
    record Val(String type, Object v) {
    }

    interface Node {
        boolean eval(java.util.function.Function<String, Val> props);
    }

    private record Cmp(String prop, String op, Val lit) implements Node {
        @Override
        public boolean eval(java.util.function.Function<String, Val> props) {
            Val a = props.apply(prop);
            if (a == null) {
                return false;
            }
            Integer c = compare(a, lit);
            if (c == null) {
                return false;
            }
            return switch (op) {
                case "eq" -> c == 0;
                case "ne" -> c != 0;
                case "gt" -> c > 0;
                case "ge" -> c >= 0;
                case "lt" -> c < 0;
                default -> c <= 0;
            };
        }
    }

    private record Bool(String prop) implements Node {
        @Override
        public boolean eval(java.util.function.Function<String, Val> props) {
            Val a = props.apply(prop);
            return a != null && a.v() instanceof Boolean b && b;
        }
    }

    private record And(Node l, Node r) implements Node {
        @Override
        public boolean eval(java.util.function.Function<String, Val> p) {
            return l.eval(p) && r.eval(p);
        }
    }

    private record Or(Node l, Node r) implements Node {
        @Override
        public boolean eval(java.util.function.Function<String, Val> p) {
            return l.eval(p) || r.eval(p);
        }
    }

    private record Not(Node n) implements Node {
        @Override
        public boolean eval(java.util.function.Function<String, Val> p) {
            return !n.eval(p);
        }
    }

    private static boolean numeric(String t) {
        return t.equals("Edm.Int32") || t.equals("Edm.Int64") || t.equals("Edm.Double");
    }

    static Integer compare(Val a, Val b) {
        if (numeric(a.type()) && numeric(b.type())) {
            if (a.v() instanceof Double x && (x.isNaN() || x.isInfinite()) || b.v() instanceof Double y && (y.isNaN() || y.isInfinite())) {
                double x = ((Number) a.v()).doubleValue();
                double y = ((Number) b.v()).doubleValue();
                return Double.compare(x, y);
            }
            return new BigDecimal(a.v().toString()).compareTo(new BigDecimal(b.v().toString()));
        }
        if (!a.type().equals(b.type())) {
            return null;
        }
        if (a.v() instanceof byte[] x) {
            return java.util.Arrays.compareUnsigned(x, (byte[]) b.v());
        }
        if (a.v() instanceof Boolean x) {
            return Boolean.compare(x, (Boolean) b.v());
        }
        return a.v().toString().compareTo(b.v().toString());
    }

    // ---- constraints extracted for SQL pushdown

    /** Top-level AND conjuncts on PartitionKey / RowKey: (op, value). */
    record Bound(String prop, String op, String value) {
    }

    private final Node root;
    private final List<Bound> bounds = new ArrayList<>();

    private ODataFilter(Node root) {
        this.root = root;
    }

    boolean matches(java.util.function.Function<String, Val> props) {
        return root == null || root.eval(props);
    }

    List<Bound> bounds() {
        return bounds;
    }

    static ODataFilter parse(String text) {
        if (text == null || text.isBlank()) {
            return new ODataFilter(null);
        }
        P p = new P(text);
        Node n = p.or();
        p.ws();
        if (p.i < text.length()) {
            throw p.err("unexpected input");
        }
        ODataFilter f = new ODataFilter(n);
        collect(n, f.bounds);
        return f;
    }

    private static void collect(Node n, List<Bound> out) {
        if (n instanceof And a) {
            collect(a.l(), out);
            collect(a.r(), out);
        } else if (n instanceof Cmp c && (c.prop().equals("PartitionKey") || c.prop().equals("RowKey"))
                && c.lit().v() instanceof String s && c.lit().type().equals("Edm.String") && !c.op().equals("ne")) {
            out.add(new Bound(c.prop(), c.op(), s));
        }
    }

    static final class ParseError extends RuntimeException {
        ParseError(String m) {
            super(m);
        }
    }

    private static final class P {
        final String s;
        int i;

        P(String s) {
            this.s = s;
        }

        ParseError err(String m) {
            return new ParseError(m + " at position " + i);
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        boolean kw(String k) {
            ws();
            int n = k.length();
            if (s.regionMatches(true, i, k, 0, n) && (i + n >= s.length() || !Character.isLetterOrDigit(s.charAt(i + n))
                    && s.charAt(i + n) != '_')) {
                i += n;
                return true;
            }
            return false;
        }

        Node or() {
            Node l = and();
            while (kw("or")) {
                l = new Or(l, and());
            }
            return l;
        }

        Node and() {
            Node l = not();
            while (kw("and")) {
                l = new And(l, not());
            }
            return l;
        }

        Node not() {
            if (kw("not")) {
                return new Not(not());
            }
            return primary();
        }

        Node primary() {
            ws();
            if (i < s.length() && s.charAt(i) == '(') {
                i++;
                Node n = or();
                ws();
                if (i >= s.length() || s.charAt(i) != ')') {
                    throw err("expected )");
                }
                i++;
                return n;
            }
            int st = i;
            while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                i++;
            }
            if (st == i) {
                throw err("expected a property name");
            }
            String name = s.substring(st, i);
            ws();
            String op = null;
            for (String o : new String[] {"eq", "ne", "gt", "ge", "lt", "le"}) {
                if (kw(o)) {
                    op = o;
                    break;
                }
            }
            if (op == null) {
                return new Bool(name);
            }
            return new Cmp(name, op, literal());
        }

        Val literal() {
            ws();
            if (i >= s.length()) {
                throw err("expected a literal");
            }
            char c = s.charAt(i);
            if (c == '\'') {
                return new Val("Edm.String", quoted());
            }
            if (kw("true")) {
                return new Val("Edm.Boolean", Boolean.TRUE);
            }
            if (kw("false")) {
                return new Val("Edm.Boolean", Boolean.FALSE);
            }
            String low = s.substring(i).toLowerCase(Locale.ROOT);
            if (low.startsWith("datetime'")) {
                i += 8;
                return new Val("Edm.DateTime", normalizeDate(quoted()));
            }
            if (low.startsWith("guid'")) {
                i += 4;
                return new Val("Edm.Guid", quoted().toLowerCase(Locale.ROOT));
            }
            if (low.startsWith("x'") || low.startsWith("binary'")) {
                i += low.startsWith("x'") ? 1 : 6;
                String h = quoted();
                return new Val("Edm.Binary", low.startsWith("x'") ? hex(h) : Base64.getDecoder().decode(h));
            }
            int st = i;
            if (c == '-' || c == '+') {
                i++;
            }
            while (i < s.length() && (Character.isDigit(s.charAt(i)) || ".eE".indexOf(s.charAt(i)) >= 0
                    || (s.charAt(i) == '-' || s.charAt(i) == '+') && (s.charAt(i - 1) == 'e' || s.charAt(i - 1) == 'E'))) {
                i++;
            }
            String num = s.substring(st, i);
            if (num.isEmpty() || num.equals("-") || num.equals("+")) {
                if (s.startsWith("INF", i) || s.startsWith("NaN", i)) {
                    boolean nan = s.startsWith("NaN", i);
                    i += 3;
                    return new Val("Edm.Double", nan ? Double.NaN : c == '-' ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
                }
                throw err("invalid literal");
            }
            char suf = i < s.length() ? Character.toLowerCase(s.charAt(i)) : ' ';
            try {
                if (suf == 'l') {
                    i++;
                    return new Val("Edm.Int64", Long.parseLong(num));
                }
                if (suf == 'd') {
                    i++;
                    return new Val("Edm.Double", Double.parseDouble(num));
                }
                if (num.contains(".") || num.contains("e") || num.contains("E")) {
                    return new Val("Edm.Double", Double.parseDouble(num));
                }
                long v = Long.parseLong(num);
                return v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE ? new Val("Edm.Int32", (int) v) : new Val("Edm.Int64", v);
            } catch (NumberFormatException e) {
                throw err("invalid number " + num);
            }
        }

        String quoted() {
            if (i >= s.length() || s.charAt(i) != '\'') {
                throw err("expected '");
            }
            i++;
            StringBuilder b = new StringBuilder();
            while (true) {
                if (i >= s.length()) {
                    throw err("unterminated string");
                }
                char c = s.charAt(i++);
                if (c == '\'') {
                    if (i < s.length() && s.charAt(i) == '\'') {
                        b.append('\'');
                        i++;
                        continue;
                    }
                    return b.toString();
                }
                b.append(c);
            }
        }
    }

    static byte[] hex(String h) {
        if (h.length() % 2 != 0) {
            throw new ParseError("invalid binary literal");
        }
        byte[] out = new byte[h.length() / 2];
        for (int k = 0; k < out.length; k++) {
            out[k] = (byte) Integer.parseInt(h.substring(2 * k, 2 * k + 2), 16);
        }
        return out;
    }

    /** Normalises an ISO date-time to seven fractional digits so string comparison is chronological. */
    static String normalizeDate(String v) {
        try {
            java.time.Instant t = java.time.OffsetDateTime.parse(v).toInstant();
            return tableTime(t);
        } catch (RuntimeException e) {
            throw new ParseError("invalid datetime literal " + v);
        }
    }

    static String tableTime(java.time.Instant t) {
        return java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
                .withZone(java.time.ZoneOffset.UTC).format(t) + "." + String.format("%07d", t.getNano() / 100) + "Z";
    }
}
