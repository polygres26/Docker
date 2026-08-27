package com.polygres.wire.influxwire;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A hand-written parser for a real, bounded subset of InfluxQL -- not a full grammar. Recognizes:
 * <pre>
 *   SELECT &lt;select-list&gt; FROM &lt;measurement&gt;
 *     [WHERE &lt;cond&gt; [AND &lt;cond&gt;]*]
 *     [GROUP BY time(&lt;duration&gt;) [, &lt;tag&gt;]*]
 *     [LIMIT &lt;n&gt;]
 * </pre>
 * where {@code select-list} is {@code *} or a comma list of a bare field name or
 * {@code mean|sum|count|min|max(field)}, and {@code cond} is {@code tag_or_field OP value} or
 * {@code time OP (a quoted RFC3339 timestamp | now() [- duration])}, {@code OP} being
 * {@code = != &gt; &lt; &gt;= &lt;=}.
 *
 * <p>Deliberately does NOT support: {@code OR}, parenthesized conditions, subqueries,
 * {@code ORDER BY}, {@code fill()}, regex tag matching, or any function beyond the five aggregates
 * above. Any of those -- or genuinely malformed input -- throws {@link InfluxException} with the
 * original query text, matching {@code OpenSearchAdapter}'s "unrecognized clause fails loudly"
 * policy: a caller gets a clear 400 explaining what wasn't understood, never a silently wrong or
 * partial translation.
 */
final class InfluxQlParser {

    enum AggFunc { MEAN, SUM, COUNT, MIN, MAX }

    record SelectItem(AggFunc func, String field) {
        boolean isWildcard() {
            return func == null && field.equals("*");
        }
    }

    enum CmpOp { EQ, NEQ, GT, LT, GTE, LTE }

    /** {@code isTime} distinguishes a condition against the {@code time} column (compared as a
     * real timestamp) from one against a tag/field (compared via the {@code tags}/{@code fields}
     * jsonb columns) -- {@link PgTimeSeriesStore} needs to know which to build the right SQL. */
    record Condition(String column, boolean isTime, CmpOp op, String value) {
    }

    record GroupBy(String durationLiteral, List<String> tagColumns) {
    }

    record SelectStatement(List<SelectItem> selectList, String measurement, List<Condition> where,
            GroupBy groupBy, Integer limit) {
    }

    private static final Set<String> AGG_FUNCS = Set.of("mean", "sum", "count", "min", "max");

    private final List<String> tokens;
    private int pos;
    private final String original;

    private InfluxQlParser(List<String> tokens, String original) {
        this.tokens = tokens;
        this.original = original;
    }

    static SelectStatement parse(String query) {
        InfluxQlParser p = new InfluxQlParser(tokenize(query), query);
        return p.parseSelect();
    }

    private SelectStatement parseSelect() {
        expectKeyword("SELECT");
        List<SelectItem> selectList = parseSelectList();
        expectKeyword("FROM");
        String measurement = unquoteIdent(next());
        List<Condition> where = List.of();
        if (peekKeyword("WHERE")) {
            next();
            where = parseConditions();
        }
        GroupBy groupBy = null;
        if (peekKeyword("GROUP")) {
            next();
            expectKeyword("BY");
            groupBy = parseGroupBy();
        }
        Integer limit = null;
        if (peekKeyword("LIMIT")) {
            next();
            limit = Integer.parseInt(next());
        }
        if (pos < tokens.size()) {
            throw fail("unexpected trailing input near \"" + tokens.get(pos) + "\"");
        }
        return new SelectStatement(selectList, measurement, where, groupBy, limit);
    }

    private List<SelectItem> parseSelectList() {
        List<SelectItem> items = new ArrayList<>();
        if (peek().equals("*")) {
            next();
            items.add(new SelectItem(null, "*"));
            return items;
        }
        while (true) {
            String tok = next();
            String lower = tok.toLowerCase(Locale.ROOT);
            if (AGG_FUNCS.contains(lower) && peek().equals("(")) {
                next(); // (
                String field = unquoteIdent(next());
                expect(")");
                items.add(new SelectItem(AggFunc.valueOf(lower.toUpperCase(Locale.ROOT)), field));
            } else {
                items.add(new SelectItem(null, unquoteIdent(tok)));
            }
            if (peek().equals(",")) {
                next();
                continue;
            }
            break;
        }
        return items;
    }

    private List<Condition> parseConditions() {
        List<Condition> conditions = new ArrayList<>();
        conditions.add(parseCondition());
        while (peekKeyword("AND")) {
            next();
            conditions.add(parseCondition());
        }
        if (peekKeyword("OR")) {
            throw fail("influxwire V1 doesn't support OR in WHERE -- only AND-combined conditions");
        }
        return conditions;
    }

    private Condition parseCondition() {
        String column = unquoteIdent(next());
        CmpOp op = parseOp();
        boolean isTime = column.equalsIgnoreCase("time");
        if (isTime) {
            String value = parseTimeValue();
            return new Condition("time", true, op, value);
        }
        String value = parseScalarValue();
        return new Condition(column, false, op, value);
    }

    private CmpOp parseOp() {
        String tok = next();
        return switch (tok) {
            case "=" -> CmpOp.EQ;
            case "!=" -> CmpOp.NEQ;
            case ">" -> CmpOp.GT;
            case "<" -> CmpOp.LT;
            case ">=" -> CmpOp.GTE;
            case "<=" -> CmpOp.LTE;
            default -> throw fail("expected a comparison operator, got \"" + tok + "\"");
        };
    }

    /** Real InfluxQL time-comparison shapes this recognizes: a quoted RFC3339 timestamp, or
     * {@code now()} optionally followed by {@code - <duration>} (e.g. {@code now() - 1h}) --
     * resolved to an absolute ISO-8601 instant string here, at parse time, not left symbolic; see
     * {@link PgTimeSeriesStore} for where this string becomes a real bound parameter. */
    private String parseTimeValue() {
        String tok = next();
        if (tok.equalsIgnoreCase("now") ) {
            expect("(");
            expect(")");
            java.time.Instant now = java.time.Instant.now();
            if (peek().equals("-")) {
                next();
                String duration = next();
                now = now.minus(parseDurationMillis(duration), java.time.temporal.ChronoUnit.MILLIS);
            }
            return now.toString();
        }
        if (tok.startsWith("'") || tok.startsWith("\"")) {
            return unquoteIdent(tok);
        }
        throw fail("expected a quoted timestamp or now() near \"" + tok + "\"");
    }

    private String parseScalarValue() {
        String tok = next();
        return unquoteIdent(tok);
    }

    private GroupBy parseGroupBy() {
        String tok = next();
        if (!tok.equalsIgnoreCase("time")) {
            throw fail("influxwire V1 only supports GROUP BY time(...) [, tag] -- got \"" + tok + "\"");
        }
        expect("(");
        String duration = next();
        expect(")");
        List<String> tagColumns = new ArrayList<>();
        while (peek().equals(",")) {
            next();
            tagColumns.add(unquoteIdent(next()));
        }
        return new GroupBy(duration, tagColumns);
    }

    /** @return milliseconds -- real InfluxQL duration literal units: {@code ns us ms s m h d w}. */
    static long parseDurationMillis(String literal) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("^(\\d+)(ns|us|ms|s|m|h|d|w)$").matcher(literal);
        if (!m.matches()) {
            throw new InfluxException("influxwire: unrecognized duration literal \"" + literal + "\" "
                    + "-- expected a number followed by one of ns/us/ms/s/m/h/d/w");
        }
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ns" -> Math.max(1, n / 1_000_000);
            case "us" -> Math.max(1, n / 1_000);
            case "ms" -> n;
            case "s" -> n * 1_000;
            case "m" -> n * 60_000;
            case "h" -> n * 3_600_000;
            case "d" -> n * 86_400_000;
            case "w" -> n * 604_800_000;
            default -> throw new IllegalStateException();
        };
    }

    // ---- tokenizer ----

    private static List<String> tokenize(String q) {
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = q.length();
        while (i < n) {
            char c = q.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '\'' || c == '"') {
                int j = i + 1;
                StringBuilder sb = new StringBuilder();
                sb.append(c);
                while (j < n && q.charAt(j) != c) {
                    sb.append(q.charAt(j));
                    j++;
                }
                if (j >= n) {
                    throw new InfluxException("influxwire: unterminated quoted string in: " + q);
                }
                sb.append(c);
                out.add(sb.toString());
                i = j + 1;
                continue;
            }
            if (">=<!".indexOf(c) >= 0 && i + 1 < n && q.charAt(i + 1) == '=') {
                out.add(q.substring(i, i + 2));
                i += 2;
                continue;
            }
            if ("(),*=<>;".indexOf(c) >= 0) {
                out.add(String.valueOf(c));
                i++;
                continue;
            }
            if (c == '-') {
                out.add("-");
                i++;
                continue;
            }
            int j = i;
            while (j < n && !Character.isWhitespace(q.charAt(j)) && "(),*=<>!;'\"".indexOf(q.charAt(j)) < 0) {
                j++;
            }
            out.add(q.substring(i, j));
            i = j;
        }
        return out;
    }

    private static String unquoteIdent(String tok) {
        if (tok.length() >= 2 && (tok.charAt(0) == '\'' || tok.charAt(0) == '"')
                && tok.charAt(tok.length() - 1) == tok.charAt(0)) {
            return tok.substring(1, tok.length() - 1);
        }
        return tok;
    }

    private String peek() {
        return pos < tokens.size() ? tokens.get(pos) : "";
    }

    private boolean peekKeyword(String kw) {
        return pos < tokens.size() && tokens.get(pos).equalsIgnoreCase(kw);
    }

    private String next() {
        if (pos >= tokens.size()) {
            throw fail("unexpected end of query");
        }
        return tokens.get(pos++);
    }

    private void expect(String literal) {
        String tok = next();
        if (!tok.equals(literal)) {
            throw fail("expected \"" + literal + "\", got \"" + tok + "\"");
        }
    }

    private void expectKeyword(String kw) {
        String tok = next();
        if (!tok.equalsIgnoreCase(kw)) {
            throw fail("expected \"" + kw + "\", got \"" + tok + "\"");
        }
    }

    private InfluxException fail(String reason) {
        return new InfluxException("influxwire: couldn't parse InfluxQL (" + reason + "): " + original);
    }
}
