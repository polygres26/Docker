package com.sayonora.wire.boltwire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A hand-written parser for a real, bounded subset of Cypher's {@code CREATE} statement --
 * boltwire's Phase 2 write path, the same "bounded but genuine, unrecognized shapes fail loudly"
 * approach {@code InfluxQlParser} already established for InfluxQL in this codebase. Recognizes:
 * <pre>
 *   CREATE (var1:Label1:Label2 {prop: value, ...})
 *     [ -[ [:REL_TYPE] {prop: value, ...} ]-&gt; (var2:Label3 {prop: value, ...}) ]
 *   [RETURN item [, item ...]]
 * </pre>
 * where an {@code item} is {@code var} (the whole created node, encoded as a real Bolt Node
 * struct -- see {@link GraphNode}) or {@code var.property} (a scalar), each optionally followed by
 * {@code AS alias}.
 *
 * <p>Deliberately does NOT support (Phase 2 scope; a later phase's concern): more than one
 * relationship per statement (chained patterns like {@code (a)-[:X]->(b)-[:Y]->(c)}),
 * bidirectional/undirected relationships, returning the created relationship itself as a Bolt
 * Relationship struct (the edge still gets created for real in Postgres -- see
 * {@code PgGraphStore#createEdge} -- just not returned as a wire object yet), {@code MERGE},
 * {@code SET}, or expressions in property values beyond literals. Any of those, or genuinely
 * malformed input, throws {@link InfluxException}-style failure via {@link CypherException} with
 * the original query text -- see this class's one caller ({@code BoltWireSessionHandler}) for how
 * that becomes a real Bolt FAILURE message.
 */
final class CypherParser {

    record NodePattern(String variable, List<String> labels, Map<String, Object> properties) {
    }

    /** @param minHops/@param maxHops null for both = a plain, fixed single-hop relationship
     * (Phase 2/3's original scope). Both set = a real variable-length path ({@code [*1..3]} ->
     * minHops=1, maxHops=3; {@code [*2]} -> minHops=maxHops=2) -- Phase 4's own addition, see
     * {@code BoltWireSessionHandler#runMatch}'s WITH RECURSIVE translation. A bare, unbounded
     * {@code [*]} is deliberately rejected at parse time (see {@link #parseRel}) rather than
     * accepted and left to a real WITH RECURSIVE query with no depth bound to run away against a
     * large graph -- not implemented, not silently capped to some arbitrary default either. */
    record RelPattern(String variable, String type, Map<String, Object> properties, Integer minHops, Integer maxHops) {
    }

    record ReturnItem(String variable, String property, String alias) {
    }

    record CreateStatement(NodePattern first, RelPattern rel, NodePattern second, List<ReturnItem> returnItems) {
    }

    enum CmpOp { EQ, NEQ, GT, LT, GTE, LTE }

    /** A WHERE condition against {@code variable.property} -- always a matched node/relationship's
     * own property, never a bare literal comparison (Cypher's WHERE can express more, e.g.
     * label-existence predicates; Phase 3 scope is deliberately just this common case). */
    record Condition(String variable, String property, CmpOp op, Object value) {
    }

    /**
     * Phase 3's read path: {@code MATCH (var1:Label1) [-[[:TYPE]]-> (var2:Label2)] [WHERE cond
     * [AND cond...]] RETURN item [, item...] [LIMIT n]}. Only a single fixed-length relationship
     * hop (same bound as CREATE's Phase 2 pattern) -- variable-length paths ({@code [*1..3]}) are a
     * later phase, see {@code BoltWireSessionHandler}'s own javadoc.
     */
    record MatchStatement(NodePattern first, RelPattern rel, NodePattern second, List<Condition> where,
            List<ReturnItem> returnItems, Integer limit) {
    }

    private final List<String> tokens;
    private int pos;
    private final String original;

    private CypherParser(List<String> tokens, String original) {
        this.tokens = tokens;
        this.original = original;
    }

    static CreateStatement parseCreate(String query) {
        CypherParser p = new CypherParser(tokenize(query), query);
        return p.parseCreateStatement();
    }

    static MatchStatement parseMatch(String query) {
        CypherParser p = new CypherParser(tokenize(query), query);
        return p.parseMatchStatement();
    }

    private MatchStatement parseMatchStatement() {
        expectKeyword("MATCH");
        NodePattern first = parseNode();
        RelPattern rel = null;
        NodePattern second = null;
        if (peek().equals("-")) {
            rel = parseRel();
            second = parseNode();
        }
        List<Condition> where = List.of();
        if (peekKeyword("WHERE")) {
            next();
            where = parseConditions();
        }
        expectKeyword("RETURN");
        List<ReturnItem> returnItems = parseReturnItems();
        Integer limit = null;
        if (peekKeyword("LIMIT")) {
            next();
            limit = Integer.parseInt(next());
        }
        if (pos < tokens.size()) {
            throw fail("unexpected trailing input near \"" + tokens.get(pos) + "\"");
        }
        return new MatchStatement(first, rel, second, where, returnItems, limit);
    }

    private List<Condition> parseConditions() {
        List<Condition> conditions = new ArrayList<>();
        conditions.add(parseCondition());
        while (peekKeyword("AND")) {
            next();
            conditions.add(parseCondition());
        }
        if (peekKeyword("OR")) {
            throw fail("boltwire Phase 3 doesn't support OR in WHERE -- only AND-combined conditions");
        }
        return conditions;
    }

    private Condition parseCondition() {
        String variable = next();
        expect(".");
        String property = next();
        String opTok = next();
        CmpOp op = switch (opTok) {
            case "=" -> CmpOp.EQ;
            case "!=", "<>" -> CmpOp.NEQ;
            case ">" -> CmpOp.GT;
            case "<" -> CmpOp.LT;
            case ">=" -> CmpOp.GTE;
            case "<=" -> CmpOp.LTE;
            default -> throw fail("expected a comparison operator, got \"" + opTok + "\"");
        };
        Object value = parseValue();
        return new Condition(variable, property, op, value);
    }

    private CreateStatement parseCreateStatement() {
        expectKeyword("CREATE");
        NodePattern first = parseNode();
        RelPattern rel = null;
        NodePattern second = null;
        if (peek().equals("-")) {
            rel = parseRel();
            second = parseNode();
        }
        List<ReturnItem> returnItems = List.of();
        if (peekKeyword("RETURN")) {
            next();
            returnItems = parseReturnItems();
        }
        if (pos < tokens.size()) {
            throw fail("unexpected trailing input near \"" + tokens.get(pos) + "\"");
        }
        return new CreateStatement(first, rel, second, returnItems);
    }

    private NodePattern parseNode() {
        expect("(");
        String variable = null;
        if (!peek().equals(":") && !peek().equals(")") && !peek().equals("{")) {
            variable = next();
        }
        List<String> labels = new ArrayList<>();
        while (peek().equals(":")) {
            next();
            labels.add(next());
        }
        Map<String, Object> properties = peek().equals("{") ? parsePropertyMap() : Map.of();
        expect(")");
        return new NodePattern(variable, labels, properties);
    }

    private RelPattern parseRel() {
        expect("-");
        expect("[");
        String variable = null;
        if (!peek().equals(":") && !peek().equals("]") && !peek().equals("{") && !peek().startsWith("*")) {
            variable = next();
        }
        String type = null;
        if (peek().equals(":")) {
            next();
            type = next();
        }
        Integer minHops = null;
        Integer maxHops = null;
        if (peek().startsWith("*")) {
            int[] hops = parseHopSpec();
            minHops = hops[0];
            maxHops = hops[1];
        }
        Map<String, Object> properties = peek().equals("{") ? parsePropertyMap() : Map.of();
        expect("]");
        expect("-");
        expect(">");
        return new RelPattern(variable, type, properties, minHops, maxHops);
    }

    /** Parses a variable-length-path hop bound: {@code *N} (exact), {@code *N..M} (range), or a
     * bare unbounded {@code *} -- the last of which is rejected here (see {@link RelPattern}'s own
     * javadoc for why), not accepted and left to run away later. {@code '*'} is its own punctuation
     * token (see {@code tokenize}), so this always consumes it separately from the digits that
     * follow. */
    private int[] parseHopSpec() {
        expect("*");
        String rest;
        if (pos < tokens.size() && tokens.get(pos).matches("\\d+")) {
            rest = next();
        } else {
            throw fail("boltwire Phase 4 doesn't support an unbounded variable-length path ([*]) -- "
                    + "an explicit bound like [*1..3] is required");
        }
        int min = Integer.parseInt(rest);
        int max = min;
        if (peek().equals(".")) {
            next();
            expect(".");
            String maxTok = next();
            if (!maxTok.matches("\\d+")) {
                throw fail("expected a number after \"..\" in a variable-length relationship, got \"" + maxTok + "\"");
            }
            max = Integer.parseInt(maxTok);
        }
        return new int[] {min, max};
    }

    private Map<String, Object> parsePropertyMap() {
        expect("{");
        Map<String, Object> map = new LinkedHashMap<>();
        if (!peek().equals("}")) {
            while (true) {
                String key = next();
                expect(":");
                Object value = parseValue();
                map.put(key, value);
                if (peek().equals(",")) {
                    next();
                    continue;
                }
                break;
            }
        }
        expect("}");
        return map;
    }

    private Object parseValue() {
        String tok = next();
        if (tok.length() >= 2 && (tok.charAt(0) == '\'' || tok.charAt(0) == '"')) {
            return tok.substring(1, tok.length() - 1);
        }
        if (tok.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (tok.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (tok.equalsIgnoreCase("null")) {
            return null;
        }
        if (tok.matches("-?\\d+")) {
            return Long.parseLong(tok);
        }
        if (tok.matches("-?\\d+\\.\\d+")) {
            return Double.parseDouble(tok);
        }
        throw fail("expected a property value (string/number/bool/null), got \"" + tok + "\"");
    }

    private List<ReturnItem> parseReturnItems() {
        List<ReturnItem> items = new ArrayList<>();
        while (true) {
            String variable = next();
            String property = null;
            if (peek().equals(".")) {
                next();
                property = next();
            }
            String alias = null;
            if (peekKeyword("AS")) {
                next();
                alias = next();
            }
            items.add(new ReturnItem(variable, property, alias));
            if (peek().equals(",")) {
                next();
                continue;
            }
            break;
        }
        return items;
    }

    // ---- tokenizer: reused approach from InfluxQlParser, extended with graph-pattern punctuation
    // ('(' ')' '[' ']' '{' '}' ':' '-' '>' '.') this grammar needs that InfluxQL never did. ----

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
                while (j < n && q.charAt(j) != c) {
                    j++;
                }
                if (j >= n) {
                    throw new CypherException("boltwire: unterminated quoted string in: " + q);
                }
                out.add(q.substring(i, j + 1));
                i = j + 1;
                continue;
            }
            // '*' is its own single-char punctuation token (added for Phase 4's [*1..3] variable-
            // length-path syntax) -- real bug, found live: without this, the generic identifier
            // scan below (which only stops at the chars excluded there) happily swallowed '*' as
            // part of the relationship type token itself (":KNOWS*1..3" tokenized as one type
            // token "KNOWS*1" before finally stopping at the first "."), leaving parseRel() with
            // no separate token to recognize as the hop-count marker at all.
            if ("(){}[]:,.-*".indexOf(c) >= 0) {
                out.add(String.valueOf(c));
                i++;
                continue;
            }
            // Real bug, found live once WHERE conditions (Phase 3) needed comparison operators
            // CREATE's own grammar (Phase 2) never used: '=', '!=', '<', '<=', '>=', '<>' weren't
            // tokenized as operators at all -- '<' in particular wasn't in the punctuation set OR
            // reachable via the generic identifier scan below (which explicitly excludes it as a
            // stop character), so the scan's own start/end pointers never advanced past it,
            // producing an empty token forever -- an infinite loop on the very first WHERE clause
            // tested live. Handling '=', '!', '<', '>' explicitly here, checking for a following
            // '=' to form the two-character operators, fixes it -- the same "scan for a following
            // '=' " approach InfluxQlParser's own tokenizer already uses for its own operators.
            if ("=!<>".indexOf(c) >= 0) {
                if (i + 1 < n && q.charAt(i + 1) == '=') {
                    out.add(q.substring(i, i + 2));
                    i += 2;
                } else if (c == '<' && i + 1 < n && q.charAt(i + 1) == '>') {
                    out.add("<>");
                    i += 2;
                } else {
                    out.add(String.valueOf(c));
                    i++;
                }
                continue;
            }
            int j = i;
            while (j < n && !Character.isWhitespace(q.charAt(j)) && "(){}[]:,.-'\"><=!*".indexOf(q.charAt(j)) < 0) {
                j++;
            }
            out.add(q.substring(i, j));
            i = j;
        }
        return out;
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

    private CypherException fail(String reason) {
        return new CypherException("boltwire: couldn't parse Cypher (" + reason + "): " + original);
    }
}
