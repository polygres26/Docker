package com.polygres.wire.boltwire;

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

    record RelPattern(String type, Map<String, Object> properties) {
    }

    record ReturnItem(String variable, String property, String alias) {
    }

    record CreateStatement(NodePattern first, RelPattern rel, NodePattern second, List<ReturnItem> returnItems) {
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
        String type = null;
        if (peek().equals(":")) {
            next();
            type = next();
        }
        Map<String, Object> properties = peek().equals("{") ? parsePropertyMap() : Map.of();
        expect("]");
        expect("-");
        expect(">");
        return new RelPattern(type, properties);
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
            if ("(){}[]:,.-".indexOf(c) >= 0) {
                out.add(String.valueOf(c));
                i++;
                continue;
            }
            if (c == '>') {
                out.add(">");
                i++;
                continue;
            }
            int j = i;
            while (j < n && !Character.isWhitespace(q.charAt(j)) && "(){}[]:,.-'\"><".indexOf(q.charAt(j)) < 0) {
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
