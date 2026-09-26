package com.sayonora.wire.pubsubwire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parser and evaluator of the Pub/Sub subscription filter grammar:
 * <pre>
 *   expr    := or
 *   or      := and ( "OR" and )*
 *   and     := not ( "AND" not )*
 *   not     := ( "NOT" | "-" ) not | primary
 *   primary := "(" expr ")" | "attributes" ":" name | "attributes" "." name ("=" | "!=") string
 *            | "hasPrefix" "(" "attributes" "." name "," string ")"
 * </pre>
 * Precedence is NOT, then AND, then OR. {@code attributes.k = "v"} and {@code !=} require the attribute to be present (a message
 * without it matches neither), {@code attributes:k} tests presence, {@code hasPrefix} tests the value prefix. A filter is limited
 * to 256 bytes like Pub/Sub's.
 */
final class PsFilter {

    interface Node {
        boolean test(Map<String, String> attrs);
    }

    private final Node root;
    final String source;

    private PsFilter(String source, Node root) {
        this.source = source;
        this.root = root;
    }

    boolean matches(Map<String, String> attributes) {
        return root == null || root.test(attributes);
    }

    static PsFilter parse(String text) {
        if (text == null || text.isBlank()) {
            return new PsFilter("", null);
        }
        if (text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 256) {
            throw new IllegalArgumentException("filter is longer than 256 bytes");
        }
        Parser p = new Parser(lex(text));
        Node n = p.expr();
        if (!p.atEnd()) {
            throw new IllegalArgumentException("unexpected token '" + p.peek().text + "'");
        }
        return new PsFilter(text, n);
    }

    // ---- lexer ----
    private record Tok(char kind, String text) {
    } // kind: w=word, s=string, p=punct, e=end

    private static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < s.length()) {
                    char d = s.charAt(i++);
                    if (d == '\\' && i < s.length()) {
                        b.append(s.charAt(i++));
                    } else if (d == '"') {
                        closed = true;
                        break;
                    } else {
                        b.append(d);
                    }
                }
                if (!closed) {
                    throw new IllegalArgumentException("unterminated string");
                }
                out.add(new Tok('s', b.toString()));
            } else if (c == '!' && i + 1 < s.length() && s.charAt(i + 1) == '=') {
                out.add(new Tok('p', "!="));
                i += 2;
            } else if ("()=:.,-".indexOf(c) >= 0) {
                out.add(new Tok('p', String.valueOf(c)));
                i++;
            } else if (Character.isLetterOrDigit(c) || c == '_') {
                int j = i;
                while (j < s.length() && (Character.isLetterOrDigit(s.charAt(j)) || s.charAt(j) == '_'
                        || (s.charAt(j) == '-' && j + 1 < s.length() && Character.isLetterOrDigit(s.charAt(j + 1))))) {
                    j++;
                }
                out.add(new Tok('w', s.substring(i, j)));
                i = j;
            } else {
                throw new IllegalArgumentException("unexpected character '" + c + "'");
            }
        }
        out.add(new Tok('e', ""));
        return out;
    }

    private static final class Parser {
        private final List<Tok> toks;
        private int pos;

        Parser(List<Tok> toks) {
            this.toks = toks;
        }

        Tok peek() {
            return toks.get(pos);
        }

        boolean atEnd() {
            return peek().kind == 'e';
        }

        private boolean isWord(String w) {
            return peek().kind == 'w' && peek().text.equals(w);
        }

        private boolean isPunct(String p) {
            return peek().kind == 'p' && peek().text.equals(p);
        }

        private void expectPunct(String p) {
            if (!isPunct(p)) {
                throw new IllegalArgumentException("expected '" + p + "' but found '" + peek().text + "'");
            }
            pos++;
        }

        Node expr() {
            Node l = and();
            while (isWord("OR")) {
                pos++;
                Node a = l;
                Node r = and();
                l = m -> a.test(m) || r.test(m);
            }
            return l;
        }

        Node and() {
            Node l = not();
            while (isWord("AND")) {
                pos++;
                Node a = l;
                Node r = not();
                l = m -> a.test(m) && r.test(m);
            }
            return l;
        }

        Node not() {
            if (isWord("NOT") || isPunct("-")) {
                pos++;
                Node n = not();
                return m -> !n.test(m);
            }
            return primary();
        }

        private String attrName() {
            Tok t = peek();
            if (t.kind == 's') {
                pos++;
                return t.text;
            }
            if (t.kind != 'w') {
                throw new IllegalArgumentException("expected an attribute name but found '" + t.text + "'");
            }
            StringBuilder b = new StringBuilder(t.text);
            pos++;
            // bare names may contain dots: attributes:iana.org
            while (isPunct(".") && toks.get(pos + 1).kind == 'w') {
                b.append('.').append(toks.get(pos + 1).text);
                pos += 2;
            }
            return b.toString();
        }

        private String string() {
            Tok t = peek();
            if (t.kind != 's') {
                throw new IllegalArgumentException("expected a quoted string but found '" + t.text + "'");
            }
            pos++;
            return t.text;
        }

        Node primary() {
            if (isPunct("(")) {
                pos++;
                Node n = expr();
                expectPunct(")");
                return n;
            }
            if (isWord("hasPrefix")) {
                pos++;
                expectPunct("(");
                if (!isWord("attributes")) {
                    throw new IllegalArgumentException("hasPrefix requires an attributes.name argument");
                }
                pos++;
                expectPunct(".");
                String name = attrName();
                expectPunct(",");
                String prefix = string();
                expectPunct(")");
                return m -> {
                    String v = m.get(name);
                    return v != null && v.startsWith(prefix);
                };
            }
            if (isWord("attributes")) {
                pos++;
                if (isPunct(":")) {
                    pos++;
                    String name = attrName();
                    return m -> m.containsKey(name);
                }
                expectPunct(".");
                String name = attrName();
                if (isPunct("=")) {
                    pos++;
                    String v = string();
                    return m -> v.equals(m.get(name));
                }
                if (isPunct("!=")) {
                    pos++;
                    String v = string();
                    return m -> m.containsKey(name) && !v.equals(m.get(name));
                }
                throw new IllegalArgumentException("expected '=' or '!=' after attributes." + name);
            }
            throw new IllegalArgumentException("unexpected token '" + peek().text + "'");
        }
    }
}
