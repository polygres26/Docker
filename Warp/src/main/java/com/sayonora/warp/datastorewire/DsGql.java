package com.sayonora.warp.datastorewire;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.GqlQueryParameter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Projection;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A basic GQL parser: {@code SELECT [DISTINCT [ON (p, ..)]] * | __key__ | p, .. FROM kind [WHERE cond AND ..] [ORDER BY p [ASC|DESC], ..]
 * [LIMIT n] [OFFSET n]} with conditions {@code p (= | < | <= | > | >= | != | IN | CONTAINS) value} and {@code __key__ HAS ANCESTOR key},
 * literals (integers, doubles, 'strings', true/false, NULL, DATETIME('..'), KEY(kind, id|'name', ..), BLOB('..')) and bindings
 * ({@code @name}, {@code @1}, {@code :1}). Literals are refused unless {@code allow_literals} is set, like Cloud Datastore.
 * Not supported: OR, aggregation clauses, nested projections, {@code SELECT ... AS}.
 */
final class DsGql {

    final Query query;

    private DsGql(Query q) {
        this.query = q;
    }

    static DsGql parse(GqlQuery g, DsKeys.Part part) {
        return new DsGql(new Parser(g, part).parse());
    }

    private static final class Tok {
        final String text;
        final int type; // 0 word, 1 number, 2 string, 3 symbol, 4 binding
        final int pos;
        String raw;

        Tok(String text, int type, int pos) {
            this.text = text;
            this.type = type;
            this.pos = pos;
            this.raw = text;
        }
    }

    private static final class Parser {
        private final GqlQuery g;
        private final DsKeys.Part part;
        private final List<Tok> toks = new ArrayList<>();
        private int i;
        private int positional;

        Parser(GqlQuery g, DsKeys.Part part) {
            this.g = g;
            this.part = part;
            lex(g.getQueryString());
        }

        private void lex(String s) {
            int p = 0;
            while (p < s.length()) {
                char c = s.charAt(p);
                if (Character.isWhitespace(c)) {
                    p++;
                } else if (c == '\'' || c == '"') {
                    StringBuilder sb = new StringBuilder();
                    int st = p++;
                    boolean closed = false;
                    while (p < s.length()) {
                        char d = s.charAt(p);
                        if (d == '\\' && p + 1 < s.length()) {
                            sb.append(s.charAt(p + 1));
                            p += 2;
                        } else if (d == c) {
                            if (p + 1 < s.length() && s.charAt(p + 1) == c) {
                                sb.append(c);
                                p += 2;
                            } else {
                                closed = true;
                                p++;
                                break;
                            }
                        } else {
                            sb.append(d);
                            p++;
                        }
                    }
                    if (!closed) {
                        throw err("Unterminated string literal", st);
                    }
                    Tok st2 = new Tok(sb.toString(), 2, st);
                    st2.raw = s.substring(st, p);
                    toks.add(st2);
                } else if (Character.isDigit(c) || (c == '-' && p + 1 < s.length() && Character.isDigit(s.charAt(p + 1)))) {
                    int st = p++;
                    while (p < s.length() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.' || s.charAt(p) == 'e' || s.charAt(p) == 'E'
                            || ((s.charAt(p) == '-' || s.charAt(p) == '+') && (s.charAt(p - 1) == 'e' || s.charAt(p - 1) == 'E')))) {
                        p++;
                    }
                    toks.add(new Tok(s.substring(st, p), 1, st));
                } else if (c == '@' || c == ':') {
                    int st = p++;
                    while (p < s.length() && (Character.isLetterOrDigit(s.charAt(p)) || s.charAt(p) == '_')) {
                        p++;
                    }
                    toks.add(new Tok(s.substring(st + 1, p), 4, st));
                } else if (Character.isLetter(c) || c == '_' || c == '`') {
                    int st = p;
                    if (c == '`') {
                        p++;
                        while (p < s.length() && s.charAt(p) != '`') {
                            p++;
                        }
                        toks.add(new Tok(s.substring(st + 1, p), 0, st));
                        p++;
                        continue;
                    }
                    while (p < s.length() && (Character.isLetterOrDigit(s.charAt(p)) || s.charAt(p) == '_' || s.charAt(p) == '.')) {
                        p++;
                    }
                    toks.add(new Tok(s.substring(st, p), 0, st));
                } else if ((c == '<' || c == '>' || c == '!') && p + 1 < s.length() && s.charAt(p + 1) == '=') {
                    toks.add(new Tok(s.substring(p, p + 2), 3, p));
                    p += 2;
                } else if ("=<>(),*".indexOf(c) >= 0) {
                    toks.add(new Tok(String.valueOf(c), 3, p));
                    p++;
                } else {
                    throw err("Unexpected character '" + c + "'", p);
                }
            }
        }

        private DsException err(String m, int pos) {
            return DsException.invalid("GQL syntax error: " + m + " at position " + pos + ".");
        }

        private Tok peek() {
            return i < toks.size() ? toks.get(i) : null;
        }

        private boolean isWord(String w) {
            Tok t = peek();
            return t != null && t.type == 0 && t.text.equalsIgnoreCase(w);
        }

        private boolean isSym(String s) {
            Tok t = peek();
            return t != null && t.type == 3 && t.text.equals(s);
        }

        private Tok next() {
            if (i >= toks.size()) {
                throw DsException.invalid("GQL syntax error: unexpected end of query.");
            }
            return toks.get(i++);
        }

        private void word(String w) {
            Tok t = next();
            if (t.type != 0 || !t.text.equalsIgnoreCase(w)) {
                throw err("expected " + w + " but found \"" + t.text + "\"", t.pos);
            }
        }

        private void sym(String s) {
            Tok t = next();
            if (t.type != 3 || !t.text.equals(s)) {
                throw err("expected " + s + " but found \"" + t.text + "\"", t.pos);
            }
        }

        Query parse() {
            Query.Builder q = Query.newBuilder();
            word("SELECT");
            List<String> distinct = new ArrayList<>();
            boolean distinctAll = false;
            if (isWord("DISTINCT")) {
                next();
                distinctAll = true;
                if (isWord("ON")) {
                    next();
                    sym("(");
                    do {
                        if (isSym(",")) {
                            next();
                        }
                        distinct.add(next().text);
                    } while (isSym(","));
                    sym(")");
                    distinctAll = false;
                }
            }
            List<String> proj = new ArrayList<>();
            if (isSym("*")) {
                next();
            } else {
                do {
                    if (isSym(",")) {
                        next();
                    }
                    proj.add(next().text);
                } while (isSym(","));
            }
            if (distinctAll) {
                distinct.addAll(proj);
            }
            word("FROM");
            Tok kind = next();
            q.addKind(KindExpression.newBuilder().setName(kind.text));
            for (String p : proj) {
                q.addProjection(Projection.newBuilder().setProperty(PropertyReference.newBuilder().setName(p)));
            }
            for (String d : distinct) {
                q.addDistinctOn(PropertyReference.newBuilder().setName(d));
            }
            if (isWord("WHERE")) {
                next();
                List<Filter> fs = new ArrayList<>();
                do {
                    if (isWord("AND")) {
                        next();
                    }
                    fs.add(condition());
                } while (isWord("AND"));
                if (isWord("OR")) {
                    throw DsException.invalid("GQL syntax error: OR is not supported by this GQL parser.");
                }
                q.setFilter(fs.size() == 1 ? fs.get(0) : Filter.newBuilder().setCompositeFilter(CompositeFilter.newBuilder()
                        .setOp(CompositeFilter.Operator.AND).addAllFilters(fs)).build());
            }
            if (isWord("ORDER")) {
                next();
                word("BY");
                do {
                    if (isSym(",")) {
                        next();
                    }
                    String p = next().text;
                    PropertyOrder.Direction d = PropertyOrder.Direction.ASCENDING;
                    if (isWord("ASC")) {
                        next();
                    } else if (isWord("DESC")) {
                        next();
                        d = PropertyOrder.Direction.DESCENDING;
                    }
                    q.addOrder(PropertyOrder.newBuilder().setProperty(PropertyReference.newBuilder().setName(p)).setDirection(d));
                } while (isSym(","));
            }
            while (isWord("LIMIT") || isWord("OFFSET")) {
                boolean limit = next().text.equalsIgnoreCase("LIMIT");
                Tok t = next();
                if (t.type == 4) {
                    GqlQueryParameter b = binding(t);
                    if (b.getParameterTypeCase() == GqlQueryParameter.ParameterTypeCase.CURSOR) {
                        if (limit) {
                            throw err("a cursor binding cannot be used as a limit", t.pos);
                        }
                        q.setStartCursor(b.getCursor());
                    } else {
                        int n = (int) b.getValue().getIntegerValue();
                        if (limit) {
                            q.setLimit(Int32Value.of(n));
                        } else {
                            q.setOffset(n);
                        }
                    }
                } else if (t.type == 1) {
                    literalCheck(t);
                    int n = Integer.parseInt(t.text);
                    if (limit) {
                        q.setLimit(Int32Value.of(n));
                    } else {
                        q.setOffset(n);
                    }
                } else {
                    throw err("expected a number", t.pos);
                }
            }
            if (i < toks.size()) {
                throw err("unexpected \"" + toks.get(i).text + "\"", toks.get(i).pos);
            }
            return q.build();
        }

        private void literalCheck(Tok t) {
            if (!g.getAllowLiterals()) {
                throw DsException.invalid("Disallowed literal: " + t.raw + ".");
            }
        }

        private Filter condition() {
            Tok p = next();
            String prop = p.text;
            PropertyFilter.Builder pf = PropertyFilter.newBuilder().setProperty(PropertyReference.newBuilder().setName(prop));
            if (isWord("HAS")) {
                next();
                word("ANCESTOR");
                pf.setOp(PropertyFilter.Operator.HAS_ANCESTOR).setValue(value());
                return Filter.newBuilder().setPropertyFilter(pf).build();
            }
            if (isWord("IN")) {
                next();
                List<Value> vs = new ArrayList<>();
                if (isSym("(")) {
                    next();
                    do {
                        if (isSym(",")) {
                            next();
                        }
                        vs.add(value());
                    } while (isSym(","));
                    sym(")");
                    pf.setValue(Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(vs)));
                } else {
                    pf.setValue(value());
                }
                pf.setOp(PropertyFilter.Operator.IN);
                return Filter.newBuilder().setPropertyFilter(pf).build();
            }
            Tok op = next();
            PropertyFilter.Operator o;
            String t = op.text.toUpperCase(Locale.ROOT);
            switch (t) {
                case "=":
                case "CONTAINS":
                    o = PropertyFilter.Operator.EQUAL;
                    break;
                case "<":
                    o = PropertyFilter.Operator.LESS_THAN;
                    break;
                case "<=":
                    o = PropertyFilter.Operator.LESS_THAN_OR_EQUAL;
                    break;
                case ">":
                    o = PropertyFilter.Operator.GREATER_THAN;
                    break;
                case ">=":
                    o = PropertyFilter.Operator.GREATER_THAN_OR_EQUAL;
                    break;
                case "!=":
                    o = PropertyFilter.Operator.NOT_EQUAL;
                    break;
                default:
                    throw err("unknown operator \"" + op.text + "\"", op.pos);
            }
            pf.setOp(o).setValue(value());
            return Filter.newBuilder().setPropertyFilter(pf).build();
        }

        private GqlQueryParameter binding(Tok t) {
            boolean numeric = !t.text.isEmpty() && Character.isDigit(t.text.charAt(0));
            if (numeric) {
                int idx = Integer.parseInt(t.text) - 1;
                if (idx < 0 || idx >= g.getPositionalBindingsCount()) {
                    throw DsException.invalid("Absent positional binding: " + t.text + ".");
                }
                return g.getPositionalBindings(idx);
            }
            GqlQueryParameter p = g.getNamedBindingsMap().get(t.text);
            if (p == null) {
                throw DsException.invalid("Absent named binding: " + t.text + ".");
            }
            return p;
        }

        private Value value() {
            Tok t = next();
            switch (t.type) {
                case 4: {
                    GqlQueryParameter b = binding(t);
                    if (b.getParameterTypeCase() != GqlQueryParameter.ParameterTypeCase.VALUE) {
                        throw err("a cursor cannot be used as a value", t.pos);
                    }
                    return b.getValue();
                }
                case 1:
                    literalCheck(t);
                    if (t.text.contains(".") || t.text.contains("e") || t.text.contains("E")) {
                        return Value.newBuilder().setDoubleValue(Double.parseDouble(t.text)).build();
                    }
                    return Value.newBuilder().setIntegerValue(Long.parseLong(t.text)).build();
                case 2:
                    literalCheck(t);
                    return Value.newBuilder().setStringValue(t.text).build();
                case 0: {
                    String w = t.text.toUpperCase(Locale.ROOT);
                    switch (w) {
                        case "TRUE":
                            literalCheck(t);
                            return Value.newBuilder().setBooleanValue(true).build();
                        case "FALSE":
                            literalCheck(t);
                            return Value.newBuilder().setBooleanValue(false).build();
                        case "NULL":
                            literalCheck(t);
                            return DsValues.nullValue();
                        case "DATETIME": {
                            sym("(");
                            Tok s = next();
                            sym(")");
                            literalCheck(t);
                            try {
                                java.time.Instant in = java.time.OffsetDateTime.parse(s.text).toInstant();
                                return Value.newBuilder().setTimestampValue(com.google.protobuf.Timestamp.newBuilder().setSeconds(in.getEpochSecond()).setNanos(in.getNano())).build();
                            } catch (RuntimeException e) {
                                throw err("invalid datetime \"" + s.text + "\"", s.pos);
                            }
                        }
                        case "BLOB": {
                            sym("(");
                            Tok s = next();
                            sym(")");
                            literalCheck(t);
                            return Value.newBuilder().setBlobValue(ByteString.copyFrom(java.util.Base64.getDecoder().decode(s.text))).build();
                        }
                        case "KEY": {
                            sym("(");
                            Key.Builder k = Key.newBuilder().setPartitionId(com.google.datastore.v1.PartitionId.getDefaultInstance());
                            do {
                                if (isSym(",")) {
                                    next();
                                }
                                Tok kind = next();
                                sym(",");
                                Tok id = next();
                                Key.PathElement.Builder e = Key.PathElement.newBuilder().setKind(kind.text);
                                if (id.type == 1) {
                                    e.setId(Long.parseLong(id.text));
                                } else if (id.type == 4) {
                                    GqlQueryParameter b = binding(id);
                                    if (b.getValue().getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
                                        e.setId(b.getValue().getIntegerValue());
                                    } else {
                                        e.setName(b.getValue().getStringValue());
                                    }
                                } else {
                                    e.setName(id.text);
                                }
                                k.addPath(e);
                            } while (isSym(","));
                            sym(")");
                            return Value.newBuilder().setKeyValue(k).build();
                        }
                        default:
                            throw err("unexpected \"" + t.text + "\"", t.pos);
                    }
                }
                default:
                    throw err("unexpected \"" + t.text + "\"", t.pos);
            }
        }
    }
}
