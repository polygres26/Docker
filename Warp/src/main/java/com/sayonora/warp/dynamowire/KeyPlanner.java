package com.sayonora.warp.dynamowire;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns a Query's key condition (a parsed KeyConditionExpression, or legacy KeyConditions) into a
 * {@link Plan} against a table or index key schema, applying DynamoDB's rules: every partition
 * (HASH) attribute needs an equality; sort (RANGE) attributes may be constrained left to right
 * with equalities and one final range condition; values must have the key attribute's type.
 */
final class KeyPlanner {

    private KeyPlanner() {}

    /** Resolved key condition: values for the hash attributes and conditions for a prefix of the range attributes. */
    record Plan(List<TableSchema.KeyAttr> hashAttrs, List<AttributeValue> hashValues,
            List<TableSchema.KeyAttr> rangeAttrs, List<Expr.KeyCond> rangeConds) {}

    /** The key attributes of an access path. */
    static List<TableSchema.KeyAttr> hashAttrs(TableSchema s, TableSchema.IndexDef idx) {
        if (idx != null) return idx.hash();
        return List.of(new TableSchema.KeyAttr(s.partitionKeyName(), s.partitionKeyType()));
    }

    static List<TableSchema.KeyAttr> rangeAttrs(TableSchema s, TableSchema.IndexDef idx) {
        if (idx != null) return idx.range();
        return s.hasSortKey() ? List.of(new TableSchema.KeyAttr(s.sortKeyName(), s.sortKeyType())) : List.of();
    }

    // ------------------------------------------------------------------------------- sources

    /** KeyConditionExpression (already parsed as a condition) -> terms. */
    static List<Expr.KeyCond> termsFromCondition(Expr.Cond cond) {
        List<Expr.KeyCond> out = new ArrayList<>();
        collect(cond, out);
        return out;
    }

    private static void collect(Expr.Cond c, List<Expr.KeyCond> out) {
        switch (c) {
            case Expr.And a -> {
                for (Expr.Cond p : a.parts()) collect(p, out);
            }
            case Expr.Cmp cmp -> {
                String op = switch (cmp.op()) {
                    case "=" -> "EQ";
                    case "<" -> "LT";
                    case "<=" -> "LE";
                    case ">" -> "GT";
                    case ">=" -> "GE";
                    default -> throw DynamoException.validation("Invalid operator used in KeyConditionExpression: " + cmp.op());
                };
                if (cmp.left() instanceof Expr.PathOp p && cmp.right() instanceof Expr.ValueOp v) {
                    out.add(new Expr.KeyCond(attrOf(p), op, v.value(), null));
                } else if (cmp.left() instanceof Expr.ValueOp v && cmp.right() instanceof Expr.PathOp p) {
                    out.add(new Expr.KeyCond(attrOf(p), flip(op), v.value(), null));
                } else {
                    throw DynamoException.validation("Invalid KeyConditionExpression: Unsupported condition; key conditions compare a key attribute with a value");
                }
            }
            case Expr.Between b -> {
                if (b.value() instanceof Expr.PathOp p && b.low() instanceof Expr.ValueOp lo && b.high() instanceof Expr.ValueOp hi) {
                    out.add(new Expr.KeyCond(attrOf(p), "BETWEEN", lo.value(), hi.value()));
                } else {
                    throw DynamoException.validation("Invalid KeyConditionExpression: Unsupported condition; BETWEEN needs a key attribute and two values");
                }
            }
            case Expr.Fn f when f.name().equals("begins_with") -> {
                if (f.args().get(0) instanceof Expr.PathOp p && f.args().get(1) instanceof Expr.ValueOp v) {
                    out.add(new Expr.KeyCond(attrOf(p), "BEGINS_WITH", v.value(), null));
                } else {
                    throw DynamoException.validation("Invalid KeyConditionExpression: Unsupported condition; begins_with needs a key attribute and a value");
                }
            }
            case Expr.Or o -> throw DynamoException.validation("Invalid operator used in KeyConditionExpression: OR");
            case Expr.Not n -> throw DynamoException.validation("Invalid operator used in KeyConditionExpression: NOT");
            case Expr.In in -> throw DynamoException.validation("Invalid operator used in KeyConditionExpression: IN");
            default -> throw DynamoException.validation("Invalid operator used in KeyConditionExpression: "
                    + (c instanceof Expr.Fn f ? f.name() : c.getClass().getSimpleName()));
        }
    }

    private static String attrOf(Expr.PathOp p) {
        if (!p.path().isTopLevel()) {
            throw DynamoException.validation("Invalid KeyConditionExpression: Key attributes must be top-level attribute names");
        }
        return p.path().top();
    }

    private static String flip(String op) {
        return switch (op) {
            case "LT" -> "GT";
            case "LE" -> "GE";
            case "GT" -> "LT";
            case "GE" -> "LE";
            default -> op;
        };
    }

    // ------------------------------------------------------------------------------- planning

    static Plan plan(TableSchema s, TableSchema.IndexDef idx, List<Expr.KeyCond> terms) {
        List<TableSchema.KeyAttr> hash = hashAttrs(s, idx), range = rangeAttrs(s, idx);
        Set<String> keyNames = new HashSet<>();
        for (TableSchema.KeyAttr k : hash) keyNames.add(k.name());
        for (TableSchema.KeyAttr k : range) keyNames.add(k.name());
        Set<String> seen = new HashSet<>();
        for (Expr.KeyCond t : terms) {
            if (!keyNames.contains(t.attr())) {
                throw DynamoException.validation("Query key condition not supported");
            }
            if (!seen.add(t.attr())) {
                throw DynamoException.validation("KeyConditionExpressions must only contain one condition per key");
            }
        }
        List<AttributeValue> hashValues = new ArrayList<>();
        for (TableSchema.KeyAttr k : hash) {
            Expr.KeyCond t = forAttr(terms, k.name());
            if (t == null) {
                throw DynamoException.validation("Query condition missed key schema element: " + k.name());
            }
            if (!t.op().equals("EQ")) {
                throw DynamoException.validation("Query key condition not supported");
            }
            checkType(k, t, false);
            hashValues.add(t.v1());
        }
        List<Expr.KeyCond> rangeConds = new ArrayList<>();
        boolean gap = false;
        for (int i = 0; i < range.size(); i++) {
            TableSchema.KeyAttr k = range.get(i);
            Expr.KeyCond t = forAttr(terms, k.name());
            if (t == null) {
                gap = true;
                continue;
            }
            if (gap) {
                throw DynamoException.validation("Query condition missed key schema element: " + range.get(i - 1).name());
            }
            checkType(k, t, true);
            rangeConds.add(t);
        }
        for (int i = 0; i < rangeConds.size() - 1; i++) {
            if (!rangeConds.get(i).op().equals("EQ")) {
                throw DynamoException.validation("Query key condition not supported");
            }
        }
        return new Plan(hash, hashValues, range, rangeConds);
    }

    private static Expr.KeyCond forAttr(List<Expr.KeyCond> terms, String attr) {
        for (Expr.KeyCond t : terms) if (t.attr().equals(attr)) return t;
        return null;
    }

    private static void checkType(TableSchema.KeyAttr k, Expr.KeyCond t, boolean range) {
        for (AttributeValue v : new AttributeValue[] {t.v1(), t.v2()}) {
            if (v == null) continue;
            if (!k.type().equals(v.type.name())) {
                throw DynamoException.validation("One or more parameter values were invalid: Condition parameter type does not match schema type");
            }
        }
        if (t.op().equals("BEGINS_WITH") && !k.type().equals("S") && !k.type().equals("B")) {
            throw DynamoException.validation("Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: "
                    + k.type());
        }
        if (t.op().equals("BETWEEN") && t.v1().compareOrNull(t.v2()) != null && t.v1().compareOrNull(t.v2()) > 0) {
            throw DynamoException.validation("Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound");
        }
    }

    // ------------------------------------------------------------------------------- filter check

    /** Query FilterExpressions may not mention the key attributes of the queried table/index. */
    static void checkFilterHasNoKeys(Expr.Cond filter, TableSchema s, TableSchema.IndexDef idx) {
        if (filter == null) return;
        Set<String> keys = new HashSet<>();
        for (TableSchema.KeyAttr k : hashAttrs(s, idx)) keys.add(k.name());
        for (TableSchema.KeyAttr k : rangeAttrs(s, idx)) keys.add(k.name());
        Set<String> used = new HashSet<>();
        attrs(filter, used);
        for (String u : used) {
            if (keys.contains(u)) {
                throw DynamoException.validation("Filter Expression can only contain non-primary key attributes: Primary key attribute: " + u);
            }
        }
    }

    private static void attrs(Expr.Cond c, Set<String> out) {
        switch (c) {
            case Expr.And a -> a.parts().forEach(p -> attrs(p, out));
            case Expr.Or o -> o.parts().forEach(p -> attrs(p, out));
            case Expr.Not n -> attrs(n.inner(), out);
            case Expr.Cmp cmp -> {
                operandAttrs(cmp.left(), out);
                operandAttrs(cmp.right(), out);
            }
            case Expr.Between b -> {
                operandAttrs(b.value(), out);
                operandAttrs(b.low(), out);
                operandAttrs(b.high(), out);
            }
            case Expr.In in -> {
                operandAttrs(in.value(), out);
                in.candidates().forEach(o -> operandAttrs(o, out));
            }
            case Expr.Fn f -> f.args().forEach(o -> operandAttrs(o, out));
        }
    }

    private static void operandAttrs(Expr.Operand o, Set<String> out) {
        if (o instanceof Expr.PathOp p) out.add(p.path().top());
        else if (o instanceof Expr.SizeOp s) out.add(s.path().top());
    }

    /** Wrong-shaped ExclusiveStartKey -> ValidationException. */
    static void checkStartKey(TableSchema s, TableSchema.IndexDef idx, Map<String, AttributeValue> startKey) {
        Set<String> expected = new java.util.LinkedHashSet<>();
        expected.add(s.partitionKeyName());
        if (s.hasSortKey()) expected.add(s.sortKeyName());
        Map<String, String> types = new java.util.LinkedHashMap<>();
        types.put(s.partitionKeyName(), s.partitionKeyType());
        if (s.hasSortKey()) types.put(s.sortKeyName(), s.sortKeyType());
        if (idx != null) {
            for (TableSchema.KeyAttr k : idx.allKeys()) {
                expected.add(k.name());
                types.put(k.name(), k.type());
            }
        }
        if (!startKey.keySet().equals(expected)) {
            throw DynamoException.validation("The provided starting key is invalid: The provided key element does not match the schema");
        }
        for (var e : startKey.entrySet()) {
            if (!types.get(e.getKey()).equals(e.getValue().type.name())) {
                throw DynamoException.validation("The provided starting key is invalid: One or more parameter values were invalid: Type mismatch for key "
                        + e.getKey() + " expected: " + types.get(e.getKey()) + " actual: " + e.getValue().type);
            }
        }
    }
}
