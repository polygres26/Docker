package com.sayonora.wire.dynamowire;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Evaluation of parsed condition expressions and application of parsed update plans. */
public final class ExprEval {

    private ExprEval() {}

    // ------------------------------------------------------------------------------- conditions

    public static boolean test(Expr.Cond cond, Map<String, AttributeValue> item) {
        if (cond == null) {
            return true;
        }
        return switch (cond) {
            case Expr.And a -> {
                for (Expr.Cond c : a.parts()) if (!test(c, item)) yield false;
                yield true;
            }
            case Expr.Or o -> {
                for (Expr.Cond c : o.parts()) if (test(c, item)) yield true;
                yield false;
            }
            case Expr.Not n -> !test(n.inner(), item);
            case Expr.Cmp c -> compare(c.op(), operand(c.left(), item), operand(c.right(), item));
            case Expr.Between b -> {
                AttributeValue v = operand(b.value(), item);
                AttributeValue lo = operand(b.low(), item);
                AttributeValue hi = operand(b.high(), item);
                if (v == null || lo == null || hi == null) yield false;
                Integer c1 = v.compareOrNull(lo), c2 = v.compareOrNull(hi);
                yield c1 != null && c2 != null && c1 >= 0 && c2 <= 0;
            }
            case Expr.In in -> {
                AttributeValue v = operand(in.value(), item);
                if (v == null) yield false;
                for (Expr.Operand cand : in.candidates()) {
                    if (v.deepEquals(operand(cand, item))) yield true;
                }
                yield false;
            }
            case Expr.Fn f -> function(f, item);
        };
    }

    private static boolean compare(String op, AttributeValue l, AttributeValue r) {
        switch (op) {
            case "=":
                return l != null && l.deepEquals(r);
            case "<>":
                return l == null || r == null || !l.deepEquals(r);
            default:
                if (l == null || r == null) return false;
                Integer c = l.compareOrNull(r);
                if (c == null) return false;
                return switch (op) {
                    case "<" -> c < 0;
                    case "<=" -> c <= 0;
                    case ">" -> c > 0;
                    case ">=" -> c >= 0;
                    default -> false;
                };
        }
    }

    private static boolean function(Expr.Fn f, Map<String, AttributeValue> item) {
        switch (f.name()) {
            case "attribute_exists":
                return operand(f.args().get(0), item) != null;
            case "attribute_not_exists":
                return operand(f.args().get(0), item) == null;
            case "attribute_type": {
                AttributeValue v = operand(f.args().get(0), item);
                AttributeValue t = operand(f.args().get(1), item);
                return v != null && t != null && v.type.name().equals(t.scalar);
            }
            case "begins_with": {
                AttributeValue v = operand(f.args().get(0), item);
                AttributeValue p = operand(f.args().get(1), item);
                if (v == null || p == null || v.type != p.type) return false;
                if (v.type == AttributeValue.Type.S) return v.scalar.startsWith(p.scalar);
                if (v.type == AttributeValue.Type.B) {
                    byte[] a = v.bytes(), b = p.bytes();
                    if (b.length > a.length) return false;
                    for (int i = 0; i < b.length; i++) if (a[i] != b[i]) return false;
                    return true;
                }
                return false;
            }
            case "contains": {
                AttributeValue v = operand(f.args().get(0), item);
                AttributeValue needle = operand(f.args().get(1), item);
                if (v == null || needle == null) return false;
                switch (v.type) {
                    case S:
                        return needle.type == AttributeValue.Type.S && v.scalar.contains(needle.scalar);
                    case B: {
                        if (needle.type != AttributeValue.Type.B) return false;
                        return indexOf(v.bytes(), needle.bytes()) >= 0;
                    }
                    case SS:
                        return needle.type == AttributeValue.Type.S && v.stringSet.contains(needle.scalar);
                    case NS:
                        return needle.type == AttributeValue.Type.N && v.stringSet.contains(needle.scalar);
                    case BS:
                        return needle.type == AttributeValue.Type.B && v.stringSet.contains(needle.scalar);
                    case L:
                        for (AttributeValue el : v.list) if (el.deepEquals(needle)) return true;
                        return false;
                    default:
                        return false;
                }
            }
            default:
                throw DynamoException.validation("Invalid function name; function: " + f.name());
        }
    }

    private static int indexOf(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) if (hay[i + j] != needle[j]) continue outer;
            return i;
        }
        return -1;
    }

    /** The operand's value, or null when it refers to a path that is absent. */
    public static AttributeValue operand(Expr.Operand op, Map<String, AttributeValue> item) {
        return switch (op) {
            case Expr.PathOp p -> p.path().get(item);
            case Expr.ValueOp v -> v.value();
            case Expr.SizeOp s -> {
                AttributeValue v = s.path().get(item);
                if (v == null) yield null;
                yield switch (v.type) {
                    case S -> AttributeValue.ofN(String.valueOf(v.scalar.codePointCount(0, v.scalar.length())));
                    case B -> AttributeValue.ofN(String.valueOf(v.bytes().length));
                    case SS, NS, BS -> AttributeValue.ofN(String.valueOf(v.stringSet.size()));
                    case L -> AttributeValue.ofN(String.valueOf(v.list.size()));
                    case M -> AttributeValue.ofN(String.valueOf(v.map.size()));
                    default -> null;
                };
            }
            case Expr.IfNotExists i -> {
                AttributeValue v = i.path().get(item);
                yield v != null ? v : operand(i.fallback(), item);
            }
            case Expr.ListAppend la -> {
                AttributeValue a = operand(la.left(), item), b = operand(la.right(), item);
                for (AttributeValue v : new AttributeValue[] {a, b}) {
                    if (v != null && v.type != AttributeValue.Type.L) {
                        throw DynamoException.validation("Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: "
                                + v.type);
                    }
                }
                if (a == null || b == null) {
                    throw DynamoException.validation("The provided expression refers to an attribute that does not exist in the item");
                }
                List<AttributeValue> merged = new ArrayList<>(a.list);
                merged.addAll(b.list);
                yield AttributeValue.ofL(merged);
            }
            case Expr.Arith ar -> {
                AttributeValue a = operand(ar.left(), item), b = operand(ar.right(), item);
                for (AttributeValue v : new AttributeValue[] {a, b}) {
                    if (v != null && v.type != AttributeValue.Type.N) {
                        throw DynamoException.validation("Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: "
                                + ar.op() + ", operand type: " + v.type);
                    }
                }
                if (a == null || b == null) {
                    throw DynamoException.validation("The provided expression refers to an attribute that does not exist in the item");
                }
                BigDecimal r = ar.op() == '+' ? a.number().add(b.number()) : a.number().subtract(b.number());
                yield AttributeValue.ofNumber(r);
            }
        };
    }

    // ------------------------------------------------------------------------------- updates

    private static final AttributeValue TOMBSTONE = AttributeValue.ofNull();

    public record UpdateResult(Map<String, AttributeValue> item, List<Expr.Path> touched) {}

    /**
     * Applies {@code plan} to (a deep copy of) {@code base}. All operand values are evaluated against
     * the original item first (so {@code SET a = b, b = a} swaps), then the mutations are applied.
     */
    public static UpdateResult apply(Expr.UpdatePlan plan, Map<String, AttributeValue> base) {
        Map<String, AttributeValue> item = AttributeValue.copyItem(base);
        List<AttributeValue> computed = new ArrayList<>();
        for (Expr.UpdateAction a : plan.actions()) {
            computed.add(a.kind() == Expr.ActionKind.SET ? operand(a.operand(), base)
                    : a.kind() == Expr.ActionKind.REMOVE ? null : ((Expr.ValueOp) a.operand()).value());
            if (a.kind() == Expr.ActionKind.SET && computed.get(computed.size() - 1) == null) {
                throw DynamoException.validation("The provided expression refers to an attribute that does not exist in the item");
            }
        }
        boolean removedListElement = false;
        List<Expr.Path> touched = new ArrayList<>();
        for (int i = 0; i < plan.actions().size(); i++) {
            Expr.UpdateAction a = plan.actions().get(i);
            touched.add(a.path());
            switch (a.kind()) {
                case SET -> set(item, a.path(), computed.get(i).deepCopy());
                case REMOVE -> removedListElement |= remove(item, a.path());
                case ADD -> add(item, a.path(), computed.get(i));
                case DELETE -> delete(item, a.path(), computed.get(i));
            }
        }
        if (removedListElement) {
            compact(item);
        }
        return new UpdateResult(item, touched);
    }

    private static void set(Map<String, AttributeValue> item, Expr.Path path, AttributeValue value) {
        if (path.isTopLevel()) {
            item.put(path.top(), value);
            return;
        }
        AttributeValue parent = parentOf(item, path);
        Object last = path.segs().get(path.segs().size() - 1);
        if (last instanceof String key) {
            if (parent == null || parent.type != AttributeValue.Type.M) {
                throw DynamoException.validation("The document path provided in the update expression is invalid for update");
            }
            parent.map.put(key, value);
        } else {
            int idx = (Integer) last;
            if (parent == null || parent.type != AttributeValue.Type.L) {
                throw DynamoException.validation("The document path provided in the update expression is invalid for update");
            }
            if (idx < parent.list.size()) parent.list.set(idx, value);
            else parent.list.add(value);
        }
    }

    /** Container holding the last segment of {@code path}, or null if any step is missing. */
    private static AttributeValue parentOf(Map<String, AttributeValue> item, Expr.Path path) {
        AttributeValue cur = item.get(path.top());
        for (int i = 1; i < path.segs().size() - 1 && cur != null; i++) {
            Object seg = path.segs().get(i);
            if (seg instanceof String key) {
                cur = cur.type == AttributeValue.Type.M ? cur.map.get(key) : null;
            } else {
                int idx = (Integer) seg;
                cur = cur.type == AttributeValue.Type.L && idx < cur.list.size() ? cur.list.get(idx) : null;
            }
        }
        return cur;
    }

    private static boolean remove(Map<String, AttributeValue> item, Expr.Path path) {
        if (path.isTopLevel()) {
            item.remove(path.top());
            return false;
        }
        AttributeValue parent = parentOf(item, path);
        if (parent == null) {
            throw DynamoException.validation("The document path provided in the update expression is invalid for update");
        }
        Object last = path.segs().get(path.segs().size() - 1);
        if (last instanceof String key) {
            if (parent.type == AttributeValue.Type.M) parent.map.remove(key);
            return false;
        }
        int idx = (Integer) last;
        if (parent.type == AttributeValue.Type.L && idx < parent.list.size()) {
            parent.list.set(idx, TOMBSTONE);
            return true;
        }
        return false;
    }

    private static void compact(Map<String, AttributeValue> map) {
        for (AttributeValue v : map.values()) compactValue(v);
    }

    private static void compactValue(AttributeValue v) {
        if (v.type == AttributeValue.Type.L) {
            v.list.removeIf(e -> e == TOMBSTONE);
            for (AttributeValue e : v.list) compactValue(e);
        } else if (v.type == AttributeValue.Type.M) {
            for (AttributeValue e : v.map.values()) compactValue(e);
        }
    }

    private static void add(Map<String, AttributeValue> item, Expr.Path path, AttributeValue delta) {
        AttributeValue existing = path.get(item);
        AttributeValue result;
        if (delta.type == AttributeValue.Type.N) {
            if (existing != null && existing.type != AttributeValue.Type.N) {
                throw DynamoException.validation("An operand in the update expression has an incorrect data type");
            }
            BigDecimal base = existing != null ? existing.number() : BigDecimal.ZERO;
            result = AttributeValue.ofNumber(base.add(delta.number()));
        } else if (delta.isSet()) {
            if (existing != null && existing.type != delta.type) {
                throw DynamoException.validation("An operand in the update expression has an incorrect data type");
            }
            Set<String> union = existing != null ? new LinkedHashSet<>(existing.stringSet) : new LinkedHashSet<>();
            union.addAll(delta.stringSet);
            result = AttributeValue.ofSet(delta.type, union);
        } else {
            throw DynamoException.validation("Invalid UpdateExpression: Incorrect operand type for operator or function; operator: ADD, operand type: "
                    + delta.typeName() + ", typeSet: ALLOWED_FOR_ADD_OPERAND");
        }
        set(item, path, result);
    }

    private static void delete(Map<String, AttributeValue> item, Expr.Path path, AttributeValue toRemove) {
        if (!toRemove.isSet()) {
            throw DynamoException.validation("Invalid UpdateExpression: Incorrect operand type for operator or function; operator: DELETE, operand type: "
                    + toRemove.typeName() + ", typeSet: ALLOWED_FOR_DELETE_OPERAND");
        }
        AttributeValue existing = path.get(item);
        if (existing == null) return;
        if (existing.type != toRemove.type) {
            throw DynamoException.validation("An operand in the update expression has an incorrect data type");
        }
        Set<String> remaining = new LinkedHashSet<>(existing.stringSet);
        remaining.removeAll(toRemove.stringSet);
        if (remaining.isEmpty()) remove(item, path);
        else set(item, path, AttributeValue.ofSet(existing.type, remaining));
    }

    // ------------------------------------------------------------------------------- projection

    /** Applies a ProjectionExpression (parsed paths) to an item; null/empty paths keep everything. */
    public static Map<String, AttributeValue> project(Map<String, AttributeValue> item, List<Expr.Path> paths) {
        if (paths == null || paths.isEmpty()) {
            return item;
        }
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        for (Expr.Path p : paths) {
            AttributeValue v = p.get(item);
            if (v != null) p.putInto(out, v.deepCopy());
        }
        return out;
    }

    /** The top-level attributes of {@code item} that the given (touched) paths belong to, for
     * ReturnValues=UPDATED_OLD/NEW (DynamoDB returns the whole attribute, e.g. the entire map when a
     * nested key was updated). */
    public static Map<String, AttributeValue> valuesAt(Map<String, AttributeValue> item, List<Expr.Path> paths) {
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        if (item == null) return out;
        for (Expr.Path p : paths) {
            AttributeValue v = item.get(p.top());
            if (v != null) out.put(p.top(), v.deepCopy());
        }
        return out;
    }
}
