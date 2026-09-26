package com.sayonora.warp.dynamowire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The parsed form of DynamoDB expressions (condition / filter / key-condition, update, projection).
 * Both the expression-string parser ({@link ExprParser}), the legacy parameter translation
 * ({@link LegacyParams}) and the PartiQL front end produce these nodes, so there is exactly one
 * evaluator for all of them.
 */
public final class Expr {

    private Expr() {}

    // ------------------------------------------------------------------------------- paths

    /** A document path: attribute name segments (String) and list indexes (Integer). */
    public record Path(List<Object> segs) {

        public static Path of(String attr) {
            List<Object> l = new ArrayList<>();
            l.add(attr);
            return new Path(l);
        }

        public String top() {
            return (String) segs.get(0);
        }

        public boolean isTopLevel() {
            return segs.size() == 1;
        }

        /** The value at this path, or null when any step is missing / of the wrong shape. */
        public AttributeValue get(Map<String, AttributeValue> item) {
            if (item == null) return null;
            AttributeValue cur = item.get(top());
            for (int i = 1; i < segs.size() && cur != null; i++) {
                Object seg = segs.get(i);
                if (seg instanceof String key) {
                    cur = cur.type == AttributeValue.Type.M ? cur.map.get(key) : null;
                } else {
                    int idx = (Integer) seg;
                    cur = cur.type == AttributeValue.Type.L && idx < cur.list.size() ? cur.list.get(idx) : null;
                }
            }
            return cur;
        }

        /** Builds the parent structure inside {@code out} so that {@code value} sits at this path
         * (used to assemble ReturnValues=UPDATED_* and projection results). */
        public void putInto(Map<String, AttributeValue> out, AttributeValue value) {
            if (segs.size() == 1) {
                out.put(top(), value);
                return;
            }
            AttributeValue root = out.get(top());
            if (root == null) {
                root = containerFor(segs.get(1));
                out.put(top(), root);
            }
            AttributeValue cur = root;
            for (int i = 1; i < segs.size(); i++) {
                Object seg = segs.get(i);
                boolean last = i == segs.size() - 1;
                if (seg instanceof String key) {
                    if (cur.type != AttributeValue.Type.M) return;
                    if (last) {
                        cur.map.put(key, value);
                    } else {
                        AttributeValue next = cur.map.get(key);
                        if (next == null) {
                            next = containerFor(segs.get(i + 1));
                            cur.map.put(key, next);
                        }
                        cur = next;
                    }
                } else {
                    if (cur.type != AttributeValue.Type.L) return;
                    if (last) {
                        cur.list.add(value);
                    } else {
                        // projection keeps only the addressed element, re-indexed from 0
                        AttributeValue next = cur.list.isEmpty() ? null : cur.list.get(cur.list.size() - 1);
                        if (next == null || next.type != containerFor(segs.get(i + 1)).type) {
                            next = containerFor(segs.get(i + 1));
                            cur.list.add(next);
                        }
                        cur = next;
                    }
                }
            }
        }

        private static AttributeValue containerFor(Object nextSeg) {
            return nextSeg instanceof Integer ? AttributeValue.ofL(new ArrayList<>())
                    : AttributeValue.ofM(new LinkedHashMap<>());
        }

        public String display() {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < segs.size(); i++) {
                Object s = segs.get(i);
                if (i > 0) sb.append(", ");
                sb.append(s instanceof Integer n ? "[" + n + "]" : s);
            }
            return sb.append("]").toString();
        }

        /** True when {@code other} is this path or lies below it (or vice versa). */
        public boolean overlaps(Path other) {
            int n = Math.min(segs.size(), other.segs.size());
            for (int i = 0; i < n; i++) {
                if (!segs.get(i).equals(other.segs.get(i))) return false;
            }
            return true;
        }

        /** Same prefix, then a name step meets an index step: DynamoDB calls this a conflict. */
        public boolean conflicts(Path other) {
            int n = Math.min(segs.size(), other.segs.size());
            for (int i = 0; i < n; i++) {
                Object a = segs.get(i), b = other.segs.get(i);
                if (a.equals(b)) continue;
                return (a instanceof Integer) != (b instanceof Integer);
            }
            return false;
        }
    }

    // ------------------------------------------------------------------------------- operands

    public sealed interface Operand permits PathOp, ValueOp, SizeOp, IfNotExists, ListAppend, Arith {}

    public record PathOp(Path path) implements Operand {}

    public record ValueOp(AttributeValue value) implements Operand {}

    /** {@code size(path)} */
    public record SizeOp(Path path) implements Operand {}

    public record IfNotExists(Path path, Operand fallback) implements Operand {}

    public record ListAppend(Operand left, Operand right) implements Operand {}

    /** {@code left + right} / {@code left - right} (update expressions only). */
    public record Arith(char op, Operand left, Operand right) implements Operand {}

    // ------------------------------------------------------------------------------- conditions

    public sealed interface Cond permits And, Or, Not, Cmp, Between, In, Fn {}

    public record And(List<Cond> parts) implements Cond {}

    public record Or(List<Cond> parts) implements Cond {}

    public record Not(Cond inner) implements Cond {}

    /** op is one of = <> < <= > >= */
    public record Cmp(String op, Operand left, Operand right) implements Cond {}

    public record Between(Operand value, Operand low, Operand high) implements Cond {}

    public record In(Operand value, List<Operand> candidates) implements Cond {}

    /** attribute_exists, attribute_not_exists, attribute_type, begins_with, contains */
    public record Fn(String name, List<Operand> args) implements Cond {}

    // ------------------------------------------------------------------------------- updates

    public enum ActionKind { SET, REMOVE, ADD, DELETE }

    public record UpdateAction(ActionKind kind, Path path, Operand operand) {}

    public record UpdatePlan(List<UpdateAction> actions) {
        public boolean isEmpty() {
            return actions.isEmpty();
        }
    }

    // ------------------------------------------------------------------------------- key condition

    /** One conjunct of a KeyConditionExpression / KeyConditions on a key attribute. */
    public record KeyCond(String attr, String op, AttributeValue v1, AttributeValue v2) {}

    /** A structured Query key condition: attr -> condition (one per key attribute). */
    public record KeyConditions(List<KeyCond> conds) {
        public KeyCond forAttr(String attr) {
            for (KeyCond c : conds) if (c.attr().equals(attr)) return c;
            return null;
        }
    }
}
