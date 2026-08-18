package com.polygres.wire.dynamowire;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes DynamoDB {@code UpdateExpression}s against an item's attribute map, mutating it in
 * place. Scope covered: {@code SET path = value}, {@code SET path = path}, {@code SET path =
 * path + value} / {@code path - value} (numeric), {@code SET path = if_not_exists(path, value)},
 * {@code SET path = list_append(path, value)}, {@code REMOVE path[, path...]},
 * {@code ADD path value} (numeric increment, or SS/NS/BS set union), {@code DELETE path value}
 * (SS/NS/BS set difference) or bare {@code DELETE path}. Clauses are separated by the {@code SET}/
 * {@code REMOVE}/{@code ADD}/{@code DELETE} keywords (case-sensitive, matching real DynamoDB);
 * multiple assignments within one clause are comma-separated. Not covered: nested function
 * composition beyond one level (e.g. {@code list_append} inside {@code if_not_exists}), and
 * {@code REMOVE} of list elements by index renumbering semantics beyond a simple map delete.
 */
public final class UpdateExpressionParser {

    private static final Pattern CLAUSE_SPLIT = Pattern.compile("(?=\\b(SET|REMOVE|ADD|DELETE)\\b)");

    public static void apply(String expr, Map<String, AttributeValue> item, ExpressionContext ctx) {
        if (expr == null || expr.isBlank()) return;
        String[] parts = CLAUSE_SPLIT.split(expr.trim());
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;
            if (part.startsWith("SET")) applySet(part.substring(3).trim(), item, ctx);
            else if (part.startsWith("REMOVE")) applyRemove(part.substring(6).trim(), item, ctx);
            else if (part.startsWith("ADD")) applyAdd(part.substring(3).trim(), item, ctx);
            else if (part.startsWith("DELETE")) applyDelete(part.substring(6).trim(), item, ctx);
            else throw new DynamoException("ValidationException", "Unrecognized UpdateExpression clause: " + part);
        }
    }

    private static java.util.List<String> splitTopLevelCommas(String s) {
        java.util.List<String> out = new java.util.ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) { out.add(s.substring(start, i).trim()); start = i + 1; }
        }
        out.add(s.substring(start).trim());
        return out;
    }

    private static void applySet(String clause, Map<String, AttributeValue> item, ExpressionContext ctx) {
        for (String assignment : splitTopLevelCommas(clause)) {
            int eq = topLevelIndexOf(assignment, '=');
            if (eq < 0) throw new DynamoException("ValidationException", "Invalid SET assignment: " + assignment);
            String pathToken = assignment.substring(0, eq).trim();
            String rhs = assignment.substring(eq + 1).trim();
            ItemPath path = ItemPath.parse(pathToken, ctx);
            AttributeValue value = evalOperand(rhs, item, ctx);
            path.set(item, value);
        }
    }

    private static int topLevelIndexOf(String s, char target) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == target && depth == 0) return i;
        }
        return -1;
    }

    private static AttributeValue evalOperand(String rhs, Map<String, AttributeValue> item, ExpressionContext ctx) {
        rhs = rhs.trim();
        Matcher ifNotExists = Pattern.compile("^if_not_exists\\(\\s*([^,]+),\\s*(.+)\\)$").matcher(rhs);
        if (ifNotExists.matches()) {
            ItemPath p = ItemPath.parse(ifNotExists.group(1).trim(), ctx);
            AttributeValue existing = p.get(item);
            return existing != null ? existing : evalOperand(ifNotExists.group(2).trim(), item, ctx);
        }
        Matcher listAppend = Pattern.compile("^list_append\\(\\s*(.+),\\s*(.+)\\)$").matcher(rhs);
        if (listAppend.matches()) {
            AttributeValue a = evalOperand(listAppend.group(1).trim(), item, ctx);
            AttributeValue b = evalOperand(listAppend.group(2).trim(), item, ctx);
            java.util.List<AttributeValue> merged = new java.util.ArrayList<>();
            if (a != null) merged.addAll(a.list);
            if (b != null) merged.addAll(b.list);
            return AttributeValue.ofL(merged);
        }
        // path + value / path - value (numeric)
        int plus = topLevelIndexOf(rhs, '+');
        int minus = topLevelIndexOf(rhs, '-');
        if (plus > 0 || minus > 0) {
            boolean isPlus = plus > 0 && (minus < 0 || plus < minus);
            int opIdx = isPlus ? plus : minus;
            AttributeValue left = evalOperand(rhs.substring(0, opIdx).trim(), item, ctx);
            AttributeValue right = evalOperand(rhs.substring(opIdx + 1).trim(), item, ctx);
            BigDecimal l = new BigDecimal(left.scalar);
            BigDecimal r = new BigDecimal(right.scalar);
            return AttributeValue.ofN((isPlus ? l.add(r) : l.subtract(r)).stripTrailingZeros().toPlainString());
        }
        return resolveOperandToken(rhs, item, ctx);
    }

    private static AttributeValue resolveOperandToken(String token, Map<String, AttributeValue> item, ExpressionContext ctx) {
        if (token.startsWith(":")) return ctx.resolveValue(token);
        ItemPath p = ItemPath.parse(token, ctx);
        AttributeValue v = p.get(item);
        if (v == null) throw new DynamoException("ValidationException", "Referenced path does not exist in the item: " + token);
        return v;
    }

    private static void applyRemove(String clause, Map<String, AttributeValue> item, ExpressionContext ctx) {
        for (String pathToken : splitTopLevelCommas(clause)) {
            if (pathToken.isBlank()) continue;
            ItemPath.parse(pathToken, ctx).remove(item);
        }
    }

    private static void applyAdd(String clause, Map<String, AttributeValue> item, ExpressionContext ctx) {
        for (String assignment : splitTopLevelCommas(clause)) {
            String[] toks = assignment.trim().split("\\s+", 2);
            if (toks.length != 2) throw new DynamoException("ValidationException", "Invalid ADD clause: " + assignment);
            ItemPath path = ItemPath.parse(toks[0], ctx);
            AttributeValue delta = ctx.resolveValue(toks[1].trim());
            AttributeValue existing = path.get(item);
            if (delta.type == AttributeValue.Type.N) {
                BigDecimal base = existing != null ? new BigDecimal(existing.scalar) : BigDecimal.ZERO;
                path.set(item, AttributeValue.ofN(base.add(new BigDecimal(delta.scalar)).stripTrailingZeros().toPlainString()));
            } else if (delta.type == AttributeValue.Type.SS || delta.type == AttributeValue.Type.NS || delta.type == AttributeValue.Type.BS) {
                Set<String> union = existing != null ? new LinkedHashSet<>(existing.stringSet) : new LinkedHashSet<>();
                union.addAll(delta.stringSet);
                path.set(item, rebuildSet(delta.type, union));
            } else {
                throw new DynamoException("ValidationException", "ADD only supports N or set (SS/NS/BS) operands");
            }
        }
    }

    private static void applyDelete(String clause, Map<String, AttributeValue> item, ExpressionContext ctx) {
        for (String assignment : splitTopLevelCommas(clause)) {
            String[] toks = assignment.trim().split("\\s+", 2);
            ItemPath path = ItemPath.parse(toks[0], ctx);
            if (toks.length == 1) { path.remove(item); continue; }
            AttributeValue toRemove = ctx.resolveValue(toks[1].trim());
            AttributeValue existing = path.get(item);
            if (existing == null) continue;
            Set<String> remaining = new LinkedHashSet<>(existing.stringSet);
            remaining.removeAll(toRemove.stringSet);
            if (remaining.isEmpty()) path.remove(item);
            else path.set(item, rebuildSet(existing.type, remaining));
        }
    }

    private static AttributeValue rebuildSet(AttributeValue.Type type, Set<String> values) {
        Map<String, AttributeValue> dummy = new LinkedHashMap<>();
        // Reuse AttributeValue.fromJson-free construction via toJson round trip for simplicity.
        com.google.gson.JsonObject obj = new com.google.gson.JsonObject();
        com.google.gson.JsonArray arr = new com.google.gson.JsonArray();
        for (String v : values) arr.add(v);
        obj.add(type.name(), arr);
        return AttributeValue.fromJson(obj);
    }
}
