package com.nexagres.wire.dynamowire;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ConditionExpressionEvaluator {

    private final String expr;
    private int pos;
    private final Map<String, AttributeValue> item;
    private final ExpressionContext ctx;

    private ConditionExpressionEvaluator(String expr, Map<String, AttributeValue> item, ExpressionContext ctx) {
        this.expr = expr;
        this.item = item;
        this.ctx = ctx;
    }

    public static boolean evaluate(String expr, Map<String, AttributeValue> item, ExpressionContext ctx) {
        if (expr == null || expr.isBlank()) return true;
        ConditionExpressionEvaluator ev = new ConditionExpressionEvaluator(expr.trim(), item, ctx);
        boolean result = ev.parseOr();
        ev.skipWs();
        if (ev.pos != ev.expr.length()) {
            throw new DynamoException("ValidationException", "Unexpected trailing content in expression: " + expr.substring(ev.pos));
        }
        return result;
    }

    private boolean parseOr() {
        boolean v = parseAnd();
        skipWs();
        while (matchKeyword("OR")) {
            boolean rhs = parseAnd();
            v = v || rhs;
            skipWs();
        }
        return v;
    }

    private boolean parseAnd() {
        boolean v = parseNot();
        skipWs();
        while (matchKeyword("AND")) {
            boolean rhs = parseNot();
            v = v && rhs;
            skipWs();
        }
        return v;
    }

    private boolean parseNot() {
        skipWs();
        if (matchKeyword("NOT")) return !parseNot();
        return parseAtom();
    }

    private boolean parseAtom() {
        skipWs();
        if (peek() == '(') {
            pos++;
            boolean v = parseOr();
            skipWs();
            expect(')');
            return v;
        }
        String funcName = tryParseIdentifier();
        if (funcName != null) {
            skipWs();
            if (peek() == '(') {
                return parseFunctionCall(funcName);
            }
            
            return parseComparisonFrom(funcName);
        }
        throw new DynamoException("ValidationException", "Expected expression at position " + pos + " in: " + expr);
    }

    private boolean parseFunctionCall(String funcName) {
        pos++;
        List<String> args = parseArgList();
        switch (funcName) {
            case "attribute_exists" -> {
                return ItemPath.parse(args.get(0), ctx).get(item == null ? Map.of() : item) != null;
            }
            case "attribute_not_exists" -> {
                return ItemPath.parse(args.get(0), ctx).get(item == null ? Map.of() : item) == null;
            }
            case "begins_with" -> {
                AttributeValue v = resolveOperand(args.get(0));
                AttributeValue prefix = resolveOperand(args.get(1));
                return v != null && v.type == AttributeValue.Type.S && v.scalar.startsWith(prefix.scalar);
            }
            case "contains" -> {
                AttributeValue v = resolveOperand(args.get(0));
                AttributeValue needle = resolveOperand(args.get(1));
                if (v == null) return false;
                if (v.type == AttributeValue.Type.S) return v.scalar.contains(needle.scalar);
                if (v.type == AttributeValue.Type.SS || v.type == AttributeValue.Type.NS || v.type == AttributeValue.Type.BS) {
                    return v.stringSet.contains(needle.scalar);
                }
                if (v.type == AttributeValue.Type.L) {
                    for (AttributeValue el : v.list) if (el.deepEquals(needle)) return true;
                    return false;
                }
                return false;
            }
            default -> throw new DynamoException("ValidationException", "Unsupported function: " + funcName);
        }
    }

    private List<String> parseArgList() {
        List<String> args = new java.util.ArrayList<>();
        int depth = 1;
        StringBuilder cur = new StringBuilder();
        while (depth > 0) {
            char c = expr.charAt(pos++);
            if (c == '(') { depth++; cur.append(c); }
            else if (c == ')') { depth--; if (depth > 0) cur.append(c); }
            else if (c == ',' && depth == 1) { args.add(cur.toString().trim()); cur.setLength(0); }
            else cur.append(c);
        }
        if (!cur.toString().isBlank() || !args.isEmpty()) args.add(cur.toString().trim());
        return args;
    }

    private boolean parseComparisonFrom(String pathToken) {
        skipWs();
        StringBuilder opBuf = new StringBuilder();
        while (pos < expr.length() && "<>=!".indexOf(expr.charAt(pos)) >= 0) { opBuf.append(expr.charAt(pos)); pos++; }
        String op = opBuf.toString();
        if (!op.isEmpty()) {
            AttributeValue left = resolveOperand(pathToken);
            skipWs();
            String rhsToken = readOperandToken();
            AttributeValue right = resolveOperand(rhsToken);
            return compare(op, left, right);
        }
        
        int save = pos;
        skipWs();
        if (matchKeyword("BETWEEN")) {
            AttributeValue left = resolveOperand(pathToken);
            skipWs();
            String lowTok = readOperandToken();
            skipWs();
            if (!matchKeyword("AND")) throw new DynamoException("ValidationException", "Expected AND in BETWEEN");
            skipWs();
            String highTok = readOperandToken();
            AttributeValue low = resolveOperand(lowTok);
            AttributeValue high = resolveOperand(highTok);
            if (left == null) return false;
            return left.compareTo(low) >= 0 && left.compareTo(high) <= 0;
        }
        pos = save;
        skipWs();
        if (matchKeyword("IN")) {
            AttributeValue left = resolveOperand(pathToken);
            skipWs();
            expect('(');
            List<String> args = new java.util.ArrayList<>();
            int depth = 1;
            StringBuilder cur = new StringBuilder();
            while (depth > 0) {
                char c = expr.charAt(pos++);
                if (c == '(') { depth++; cur.append(c); }
                else if (c == ')') { depth--; if (depth > 0) cur.append(c); }
                else if (c == ',' && depth == 1) { args.add(cur.toString().trim()); cur.setLength(0); }
                else cur.append(c);
            }
            if (!cur.toString().isBlank()) args.add(cur.toString().trim());
            if (left == null) return false;
            for (String a : args) if (left.deepEquals(resolveOperand(a))) return true;
            return false;
        }
        throw new DynamoException("ValidationException", "Expected a comparison operator after " + pathToken);
    }

    private boolean compare(String op, AttributeValue left, AttributeValue right) {
        if (left == null) return false;
        int c = left.compareTo(right);
        return switch (op) {
            case "=" -> left.deepEquals(right);
            case "<>" -> !left.deepEquals(right);
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            case ">" -> c > 0;
            case ">=" -> c >= 0;
            default -> throw new DynamoException("ValidationException", "Unknown operator: " + op);
        };
    }

    private AttributeValue resolveOperand(String token) {
        token = token.trim();
        if (token.startsWith(":")) return ctx.resolveValue(token);
        return ItemPath.parse(token, ctx).get(item == null ? Map.of() : item);
    }

    private String readOperandToken() {
        skipWs();
        int start = pos;
        while (pos < expr.length() && " ()<>=!,".indexOf(expr.charAt(pos)) < 0) pos++;
        return expr.substring(start, pos).trim();
    }

    private String tryParseIdentifier() {
        skipWs();
        int start = pos;
        while (pos < expr.length()) {
            char c = expr.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '#' || c == ':' || c == '.' || c == '[' || c == ']') pos++;
            else break;
        }
        if (pos == start) return null;
        return expr.substring(start, pos);
    }

    private boolean matchKeyword(String kw) {
        skipWs();
        if (expr.regionMatches(pos, kw, 0, kw.length())) {
            int end = pos + kw.length();
            boolean boundaryOk = end == expr.length() || !Character.isLetterOrDigit(expr.charAt(end));
            if (boundaryOk) { pos = end; return true; }
        }
        return false;
    }

    private void skipWs() {
        while (pos < expr.length() && Character.isWhitespace(expr.charAt(pos))) pos++;
    }

    private char peek() {
        return pos < expr.length() ? expr.charAt(pos) : '\0';
    }

    private void expect(char c) {
        skipWs();
        if (pos >= expr.length() || expr.charAt(pos) != c) {
            throw new DynamoException("ValidationException", "Expected '" + c + "' at position " + pos);
        }
        pos++;
    }
}
