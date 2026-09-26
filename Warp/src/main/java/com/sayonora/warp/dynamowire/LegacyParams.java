package com.sayonora.warp.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DynamoDB's pre-expression ("legacy") request parameters -- AttributesToGet, Expected,
 * ConditionalOperator, AttributeUpdates, QueryFilter/ScanFilter, KeyConditions -- translated into
 * the same {@link Expr} nodes the expression parser produces, so one evaluator serves both.
 */
final class LegacyParams {

    private LegacyParams() {}

    static boolean has(JsonObject o, String field) {
        return o.has(field) && !o.get(field).isJsonNull();
    }

    /**
     * DynamoDB refuses a request that mixes the two styles.
     * @param legacy legacy parameter names, @param expression expression parameter names
     */
    static void rejectMixing(JsonObject req, List<String> legacy, List<String> expression) {
        List<String> l = new ArrayList<>(), e = new ArrayList<>();
        for (String n : legacy) if (has(req, n)) l.add(n);
        for (String n : expression) if (has(req, n)) e.add(n);
        if (!l.isEmpty() && !e.isEmpty()) {
            throw DynamoException.validation("Can not use both expression and non-expression parameters in the same request: "
                    + "Non-expression parameters: {" + String.join(", ", l) + "} Expression parameters: {"
                    + String.join(", ", e) + "}");
        }
    }

    // ------------------------------------------------------------------------- attributes to get

    static List<Expr.Path> attributesToGet(JsonObject req) {
        if (!has(req, "AttributesToGet")) return null;
        JsonArray a = req.getAsJsonArray("AttributesToGet");
        if (a.isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'attributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        List<Expr.Path> paths = new ArrayList<>();
        Set<String> seen = new java.util.HashSet<>();
        for (JsonElement e : a) {
            String n = e.getAsString();
            if (!seen.add(n)) {
                throw DynamoException.validation("One or more parameter values were invalid: Duplicate value in attribute name: " + n);
            }
            paths.add(Expr.Path.of(n));
        }
        return paths;
    }

    // ------------------------------------------------------------------------- Condition maps

    private static Expr.Cond forOperator(String attr, String op, List<AttributeValue> args) {
        Expr.PathOp path = new Expr.PathOp(Expr.Path.of(attr));
        int n = args.size();
        switch (op) {
            case "EQ", "NE", "LE", "LT", "GE", "GT": {
                need(op, n, 1);
                AttributeValue v = args.get(0);
                if (!op.equals("EQ") && !op.equals("NE") && !v.isScalarKeyType()) {
                    throw DynamoException.validation("One or more parameter values were invalid: ComparisonOperator " + op
                            + " is not valid for " + v.type + " AttributeValue type");
                }
                String cmp = switch (op) {
                    case "EQ" -> "=";
                    case "NE" -> "<>";
                    case "LE" -> "<=";
                    case "LT" -> "<";
                    case "GE" -> ">=";
                    default -> ">";
                };
                return new Expr.Cmp(cmp, path, new Expr.ValueOp(v));
            }
            case "NOT_NULL":
                need(op, n, 0);
                return new Expr.Fn("attribute_exists", List.of(path));
            case "NULL":
                need(op, n, 0);
                return new Expr.Fn("attribute_not_exists", List.of(path));
            case "CONTAINS":
                need(op, n, 1);
                return new Expr.Fn("contains", List.of(path, new Expr.ValueOp(args.get(0))));
            case "NOT_CONTAINS":
                need(op, n, 1);
                return new Expr.Not(new Expr.Fn("contains", List.of(path, new Expr.ValueOp(args.get(0)))));
            case "BEGINS_WITH":
                need(op, n, 1);
                return new Expr.Fn("begins_with", List.of(path, new Expr.ValueOp(args.get(0))));
            case "IN": {
                if (n < 1) need(op, n, 1);
                List<Expr.Operand> c = new ArrayList<>();
                for (AttributeValue v : args) c.add(new Expr.ValueOp(v));
                return new Expr.In(path, c);
            }
            case "BETWEEN":
                need(op, n, 2);
                return new Expr.Between(path, new Expr.ValueOp(args.get(0)), new Expr.ValueOp(args.get(1)));
            default:
                throw DynamoException.validation("1 validation error detected: Value '" + op + "' at 'comparisonOperator' failed to satisfy constraint: "
                        + "Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]");
        }
    }

    private static void need(String op, int actual, int expected) {
        if (actual != expected) {
            throw DynamoException.validation("One or more parameter values were invalid: Invalid number of argument(s) for the "
                    + op + " ComparisonOperator");
        }
    }

    private static List<AttributeValue> values(JsonObject cond) {
        List<AttributeValue> out = new ArrayList<>();
        if (has(cond, "AttributeValueList")) {
            for (JsonElement e : cond.getAsJsonArray("AttributeValueList")) out.add(AttributeValue.fromJson(e));
        }
        return out;
    }

    private static Expr.Cond combine(List<Expr.Cond> parts, String conditionalOperator) {
        if (parts.isEmpty()) return null;
        if (parts.size() == 1) return parts.get(0);
        return "OR".equals(conditionalOperator) ? new Expr.Or(parts) : new Expr.And(parts);
    }

    static void checkConditionalOperator(String op) {
        if (op != null && !op.equals("AND") && !op.equals("OR")) {
            throw DynamoException.validation("1 validation error detected: Value '" + op
                    + "' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]");
        }
    }

    /** QueryFilter / ScanFilter. */
    static Expr.Cond filterMap(JsonObject req, String field, String conditionalOperator) {
        if (!has(req, field)) return null;
        List<Expr.Cond> parts = new ArrayList<>();
        for (Map.Entry<String, JsonElement> e : req.getAsJsonObject(field).entrySet()) {
            JsonObject cond = e.getValue().getAsJsonObject();
            parts.add(forOperator(e.getKey(), cond.get("ComparisonOperator").getAsString(), values(cond)));
        }
        return combine(parts, conditionalOperator);
    }

    /** Expected (legacy conditional-write parameters). */
    static Expr.Cond expected(JsonObject req, String conditionalOperator) {
        if (!has(req, "Expected")) return null;
        List<Expr.Cond> parts = new ArrayList<>();
        for (Map.Entry<String, JsonElement> e : req.getAsJsonObject("Expected").entrySet()) {
            JsonObject exp = e.getValue().getAsJsonObject();
            String attr = e.getKey();
            boolean hasExists = has(exp, "Exists"), hasValue = has(exp, "Value"), hasOp = has(exp, "ComparisonOperator"),
                    hasList = has(exp, "AttributeValueList");
            if (hasExists && (hasOp || hasList)) {
                throw DynamoException.validation("One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: " + attr);
            }
            if (hasOp) {
                if (hasValue) {
                    throw DynamoException.validation("One or more parameter values were invalid: Value and ComparisonOperator cannot be used together for Attribute: " + attr);
                }
                parts.add(forOperator(attr, exp.get("ComparisonOperator").getAsString(), values(exp)));
                continue;
            }
            if (hasList) {
                throw DynamoException.validation("One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: " + attr);
            }
            boolean exists = !hasExists || exp.get("Exists").getAsBoolean();
            Expr.PathOp path = new Expr.PathOp(Expr.Path.of(attr));
            if (exists) {
                if (!hasValue) {
                    throw DynamoException.validation("One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: " + attr);
                }
                parts.add(new Expr.Cmp("=", path, new Expr.ValueOp(AttributeValue.fromJson(exp.get("Value")))));
            } else {
                if (hasValue) {
                    throw DynamoException.validation("One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: " + attr);
                }
                parts.add(new Expr.Fn("attribute_not_exists", List.of(path)));
            }
        }
        return combine(parts, conditionalOperator);
    }

    /** KeyConditions (legacy Query key conditions). */
    static List<Expr.KeyCond> keyConditions(JsonObject req) {
        List<Expr.KeyCond> out = new ArrayList<>();
        for (Map.Entry<String, JsonElement> e : req.getAsJsonObject("KeyConditions").entrySet()) {
            JsonObject cond = e.getValue().getAsJsonObject();
            String op = cond.get("ComparisonOperator").getAsString();
            List<AttributeValue> v = values(cond);
            switch (op) {
                case "EQ", "LE", "LT", "GE", "GT", "BEGINS_WITH" -> {
                    if (v.size() != 1) need(op, v.size(), 1);
                    out.add(new Expr.KeyCond(e.getKey(), op, v.get(0), null));
                }
                case "BETWEEN" -> {
                    if (v.size() != 2) need(op, v.size(), 2);
                    out.add(new Expr.KeyCond(e.getKey(), op, v.get(0), v.get(1)));
                }
                default -> throw DynamoException.validation("One or more parameter values were invalid: Unsupported operator on KeyCondition: " + op);
            }
        }
        return out;
    }

    /** AttributeUpdates -> an update plan. */
    static Expr.UpdatePlan attributeUpdates(JsonObject req) {
        List<Expr.UpdateAction> actions = new ArrayList<>();
        for (Map.Entry<String, JsonElement> e : req.getAsJsonObject("AttributeUpdates").entrySet()) {
            JsonObject u = e.getValue().getAsJsonObject();
            String action = has(u, "Action") ? u.get("Action").getAsString() : "PUT";
            Expr.Path path = Expr.Path.of(e.getKey());
            AttributeValue value = has(u, "Value") ? AttributeValue.fromJson(u.get("Value")) : null;
            switch (action) {
                case "PUT" -> {
                    if (value == null) {
                        throw DynamoException.validation("One or more parameter values were invalid: Only DELETE action is allowed when no attribute value is specified");
                    }
                    actions.add(new Expr.UpdateAction(Expr.ActionKind.SET, path, new Expr.ValueOp(value)));
                }
                case "ADD" -> {
                    if (value == null) {
                        throw DynamoException.validation("One or more parameter values were invalid: Only DELETE action is allowed when no attribute value is specified");
                    }
                    actions.add(new Expr.UpdateAction(Expr.ActionKind.ADD, path, new Expr.ValueOp(value)));
                }
                case "DELETE" -> {
                    if (value == null) {
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.REMOVE, path, null));
                    } else {
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.DELETE, path, new Expr.ValueOp(value)));
                    }
                }
                default -> throw DynamoException.validation("1 validation error detected: Value '" + action
                        + "' at 'attributeUpdates' failed to satisfy constraint: Member must satisfy enum value set: [ADD, PUT, DELETE]");
            }
        }
        return new Expr.UpdatePlan(actions);
    }
}
