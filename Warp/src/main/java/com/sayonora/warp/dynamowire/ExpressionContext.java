package com.sayonora.warp.dynamowire;

import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The ExpressionAttributeNames / ExpressionAttributeValues of one request, plus the bookkeeping
 * DynamoDB does over them: every {@code #name} / {@code :value} an expression mentions must be
 * defined, and every entry that was supplied must be used by some expression of the request.
 */
public final class ExpressionContext {

    public final Map<String, String> names = new HashMap<>();
    public final Map<String, AttributeValue> values = new HashMap<>();

    private final Set<String> usedNames = new LinkedHashSet<>();
    private final Set<String> usedValues = new LinkedHashSet<>();
    /** Which expression is being parsed, for the "Invalid ConditionExpression: ..." prefixes. */
    String currentExpression = "ConditionExpression";

    public static ExpressionContext parse(JsonObject request) {
        ExpressionContext ctx = new ExpressionContext();
        if (request.has("ExpressionAttributeNames") && !request.get("ExpressionAttributeNames").isJsonNull()) {
            JsonObject n = request.getAsJsonObject("ExpressionAttributeNames");
            if (n.entrySet().isEmpty()) {
                throw DynamoException.validation("ExpressionAttributeNames must not be empty");
            }
            for (var e : n.entrySet()) {
                if (!e.getKey().startsWith("#") || e.getKey().length() < 2) {
                    throw DynamoException.validation("ExpressionAttributeNames contains invalid key: Syntax error; key: \""
                            + e.getKey() + "\"");
                }
                ctx.names.put(e.getKey(), e.getValue().getAsString());
            }
        }
        if (request.has("ExpressionAttributeValues") && !request.get("ExpressionAttributeValues").isJsonNull()) {
            JsonObject v = request.getAsJsonObject("ExpressionAttributeValues");
            if (v.entrySet().isEmpty()) {
                throw DynamoException.validation("ExpressionAttributeValues must not be empty");
            }
            for (var e : v.entrySet()) {
                if (!e.getKey().startsWith(":") || e.getKey().length() < 2) {
                    throw DynamoException.validation("ExpressionAttributeValues contains invalid key: Syntax error; key: \""
                            + e.getKey() + "\"");
                }
                ctx.values.put(e.getKey(), AttributeValue.fromJson(e.getValue()));
            }
        }
        return ctx;
    }

    public String resolveName(String token) {
        if (token.startsWith("#")) {
            String real = names.get(token);
            if (real == null) {
                throw DynamoException.validation("Invalid " + currentExpression
                        + ": An expression attribute name used in the document path is not defined; attribute name: " + token);
            }
            usedNames.add(token);
            return real;
        }
        return token;
    }

    public AttributeValue resolveValue(String token) {
        if (!token.startsWith(":")) {
            throw DynamoException.validation("Expected a value placeholder, got " + token);
        }
        AttributeValue v = values.get(token);
        if (v == null) {
            throw DynamoException.validation("Invalid " + currentExpression
                    + ": An expression attribute value used in expression is not defined; attribute value: " + token);
        }
        usedValues.add(token);
        return v;
    }

    /** Called once all of a request's expressions have been parsed. */
    public void checkUnused(String operationExpressionHint) {
        if (!usedExpressions && (!names.isEmpty() || !values.isEmpty())) {
            if (!names.isEmpty()) {
                throw DynamoException.validation("ExpressionAttributeNames can only be specified when using expressions");
            }
            throw DynamoException.validation("ExpressionAttributeValues can only be specified when using expressions: "
                    + operationExpressionHint + " is null");
        }
        Set<String> unusedNames = new TreeSet<>(names.keySet());
        unusedNames.removeAll(usedNames);
        if (!unusedNames.isEmpty()) {
            throw DynamoException.validation("Value provided in ExpressionAttributeNames unused in expressions: keys: {"
                    + String.join(", ", unusedNames) + "}");
        }
        Set<String> unusedValues = new TreeSet<>(values.keySet());
        unusedValues.removeAll(usedValues);
        if (!unusedValues.isEmpty()) {
            throw DynamoException.validation("Value provided in ExpressionAttributeValues unused in expressions: keys: {"
                    + String.join(", ", unusedValues) + "}");
        }
    }

    private boolean usedExpressions;

    void markExpressionUsed() {
        usedExpressions = true;
    }

    public boolean isEmpty() {
        return names.isEmpty() && values.isEmpty();
    }
}
