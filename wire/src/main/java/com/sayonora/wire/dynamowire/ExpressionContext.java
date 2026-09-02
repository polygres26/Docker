package com.sayonora.wire.dynamowire;

import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

public final class ExpressionContext {

    public final Map<String, String> names = new HashMap<>();
    public final Map<String, AttributeValue> values = new HashMap<>();

    public static ExpressionContext parse(JsonObject request) {
        ExpressionContext ctx = new ExpressionContext();
        if (request.has("ExpressionAttributeNames") && !request.get("ExpressionAttributeNames").isJsonNull()) {
            for (var e : request.getAsJsonObject("ExpressionAttributeNames").entrySet()) {
                ctx.names.put(e.getKey(), e.getValue().getAsString());
            }
        }
        if (request.has("ExpressionAttributeValues") && !request.get("ExpressionAttributeValues").isJsonNull()) {
            for (var e : request.getAsJsonObject("ExpressionAttributeValues").entrySet()) {
                ctx.values.put(e.getKey(), AttributeValue.fromJson(e.getValue()));
            }
        }
        return ctx;
    }

    public String resolveName(String token) {
        if (token.startsWith("#")) {
            String real = names.get(token);
            if (real == null) throw new DynamoException("ValidationException", "ExpressionAttributeNames missing entry for " + token);
            return real;
        }
        return token;
    }

    public AttributeValue resolveValue(String token) {
        if (!token.startsWith(":")) throw new DynamoException("ValidationException", "Expected a value placeholder, got " + token);
        AttributeValue v = values.get(token);
        if (v == null) throw new DynamoException("ValidationException", "ExpressionAttributeValues missing entry for " + token);
        return v;
    }
}
