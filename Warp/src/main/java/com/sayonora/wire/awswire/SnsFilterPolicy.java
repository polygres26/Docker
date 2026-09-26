package com.sayonora.wire.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SNS subscription filter policies. A policy is a JSON object: keys are ANDed, the values of one key (an array) are
 * ORed; an object value is a nested policy (message-body scope only); {@code $or} takes an array of policies. Leaf values
 * are exact strings/numbers/booleans/null or operator objects: {@code prefix}, {@code suffix}, {@code equals-ignore-case},
 * {@code anything-but} (value list or a prefix/suffix object), {@code numeric}, {@code exists}, {@code cidr}.
 * Scope {@code MessageAttributes} evaluates against the message attributes (a {@code String.Array} attribute matches when
 * any element does); scope {@code MessageBody} against the JSON body of the message.
 */
public final class SnsFilterPolicy {

    private SnsFilterPolicy() {
    }

    /** A message attribute as published. */
    public record Attr(String dataType, String stringValue) {
    }

    /** @throws IllegalArgumentException with the AWS-style reason when {@code policy} is not a valid filter policy */
    public static JsonObject parse(String policy, boolean bodyScope) {
        JsonElement e;
        try {
            e = JsonParser.parseString(policy);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid JSON: " + ex.getMessage());
        }
        if (!e.isJsonObject()) {
            throw new IllegalArgumentException("Filter policy must be a JSON object");
        }
        validate(e.getAsJsonObject(), bodyScope, 0);
        return e.getAsJsonObject();
    }

    private static void validate(JsonObject o, boolean nestedAllowed, int depth) {
        if (depth > 5) {
            throw new IllegalArgumentException("Filter policy is too deeply nested");
        }
        for (var en : o.entrySet()) {
            JsonElement v = en.getValue();
            if (en.getKey().equals("$or")) {
                if (!v.isJsonArray()) {
                    throw new IllegalArgumentException("Match value must be an array for $or");
                }
                for (JsonElement sub : v.getAsJsonArray()) {
                    if (!sub.isJsonObject()) {
                        throw new IllegalArgumentException("$or elements must be objects");
                    }
                    validate(sub.getAsJsonObject(), nestedAllowed, depth + 1);
                }
            } else if (v.isJsonObject()) {
                if (!nestedAllowed) {
                    throw new IllegalArgumentException("Filter policy scope MessageAttributes does not support nested filter policy");
                }
                validate(v.getAsJsonObject(), true, depth + 1);
            } else if (v.isJsonArray()) {
                for (JsonElement c : v.getAsJsonArray()) {
                    validateCondition(c);
                }
            } else {
                throw new IllegalArgumentException("Match value must be String, number, true, false, or null");
            }
        }
    }

    private static void validateCondition(JsonElement c) {
        if (c.isJsonPrimitive() || c.isJsonNull()) {
            return;
        }
        if (!c.isJsonObject() || c.getAsJsonObject().size() != 1) {
            throw new IllegalArgumentException("Match value must be String, number, true, false, or null");
        }
        var en = c.getAsJsonObject().entrySet().iterator().next();
        String op = en.getKey();
        JsonElement v = en.getValue();
        switch (op) {
            case "prefix", "suffix", "equals-ignore-case" -> {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isString()) {
                    throw new IllegalArgumentException("Value of " + op + " must be String");
                }
            }
            case "exists" -> {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isBoolean()) {
                    throw new IllegalArgumentException("Value of exists must be true or false");
                }
            }
            case "anything-but" -> {
                if (v.isJsonObject()) {
                    validateCondition(v);
                }
            }
            case "numeric" -> {
                if (!v.isJsonArray() || v.getAsJsonArray().size() < 2 || v.getAsJsonArray().size() % 2 != 0
                        || v.getAsJsonArray().size() > 4) {
                    throw new IllegalArgumentException("Invalid member in numeric match");
                }
                JsonArray a = v.getAsJsonArray();
                for (int i = 0; i < a.size(); i += 2) {
                    String cmp = a.get(i).getAsString();
                    if (!List.of("=", "<", "<=", ">", ">=").contains(cmp) || !a.get(i + 1).getAsJsonPrimitive().isNumber()) {
                        throw new IllegalArgumentException("Invalid member in numeric match: " + cmp);
                    }
                }
            }
            case "cidr" -> {
                if (!v.isJsonPrimitive()) {
                    throw new IllegalArgumentException("Value of cidr must be String");
                }
            }
            default -> throw new IllegalArgumentException("Unrecognized match type " + op);
        }
    }

    // ------------------------------------------------------------------------------------------------ matching

    public static boolean matchesAttributes(JsonObject policy, Map<String, Attr> attrs) {
        return matchObject(policy, key -> {
            Attr a = attrs.get(key);
            return a == null ? null : attrValues(a);
        }, false);
    }

    public static boolean matchesBody(JsonObject policy, String body) {
        JsonElement b;
        try {
            b = JsonParser.parseString(body);
        } catch (RuntimeException e) {
            return false;
        }
        return b.isJsonObject() && matchBody(policy, b.getAsJsonObject());
    }

    private static List<JsonElement> attrValues(Attr a) {
        List<JsonElement> out = new ArrayList<>();
        String t = a.dataType() == null ? "String" : a.dataType();
        if (t.startsWith("Number")) {
            try {
                out.add(new JsonPrimitive(new BigDecimal(a.stringValue().trim())));
            } catch (RuntimeException e) {
                // a non-numeric Number attribute matches nothing
            }
        } else if (t.equals("String.Array")) {
            try {
                for (JsonElement e : JsonParser.parseString(a.stringValue()).getAsJsonArray()) {
                    out.add(e);
                }
            } catch (RuntimeException e) {
                // malformed array matches nothing
            }
        } else if (t.startsWith("String")) {
            out.add(new JsonPrimitive(a.stringValue()));
        }
        return out;
    }

    private interface Lookup {
        /** values of the key, or null when absent */
        List<JsonElement> get(String key);
    }

    private static boolean matchObject(JsonObject policy, Lookup lookup, boolean body) {
        for (var en : policy.entrySet()) {
            if (en.getKey().equals("$or")) {
                boolean any = false;
                for (JsonElement sub : en.getValue().getAsJsonArray()) {
                    if (matchObject(sub.getAsJsonObject(), lookup, body)) {
                        any = true;
                        break;
                    }
                }
                if (!any) {
                    return false;
                }
                continue;
            }
            if (!en.getValue().isJsonArray()) {
                return false;
            }
            if (!matchConditions(en.getValue().getAsJsonArray(), lookup.get(en.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchBody(JsonObject policy, JsonObject body) {
        for (var en : policy.entrySet()) {
            if (en.getKey().equals("$or")) {
                boolean any = false;
                for (JsonElement sub : en.getValue().getAsJsonArray()) {
                    if (matchBody(sub.getAsJsonObject(), body)) {
                        any = true;
                        break;
                    }
                }
                if (!any) {
                    return false;
                }
                continue;
            }
            JsonElement v = body.get(en.getKey());
            JsonElement cond = en.getValue();
            if (cond.isJsonObject()) {
                if (v == null || !nestedMatches(cond.getAsJsonObject(), v)) {
                    return false;
                }
            } else {
                List<JsonElement> values = null;
                if (v != null) {
                    values = new ArrayList<>();
                    if (v.isJsonArray()) {
                        for (JsonElement x : v.getAsJsonArray()) {
                            values.add(x);
                        }
                    } else {
                        values.add(v);
                    }
                }
                if (!matchConditions(cond.getAsJsonArray(), values)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean nestedMatches(JsonObject policy, JsonElement v) {
        if (v.isJsonObject()) {
            return matchBody(policy, v.getAsJsonObject());
        }
        if (v.isJsonArray()) {
            for (JsonElement x : v.getAsJsonArray()) {
                if (nestedMatches(policy, x)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Conditions are ORed against the key's values; {@code values == null} means the key is absent. */
    private static boolean matchConditions(JsonArray conds, List<JsonElement> values) {
        for (JsonElement c : conds) {
            if (c.isJsonObject() && c.getAsJsonObject().has("exists")) {
                boolean want = c.getAsJsonObject().get("exists").getAsBoolean();
                if (want == (values != null)) {
                    return true;
                }
                continue;
            }
            if (values == null) {
                continue;
            }
            if (c.isJsonObject() && c.getAsJsonObject().has("anything-but")) {
                boolean all = !values.isEmpty();
                for (JsonElement v : values) {
                    if (anythingButHits(c.getAsJsonObject().get("anything-but"), v)) {
                        all = false;
                        break;
                    }
                }
                if (all) {
                    return true;
                }
                continue;
            }
            for (JsonElement v : values) {
                if (leafMatches(c, v)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean anythingButHits(JsonElement spec, JsonElement v) {
        if (spec.isJsonArray()) {
            for (JsonElement s : spec.getAsJsonArray()) {
                if (equalsLeaf(s, v)) {
                    return true;
                }
            }
            return false;
        }
        if (spec.isJsonObject()) {
            return leafMatches(spec, v);
        }
        return equalsLeaf(spec, v);
    }

    private static boolean equalsLeaf(JsonElement cond, JsonElement v) {
        if (cond.isJsonNull() || v.isJsonNull()) {
            return cond.isJsonNull() && v.isJsonNull();
        }
        if (!cond.isJsonPrimitive() || !v.isJsonPrimitive()) {
            return false;
        }
        JsonPrimitive a = cond.getAsJsonPrimitive(), b = v.getAsJsonPrimitive();
        if (a.isNumber() && b.isNumber()) {
            return a.getAsBigDecimal().compareTo(b.getAsBigDecimal()) == 0;
        }
        if (a.isBoolean() && b.isBoolean()) {
            return a.getAsBoolean() == b.getAsBoolean();
        }
        if (a.isString() && b.isString()) {
            return a.getAsString().equals(b.getAsString());
        }
        return false;
    }

    private static boolean leafMatches(JsonElement cond, JsonElement v) {
        if (!cond.isJsonObject()) {
            return equalsLeaf(cond, v);
        }
        var en = cond.getAsJsonObject().entrySet().iterator().next();
        JsonElement arg = en.getValue();
        switch (en.getKey()) {
            case "prefix":
                return isString(v) && v.getAsString().startsWith(arg.getAsString());
            case "suffix":
                return isString(v) && v.getAsString().endsWith(arg.getAsString());
            case "equals-ignore-case":
                return isString(v) && v.getAsString().equalsIgnoreCase(arg.getAsString());
            case "numeric": {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isNumber()) {
                    return false;
                }
                BigDecimal x = v.getAsBigDecimal();
                JsonArray a = arg.getAsJsonArray();
                for (int i = 0; i < a.size(); i += 2) {
                    int cmp = x.compareTo(a.get(i + 1).getAsBigDecimal());
                    boolean ok = switch (a.get(i).getAsString()) {
                        case "=" -> cmp == 0;
                        case "<" -> cmp < 0;
                        case "<=" -> cmp <= 0;
                        case ">" -> cmp > 0;
                        default -> cmp >= 0;
                    };
                    if (!ok) {
                        return false;
                    }
                }
                return true;
            }
            case "cidr":
                return isString(v) && cidr(arg.getAsString(), v.getAsString());
            default:
                return false;
        }
    }

    private static boolean isString(JsonElement v) {
        return v.isJsonPrimitive() && v.getAsJsonPrimitive().isString();
    }

    private static boolean cidr(String cidr, String ip) {
        try {
            String[] parts = cidr.split("/");
            byte[] net = InetAddress.getByName(parts[0]).getAddress();
            byte[] addr = InetAddress.getByName(ip).getAddress();
            if (net.length != addr.length) {
                return false;
            }
            int bits = parts.length > 1 ? Integer.parseInt(parts[1]) : net.length * 8;
            for (int i = 0; i < bits; i++) {
                int mask = 0x80 >> (i % 8);
                if ((net[i / 8] & mask) != (addr[i / 8] & mask)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }
}
