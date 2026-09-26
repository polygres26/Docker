package com.sayonora.warp.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Partial document update (Cosmos "patch item"): JSON Patch operations add, set, replace, remove, incr and move over /-separated
 * paths ({@code /tags/-} appends to an array). At most {@value #MAX_OPS} operations per request. Works on a deep copy.
 */
final class CosmosPatch {

    static final int MAX_OPS = 10;

    private CosmosPatch() {
    }

    /** Applies {@code ops} (a JSON array) to a copy of {@code doc} and returns the copy. */
    static JsonObject apply(JsonObject doc, JsonArray ops) {
        if (ops == null || ops.isEmpty()) {
            throw CosmosException.badRequest("The patch request must contain at least one operation.");
        }
        if (ops.size() > MAX_OPS) {
            throw CosmosException.badRequest("Patch request has too many operations: at most " + MAX_OPS + " are allowed, got " + ops.size() + ".");
        }
        JsonObject d = doc.deepCopy();
        for (JsonElement e : ops) {
            if (!e.isJsonObject()) {
                throw CosmosException.badRequest("Each patch operation must be an object.");
            }
            JsonObject op = e.getAsJsonObject();
            String name = str(op, "op");
            String path = str(op, "path");
            if (name == null || path == null) {
                throw CosmosException.badRequest("A patch operation needs 'op' and 'path'.");
            }
            switch (name.toLowerCase(Locale.ROOT)) {
                case "add" -> put(d, path, need(op), true, false);
                case "set" -> put(d, path, need(op), true, true);
                case "replace" -> replace(d, path, need(op));
                case "remove" -> remove(d, path);
                case "incr" -> incr(d, path, need(op));
                case "move" -> {
                    String from = str(op, "from");
                    if (from == null) {
                        throw CosmosException.badRequest("A move operation needs 'from'.");
                    }
                    JsonElement v = get(d, from);
                    if (v == null) {
                        throw CosmosException.badRequest("Move source path '" + from + "' does not exist.");
                    }
                    JsonElement copy = v.deepCopy();
                    remove(d, from);
                    put(d, path, copy, true, false);
                }
                default -> throw CosmosException.badRequest("Unknown patch operation '" + name + "'; expected add, set, replace, remove, incr or move.");
            }
        }
        return d;
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) && o.get(k).isJsonPrimitive() ? o.get(k).getAsString() : null;
    }

    private static JsonElement need(JsonObject op) {
        if (!op.has("value")) {
            throw CosmosException.badRequest("The '" + str(op, "op") + "' operation needs a 'value'.");
        }
        return op.get("value");
    }

    static List<String> tokens(String path) {
        if (!path.startsWith("/")) {
            throw CosmosException.badRequest("Patch path '" + path + "' must start with '/'.");
        }
        List<String> out = new ArrayList<>();
        for (String s : path.substring(1).split("/", -1)) {
            out.add(s.replace("~1", "/").replace("~0", "~"));
        }
        if (out.size() == 1 && out.get(0).isEmpty()) {
            throw CosmosException.badRequest("Patch path '" + path + "' is not valid.");
        }
        return out;
    }

    private static JsonElement child(JsonElement parent, String tok) {
        if (parent.isJsonObject()) {
            return parent.getAsJsonObject().get(tok);
        }
        if (parent.isJsonArray()) {
            int i = index(tok, parent.getAsJsonArray().size(), false);
            return i < 0 ? null : parent.getAsJsonArray().get(i);
        }
        return null;
    }

    private static int index(String tok, int size, boolean allowEnd) {
        try {
            int i = Integer.parseInt(tok);
            if (i < 0 || i > size || i == size && !allowEnd) {
                return -1;
            }
            return i;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static JsonElement get(JsonObject doc, String path) {
        JsonElement cur = doc;
        for (String t : tokens(path)) {
            cur = cur == null ? null : child(cur, t);
            if (cur == null) {
                return null;
            }
        }
        return cur;
    }

    private static JsonElement parentOf(JsonObject doc, List<String> toks, String path) {
        JsonElement cur = doc;
        for (int i = 0; i < toks.size() - 1; i++) {
            cur = child(cur, toks.get(i));
            if (cur == null || !cur.isJsonObject() && !cur.isJsonArray()) {
                throw CosmosException.badRequest("Patch path '" + path + "' does not exist: the parent of the target is missing.");
            }
        }
        return cur;
    }

    private static void put(JsonObject doc, String path, JsonElement v, boolean createProp, boolean overwriteElement) {
        List<String> toks = tokens(path);
        JsonElement parent = parentOf(doc, toks, path);
        String last = toks.get(toks.size() - 1);
        if (parent.isJsonObject()) {
            parent.getAsJsonObject().add(last, v);
        } else {
            JsonArray a = parent.getAsJsonArray();
            if (last.equals("-")) {
                a.add(v);
                return;
            }
            int i = index(last, a.size(), true);
            if (i < 0) {
                throw CosmosException.badRequest("Patch path '" + path + "': array index out of range.");
            }
            if (overwriteElement && i < a.size()) {
                a.set(i, v);
            } else {
                // JsonArray has no insert: rebuild
                List<JsonElement> l = new ArrayList<>();
                a.forEach(l::add);
                l.add(i, v);
                while (a.size() > 0) {
                    a.remove(a.size() - 1);
                }
                l.forEach(a::add);
            }
        }
    }

    private static void replace(JsonObject doc, String path, JsonElement v) {
        List<String> toks = tokens(path);
        JsonElement parent = parentOf(doc, toks, path);
        String last = toks.get(toks.size() - 1);
        if (parent.isJsonObject()) {
            if (!parent.getAsJsonObject().has(last)) {
                throw CosmosException.badRequest("Patch replace: path '" + path + "' does not exist.");
            }
            parent.getAsJsonObject().add(last, v);
        } else {
            int i = index(last, parent.getAsJsonArray().size(), false);
            if (i < 0) {
                throw CosmosException.badRequest("Patch replace: path '" + path + "' does not exist.");
            }
            parent.getAsJsonArray().set(i, v);
        }
    }

    private static void remove(JsonObject doc, String path) {
        List<String> toks = tokens(path);
        JsonElement parent = parentOf(doc, toks, path);
        String last = toks.get(toks.size() - 1);
        if (parent.isJsonObject()) {
            if (parent.getAsJsonObject().remove(last) == null) {
                throw CosmosException.badRequest("Patch remove: path '" + path + "' does not exist.");
            }
        } else {
            int i = index(last, parent.getAsJsonArray().size(), false);
            if (i < 0) {
                throw CosmosException.badRequest("Patch remove: path '" + path + "' does not exist.");
            }
            parent.getAsJsonArray().remove(i);
        }
    }

    private static void incr(JsonObject doc, String path, JsonElement v) {
        if (!CosmosJson.isNum(v)) {
            throw CosmosException.badRequest("Patch incr: the value must be a number.");
        }
        List<String> toks = tokens(path);
        JsonElement parent = parentOf(doc, toks, path);
        String last = toks.get(toks.size() - 1);
        JsonElement cur = child(parent, last);
        if (cur == null) {
            if (parent.isJsonObject()) {
                parent.getAsJsonObject().add(last, v);
                return;
            }
            throw CosmosException.badRequest("Patch incr: path '" + path + "' does not exist.");
        }
        if (!CosmosJson.isNum(cur)) {
            throw CosmosException.badRequest("Patch incr: the value at '" + path + "' is not a number.");
        }
        JsonElement sum = cur.getAsJsonPrimitive().getAsBigDecimal().scale() <= 0 && v.getAsJsonPrimitive().getAsBigDecimal().scale() <= 0
                ? new JsonPrimitive(cur.getAsJsonPrimitive().getAsBigDecimal().add(v.getAsJsonPrimitive().getAsBigDecimal()).longValue())
                : CosmosJson.num(CosmosJson.dbl(cur) + CosmosJson.dbl(v));
        if (parent.isJsonObject()) {
            parent.getAsJsonObject().add(last, sum);
        } else {
            parent.getAsJsonArray().set(Integer.parseInt(last), sum);
        }
    }
}
