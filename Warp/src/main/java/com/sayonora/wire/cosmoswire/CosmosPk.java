package com.sayonora.wire.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Logical partition keys. A key is a list of 1..3 values (one per partition key path; several for hierarchical "MultiHash" keys);
 * a value is a string, number, boolean, JSON null, or undefined (Java null: the property is missing, header form {@code [{}]}).
 */
final class CosmosPk {

    private CosmosPk() {
    }

    /** Parses the {@code x-ms-documentdb-partitionkey} header: a JSON array of values. Returns null when absent or {@code []}. */
    static List<JsonElement> parseHeader(String header) {
        if (header == null || header.isBlank()) {
            return null;
        }
        JsonElement e;
        try {
            e = com.google.gson.JsonParser.parseString(header);
        } catch (RuntimeException ex) {
            throw CosmosException.badRequest("The partition key header is not valid JSON: " + header);
        }
        if (!e.isJsonArray()) {
            throw CosmosException.badRequest("The partition key header must be a JSON array.");
        }
        JsonArray a = e.getAsJsonArray();
        if (a.isEmpty()) {
            return null;
        }
        List<JsonElement> out = new ArrayList<>();
        for (JsonElement v : a) {
            if (v.isJsonObject() && v.getAsJsonObject().isEmpty()) {
                out.add(null); // undefined / PartitionKey.None
            } else if (v.isJsonArray() || v.isJsonObject()) {
                throw CosmosException.badRequest("A partition key value must be a string, number, boolean or null.");
            } else {
                out.add(v);
            }
        }
        return out;
    }

    /** The partition key values of {@code doc} at the container's paths. */
    static List<JsonElement> extract(List<String> paths, JsonObject doc) {
        List<JsonElement> out = new ArrayList<>();
        for (String p : paths) {
            JsonElement v = CosmosJson.path(doc, p);
            if (v != null && (v.isJsonArray() || v.isJsonObject())) {
                throw CosmosException.badRequest("PartitionKey extracted from document is not a supported type at path " + p);
            }
            out.add(v);
        }
        return out;
    }

    /** Canonical text of a key: {@code ["a",1]}; undefined is {@code {}}. */
    static String canon(List<JsonElement> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values.get(i) == null ? "{}" : CosmosJson.canon(values.get(i)));
        }
        return sb.append(']').toString();
    }

    /** Text of a key prefix (hierarchical keys): {@code ["a",} - every full key starting with it begins with this text. */
    static String prefix(List<JsonElement> values) {
        String c = canon(values);
        return c.substring(0, c.length() - 1) + ",";
    }

    static String hostKey(List<JsonElement> values) {
        return values.get(0) == null ? "{}" : CosmosJson.canon(values.get(0));
    }

    static boolean same(List<JsonElement> a, List<JsonElement> b) {
        return canon(a).equals(canon(b));
    }

    static String describe(List<JsonElement> v) {
        return canon(v);
    }
}
