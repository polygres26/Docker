package com.sayonora.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

/** The {@code filter_path} response filter: include patterns ({@code hits.hits._source}, {@code *.count}, {@code **.id}) and {@code -exclusions}. */
final class FilterPath {

    private FilterPath() {
    }

    static JsonElement apply(JsonElement root, String param) {
        List<String[]> inc = new ArrayList<>(), exc = new ArrayList<>();
        for (String p : param.split(",")) {
            p = p.trim();
            if (p.isEmpty()) {
                continue;
            }
            if (p.startsWith("-")) {
                exc.add(p.substring(1).split("\\."));
            } else {
                inc.add(p.split("\\."));
            }
        }
        JsonElement out = root;
        if (!inc.isEmpty()) {
            JsonElement f = include(root, inc);
            out = f == null ? new JsonObject() : f;
        }
        if (!exc.isEmpty()) {
            out = exclude(out.deepCopy(), exc);
        }
        return out;
    }

    private static boolean segMatch(String pat, String key) {
        return pat.equals("*") || pat.equals("**") || (pat.contains("*") ? Mappings.wildcardMatch(pat, key) : pat.equals(key));
    }

    /** {@code pats} = remaining pattern suffixes; returns filtered element or null if nothing matched. */
    private static JsonElement include(JsonElement e, List<String[]> pats) {
        if (e.isJsonArray()) {
            JsonArray out = new JsonArray();
            for (JsonElement x : e.getAsJsonArray()) {
                JsonElement f = x.isJsonObject() || x.isJsonArray() ? include(x, pats) : null;
                if (f != null) {
                    out.add(f);
                }
            }
            return out.size() == 0 ? null : out;
        }
        if (!e.isJsonObject()) {
            return null;
        }
        JsonObject o = e.getAsJsonObject();
        JsonObject out = new JsonObject();
        for (var en : o.entrySet()) {
            boolean full = false;
            List<String[]> next = new ArrayList<>();
            for (String[] p : pats) {
                if (p.length == 0) {
                    continue;
                }
                if (p[0].equals("**")) {
                    next.add(p); // ** keeps consuming
                    if (p.length > 1 && segMatch(p[1], en.getKey())) {
                        String[] rest = java.util.Arrays.copyOfRange(p, 2, p.length);
                        if (rest.length == 0) {
                            full = true;
                        } else {
                            next.add(rest);
                        }
                    }
                } else if (segMatch(p[0], en.getKey())) {
                    String[] rest = java.util.Arrays.copyOfRange(p, 1, p.length);
                    if (rest.length == 0) {
                        full = true;
                    } else {
                        next.add(rest);
                    }
                }
            }
            if (full) {
                out.add(en.getKey(), en.getValue());
            } else if (!next.isEmpty() && (en.getValue().isJsonObject() || en.getValue().isJsonArray())) {
                JsonElement f = include(en.getValue(), next);
                if (f != null) {
                    out.add(en.getKey(), f);
                }
            }
        }
        return out.size() == 0 ? null : out;
    }

    private static JsonElement exclude(JsonElement e, List<String[]> pats) {
        if (e.isJsonArray()) {
            for (JsonElement x : e.getAsJsonArray()) {
                exclude(x, pats);
            }
            return e;
        }
        if (!e.isJsonObject()) {
            return e;
        }
        JsonObject o = e.getAsJsonObject();
        for (String k : new ArrayList<>(o.keySet())) {
            List<String[]> next = new ArrayList<>();
            boolean drop = false;
            for (String[] p : pats) {
                if (p.length == 0) {
                    continue;
                }
                if (p[0].equals("**")) {
                    next.add(p);
                    if (p.length > 1 && segMatch(p[1], k)) {
                        String[] rest = java.util.Arrays.copyOfRange(p, 2, p.length);
                        if (rest.length == 0) {
                            drop = true;
                        } else {
                            next.add(rest);
                        }
                    }
                } else if (segMatch(p[0], k)) {
                    String[] rest = java.util.Arrays.copyOfRange(p, 1, p.length);
                    if (rest.length == 0) {
                        drop = true;
                    } else {
                        next.add(rest);
                    }
                }
            }
            if (drop) {
                o.remove(k);
            } else if (!next.isEmpty()) {
                exclude(o.get(k), next);
            }
        }
        return e;
    }
}
