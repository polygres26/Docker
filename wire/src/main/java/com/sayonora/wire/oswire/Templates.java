package com.sayonora.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Index templates (legacy {@code _template}, composable {@code _index_template} + {@code _component_template}) applied when an index is created. */
final class Templates {

    static final class Applied {
        JsonObject settings = new JsonObject();
        JsonObject mappings = new JsonObject();
        JsonObject aliases = new JsonObject();
        String templateName;
    }

    private Templates() {
    }

    static List<String> patterns(JsonObject t) {
        List<String> out = new ArrayList<>();
        JsonElement p = t.has("index_patterns") ? t.get("index_patterns") : t.get("template");
        if (p == null) {
            return out;
        }
        if (p.isJsonArray()) {
            p.getAsJsonArray().forEach(e -> out.add(e.getAsString()));
        } else if (p.isJsonPrimitive()) {
            out.add(p.getAsString());
        }
        return out;
    }

    static boolean matches(JsonObject t, String index) {
        boolean any = false;
        for (String p : patterns(t)) {
            if (p.startsWith("-")) {
                if (Mappings.wildcardMatch(p.substring(1), index)) {
                    return false;
                }
            } else if (Mappings.wildcardMatch(p, index)) {
                any = true;
            }
        }
        return any;
    }

    static Applied forNewIndex(PostgresSearchStore store, String index) throws SQLException {
        Applied a = new Applied();
        // composable templates take precedence over legacy ones
        String bestName = null;
        JsonObject best = null;
        long bestPrio = Long.MIN_VALUE;
        for (Map.Entry<String, JsonObject> e : store.templates("index_template").entrySet()) {
            if (matches(e.getValue(), index)) {
                long prio = e.getValue().has("priority") ? e.getValue().get("priority").getAsLong() : 0;
                if (prio > bestPrio) {
                    bestPrio = prio;
                    best = e.getValue();
                    bestName = e.getKey();
                }
            }
        }
        if (best != null) {
            a.templateName = bestName;
            Map<String, JsonObject> comps = store.templates("component_template");
            if (best.has("composed_of")) {
                for (JsonElement c : best.getAsJsonArray("composed_of")) {
                    JsonObject comp = comps.get(c.getAsString());
                    if (comp != null && comp.has("template")) {
                        mergeInto(a, comp.getAsJsonObject("template"));
                    }
                }
            }
            if (best.has("template")) {
                mergeInto(a, best.getAsJsonObject("template"));
            }
            return a;
        }
        List<Map.Entry<String, JsonObject>> legacy = new ArrayList<>();
        for (Map.Entry<String, JsonObject> e : store.templates("template").entrySet()) {
            if (matches(e.getValue(), index)) {
                legacy.add(e);
            }
        }
        legacy.sort((x, y) -> Integer.compare(x.getValue().has("order") ? x.getValue().get("order").getAsInt() : 0,
                y.getValue().has("order") ? y.getValue().get("order").getAsInt() : 0));
        for (var e : legacy) {
            mergeInto(a, e.getValue());
        }
        return a;
    }

    private static void mergeInto(Applied a, JsonObject t) {
        if (t.has("settings")) {
            deepMerge(a.settings, PostgresSearchStore.normalizeSettings(t.getAsJsonObject("settings")));
        }
        if (t.has("mappings")) {
            JsonObject m = t.getAsJsonObject("mappings");
            if (m.has("_doc") && m.size() == 1) {
                m = m.getAsJsonObject("_doc");
            }
            deepMerge(a.mappings, m);
        }
        if (t.has("aliases")) {
            for (var e : t.getAsJsonObject("aliases").entrySet()) {
                a.aliases.add(e.getKey(), e.getValue());
            }
        }
    }

    static void deepMerge(JsonObject into, JsonObject from) {
        for (var e : from.entrySet()) {
            JsonElement cur = into.get(e.getKey());
            if (cur != null && cur.isJsonObject() && e.getValue().isJsonObject()) {
                deepMerge(cur.getAsJsonObject(), e.getValue().getAsJsonObject());
            } else if (e.getValue().isJsonArray() && e.getKey().equals("dynamic_templates") && cur != null && cur.isJsonArray()) {
                JsonArray merged = cur.getAsJsonArray().deepCopy();
                e.getValue().getAsJsonArray().forEach(merged::add);
                into.add(e.getKey(), merged);
            } else {
                into.add(e.getKey(), e.getValue().deepCopy());
            }
        }
    }
}
