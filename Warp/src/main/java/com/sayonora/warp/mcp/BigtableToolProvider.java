package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.bigtablewire.BigtableEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Google Cloud Bigtable vocabulary (bigtable_* tools: list/describe/create/delete tables, read rows by key, prefix or range with
 * filters, mutate rows, delete rows, increment a counter, drop a row range). Each tool calls the same Bigtable data and
 * table-admin services bigtablewire serves over gRPC (through a private in-process channel), so filters, versions, GC rules,
 * atomic mutations and error statuses are the wire protocol's. Row keys, qualifiers and values are text in and out (UTF-8; a
 * value that is not valid UTF-8, such as a counter, comes back as {"base64": "..."}).
 */
final class BigtableToolProvider extends StoreToolProvider {

    private static final int MAX_ROWS = 1000;

    BigtableToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.BIGTABLE, describer, stores);
    }

    private BigtableEmbedded bt() {
        return stores.engine("bigtable", BigtableEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject project = str("Project id (default: WARP_MCP_GCP_PROJECT, else \"warp-project\")");
        JsonObject instance = str("Instance id (default: WARP_MCP_BIGTABLE_INSTANCE, else \"warp-instance\")");
        JsonObject table = str("Table id");
        JsonObject rowKey = str("Row key (text)");
        JsonObject set = arr("Cells to write: [{family, qualifier, value, timestampMicros?}] (timestamp defaults to server time)");
        return List.of(
                new Tool("bigtable_list_tables", "List the tables of an instance.", schema(List.of(), "project", project, "instance", instance), false),
                new Tool("bigtable_get_table", "A table's schema: column families and their GC rules.", schema(List.of("table"),
                        "project", project, "instance", instance, "table", table), false),
                new Tool("bigtable_create_table", "Create a table with column families.", schema(List.of("table"), "project", project,
                        "instance", instance, "table", table, "families", obj("Family name -> {maxVersions?, maxAgeSeconds?} (or {} for no GC rule)")), true),
                new Tool("bigtable_delete_table", "Delete a table and its rows.", schema(List.of("table"), "project", project, "instance", instance,
                        "table", table), true),
                new Tool("bigtable_read_rows", "Read rows by exact keys, key prefix or key range, optionally filtered, newest cell first per column.",
                        schema(List.of("table"), "project", project, "instance", instance, "table", table,
                                "rowKeys", strings("Exact row keys"), "prefix", str("Key prefix"), "startKey", str("Range start (inclusive)"),
                                "endKey", str("Range end (exclusive)"), "limit", num("Max rows (default 100, max 1000)"),
                                "family", str("Only this column family (regex)"), "qualifier", str("Only this column qualifier (regex)"),
                                "latestOnly", bool("Only the latest cell of each column"), "reversed", bool("Descending key order"),
                                "filter", obj("Raw RowFilter (proto3 JSON) instead of the shortcuts above")), false),
                new Tool("bigtable_read_row", "Read one row by key.", schema(List.of("table", "rowKey"), "project", project, "instance", instance,
                        "table", table, "rowKey", rowKey, "latestOnly", bool("Only the latest cell of each column")), false),
                new Tool("bigtable_mutate_row", "Atomically apply mutations to one row: set cells, delete columns or families.",
                        schema(List.of("table", "rowKey"), "project", project, "instance", instance, "table", table, "rowKey", rowKey,
                                "set", set, "deleteColumns", arr("[{family, qualifier}]"), "deleteFamilies", strings("Families to clear in this row")), true),
                new Tool("bigtable_mutate_rows", "Apply set-cell mutations to several rows (one atomic mutation per row).",
                        schema(List.of("table", "entries"), "project", project, "instance", instance, "table", table,
                                "entries", arr("[{rowKey, set: [{family, qualifier, value}]}]")), true),
                new Tool("bigtable_delete_row", "Delete a row.", schema(List.of("table", "rowKey"), "project", project, "instance", instance,
                        "table", table, "rowKey", rowKey), true),
                new Tool("bigtable_increment", "Atomically add to a 64-bit counter cell; returns the new value.",
                        schema(List.of("table", "rowKey", "family", "qualifier"), "project", project, "instance", instance, "table", table,
                                "rowKey", rowKey, "family", str("Column family"), "qualifier", str("Column qualifier"), "by", num("Increment (default 1)")), true),
                new Tool("bigtable_drop_row_range", "Delete every row whose key starts with a prefix (or all rows).",
                        schema(List.of("table"), "project", project, "instance", instance, "table", table,
                                "prefix", str("Key prefix"), "all", bool("Delete all rows")), true));
    }

    private static String instancePath(JsonObject a) {
        String inst = optString(a, "instance");
        if (inst == null) {
            String env = System.getenv("WARP_MCP_BIGTABLE_INSTANCE");
            inst = env == null || env.isBlank() ? "warp-instance" : env.trim();
        }
        return "projects/" + gcpProject(a) + "/instances/" + inst;
    }

    private static String tablePath(JsonObject a) {
        return instancePath(a) + "/tables/" + requireString(a, "table");
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] successor(byte[] prefix) {
        byte[] p = prefix.clone();
        for (int i = p.length - 1; i >= 0; i--) {
            if ((p[i] & 0xff) != 0xff) {
                p[i]++;
                return java.util.Arrays.copyOf(p, i + 1);
            }
        }
        return new byte[0]; // all 0xff: open-ended
    }

    private static JsonObject setCell(JsonObject c) {
        JsonObject sc = new JsonObject();
        sc.addProperty("familyName", requireString(c, "family"));
        sc.addProperty("columnQualifier", b64(requireString(c, "qualifier")));
        sc.addProperty("timestampMicros", optLong(c, "timestampMicros") == null ? "-1" : String.valueOf(optLong(c, "timestampMicros")));
        sc.addProperty("value", b64(c.has("value") && c.get("value").isJsonPrimitive() ? c.get("value").getAsString()
                : c.has("value") ? c.get("value").toString() : ""));
        JsonObject m = new JsonObject();
        m.add("setCell", sc);
        return m;
    }

    private static JsonArray setCells(JsonObject holder) {
        JsonArray ms = new JsonArray();
        if (holder.has("set") && holder.get("set").isJsonArray()) {
            holder.getAsJsonArray("set").forEach(c -> ms.add(setCell(c.getAsJsonObject())));
        }
        return ms;
    }

    private JsonObject data(String method, JsonObject body) {
        String res = bt().data(method, body.toString());
        JsonElement e = JsonParser.parseString(res);
        return e.isJsonObject() ? e.getAsJsonObject() : new JsonObject();
    }

    private JsonObject readRows(JsonObject a, JsonObject rowSet, int limit) {
        JsonObject req = new JsonObject();
        req.addProperty("tableName", tablePath(a));
        req.add("rows", rowSet);
        req.addProperty("rowsLimit", String.valueOf(limit + 1));
        req.addProperty("reversed", optBool(a, "reversed", false));
        JsonArray chain = new JsonArray();
        if (a.has("filter") && a.get("filter").isJsonObject()) {
            chain.add(a.get("filter"));
        } else {
            if (optString(a, "family") != null) {
                JsonObject f = new JsonObject();
                f.addProperty("familyNameRegexFilter", optString(a, "family"));
                chain.add(f);
            }
            if (optString(a, "qualifier") != null) {
                JsonObject f = new JsonObject();
                f.addProperty("columnQualifierRegexFilter", b64(optString(a, "qualifier")));
                chain.add(f);
            }
            if (optBool(a, "latestOnly", false)) {
                JsonObject f = new JsonObject();
                f.addProperty("cellsPerColumnLimitFilter", 1);
                chain.add(f);
            }
        }
        if (chain.size() == 1) {
            req.add("filter", chain.get(0));
        } else if (chain.size() > 1) {
            JsonObject c = new JsonObject();
            JsonObject inner = new JsonObject();
            inner.add("filters", chain);
            c.add("chain", inner);
            req.add("filter", c);
        }
        JsonObject res = bt().readRows(req.toString());
        JsonArray rows = res.getAsJsonArray("rows");
        boolean truncated = rows.size() > limit;
        JsonArray cut = new JsonArray();
        for (int i = 0; i < rows.size() && i < limit; i++) {
            cut.add(rows.get(i));
        }
        JsonObject out = new JsonObject();
        out.add("rows", cut);
        out.addProperty("count", cut.size());
        out.addProperty("truncated", truncated);
        return out;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        switch (tool) {
            case "bigtable_list_tables": {
                JsonObject b = new JsonObject();
                b.addProperty("parent", instancePath(a));
                b.addProperty("view", "NAME_ONLY");
                return json(JsonParser.parseString(bt().admin("ListTables", b.toString())));
            }
            case "bigtable_get_table": {
                JsonObject b = new JsonObject();
                b.addProperty("name", tablePath(a));
                b.addProperty("view", "SCHEMA_VIEW");
                return json(JsonParser.parseString(bt().admin("GetTable", b.toString())));
            }
            case "bigtable_create_table": {
                JsonObject b = new JsonObject();
                b.addProperty("parent", instancePath(a));
                b.addProperty("tableId", requireString(a, "table"));
                JsonObject fams = new JsonObject();
                if (a.has("families") && a.get("families").isJsonObject()) {
                    for (var e : a.getAsJsonObject("families").entrySet()) {
                        JsonObject cf = new JsonObject();
                        if (e.getValue().isJsonObject()) {
                            JsonObject spec = e.getValue().getAsJsonObject();
                            if (optInt(spec, "maxVersions") != null) {
                                JsonObject rule = new JsonObject();
                                rule.addProperty("maxNumVersions", optInt(spec, "maxVersions"));
                                cf.add("gcRule", rule);
                            } else if (optLong(spec, "maxAgeSeconds") != null) {
                                JsonObject rule = new JsonObject();
                                rule.addProperty("maxAge", optLong(spec, "maxAgeSeconds") + "s");
                                cf.add("gcRule", rule);
                            }
                        }
                        fams.add(e.getKey(), cf);
                    }
                }
                JsonObject table = new JsonObject();
                table.add("columnFamilies", fams);
                b.add("table", table);
                return json(JsonParser.parseString(bt().admin("CreateTable", b.toString())));
            }
            case "bigtable_delete_table": {
                JsonObject b = new JsonObject();
                b.addProperty("name", tablePath(a));
                bt().admin("DeleteTable", b.toString());
                return ok();
            }
            case "bigtable_read_rows": {
                JsonObject rows = new JsonObject();
                if (a.has("rowKeys") && a.get("rowKeys").isJsonArray() && !a.getAsJsonArray("rowKeys").isEmpty()) {
                    JsonArray keys = new JsonArray();
                    a.getAsJsonArray("rowKeys").forEach(k -> keys.add(b64(k.getAsString())));
                    rows.add("rowKeys", keys);
                }
                JsonObject range = null;
                if (optString(a, "prefix") != null) {
                    range = new JsonObject();
                    range.addProperty("startKeyClosed", b64(optString(a, "prefix")));
                    byte[] end = successor(optString(a, "prefix").getBytes(StandardCharsets.UTF_8));
                    if (end.length > 0) {
                        range.addProperty("endKeyOpen", Base64.getEncoder().encodeToString(end));
                    }
                } else if (optString(a, "startKey") != null || optString(a, "endKey") != null) {
                    range = new JsonObject();
                    if (optString(a, "startKey") != null) {
                        range.addProperty("startKeyClosed", b64(optString(a, "startKey")));
                    }
                    if (optString(a, "endKey") != null) {
                        range.addProperty("endKeyOpen", b64(optString(a, "endKey")));
                    }
                }
                if (range != null) {
                    JsonArray ranges = new JsonArray();
                    ranges.add(range);
                    rows.add("rowRanges", ranges);
                }
                return json(readRows(a, rows, limit(a, "limit", 100, MAX_ROWS)));
            }
            case "bigtable_read_row": {
                JsonObject rows = new JsonObject();
                JsonArray keys = new JsonArray();
                keys.add(b64(requireString(a, "rowKey")));
                rows.add("rowKeys", keys);
                JsonObject out = readRows(a, rows, 1);
                JsonArray arr = out.getAsJsonArray("rows");
                JsonObject o = new JsonObject();
                o.addProperty("found", arr.size() > 0);
                o.add("row", arr.size() > 0 ? arr.get(0) : com.google.gson.JsonNull.INSTANCE);
                return json(o);
            }
            case "bigtable_mutate_row": {
                JsonObject b = new JsonObject();
                b.addProperty("tableName", tablePath(a));
                b.addProperty("rowKey", b64(requireString(a, "rowKey")));
                JsonArray ms = setCells(a);
                if (a.has("deleteColumns") && a.get("deleteColumns").isJsonArray()) {
                    for (JsonElement e : a.getAsJsonArray("deleteColumns")) {
                        JsonObject c = e.getAsJsonObject();
                        JsonObject d = new JsonObject();
                        d.addProperty("familyName", requireString(c, "family"));
                        d.addProperty("columnQualifier", b64(requireString(c, "qualifier")));
                        JsonObject m = new JsonObject();
                        m.add("deleteFromColumn", d);
                        ms.add(m);
                    }
                }
                if (a.has("deleteFamilies") && a.get("deleteFamilies").isJsonArray()) {
                    for (JsonElement e : a.getAsJsonArray("deleteFamilies")) {
                        JsonObject d = new JsonObject();
                        d.addProperty("familyName", e.getAsString());
                        JsonObject m = new JsonObject();
                        m.add("deleteFromFamily", d);
                        ms.add(m);
                    }
                }
                if (ms.isEmpty()) {
                    throw new IllegalArgumentException("give at least one of set, deleteColumns, deleteFamilies");
                }
                b.add("mutations", ms);
                data("MutateRow", b);
                return ok();
            }
            case "bigtable_mutate_rows": {
                JsonObject b = new JsonObject();
                b.addProperty("tableName", tablePath(a));
                JsonArray entries = new JsonArray();
                if (!a.has("entries") || !a.get("entries").isJsonArray() || a.getAsJsonArray("entries").isEmpty()) {
                    throw new IllegalArgumentException("entries must be a non-empty array");
                }
                for (JsonElement e : a.getAsJsonArray("entries")) {
                    JsonObject in = e.getAsJsonObject();
                    JsonObject en = new JsonObject();
                    en.addProperty("rowKey", b64(requireString(in, "rowKey")));
                    en.add("mutations", setCells(in));
                    entries.add(en);
                }
                b.add("entries", entries);
                String res = bt().data("MutateRows", b.toString());
                JsonObject o = new JsonObject();
                int failed = 0;
                for (JsonElement r : JsonParser.parseString(res).getAsJsonArray()) {
                    if (r.getAsJsonObject().has("entries")) {
                        for (JsonElement en : r.getAsJsonObject().getAsJsonArray("entries")) {
                            JsonObject st = en.getAsJsonObject().has("status") ? en.getAsJsonObject().getAsJsonObject("status") : null;
                            if (st != null && st.has("code") && st.get("code").getAsInt() != 0) {
                                failed++;
                            }
                        }
                    }
                }
                o.addProperty("entries", entries.size());
                o.addProperty("failed", failed);
                return json(o);
            }
            case "bigtable_delete_row": {
                JsonObject b = new JsonObject();
                b.addProperty("tableName", tablePath(a));
                b.addProperty("rowKey", b64(requireString(a, "rowKey")));
                JsonArray ms = new JsonArray();
                JsonObject m = new JsonObject();
                m.add("deleteFromRow", new JsonObject());
                ms.add(m);
                b.add("mutations", ms);
                data("MutateRow", b);
                return ok();
            }
            case "bigtable_increment": {
                JsonObject b = new JsonObject();
                b.addProperty("tableName", tablePath(a));
                b.addProperty("rowKey", b64(requireString(a, "rowKey")));
                JsonObject rule = new JsonObject();
                rule.addProperty("familyName", requireString(a, "family"));
                rule.addProperty("columnQualifier", b64(requireString(a, "qualifier")));
                rule.addProperty("incrementAmount", String.valueOf(optLong(a, "by") == null ? 1 : optLong(a, "by")));
                JsonArray rules = new JsonArray();
                rules.add(rule);
                b.add("rules", rules);
                JsonObject res = data("ReadModifyWriteRow", b);
                // the counter is an 8-byte big-endian integer: decode it for the caller
                long value = 0;
                try {
                    var fams = res.getAsJsonObject("row").getAsJsonArray("families");
                    for (JsonElement f : fams) {
                        if (f.getAsJsonObject().get("name").getAsString().equals(requireString(a, "family"))) {
                            for (JsonElement c : f.getAsJsonObject().getAsJsonArray("columns")) {
                                if (new String(Base64.getDecoder().decode(c.getAsJsonObject().get("qualifier").getAsString()), StandardCharsets.UTF_8)
                                        .equals(requireString(a, "qualifier"))) {
                                    byte[] v = Base64.getDecoder().decode(c.getAsJsonObject().getAsJsonArray("cells").get(0).getAsJsonObject()
                                            .get("value").getAsString());
                                    value = java.nio.ByteBuffer.wrap(v).getLong();
                                }
                            }
                        }
                    }
                } catch (RuntimeException ignored) {
                    // leave 0 if the answer has an unexpected shape
                }
                JsonObject o = new JsonObject();
                o.addProperty("value", value);
                return json(o);
            }
            case "bigtable_drop_row_range": {
                JsonObject b = new JsonObject();
                b.addProperty("name", tablePath(a));
                if (optBool(a, "all", false)) {
                    b.addProperty("deleteAllDataFromTable", true);
                } else {
                    b.addProperty("rowKeyPrefix", b64(requireString(a, "prefix")));
                }
                bt().admin("DropRowRange", b.toString());
                return ok();
            }
            default:
                return Outcome.error("unknown bigtable tool: " + tool);
        }
    }

    private static Outcome ok() {
        JsonObject o = new JsonObject();
        o.addProperty("ok", true);
        return json(o);
    }
}
