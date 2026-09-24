package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.AdHocQueryRunner;
import com.sayonora.wire.influxwire.InfluxEmbedded;
import com.sayonora.wire.influxwire.InfluxException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * InfluxDB vocabulary (tool names follow InfluxData's influxdb3_mcp_server: list_databases,
 * get_measurements, get_measurement_schema, list_tables, describe_table, query_sql, query_influxql,
 * write_line_protocol, health_check). Backed by influxwire's Postgres measurement tables
 * ({@code warp_influx_<m>}: time, tags jsonb, fields jsonb). Metadata and {@code query_sql} run as
 * governed SQL; InfluxQL and line-protocol writes use influxwire's own parser/store.
 *
 * <p>influxwire has a single logical namespace: {@code db} is accepted for compatibility and
 * ignored, and {@code list_databases} reports that one namespace.
 */
final class InfluxToolProvider implements BackendToolProvider {

    private static final int MAX_ROWS = 1000;
    private static final Pattern FROM_JOIN = Pattern.compile("(?i)\\b(?:from|join)\\s+([\"\\w.]+)");

    private final EmulatedStores stores;

    InfluxToolProvider(EmulatedStores stores) {
        this.stores = stores;
    }

    @Override
    public BackendKind kind() {
        return BackendKind.INFLUX;
    }

    @Override
    public List<Tool> tools() {
        JsonObject db = str("Database name (accepted for compatibility; influxwire has one namespace)");
        return List.of(
                new Tool("health_check", "Check the InfluxDB-shaped store is reachable.", schema(List.of()), false),
                new Tool("list_databases", "List databases (influxwire exposes one logical database).",
                        schema(List.of()), false),
                new Tool("get_measurements", "List all measurements in a database.",
                        schema(List.of("db"), "db", db), false),
                new Tool("list_tables", "List tables, i.e. measurements, in a database.",
                        schema(List.of("db"), "db", db), false),
                new Tool("get_measurement_schema", "Get a measurement's columns: time, tag keys, field keys with types.",
                        schema(List.of("db", "measurement"), "db", db, "measurement", str("Measurement name")), false),
                new Tool("describe_table", "Describe a measurement's schema with column categories (time/tag/field).",
                        schema(List.of("db", "table"), "db", db, "table", str("Measurement name")), false),
                new Tool("query_sql", "Run bounded read-only SQL through Warp's governed pipeline. Measurement "
                        + "data lives in tables named warp_influx_<measurement> with columns time, tags (jsonb) and "
                        + "fields (jsonb), e.g. SELECT time, fields->>'value' FROM warp_influx_temp.",
                        schema(List.of("db", "q"), "db", db, "q", str("SQL SELECT"),
                                "params", arr("Not supported; must be empty or omitted")), false),
                new Tool("query_influxql", "Run bounded read-only InfluxQL (SHOW MEASUREMENTS, or SELECT with "
                        + "WHERE / GROUP BY time() / mean|sum|count|min|max); InfluxDB v1 result shape.",
                        schema(List.of("db", "q"), "db", db, "q", str("InfluxQL query")), false),
                new Tool("write_line_protocol", "Write points in InfluxDB line protocol.",
                        schema(List.of("db", "data"), "db", db, "data", str("Line protocol payload"),
                                "precision", str("ns (default), us, ms or s")), true));
    }

    @Override
    public JsonObject describe(Ctx ctx) throws Exception {
        InfluxEmbedded influx = stores.influx();
        if (influx == null) {
            throw new IllegalStateException("influxwire store unavailable: no default backend configured");
        }
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        List<String> ms = influx.measurements().stream().sorted().toList();
        ms.forEach(arr::add);
        out.addProperty("measurementCount", ms.size());
        out.add("measurements", arr);
        return out;
    }

    @Override
    public Outcome call(String tool, JsonObject a, Ctx ctx) throws Exception {
        InfluxEmbedded influx = ctx.backend().emulated() ? stores.influx() : null;
        if (!ctx.backend().emulated()) {
            return Outcome.error("UnsupportedOperation: InfluxDB is only available as Warp's emulated influxwire store");
        }
        if (influx == null) {
            return Outcome.error("influxwire store unavailable: no default backend configured");
        }
        try {
            switch (tool) {
                case "health_check" -> {
                    influx.measurements();
                    return Outcome.ok("{\"status\":\"pass\",\"message\":\"influxwire store reachable\"}");
                }
                case "list_databases" -> {
                    return Outcome.ok("{\"databases\":[{\"name\":\"default\"}]}");
                }
                case "get_measurements", "list_tables" -> {
                    JsonObject out = new JsonObject();
                    JsonArray arr = new JsonArray();
                    influx.measurements().stream().sorted().forEach(arr::add);
                    out.add(tool.equals("list_tables") ? "tables" : "measurements", arr);
                    return Outcome.ok(out.toString());
                }
                case "get_measurement_schema", "describe_table" -> {
                    String m = requireString(a, tool.equals("describe_table") ? "table" : "measurement");
                    String table = InfluxEmbedded.physicalTable(m);
                    if (!influx.measurements().contains(m.toLowerCase(java.util.Locale.ROOT))) {
                        return Outcome.error("measurement \"" + m + "\" not found");
                    }
                    JsonArray cols = new JsonArray();
                    cols.add(col("time", "timestamp", "time"));
                    AdHocQueryRunner.Result tags = ctx.sql("SELECT DISTINCT k FROM " + table
                            + ", jsonb_object_keys(tags) AS k ORDER BY k");
                    if (!tags.success()) {
                        return Outcome.error("ERROR [" + tags.sqlState() + "]: " + tags.error());
                    }
                    tags.rows().forEach(r -> cols.add(col(String.valueOf(r.get(0)), "string", "tag")));
                    AdHocQueryRunner.Result fields = ctx.sql("SELECT k, jsonb_typeof(v) AS t FROM " + table
                            + ", jsonb_each(fields) AS f(k, v) GROUP BY k, jsonb_typeof(v) ORDER BY k");
                    if (!fields.success()) {
                        return Outcome.error("ERROR [" + fields.sqlState() + "]: " + fields.error());
                    }
                    fields.rows().forEach(r -> cols.add(col(String.valueOf(r.get(0)), String.valueOf(r.get(1)), "field")));
                    JsonObject out = new JsonObject();
                    out.addProperty("measurement", m);
                    out.add("columns", cols);
                    return Outcome.ok(out.toString());
                }
                case "query_sql" -> {
                    return querySql(a, ctx);
                }
                case "query_influxql" -> {
                    return Outcome.ok(influx.queryInfluxQl(requireString(a, "q")).toString());
                }
                case "write_line_protocol" -> {
                    int n = influx.write(requireString(a, "data"), optString(a, "precision"));
                    return Outcome.ok("{\"success\":true,\"points_written\":" + n + "}");
                }
                default -> {
                    return Outcome.error("unknown InfluxDB tool: " + tool);
                }
            }
        } catch (InfluxException | IllegalArgumentException e) {
            return Outcome.error(e.getMessage());
        }
    }

    private Outcome querySql(JsonObject a, Ctx ctx) {
        if (a.has("params") && a.get("params").isJsonArray() && a.getAsJsonArray("params").size() > 0) {
            return Outcome.error("query_sql parameters are not supported by this endpoint; inline the values");
        }
        String q = requireString(a, "q").strip();
        while (q.endsWith(";")) {
            q = q.substring(0, q.length() - 1).strip();
        }
        String lower = q.toLowerCase(java.util.Locale.ROOT);
        if (!(lower.startsWith("select") || lower.startsWith("with")) || q.contains(";")) {
            return Outcome.error("query_sql is read-only: a single SELECT (or WITH ... SELECT) is required");
        }
        Matcher m = FROM_JOIN.matcher(q);
        while (m.find()) {
            String t = m.group(1).replace("\"", "").toLowerCase(java.util.Locale.ROOT);
            if (t.startsWith("public.")) {
                t = t.substring(7);
            }
            if (!t.startsWith("warp_influx_") && !t.startsWith("jsonb_") && !t.startsWith("generate_series")) {
                return Outcome.error("query_sql on this endpoint may only read measurement tables "
                        + "(warp_influx_<measurement>); refused \"" + m.group(1) + "\"");
            }
        }
        AdHocQueryRunner.Result r = ctx.sql(q);
        if (!r.success()) {
            return Outcome.error("ERROR [" + r.sqlState() + "]: " + r.error());
        }
        JsonObject out = new JsonObject();
        out.add("rows", rowsAsObjects(r, MAX_ROWS));
        out.addProperty("row_count", Math.min(r.rows().size(), MAX_ROWS));
        out.addProperty("truncated", r.rows().size() > MAX_ROWS);
        return Outcome.ok(out.toString());
    }

    private static JsonObject col(String name, String type, String category) {
        JsonObject o = new JsonObject();
        o.addProperty("name", name);
        o.addProperty("type", type);
        o.addProperty("category", category);
        return o;
    }
}
