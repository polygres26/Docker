package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** {@code _cat/*} tables (text with {@code v}/{@code h}/{@code s}, or {@code format=json}). */
final class Cat {

    record Col(String name, String alias, boolean right) {
    }

    private Cat() {
    }

    private static Col c(String name, String alias, boolean right) {
        return new Col(name, alias, right);
    }

    static String bytes(long b, String unit) {
        if (unit != null) {
            return switch (unit) {
                case "b" -> Long.toString(b);
                case "kb", "k" -> Long.toString(b / 1024);
                case "mb", "m" -> Long.toString(b / (1024 * 1024));
                case "gb", "g" -> Long.toString(b / (1024L * 1024 * 1024));
                default -> Long.toString(b);
            };
        }
        if (b < 1024) {
            return b + "b";
        }
        double v = b;
        String[] u = {"kb", "mb", "gb", "tb"};
        int i = -1;
        while (v >= 1024 && i < u.length - 1) {
            v /= 1024;
            i++;
        }
        return String.format(Locale.ROOT, "%.1f%s", v, u[i]);
    }

    static RestApi.Response render(RestApi.Request r, List<Col> cols, List<Map<String, String>> rows) {
        if ("true".equals(r.q.get("help"))) {
            StringBuilder sb = new StringBuilder();
            for (Col col : cols) {
                sb.append(col.name).append(" | ").append(col.alias).append(" | \n");
            }
            return RestApi.Response.text(200, sb.toString());
        }
        List<Col> shown = new ArrayList<>();
        String h = r.q.get("h");
        if (h == null || h.isEmpty()) {
            shown.addAll(cols);
        } else {
            for (String want : h.split(",")) {
                boolean found = false;
                for (Col col : cols) {
                    if (col.name.equals(want) || col.alias.equals(want) || (want.contains("*") && Mappings.wildcardMatch(want, col.name))) {
                        shown.add(col);
                        found = true;
                        if (!want.contains("*")) {
                            break;
                        }
                    }
                }
                if (!found) {
                    // unknown columns are ignored by OpenSearch
                    continue;
                }
            }
        }
        String s = r.q.get("s");
        if (s != null) {
            List<Comparator<Map<String, String>>> cmps = new ArrayList<>();
            for (String part : s.split(",")) {
                String[] p = part.split(":");
                String col = p[0];
                for (Col cc : cols) {
                    if (cc.alias.equals(col)) {
                        col = cc.name;
                    }
                }
                final String key = col;
                boolean desc = p.length > 1 && p[1].equalsIgnoreCase("desc");
                Comparator<Map<String, String>> cmp = (a, b) -> compareCell(a.getOrDefault(key, ""), b.getOrDefault(key, ""));
                cmps.add(desc ? cmp.reversed() : cmp);
            }
            rows.sort((a, b) -> {
                for (var cmp : cmps) {
                    int x = cmp.compare(a, b);
                    if (x != 0) {
                        return x;
                    }
                }
                return 0;
            });
        }
        if ("json".equals(r.q.get("format"))) {
            JsonArray arr = new JsonArray();
            for (Map<String, String> row : rows) {
                JsonObject o = new JsonObject();
                for (Col col : shown) {
                    String v = row.get(col.name);
                    if (v == null) {
                        o.add(col.name, com.google.gson.JsonNull.INSTANCE);
                    } else {
                        o.addProperty(col.name, v);
                    }
                }
                arr.add(o);
            }
            return RestApi.Response.json(200, arr);
        }
        int[] w = new int[shown.size()];
        boolean v = r.q.containsKey("v") && !"false".equals(r.q.get("v"));
        for (int i = 0; i < shown.size(); i++) {
            w[i] = v ? shown.get(i).name.length() : 0;
            for (Map<String, String> row : rows) {
                w[i] = Math.max(w[i], row.getOrDefault(shown.get(i).name, "").length());
            }
        }
        StringBuilder sb = new StringBuilder();
        if (v) {
            line(sb, shown, w, null);
        }
        for (Map<String, String> row : rows) {
            line(sb, shown, w, row);
        }
        return RestApi.Response.text(200, sb.toString());
    }

    private static int compareCell(String a, String b) {
        try {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    private static void line(StringBuilder sb, List<Col> cols, int[] w, Map<String, String> row) {
        for (int i = 0; i < cols.size(); i++) {
            String cell = row == null ? cols.get(i).name : row.getOrDefault(cols.get(i).name, "");
            if (i > 0) {
                sb.append(' ');
            }
            if (cols.get(i).right && row != null) {
                sb.append(" ".repeat(Math.max(0, w[i] - cell.length()))).append(cell);
            } else if (i == cols.size() - 1) {
                sb.append(cell).append(" ".repeat(Math.max(0, w[i] - cell.length())));
            } else {
                sb.append(cell).append(" ".repeat(Math.max(0, w[i] - cell.length())));
            }
        }
        sb.append('\n');
    }

    private static Map<String, String> row(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    static RestApi.Response indices(RestApi api, RestApi.Request r) throws SQLException {
        PostgresSearchStore.ResolveOpts ro = api.resolveOpts(r);
        ro.expandClosed = true;
        List<PostgresSearchStore.Resolved> ts = api.store.resolve(r.var("index"), ro);
        String healthFilter = r.q.get("health");
        if (healthFilter != null && !Set.of("green", "yellow", "red").contains(healthFilter)) {
            throw new OpenSearchException("illegal_argument_exception", "No enum constant org.opensearch.cluster.health.ClusterHealthStatus." + healthFilter.toUpperCase(Locale.ROOT));
        }
        List<Col> cols = List.of(c("health", "h", false), c("status", "s", false), c("index", "i", false), c("uuid", "id", false),
                c("pri", "p", true), c("rep", "r", true), c("docs.count", "dc", true), c("docs.deleted", "dd", true),
                c("store.size", "ss", true), c("pri.store.size", "pri.ss", true));
        List<Map<String, String>> rows = new ArrayList<>();
        String unit = r.q.get("bytes");
        for (var t : ts) {
            var m = t.index();
            if (healthFilter != null && !healthFilter.equals("green")) {
                continue;
            }
            long docs = api.store.count(m);
            String size = bytes(api.store.storeSizeBytes(m), unit);
            rows.add(row("health", "green", "status", m.closed ? "close" : "open", "index", m.name, "uuid", m.uuid,
                    "pri", String.valueOf(SearchEngine.shardsOf(m)), "rep", m.settings.getAsJsonObject("index").get("number_of_replicas").getAsString(),
                    "docs.count", String.valueOf(docs), "docs.deleted", "0", "store.size", size, "pri.store.size", size));
        }
        rows.sort(Comparator.comparing(x -> x.get("index")));
        return render(r, cols, rows);
    }

    private static String[] clock() {
        Instant now = Instant.now();
        return new String[] {String.valueOf(now.getEpochSecond()), DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC).format(now)};
    }

    static RestApi.Response health(RestApi api, RestApi.Request r) throws SQLException {
        int shards = 0;
        for (var t : api.store.resolve(null, new PostgresSearchStore.ResolveOpts())) {
            shards += SearchEngine.shardsOf(t.index());
        }
        String[] ck = clock();
        boolean ts = !"false".equals(r.q.get("ts"));
        List<Col> cols = new ArrayList<>();
        if (ts) {
            cols.add(c("epoch", "t", true));
            cols.add(c("timestamp", "ts", false));
        }
        cols.addAll(List.of(c("cluster", "cl", false), c("status", "st", false), c("node.total", "nt", true), c("node.data", "nd", true),
                c("discovered_cluster_manager", "dcm", false), c("shards", "t", true), c("pri", "p", true), c("relo", "r", true),
                c("init", "i", true), c("unassign", "u", true), c("pending_tasks", "pt", true), c("max_task_wait_time", "mtwt", true),
                c("active_shards_percent", "asp", true)));
        return render(r, cols, new ArrayList<>(List.of(row("epoch", ck[0], "timestamp", ck[1], "cluster", RestApi.CLUSTER_NAME, "status", "green",
                "node.total", "1", "node.data", "1", "discovered_cluster_manager", "true", "shards", String.valueOf(shards),
                "pri", String.valueOf(shards), "relo", "0", "init", "0", "unassign", "0", "pending_tasks", "0", "max_task_wait_time", "-",
                "active_shards_percent", "100.0%"))));
    }

    static RestApi.Response count(RestApi api, RestApi.Request r) throws SQLException {
        var ts = r.var("index") == null ? api.store.resolve(null, new PostgresSearchStore.ResolveOpts()) : api.store.resolve(r.var("index"), api.resolveOpts(r));
        long n = 0;
        for (var t : ts) {
            n += api.store.count(t.index());
        }
        String[] ck = clock();
        return render(r, List.of(c("epoch", "t", true), c("timestamp", "ts", false), c("count", "dc", true)),
                new ArrayList<>(List.of(row("epoch", ck[0], "timestamp", ck[1], "count", String.valueOf(n)))));
    }

    static RestApi.Response shards(RestApi api, RestApi.Request r) throws SQLException {
        var ts = r.var("index") == null ? api.store.resolve(null, new PostgresSearchStore.ResolveOpts()) : api.store.resolve(r.var("index"), api.resolveOpts(r));
        List<Col> cols = List.of(c("index", "i", false), c("shard", "s", true), c("prirep", "p", false), c("state", "st", false),
                c("docs", "d", true), c("store", "sto", true), c("ip", "ip", false), c("node", "n", false));
        List<Map<String, String>> rows = new ArrayList<>();
        for (var t : ts) {
            long docs = api.store.count(t.index());
            for (int s = 0; s < SearchEngine.shardsOf(t.index()); s++) {
                rows.add(row("index", t.index().name, "shard", String.valueOf(s), "prirep", "p", "state", "STARTED",
                        "docs", s == 0 ? String.valueOf(docs) : "0", "store", bytes(api.store.storeSizeBytes(t.index()), r.q.get("bytes")),
                        "ip", "127.0.0.1", "node", RestApi.NODE_NAME));
            }
        }
        return render(r, cols, rows);
    }

    static RestApi.Response aliases(RestApi api, RestApi.Request r) throws SQLException {
        List<Col> cols = List.of(c("alias", "a", false), c("index", "i", false), c("filter", "f", false), c("routing.index", "ri", false),
                c("routing.search", "rs", false), c("is_write_index", "w", false));
        List<Map<String, String>> rows = new ArrayList<>();
        for (var m : api.store.allMetas()) {
            for (var e : m.aliases.entrySet()) {
                if (r.var("name") != null && !Mappings.wildcardMatch(r.var("name"), e.getKey())) {
                    continue;
                }
                JsonObject d = e.getValue().isJsonObject() ? e.getValue().getAsJsonObject() : new JsonObject();
                rows.add(row("alias", e.getKey(), "index", m.name, "filter", d.has("filter") ? "*" : "-",
                        "routing.index", d.has("index_routing") ? d.get("index_routing").getAsString() : "-",
                        "routing.search", d.has("search_routing") ? d.get("search_routing").getAsString() : "-",
                        "is_write_index", d.has("is_write_index") ? d.get("is_write_index").getAsString() : "-"));
            }
        }
        rows.sort(Comparator.comparing((Map<String, String> x) -> x.get("alias")).thenComparing(x -> x.get("index")));
        return render(r, cols, rows);
    }

    static RestApi.Response nodes(RestApi api, RestApi.Request r) {
        List<Col> cols = List.of(c("ip", "i", false), c("heap.percent", "hp", true), c("ram.percent", "rp", true), c("cpu", "cpu", true),
                c("load_1m", "l", true), c("load_5m", "l5", true), c("load_15m", "l15", true), c("node.role", "r", false),
                c("node.roles", "role", false), c("cluster_manager", "cm", false), c("name", "n", false));
        return render(r, cols, new ArrayList<>(List.of(row("ip", "127.0.0.1", "heap.percent", "10", "ram.percent", "50", "cpu", "1",
                "load_1m", "0.00", "load_5m", "0.00", "load_15m", "0.00", "node.role", "dim", "node.roles", "cluster_manager,data,ingest",
                "cluster_manager", "*", "name", RestApi.NODE_NAME))));
    }

    static RestApi.Response master(RestApi api, RestApi.Request r) {
        return render(r, List.of(c("id", "id", false), c("host", "h", false), c("ip", "ip", false), c("node", "n", false)),
                new ArrayList<>(List.of(row("id", "warp-node-1", "host", "127.0.0.1", "ip", "127.0.0.1", "node", RestApi.NODE_NAME))));
    }

    static RestApi.Response templates(RestApi api, RestApi.Request r) throws SQLException {
        List<Col> cols = List.of(c("name", "n", false), c("index_patterns", "t", false), c("order", "o", true), c("version", "v", true),
                c("composed_of", "c", false));
        List<Map<String, String>> rows = new ArrayList<>();
        for (var e : api.store.templates("template").entrySet()) {
            rows.add(row("name", e.getKey(), "index_patterns", "[" + String.join(", ", Templates.patterns(e.getValue())) + "]",
                    "order", e.getValue().has("order") ? e.getValue().get("order").getAsString() : "0",
                    "version", e.getValue().has("version") ? e.getValue().get("version").getAsString() : "", "composed_of", ""));
        }
        for (var e : api.store.templates("index_template").entrySet()) {
            rows.add(row("name", e.getKey(), "index_patterns", "[" + String.join(", ", Templates.patterns(e.getValue())) + "]",
                    "order", e.getValue().has("priority") ? e.getValue().get("priority").getAsString() : "0",
                    "version", e.getValue().has("version") ? e.getValue().get("version").getAsString() : "",
                    "composed_of", e.getValue().has("composed_of") ? e.getValue().get("composed_of").toString().replace("\"", "") : "[]"));
        }
        if (r.var("name") != null) {
            rows.removeIf(x -> !Mappings.wildcardMatch(r.var("name"), x.get("name")));
        }
        return render(r, cols, rows);
    }

    static RestApi.Response plugins(RestApi api, RestApi.Request r) {
        return render(r, List.of(c("name", "n", false), c("component", "c", false), c("version", "v", false)), new ArrayList<>());
    }

    static RestApi.Response allocation(RestApi api, RestApi.Request r) throws SQLException {
        int shards = 0;
        for (var t : api.store.resolve(null, new PostgresSearchStore.ResolveOpts())) {
            shards += SearchEngine.shardsOf(t.index());
        }
        return render(r, List.of(c("shards", "s", true), c("disk.indices", "di", true), c("disk.used", "du", true), c("disk.avail", "da", true),
                c("disk.total", "dt", true), c("disk.percent", "dp", true), c("host", "h", false), c("ip", "ip", false), c("node", "n", false)),
                new ArrayList<>(List.of(row("shards", String.valueOf(shards), "disk.indices", "0b", "disk.used", "0b", "disk.avail", "0b",
                        "disk.total", "0b", "disk.percent", "0", "host", "127.0.0.1", "ip", "127.0.0.1", "node", RestApi.NODE_NAME))));
    }
}
