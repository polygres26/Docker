package com.sayonora.wire.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.StoreType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.TreeMap;

/**
 * Describe-only provider for the Warp-hosted stores that have no MCP data tools of their own yet
 * (SQS queues, OpenSearch indexes, the Neo4j graph): they are listed as typed stores of the
 * backend(s) that host them and {@code describe_backend} shows what they contain, read straight
 * from the hosting Postgres (fixed catalog/metadata queries only -- never caller SQL). Data access
 * is through the store's own wire protocol (sqswire / oswire / boltwire).
 */
final class StoreDescribeProvider implements BackendToolProvider {

    private final BackendKind kind;
    private final StoreType store;
    private final BackendRegistry registry;

    StoreDescribeProvider(BackendKind kind, StoreType store, BackendRegistry registry) {
        this.kind = kind;
        this.store = store;
        this.registry = registry;
    }

    @Override
    public BackendKind kind() {
        return kind;
    }

    @Override
    public List<Tool> tools() {
        return List.of();
    }

    @Override
    public Outcome call(String tool, JsonObject args, Ctx ctx) {
        return Outcome.error("UnsupportedOperation: the " + kind.id() + " store has no MCP data tools; use its wire "
                + "protocol (see describe_backend for what it contains)");
    }

    @Override
    public JsonObject describe(Ctx ctx) throws Exception {
        List<String> hosts = registry.storeHosts(store);
        JsonObject out = new JsonObject();
        JsonArray hostArr = new JsonArray();
        hosts.forEach(hostArr::add);
        out.add("hosts", hostArr);
        out.addProperty("sharded", hosts.size() > 1);
        switch (store) {
            case SQS -> {
                String home = registry.storeHome(store);
                JsonArray queues = new JsonArray();
                if (home != null) {
                    try (Connection c = registry.get(home).open(); Statement st = c.createStatement();
                            ResultSet rs = st.executeQuery(
                                    "SELECT queue_name, is_fifo FROM sqs_queues_catalog ORDER BY queue_name")) {
                        while (rs.next()) {
                            JsonObject q = new JsonObject();
                            q.addProperty("name", rs.getString(1));
                            q.addProperty("fifo", rs.getBoolean(2));
                            q.addProperty("shard", shardOf(hosts, rs.getString(1)));
                            queues.add(q);
                        }
                    }
                }
                out.addProperty("queueCount", queues.size());
                out.add("queues", queues);
            }
            case OPENSEARCH -> {
                TreeMap<String, Long> docs = new TreeMap<>();
                for (String h : hosts) {
                    BackendTarget t = registry.get(h);
                    try (Connection c = t.open(); Statement st = c.createStatement();
                            ResultSet rs = st.executeQuery("SELECT table_name FROM information_schema.tables "
                                    + "WHERE table_schema = 'public' AND table_name LIKE 'warp\\_search\\_%'")) {
                        List<String> tables = new java.util.ArrayList<>();
                        while (rs.next()) {
                            tables.add(rs.getString(1));
                        }
                        for (String table : tables) {
                            try (Statement cs = c.createStatement();
                                    ResultSet cr = cs.executeQuery("SELECT count(*) FROM \"" + table + "\"")) {
                                cr.next();
                                docs.merge(table.substring("warp_search_".length()), cr.getLong(1), Long::sum);
                            }
                        }
                    }
                }
                JsonArray idx = new JsonArray();
                docs.forEach((name, n) -> {
                    JsonObject o = new JsonObject();
                    o.addProperty("index", name);
                    o.addProperty("documents", n);
                    idx.add(o);
                });
                out.addProperty("indexCount", idx.size());
                out.add("indexes", idx);
            }
            case NEO4J -> {
                String home = registry.storeHome(store);
                if (home != null) {
                    try (Connection c = registry.get(home).open(); Statement st = c.createStatement()) {
                        out.addProperty("graphHost", home);
                        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM warp_graph_nodes")) {
                            rs.next();
                            out.addProperty("nodeCount", rs.getLong(1));
                        }
                        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM warp_graph_edges")) {
                            rs.next();
                            out.addProperty("relationshipCount", rs.getLong(1));
                        }
                    } catch (SQLException e) {
                        out.addProperty("note", "graph tables not readable yet: " + e.getMessage());
                    }
                }
            }
            default -> {
            }
        }
        return out;
    }

    private static String shardOf(List<String> hosts, String key) {
        return hosts.isEmpty() ? null : com.sayonora.wire.core.ShardingStrategy.hash(hosts).resolve(key);
    }
}
