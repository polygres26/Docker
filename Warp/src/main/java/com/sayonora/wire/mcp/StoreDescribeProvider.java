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
 * (SQS queues, OpenSearch indexes, the Neo4j graph, S3 buckets with object counts and bytes per shard): they are listed as typed stores of the
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
                // index names that are not plain identifiers (my-logs-2024.01.01) live in warp_os_catalog; the table
                // name is only a sanitised form of them
                java.util.Map<String, String> tableToIndex = new java.util.HashMap<>();
                String osHome = registry.storeHome(store);
                if (osHome != null) {
                    try (Connection c = registry.get(osHome).open(); Statement st = c.createStatement();
                            ResultSet rs = st.executeQuery("SELECT table_name, name FROM warp_os_catalog")) {
                        while (rs.next()) {
                            tableToIndex.put(rs.getString(1), rs.getString(2));
                        }
                    } catch (SQLException noCatalogYet) {
                        // no index created through the catalog yet: fall back to table names below
                    }
                }
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
                                docs.merge(tableToIndex.getOrDefault(table, table.substring("warp_search_".length())), cr.getLong(1), Long::sum);
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
            case FIRESTORE, DATASTORE -> {
                boolean fs = store == com.sayonora.wire.core.StoreType.FIRESTORE;
                String sql = fs ? "SELECT db, '', count(*) FROM warp_firestore_docs GROUP BY 1 ORDER BY 1"
                        : "SELECT ns, kind, count(*) FROM warp_datastore_entities GROUP BY 1, 2 ORDER BY 1, 2";
                java.util.TreeMap<String, Long> counts = new java.util.TreeMap<>();
                for (String h : hosts) {
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                        while (rs.next()) {
                            counts.merge(rs.getString(1) + (rs.getString(2).isEmpty() ? "" : " / " + rs.getString(2)), rs.getLong(3), Long::sum);
                        }
                    } catch (SQLException e) {
                        out.addProperty("note", (fs ? "firestore" : "datastore") + " tables not readable yet: " + e.getMessage());
                    }
                }
                JsonArray arr = new JsonArray();
                for (var e : counts.entrySet()) {
                    JsonObject o = new JsonObject();
                    o.addProperty(fs ? "database" : "namespaceKind", e.getKey());
                    o.addProperty(fs ? "documents" : "entities", e.getValue());
                    arr.add(o);
                }
                out.add(fs ? "databases" : "namespaces", arr);
            }
            case GREMLIN -> {
                // vertices / edges per host and label counts (a vertex lives on one host, an edge with its out-vertex)
                java.util.TreeMap<String, long[]> vlabels = new java.util.TreeMap<>();
                java.util.TreeMap<String, long[]> elabels = new java.util.TreeMap<>();
                JsonArray shards = new JsonArray();
                long vertices = 0;
                long edges = 0;
                for (String h : hosts) {
                    JsonObject sh = new JsonObject();
                    sh.addProperty("host", h);
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement()) {
                        long hv = 0;
                        long he = 0;
                        try (ResultSet rs = st.executeQuery("SELECT label, count(*) FROM warp_gremlin_vertices GROUP BY label")) {
                            while (rs.next()) {
                                vlabels.computeIfAbsent(rs.getString(1), k -> new long[1])[0] += rs.getLong(2);
                                hv += rs.getLong(2);
                            }
                        }
                        try (ResultSet rs = st.executeQuery("SELECT label, count(*) FROM warp_gremlin_edges GROUP BY label")) {
                            while (rs.next()) {
                                elabels.computeIfAbsent(rs.getString(1), k -> new long[1])[0] += rs.getLong(2);
                                he += rs.getLong(2);
                            }
                        }
                        sh.addProperty("vertexCount", hv);
                        sh.addProperty("edgeCount", he);
                        vertices += hv;
                        edges += he;
                    } catch (SQLException e) {
                        sh.addProperty("note", "gremlin tables not readable yet: " + e.getMessage());
                    }
                    shards.add(sh);
                }
                out.addProperty("vertexCount", vertices);
                out.addProperty("edgeCount", edges);
                JsonArray vl = new JsonArray();
                vlabels.forEach((k, v) -> {
                    JsonObject o = new JsonObject();
                    o.addProperty("label", k);
                    o.addProperty("count", v[0]);
                    vl.add(o);
                });
                JsonArray el = new JsonArray();
                elabels.forEach((k, v) -> {
                    JsonObject o = new JsonObject();
                    o.addProperty("label", k);
                    o.addProperty("count", v[0]);
                    el.add(o);
                });
                out.add("vertexLabels", vl);
                out.add("edgeLabels", el);
                out.add("shards", shards);
            }
            case AZBLOB, AZQUEUE, AZTABLE, GCS, BIGTABLE, PUBSUB, CQL, KAFKA -> {
                String[][] q = switch (store) {
                    case AZBLOB -> new String[][] {{"containers", "SELECT account || '/' || name, 0, 0 FROM warp_azblob_containers ORDER BY 1"},
                        {"blobs", "SELECT account || '/' || container, count(*), coalesce(sum(size),0) FROM warp_azblob_blobs WHERE snapshot='' GROUP BY 1 ORDER BY 1"}};
                    case AZQUEUE -> new String[][] {{"queues", "SELECT account || '/' || name, 0, 0 FROM warp_azqueue_queues ORDER BY 1"},
                        {"messages", "SELECT account || '/' || queue, count(*), 0 FROM warp_azqueue_messages WHERE expires_at > now() GROUP BY 1 ORDER BY 1"}};
                    case GCS -> new String[][] {{"buckets", "SELECT name, 0, 0 FROM warp_gcs_buckets ORDER BY 1"},
                        {"objects", "SELECT bucket, count(*), coalesce(sum(size),0) FROM warp_gcs_objects WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 1"}};
                    case BIGTABLE -> new String[][] {{"tables", "SELECT name, 0, 0 FROM warp_bt_tables ORDER BY 1"},
                        {"rows", "SELECT tbl, count(DISTINCT row_key), coalesce(sum(length(val)),0) FROM warp_bt_cells GROUP BY 1 ORDER BY 1"}};
                    case CQL -> new String[][] {{"tables", "SELECT ks || '.' || name, 0, 0 FROM warp_cql_schema WHERE kind = 'table' ORDER BY 1"},
                        {"cells", "SELECT 'all tables', count(*), coalesce(sum(length(val)),0) FROM warp_cql_cells"}};
                    case KAFKA -> new String[][] {{"topics", "SELECT name, partitions, 0 FROM warp_kafka_topics ORDER BY 1"},
                        {"retainedBatches", "SELECT topic, count(*), coalesce(sum(nbytes),0) FROM warp_kafka_log GROUP BY 1 ORDER BY 1"}};
                    case PUBSUB -> new String[][] {{"topics", "SELECT name, 0, 0 FROM warp_pubsub_topics ORDER BY 1"},
                        {"subscriptionBacklog", "SELECT sub, count(*), coalesce(sum(length(data)),0) FROM warp_pubsub_msgs WHERE NOT acked GROUP BY 1 ORDER BY 1"}};
                    default -> new String[][] {{"tables", "SELECT account || '/' || name, 0, 0 FROM warp_aztable_tables ORDER BY 1"},
                        {"entities", "SELECT account || '/' || tbl, count(*), 0 FROM warp_aztable_entities GROUP BY 1 ORDER BY 1"}};
                };
                java.util.TreeMap<String, long[]> counts = new java.util.TreeMap<>();
                JsonArray names = new JsonArray();
                for (int i = 0; i < hosts.size(); i++) {
                    String h = hosts.get(i);
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement()) {
                        if (i == 0) {
                            try (ResultSet rs = st.executeQuery(q[0][1])) {
                                while (rs.next()) {
                                    names.add(rs.getString(1));
                                }
                            }
                        }
                        try (ResultSet rs = st.executeQuery(q[1][1])) {
                            while (rs.next()) {
                                long[] t = counts.computeIfAbsent(rs.getString(1), k -> new long[2]);
                                t[0] += rs.getLong(2);
                                t[1] += rs.getLong(3);
                            }
                        }
                    } catch (SQLException e) {
                        out.addProperty("note", "azure tables not readable yet: " + e.getMessage());
                    }
                }
                out.add(q[0][0], names);
                JsonArray cs = new JsonArray();
                counts.forEach((k, v) -> {
                    JsonObject o = new JsonObject();
                    o.addProperty("name", k);
                    o.addProperty("count", v[0]);
                    o.addProperty("bytes", v[1]);
                    cs.add(o);
                });
                out.add(q[1][0], cs);
            }
            case SNS, KINESIS, AWSPARAMS -> {
                // topics / streams / secrets, parameters, keys: counted on every host (each item lives on one host)
                String[][] q = switch (store) {
                    case SNS -> new String[][] {{"topics", "SELECT count(*) FROM warp_sns_topics"},
                        {"subscriptions", "SELECT count(*) FROM warp_sns_subscriptions"}};
                    case KINESIS -> new String[][] {{"streams", "SELECT count(*) FROM warp_kinesis_streams"},
                        {"records", "SELECT count(*) FROM warp_kinesis_records"}};
                    default -> new String[][] {{"secrets", "SELECT count(*) FROM warp_awsparams_secrets"},
                        {"parameters", "SELECT count(*) FROM warp_awsparams_ssm_params"},
                        {"kmsKeys", "SELECT count(*) FROM warp_awsparams_kms_keys"}};
                };
                long[] totals = new long[q.length];
                for (String h : hosts) {
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement()) {
                        for (int i = 0; i < q.length; i++) {
                            try (ResultSet rs = st.executeQuery(q[i][1])) {
                                rs.next();
                                totals[i] += rs.getLong(1);
                            }
                        }
                    } catch (SQLException e) {
                        out.addProperty("note", "tables not readable yet: " + e.getMessage());
                    }
                }
                for (int i = 0; i < q.length; i++) {
                    out.addProperty(q[i][0], totals[i]);
                }
            }
            case S3 -> {
                // per-host object/byte counts (from the fixed catalog tables); buckets come from the first host
                JsonArray shards = new JsonArray();
                java.util.TreeMap<String, long[]> perBucket = new java.util.TreeMap<>();
                for (String h : hosts) {
                    JsonObject sh = new JsonObject();
                    sh.addProperty("host", h);
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement()) {
                        try (ResultSet rs = st.executeQuery("SELECT bucket, count(*), COALESCE(sum(size), 0) "
                                + "FROM warp_s3_objects GROUP BY bucket")) {
                            long objects = 0;
                            long bytes = 0;
                            while (rs.next()) {
                                long[] t = perBucket.computeIfAbsent(rs.getString(1), k -> new long[2]);
                                t[0] += rs.getLong(2);
                                t[1] += rs.getLong(3);
                                objects += rs.getLong(2);
                                bytes += rs.getLong(3);
                            }
                            sh.addProperty("objectCount", objects);
                            sh.addProperty("totalBytes", bytes);
                        }
                        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM warp_s3_blobs "
                                + "WHERE state = 'garbage'")) {
                            rs.next();
                            sh.addProperty("garbageBlobs", rs.getLong(1));
                        }
                    } catch (SQLException e) {
                        sh.addProperty("note", "s3 tables not readable yet: " + e.getMessage());
                    }
                    shards.add(sh);
                }
                JsonArray buckets = new JsonArray();
                if (!hosts.isEmpty()) {
                    try (Connection c = registry.get(hosts.get(0)).open(); Statement st = c.createStatement();
                            ResultSet rs = st.executeQuery("SELECT name, created_at FROM warp_s3_buckets ORDER BY name")) {
                        while (rs.next()) {
                            JsonObject b = new JsonObject();
                            String name = rs.getString(1);
                            long[] t = perBucket.getOrDefault(name, new long[2]);
                            b.addProperty("name", name);
                            b.addProperty("created", rs.getTimestamp(2).toInstant().toString());
                            b.addProperty("objectCount", t[0]);
                            b.addProperty("totalBytes", t[1]);
                            buckets.add(b);
                        }
                    } catch (SQLException e) {
                        out.addProperty("note", "bucket catalog not readable yet: " + e.getMessage());
                    }
                }
                out.addProperty("bucketCount", buckets.size());
                out.add("buckets", buckets);
                out.add("shards", shards);
            }
            case REDIS -> {
                // keys per type and a rough memory estimate, per host (fixed catalog queries only)
                java.util.TreeMap<String, Long> byType = new java.util.TreeMap<>();
                long bytes = 0;
                JsonArray shards = new JsonArray();
                String[] names = {"string", "hash", "list", "set", "zset", "stream"};
                for (String h : hosts) {
                    JsonObject sh = new JsonObject();
                    sh.addProperty("host", h);
                    try (Connection c = registry.get(h).open(); Statement st = c.createStatement();
                            ResultSet rs = st.executeQuery("SELECT type, count(*), COALESCE(sum(octet_length(k) + coalesce(octet_length(sv), 0) + 48), 0) "
                                    + "FROM warp_redis_keys WHERE exp IS NULL OR exp > (extract(epoch FROM clock_timestamp()) * 1000)::bigint GROUP BY type")) {
                        long keys = 0;
                        while (rs.next()) {
                            String tn = names[Math.min(rs.getInt(1), names.length - 1)];
                            byType.merge(tn, rs.getLong(2), Long::sum);
                            keys += rs.getLong(2);
                            bytes += rs.getLong(3);
                        }
                        sh.addProperty("keys", keys);
                    } catch (SQLException e) {
                        sh.addProperty("note", "redis tables not readable yet: " + e.getMessage());
                    }
                    shards.add(sh);
                }
                JsonObject types = new JsonObject();
                byType.forEach(types::addProperty);
                out.add("keysByType", types);
                out.addProperty("keyCount", byType.values().stream().mapToLong(Long::longValue).sum());
                out.addProperty("estimatedMemoryBytes", bytes);
                out.add("shards", shards);
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
