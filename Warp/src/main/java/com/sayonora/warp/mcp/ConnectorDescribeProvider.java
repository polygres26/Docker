package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.core.connector.cassandra.CassandraSchemaFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Kafka / Cassandra / Splunk backends: these are reachable as federated SQL sources (through
 * {@code query_federated}), and have no data tools of their own on the MCP endpoint. This provider
 * only makes them describable: {@code describe_backend} lists their live catalogue where the
 * connector exposes one (Kafka topics via the admin client, Cassandra keyspaces/tables via
 * {@code system_schema}) plus the tables the operator declared on the backend URL.
 */
final class ConnectorDescribeProvider implements BackendToolProvider {

    private static final Set<String> SYSTEM_KEYSPACES = Set.of("system", "system_schema", "system_auth",
            "system_distributed", "system_traces", "system_views", "system_virtual_schema");

    private final BackendKind kind;

    ConnectorDescribeProvider(BackendKind kind) {
        this.kind = kind;
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
        return Outcome.error("UnsupportedOperation: " + kind.id() + " backends have no MCP data tools; query them as "
                + "federated SQL sources with query_federated (see describe_backend for what they contain)");
    }

    @Override
    public JsonObject describe(Ctx ctx) throws Exception {
        Map<String, Object> operand = ExternalClients.operand(ctx.backend().target());
        JsonObject out = new JsonObject();
        JsonObject declared = new JsonObject();
        if (operand.get("tables") instanceof Map<?, ?> tables) {
            tables.forEach((name, def) -> declared.addProperty(String.valueOf(name), String.valueOf(def)));
        }
        out.add("declaredTables", declared);
        switch (kind) {
            case KAFKA -> {
                Properties p = new Properties();
                p.put("bootstrap.servers", String.valueOf(operand.get("bootstrapServers")));
                p.put("request.timeout.ms", "8000");
                p.put("default.api.timeout.ms", "8000");
                try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(p)) {
                    List<String> topics = new ArrayList<>(admin.listTopics().names().get(10, TimeUnit.SECONDS));
                    java.util.Collections.sort(topics);
                    JsonArray arr = new JsonArray();
                    topics.forEach(arr::add);
                    out.add("topics", arr);
                }
            }
            case CASSANDRA -> {
                try (com.datastax.oss.driver.api.core.CqlSession session = CassandraSchemaFactory.buildSession(operand)) {
                    Map<String, List<String>> byKeyspace = new TreeMap<>();
                    for (var row : session.execute("SELECT keyspace_name, table_name FROM system_schema.tables")) {
                        String ks = row.getString("keyspace_name");
                        if (!SYSTEM_KEYSPACES.contains(ks)) {
                            byKeyspace.computeIfAbsent(ks, k -> new ArrayList<>()).add(row.getString("table_name"));
                        }
                    }
                    JsonObject keyspaces = new JsonObject();
                    byKeyspace.forEach((ks, ts) -> {
                        java.util.Collections.sort(ts);
                        JsonArray arr = new JsonArray();
                        ts.forEach(arr::add);
                        keyspaces.add(ks, arr);
                    });
                    out.add("keyspaces", keyspaces);
                }
            }
            default -> out.addProperty("note", "Splunk has no cheap catalogue: only the searches declared on the "
                    + "backend URL are listed (declaredTables)");
        }
        return out;
    }
}
