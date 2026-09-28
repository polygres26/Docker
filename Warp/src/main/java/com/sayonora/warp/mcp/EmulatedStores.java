package com.sayonora.warp.mcp;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.dynamowire.DynamoWireServer;
import com.sayonora.warp.influxwire.InfluxEmbedded;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.mongowire.MongoWireEmbedded;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Handles to the Warp-emulated stores (dynamowire/influxwire/mongowire data living in Postgres)
 * the kind-specific MCP tools read and write. {@code Main} injects the running dynamowire server
 * and mongowire dispatcher so MCP shares their row caches; influx needs only the registry.
 */
public final class EmulatedStores {

    private final BackendRegistry registry;
    private volatile DynamoWireServer dynamo;
    private volatile MongoWireEmbedded mongo;
    private volatile InfluxEmbedded influx;

    private volatile SqlMetricsCollector sqlMetrics;
    private final Map<String, Object> embedded = new ConcurrentHashMap<>();

    public EmulatedStores(BackendRegistry registry) {
        this.registry = registry;
    }

    /** The gateway's shared statement/operation metrics; set by {@code Main} so MCP store tools show up next to wire traffic. */
    public void setSqlMetrics(SqlMetricsCollector sqlMetrics) {
        this.sqlMetrics = sqlMetrics;
    }

    public SqlMetricsCollector sqlMetrics() {
        return sqlMetrics;
    }

    public BackendRegistry registry() {
        return registry;
    }

    /**
     * The in-process engine of a Warp-hosted store (rediswire, azurewire, gcswire, awswire, pubsubwire, firestorewire,
     * datastorewire, bigtablewire), created on first use from the backend registry and shared by every MCP tool call. A
     * new store plugs in the same way: {@code stores.engine("cassandra", reg -> new ...)}.
     */
    @SuppressWarnings("unchecked")
    public <T> T engine(String key, Function<BackendRegistry, T> factory) {
        return (T) embedded.computeIfAbsent(key, k -> factory.apply(registry));
    }

    public void setDynamo(DynamoWireServer dynamo) {
        this.dynamo = dynamo;
    }

    public void setMongo(MongoWireEmbedded mongo) {
        this.mongo = mongo;
    }

    public DynamoWireServer dynamo() {
        return dynamo;
    }

    public MongoWireEmbedded mongo() {
        return mongo;
    }

    public synchronized InfluxEmbedded influx() {
        if (influx == null && registry != null) {
            influx = new InfluxEmbedded(registry);
        }
        return influx;
    }
}
