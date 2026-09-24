package com.sayonora.wire.mcp;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.dynamowire.DynamoWireServer;
import com.sayonora.wire.influxwire.InfluxEmbedded;
import com.sayonora.wire.mongowire.MongoWireEmbedded;

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

    public EmulatedStores(BackendRegistry registry) {
        this.registry = registry;
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
