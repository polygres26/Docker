package com.sayonora.warp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link NodeRegistry#stableNodeId} -- the fix for a real bug a UI review
 * surfaced: node identity used to be {@code UUID.randomUUID()} per process start, so every
 * restart of the SAME process/host permanently orphaned its previous {@code warp_nodes} row
 * (eventually swept by the 24h stale-row delete in {@code heartbeatOnce}, but inflating the node
 * count and Overview's stale-node risk list in the meantime). Identity is now deterministic from
 * (host, adminPort) -- the same pair {@code MetricsServer#fanOutToPeers}/{@code
 * listLivePeerWorkers} already use to recognize "this is my own row" -- so a restart's heartbeat
 * upsert naturally reuses the existing row instead of inserting a new one.
 */
class NodeRegistryTest {

    @Test
    void sameHostAndPortAlwaysProduceTheSameId() {
        UUID first = NodeRegistry.stableNodeId("host-a", 19090);
        UUID second = NodeRegistry.stableNodeId("host-a", 19090);
        assertEquals(first, second, "a restart on the same host/port must reuse the same node_id");
    }

    @Test
    void aDifferentPortIsADifferentId() {
        assertNotEquals(NodeRegistry.stableNodeId("host-a", 19090), NodeRegistry.stableNodeId("host-a", 19091));
    }

    @Test
    void aDifferentHostIsADifferentId() {
        assertNotEquals(NodeRegistry.stableNodeId("host-a", 19090), NodeRegistry.stableNodeId("host-b", 19090));
    }

    @Test
    void idIsAValidUuidV3NameBasedValue() {
        // Not load-bearing for correctness, just confirms this is a real deterministic name-based
        // UUID (RFC 4122 version 3) rather than an accidental fixed constant.
        UUID id = NodeRegistry.stableNodeId("host-a", 19090);
        assertEquals(3, id.version());
    }
}
