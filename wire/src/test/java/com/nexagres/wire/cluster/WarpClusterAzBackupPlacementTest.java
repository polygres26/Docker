package com.nexagres.wire.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real, live proof of gap #2 from the multi-AZ hardening pass (see {@link WarpCluster}'s
 * class javadoc): starts three real embedded Ignite nodes in this JVM (same mechanism {@link
 * WarpCluster#start} uses, replicated inline here rather than spawning OS processes so a plain
 * {@code mvn test} run proves it, no shell orchestration needed) tagged az-a/az-b/az-a — two
 * nodes sharing an AZ, one different, exactly the shape the task called for — populates real cache
 * entries, then uses Ignite's own {@link Affinity#mapKeyToPrimaryAndBackups} to show that no key's
 * backup ever lands on a node sharing {@link WarpCluster#AZ_ATTRIBUTE} with its primary.
 */
class WarpClusterAzBackupPlacementTest {

    private Ignite nodeA1;
    private Ignite nodeB;
    private Ignite nodeA2;

    @AfterEach
    void tearDown() {
        for (Ignite ignite : List.of(nodeA1, nodeB, nodeA2)) {
            if (ignite != null) {
                try {
                    ignite.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    @Test
    void backupNeverSharesAzWithPrimary() throws Exception {
        List<String> seeds = List.of("127.0.0.1:47510", "127.0.0.1:47511", "127.0.0.1:47512");

        nodeA1 = startNode("warp-test-node-a1", 47510, seeds, "az-a");
        nodeB = startNode("warp-test-node-b", 47511, seeds, "az-b");
        nodeA2 = startNode("warp-test-node-a2", 47512, seeds, "az-a");

        assertEquals(3, nodeA1.cluster().nodes().size(), "all three test nodes must have joined one cluster");

        CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<>("az-placement-test-cache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        RendezvousAffinityFunction affinity = new RendezvousAffinityFunction();
        affinity.setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter(WarpCluster.AZ_ATTRIBUTE));
        cacheCfg.setAffinity(affinity);

        IgniteCache<String, String> cache = nodeA1.getOrCreateCache(cacheCfg);

        // Real entries, not just affinity-mapped keys never actually written.
        int numKeys = 50;
        for (int i = 0; i < numKeys; i++) {
            cache.put("key-" + i, "value-" + i);
        }

        Affinity<String> cacheAffinity = nodeA1.affinity("az-placement-test-cache");
        int checked = 0;
        for (int i = 0; i < numKeys; i++) {
            String key = "key-" + i;
            Collection<ClusterNode> mappingRaw = cacheAffinity.mapKeyToPrimaryAndBackups(key);
            List<ClusterNode> mapping = new ArrayList<>(mappingRaw);
            assertEquals(2, mapping.size(), "expected 1 primary + 1 backup for key " + key);
            ClusterNode primary = mapping.get(0);
            ClusterNode backup = mapping.get(1);
            String primaryAz = (String) primary.attribute(WarpCluster.AZ_ATTRIBUTE);
            String backupAz = (String) backup.attribute(WarpCluster.AZ_ATTRIBUTE);
            assertNotEquals(primary.id(), backup.id(), "primary and backup must be different nodes for key " + key);
            assertNotEquals(primaryAz, backupAz,
                    "key " + key + ": backup (" + backupAz + ") must never share an AZ with primary (" + primaryAz + ")");
            checked++;
        }
        assertTrue(checked == numKeys, "sanity: every key was actually checked");
    }

    private static Ignite startNode(String instanceName, int localDiscoveryPort, List<String> seeds, String az) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(seeds);
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);
        discoverySpi.setLocalPort(localDiscoveryPort);
        discoverySpi.setLocalPortRange(0);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(instanceName);
        cfg.setDiscoverySpi(discoverySpi);
        cfg.setClientMode(false);
        Map<String, String> attrs = new HashMap<>();
        attrs.put(WarpCluster.AZ_ATTRIBUTE, az);
        cfg.setUserAttributes(attrs);

        return Ignition.start(cfg);
    }
}
