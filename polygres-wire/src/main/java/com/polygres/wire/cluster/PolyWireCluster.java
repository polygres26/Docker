package com.polygres.wire.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §12: the one embedded dependency (Apache Ignite, Apache-2.0) covering cluster
 * membership, the Coherence-style distributed cache ({@link com.polygres.wire.cluster.CacheStage}), and
 * cluster-size-aware QoS reconciliation ({@code QosControlStage}) — chosen over a hand-rolled
 * gossip protocol (ProxySQL's model) plus a separate cache, and over requiring an external
 * ZooKeeper/etcd (ShardingSphere's model), for the reasons laid out in §12.2.
 *
 * <p>Opt-in and fully backward compatible: {@link #disabled()} is what every existing
 * single-node deployment gets (no {@code POLYWIRE_CLUSTER_ENABLED=true}), and it behaves as
 * "cluster size 1, no cache, no Ignite process started at all" — nothing about not opting in
 * changes any existing behavior or startup cost.
 */
public final class PolyWireCluster {

    private static final Logger log = LoggerFactory.getLogger(PolyWireCluster.class);

    private final Ignite ignite; // null when disabled
    private final ScheduledExecutorService qosPublisher;

    private PolyWireCluster(Ignite ignite) {
        this.ignite = ignite;
        this.qosPublisher = ignite == null ? null : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-qos-publish");
            t.setDaemon(true);
            return t;
        });
    }

    public static PolyWireCluster disabled() {
        return new PolyWireCluster(null);
    }

    /**
     * {@code POLYWIRE_CLUSTER_ENABLED=true} to opt in. {@code POLYWIRE_CLUSTER_SEED_NODES}:
     * comma-separated {@code host:port} list for {@link TcpDiscoverySpi}'s static IP finder — the
     * simple, dependency-free discovery mode (§12.2), not multicast (unreliable in most container
     * networks) and not a ZooKeeper-backed SPI (a real future option, not wired up here).
     */
    public static PolyWireCluster fromEnv() {
        boolean enabled = "true".equalsIgnoreCase(System.getenv("POLYWIRE_CLUSTER_ENABLED"));
        if (!enabled) {
            return disabled();
        }
        String seedSpec = System.getenv("POLYWIRE_CLUSTER_SEED_NODES");
        List<String> seeds = new ArrayList<>();
        if (seedSpec != null && !seedSpec.isBlank()) {
            for (String entry : seedSpec.split(",")) {
                String trimmed = entry.trim();
                if (!trimmed.isEmpty()) {
                    seeds.add(trimmed);
                }
            }
        }
        return start(seeds.isEmpty() ? List.of("127.0.0.1:47500") : seeds);
    }

    /**
     * Starts a single-node embedded Ignite instance with no seed nodes configured, bypassing the
     * {@code POLYWIRE_CLUSTER_ENABLED} env-var gate — for {@code Main} to use when a cache table
     * was configured ({@code POLYWIRE_CACHE_TABLES}) but full multi-node clustering wasn't opted
     * into. {@link CacheStage} needs somewhere to put entries either way; this gets it one without
     * requiring an operator to also flip on cluster membership they don't want.
     */
    public static PolyWireCluster startSingleNodeForCacheOnly() {
        // TcpDiscoveryVmIpFinder's non-shared mode requires at least one registered address even
        // for a lone node — "127.0.0.1:47500" is Ignite's own documented default discovery port
        // for exactly this single-node case.
        return start(List.of("127.0.0.1:47500"));
    }

    private static PolyWireCluster start(List<String> seeds) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(seeds);
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("polywire");
        cfg.setDiscoverySpi(discoverySpi);
        // This process only ever wants the grid, not a full-blown server surface — cuts unrelated
        // Ignite subsystems (SQL, ODBC/JDBC/thin-client listeners) that PolyWire has no use for.
        cfg.setClientMode(false);

        log.info("starting embedded Ignite node for cluster membership, seed nodes: {}", seeds);
        Ignite ignite = Ignition.start(cfg);
        log.info("polywire cluster joined, current size={}", ignite.cluster().nodes().size());
        return new PolyWireCluster(ignite);
    }

    public boolean enabled() {
        return ignite != null;
    }

    /** {@code 1} when clustering is disabled — the correct "no division" value for QoS reconciliation (§12.4). */
    public int clusterSize() {
        return ignite == null ? 1 : Math.max(1, ignite.cluster().nodes().size());
    }

    /**
     * One named cache per {@code (name, ttlMillis)} combination a caller asks for — {@code
     * CacheStage} uses this for its result cache. {@code PARTITIONED} mode (Coherence's default
     * shape too, §12.3): the keyspace can outgrow one node's memory, unlike a {@code REPLICATED}
     * cache. {@code backups=1} so a single node dying doesn't cold-start every hot key it owned.
     *
     * <p><b>No near-cache, found live, not per the original design</b>: §12.3 originally assumed
     * a near-cache would make a same-node repeat read never cross the network. Ignite rejects that
     * outright here — {@code "Failed to start near cache (local node is an affinity node for
     * cache)"} — because every PolyWire node is symmetric: a full Ignite data/affinity node for
     * this cache, not a thin client. Near caches are for nodes that <em>don't</em> already store a
     * share of the cache's own partitions; ours all do. Every cache lookup here genuinely crosses
     * the (fast, local-network) grid unless the requesting node happens to own that key's
     * partition — a real, documented correctness-neutral gap from the original plan's stated
     * benefit, not silently dropped.
     */
    public <T> IgniteCache<String, T> getOrCreateCache(String name, long ttlMillis) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        CacheConfiguration<String, T> cfg = new CacheConfiguration<>(name);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(1);
        if (ttlMillis > 0) {
            cfg.setExpiryPolicyFactory(javax.cache.expiry.CreatedExpiryPolicy.factoryOf(
                    new javax.cache.expiry.Duration(TimeUnit.MILLISECONDS, ttlMillis)));
        }
        return ignite.getOrCreateCache(cfg);
    }

    /**
     * Cluster-wide monotonically increasing counter — every node calling {@code
     * nextSequence("polywire-sql-plan-id")} gets a distinct value, batched internally by Ignite's
     * {@code IgniteAtomicSequence} for throughput (each node reserves a range locally rather than
     * round-tripping the grid per call). Used by {@link com.polygres.wire.core.ClusterSqlPlanStore} so a
     * {@code plan_id} is unique across the whole cluster, not just this process — unlike a plain
     * {@code AtomicLong}, which would collide the moment a second node assigned the same id to a
     * different query.
     */
    public long nextSequence(String name) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        return ignite.atomicSequence(name, 0L, true).incrementAndGet();
    }

    /**
     * ARCHITECTURE.md §12.4 v1: publishes this node's local admitted/rejected counts for a
     * bucket-key every {@code intervalMillis}, purely for cluster-wide observability — never
     * consulted by the admission decision itself, which stays a pure local memory operation.
     * A no-op when clustering is disabled.
     */
    public void publishQosCountersPeriodically(String nodeLocalKey,
            java.util.function.Supplier<long[]> admittedRejectedSnapshot, long intervalMillis) {
        if (ignite == null) {
            return;
        }
        IgniteCache<String, long[]> counters = ignite.getOrCreateCache(
                new CacheConfiguration<String, long[]>("polywire-qos-counters").setCacheMode(CacheMode.REPLICATED));
        qosPublisher.scheduleWithFixedDelay(() -> {
            try {
                counters.put(nodeLocalKey, admittedRejectedSnapshot.get());
            } catch (Exception e) {
                log.debug("qos counter publish failed (non-fatal): {}", e.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        if (qosPublisher != null) {
            qosPublisher.shutdownNow();
        }
        if (ignite != null) {
            ignite.close();
        }
    }
}
