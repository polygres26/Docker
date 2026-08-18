package com.polygres.wire.stats;

import com.polygres.wire.cluster.PolyWireCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCache;

/**
 * Cluster-shared table row-count statistics — the piece that answers "how does another PolyWire
 * server in a cluster see the same numbers": same dual-mode shape every other cluster-shared store
 * in this codebase already uses ({@code ConversationStore}, {@code ClusterSqlPlanStore}, {@code
 * SessionStore}, {@code OntologyStore}) — a local {@link ConcurrentHashMap} when no cluster is
 * configured, or its own Ignite cache ({@code polywire-table-stats}, deliberately separate from
 * every other cache/region for the same "one region can't serve two different eviction/access
 * patterns well" reasoning {@code SqlPlanStore}'s javadoc documents) when one is — so a statistic
 * collected by whichever node happened to run {@link StatisticsScheduler}'s pass is visible to every
 * node's {@link com.polygres.wire.core.FederationStage} planner, not just the one that collected it.
 *
 * <p>TTL-bounded (default 24h, {@code POLYWIRE_STATS_TTL_MS}), not versioned/invalidated on write —
 * a stale statistic (source data changed since last collection) degrades to a worse cost estimate,
 * not a wrong query result; {@link StatisticsScheduler}'s periodic re-collection is what keeps this
 * fresh, same "detective, not preventive" posture the TTL choice for {@code ClusterSqlPlanStore}'s
 * plan history already uses.
 */
public final class StatisticsStore {

    private static final String CACHE_NAME = "polywire-table-stats";
    private static final long DEFAULT_TTL_MILLIS = 24L * 60 * 60 * 1000;

    private final Map<String, TableStatistics> local;
    private final IgniteCache<String, byte[]> clusterCache;

    public StatisticsStore() {
        this(null, DEFAULT_TTL_MILLIS);
    }

    public StatisticsStore(PolyWireCluster cluster) {
        this(cluster, ttlFromEnvOrDefault());
    }

    StatisticsStore(PolyWireCluster cluster, long ttlMillis) {
        if (cluster != null && cluster.enabled()) {
            this.clusterCache = cluster.getOrCreateCache(CACHE_NAME, ttlMillis);
            this.local = null;
        } else {
            this.clusterCache = null;
            this.local = new ConcurrentHashMap<>();
        }
    }

    private static long ttlFromEnvOrDefault() {
        String raw = System.getenv("POLYWIRE_STATS_TTL_MS");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_TTL_MILLIS;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_TTL_MILLIS;
        }
    }

    private boolean clustered() {
        return clusterCache != null;
    }

    public void put(TableStatistics stats) {
        if (clustered()) {
            clusterCache.put(stats.qualifiedTableName(), serialize(stats));
        } else {
            local.put(stats.qualifiedTableName(), stats);
        }
    }

    /** {@code null} if never collected (or evicted by TTL) — callers must treat that as "no opinion," falling back to Calcite's own default (see {@code FederationStage}'s Statistic wrapper), not as zero rows. */
    public TableStatistics get(String qualifiedTableName) {
        if (clustered()) {
            byte[] bytes = clusterCache.get(qualifiedTableName);
            return bytes == null ? null : deserialize(bytes);
        }
        return local.get(qualifiedTableName);
    }

    private static byte[] serialize(TableStatistics stats) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(stats);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize table statistics", e);
        }
    }

    private static TableStatistics deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (TableStatistics) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException("failed to deserialize table statistics", new IOException(e));
        }
    }
}
