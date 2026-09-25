package com.sayonora.wire.core;

import com.sayonora.wire.cluster.WarpCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;

/** {@link SqlPlanStore} backed by its own, separate Ignite cache -- {@code
 * "warp-federation-plan-cache"} -- so every Warp instance in a real cluster
 * ({@code WARP_CLUSTER_ENABLED=true}, not just the default single-node cache-only Ignite grid
 * every instance already runs for {@code CacheStage}'s own sake) sees the SAME federated-query
 * plan history, regardless of which instance actually ran each query -- see {@link
 * StatisticsStore}'s own matching javadoc for the identical reasoning applied to row-count
 * statistics. Ported directly from the sibling Omnigate project's own class of the same name/shape
 * (real, tested, production code there) -- {@link WarpCluster#nextSequence} gives every
 * instance a globally-unique, monotonically-increasing plan ID the same way {@link
 * WarpCluster#getOrCreateCache} gives every instance the same backing cache. */
final class ClusterSqlPlanStore implements SqlPlanStore {

    private final WarpCluster cluster;
    private final int capacity;
    private final IgniteCache<String, byte[]> cache;
    private static final String SEQUENCE_NAME = "warp-federation-plan-id";
    private static final String CACHE_NAME = "warp-federation-plan-cache";

    ClusterSqlPlanStore(WarpCluster cluster, int capacity, long ttlMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("ClusterSqlPlanStore capacity must be positive, got " + capacity);
        }
        this.cluster = cluster;
        this.capacity = capacity;
        this.cache = cluster.getOrCreateCache(CACHE_NAME, ttlMillis);
    }

    @Override
    public long record(String backends, String sqlText, String planText, long elapsedMillis, long rowCount,
            boolean success, String errorMessage, List<LeafScanMetric> leafScans) {
        long id = cluster.nextSequence(SEQUENCE_NAME);
        PlanEntry entry = new PlanEntry(id, Instant.now(), backends, sqlText, planText, elapsedMillis, rowCount,
                success, errorMessage, leafScans);
        cache.put(String.valueOf(id), serialize(entry));
        return id;
    }

    /** Real cross-instance read -- {@link ScanQuery} visits every entry this cache currently holds
     * ANYWHERE in the cluster, not just what this one node itself wrote, then sorts and trims to
     * {@code capacity} here (Ignite's own TTL, not a capacity-based eviction, is what actually
     * bounds how long an entry lives -- see this class's own constructor). */
    @Override
    public List<PlanEntry> snapshot() {
        List<PlanEntry> entries = new ArrayList<>();
        try (var cursor = cache.query(new ScanQuery<String, byte[]>())) {
            for (var kv : cursor) {
                entries.add(deserialize(kv.getValue()));
            }
        }
        entries.sort(Comparator.comparingLong(PlanEntry::planId).reversed());
        return entries.size() > capacity ? entries.subList(0, capacity) : entries;
    }

    private static byte[] serialize(PlanEntry entry) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(entry);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize sql plan entry", e);
        }
    }

    private static PlanEntry deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (PlanEntry) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException("failed to deserialize sql plan entry", new IOException(e));
        }
    }
}
