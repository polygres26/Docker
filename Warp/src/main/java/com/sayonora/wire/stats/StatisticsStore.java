package com.sayonora.wire.stats;

import com.sayonora.wire.cluster.WarpCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCache;

public final class StatisticsStore {

    private static final String CACHE_NAME = "warp-table-stats";
    private static final long DEFAULT_TTL_MILLIS = 24L * 60 * 60 * 1000;

    private final Map<String, TableStatistics> local;
    private final IgniteCache<String, byte[]> clusterCache;

    public StatisticsStore() {
        this(null, DEFAULT_TTL_MILLIS);
    }

    public StatisticsStore(WarpCluster cluster) {
        this(cluster, ttlFromEnvOrDefault());
    }

    StatisticsStore(WarpCluster cluster, long ttlMillis) {
        if (cluster != null && cluster.enabled()) {
            this.clusterCache = cluster.getOrCreateCache(CACHE_NAME, ttlMillis);
            this.local = null;
        } else {
            this.clusterCache = null;
            this.local = new ConcurrentHashMap<>();
        }
    }

    private static long ttlFromEnvOrDefault() {
        String raw = System.getenv("WARP_STATS_TTL_MS");
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
