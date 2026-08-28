package com.nexagres.wire.core;

import com.nexagres.wire.cluster.PolyWireCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real row-count statistics for federated (cross-shard/cross-backend) query planning -- without
 * this, every table Calcite mounts for {@link ShardJoinExecutor}/{@link SchemaFederationStage} gets
 * its own default {@code Statistics.UNKNOWN}, which starves the planner's join-order cost model of
 * the one signal that matters most for picking a sane join order (build the hash table from the
 * smaller side, not whichever side happens to appear first in the query text). Ported from the
 * sibling Omnigate project's own {@code StatisticsStore} (real, tested, production code there),
 * simplified to row counts only (no column NDV/selectivity -- that only ever fed Omnigate's own
 * opt-in embedded-planner cost path, not ported here). The local-vs-cluster split IS ported --
 * see this class's own "Cluster-shared mode" section below.
 *
 * <p><b>Real Postgres planner statistics, not a slow {@code COUNT(*)} scan</b>: {@code
 * pg_class.reltuples} is the same row-count ESTIMATE Postgres's own query planner already uses
 * internally (refreshed by {@code ANALYZE}/autovacuum) -- reading it is a single fast catalog
 * lookup, not a full table scan, and "approximate, not exact" is exactly the precision a join-order
 * decision needs.
 *
 * <p><b>Two ways an entry gets filled, both converging on the same cache</b>: {@link
 * StatisticsScheduler} proactively warms it on a background interval (see that class's own javadoc)
 * so a real cost-based decision is usually already available by the time a query runs; {@link
 * #rowCount} also probes and caches on demand (the same "found live, needed both" pattern this
 * codebase already uses elsewhere) so the very first federated query after startup -- before the
 * scheduler's first pass, or if it's not configured at all -- still gets a real number instead of
 * {@code Statistics.UNKNOWN} for its whole session.
 *
 * <p><b>TTL-bounded (default 24h, {@code POLYWIRE_STATS_TTL_MS}), not versioned/invalidated on
 * write</b> -- a stale statistic (source data changed since last collection) degrades to a worse
 * cost estimate, not a wrong query result; row counts only ever inform planning, never execution.
 *
 * <p><b>Cluster-shared mode</b>: when constructed with a real, genuinely-clustered {@link
 * PolyWireCluster} ({@code POLYWIRE_CLUSTER_ENABLED=true}, not just the default single-node
 * cache-only Ignite grid every instance already runs for {@code CacheStage}'s own sake), entries
 * live in a shared {@code IgniteCache} instead of a local {@code ConcurrentHashMap} -- every
 * PolyWire instance in the cluster sees the SAME row-count statistics, regardless of which
 * instance's {@link StatisticsScheduler} (or on-demand probe) actually collected them. Mirrors
 * {@link ClusterSqlPlanStore}'s own identical local/cluster split for plan history -- see that
 * class's javadoc for the matching reasoning.
 */
public final class StatisticsStore {

    private static final Logger log = LoggerFactory.getLogger(StatisticsStore.class);
    private static final long DEFAULT_TTL_MILLIS = 24L * 60 * 60 * 1000;
    private static final String CLUSTER_CACHE_NAME = "polywire-federation-stats-cache";

    private record Entry(long rowCount, long collectedAtMillis) implements Serializable {
    }

    private final ConcurrentHashMap<String, Entry> local;
    private final IgniteCache<String, byte[]> clusterCache;
    private final long ttlMillis;

    public StatisticsStore() {
        this(ttlFromEnvOrDefault());
    }

    public StatisticsStore(long ttlMillis) {
        this(null, ttlMillis);
    }

    /** @param cluster {@code null}, or not genuinely clustered, means plain local caching (same
     *     behavior as before this constructor existed); a real cluster means every instance shares
     *     the same statistics via Ignite instead. */
    public StatisticsStore(PolyWireCluster cluster, long ttlMillis) {
        this.ttlMillis = ttlMillis;
        if (cluster != null && cluster.enabled()) {
            this.local = null;
            this.clusterCache = cluster.getOrCreateCache(CLUSTER_CACHE_NAME, ttlMillis);
        } else {
            this.local = new ConcurrentHashMap<>();
            this.clusterCache = null;
        }
    }

    private boolean clustered() {
        return clusterCache != null;
    }

    /** Public wrapper so callers (e.g. {@code Main}) that need to pass an explicit {@link
     * PolyWireCluster} into the constructor can still get the same env-configured TTL the
     * no-cluster constructor uses by default. */
    public static long ttlFromEnvOrDefaultPublic() {
        return ttlFromEnvOrDefault();
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

    /** @return a real row-count estimate for {@code schema.table} on the backend {@code
     *     connection} is already open against, or {@code null} if it can't be determined (the
     *     table doesn't exist yet, {@code reltuples} hasn't been populated by a real
     *     {@code ANALYZE} yet, TTL-expired with no fresher collection since, or any other real
     *     failure) -- {@code null}, not a made-up default, so the caller degrades to Calcite's own
     *     {@code Statistics.UNKNOWN} rather than mislead the planner with a fabricated number. */
    Long rowCount(Connection connection, String cacheKey, String schema, String table) {
        Entry cached = getEntry(cacheKey);
        if (cached != null && System.currentTimeMillis() - cached.collectedAtMillis() < ttlMillis) {
            return cached.rowCount();
        }
        Long fresh = probe(connection, schema, table);
        if (fresh != null) {
            putEntry(cacheKey, new Entry(fresh, System.currentTimeMillis()));
            return fresh;
        }
        // A stale-but-present entry beats no entry at all once TTL has lapsed and a fresh probe
        // failed (e.g. a transient connection error) -- degrades the cost estimate a little
        // further, still strictly better than Statistics.UNKNOWN.
        return cached != null ? cached.rowCount() : null;
    }

    /** Package-visible so {@link StatisticsScheduler} can proactively warm this cache without
     * needing a live query's own connection/cache-key convention. */
    void put(String cacheKey, long rowCount) {
        putEntry(cacheKey, new Entry(rowCount, System.currentTimeMillis()));
    }

    private Entry getEntry(String cacheKey) {
        if (clustered()) {
            byte[] bytes = clusterCache.get(cacheKey);
            return bytes == null ? null : deserialize(bytes);
        }
        return local.get(cacheKey);
    }

    private void putEntry(String cacheKey, Entry entry) {
        if (clustered()) {
            clusterCache.put(cacheKey, serialize(entry));
        } else {
            local.put(cacheKey, entry);
        }
    }

    private static byte[] serialize(Entry entry) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(entry);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize statistics entry", e);
        }
    }

    private static Entry deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (Entry) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException("failed to deserialize statistics entry", new IOException(e));
        }
    }

    private Long probe(Connection connection, String schema, String table) {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE c.relname = ? AND n.nspname = ?")) {
            ps.setString(1, table);
            ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                long estimate = rs.getLong(1);
                // reltuples is -1 for a table ANALYZE has never touched -- a real, honest "no
                // estimate yet" signal, not a row count to report as zero (which would tell the
                // planner this table is empty and bias join order just as badly as a fabricated
                // number would).
                return estimate < 0 ? null : estimate;
            }
        } catch (SQLException e) {
            log.debug("statistics: row-count probe failed for {}.{} ({}) -- Calcite falls back to "
                    + "Statistics.UNKNOWN for this table, query correctness is unaffected", schema, table, e.toString());
            return null;
        }
    }
}
