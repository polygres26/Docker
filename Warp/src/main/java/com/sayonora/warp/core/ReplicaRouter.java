package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses a read replica for a read, and only when that replica's replication lag is currently
 * within its own {@code maxLagSeconds}. One instance per {@link BackendRegistry}.
 *
 * <p>Eligibility is deliberately strict -- a replica is used only when ALL of these hold:
 * its lag was sampled recently (a stale sample is treated as unknown, never as "fine"), the probe
 * succeeded and confirmed it really is a replica, the sampled lag is within the replica's
 * threshold, and it is not quarantined after a recent connect/read-only failure. When no replica
 * is eligible the read simply runs on the primary, exactly as it did before replicas existed.
 *
 * <p>Lag is sampled by a background thread every {@code WARP_REPLICA_LAG_CHECK_SECONDS} (default
 * 5; {@code 0} disables replica routing entirely, since nothing would ever be eligible). Only
 * engines with a {@link LagProbe} are supported; an engine without one is never eligible rather
 * than guessed at.
 */
public final class ReplicaRouter {

    private static final Logger log = LoggerFactory.getLogger(ReplicaRouter.class);

    /** One lag measurement. {@code ok=false} means the probe could not tell (unreachable, no
     * privilege, unsupported engine) and the replica must be treated as ineligible. */
    public record LagSample(boolean ok, boolean isReplica, double lagSeconds, String message, long takenAtMillis) {
    }

    @FunctionalInterface
    public interface LagProbe {
        LagSample probe(BackendTarget replica, long nowMillis);
    }

    /** A replica resolved against its primary's credentials. */
    public record Replica(String primaryName, int index, BackendTarget target, double maxLagSeconds) {
        public String id() {
            return primaryName + "#" + index;
        }

        /** Stable identity of the replica's server. Lag samples and quarantine are keyed by this,
         * NOT by {@link #id()}: after a failover the replica list is reordered, and a sample
         * measured on one server must never be attributed to whichever server inherits its index. */
        public String key() {
            return target.jdbcUrl();
        }
    }

    public enum Reason {
        ROUTED_TO_REPLICA, NOT_READ_SAFE, RECENT_WRITE, SESSION_STATE, NO_ELIGIBLE_REPLICA, REPLICA_RETRIED_ON_PRIMARY
    }

    private final BackendRegistry registry;
    private final LagProbe probe;
    private final LongSupplier clock;
    private final long intervalSeconds;
    private final long quarantineMillis;

    private final Map<String, LagSample> samples = new ConcurrentHashMap<>();
    private final Map<String, Long> quarantinedUntil = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> routedPerReplica = new ConcurrentHashMap<>();
    private final Map<String, Map<Reason, AtomicLong>> reasonsPerPrimary = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> roundRobin = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService scheduler;

    public ReplicaRouter(BackendRegistry registry) {
        this(registry, ReplicaRouter::probeByEngine, System::currentTimeMillis,
                longEnv("WARP_REPLICA_LAG_CHECK_SECONDS", 5), longEnv("WARP_REPLICA_QUARANTINE_SECONDS", 30));
    }

    ReplicaRouter(BackendRegistry registry, LagProbe probe, LongSupplier clock, long intervalSeconds,
            long quarantineSeconds) {
        this.registry = registry;
        this.probe = probe;
        this.clock = clock;
        this.intervalSeconds = intervalSeconds;
        this.quarantineMillis = quarantineSeconds * 1000L;
    }

    private static long longEnv(String name, long fallback) {
        try {
            String v = System.getenv(name);
            return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Global kill switch: {@code WARP_REPLICA_READ_ROUTING=false} keeps every read on its primary
     * even when replicas are configured. */
    public static boolean enabledByEnv() {
        return !"false".equalsIgnoreCase(System.getenv("WARP_REPLICA_READ_ROUTING"));
    }

    /** Replicas configured for {@code primaryName}, resolved against the primary's credentials. */
    public List<Replica> replicasOf(String primaryName) {
        List<ReplicaSpec> specs = registry.replicaSpecsOf(primaryName);
        if (specs.isEmpty()) {
            return List.of();
        }
        BackendTarget primary = registry.get(primaryName);
        if (primary == null) {
            return List.of();
        }
        List<Replica> out = new ArrayList<>(specs.size());
        for (int i = 0; i < specs.size(); i++) {
            ReplicaSpec s = specs.get(i);
            BackendTarget t = new BackendTarget(primaryName + "#" + (i + 1), s.url(), primary.user(), primary.password());
            out.add(new Replica(primaryName, i + 1, t, s.maxLagSeconds()));
        }
        return out;
    }

    public boolean hasReplicas(String primaryName) {
        return !registry.replicaSpecsOf(primaryName).isEmpty();
    }

    /** The next eligible replica for {@code primaryName} (round-robin among eligible ones), or
     * {@code null} when none is -- caller then uses the primary. Counts the outcome. */
    public Replica pick(String primaryName) {
        List<Replica> replicas = replicasOf(primaryName);
        if (replicas.isEmpty()) {
            return null;
        }
        List<Replica> eligible = new ArrayList<>(replicas.size());
        for (Replica r : replicas) {
            if (isEligible(r)) {
                eligible.add(r);
            }
        }
        if (eligible.isEmpty()) {
            record(primaryName, Reason.NO_ELIGIBLE_REPLICA);
            return null;
        }
        int idx = Math.floorMod(roundRobin.computeIfAbsent(primaryName, k -> new AtomicInteger()).getAndIncrement(),
                eligible.size());
        Replica chosen = eligible.get(idx);
        record(primaryName, Reason.ROUTED_TO_REPLICA);
        routedPerReplica.computeIfAbsent(chosen.key(), k -> new AtomicLong()).incrementAndGet();
        return chosen;
    }

    boolean isEligible(Replica r) {
        long now = clock.getAsLong();
        Long until = quarantinedUntil.get(r.key());
        if (until != null && now < until) {
            return false;
        }
        LagSample s = samples.get(r.key());
        if (s == null || !s.ok() || !s.isReplica()) {
            return false;
        }
        long maxAgeMillis = (Math.max(intervalSeconds, 1) * 3 + 10) * 1000L;
        if (now - s.takenAtMillis() > maxAgeMillis) {
            return false;
        }
        return s.lagSeconds() <= r.maxLagSeconds();
    }

    /** Counts a decision that kept (or returned) a read on the primary. Only meaningful for a
     * primary that has replicas -- callers check {@link #hasReplicas} first. */
    public void record(String primaryName, Reason reason) {
        reasonsPerPrimary.computeIfAbsent(primaryName, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();
    }

    /** A routed read failed on {@code replica}: stop using it for a while and let the caller retry
     * on the primary. */
    public void quarantine(Replica replica, String why) {
        quarantinedUntil.put(replica.key(), clock.getAsLong() + quarantineMillis);
        log.warn("replica routing: quarantining {} for {}s ({})", replica.id(), quarantineMillis / 1000, why);
    }

    /** Samples every configured replica once. Package-visible for tests; the scheduler calls it. */
    void probeAll() {
        List<Replica> all = new ArrayList<>();
        for (String primary : registry.allReplicaSpecs().keySet()) {
            all.addAll(replicasOf(primary));
        }
        if (all.isEmpty()) {
            return;
        }
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(4, all.size()), r -> {
            Thread t = new Thread(r, "warp-replica-lag-probe");
            t.setDaemon(true);
            return t;
        });
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (Replica r : all) {
                futures.add(pool.submit(() -> {
                    LagSample s;
                    try {
                        s = probe.probe(r.target(), clock.getAsLong());
                    } catch (RuntimeException e) {
                        s = new LagSample(false, false, 0, "probe failed: " + e.getMessage(), clock.getAsLong());
                    }
                    recordSample(r.key(), s);
                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    f.cancel(true);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** Stores a lag sample for the replica server at {@code replicaUrl}. */
    void recordSample(String replicaUrl, LagSample sample) {
        samples.put(replicaUrl, sample);
    }

    /** Starts the background sampler (idempotent). A no-op when disabled by env. */
    public synchronized void start() {
        if (scheduler != null || intervalSeconds <= 0 || !enabledByEnv()) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-replica-lag-monitor");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                probeAll();
            } catch (RuntimeException e) {
                log.warn("replica routing: lag sampling pass failed: {}", e.toString());
            }
        }, 0, intervalSeconds, TimeUnit.SECONDS);
        log.info("replica routing: lag monitor started (every {}s)", intervalSeconds);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    /** Engine dispatch. Engines without an {@link EngineHa} yield an {@code ok=false} sample, so their
     * replicas are never eligible (reads stay on the primary) instead of being routed on an
     * unmeasured lag. */
    static LagSample probeByEngine(BackendTarget replica, long nowMillis) {
        EngineHa ha = EngineHa.forDialect(replica.dialect());
        if (ha != null) {
            return ha.lag(replica, nowMillis);
        }
        return new LagSample(false, false, 0,
                "no replication-lag probe for " + replica.dialect() + " yet -- replica not used for reads", nowMillis);
    }

    // ---- observability ---------------------------------------------------------------------

    public Map<String, LagSample> samplesSnapshot() {
        return Map.copyOf(samples);
    }

    public long routedCount(String replicaUrl) {
        AtomicLong c = routedPerReplica.get(replicaUrl);
        return c == null ? 0 : c.get();
    }

    public Map<Reason, Long> reasonCounts(String primaryName) {
        Map<Reason, Long> out = new java.util.EnumMap<>(Reason.class);
        Map<Reason, AtomicLong> m = reasonsPerPrimary.get(primaryName);
        if (m != null) {
            m.forEach((k, v) -> out.put(k, v.get()));
        }
        return out;
    }

    public boolean isQuarantined(Replica r) {
        Long until = quarantinedUntil.get(r.key());
        return until != null && clock.getAsLong() < until;
    }

    public boolean eligible(Replica r) {
        return isEligible(r);
    }

    /** Status document for {@code GET /api/replicas}: per primary with replicas, each replica's
     * latest lag sample, whether it is currently eligible, and how reads have been routed. */
    public com.google.gson.JsonObject toJson() {
        com.google.gson.JsonObject root = new com.google.gson.JsonObject();
        root.addProperty("enabled", enabledByEnv() && intervalSeconds > 0);
        root.addProperty("lagCheckSeconds", intervalSeconds);
        long now = clock.getAsLong();
        com.google.gson.JsonArray primaries = new com.google.gson.JsonArray();
        for (String primary : registry.allReplicaSpecs().keySet()) {
            com.google.gson.JsonObject po = new com.google.gson.JsonObject();
            po.addProperty("primary", primary);
            com.google.gson.JsonArray ra = new com.google.gson.JsonArray();
            for (Replica r : replicasOf(primary)) {
                com.google.gson.JsonObject ro = new com.google.gson.JsonObject();
                ro.addProperty("id", r.id());
                ro.addProperty("url", BackendSetModel.maskUrl(r.target().jdbcUrl()));
                ro.addProperty("maxLagSeconds", r.maxLagSeconds());
                LagSample sample = samples.get(r.key());
                if (sample == null) {
                    ro.add("sample", com.google.gson.JsonNull.INSTANCE);
                } else {
                    com.google.gson.JsonObject so = new com.google.gson.JsonObject();
                    so.addProperty("ok", sample.ok());
                    so.addProperty("isReplica", sample.isReplica());
                    so.addProperty("lagSeconds", sample.lagSeconds());
                    so.addProperty("message", sample.message());
                    so.addProperty("ageSeconds", (now - sample.takenAtMillis()) / 1000.0);
                    ro.add("sample", so);
                }
                ro.addProperty("eligible", isEligible(r));
                ro.addProperty("quarantined", isQuarantined(r));
                ro.addProperty("routedReads", routedCount(r.key()));
                ra.add(ro);
            }
            po.add("replicas", ra);
            com.google.gson.JsonObject decisions = new com.google.gson.JsonObject();
            reasonCounts(primary).forEach((k, v) -> decisions.addProperty(k.name().toLowerCase(java.util.Locale.ROOT), v));
            po.add("decisions", decisions);
            primaries.add(po);
        }
        root.add("primaries", primaries);
        return root;
    }
}
