package com.polygres.wire.core;

import com.polygres.wire.telemetry.PolyWireTelemetry;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * ARCHITECTURE.md §5.2: token-bucket admission control per
 * {@link Statement#workloadClass()} (populated by {@link RouterStage},
 * which runs before this stage in the chain), tenant-scoped
 * ({@code tenantId:workloadClass} is the bucket key).
 *
 * <ul>
 *   <li><b>Per-class limits</b>: {@code POLYWIRE_QOS_CLASS_LIMITS} overrides
 *   the global rate/burst/max-wait for specific classes — e.g. give
 *   {@code batch} a smaller budget than {@code query} — falling back to the
 *   global {@code POLYWIRE_QOS_RATE_PER_SEC}/{@code _BURST}/{@code _MAX_WAIT_MS}
 *   for any class not listed.</li>
 *   <li><b>Soft-reject slow lane</b>: an over-limit statement doesn't fail
 *   immediately — it blocks the calling frontend thread (same pattern as
 *   HikariCP's own connection-wait, already used throughout this codebase)
 *   for up to {@code maxWaitMillis}, retrying as the bucket refills, only
 *   hard-rejecting once that budget is exhausted. {@code maxWaitMillis=0}
 *   (the default) is the original instant-reject behavior — this is opt-in,
 *   not a behavior change for existing config. There's no real scheduler or
 *   priority queue here — it's a bounded busy-wait, the honest narrow-slice
 *   version of OJP's SQS "slow lane" concept, not a full one.</li>
 *   <li><b>Pool-aware admission</b>: for statements {@link RouterStage}
 *   already resolved to a specific {@link BackendTarget}, also checks
 *   {@link BackendConnectionPools#statsFor} — if that backend's pool
 *   already has {@code POLYWIRE_QOS_POOL_WAIT_THRESHOLD} or more sessions
 *   queued waiting for a physical connection, this statement is rejected at
 *   admission instead of also piling into that queue. Opt-in
 *   (threshold {@code -1} by default = disabled) since it's new rejection
 *   behavior. Only applies to routed statements — a statement bound for a
 *   frontend's own default connection has no resolvable pool key here,
 *   since {@code QosControlStage} is protocol-agnostic and doesn't know
 *   which frontend it's running under; documented narrow-slice limit.</li>
 *   <li><b>Metrics</b>: admitted/rejected counts per {@code tenant:class},
 *   surfaced on {@code /metrics} ({@link #snapshot()}) and — when telemetry
 *   is configured — pushed via OTLP the same way {@link StatsCollectorStage}
 *   does.</li>
 *   <li><b>Cluster-size reconciliation</b> (ARCHITECTURE.md §12.4 v1): when
 *   {@code com.polygres.wire.cluster.PolyWireCluster} is enabled, each node's
 *   local bucket is sized at {@code configured_rate / current_cluster_size}
 *   instead of the full configured rate — so N nodes admitting
 *   independently still sum to roughly the configured cluster-wide rate,
 *   without a distributed counter on the admission hot path (that would
 *   put a network round-trip back on the path §11 just optimized). {@code
 *   clusterSizeSupplier} defaults to always-1 (today's single-node
 *   behavior, unchanged) when clustering is disabled or not wired in —
 *   this is <em>divide-by-N</em>, an intentionally simple v1 that assumes
 *   roughly even load across nodes; §12.4 names the real weakness (skewed
 *   load, e.g. {@code orawire}'s session-pinned connections landing
 *   unevenly) and a v2 credit-borrowing follow-on, not attempted here.</li>
 * </ul>
 */
public final class QosControlStage implements PipelineStage {

    public record ClassLimit(double ratePerSecond, double burstCapacity, long maxWaitMillis) {
    }

    public record Counters(LongAdder admitted, LongAdder rejected) {
        Counters() {
            this(new LongAdder(), new LongAdder());
        }
    }

    private final ClassLimit defaultLimit;
    private final Map<String, ClassLimit> classLimits;
    private final long poolWaitThreshold;
    private final PolyWireTelemetry telemetry;
    private final IntSupplier clusterSizeSupplier;
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counters> countersByKey = new ConcurrentHashMap<>();

    public QosControlStage(ClassLimit defaultLimit, Map<String, ClassLimit> classLimits, long poolWaitThreshold,
            PolyWireTelemetry telemetry) {
        this(defaultLimit, classLimits, poolWaitThreshold, telemetry, () -> 1);
    }

    public QosControlStage(ClassLimit defaultLimit, Map<String, ClassLimit> classLimits, long poolWaitThreshold,
            PolyWireTelemetry telemetry, IntSupplier clusterSizeSupplier) {
        this.defaultLimit = defaultLimit;
        this.classLimits = Map.copyOf(classLimits);
        this.poolWaitThreshold = poolWaitThreshold;
        this.telemetry = telemetry;
        this.clusterSizeSupplier = clusterSizeSupplier;
    }

    /**
     * {@code classLimitsSpec}: "class:ratePerSec:burst:maxWaitMs,..." — overrides for specific
     * workload classes; any class not listed uses the global rate/burst/maxWait arguments.
     * {@code poolWaitThresholdEnv}: {@code POLYWIRE_QOS_POOL_WAIT_THRESHOLD}, "-1"/unset disables
     * pool-aware admission.
     */
    public static QosControlStage fromConfig(String rateEnv, String burstEnv, String maxWaitEnv,
            String classLimitsSpec, String poolWaitThresholdEnv, PolyWireTelemetry telemetry) {
        return fromConfig(rateEnv, burstEnv, maxWaitEnv, classLimitsSpec, poolWaitThresholdEnv, telemetry, () -> 1);
    }

    public static QosControlStage fromConfig(String rateEnv, String burstEnv, String maxWaitEnv,
            String classLimitsSpec, String poolWaitThresholdEnv, PolyWireTelemetry telemetry,
            IntSupplier clusterSizeSupplier) {
        double rate = rateEnv == null || rateEnv.isBlank() ? 200.0 : Double.parseDouble(rateEnv);
        double burst = burstEnv == null || burstEnv.isBlank() ? rate * 2 : Double.parseDouble(burstEnv);
        long maxWait = maxWaitEnv == null || maxWaitEnv.isBlank() ? 0 : Long.parseLong(maxWaitEnv);
        ClassLimit defaultLimit = new ClassLimit(rate, burst, maxWait);

        Map<String, ClassLimit> classLimits = new HashMap<>();
        if (classLimitsSpec != null && !classLimitsSpec.isBlank()) {
            for (String entry : classLimitsSpec.split(",")) {
                String[] parts = entry.split(":");
                if (parts.length >= 3) {
                    double classRate = Double.parseDouble(parts[1].trim());
                    double classBurst = Double.parseDouble(parts[2].trim());
                    long classMaxWait = parts.length >= 4 ? Long.parseLong(parts[3].trim()) : maxWait;
                    classLimits.put(parts[0].trim(), new ClassLimit(classRate, classBurst, classMaxWait));
                }
            }
        }

        long poolWaitThreshold = poolWaitThresholdEnv == null || poolWaitThresholdEnv.isBlank()
                ? -1 : Long.parseLong(poolWaitThresholdEnv);

        return new QosControlStage(defaultLimit, classLimits, poolWaitThreshold, telemetry, clusterSizeSupplier);
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String workloadClass = statement.workloadClass();
        String countersKey = statement.tenantId() + ":" + workloadClass;
        Counters counters = countersByKey.computeIfAbsent(countersKey, k -> new Counters());

        ClassLimit configuredLimit = classLimits.getOrDefault(workloadClass, defaultLimit);
        // §12.4 v1: divide by current cluster size (1 = clustering disabled, today's unchanged
        // behavior) on every call, not once at bucket creation — so a cluster-size change (a node
        // joining/leaving) takes effect on the very next statement, not after a restart.
        ClassLimit limit = divideByClusterSize(configuredLimit);
        String bucketKey = statement.tenantId() + ":" + workloadClass;
        Bucket bucket = buckets.computeIfAbsent(bucketKey, k -> new Bucket(limit.burstCapacity()));

        if (!bucket.awaitToken(limit)) {
            counters.rejected().increment();
            record(statement.tenantId(), workloadClass, false);
            throw new SQLException("rate limit exceeded for workload class \"" + workloadClass + "\"", "57014");
        }

        String targetBackend = statement.targetBackend();
        if (poolWaitThreshold >= 0 && targetBackend != null && !RoutingBackendExecutor.SCATTER_ALL.equals(targetBackend)) {
            BackendConnectionPools.PoolStats poolStats = BackendConnectionPools.statsFor(targetBackend);
            if (poolStats != null && poolStats.threadsAwaitingConnection() >= poolWaitThreshold) {
                counters.rejected().increment();
                record(statement.tenantId(), workloadClass, false);
                throw new SQLException("backend \"" + targetBackend + "\" pool saturated ("
                        + poolStats.threadsAwaitingConnection() + " already waiting)", "57014");
            }
        }

        counters.admitted().increment();
        record(statement.tenantId(), workloadClass, true);
        return next.proceed(statement);
    }

    private void record(String tenant, String workloadClass, boolean admitted) {
        if (telemetry != null) {
            telemetry.recordAdmission(tenant, workloadClass, admitted);
        }
    }

    private ClassLimit divideByClusterSize(ClassLimit limit) {
        int clusterSize = Math.max(1, clusterSizeSupplier.getAsInt());
        if (clusterSize == 1) {
            return limit; // clustering disabled (or a genuinely solo node) — no division, unchanged behavior
        }
        return new ClassLimit(limit.ratePerSecond() / clusterSize, limit.burstCapacity() / clusterSize,
                limit.maxWaitMillis());
    }

    public Map<String, Counters> snapshot() {
        return Map.copyOf(countersByKey);
    }

    /** For read-only config introspection (the HTTP admin API). */
    public ClassLimit defaultLimit() {
        return defaultLimit;
    }

    public Map<String, ClassLimit> classLimits() {
        return classLimits;
    }

    public long poolWaitThreshold() {
        return poolWaitThreshold;
    }

    private static final class Bucket {
        private double tokens;
        private long lastRefillNanos = System.nanoTime();

        Bucket(double initialTokens) {
            this.tokens = initialTokens;
        }

        /** Tries once immediately; if that fails and {@code limit.maxWaitMillis() > 0}, retries as the bucket refills, up to that budget. */
        boolean awaitToken(ClassLimit limit) {
            long deadlineNanos = System.nanoTime() + limit.maxWaitMillis() * 1_000_000L;
            while (true) {
                if (tryConsume(limit.ratePerSecond(), limit.burstCapacity())) {
                    return true;
                }
                if (limit.maxWaitMillis() <= 0 || System.nanoTime() >= deadlineNanos) {
                    return false;
                }
                try {
                    Thread.sleep(Math.min(20, limit.maxWaitMillis()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        synchronized boolean tryConsume(double ratePerSecond, double capacity) {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
            lastRefillNanos = now;
            tokens = Math.min(capacity, tokens + elapsedSeconds * ratePerSecond);
            if (tokens < 1.0) {
                return false;
            }
            tokens -= 1.0;
            return true;
        }
    }
}
