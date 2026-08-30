package com.nexagres.wire.core;

import com.nexagres.wire.telemetry.PolyWireTelemetry;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

public final class QosControlStage implements PipelineStage {

    public record ClassLimit(double ratePerSecond, double burstCapacity, long maxWaitMillis) {
    }

    public record Counters(LongAdder admitted, LongAdder rejected) {
        Counters() {
            this(new LongAdder(), new LongAdder());
        }
    }

    private volatile ClassLimit defaultLimit;
    private volatile Map<String, ClassLimit> classLimits;
    private volatile long poolWaitThreshold;
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

        Map<String, ClassLimit> classLimits = parseClassLimitsSpec(classLimitsSpec, maxWait);

        long poolWaitThreshold = poolWaitThresholdEnv == null || poolWaitThresholdEnv.isBlank()
                ? -1 : Long.parseLong(poolWaitThresholdEnv);

        return new QosControlStage(defaultLimit, classLimits, poolWaitThreshold, telemetry, clusterSizeSupplier);
    }

    /** {@code "class:rate:burst[:maxWait]"} entries, comma-separated -- the same grammar {@code
     * POLYWIRE_QOS_CLASS_LIMITS}/{@code polywire_config.qos_class_limits} has always used.
     * Exposed (not just inlined into {@link #fromConfig}) so other admin-surface code that needs
     * to read or rebuild this exact string -- e.g. {@code MetricsServer}'s QoS-tuning-suggestion
     * draft endpoint, which merges one proposed class change into the rest of the current spec
     * unchanged -- shares this one parser instead of a second, possibly-drifting copy of the
     * grammar. */
    public static Map<String, ClassLimit> parseClassLimitsSpec(String classLimitsSpec, long defaultMaxWait) {
        Map<String, ClassLimit> classLimits = new HashMap<>();
        if (classLimitsSpec != null && !classLimitsSpec.isBlank()) {
            for (String entry : classLimitsSpec.split(",")) {
                String[] parts = entry.split(":");
                if (parts.length >= 3) {
                    double classRate = Double.parseDouble(parts[1].trim());
                    double classBurst = Double.parseDouble(parts[2].trim());
                    long classMaxWait = parts.length >= 4 ? Long.parseLong(parts[3].trim()) : defaultMaxWait;
                    classLimits.put(parts[0].trim(), new ClassLimit(classRate, classBurst, classMaxWait));
                }
            }
        }
        return classLimits;
    }

    /** The inverse of {@link #parseClassLimitsSpec} -- rebuilds the {@code
     * "class:rate:burst:maxWait"} spec string from a limits map, in insertion order. */
    public static String formatClassLimitsSpec(Map<String, ClassLimit> classLimits) {
        StringBuilder spec = new StringBuilder();
        for (var entry : classLimits.entrySet()) {
            if (spec.length() > 0) {
                spec.append(',');
            }
            ClassLimit limit = entry.getValue();
            spec.append(entry.getKey()).append(':').append(limit.ratePerSecond()).append(':')
                    .append(limit.burstCapacity()).append(':').append(limit.maxWaitMillis());
        }
        return spec.toString();
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String workloadClass = statement.workloadClass();
        String countersKey = statement.tenantId() + ":" + workloadClass;
        Counters counters = countersByKey.computeIfAbsent(countersKey, k -> new Counters());

        ClassLimit configuredLimit = classLimits.getOrDefault(workloadClass, defaultLimit);
        
        ClassLimit limit = divideByClusterSize(configuredLimit);
        String bucketKey = statement.tenantId() + ":" + workloadClass;
        Bucket bucket = buckets.computeIfAbsent(bucketKey, k -> new Bucket(limit.burstCapacity()));

        if (!bucket.awaitToken(limit)) {
            counters.rejected().increment();
            record(statement.tenantId(), workloadClass, false);
            throw ErrorCatalog.sqlExceptionWithState("ERR_QOS_RATE_LIMIT", "57014", workloadClass);
        }

        String targetBackend = statement.targetBackend();
        if (poolWaitThreshold >= 0 && targetBackend != null && !RoutingBackendExecutor.SCATTER_ALL.equals(targetBackend)) {
            BackendConnectionPools.PoolStats poolStats = BackendConnectionPools.statsFor(targetBackend);
            if (poolStats != null && poolStats.threadsAwaitingConnection() >= poolWaitThreshold) {
                counters.rejected().increment();
                record(statement.tenantId(), workloadClass, false);
                throw ErrorCatalog.sqlExceptionWithState("ERR_QOS_POOL_SATURATED", "57014",
                        targetBackend, poolStats.threadsAwaitingConnection());
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
            return limit;
        }
        return new ClassLimit(limit.ratePerSecond() / clusterSize, limit.burstCapacity() / clusterSize,
                limit.maxWaitMillis());
    }

    public Map<String, Counters> snapshot() {
        return Map.copyOf(countersByKey);
    }

    public void reconfigure(ClassLimit newDefaultLimit, Map<String, ClassLimit> newClassLimits, long newPoolWaitThreshold) {
        this.defaultLimit = newDefaultLimit;
        this.classLimits = Map.copyOf(newClassLimits);
        this.poolWaitThreshold = newPoolWaitThreshold;
    }

    public static QosControlStage parseAndApply(QosControlStage stage, String rateEnv, String burstEnv,
            String maxWaitEnv, String classLimitsSpec, String poolWaitThresholdEnv) {
        QosControlStage parsed = fromConfig(rateEnv, burstEnv, maxWaitEnv, classLimitsSpec, poolWaitThresholdEnv, null);
        stage.reconfigure(parsed.defaultLimit(), parsed.classLimits(), parsed.poolWaitThreshold());
        return stage;
    }

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
