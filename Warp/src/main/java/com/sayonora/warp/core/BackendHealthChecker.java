package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 3 of the planned-switchover / unplanned-failover design: the unplanned-downtime half.
 * Phase 1 (see {@code BackendRegistry.resolveForRouting}, {@code BackendConnectionPools.drain})
 * gives an operator a way to explicitly mark a backend {@code DRAINING} ahead of planned
 * maintenance; this is what does the equivalent automatically when a backend just stops
 * responding, with no operator involved.
 *
 * <p>Periodically re-probes every backend in the registry with {@link
 * BackendConnectivityTest#test} (the same one-shot, non-pooled check the admin {@code
 * /api/backends/{name}/test} route already uses) and flips its {@link
 * BackendRegistry.BackendState} between {@code ACTIVE} and {@code DOWN} based on the result.
 * {@code DOWN} is routed exactly like {@code DRAINING} (see {@code resolveForRouting}'s javadoc) --
 * new statements prefer the backend's configured fallback, existing sessions already bound to a
 * connection are unaffected.
 *
 * <p>Deliberately never touches a backend an operator has explicitly put into {@code DRAINING} --
 * that's a human decision this background loop has no business overriding in either direction
 * (won't auto-flip it to {@code DOWN} on a probe failure, and won't auto-flip it back to {@code
 * ACTIVE} just because a probe happens to succeed while maintenance is still in progress). It only
 * ever moves a backend between {@code ACTIVE} and {@code DOWN}.
 *
 * <p>Scale: every backend of a cycle is probed CONCURRENTLY, each with its own timeout
 * ({@code WARP_BACKEND_HEALTH_PROBE_TIMEOUT_SECONDS}, default 10), so one dead or hanging backend
 * among 100 costs the cycle at most that timeout instead of stalling every probe queued behind it. A
 * probe that times out counts as a failed probe.
 */
public final class BackendHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(BackendHealthChecker.class);

    private final BackendRegistry registry;
    private final long periodSeconds;
    // "Acceptable data loss" for an UNPLANNED failover -- the RPO an operator is willing to accept
    // when a primary just dies with no warning and nothing about the timing was chosen. Null (the
    // WARP_FAILOVER_MAX_LAG_SECONDS default) means no check is made at all: every failover is
    // logged as a plain ACTIVE->DOWN transition, same as before this existed. When set, exceeding
    // it does NOT block the failover -- the primary is already unreachable, so refusing to route
    // to its fallback would only turn a bounded-loss failover into a total outage. It only changes
    // whether the resulting log line reads as a routine failover or a loud "this cost you more
    // than your accepted RPO" warning -- see probeOne's javadoc.
    private final Double maxAcceptableFailoverLagSeconds;
    private ScheduledExecutorService scheduler;
    private final Function<BackendTarget, BackendConnectivityTest.Result> prober;
    private final long probeTimeoutMillis;
    private final ExecutorService probePool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "warp-backend-probe");
        t.setDaemon(true);
        return t;
    });

    public BackendHealthChecker(BackendRegistry registry, long periodSeconds) {
        this(registry, periodSeconds, null);
    }

    public BackendHealthChecker(BackendRegistry registry, long periodSeconds, Double maxAcceptableFailoverLagSeconds) {
        this(registry, periodSeconds, maxAcceptableFailoverLagSeconds,
                t -> BackendConnectivityTest.test(t.jdbcUrl(), t.user(), t.password()),
                envSeconds("WARP_BACKEND_HEALTH_PROBE_TIMEOUT_SECONDS", 10) * 1000);
    }

    /** {@code prober} and {@code probeTimeoutMillis} are injectable for tests. */
    BackendHealthChecker(BackendRegistry registry, long periodSeconds, Double maxAcceptableFailoverLagSeconds,
            Function<BackendTarget, BackendConnectivityTest.Result> prober, long probeTimeoutMillis) {
        this.registry = registry;
        this.periodSeconds = periodSeconds;
        this.maxAcceptableFailoverLagSeconds = maxAcceptableFailoverLagSeconds;
        this.prober = prober;
        this.probeTimeoutMillis = probeTimeoutMillis;
    }

    private static long envSeconds(String name, long dflt) {
        String v = System.getenv(name);
        try {
            return v == null || v.isBlank() ? dflt : Math.max(1, Long.parseLong(v.trim()));
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-backend-health");
            t.setDaemon(true);
            return t;
        });
        // Skips an immediate first run (initialDelay = periodSeconds, not 0) -- every backend was
        // just resolved from WARP_BACKENDS/warp_config moments ago at startup; giving the
        // rest of Main's own startup connections a chance to land first avoids this loop's probes
        // being what a cold-start operator sees complaining in the logs before anything else has
        // even had a chance to try connecting normally.
        scheduler.scheduleAtFixedRate(this::probeAllSafely, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        log.info("backend health: probing every backend every {}s, auto-marking ACTIVE<->DOWN "
                + "(DRAINING backends are left alone -- that's an operator decision)", periodSeconds);
    }

    /** One probe cycle: all eligible backends concurrently, each bounded by the probe timeout. */
    void probeAllSafely() {
        Map<BackendTarget, Future<BackendConnectivityTest.Result>> inFlight = new LinkedHashMap<>();
        for (BackendTarget target : new ArrayList<>(registry.all())) {
            if (!eligible(target)) {
                continue;
            }
            inFlight.put(target, probePool.submit(() -> prober.apply(target)));
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(probeTimeoutMillis);
        for (Map.Entry<BackendTarget, Future<BackendConnectivityTest.Result>> e : inFlight.entrySet()) {
            BackendTarget target = e.getKey();
            BackendConnectivityTest.Result result;
            try {
                long remaining = Math.max(1, deadline - System.nanoTime());
                result = e.getValue().get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException timeout) {
                e.getValue().cancel(true);
                result = new BackendConnectivityTest.Result(false,
                        "probe timed out after " + probeTimeoutMillis + "ms", probeTimeoutMillis, null);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (java.util.concurrent.ExecutionException | RuntimeException failure) {
                log.warn("backend health: probe of '{}' itself threw unexpectedly (treating as a "
                        + "transient checker failure, not a backend-down signal): {}", target.name(),
                        failure.toString());
                continue;
            }
            try {
                applyResult(target, result);
            } catch (RuntimeException failure) {
                log.warn("backend health: applying the probe of '{}' threw unexpectedly: {}", target.name(),
                        failure.toString());
            }
        }
    }

    private boolean eligible(BackendTarget target) {
        if (registry.stateOf(target.name()) == BackendRegistry.BackendState.DRAINING) {
            return false;
        }
        // A DynamoDB/Mongo connector backend has no JDBC URL to probe -- a JDBC connectivity test
        // would always "fail" and wrongly mark it DOWN. Not health-checked in this first version.
        return !target.isFederationOnlyConnector();
    }

    private void applyResult(BackendTarget target, BackendConnectivityTest.Result result) {
        BackendRegistry.BackendState current = registry.stateOf(target.name());
        if (current == BackendRegistry.BackendState.DRAINING) {
            return; // drained by an operator while the probe was in flight
        }
        if (result.ok() && current == BackendRegistry.BackendState.DOWN) {
            registry.setState(target.name(), BackendRegistry.BackendState.ACTIVE);
            log.info("backend health: '{}' is reachable again -- ACTIVE (routing no longer prefers its "
                    + "fallback)", target.name());
        } else if (!result.ok() && current == BackendRegistry.BackendState.ACTIVE) {
            registry.setState(target.name(), BackendRegistry.BackendState.DOWN);
            log.warn("backend health: '{}' failed its connectivity probe ({}) -- marking DOWN; new "
                    + "statements will route to its configured fallback, if any (routing falls straight "
                    + "through to '{}' itself if none is configured)",
                    target.name(), result.message(), target.name());
            logFailoverLagIfConfigured(target);
        }
    }

    /** Best-effort, log-only -- see {@link #maxAcceptableFailoverLagSeconds}'s javadoc for why
     * this never blocks the failover it's reporting on. */
    private void logFailoverLagIfConfigured(BackendTarget target) {
        if (maxAcceptableFailoverLagSeconds == null || target.fallbackName() == null) {
            return;
        }
        BackendTarget fallback = registry.get(target.fallbackName());
        if (fallback == null) {
            return;
        }
        ReplicationLag.Result lag = ReplicationLag.check(fallback);
        if (!lag.ok() || !lag.isReplica()) {
            return;
        }
        if (lag.lagSeconds() > maxAcceptableFailoverLagSeconds) {
            log.warn("backend health: failing over '{}' to '{}' EXCEEDS the accepted data-loss window -- "
                    + "fallback lag {}s > WARP_FAILOVER_MAX_LAG_SECONDS={}s; some committed writes on "
                    + "'{}' may not be present on '{}' yet",
                    target.name(), target.fallbackName(), String.format("%.1f", lag.lagSeconds()),
                    maxAcceptableFailoverLagSeconds, target.name(), target.fallbackName());
        } else {
            log.info("backend health: failing over '{}' to '{}' -- fallback lag {}s is within the accepted "
                    + "{}s data-loss window", target.name(), target.fallbackName(),
                    String.format("%.1f", lag.lagSeconds()), maxAcceptableFailoverLagSeconds);
        }
    }
}
