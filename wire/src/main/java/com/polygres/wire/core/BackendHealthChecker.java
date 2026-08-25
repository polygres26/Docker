package com.polygres.wire.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 */
public final class BackendHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(BackendHealthChecker.class);

    private final BackendRegistry registry;
    private final long periodSeconds;
    private ScheduledExecutorService scheduler;

    public BackendHealthChecker(BackendRegistry registry, long periodSeconds) {
        this.registry = registry;
        this.periodSeconds = periodSeconds;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-backend-health");
            t.setDaemon(true);
            return t;
        });
        // Skips an immediate first run (initialDelay = periodSeconds, not 0) -- every backend was
        // just resolved from POLYWIRE_BACKENDS/polywire_config moments ago at startup; giving the
        // rest of Main's own startup connections a chance to land first avoids this loop's probes
        // being what a cold-start operator sees complaining in the logs before anything else has
        // even had a chance to try connecting normally.
        scheduler.scheduleAtFixedRate(this::probeAllSafely, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        log.info("backend health: probing every backend every {}s, auto-marking ACTIVE<->DOWN "
                + "(DRAINING backends are left alone -- that's an operator decision)", periodSeconds);
    }

    private void probeAllSafely() {
        for (BackendTarget target : registry.all()) {
            try {
                probeOne(target);
            } catch (RuntimeException e) {
                log.warn("backend health: probe of '{}' itself threw unexpectedly (treating as a "
                        + "transient checker failure, not a backend-down signal): {}", target.name(), e.toString());
            }
        }
    }

    private void probeOne(BackendTarget target) {
        BackendRegistry.BackendState current = registry.stateOf(target.name());
        if (current == BackendRegistry.BackendState.DRAINING) {
            return;
        }
        var result = BackendConnectivityTest.test(target.jdbcUrl(), target.user(), target.password());
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
        }
    }
}
