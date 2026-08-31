package com.nexagres.migration.readiness;

import com.nexagres.migration.core.MigrationLicensing;
import com.nexagres.migration.verify.VerificationResult;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Automatic cutover at the right time" -- the Enterprise-only automation layered ON TOP OF
 * {@link CutoverReadinessChecker} (see {@code MigrationLicensing}'s own javadoc on why these are
 * two separate gates, not one): rather than an operator running {@code CutoverReadinessCli} by
 * hand every so often and deciding when to act, this polls readiness on a fixed interval and
 * fires the caller-supplied cutover action itself, unattended, the FIRST time every gate turns
 * green -- then stops polling. Firing more than once (e.g. on a transient flap back to NOT READY
 * and green again) would mean re-running whatever "point the connection string at Postgres and
 * retire the legacy source" action the caller supplied a second time, which for most real cutover
 * actions (a DNS flip, a feature-flag toggle, a config republish) is either a no-op or actively
 * harmful to do twice -- so this deliberately fires the callback exactly once per scheduler
 * instance, not once per poll that happens to be green.
 *
 * <p>The actual cutover action -- what "cut over" even means for a given deployment (flip a
 * connection string, toggle a feature flag, call a webhook, page an operator to confirm) -- is
 * necessarily caller-specific; this class only owns the "when," supplied as a plain {@code
 * Runnable} the caller constructs however fits their own environment. This mirrors {@code
 * CutoverReadinessChecker}'s own division of labor: that class doesn't know how to compute a
 * verification pass either (see its own javadoc), it just accepts the result.
 *
 * <p>A single background thread, not a thread pool -- one scheduler polls one source's readiness;
 * running several sources means constructing several schedulers, each with its own thread, the
 * same shape {@code Coordinator}'s own one-thread-per-partition pattern already uses elsewhere in
 * this module.
 */
public final class AutomaticCutoverScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AutomaticCutoverScheduler.class);

    private final CutoverReadinessChecker checker;
    private final String sourceKey;
    private final boolean hasLiveChangeFeed;
    private final long maxLagSeconds;
    private final Supplier<VerificationResult> verificationSupplier;
    private final Runnable onCutover;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> pollTask;

    /**
     * @param verificationSupplier supplies a freshly computed source/target comparison on each
     *     poll, or {@code null} to omit that gate from every check -- called once per poll (not
     *     once total), since a verification result computed at construction time would grow stale
     *     across a polling window that can span the entire remainder of a migration
     * @param onCutover the caller's own cutover action, invoked exactly once, on this scheduler's
     *     background polling thread, the first time {@link CutoverReadinessReport#ready()} is
     *     {@code true}
     * @throws IllegalStateException if the current process isn't Enterprise-licensed -- see
     *     {@link MigrationLicensing#requireEnterpriseForAutomaticCutover()}
     */
    public AutomaticCutoverScheduler(CutoverReadinessChecker checker, String sourceKey, boolean hasLiveChangeFeed,
            long maxLagSeconds, Supplier<VerificationResult> verificationSupplier, Runnable onCutover) {
        MigrationLicensing.requireEnterpriseForAutomaticCutover();
        this.checker = checker;
        this.sourceKey = sourceKey;
        this.hasLiveChangeFeed = hasLiveChangeFeed;
        this.maxLagSeconds = maxLagSeconds;
        this.verificationSupplier = verificationSupplier;
        this.onCutover = onCutover;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "automatic-cutover-" + sourceKey);
            t.setDaemon(true);
            return t;
        });
    }

    /** Starts polling every {@code pollIntervalSeconds}, beginning after the first interval
     * elapses (not immediately -- a migration is essentially never ready at the instant this is
     * constructed, so an immediate first poll is almost always a wasted round trip). */
    public void start(long pollIntervalSeconds) {
        pollTask = executor.scheduleWithFixedDelay(this::pollOnce, pollIntervalSeconds, pollIntervalSeconds,
                TimeUnit.SECONDS);
    }

    private void pollOnce() {
        if (fired.get()) {
            return;
        }
        try {
            VerificationResult verification = verificationSupplier == null ? null : verificationSupplier.get();
            CutoverReadinessReport report = checker.check(sourceKey, hasLiveChangeFeed, maxLagSeconds, verification);
            if (!report.ready()) {
                log.info("automatic cutover[{}]: not ready yet\n{}", sourceKey, report.render());
                return;
            }
            if (fired.compareAndSet(false, true)) {
                log.info("automatic cutover[{}]: every gate passed -- firing cutover action\n{}", sourceKey,
                        report.render());
                onCutover.run();
                stopPolling();
            }
        } catch (SQLException e) {
            log.warn("automatic cutover[{}]: readiness check failed, will retry next interval", sourceKey, e);
        } catch (RuntimeException e) {
            log.error("automatic cutover[{}]: cutover action threw -- NOT retrying automatically, the action "
                    + "may have partially applied; investigate and cut over manually", sourceKey, e);
        }
    }

    private void stopPolling() {
        ScheduledFuture<?> task = pollTask;
        if (task != null) {
            task.cancel(false);
        }
    }

    /** Whether the cutover action has already fired -- {@code true} forever after the first
     * success, useful for a caller that wants to know without polling {@link #close()}'s own
     * shutdown state. */
    public boolean hasFired() {
        return fired.get();
    }

    @Override
    public void close() {
        stopPolling();
        executor.shutdownNow();
    }
}
