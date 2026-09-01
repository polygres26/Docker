package com.nexagres.migration.readiness;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.core.MigrationLicensing;
import com.nexagres.migration.core.MigrationLicensingTestSupport;
import com.nexagres.migration.coordinator.PartitionLeaseStore;
import com.nexagres.migration.testsupport.RealPostgres;
import com.nexagres.wire.license.LicenseTier;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real Postgres throughout, same bookkeeping tables {@link CutoverReadinessCheckerIntegrationTest}
 * already exercises -- this class's own job is proving the polling/firing mechanics on top of
 * that, not re-proving readiness logic itself: starts NOT READY, fires the supplied cutover action
 * exactly once the first time every gate turns green, and never fires again afterward even if
 * polled repeatedly past that point.
 */
class AutomaticCutoverSchedulerIntegrationTest {

    private static final String SOURCE_KEY = "mysql:mydb.orders";

    @AfterEach
    void resetTier() {
        MigrationLicensingTestSupport.reset();
    }

    @Test
    void freeTierRefusesToConstructOutright() throws Exception {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        try (RealPostgres postgres = RealPostgres.start()) {
            CutoverReadinessChecker checker = new CutoverReadinessChecker(postgres.jdbcUrl(), postgres.username(),
                    postgres.password());
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> new AutomaticCutoverScheduler(checker, SOURCE_KEY, true, 10, null, () -> { }));
            assertTrue(e.getMessage().contains("WARP_LICENSE_KEY"));
        }
    }

    @Test
    void firesTheCutoverActionExactlyOnceAssoonAsReady() throws Exception {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        try (RealPostgres postgres = RealPostgres.start()) {
            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(),
                    postgres.password());
            checkpoints.ensureSchema();
            PartitionLeaseStore leases = new PartitionLeaseStore(postgres.jdbcUrl(), postgres.username(),
                    postgres.password());
            leases.ensureSchema();

            CutoverReadinessChecker checker = new CutoverReadinessChecker(postgres.jdbcUrl(), postgres.username(),
                    postgres.password());
            AtomicInteger fireCount = new AtomicInteger();

            try (AutomaticCutoverScheduler scheduler = new AutomaticCutoverScheduler(checker, SOURCE_KEY, true, 10,
                    null, fireCount::incrementAndGet)) {
                scheduler.start(1);

                // Not ready yet -- no partitions recorded -- should not fire across a couple of
                // poll intervals.
                Thread.sleep(2500);
                assertFalse(scheduler.hasFired());
                assertEquals0(fireCount);

                // Make every gate pass.
                assertTrue(leases.tryClaim(SOURCE_KEY, SOURCE_KEY + "#p0", "worker-1", 3600));
                leases.markDone(SOURCE_KEY, SOURCE_KEY + "#p0");
                checkpoints.save(SOURCE_KEY, "\"resume-token\"", Instant.now());

                waitUntil(Duration.ofSeconds(10), scheduler::hasFired);
                assertTrue(fireCount.get() >= 1);

                // Stays fired, and never fires a second time even if it were polled again.
                Thread.sleep(2500);
                org.junit.jupiter.api.Assertions.assertEquals(1, fireCount.get());
            }
        }
    }

    private static void assertEquals0(AtomicInteger counter) {
        org.junit.jupiter.api.Assertions.assertEquals(0, counter.get());
    }

    private static void waitUntil(Duration timeout, Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
