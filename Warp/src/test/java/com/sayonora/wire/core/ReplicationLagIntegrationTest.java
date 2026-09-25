package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real Postgres throughout, but deliberately not real streaming replication -- standing up a
 * genuine primary/physical-standby pair (pg_basebackup, {@code standby.signal}, a replication
 * role, a docker network for container-to-container discovery) is a lot of test infrastructure to
 * verify what's ultimately three lines of SQL. Instead, this shadows {@code pg_is_in_recovery()}/
 * {@code pg_last_xact_replay_timestamp()} with same-signature SQL functions in {@code public},
 * and points the database's default {@code search_path} at {@code public} before {@code
 * pg_catalog} -- Postgres resolves the unqualified call {@link ReplicationLag#check} actually
 * issues to these functions for any NEW connection, exactly the same name-resolution mechanism a
 * real replica's built-in versions would go through. This is genuinely exercising {@link
 * ReplicationLag}'s real SQL and real connection-per-probe behavior against a real server -- only
 * the underlying "is this actually a physical replica" fact is substituted for one this test can
 * drive deterministically and instantly, including asserting on real polling/timeout behavior in
 * {@link ReplicationLag#awaitLagBelow} that a slow real replica's natural catch-up couldn't
 * exercise nearly as fast or reliably.
 */
class ReplicationLagIntegrationTest {

    private RealPostgres pg;
    private BackendTarget target;

    @BeforeEach
    void startInfra() throws Exception {
        pg = RealPostgres.start();
        target = new BackendTarget("fake-replica", pg.jdbcUrl(), pg.username(), pg.password());
    }

    @AfterEach
    void stopInfra() {
        if (pg != null) pg.close();
    }

    @Test
    void anOrdinaryPostgresIsReportedAsNotAReplica() {
        ReplicationLag.Result result = ReplicationLag.check(target);
        assertTrue(result.ok());
        assertFalse(result.isReplica(), "a plain, non-standby Postgres must never be reported as a replica");
    }

    @Test
    void aShadowedInRecoveryTargetReportsItsConfiguredLag() throws Exception {
        setShadowLagSeconds(7);
        ReplicationLag.Result result = ReplicationLag.check(target);
        assertTrue(result.ok());
        assertTrue(result.isReplica());
        assertEquals(7.0, result.lagSeconds(), 1.0, "reported lag should match the ~7s this test configured");
    }

    @Test
    void zeroLagReportsCleanly() throws Exception {
        setShadowLagSeconds(0);
        ReplicationLag.Result result = ReplicationLag.check(target);
        assertTrue(result.ok());
        assertTrue(result.isReplica());
        assertEquals(0.0, result.lagSeconds(), 0.5);
    }

    @Test
    void awaitLagBelowGivesUpAfterItsTimeoutIfLagNeverDrops() throws Exception {
        setShadowLagSeconds(999);
        long start = System.currentTimeMillis();
        ReplicationLag.Result result = ReplicationLag.awaitLagBelow(target, 0.0, 600);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(result.isReplica());
        assertTrue(result.lagSeconds() > 0, "must report the still-nonzero lag it gave up waiting on");
        assertTrue(elapsed >= 500 && elapsed < 5000,
                "must actually wait out roughly its timeout, not return instantly nor hang far past it: " + elapsed + "ms");
    }

    @Test
    void awaitLagBelowReturnsAsSoonAsLagActuallyDrops() throws Exception {
        setShadowLagSeconds(999);
        Thread updater = new Thread(() -> {
            try {
                Thread.sleep(400);
                setShadowLagSeconds(0);
            } catch (Exception ignored) {
                // best-effort background updater
            }
        });
        updater.start();
        long start = System.currentTimeMillis();
        ReplicationLag.Result result = ReplicationLag.awaitLagBelow(target, 0.0, 10_000);
        long elapsed = System.currentTimeMillis() - start;
        updater.join();
        assertTrue(result.ok());
        assertEquals(0.0, result.lagSeconds(), 0.5);
        assertTrue(elapsed < 3000,
                "must return as soon as it observes zero lag, not wait out the full 10s timeout: " + elapsed + "ms");
    }

    /** Installs (or replaces) the shadow functions so the NEXT connection {@link ReplicationLag}
     * opens reports being {@code lagSeconds} behind. Uses its own short-lived connection --
     * deliberately not the same one {@code ReplicationLag} will use, since a per-session {@code
     * search_path} is only read fresh at connection time; each {@link ReplicationLag#check} call
     * opens its own new connection anyway (see its javadoc), so this always takes effect on the
     * very next probe. */
    private void setShadowLagSeconds(double lagSeconds) throws Exception {
        try (Connection conn = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = conn.createStatement()) {
            st.execute("CREATE OR REPLACE FUNCTION pg_is_in_recovery() RETURNS boolean AS "
                    + "$$ SELECT true $$ LANGUAGE sql");
            st.execute("CREATE OR REPLACE FUNCTION pg_last_xact_replay_timestamp() RETURNS timestamptz AS "
                    + "$$ SELECT now() - interval '" + lagSeconds + " seconds' $$ LANGUAGE sql");
            st.execute("ALTER DATABASE " + pg.database() + " SET search_path = public, pg_catalog");
        }
    }
}
