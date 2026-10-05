package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The failure paths of the operator rejoin command (the success path needs a real Postgres: see PostgresRejoinLiveTest). */
class PostgresRejoinCommandTest {

    private static final BackendTarget NODE = new BackendTarget("old", "jdbc:postgresql://127.0.0.1:1/postgres", "u", "p");
    private static final BackendTarget PRIMARY = new BackendTarget("new", "jdbc:postgresql://db.example:6543/postgres", "u", "p");

    @Test
    void aFailingCommandReportsItsExitCodeAndOutput() {
        EngineHa.RejoinResult r = PostgresHa.runRejoinCommand("echo boom; exit 3", NODE, PRIMARY, 10);
        assertEquals(EngineHa.RejoinOutcome.NEEDS_REBUILD, r.outcome());
        assertTrue(r.detail().contains("exit 3") && r.detail().contains("boom"), r.detail());
    }

    @Test
    void aCommandThatRunsTooLongIsStopped() {
        long t0 = System.nanoTime();
        EngineHa.RejoinResult r = PostgresHa.runRejoinCommand("sleep 30", NODE, PRIMARY, 1);
        assertEquals(EngineHa.RejoinOutcome.NEEDS_REBUILD, r.outcome());
        assertTrue(r.detail().contains("did not finish within 1s"), r.detail());
        assertTrue((System.nanoTime() - t0) / 1_000_000_000L < 10);
    }

    @Test
    void theCommandSeesTheNodeAndThePrimaryInItsEnvironment() {
        EngineHa.RejoinResult r = PostgresHa.runRejoinCommand(
                "test \"$WARP_REJOIN_PRIMARY_HOST:$WARP_REJOIN_PRIMARY_PORT:$WARP_REJOIN_PRIMARY_USER\" = db.example:6543:u "
                        + "&& test \"$WARP_REJOIN_NODE_URL\" = jdbc:postgresql://127.0.0.1:1/postgres && test \"$PGPASSWORD\" = p "
                        + "|| { echo bad-env; exit 9; }", NODE, PRIMARY, 10);
        // the command succeeds, so the only failure left is that the (unreachable) node never became a standby
        assertTrue(r.detail().contains("not a standby") || r.detail().contains("could not"), r.detail());
        assertTrue(!r.detail().contains("bad-env"), r.detail());
    }
}
