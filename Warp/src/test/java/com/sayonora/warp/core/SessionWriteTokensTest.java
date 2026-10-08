package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SessionWriteTokensTest {

    private final AtomicLong primaryPos = new AtomicLong(100);
    private final AtomicLong replicaPos = new AtomicLong(50);
    private boolean positionsAvailable = true;
    private int appliedCalls;

    private final SessionWriteTokens tokens = new SessionWriteTokens(new SessionWriteTokens.Positions() {
        public Optional<BigInteger> write(BackendTarget p) {
            return positionsAvailable ? Optional.of(BigInteger.valueOf(primaryPos.get())) : Optional.empty();
        }

        public Optional<BigInteger> applied(BackendTarget r) {
            appliedCalls++;
            return positionsAvailable ? Optional.of(BigInteger.valueOf(replicaPos.get())) : Optional.empty();
        }
    });

    private final BackendTarget primary = new BackendTarget("db", "jdbc:postgresql://p/db", "u", "p");
    private final BackendTarget replica = new BackendTarget("db#1", "jdbc:postgresql://r/db", "u", "p");

    @Test
    void aSessionThatNeverWroteHasNoToken() {
        assertEquals(SessionWriteTokens.Verdict.NO_TOKEN, tokens.check("db", primary, replica));
    }

    @Test
    void aReplicaIsSafeOnlyOnceItHasAppliedThePositionOfTheSessionsLastWrite() {
        tokens.recordWrite("db", primary);
        assertEquals(SessionWriteTokens.Verdict.BEHIND, tokens.check("db", primary, replica));
        replicaPos.set(99);
        assertEquals(SessionWriteTokens.Verdict.BEHIND, tokens.check("db", primary, replica));
        replicaPos.set(100);
        assertEquals(SessionWriteTokens.Verdict.SAFE, tokens.check("db", primary, replica));
    }

    @Test
    void aReplicaThatCaughtUpIsNotAskedAgainUntilTheNextWrite() {
        tokens.recordWrite("db", primary);
        replicaPos.set(120);
        tokens.check("db", primary, replica);
        int calls = appliedCalls;
        assertEquals(SessionWriteTokens.Verdict.SAFE, tokens.check("db", primary, replica));
        assertEquals(calls, appliedCalls, "no extra round trip to the replica");
        primaryPos.set(500);
        tokens.recordWrite("db", primary);
        assertEquals(SessionWriteTokens.Verdict.BEHIND, tokens.check("db", primary, replica), "a new write needs a new catch-up");
    }

    @Test
    void withoutPositionsTheCallerFallsBackToTheTimeWindow() {
        positionsAvailable = false;
        tokens.recordWrite("db", primary);
        assertEquals(SessionWriteTokens.Verdict.UNKNOWN, tokens.check("db", primary, replica));
    }

    @Test
    void aTokenFromAnotherPrimaryIsDroppedAfterAFailover() {
        tokens.recordWrite("db", primary);
        BackendTarget promoted = new BackendTarget("db", "jdbc:postgresql://r/db", "u", "p");
        assertEquals(SessionWriteTokens.Verdict.NO_TOKEN, tokens.check("db", promoted, replica));
    }
}
