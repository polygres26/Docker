package com.nexagres.wire.xa;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.sql.SQLException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.Test;

/**
 * Pure unit coverage for {@link XaTransaction} that needs no real backend -- {@link XAResource} is
 * a fake in-memory recorder. Exists specifically to pin down a real regression the in-doubt
 * recovery logging feature introduced: {@code commit()} unconditionally read
 * {@code xids.get(0)} to compute a log key, which threw {@code IndexOutOfBoundsException} for a
 * transaction that never actually added an XA branch (e.g. every statement went through the
 * plain, non-XA default-executor path) -- found live via ReadRoutingIntegrationTest's
 * transaction-scoped case failing with "connection reset", not by this test (written after,
 * to make sure it can't come back).
 */
class XaTransactionTest {

    /** Records calls; every XAResource method succeeds unless told not to. */
    private static final class FakeXAResource implements XAResource {
        boolean failPrepare;

        @Override
        public void start(Xid xid, int flags) {
        }

        @Override
        public void end(Xid xid, int flags) {
        }

        @Override
        public int prepare(Xid xid) throws XAException {
            if (failPrepare) {
                throw new XAException(XAException.XAER_RMERR);
            }
            return XA_OK;
        }

        @Override
        public void commit(Xid xid, boolean onePhase) {
        }

        @Override
        public void rollback(Xid xid) {
        }

        @Override
        public void forget(Xid xid) {
        }

        @Override
        public Xid[] recover(int flag) {
            return new Xid[0];
        }

        @Override
        public boolean isSameRM(XAResource xares) {
            return xares == this;
        }

        @Override
        public int getTransactionTimeout() {
            return 0;
        }

        @Override
        public boolean setTransactionTimeout(int seconds) {
            return false;
        }
    }

    @Test
    void commitWithNoBranchesAtAllDoesNotThrow() {
        // The exact regression: a transaction that never called addBranch() (every statement in
        // it went through the plain, non-XA default-executor path) still gets commit() called on
        // it by RoutingBackendExecutor#endTransaction. This must be a safe no-op, with or without
        // a recovery log configured.
        assertDoesNotThrow(() -> new XaTransaction().commit());
        assertDoesNotThrow(() -> new XaTransaction(recoveryLogForTesting()).commit());
    }

    @Test
    void commitWithOneBranchSucceedsWithAndWithoutARecoveryLog() throws SQLException {
        XaTransaction noLog = new XaTransaction();
        noLog.addBranch("backendA", new FakeXAResource());
        assertDoesNotThrow(noLog::commit);

        XaTransaction withLog = new XaTransaction(recoveryLogForTesting());
        withLog.addBranch("backendA", new FakeXAResource());
        // No real control-plane Postgres behind this recoveryLog -- logDecided/markBranchResolved
        // are best-effort (log a warning, never throw) by design, exactly so a control-plane
        // hiccup can never take down an otherwise-successful commit.
        assertDoesNotThrow(withLog::commit);
    }

    @Test
    void prepareFailureRollsBackAllBranchesAndNeverTouchesTheRecoveryLog() {
        XaTransaction xa = new XaTransaction(recoveryLogForTesting());
        FakeXAResource ok = new FakeXAResource();
        FakeXAResource failing = new FakeXAResource();
        failing.failPrepare = true;
        assertDoesNotThrow(() -> {
            xa.addBranch("ok", ok);
            xa.addBranch("failing", failing);
        });
        org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, xa::commit,
                "a prepare failure must still propagate as a SQLException, unchanged by the recovery-log feature");
    }

    private static XaRecoveryLog recoveryLogForTesting() {
        // No real Postgres needed for these cases -- every recoveryLog call here either never
        // fires (no branches) or is best-effort and swallows its own connection failure.
        return new XaRecoveryLog(com.nexagres.wire.server.ServerOptions.forTesting(
                "localhost", 1, "nonexistent", "nobody", "nopass"));
    }
}
