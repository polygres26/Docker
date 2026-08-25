package com.polygres.wire.xa;

import com.polygres.wire.core.BackendTarget;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class XaTransaction {

    private static final Logger log = LoggerFactory.getLogger(XaTransaction.class);

    private List<XAResource> resources;
    private List<Xid> branchXids;
    private List<String> branchBackendNames;
    // Parallel to branchBackendNames -- the exact BackendTarget each branch was actually opened
    // against, captured here (not re-resolved from a name later) so a durably-logged branch can
    // record precisely what to reconnect to during crash recovery, immune to that name being
    // repointed to a different physical target in the meantime. Null entries throughout for the
    // legacy XaTransaction(List<XAResource>) constructor, which never calls addBranch -- commit()
    // treats a null target the same as "no captured identity", falling back to name-based
    // recovery lookup, unchanged from before this existed.
    private List<BackendTarget> branchTargets;

    private final boolean incremental;
    private byte[] incrementalGtrid;

    // Nullable -- when absent (the no-arg XaTransaction() constructor, used by every unit test
    // that doesn't need real crash recovery), commit() behaves exactly as it did before this
    // in-doubt-recovery feature existed: no durable logging, no recovery is possible for a crash
    // mid-commit. RoutingBackendExecutor always supplies one in production.
    private final XaRecoveryLog recoveryLog;

    public XaTransaction(List<XAResource> resources) throws SQLException {
        this.resources = new ArrayList<>(resources);
        this.branchBackendNames = new ArrayList<>(java.util.Collections.nCopies(resources.size(), null));
        this.branchTargets = new ArrayList<>(java.util.Collections.nCopies(resources.size(), null));
        this.incremental = false;
        this.recoveryLog = null;
        startBranches();
    }

    public XaTransaction() {
        this((XaRecoveryLog) null);
    }

    public XaTransaction(XaRecoveryLog recoveryLog) {
        this.resources = new ArrayList<>();
        this.branchXids = new ArrayList<>();
        this.branchBackendNames = new ArrayList<>();
        this.branchTargets = new ArrayList<>();
        this.incremental = true;
        this.incrementalGtrid = XidImpl.newGlobalTransactionId();
        this.recoveryLog = recoveryLog;
    }

    public boolean hasBranches() {
        return !resources.isEmpty();
    }

    /** As {@link #addBranch(String, XAResource)}, capturing {@code target}'s exact jdbcUrl/user/
     * password alongside its name -- see {@link #branchTargets}'s javadoc. Every production caller
     * (only {@code RoutingBackendExecutor}) already has the {@link BackendTarget} in hand right
     * where it opens the branch, so this is the preferred overload; the name-only one remains for
     * tests that don't need a real target. */
    public void addBranch(BackendTarget target, XAResource resource) throws SQLException {
        addBranchInternal(target.name(), target, resource);
    }

    public void addBranch(String backendName, XAResource resource) throws SQLException {
        addBranchInternal(backendName, null, resource);
    }

    private void addBranchInternal(String backendName, BackendTarget target, XAResource resource) throws SQLException {
        Xid xid = XidImpl.branch(incrementalGtrid, resources.size());
        try {
            resource.start(xid, XAResource.TMNOFLAGS);
        } catch (XAException e) {
            throw wrap("start", e);
        }
        resources.add(resource);
        branchXids.add(xid);
        branchBackendNames.add(backendName);
        branchTargets.add(target);
    }

    public void commit() throws SQLException {
        List<Xid> xids = branchXids;
        List<Integer> votes = new ArrayList<>(resources.size());
        SQLException prepareFailure = null;
        for (int i = 0; i < resources.size(); i++) {
            try {
                resources.get(i).end(xids.get(i), XAResource.TMSUCCESS);
            } catch (XAException e) {
                prepareFailure = wrap("end", e);
                break;
            }
        }
        if (prepareFailure == null) {
            for (int i = 0; i < resources.size() && prepareFailure == null; i++) {
                try {
                    votes.add(resources.get(i).prepare(xids.get(i)));
                } catch (XAException e) {
                    prepareFailure = wrap("prepare", e);
                }
            }
        }
        if (prepareFailure != null) {
            log.warn("xa: prepare failed, rolling back all {} branches: {}", resources.size(), prepareFailure.getMessage());
            rollbackBranches(xids);
            rearmOrReset();
            throw prepareFailure;
        }

        // Every branch just voted to prepare -- the transaction's fate is now fixed (commit) and
        // durable-loggable. This is the point-of-no-return: a crash from here until every branch's
        // commit() below actually returns leaves that branch prepared (holding locks) at its
        // backend with nothing else recording it needs finishing -- exactly what XaRecoveryLog
        // exists to make recoverable on the next startup. Read-only branches never entered this
        // window (a read-only vote already released the branch's resources during prepare), so
        // they're excluded from the log.
        // resources can legitimately be empty here -- a transaction that never touched an XA
        // branch at all (e.g. every statement went through the plain, non-XA defaultExecutor
        // path) still calls commit() on its way through endTransaction(). xids.get(0) below would
        // throw on that empty list, so this whole block -- there's nothing to log or resolve when
        // there are no branches.
        String gtridHex = (recoveryLog == null || resources.isEmpty())
                ? null : XaRecoveryLog.hex(xids.get(0).getGlobalTransactionId());
        if (recoveryLog != null && !resources.isEmpty()) {
            List<XaRecoveryLog.Branch> toLog = new ArrayList<>();
            for (int i = 0; i < resources.size(); i++) {
                if (votes.get(i) != XAResource.XA_RDONLY) {
                    BackendTarget target = branchTargets.get(i);
                    toLog.add(new XaRecoveryLog.Branch(gtridHex, i, branchBackendNames.get(i),
                            target == null ? null : target.jdbcUrl(),
                            target == null ? null : target.user(),
                            target == null ? null : target.password()));
                }
            }
            recoveryLog.logDecided(gtridHex, toLog);
        }

        for (int i = 0; i < resources.size(); i++) {
            if (votes.get(i) == XAResource.XA_RDONLY) {
                continue;
            }
            try {
                resources.get(i).commit(xids.get(i), false);
                if (recoveryLog != null) {
                    recoveryLog.markBranchResolved(gtridHex, i);
                }
            } catch (XAException e) {

                log.error("xa: branch {} failed to commit after a successful prepare vote — in-doubt transaction"
                        + (recoveryLog != null
                                ? " logged for recovery on the next PolyWire restart (gtrid=" + gtridHex + ")"
                                : " -- NOT recoverable, this XaTransaction was created without a recovery log")
                        + ": {}", i, e.getMessage());
                rearmOrReset();
                throw wrap("commit", e);
            }
        }
        rearmOrReset();
    }

    public void rollback() throws SQLException {
        for (int i = 0; i < resources.size(); i++) {
            try {
                resources.get(i).end(branchXids.get(i), XAResource.TMFAIL);
            } catch (XAException e) {
                log.warn("xa: branch {} end(TMFAIL) failed during rollback: {}", i, e.getMessage());
            }
        }
        rollbackBranches(branchXids);
        rearmOrReset();
    }

    private void rollbackBranches(List<Xid> xids) {
        for (int i = 0; i < resources.size(); i++) {
            try {
                resources.get(i).rollback(xids.get(i));
            } catch (XAException e) {
                log.warn("xa: branch {} rollback failed: {}", i, e.getMessage());
            }
        }
    }

    private void rearmOrReset() throws SQLException {
        if (incremental) {
            resources = new ArrayList<>();
            branchXids = new ArrayList<>();
            branchBackendNames = new ArrayList<>();
            branchTargets = new ArrayList<>();
            incrementalGtrid = XidImpl.newGlobalTransactionId();
        } else {
            startBranches();
        }
    }

    private void startBranches() throws SQLException {
        byte[] gtrid = XidImpl.newGlobalTransactionId();
        List<Xid> xids = new ArrayList<>(resources.size());
        for (int i = 0; i < resources.size(); i++) {
            Xid xid = XidImpl.branch(gtrid, i);
            xids.add(xid);
            try {
                resources.get(i).start(xid, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                throw wrap("start", e);
            }
        }
        this.branchXids = xids;
    }

    private static SQLException wrap(String phase, XAException e) {
        return new SQLException("xa " + phase + " failed: " + e.getMessage(), e);
    }
}
