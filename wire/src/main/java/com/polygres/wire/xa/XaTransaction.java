package com.polygres.wire.xa;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process two-phase-commit coordinator for a fixed set of {@link XAResource} branches — in
 * PolyWire, one branch per real Postgres backend (shard) a client's transaction touched. Ported
 * from {@code com.omnigate.xa.XaTransaction}, which additionally coordinated Oracle branches;
 * PolyWire only ever talks to Postgres backends (see {@link XaBackendFactory}'s javadoc), so the
 * ported API surface is unchanged but every branch here is a real Postgres XA connection
 * ({@code org.postgresql.xa.PGXADataSource}) — same reasoning {@code BackendRegistry}/
 * {@code BackendConnectionPools} already apply project-wide (multiple Postgres <em>instances</em>,
 * not multiple dialects).
 *
 * <p>Used by {@code RoutingBackendExecutor}/the pgwire session layer to make a client's
 * {@code BEGIN}...{@code COMMIT} atomic across shards when it wrote to more than one backend: if
 * every branch votes to prepare, all commit; if any branch fails to prepare, every branch rolls
 * back instead — no branch can end up committed while another rolled back.
 *
 * <p><b>Narrow slice, not a full transaction manager:</b> the coordinator state (which branches
 * prepared, the global transaction id) lives only in this object's memory. If the polywire
 * process itself crashes between "all branches prepared" and "all branches committed," the
 * branches are left in-doubt on their resource managers with no persistent transaction log to
 * recover them from — a real production TM (e.g. Atomikos, Narayana) logs every phase to disk
 * specifically to survive that case. Recording the crash-recovery log and driving
 * {@code XAResource.recover()} on restart is real future work, not implemented here.
 */
public final class XaTransaction {

    private static final Logger log = LoggerFactory.getLogger(XaTransaction.class);

    private List<XAResource> resources;
    private List<Xid> branchXids;
    /** {@code true} only for the no-arg (incremental) constructor — see {@link #addBranch}. Governs whether commit()/rollback() re-arm via {@link #startBranches} (fixed-list mode, same object reused for the next transaction) or just reset to empty (incremental mode — caller builds a fresh {@link XaTransaction} per client transaction). */
    private final boolean incremental;
    private byte[] incrementalGtrid;

    public XaTransaction(List<XAResource> resources) throws SQLException {
        this.resources = new ArrayList<>(resources);
        this.incremental = false;
        startBranches();
    }

    /**
     * Incremental variant for callers that don't know the full branch set upfront — e.g.
     * {@code RoutingBackendExecutor}, which discovers which shard backends a client's transaction
     * touches one routed statement at a time. All branches share one global transaction id
     * generated here at construction; each call to {@link #addBranch} enlists one more branch
     * under that same gtrid (a fresh branch index per call) and calls {@code XAResource.start} on
     * it immediately, exactly like {@link #startBranches} does for the fixed-list constructor —
     * same protocol, just spread out over the transaction's lifetime instead of done all at once.
     * One instance is good for exactly one client transaction (unlike the fixed-list constructor,
     * whose object is re-armed for reuse after each commit/rollback) — build a fresh one per
     * client {@code BEGIN}.
     */
    public XaTransaction() {
        this.resources = new ArrayList<>();
        this.branchXids = new ArrayList<>();
        this.incremental = true;
        this.incrementalGtrid = XidImpl.newGlobalTransactionId();
    }

    /** True once at least one branch has been enlisted via {@link #addBranch} (incremental mode only). */
    public boolean hasBranches() {
        return !resources.isEmpty();
    }

    /** Enlists one more XA branch (a shard's {@code XAResource}) into this transaction, starting it under a fresh branch index of the shared gtrid. No-op-safe to call from a single-threaded caller only — not synchronized. */
    public void addBranch(XAResource resource) throws SQLException {
        Xid xid = XidImpl.branch(incrementalGtrid, resources.size());
        try {
            resource.start(xid, XAResource.TMNOFLAGS);
        } catch (XAException e) {
            throw wrap("start", e);
        }
        resources.add(resource);
        branchXids.add(xid);
    }

    /** Ends and prepares every branch; commits all of them only if every branch voted to prepare successfully, else rolls all back. */
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
        for (int i = 0; i < resources.size(); i++) {
            if (votes.get(i) == XAResource.XA_RDONLY) {
                continue; // read-only branches are implicitly done after a successful prepare -- no commit call needed/allowed
            }
            try {
                resources.get(i).commit(xids.get(i), false);
            } catch (XAException e) {
                // Every branch already voted XA_OK to prepare, so a commit failure here is the
                // in-doubt window described in the class javadoc -- surfaced, not silently eaten.
                log.error("xa: branch {} failed to commit after a successful prepare vote — in-doubt transaction: {}",
                        i, e.getMessage());
                rearmOrReset();
                throw wrap("commit", e);
            }
        }
        rearmOrReset();
    }

    /** Ends and rolls back every branch (best-effort — a branch failing to roll back is logged, not thrown, since the caller's intent is already "abort"). */
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

    /** Fixed-list mode: restart every branch under a fresh gtrid, ready for reuse. Incremental mode: this object is single-use (see {@link #XaTransaction()}'s javadoc), so just clear it out — a real production caller discards it and builds a fresh one for the next client transaction. */
    private void rearmOrReset() throws SQLException {
        if (incremental) {
            resources = new ArrayList<>();
            branchXids = new ArrayList<>();
            incrementalGtrid = XidImpl.newGlobalTransactionId();
        } else {
            startBranches();
        }
    }

    /** Starts a fresh global transaction (new Xid) across every branch, ready for the next commit/rollback cycle. Fixed-list mode only. */
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
