package com.polygres.wire.xa;

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
    
    private final boolean incremental;
    private byte[] incrementalGtrid;

    public XaTransaction(List<XAResource> resources) throws SQLException {
        this.resources = new ArrayList<>(resources);
        this.incremental = false;
        startBranches();
    }

    public XaTransaction() {
        this.resources = new ArrayList<>();
        this.branchXids = new ArrayList<>();
        this.incremental = true;
        this.incrementalGtrid = XidImpl.newGlobalTransactionId();
    }

    public boolean hasBranches() {
        return !resources.isEmpty();
    }

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
                continue;
            }
            try {
                resources.get(i).commit(xids.get(i), false);
            } catch (XAException e) {
                
                log.error("xa: branch {} failed to commit after a successful prepare vote — in-doubt transaction: {}",
                        i, e.getMessage());
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
