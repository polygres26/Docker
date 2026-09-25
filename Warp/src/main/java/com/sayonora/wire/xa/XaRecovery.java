package com.sayonora.wire.xa;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup pass that finishes whatever a crashed coordinator left in doubt. Run once, early in
 * {@code Main.main}, right after the backend registry is built and before Warp starts
 * accepting client connections -- an in-doubt branch is holding locks at its backend for however
 * long it stays unresolved, so this shouldn't wait for the first client transaction to trigger it.
 *
 * <p>Every branch {@link XaRecoveryLog} still has unresolved was, by construction (see
 * {@link XaRecoveryLog}'s class doc), already decided COMMIT before the crash -- so recovery's
 * only job per branch is: find out whether the backend still has it prepared (a previous
 * partially-completed attempt, live or a prior recovery pass, might have already committed it),
 * and if so, commit it for real.
 */
public final class XaRecovery {

    private static final Logger log = LoggerFactory.getLogger(XaRecovery.class);

    private XaRecovery() {
    }

    public static void recover(XaRecoveryLog recoveryLog, BackendRegistry registry) {
        Map<String, List<XaRecoveryLog.Branch>> unresolved;
        try {
            unresolved = recoveryLog.findUnresolved();
        } catch (SQLException e) {
            log.warn("xa recovery: could not read warp_xa_log -- skipping startup recovery pass "
                    + "(any in-doubt transaction from a previous crash stays unresolved until the next "
                    + "successful startup): {}", e.getMessage());
            return;
        }
        if (unresolved.isEmpty()) {
            log.info("xa recovery: no in-doubt transactions from a previous coordinator crash");
            return;
        }
        log.warn("xa recovery: {} in-doubt transaction(s) found from a previous coordinator crash -- resolving now",
                unresolved.size());
        int resolved = 0;
        int failed = 0;
        for (Map.Entry<String, List<XaRecoveryLog.Branch>> entry : unresolved.entrySet()) {
            byte[] gtrid = XaRecoveryLog.unhex(entry.getKey());
            for (XaRecoveryLog.Branch branch : entry.getValue()) {
                if (resolveBranch(recoveryLog, registry, gtrid, branch)) {
                    resolved++;
                } else {
                    failed++;
                }
            }
        }
        log.warn("xa recovery: finished -- {} branch(es) resolved, {} branch(es) still failed and will be "
                + "retried on the next restart", resolved, failed);
    }

    private static boolean resolveBranch(XaRecoveryLog recoveryLog, BackendRegistry registry, byte[] gtrid,
            XaRecoveryLog.Branch branch) {
        String gtridHex = branch.gtridHex();
        // Phase 4b: a captured jdbcUrl (present for any branch prepared after this feature
        // shipped) is authoritative and reconnects directly, bypassing name resolution entirely --
        // immune to backend_name having been repointed to a different physical target since this
        // branch was prepared (a switchover, a credential rotation, a config edit). Only a row
        // written before this existed (backendJdbcUrl null) falls back to resolving backend_name
        // through the live BackendRegistry, exactly as recovery always worked before Phase 4b.
        boolean usingCapturedIdentity = branch.backendJdbcUrl() != null;
        BackendTarget target = usingCapturedIdentity ? null : registry.get(branch.backendName());
        if (!usingCapturedIdentity && target == null) {
            log.error("xa recovery: gtrid={} branch={} references unknown backend '{}' (no captured target "
                    + "identity on this row -- it predates Phase 4b) -- WARP_BACKENDS config may have "
                    + "changed since the crash; this branch cannot be auto-recovered and needs manual "
                    + "resolution against that backend's own XA recovery tooling (e.g. psql's pg_prepared_xacts).",
                    gtridHex, branch.branchIndex(), branch.backendName());
            return false;
        }
        Xid ourXid = XidImpl.branch(gtrid, branch.branchIndex());
        try {
            XaBackendFactory.XaBranch xaBranch = usingCapturedIdentity
                    ? XaBackendFactory.openDirect(branch.backendJdbcUrl(), branch.backendUser(), branch.backendPassword())
                    : XaBackendFactory.open(target);
            try {
                // XaBackendFactory.open() sets autoCommit(false) for the normal case of a
                // connection that will actually participate in a branch via start()/end(). Pure
                // administrative recovery never does that -- it only calls recover()/commit() on
                // the XAResource -- but pgjdbc's recover() runs its own plain SQL query
                // (pg_prepared_xacts) over this same connection, and with autoCommit(false) that
                // query leaves the connection in its own open local transaction, which pgjdbc then
                // refuses to call XA commit() through ("2nd phase commit must be issued using an
                // idle connection"). Autocommit(true) keeps that query self-contained.
                xaBranch.connection().setAutoCommit(true);
                XAResource resource = xaBranch.resource();
                boolean stillPrepared = isStillPrepared(resource, ourXid);
                if (stillPrepared) {
                    resource.commit(ourXid, false);
                    log.warn("xa recovery: committed in-doubt gtrid={} branch={} on backend '{}'",
                            gtridHex, branch.branchIndex(), branch.backendName());
                } else {
                    log.info("xa recovery: gtrid={} branch={} on backend '{}' was no longer prepared -- "
                                    + "already committed by an earlier attempt, marking resolved",
                            gtridHex, branch.branchIndex(), branch.backendName());
                }
                recoveryLog.markBranchResolved(gtridHex, branch.branchIndex());
                return true;
            } finally {
                xaBranch.connection().close();
            }
        } catch (Exception e) {
            log.error("xa recovery: FAILED to resolve gtrid={} branch={} on backend '{}' -- will retry on the "
                    + "next restart: {}", gtridHex, branch.branchIndex(), branch.backendName(), e.getMessage());
            return false;
        }
    }

    private static boolean isStillPrepared(XAResource resource, Xid ourXid) throws XAException {
        Xid[] inDoubt = resource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        for (Xid x : inDoubt) {
            if (XidImpl.sameBranch(ourXid, x)) {
                return true;
            }
        }
        return false;
    }
}
