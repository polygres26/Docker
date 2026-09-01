package com.nexagres.wire.xa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.Test;
import org.postgresql.xa.PGXADataSource;

/**
 * Phase 4b of the switchover design: proves recovery reconnects to the EXACT backend a branch was
 * prepared against, not whatever its name currently resolves to. Simulates the scenario the plan
 * called out as unsafe without this -- a backend gets repointed (a switchover, a credential
 * rotation, an operator editing {@code WARP_BACKENDS}) to a different physical target between
 * when a branch went in-doubt and when startup recovery runs.
 *
 * <p>Real infra throughout: two real Postgres containers standing in for the branch's original
 * target and its post-repoint replacement, a genuinely prepared-but-uncommitted XA branch (same
 * technique as {@link XaRecoveryIntegrationTest}), no mocks. Without the captured-identity fields
 * this test would either fail outright (the repointed name's backend has no matching prepared
 * xid to find) or -- far worse in a real deployment -- resolve to whatever unrelated database now
 * happens to sit behind that name.
 */
class XaRecoveryTargetGenerationIntegrationTest {

    @Test
    void recoveryReconnectsToTheCapturedTargetEvenAfterItsNameIsRepointedElsewhere() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres originalBackend = RealPostgres.start(java.util.List.of("max_prepared_transactions=10"));
                RealPostgres repointedBackend = RealPostgres.start()) {

            try (Connection c = DriverManager.getConnection(originalBackend.jdbcUrl(),
                    originalBackend.username(), originalBackend.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE recovery_check (id int)");
            }
            // The repointed backend is a real, working, unrelated Postgres -- proves this isn't
            // "recovery happens to fail loudly and that's why it doesn't miscommit there", but
            // that it genuinely never even attempts to talk to it.
            try (Connection c = DriverManager.getConnection(repointedBackend.jdbcUrl(),
                    repointedBackend.username(), repointedBackend.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE recovery_check (id int)");
            }

            // Same "simulate a crash between prepare and commit" technique as
            // XaRecoveryIntegrationTest -- a genuinely prepared, in-doubt branch on
            // originalBackend, not a fabrication.
            PGXADataSource dataSource = new PGXADataSource();
            dataSource.setUrl(originalBackend.jdbcUrl());
            dataSource.setUser(originalBackend.username());
            dataSource.setPassword(originalBackend.password());
            byte[] gtrid = XidImpl.newGlobalTransactionId();
            Xid xid = XidImpl.branch(gtrid, 0);
            XAConnection xaConn = dataSource.getXAConnection();
            try {
                XAResource resource = xaConn.getXAResource();
                Connection conn = xaConn.getConnection();
                conn.setAutoCommit(false);
                resource.start(xid, XAResource.TMNOFLAGS);
                try (Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO recovery_check (id) VALUES (42)");
                }
                resource.end(xid, XAResource.TMSUCCESS);
                resource.prepare(xid);
                // "Crash" here, branch left prepared but in-doubt on originalBackend.
            } finally {
                xaConn.close();
            }

            XaRecoveryLog recoveryLog = new XaRecoveryLog(controlPlaneOptions(controlPlane));
            recoveryLog.ensureSchema();
            String gtridHex = XaRecoveryLog.hex(gtrid);
            // The captured identity points at originalBackend -- exactly what XaTransaction.commit()
            // would have logged had this gone through the real coordinator path instead of this
            // test's direct XAResource manipulation.
            recoveryLog.logDecided(gtridHex, java.util.List.of(new XaRecoveryLog.Branch(gtridHex, 0, "primary",
                    originalBackend.jdbcUrl(), originalBackend.username(), originalBackend.password())));

            // The registry, as it exists AT RECOVERY TIME, has "primary" repointed to
            // repointedBackend -- simulating an operator having edited WARP_BACKENDS (or run a
            // switchover) between the crash and this restart. If recovery resolved by name instead
            // of using the captured identity, it would reconnect here, not to originalBackend.
            com.nexagres.wire.core.BackendRegistry registry = com.nexagres.wire.core.BackendRegistry.fromConfig(
                    "primary=" + repointedBackend.jdbcUrl() + "|" + repointedBackend.username() + "|"
                            + repointedBackend.password(),
                    null);

            XaRecovery.recover(recoveryLog, registry);

            try (Connection c = DriverManager.getConnection(originalBackend.jdbcUrl(),
                    originalBackend.username(), originalBackend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM recovery_check")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1),
                        "recovery must have committed the branch on its ORIGINAL backend, using the "
                                + "captured jdbcUrl/user/password, not the name's current (repointed) target");
            }
            try (Connection c = DriverManager.getConnection(repointedBackend.jdbcUrl(),
                    repointedBackend.username(), repointedBackend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM recovery_check")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1),
                        "recovery must never have touched the repointed backend at all");
            }
            assertTrue(recoveryLog.findUnresolved().isEmpty());
        }
    }

    private static com.nexagres.wire.server.ServerOptions controlPlaneOptions(RealPostgres controlPlane) {
        return com.nexagres.wire.server.ServerOptions.forTesting(
                controlPlane.host(), controlPlane.port(), controlPlane.database(),
                controlPlane.username(), controlPlane.password());
    }
}
