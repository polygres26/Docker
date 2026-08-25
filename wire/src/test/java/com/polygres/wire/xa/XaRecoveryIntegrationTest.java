package com.polygres.wire.xa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.testsupport.PolyWireProcess;
import com.polygres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.Test;
import org.postgresql.xa.PGXADataSource;

/**
 * End-to-end proof of the gap fixed against ShardingSphere's crash-recoverable 2PC:
 * {@link XaTransaction#commit()} used to log-and-rethrow when a branch failed to commit after a
 * successful prepare vote, leaving that branch prepared (holding locks) at its backend forever
 * with nothing recording it needed resolving. Real infra throughout -- two real Postgres
 * containers as XA branches, a real Main subprocess as the coordinator, no mocks -- matching this
 * project's established verification style.
 */
class XaRecoveryIntegrationTest {

    @Test
    void twoPhaseCommitAcrossTwoRealBackendsAppliesBothBranches() throws Exception {
        // Regression coverage for the addBranch()/commit() signature and logging changes this
        // feature made: a normal, uninterrupted 2PC across two backends must still apply both
        // branches exactly as before.
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres backendA = RealPostgres.start(java.util.List.of("max_prepared_transactions=10"));
                RealPostgres backendB = RealPostgres.start(java.util.List.of("max_prepared_transactions=10"))) {

            try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA shopA");
                st.execute("CREATE TABLE shopA.orders (id int)");
            }
            try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA shopB");
                st.execute("CREATE TABLE shopB.orders (id int)");
            }

            // A spec-with-entries POLYWIRE_BACKENDS must include an explicit "default" entry --
            // BackendRegistry.fromConfig only auto-registers the implicit control-plane target as
            // "default" when POLYWIRE_BACKENDS is unset entirely (see its javadoc/PgItemStore's).
            // Reuses backendA as the catalog home for anything that doesn't match a schema rule.
            String backends = "default=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                    + ";backendA=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                    + ";backendB=" + backendB.jdbcUrl() + "|" + backendB.username() + "|" + backendB.password();

            try (PolyWireProcess polywire = PolyWireProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                    .env("POLYWIRE_BACKENDS", backends)
                    .env("POLYWIRE_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("POLYWIRE_ROUTER_SCHEMA_RULES", "shopA:backendA,shopB:backendB")
                    .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
                try (Connection conn = DriverManager.getConnection(url, backendA.username(), backendA.password())) {
                    conn.setAutoCommit(false);
                    try (Statement st = conn.createStatement()) {
                        st.execute("INSERT INTO shopA.orders (id) VALUES (1)");
                        st.execute("INSERT INTO shopB.orders (id) VALUES (2)");
                    }
                    conn.commit();
                }
            }

            try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM shopA.orders")) {
                assertEquals(true, rs.next());
                assertEquals(1, rs.getInt(1), "branch A must have committed for real");
            }
            try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM shopB.orders")) {
                assertEquals(true, rs.next());
                assertEquals(1, rs.getInt(1), "branch B must have committed for real");
            }
        }
    }

    @Test
    void inDoubtBranchLeftByASimulatedCrashIsCommittedByStartupRecovery() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres backend = RealPostgres.start(java.util.List.of("max_prepared_transactions=10"))) {
            try (Connection c = DriverManager.getConnection(backend.jdbcUrl(), backend.username(), backend.password());
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE recovery_check (id int)");
            }

            // Simulate exactly the crash window XaRecoveryLog exists for: a branch votes to
            // prepare, the coordinator durably logs the commit decision, and then crashes before
            // calling commit() on the branch -- done here directly against a real XAResource
            // rather than through XaTransaction, so the branch is left genuinely prepared
            // (holding its lock) at the backend, not just simulated in the log.
            PGXADataSource dataSource = new PGXADataSource();
            dataSource.setUrl(backend.jdbcUrl());
            dataSource.setUser(backend.username());
            dataSource.setPassword(backend.password());
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
                // "Crash" here: never call resource.commit(...). The insert is NOT visible to any
                // other connection yet -- the branch is prepared but in doubt.
            } finally {
                xaConn.close();
            }

            // Confirm the pre-recovery state really is in-doubt (not visible), so the assertion
            // below can only pass if recovery genuinely committed it, not because it was already
            // committed some other way.
            try (Connection c = DriverManager.getConnection(backend.jdbcUrl(), backend.username(), backend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM recovery_check")) {
                assertEquals(true, rs.next());
                assertEquals(0, rs.getInt(1), "the branch must still be in-doubt (uncommitted) before recovery runs");
            }

            XaRecoveryLog recoveryLog = new XaRecoveryLog(controlPlaneOptions(controlPlane));
            recoveryLog.ensureSchema();
            String gtridHex = XaRecoveryLog.hex(gtrid);
            recoveryLog.logDecided(gtridHex,
                    java.util.List.of(new XaRecoveryLog.Branch(gtridHex, 0, "recoveryBackend")));

            com.polygres.wire.core.BackendTarget target = new com.polygres.wire.core.BackendTarget(
                    "recoveryBackend", backend.jdbcUrl(), backend.username(), backend.password());
            com.polygres.wire.core.BackendRegistry registry = com.polygres.wire.core.BackendRegistry.fromConfig(
                    "recoveryBackend=" + backend.jdbcUrl() + "|" + backend.username() + "|" + backend.password(), null);

            XaRecovery.recover(recoveryLog, registry);

            try (Connection c = DriverManager.getConnection(backend.jdbcUrl(), backend.username(), backend.password());
                    Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM recovery_check")) {
                assertEquals(true, rs.next());
                assertEquals(1, rs.getInt(1), "startup recovery must have committed the in-doubt branch");
            }
            assertTrue(recoveryLog.findUnresolved().isEmpty(),
                    "the recovered branch must be marked resolved so it isn't retried forever");
        }
    }

    private static com.polygres.wire.server.ServerOptions controlPlaneOptions(RealPostgres controlPlane) {
        return com.polygres.wire.server.ServerOptions.forTesting(
                controlPlane.host(), controlPlane.port(), controlPlane.database(),
                controlPlane.username(), controlPlane.password());
    }
}
