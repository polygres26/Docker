package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real MySQL client's cross-statement transactions actually work through
 * mywire -- a genuine, serious gap found auditing this frontend for GA transparency: every
 * statement used to open a brand-new backend connection, force autocommit=true, execute, and
 * close, so {@code START TRANSACTION}/{@code COMMIT}/{@code ROLLBACK} (and JDBC's own
 * {@code Connection.setAutoCommit(false)}, which mysql-connector-j implements as a literal
 * {@code SET autocommit = 0} statement) had zero effect on any OTHER statement. Any ORM
 * transaction (ActiveRecord, Django, Hibernate, Sequelize -- virtually every write path in a
 * typical app) was silently non-atomic: a rollback was a no-op, and a crash mid-transaction left
 * partial writes committed instead of rolled back.
 */
class MySqlTransactionIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void rollbackViaSqlVerbsActuallyDiscardsUncommittedWrites() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mysql_txn_it (id INTEGER PRIMARY KEY, val VARCHAR(20))");
                stmt.execute("START TRANSACTION");
                stmt.executeUpdate("INSERT INTO mysql_txn_it (id, val) VALUES (1, 'should-vanish')");
                stmt.execute("ROLLBACK");

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mysql_txn_it")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "the rolled-back INSERT must not have landed");
                }

                stmt.execute("START TRANSACTION");
                stmt.executeUpdate("INSERT INTO mysql_txn_it (id, val) VALUES (2, 'should-persist')");
                stmt.execute("COMMIT");

                try (ResultSet rs = stmt.executeQuery("SELECT val FROM mysql_txn_it WHERE id = 2")) {
                    assertTrue(rs.next());
                    assertEquals("should-persist", rs.getString(1), "the committed INSERT must be visible");
                }

                stmt.execute("DROP TABLE mysql_txn_it");
            }
        }
    }

    @Test
    void jdbcSetAutoCommitFalseActuallyStartsARealTransaction() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres)) {
                try (Statement setup = conn.createStatement()) {
                    setup.execute("CREATE TABLE mysql_txn_autocommit_it (id INTEGER PRIMARY KEY)");
                }
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("INSERT INTO mysql_txn_autocommit_it (id) VALUES (1)");
                }
                conn.rollback();

                conn.setAutoCommit(true);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mysql_txn_autocommit_it")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1),
                            "Connection.setAutoCommit(false) + rollback() must actually roll back the INSERT");
                }

                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE mysql_txn_autocommit_it");
                }
            }
        }
    }

    /** Proves the fix didn't just move the bug: a statement OUTSIDE any transaction must still
     * commit itself immediately (real MySQL's own default autocommit=1 behavior), not get stuck
     * pending until some later COMMIT that may never come. */
    @Test
    void statementsOutsideATransactionStillAutocommitImmediately() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mysql_autocommit_default_it (id INTEGER PRIMARY KEY)");
                stmt.executeUpdate("INSERT INTO mysql_autocommit_default_it (id) VALUES (1)");

                // A completely separate connection must already see it -- proves the first
                // connection's own INSERT was really committed, not left pending.
                try (Connection other = connect(warp, postgres);
                        Statement otherStmt = other.createStatement();
                        ResultSet rs = otherStmt.executeQuery("SELECT COUNT(*) FROM mysql_autocommit_default_it")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1), "a plain statement outside any transaction must autocommit immediately");
                }

                stmt.execute("DROP TABLE mysql_autocommit_default_it");
            }
        }
    }
}
