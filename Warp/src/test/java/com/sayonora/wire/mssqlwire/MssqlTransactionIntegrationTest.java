package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real SQL Server client's cross-statement transactions actually work through
 * mssqlwire -- a genuine, serious gap found auditing this frontend for GA transparency: every
 * statement used to open a brand-new backend connection, execute, and close it, so
 * {@code BEGIN TRAN}/{@code COMMIT TRAN}/{@code ROLLBACK TRAN} -- including mssql-jdbc's own
 * {@code "IF @@TRANCOUNT > 0 COMMIT/ROLLBACK TRAN"} idiom for {@code Connection#commit()}/
 * {@code rollback()} -- were no-op'd outright rather than reaching a real backend connection in
 * manual-commit mode. Any ORM transaction (Entity Framework, Hibernate, Sequelize) was silently
 * non-atomic.
 */
class MssqlTransactionIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws SQLException {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void rollbackViaSqlVerbsActuallyDiscardsUncommittedWrites() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mssql_txn_it (id INTEGER PRIMARY KEY, val VARCHAR(20))");
                stmt.execute("BEGIN TRAN");
                stmt.executeUpdate("INSERT INTO mssql_txn_it (id, val) VALUES (1, 'should-vanish')");
                stmt.execute("ROLLBACK TRAN");

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mssql_txn_it")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "the rolled-back INSERT must not have landed");
                }

                stmt.execute("BEGIN TRAN");
                stmt.executeUpdate("INSERT INTO mssql_txn_it (id, val) VALUES (2, 'should-persist')");
                stmt.execute("COMMIT TRAN");

                try (ResultSet rs = stmt.executeQuery("SELECT val FROM mssql_txn_it WHERE id = 2")) {
                    assertTrue(rs.next());
                    assertEquals("should-persist", rs.getString(1), "the committed INSERT must be visible");
                }

                stmt.execute("DROP TABLE mssql_txn_it");
            }
        }
    }

    @Test
    void jdbcSetAutoCommitFalseActuallyStartsARealTransaction() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres)) {
                try (Statement setup = conn.createStatement()) {
                    setup.execute("CREATE TABLE mssql_txn_autocommit_it (id INTEGER PRIMARY KEY)");
                }
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("INSERT INTO mssql_txn_autocommit_it (id) VALUES (1)");
                }
                conn.rollback();

                conn.setAutoCommit(true);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mssql_txn_autocommit_it")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1),
                            "Connection.setAutoCommit(false) + rollback() must actually roll back the INSERT");
                }

                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE mssql_txn_autocommit_it");
                }
            }
        }
    }

    /** Proves the fix didn't just move the bug: a statement OUTSIDE any transaction must still
     * commit itself immediately, not get stuck pending until some later COMMIT that may never
     * come. */
    @Test
    void statementsOutsideATransactionStillAutocommitImmediately() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mssql_autocommit_default_it (id INTEGER PRIMARY KEY)");
                stmt.executeUpdate("INSERT INTO mssql_autocommit_default_it (id) VALUES (1)");

                try (Connection other = connect(warp, postgres);
                        Statement otherStmt = other.createStatement();
                        ResultSet rs = otherStmt.executeQuery("SELECT COUNT(*) FROM mssql_autocommit_default_it")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1), "a plain statement outside any transaction must autocommit immediately");
                }

                stmt.execute("DROP TABLE mssql_autocommit_default_it");
            }
        }
    }
}
