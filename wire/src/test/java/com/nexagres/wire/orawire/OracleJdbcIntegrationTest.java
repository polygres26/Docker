package com.nexagres.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real Oracle JDBC client (ojdbc11's thin driver, real TNS/TTC/O5LOGON
 * wire protocol, not just python-oracledb) gets correct results through orawire's SQL dialect
 * translation into a real Postgres backend -- real subprocess, real Postgres container, no mocks.
 * Complements {@code tests/python/test_orawire.py}'s python-oracledb coverage with a second, JDBC
 * client to prove the wire protocol isn't only correct for one implementation's own quirks.
 */
class OracleJdbcIntegrationTest {

    private static RealPostgres postgres;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        polywire = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "POLYWIRE_ORAWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) {
            polywire.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:oracle:thin:@//localhost:" + polywire.port("orawire") + "/anything";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void select() throws SQLException {
        try (Connection conn = connect();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 21 * 2 FROM DUAL")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void insertUpdateDeleteRoundTrip() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE ojdbc_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();

                stmt.executeUpdate("INSERT INTO ojdbc_it (id, name) VALUES (1, 'alpha')");
                stmt.executeUpdate("INSERT INTO ojdbc_it (id, name) VALUES (2, 'beta')");
                conn.commit();

                stmt.executeUpdate("UPDATE ojdbc_it SET name = 'alpha-updated' WHERE id = 1");
                conn.commit();

                try (ResultSet rs = stmt.executeQuery("SELECT name FROM ojdbc_it WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals("alpha-updated", rs.getString(1));
                }

                stmt.executeUpdate("DELETE FROM ojdbc_it WHERE id = 2");
                conn.commit();

                try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM ojdbc_it")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            } finally {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE ojdbc_it");
                    conn.commit();
                } catch (SQLException ignoredCleanupFailure) {
                    // best-effort
                }
            }
        }
    }

    /** The exact scenario the error-code plan started from: a real Oracle client creating an
     * index that already exists must see a real ORA-00955 -- the correct error CODE ({@link
     * com.nexagres.wire.core.SqlStateErrorMapper}, Phase 2) AND Oracle's own real message text
     * ({@link com.nexagres.wire.core.DialectErrorMessages}, Phase 3), not Postgres's "relation ...
     * already exists" wording behind a native-looking code. */
    @Test
    void creatingAnIndexThatAlreadyExistsReturnsARealOra00955() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE ojdbc_idx_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                stmt.execute("CREATE INDEX ojdbc_idx_it_name ON ojdbc_idx_it(name)");
                conn.commit();

                SQLException duplicate = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> stmt.execute("CREATE INDEX ojdbc_idx_it_name ON ojdbc_idx_it(name)"),
                        "creating an index under a name that already exists must fail");
                assertEquals(955, duplicate.getErrorCode(), "must be a real ORA-00955, not a generic default");
                assertTrue(duplicate.getMessage() != null && duplicate.getMessage().startsWith("ORA-00955"),
                        "client should see Oracle's own real wording, not Postgres's -- got: " + duplicate.getMessage());
            } finally {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE ojdbc_idx_it");
                    conn.commit();
                } catch (SQLException ignoredCleanupFailure) {
                    // best-effort
                }
            }
        }
    }

    /** From the same customer coverage checklist as the ORA-00955 test above: deleting a row that
     * a child table still references must return a genuine ORA-02292 ("child record found"), NOT
     * the insert-side ORA-02291 ("parent key not found") that a naive single-code-per-SQLSTATE
     * mapping would return -- Postgres uses the same 23503 SQLSTATE for both directions, but
     * Oracle (and MySQL) don't. */
    @Test
    void deletingARowThatAChildTableStillReferencesReturnsARealOra02292() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE ojdbc_fk_parent (id INTEGER PRIMARY KEY)");
                stmt.execute("CREATE TABLE ojdbc_fk_child (id INTEGER REFERENCES ojdbc_fk_parent(id))");
                stmt.executeUpdate("INSERT INTO ojdbc_fk_parent (id) VALUES (1)");
                stmt.executeUpdate("INSERT INTO ojdbc_fk_child (id) VALUES (1)");
                conn.commit();

                SQLException blocked = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> stmt.executeUpdate("DELETE FROM ojdbc_fk_parent WHERE id = 1"),
                        "deleting a still-referenced parent row must fail");
                assertEquals(2292, blocked.getErrorCode(),
                        "must be the delete-side ORA-02292, not the insert-side ORA-02291 default");
                assertTrue(blocked.getMessage() != null && blocked.getMessage().startsWith("ORA-02292")
                                && blocked.getMessage().contains("child record found"),
                        "client should see Oracle's real delete-side wording -- got: " + blocked.getMessage());
            } finally {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE ojdbc_fk_child");
                    cleanup.execute("DROP TABLE ojdbc_fk_parent");
                    conn.commit();
                } catch (SQLException ignoredCleanupFailure) {
                    // best-effort
                }
            }
        }
    }
}
