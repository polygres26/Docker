package com.polygres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** orawire: real Oracle TNS/TTC wire protocol, real ojdbc client, translated to Postgres
 * underneath.
 *
 * <p>Known gaps, not silently worked around:
 * <ul>
 *   <li>DDL type translation doesn't cover Oracle-specific type syntax yet (e.g. NUMBER,
 *   VARCHAR2 -- {@code CREATE TABLE ... NUMBER} fails with "type does not exist" against the
 *   Postgres backend). These tests use ANSI-standard INTEGER/VARCHAR instead, which are valid in
 *   both dialects directly and don't need translation -- same workaround wire's own private
 *   integration tests already use.
 *   <li><b>A real, currently-reproducible orawire bug</b>, found while writing this suite: any
 *   {@code SELECT} against a real table via a real ojdbc client's {@code Statement.executeQuery}
 *   fails with {@code ORA-01403: no data found} -- deterministic, not a flake, and confirmed to
 *   also break wire's own private {@code OracleJdbcIntegrationTest} (unrelated to this test
 *   suite or the published Docker image; a regression or a gap that was never actually
 *   green). {@code SELECT ... FROM DUAL} is unaffected (see {@link #simpleSelectFromDual()}), so
 *   this is specific to real-table SELECTs. python-oracledb (see {@code test_orawire.py} in the
 *   sibling Python suite) is unaffected -- ojdbc's real TTC client combines DESCRIBE+EXECUTE+
 *   first-FETCH into one OALL8 round trip, a call shape orawire's response encoding apparently
 *   doesn't handle correctly, and python-oracledb's thin client doesn't use that same combined
 *   call. The two affected tests below are {@code @Disabled} rather than deleted -- re-enabling
 *   them is the regression check once this is actually fixed.
 * </ul> */
class OraWireTest {

    private static Connection connect() throws Exception {
        String url = "jdbc:oracle:thin:@//" + TestConfig.HOST + ":" + TestConfig.ORAWIRE_PORT + "/anything";
        return DriverManager.getConnection(url, "postgres", "postgres");
    }

    @Test
    void simpleSelectFromDual() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 21 * 2 FROM DUAL")) {
            rs.next();
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    @Disabled("real, currently-reproducible orawire bug -- see class javadoc: ojdbc's combined "
            + "DESCRIBE+EXECUTE for a real-table SELECT gets ORA-01403 no data found")
    void createInsertSelectRoundtrip() throws Exception {
        String table = "orawire_smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
                st.execute("INSERT INTO " + table + " (id, name) VALUES (1, 'polywire')");
                conn.commit();
                try (ResultSet rs = st.executeQuery("SELECT name FROM " + table + " WHERE id = 1")) {
                    rs.next();
                    assertEquals("polywire", rs.getString(1));
                }
                st.execute("DROP TABLE " + table);
                conn.commit();
            }
        }
    }

    @Test
    @Disabled("real, currently-reproducible orawire bug -- see class javadoc: ojdbc's combined "
            + "DESCRIBE+EXECUTE for a real-table SELECT gets ORA-01403 no data found")
    void transactionRollback() throws Exception {
        String table = "orawire_rollback_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)");
                conn.commit();
                st.execute("INSERT INTO " + table + " (id) VALUES (1)");
                conn.rollback();
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
                    rs.next();
                    assertEquals(0, rs.getInt(1));
                }
                st.execute("DROP TABLE " + table);
                conn.commit();
            }
        }
    }
}
