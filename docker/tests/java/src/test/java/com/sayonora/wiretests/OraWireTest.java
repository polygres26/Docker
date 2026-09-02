package com.sayonora.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** orawire: real Oracle TNS/TTC wire protocol, real ojdbc client, translated to Postgres
 * underneath.
 *
 * <p>Known gap, not silently worked around: DDL type translation doesn't cover Oracle-specific
 * type syntax yet (e.g. NUMBER, VARCHAR2 -- {@code CREATE TABLE ... NUMBER} fails with "type
 * does not exist" against the Postgres backend). These tests use ANSI-standard INTEGER/VARCHAR
 * instead, which are valid in both dialects directly and don't need translation -- same
 * workaround wire's own private integration tests already use.
 *
 * <p><b>Found and fixed while writing this suite</b>: a real ojdbc client's {@code SELECT}
 * against a real table used to fail with {@code ORA-01403: no data found} even when the row was
 * there and committed -- deterministic, and confirmed at the time to also break wire's own
 * private {@code OracleJdbcIntegrationTest}. Root cause: {@code RequestLoop.handleExecute}
 * signaled cursor exhaustion via {@code ResponseWriter.writeInlineExhaustionEnd}, a hardcoded,
 * captured TTC byte blob with error 1403 baked in and no row-count field at all, on *every*
 * response where a query's last batch happened to exhaust the cursor -- including the common
 * case where real rows were written in that very call. Real Oracle clients correctly infer "no
 * more rows" from getting back fewer rows than requested; they don't need (and a real ojdbc
 * client didn't tolerate) an inline hard error on the same response that also carried real data.
 * Fixed by always sending a plain success end with the real row count instead;
 * {@code writeInlineExhaustionEnd} was dead code afterward and has been removed.
 * python-oracledb's more forgiving parser never surfaced this, which is why it went unnoticed
 * until a real ojdbc client was exercised. */
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
    void createInsertSelectRoundtrip() throws Exception {
        String table = "orawire_smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
                st.execute("INSERT INTO " + table + " (id, name) VALUES (1, 'warp')");
                conn.commit();
                try (ResultSet rs = st.executeQuery("SELECT name FROM " + table + " WHERE id = 1")) {
                    rs.next();
                    assertEquals("warp", rs.getString(1));
                }
                st.execute("DROP TABLE " + table);
                conn.commit();
            }
        }
    }

    @Test
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
