package com.sayonora.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** pgwire: real Postgres wire protocol v3, real JDBC client -- a direct passthrough (no dialect
 * translation), so it's the baseline every other protocol's result should match. */
class PgWireTest {

    private static Connection connect() throws Exception {
        String url = "jdbc:postgresql://" + TestConfig.HOST + ":" + TestConfig.PGWIRE_PORT + "/postgres";
        return DriverManager.getConnection(url, "postgres", "postgres");
    }

    @Test
    void simpleSelect() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 21 * 2")) {
            rs.next();
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void createInsertSelectRoundtrip() throws Exception {
        String table = "pgwire_smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT)");
            st.execute("INSERT INTO " + table + " (id, name) VALUES (1, 'warp')");
            try (ResultSet rs = st.executeQuery("SELECT name FROM " + table + " WHERE id = 1")) {
                rs.next();
                assertEquals("warp", rs.getString(1));
            }
            st.execute("DROP TABLE " + table);
        }
    }

    @Test
    void transactionRollback() throws Exception {
        String table = "pgwire_rollback_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY)");
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
