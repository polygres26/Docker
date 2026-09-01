package com.nexagres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** mssqlwire: real SQL Server TDS wire protocol, real JDBC client, translated to Postgres
 * underneath.
 *
 * <p>Known gap, not silently worked around: every value comes back as a string over TDS
 * regardless of its real Postgres type (no per-column type mapping yet). mssqlwire also has no
 * session-scoped connection (a fresh pooled Postgres connection per statement), so there's no
 * rollback test here. */
class MssqlWireTest {

    private static Connection connect() throws Exception {
        String url = "jdbc:sqlserver://" + TestConfig.HOST + ":" + TestConfig.MSSQLWIRE_PORT
                + ";databaseName=postgres;encrypt=false;trustServerCertificate=true";
        return DriverManager.getConnection(url, "postgres", "postgres");
    }

    @Test
    void simpleSelect() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 21 * 2 AS answer")) {
            rs.next();
            assertEquals("42", rs.getString(1)); // no per-column type mapping yet -- see class javadoc
        }
    }

    @Test
    void createInsertSelectRoundtrip() throws Exception {
        String table = "mssqlwire_smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
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
}
