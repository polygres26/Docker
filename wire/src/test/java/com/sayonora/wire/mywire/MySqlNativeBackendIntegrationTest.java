package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * As {@code MssqlNativeBackendIntegrationTest} -- proves {@code WARP_MYWIRE_BACKEND=mysql}
 * actually reaches a REAL MySQL instance. This feature already shipped before this test existed;
 * writing it (as part of building mssqlwire's own equivalent) found a real, previously-undiscovered
 * bug in this already-shipped MySQL path -- see {@link MySqlWireSessionHandler}'s own updated
 * comment at its native-mode execute call for the fix (the shared pipeline was still translating
 * MYSQL->POSTGRES and running Postgres-specific session setup even in native mode, since
 * {@code RouterStage}'s "default" backend is always Postgres-typed regardless of this protocol's
 * own native-mode flag).
 */
class MySqlNativeBackendIntegrationTest {

    private RealPostgres postgres;
    private RealMySql mysql;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (mysql != null) mysql.close();
        if (postgres != null) postgres.close();
    }

    @Test
    void aClientQueryInNativeModeReadsFromTheRealMySqlBackendNotPostgres() throws Exception {
        postgres = RealPostgres.start();

        mysql = RealMySql.start();
        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE native_test (id INT PRIMARY KEY, label VARCHAR(50))");
            st.execute("INSERT INTO native_test (id, label) VALUES (1, 'real-mysql-row')");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_MYWIRE_BACKEND", "mysql")
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        // Login credentials authenticate against Warp's own front door (the configured Postgres
        // user), unrelated to which real backend the query ends up running against.
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT label FROM native_test WHERE id = 1")) {
            assertTrue(rs.next(), "expected the row written directly to the real MySQL backend");
            assertEquals("real-mysql-row", rs.getString(1),
                    "the value must come from the real MySQL table -- getting no row or a "
                            + "different value would mean native mode is still hitting Postgres");
        }
    }
}
