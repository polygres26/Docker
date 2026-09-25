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
 * mywire's own version of {@code MssqlDualPortNativeAndTranslatedIntegrationTest} -- ONE running
 * Warp process serves BOTH native MySQL passthrough AND the dialect-translated-into-Postgres path
 * AT THE SAME TIME, on two different ports, not a single global mode toggle requiring a restart
 * to switch (the only thing {@link MySqlNativeBackendIntegrationTest} proves).
 */
class MySqlDualPortNativeAndTranslatedIntegrationTest {

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
    void primaryPortStaysTranslatedWhileSecondPortIsNativePassthroughOnTheSameProcess() throws Exception {
        postgres = RealPostgres.start();
        mysql = RealMySql.start();

        try (Connection c = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE dual_port_native_test (id INT PRIMARY KEY, label VARCHAR(50))");
            st.execute("INSERT INTO dual_port_native_test (id, label) VALUES (1, 'real-mysql-row')");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .frontend("mywire-native", "WARP_MYWIRE_NATIVE_PORT")
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        String translatedUrl = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        String nativeUrl = "jdbc:mysql://localhost:" + warp.port("mywire-native") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";

        // Native port: reads the row written DIRECTLY to real MySQL, bypassing Warp entirely.
        try (Connection conn = DriverManager.getConnection(nativeUrl, postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT label FROM dual_port_native_test WHERE id = 1")) {
            assertTrue(rs.next(), "native port must see the row written directly to real MySQL");
            assertEquals("real-mysql-row", rs.getString(1));
        }

        // Translated port, on the SAME running process: an ordinary CREATE TABLE/INSERT must land
        // in Warp's configured Postgres, proving the primary listener's own mode was NOT flipped
        // to native by the second listener's own startup.
        try (Connection conn = DriverManager.getConnection(translatedUrl, postgres.username(), postgres.password());
                Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE dual_port_translated_test (id INTEGER PRIMARY KEY, val VARCHAR(20))");
            st.executeUpdate("INSERT INTO dual_port_translated_test (id, val) VALUES (1, 'from-translated-port')");
        }
        try (Connection controlPlane = DriverManager.getConnection(
                "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                postgres.username(), postgres.password());
                Statement st = controlPlane.createStatement();
                ResultSet rs = st.executeQuery("SELECT val FROM dual_port_translated_test WHERE id = 1")) {
            assertTrue(rs.next(), "the translated port's own INSERT must be visible directly in real Postgres");
            assertEquals("from-translated-port", rs.getString(1));
        }
    }
}
