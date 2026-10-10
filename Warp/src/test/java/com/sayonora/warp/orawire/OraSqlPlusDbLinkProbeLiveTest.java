package com.sayonora.warp.orawire;

import com.sayonora.warp.testsupport.RealOracle;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Probe, not a guard: records what SQL*Plus and a table@link query do through orawire in emulate and bridge mode. Opt-in: WARP_TEST_ORAPROBE=true (docker). */
class OraSqlPlusDbLinkProbeLiveTest {

    private static String sqlplus(RealOracle oracle, int warpPort, String user, String password, String script) throws Exception {
        String host = "host.docker.internal";
        Process p = new ProcessBuilder("docker", "exec", oracle.containerName(), "bash", "-c",
                "printf '" + script.replace("'", "'\\''") + "\\nexit\\n' | timeout 40 sqlplus -L -S " + user + "/" + password + "@//" + host + ":" + warpPort + "/anything; echo EXIT=$?")
                .redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        p.waitFor(60, TimeUnit.SECONDS);
        return out.strip();
    }

    private static String jdbc(String url, String user, String password, String sql) {
        try (Connection c = DriverManager.getConnection(url, user, password); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1)).append(' ');
            }
            return "OK " + sb;
        } catch (Exception e) {
            return "ERROR " + e.getMessage().replace('\n', ' ');
        }
    }

    @Test
    @Timeout(900)
    void probe() throws Exception {
        Assumptions.assumeTrue("true".equals(System.getenv("WARP_TEST_ORAPROBE")), "opt-in");
        try (RealOracle oracle = RealOracle.start(); RealPostgres pg = RealPostgres.start()) {
            try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password()); Statement st = c.createStatement()) {
                st.execute("create table remote_t (id int primary key, v text)");
                st.execute("insert into remote_t values (1, 'x')");
            }
            try (Connection c = DriverManager.getConnection(oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword()); Statement st = c.createStatement()) {
                st.execute("CREATE DATABASE LINK loop CONNECT TO " + oracle.sysUsername() + " IDENTIFIED BY \"" + oracle.sysPassword() + "\" USING '//localhost:1521/" + oracle.serviceName() + "'");
            }
            for (String mode : new String[] {"bridge"}) {
                String u = mode.equals("bridge") ? "appuser" : pg.username();
                String pw = mode.equals("bridge") ? "apppw" : pg.password();
                WarpProcess.Builder b = WarpProcess.builder().pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                        .frontend("orawire", "WARP_ORAWIRE_PORT")
                        .env("WARP_GRPC_PORT", String.valueOf(free()))
                        .env("WARP_ORACLE_HOST", oracle.host()).env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                        .env("WARP_ORACLE_SERVICE", oracle.serviceName()).env("WARP_ORACLE_USER", oracle.sysUsername())
                        .env("WARP_ORACLE_PASSWORD", oracle.sysPassword()).env("WARP_OTEL_ENDPOINT", "disabled");
                if (mode.equals("bridge")) {
                    b.env("WARP_ORACLE_BACKEND_MODE", "bridge").env("WARP_ORACLE_BRIDGE_LOGIN_USER", "appuser").env("WARP_ORACLE_BRIDGE_LOGIN_PASSWORD", "apppw");
                }
                try (WarpProcess warp = b.start()) {
                    int port = warp.port("orawire");
                    String url = "jdbc:oracle:thin:@//localhost:" + port + "/anything";
                    long t0 = System.currentTimeMillis();
                    String sp = sqlplus(oracle, port, u, pw, "select 1 from dual;");
                    System.out.println("ORAPROBE " + mode + " sqlplus (" + (System.currentTimeMillis() - t0) + " ms): " + sp.replace('\n', '|'));
                    System.out.println("ORAPROBE " + mode + " jdbc select 1 from dual: " + jdbc(url, u, pw, "select 1 from dual"));
                    System.out.println("ORAPROBE " + mode + " jdbc dual@loop (real Oracle DATABASE LINK): " + jdbc(url, u, pw, "select 1 from dual@loop"));
                    System.out.println("ORAPROBE " + mode + " sqlplus dual@loop: " + sqlplus(oracle, port, u, pw, "select 1 from dual@loop;").replace('\n', '|'));
                    System.out.println("ORAPROBE " + mode + " jdbc remote_t: " + jdbc(url, u, pw, "select v from remote_t"));
                    System.out.println("ORAPROBE " + mode + " jdbc remote_t@default: " + jdbc(url, u, pw, "select v from remote_t@default"));
                    System.out.println("ORAPROBE " + mode + " jdbc remote_t@nolink: " + jdbc(url, u, pw, "select v from remote_t@nolink"));
                }
            }
        }
    }

    private static int free() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
