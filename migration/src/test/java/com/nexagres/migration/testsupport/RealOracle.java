package com.nexagres.migration.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A real, disposable Oracle Database Free instance, managed via the plain {@code docker} CLI --
 * same discipline as every other {@code Real*} test helper in this project. Uses {@code
 * gvenzl/oracle-free:23-slim}, which (unlike {@code mcr.microsoft.com/mssql/server}, see {@code
 * RealAzureSqlEdge}'s own javadoc) runs natively on this host's ARM64 architecture -- confirmed
 * live, including real end-to-end LogMiner verification (see {@code
 * OracleSourceIntegrationTest}), unlike SQL Server CDC's environment limitation.
 */
public final class RealOracle implements AutoCloseable {

    private static final String DEFAULT_IMAGE = "gvenzl/oracle-free:23-slim";

    private final String containerName;
    private final int port;

    private RealOracle(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealOracle start() throws IOException, InterruptedException {
        String containerName = "polywire-test-oracle-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":1521",
                "-e", "ORACLE_PASSWORD=OraclePass123",
                DEFAULT_IMAGE);
        RealOracle oracle = new RealOracle(containerName, port);
        oracle.waitUntilReady(Duration.ofSeconds(180)); // first boot creates the whole DB -- genuinely slow
        return oracle;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    /** Oracle Free's default pluggable database -- what every non-{@code sys}/{@code system}
     * connection targets. */
    public String serviceName() {
        return "FREEPDB1";
    }

    /** The CDB ROOT container's service name -- required for {@code ALTER DATABASE ADD
     * SUPPLEMENTAL LOG DATA} specifically: confirmed live that running it against the PDB service
     * ({@link #serviceName()}) fails with ORA-01031 (insufficient privileges) even as {@code
     * system}, because supplemental logging is a CDB-wide setting only settable from a root-
     * container session in Oracle's multitenant architecture -- a real production gotcha for any
     * multitenant Oracle instance, not specific to this test image. */
    public String rootJdbcUrl() {
        return "jdbc:oracle:thin:@" + host() + ":" + port + "/FREE";
    }

    public String sysUsername() {
        return "system";
    }

    public String sysPassword() {
        return "OraclePass123";
    }

    public String sysJdbcUrl() {
        return "jdbc:oracle:thin:@" + host() + ":" + port + "/" + serviceName();
    }

    /** Creates a real, ordinary (non-privileged) schema/user for a test to migrate FROM -- the
     * connector itself is never expected to run as {@code system}/{@code sys}. Grants exactly what
     * a real migration user needs: table creation, and the LogMiner/redo-reading privileges {@code
     * OracleSource} itself documents needing. */
    public void createSchema(String username, String password) throws SQLException {
        try (Connection conn = DriverManager.getConnection(sysJdbcUrl(), sysUsername(), sysPassword());
                Statement st = conn.createStatement()) {
            st.execute("CREATE USER " + username + " IDENTIFIED BY " + password);
            st.execute("GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO " + username);
            st.execute("GRANT EXECUTE_CATALOG_ROLE TO " + username);
            st.execute("GRANT SELECT ANY TRANSACTION TO " + username);
            st.execute("GRANT LOGMINING TO " + username);
            // Real gap found live: EXECUTE_CATALOG_ROLE alone does NOT let a non-privileged user
            // query V$DATABASE (needed for CURRENT_SCN/SUPPLEMENTAL_LOG_DATA_MIN) -- confirmed via
            // a real ORA-00942 the first time OracleSource ran as this schema owner, not the
            // privileged system account this class's own createSchema originally only tested
            // against. SELECT_CATALOG_ROLE is what actually grants that.
            st.execute("GRANT SELECT_CATALOG_ROLE TO " + username);
        }
    }

    public String jdbcUrl() {
        return "jdbc:oracle:thin:@" + host() + ":" + port + "/" + serviceName();
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        SQLException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection conn = DriverManager.getConnection(sysJdbcUrl(), sysUsername(), sysPassword());
                    Statement st = conn.createStatement()) {
                st.execute("SELECT 1 FROM DUAL");
                return;
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("Oracle container " + containerName + " did not become ready within "
                + timeout, lastFailure);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("command failed: " + String.join(" ", command) + "\n" + output);
        }
    }

    @Override
    public void close() {
        try {
            run("docker", "rm", "-f", containerName);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
