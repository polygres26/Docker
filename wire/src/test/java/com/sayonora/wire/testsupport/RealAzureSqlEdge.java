package com.sayonora.wire.testsupport;

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
 * A real, disposable Azure SQL Edge instance -- used to test {@code SqlServerSource} against a
 * REAL SQL-Server-compatible engine, since real {@code mcr.microsoft.com/mssql/server} images do
 * not run at all under QEMU emulation on an ARM64 host (confirmed live: both the {@code
 * 2022-latest} and {@code 2019-latest} images crash on startup with a real "Invalid mapping of
 * address" QEMU/jemalloc incompatibility, not a hypothetical or a timeout -- Azure SQL Edge is
 * Microsoft's own ARM64-native SQL Server-compatible image and runs correctly here).
 *
 * <p><b>Known, real, unavoidable limitation of this test helper, not a bug</b>: Azure SQL Edge has
 * no SQL Server Agent component at all -- confirmed live: {@code sys.sp_cdc_enable_db}/{@code
 * sys.sp_cdc_enable_table} both succeed (the CDC metadata/feature itself is present), but the
 * background capture job that actually reads the transaction log and populates {@code cdc.*_CT}
 * change tables never runs, so {@code sys.fn_cdc_get_max_lsn()} stays {@code NULL} forever even
 * after real inserts. This means {@code SqlServerSource}'s CDC/change-feed path CANNOT be verified
 * end to end against any real SQL-Server-compatible engine in this environment -- neither this one
 * (no Agent) nor a real SQL Server (doesn't run at all under this host's emulation). Real
 * SQL Server CDC verification is a genuine, environment-caused follow-up, not something this
 * project's test suite can currently run; the SNAPSHOT path (schema translation, parallel
 * partitioned reads, upsert/delete SQL correctness) IS verified against this real engine, since
 * that functionality doesn't depend on Agent/CDC at all.
 */
public final class RealAzureSqlEdge implements AutoCloseable {

    private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-sql-edge:latest";

    private final String containerName;
    private final int port;

    private RealAzureSqlEdge(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealAzureSqlEdge start() throws IOException, InterruptedException {
        String containerName = "warp-test-sqledge-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":1433",
                "-e", "ACCEPT_EULA=Y",
                "-e", "MSSQL_SA_PASSWORD=YourStr0ng!Passw0rd",
                DEFAULT_IMAGE);
        RealAzureSqlEdge sql = new RealAzureSqlEdge(containerName, port);
        sql.waitUntilReady(Duration.ofSeconds(60));
        return sql;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    public String username() {
        return "sa";
    }

    public String password() {
        return "YourStr0ng!Passw0rd";
    }

    public String masterJdbcUrl() {
        return "jdbc:sqlserver://" + host() + ":" + port + ";encrypt=false;trustServerCertificate=true";
    }

    public void createDatabase(String name) throws SQLException {
        try (Connection conn = DriverManager.getConnection(masterJdbcUrl(), username(), password());
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE [" + name + "]");
        }
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        SQLException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection conn = DriverManager.getConnection(masterJdbcUrl(), username(), password());
                    Statement st = conn.createStatement()) {
                st.execute("SELECT 1");
                return;
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("Azure SQL Edge container " + containerName + " did not become ready "
                + "within " + timeout, lastFailure);
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
