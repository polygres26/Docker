package com.sayonora.warp.testsupport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

/** A throwaway local MySQL server with GTIDs and a binary log (root, empty password), for tests that need to kill it or replicate from it. */
public final class LocalMySql {
    private final Path bin;
    private final Path dir;
    private final int port;
    private final int serverId;
    private final Path sock;
    private Process process;

    private LocalMySql(Path bin, Path dir, int port, int serverId) throws Exception {
        this.bin = bin;
        this.dir = dir;
        this.port = port;
        this.serverId = serverId;
        this.sock = Files.createTempDirectory("my").resolve("s.sock");
    }

    public int port() {
        return port;
    }

    public String url() {
        return "jdbc:mysql://127.0.0.1:" + port + "/mysql?allowPublicKeyRetrieval=true&useSSL=false";
    }

    public Connection conn() throws Exception {
        return DriverManager.getConnection(url(), "root", "");
    }

    public void exec(String... sql) throws Exception {
        try (Connection c = conn(); var st = c.createStatement()) {
            for (String s : sql) {
                st.execute(s);
            }
        }
    }

    public long scalar(String sql) throws Exception {
        try (Connection c = conn(); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public static LocalMySql create(String binDir, Path base, String name, int port, int serverId) throws Exception {
        Path bin = Path.of(binDir);
        Path dir = base.resolve(name);
        LocalMySql my = new LocalMySql(bin, dir, port, serverId);
        run(bin.resolve("mysqld").toString(), "--no-defaults", "--initialize-insecure", "--datadir=" + dir, "--basedir=" + bin.getParent());
        my.start();
        return my;
    }

    /** Makes this server a replica of {@code source} (GTID auto-position, a short heartbeat so tests see it quickly). */
    public void replicateFrom(LocalMySql source, int heartbeatSeconds) throws Exception {
        exec("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=" + source.port
                + ", SOURCE_USER='root', SOURCE_PASSWORD='', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1, "
                + "SOURCE_HEARTBEAT_PERIOD=" + heartbeatSeconds + ", SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=100000",
                "START REPLICA", "SET GLOBAL read_only=ON", "SET GLOBAL super_read_only=ON");
    }

    public void start() throws Exception {
        ProcessBuilder pb = new ProcessBuilder(bin.resolve("mysqld").toString(), "--no-defaults", "--datadir=" + dir,
                "--basedir=" + bin.getParent(), "--port=" + port, "--bind-address=127.0.0.1", "--socket=" + sock, "--mysqlx=OFF",
                "--server-id=" + serverId, "--log-bin=" + dir.resolve("binlog"), "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                "--max-connections=200", "--pid-file=" + dir.resolve("mysqld.pid"), "--log-error=" + dir + ".err")
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
        process = pb.start();
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            try (Connection c = conn()) {
                return;
            } catch (Exception e) {
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("mysqld did not start on " + port + " (see " + dir + ".err)");
    }

    /** SIGKILL: a crash. */
    public void crash() throws Exception {
        if (process != null) {
            process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
    }

    public void stop() {
        try {
            crash();
        } catch (Exception ignored) {
            // already gone
        }
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IllegalStateException(String.join(" ", cmd) + "\n" + out);
        }
    }
}
