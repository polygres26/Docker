package com.sayonora.warp.testsupport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;

/** A throwaway local Postgres (initdb / pg_ctl / pg_basebackup from a bin directory), for tests that need to kill or promote one. */
public final class LocalPostgres {
    private final String bin;
    private final Path dir;
    private final int port;

    private LocalPostgres(String bin, Path dir, int port) {
        this.bin = bin;
        this.dir = dir;
        this.port = port;
    }

    public int port() {
        return port;
    }

    public String url() {
        return "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
    }

    public Connection conn() throws Exception {
        return DriverManager.getConnection(url(), "warp", "secret");
    }

    private static void run(String... cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().put("LC_ALL", "en_US.UTF-8");
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IllegalStateException(String.join(" ", cmd) + "\n" + out);
        }
    }

    public static LocalPostgres primary(String bin, Path base, String name, int port) throws Exception {
        Path dir = base.resolve(name);
        Path sock = Files.createTempDirectory("lp");
        run(bin + "/initdb", "-D", dir.toString(), "-U", "warp", "-A", "trust");
        Files.writeString(dir.resolve("postgresql.conf"), "\nport=" + port + "\nlisten_addresses='127.0.0.1'\n"
                + "unix_socket_directories='" + sock + "'\nwal_level=replica\nmax_wal_senders=10\nhot_standby=on\n"
                + "max_connections=300\n", java.nio.file.StandardOpenOption.APPEND);
        Files.writeString(dir.resolve("pg_hba.conf"), "\nhost replication all 127.0.0.1/32 trust\n",
                java.nio.file.StandardOpenOption.APPEND);
        LocalPostgres pg = new LocalPostgres(bin, dir, port);
        pg.start();
        return pg;
    }

    public static LocalPostgres replicaOf(String bin, Path base, String name, LocalPostgres primary, int port) throws Exception {
        Path dir = base.resolve(name);
        Path sock = Files.createTempDirectory("lp");
        run(bin + "/pg_basebackup", "-h", "127.0.0.1", "-p", String.valueOf(primary.port), "-U", "warp", "-D",
                dir.toString(), "-R", "-X", "stream");
        Files.writeString(dir.resolve("postgresql.conf"), "\nport=" + port + "\nunix_socket_directories='" + sock + "'\n",
                java.nio.file.StandardOpenOption.APPEND);
        LocalPostgres pg = new LocalPostgres(bin, dir, port);
        pg.start();
        return pg;
    }

    public void start() throws Exception {
        run(bin + "/pg_ctl", "-D", dir.toString(), "-l", dir + ".log", "-w", "start");
    }

    public void stop(String mode) {
        try {
            run(bin + "/pg_ctl", "-D", dir.toString(), "-m", mode, "-w", "stop");
        } catch (Exception ignored) {
            // already stopped
        }
    }

    public boolean writable() {
        try (Connection c = conn(); var st = c.createStatement(); var rs = st.executeQuery("select pg_is_in_recovery()")) {
            rs.next();
            return !rs.getBoolean(1);
        } catch (Exception e) {
            return false;
        }
    }

    public static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
