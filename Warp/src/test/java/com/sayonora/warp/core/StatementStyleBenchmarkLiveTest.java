package com.sayonora.warp.core;

import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Measurement: what does the JDBC statement style cost through each wire protocol? One client connection, one
 * thread, sequential operations, Warp in Adapt mode over a Postgres backend, for every protocol and four styles:
 * a new {@code Statement} per call and a reused {@code Statement} (both with literal SQL), a reused
 * {@code PreparedStatement} and a new {@code PreparedStatement} per call (both with bound parameters). Each style
 * does {@value #OPS} inserts and then {@value #OPS} primary-key selects. Opt-in:
 * WARP_TEST_STYLE_PG_PORT (a Postgres, superuser "warp", trust auth, table {@code t(id bigint primary key, proto text, n int)}).
 * The report goes to {@code target/statement-style-benchmark.md}.
 */
class StatementStyleBenchmarkLiveTest {

    private static final int OPS = 3000;
    private static final int WARMUP = 300;
    private static final String[] PROTOCOLS = System.getenv("WARP_TEST_STYLE_PROTOCOLS") == null
            ? new String[] {"pgwire", "mywire", "mssqlwire", "orawire"}
            : System.getenv("WARP_TEST_STYLE_PROTOCOLS").split(",");
    private static final String[] STYLES = {"new Statement per call", "reused Statement", "reused PreparedStatement",
        "new PreparedStatement per call"};

    private interface Op {
        void run(long id) throws Exception;
    }

    private record Cell(double opsPerSec, double p50Ms, double p99Ms, String error) {
    }

    private static Cell time(Op op, long firstId) {
        try {
            for (int i = 0; i < WARMUP; i++) {
                op.run(firstId - WARMUP + i);
            }
            long[] us = new long[OPS];
            long t0 = System.nanoTime();
            for (int i = 0; i < OPS; i++) {
                long a = System.nanoTime();
                op.run(firstId + i);
                us[i] = (System.nanoTime() - a) / 1000;
            }
            double sec = (System.nanoTime() - t0) / 1e9;
            List<Long> sorted = new ArrayList<>();
            for (long v : us) {
                sorted.add(v);
            }
            Collections.sort(sorted);
            return new Cell(OPS / sec, sorted.get(OPS / 2) / 1000.0, sorted.get((int) (OPS * 0.99)) / 1000.0, null);
        } catch (Exception e) {
            String m = String.valueOf(e.getMessage()).replaceAll("\\s+", " ");
            return new Cell(0, 0, 0, m.length() > 100 ? m.substring(0, 100) : m);
        }
    }

    private static Op[] writeOps(Connection c, String proto) throws Exception {
        java.sql.Statement reused = c.createStatement();
        PreparedStatement reusedPs = c.prepareStatement("insert into t values (?, ?, ?)");
        return new Op[] {
            id -> {
                try (var st = c.createStatement()) {
                    st.executeUpdate("insert into t values (" + id + ", '" + proto + "', 1)");
                }
            },
            id -> reused.executeUpdate("insert into t values (" + id + ", '" + proto + "', 1)"),
            id -> {
                reusedPs.setLong(1, id);
                reusedPs.setString(2, proto);
                reusedPs.setInt(3, 1);
                reusedPs.executeUpdate();
            },
            id -> {
                try (var ps = c.prepareStatement("insert into t values (?, ?, ?)")) {
                    ps.setLong(1, id);
                    ps.setString(2, proto);
                    ps.setInt(3, 1);
                    ps.executeUpdate();
                }
            }};
    }

    private static Op[] readOps(Connection c) throws Exception {
        java.sql.Statement reused = c.createStatement();
        PreparedStatement reusedPs = c.prepareStatement("select n from t where id = ?");
        return new Op[] {
            id -> {
                try (var st = c.createStatement(); var rs = st.executeQuery("select n from t where id = " + id)) {
                    rs.next();
                    rs.getInt(1);
                }
            },
            id -> {
                try (var rs = reused.executeQuery("select n from t where id = " + id)) {
                    rs.next();
                    rs.getInt(1);
                }
            },
            id -> {
                reusedPs.setLong(1, id);
                try (var rs = reusedPs.executeQuery()) {
                    rs.next();
                    rs.getInt(1);
                }
            },
            id -> {
                try (var ps = c.prepareStatement("select n from t where id = ?")) {
                    ps.setLong(1, id);
                    try (var rs = ps.executeQuery()) {
                        rs.next();
                        rs.getInt(1);
                    }
                }
            }};
    }

    @Test
    void measureStatementStylesAcrossTheProtocols() throws Exception {
        String pgPort = System.getenv("WARP_TEST_STYLE_PG_PORT");
        Assumptions.assumeTrue(pgPort != null);
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + pgPort + "/postgres", "warp", "secret");
                var st = c.createStatement()) {
            st.execute("drop table if exists warp_config cascade"); // so the QoS env below is honoured
            st.execute("truncate t");
        }
        StringBuilder md = new StringBuilder("# Statement style cost per protocol (Postgres backend, Adapt mode)\n\n"
                + OPS + " inserts then " + OPS + " primary-key selects per style, one connection, one thread, "
                + "sequential, localhost. ops/s and p50/p99 latency in ms.\n\n");
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_QOS_RATE_PER_SEC", "1000000")
                .env("WARP_QOS_BURST", "1000000")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            Properties pgp = new Properties();
            pgp.setProperty("user", "warp");
            pgp.setProperty("password", "secret");
            int protoIndex = 0;
            for (String proto : PROTOCOLS) {
                Connection c = switch (proto) {
                    case "pgwire" -> DriverManager.getConnection("jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", pgp);
                    case "mywire" -> DriverManager.getConnection("jdbc:mysql://localhost:" + warp.port("mywire")
                            + "/postgres?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true", "warp", "secret");
                    case "mssqlwire" -> DriverManager.getConnection("jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                            + ";encrypt=false;trustServerCertificate=true", "warp", "secret");
                    default -> DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/pg", pgp);
                };
                Op[] writes = writeOps(c, proto);
                Op[] reads = readOps(c);
                md.append("## ").append(proto).append("\n\n| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |\n|---|---|---|---|---|\n");
                for (int s = 0; s < STYLES.length; s++) {
                    long base = (protoIndex * 4L + s + 1) * 10_000_000L;
                    Cell w = time(writes[s], base);
                    Cell r = time(reads[s], base);
                    md.append(String.format("| %s | %s | %s | %s | %s |%n", STYLES[s], cell(w, true), cell(w, false), cell(r, true), cell(r, false)));
                }
                md.append('\n');
                try {
                    c.close();
                } catch (Exception ignored) {
                    // done
                }
                protoIndex++;
            }
        } finally {
            Files.createDirectories(Path.of("target"));
            Files.writeString(Path.of("target/statement-style-benchmark.md"), md.toString());
            System.out.println(md);
        }
    }

    private static String cell(Cell c, boolean throughput) {
        if (c.error() != null) {
            return throughput ? "ERROR: " + c.error() : "-";
        }
        return throughput ? String.format("%.0f", c.opsPerSec()) : String.format("%.2f / %.2f", c.p50Ms(), c.p99Ms());
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
