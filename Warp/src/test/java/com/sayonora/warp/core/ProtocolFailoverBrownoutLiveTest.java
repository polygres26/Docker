package com.sayonora.warp.core;

import com.sayonora.warp.testsupport.WarpProcess;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Measurement, not a pass/fail test: runs a mixed read/write workload through every relational wire protocol that
 * Warp serves over a Postgres backend in Adapt mode (pgwire, mywire, mssqlwire, orawire) at the same time, with a
 * streaming replica, then triggers a planned switchover and (in a separate run) an unplanned failover, and reports
 * the brownout each protocol saw and whether any acknowledged write was lost. The report goes to
 * {@code target/protocol-brownout-report.md}.
 *
 * <p>Opt-in: WARP_TEST_BROWNOUT_PG_BIN=/path/to/postgres/bin (initdb, pg_ctl, pg_basebackup). It starts its own
 * Postgres servers (a config database, a primary and a replica) and its own Warp process.
 */
class ProtocolFailoverBrownoutLiveTest {

    private static final String TOKEN = "bench-token";
    /** WARP_TEST_BROWNOUT_PROTOCOLS=orawire (comma list) narrows the run; default is all four. */
    private static final String[] PROTOCOLS = System.getenv("WARP_TEST_BROWNOUT_PROTOCOLS") == null
            ? new String[] {"pgwire", "mywire", "mssqlwire", "orawire"}
            : System.getenv("WARP_TEST_BROWNOUT_PROTOCOLS").split(",");
    /** WARP_TEST_BROWNOUT_SCENARIOS=switchover (comma list); default is both. */
    private static final String[] SCENARIOS = System.getenv("WARP_TEST_BROWNOUT_SCENARIOS") == null
            ? new String[] {"switchover", "failover"}
            : System.getenv("WARP_TEST_BROWNOUT_SCENARIOS").split(",");
    private static final int WRITERS_PER_PROTOCOL = 3;
    private static final int READERS_PER_PROTOCOL = 2;
    /** Pause between operations of one thread: ~50 ops/s per thread, ~1000 ops/s in total across the four protocols. */
    private static final long PACE_MS = 20;

    // ---- local Postgres ---------------------------------------------------------------------------------

    private static final class Pg {
        final String bin;
        final Path dir;
        final Path socketDir;
        final int port;

        Pg(String bin, Path dir, Path socketDir, int port) {
            this.bin = bin;
            this.dir = dir;
            this.socketDir = socketDir;
            this.port = port;
        }

        static void run(String... cmd) throws Exception {
            ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
            pb.environment().put("LC_ALL", "en_US.UTF-8");
            Process p = pb.start();
            String out = new String(p.getInputStream().readAllBytes());
            if (p.waitFor() != 0) {
                throw new IllegalStateException(String.join(" ", cmd) + "\n" + out);
            }
        }

        static Pg primary(String bin, Path base, String name, int port) throws Exception {
            Path dir = base.resolve(name);
            Path sock = Files.createTempDirectory("bo");
            run(bin + "/initdb", "-D", dir.toString(), "-U", "warp", "-A", "trust");
            Files.writeString(dir.resolve("postgresql.conf"), "\nport=" + port + "\nlisten_addresses='127.0.0.1'\n"
                    + "unix_socket_directories='" + sock + "'\nwal_level=replica\nmax_wal_senders=10\nhot_standby=on\n"
                    + "max_connections=300\n", java.nio.file.StandardOpenOption.APPEND);
            Files.writeString(dir.resolve("pg_hba.conf"), "\nhost replication all 127.0.0.1/32 trust\n",
                    java.nio.file.StandardOpenOption.APPEND);
            Pg pg = new Pg(bin, dir, sock, port);
            pg.start();
            return pg;
        }

        static Pg replicaOf(String bin, Path base, String name, Pg primary, int port) throws Exception {
            Path dir = base.resolve(name);
            Path sock = Files.createTempDirectory("bo");
            run(bin + "/pg_basebackup", "-h", "127.0.0.1", "-p", String.valueOf(primary.port), "-U", "warp", "-D",
                    dir.toString(), "-R", "-X", "stream");
            Files.writeString(dir.resolve("postgresql.conf"), "\nport=" + port + "\nunix_socket_directories='" + sock + "'\n",
                    java.nio.file.StandardOpenOption.APPEND);
            Pg pg = new Pg(bin, dir, sock, port);
            pg.start();
            return pg;
        }

        void start() throws Exception {
            run(bin + "/pg_ctl", "-D", dir.toString(), "-l", dir + ".log", "-w", "start");
        }

        void stop(String mode) {
            try {
                run(bin + "/pg_ctl", "-D", dir.toString(), "-m", mode, "-w", "stop");
            } catch (Exception ignored) {
                // already stopped
            }
        }

        String url() {
            return "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
        }

        Connection conn() throws Exception {
            return DriverManager.getConnection(url(), "warp", "secret");
        }

        boolean writable() {
            try (Connection c = conn(); var st = c.createStatement(); var rs = st.executeQuery("select pg_is_in_recovery()")) {
                rs.next();
                return !rs.getBoolean(1);
            } catch (Exception e) {
                return false;
            }
        }
    }

    // ---- workload ---------------------------------------------------------------------------------------

    private interface ConnFactory {
        Connection open() throws Exception;
    }

    private static final class Stats {
        final String protocol;
        final long startNanos;
        final ConcurrentLinkedQueue<long[]> writes = new ConcurrentLinkedQueue<>(); // {tMs, latencyUs, ok}
        final ConcurrentLinkedQueue<long[]> reads = new ConcurrentLinkedQueue<>();
        final Set<Long> acked = ConcurrentHashMap.newKeySet();
        final Map<String, AtomicLong> errors = new ConcurrentHashMap<>();

        Stats(String protocol, long startNanos) {
            this.protocol = protocol;
            this.startNanos = startNanos;
        }

        long nowMs() {
            return (System.nanoTime() - startNanos) / 1_000_000;
        }

        void error(String kind, Exception e) {
            String m = String.valueOf(e.getMessage()).replaceAll("\\s+", " ");
            if (m.length() > 110) {
                m = m.substring(0, 110);
            }
            errors.computeIfAbsent(kind + ": " + m, k -> new AtomicLong()).incrementAndGet();
        }
    }

    private static Thread worker(String name, AtomicBoolean stop, ConnFactory factory, Stats stats, boolean writer,
            int protoIndex, int threadIndex, String proto) {
        Thread t = new Thread(() -> {
            Connection c = null;
            long seq = 0;
            while (!stop.get()) {
                long t0 = System.nanoTime();
                long id = (protoIndex + 1) * 1_000_000_000L + threadIndex * 10_000_000L + (++seq);
                boolean ok = false;
                try {
                    if (c == null) {
                        c = factory.open();
                    }
                    try (var st = c.createStatement()) {
                        if (writer) {
                            st.executeUpdate("insert into t values (" + id + ", '" + proto + "', " + seq + ")");
                        } else {
                            try (var rs = st.executeQuery("select count(*) from t where proto = '" + proto + "'")) {
                                rs.next();
                                rs.getLong(1);
                            }
                        }
                    }
                    ok = true;
                    if (writer) {
                        stats.acked.add(id);
                    }
                } catch (Exception e) {
                    stats.error(writer ? "write" : "read", e);
                    try {
                        if (c != null) {
                            c.close();
                        }
                    } catch (Exception ignored) {
                        // dropping it anyway
                    }
                    c = null;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
                long us = (System.nanoTime() - t0) / 1000;
                (writer ? stats.writes : stats.reads).add(new long[] {stats.nowMs(), us, ok ? 1 : 0});
                try {
                    Thread.sleep(PACE_MS);
                } catch (InterruptedException ie) {
                    return;
                }
            }
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception ignored) {
                // done
            }
        }, name);
        t.setDaemon(true);
        return t;
    }

    // ---- analysis ---------------------------------------------------------------------------------------

    private record Summary(long ok, long failed, long p50Ms, long p99Ms, long steadyMaxGapMs, long eventMaxGapMs,
            long firstFailMs, long lastFailMs, long recoveredMs) {
    }

    private static long pct(List<Long> sorted, double p) {
        return sorted.isEmpty() ? 0 : sorted.get((int) Math.min(sorted.size() - 1, Math.floor(p * sorted.size())));
    }

    /** {@code eventMs}: when the switchover/crash was triggered, on the workload's own clock. */
    private static Summary summarize(ConcurrentLinkedQueue<long[]> ops, long eventMs) {
        List<long[]> all = new ArrayList<>(ops);
        all.sort((a, b) -> Long.compare(a[0], b[0]));
        long ok = 0;
        long failed = 0;
        List<Long> lat = new ArrayList<>();
        List<Long> okTimes = new ArrayList<>();
        long firstFail = -1;
        long lastFail = -1;
        for (long[] o : all) {
            if (o[2] == 1) {
                ok++;
                okTimes.add(o[0]);
                if (o[0] < eventMs) {
                    lat.add(o[1] / 1000);
                }
            } else {
                failed++;
                if (o[0] >= eventMs - 500) {
                    if (firstFail < 0) {
                        firstFail = o[0];
                    }
                    lastFail = o[0];
                }
            }
        }
        Collections.sort(lat);
        long steadyGap = 0;
        long eventGap = 0;
        long recovered = -1;
        for (int i = 1; i < okTimes.size(); i++) {
            long gap = okTimes.get(i) - okTimes.get(i - 1);
            if (okTimes.get(i) < eventMs) {
                steadyGap = Math.max(steadyGap, gap);
            } else {
                eventGap = Math.max(eventGap, gap);
            }
        }
        if (lastFail >= 0) {
            for (long t : okTimes) {
                if (t > lastFail) {
                    recovered = t;
                    break;
                }
            }
        }
        return new Summary(ok, failed, pct(lat, 0.50), pct(lat, 0.99), steadyGap, eventGap, firstFail, lastFail, recovered);
    }

    // ---- scenario ---------------------------------------------------------------------------------------

    private record Result(String scenario, Map<String, Summary> writes, Map<String, Summary> reads,
            Map<String, long[]> durability, Map<String, Map<String, Long>> errors, String eventNote, String metrics,
            String failoverEvents) {
    }

    private static int freePort() throws Exception {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static String http(String method, String url, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).header("Authorization", "Bearer " + TOKEN)
                .header("Content-Type", "application/json");
        b = "POST".equals(method) ? b.POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body)) : b.GET();
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString()).body();
    }

    private static String replicaMetrics(int metricsPort) {
        try {
            StringBuilder sb = new StringBuilder();
            for (String l : http("GET", "http://localhost:" + metricsPort + "/metrics", null).split("\n")) {
                if (l.startsWith("warp_replica_reads_routed_total{") || l.startsWith("warp_replica_read_decisions_total{")) {
                    sb.append(l).append('\n');
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return "(metrics unavailable: " + e + ")";
        }
    }

    private Result runScenario(String bin, Path base, String scenario) throws Exception {
        Path dir = Files.createTempDirectory(base, scenario);
        Pg cfg = Pg.primary(bin, dir, "cfg", freePort());
        Pg primary = Pg.primary(bin, dir, "primary", freePort());
        try (Connection c = primary.conn(); var st = c.createStatement()) {
            st.execute("create table t (id bigint primary key, proto text, n int)");
        }
        Pg replica = Pg.replicaOf(bin, dir, "replica", primary, freePort());
        long deadline = System.currentTimeMillis() + 20_000;
        while (true) {
            try (Connection c = replica.conn(); var st = c.createStatement(); var rs = st.executeQuery("select count(*) from t")) {
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(300);
            }
        }
        String spec = "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote";
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", cfg.port, "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "500")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                // the harness default (1000/s) throttles this workload and would show up as "failures"; the point
                // here is the failover, not admission control
                .env("WARP_QOS_RATE_PER_SEC", "1000000")
                .env("WARP_QOS_BURST", "1000000")
                .env("WARP_ADMIN_TOKEN", TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            Properties pgp = new Properties();
            pgp.setProperty("user", "warp");
            pgp.setProperty("password", "secret");
            Map<String, ConnFactory> factories = new java.util.HashMap<>();
            factories.put("pgwire", () -> DriverManager.getConnection("jdbc:postgresql://localhost:" + warp.port("pgwire")
                    + "/pg?connectTimeout=5&socketTimeout=10&loginTimeout=5", pgp));
            factories.put("mywire", () -> DriverManager.getConnection("jdbc:mysql://localhost:" + warp.port("mywire")
                    + "/pg?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=10000", "warp", "secret"));
            factories.put("mssqlwire", () -> DriverManager.getConnection("jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                    + ";databaseName=pg;encrypt=false;trustServerCertificate=true;loginTimeout=5;socketTimeout=10000", "warp", "secret"));
            factories.put("orawire", () -> {
                Properties p = new Properties();
                p.setProperty("user", "warp");
                p.setProperty("password", "secret");
                p.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
                p.setProperty("oracle.jdbc.ReadTimeout", "10000");
                return DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/pg", p);
            });

            long startNanos = System.nanoTime();
            AtomicBoolean stop = new AtomicBoolean();
            Map<String, Stats> stats = new TreeMap<>();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < PROTOCOLS.length; i++) {
                Stats s = new Stats(PROTOCOLS[i], startNanos);
                stats.put(PROTOCOLS[i], s);
                for (int w = 0; w < WRITERS_PER_PROTOCOL; w++) {
                    threads.add(worker(PROTOCOLS[i] + "-w" + w, stop, factories.get(PROTOCOLS[i]), s, true, i, w, PROTOCOLS[i]));
                }
                for (int r = 0; r < READERS_PER_PROTOCOL; r++) {
                    threads.add(worker(PROTOCOLS[i] + "-r" + r, stop, factories.get(PROTOCOLS[i]), s, false, i, 100 + r, PROTOCOLS[i]));
                }
            }
            threads.forEach(Thread::start);
            Thread.sleep(12_000); // steady state
            String before = replicaMetrics(warp.metricsPort());

            long eventMs = (System.nanoTime() - startNanos) / 1_000_000;
            String eventNote;
            long postEvent;
            if ("switchover".equals(scenario)) {
                long t0 = System.nanoTime();
                String res = http("POST", "http://localhost:" + warp.metricsPort() + "/api/failover/pg/switchover",
                        "{\"target\":\"" + replica.url() + "\"}");
                eventNote = "POST /api/failover/pg/switchover took " + (System.nanoTime() - t0) / 1_000_000 + " ms: " + res;
                postEvent = 25_000;
            } else {
                primary.stop("immediate");
                eventNote = "primary stopped with pg_ctl -m immediate (crash), Warp left to detect and promote";
                postEvent = 45_000;
            }
            Thread.sleep(postEvent);
            stop.set(true);
            for (Thread t : threads) {
                t.join(15_000);
            }
            String after = replicaMetrics(warp.metricsPort());
            String failoverEvents = http("GET", "http://localhost:" + warp.metricsPort() + "/api/failover", null);

            // durability: every acknowledged write must exist on whichever node is the primary now
            Pg now = replica.writable() ? replica : (primary.writable() ? primary : null);
            Map<String, long[]> durability = new TreeMap<>();
            for (String p : PROTOCOLS) {
                Set<Long> present = new HashSet<>();
                if (now != null) {
                    try (Connection c = now.conn(); var st = c.createStatement();
                            var rs = st.executeQuery("select id from t where proto = '" + p + "'")) {
                        while (rs.next()) {
                            present.add(rs.getLong(1));
                        }
                    }
                }
                Set<Long> lost = new HashSet<>(stats.get(p).acked);
                lost.removeAll(present);
                Set<Long> unacked = new HashSet<>(present);
                unacked.removeAll(stats.get(p).acked);
                durability.put(p, new long[] {stats.get(p).acked.size(), present.size(), lost.size(), unacked.size()});
            }
            Map<String, Summary> w = new TreeMap<>();
            Map<String, Summary> r = new TreeMap<>();
            Map<String, Map<String, Long>> errs = new TreeMap<>();
            for (String p : PROTOCOLS) {
                w.put(p, summarize(stats.get(p).writes, eventMs));
                r.put(p, summarize(stats.get(p).reads, eventMs));
                Map<String, Long> e = new TreeMap<>();
                stats.get(p).errors.forEach((k, v) -> e.put(k, v.get()));
                errs.put(p, e);
            }
            String finalPrimary = now == null ? "NONE WRITABLE" : (now == replica ? "former replica" : "original primary");
            Result res = new Result(scenario, w, r, durability, errs, eventNote + "; writable node afterwards: " + finalPrimary,
                    "before event:\n" + before + "after run:\n" + after, failoverEvents);
            replica.stop("fast");
            primary.stop("immediate");
            cfg.stop("immediate");
            return res;
        }
    }

    private static String report(List<Result> results) {
        StringBuilder sb = new StringBuilder("# Protocol brownout results (Postgres backend, Adapt mode, one streaming replica)\n\n");
        for (Result r : results) {
            sb.append("## ").append(r.scenario()).append("\n\n").append(r.eventNote()).append("\n\n");
            sb.append("| protocol | writes ok | writes failed | write p50/p99 ms (pre-event) | steady max gap ms | max gap around event ms | failed window ms | reads ok | reads failed | read max gap around event ms | acked | lost | extra rows |\n");
            sb.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
            for (String p : PROTOCOLS) {
                Summary w = r.writes().get(p);
                Summary rd = r.reads().get(p);
                long[] d = r.durability().get(p);
                String window = w.firstFailMs() < 0 ? "none" : (w.lastFailMs() - w.firstFailMs()) + " (recovered at +"
                        + (w.recoveredMs() < 0 ? "never" : String.valueOf(w.recoveredMs() - w.firstFailMs())) + ")";
                sb.append(String.format("| %s | %d | %d | %d / %d | %d | %d | %s | %d | %d | %d | %d | %d | %d |%n", p, w.ok(),
                        w.failed(), w.p50Ms(), w.p99Ms(), w.steadyMaxGapMs(), w.eventMaxGapMs(), window, rd.ok(), rd.failed(),
                        rd.eventMaxGapMs(), d[0], d[2], d[3]));
            }
            sb.append("\nErrors seen (count):\n");
            for (String p : PROTOCOLS) {
                sb.append("- ").append(p).append(": ").append(r.errors().get(p).isEmpty() ? "none" : "").append('\n');
                r.errors().get(p).forEach((k, v) -> sb.append("    - ").append(v).append(" x ").append(k).append('\n'));
            }
            sb.append("\nReplica read routing (/metrics):\n```\n").append(r.metrics()).append("```\n\n");
            sb.append("Failover monitor state:\n```\n").append(r.failoverEvents()).append("\n```\n\n");
        }
        return sb.toString();
    }

    @Test
    void measureBrownoutAcrossAllProtocolsForSwitchoverAndFailover() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && new File(bin, "initdb").exists());
        Path base = Files.createTempDirectory("brownout");
        List<Result> results = new ArrayList<>();
        try {
            for (String scenario : SCENARIOS) {
                results.add(runScenario(bin, base, scenario));
            }
        } finally {
            String text = report(results);
            Files.createDirectories(Path.of("target"));
            Files.writeString(Path.of("target/protocol-brownout-report.md"), text);
            System.out.println(text);
        }
    }
}
