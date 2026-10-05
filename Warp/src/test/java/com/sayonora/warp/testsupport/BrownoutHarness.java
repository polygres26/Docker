package com.sayonora.warp.testsupport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs one protocol's workload against a real Warp whose Postgres backend has a streaming replica, triggers a planned
 * switchover or crashes the primary mid-run, and measures what the client saw: failed operations, the longest pause
 * between successful writes, how long until writes recovered, and whether any acknowledged write is missing afterwards.
 */
public final class BrownoutHarness {

    public static final String TOKEN = "bench-token";

    /** One client connection's worth of operations. A failed call throws; the harness drops the client and opens a new one. */
    public interface Client extends AutoCloseable {
        void write(long id) throws Exception;

        /** A cheap read (the harness does not care what it returns, only that it works). */
        void read(long id) throws Exception;

        @Override
        void close();
    }

    public interface Workload {
        /** Protocol name for reports, e.g. "dynamowire". */
        String name();

        /** Adds this protocol's listener (and any store enablement) to Warp's configuration. */
        void configure(WarpProcess.Builder warp, StoreConfig stores);

        /** Creates whatever the workload needs (table, bucket, queue...). Called once, before the load starts. */
        default void prepare(WarpProcess warp) throws Exception {
        }

        Client open(WarpProcess warp) throws Exception;

        /** Every id the data store holds after the run, read back through the protocol itself (a fresh client). */
        Set<Long> presentIds(WarpProcess warp) throws Exception;
    }

    /** Collects the {@code WARP_BACKEND_STORES} entries that workloads need on the backend named {@code pg}. */
    public static final class StoreConfig {
        private final List<String> stores = new ArrayList<>();

        public void enable(String store) {
            if (!stores.contains(store)) {
                stores.add(store);
            }
        }

        String spec() {
            return stores.isEmpty() ? null : "pg=" + String.join(",", stores);
        }
    }

    public record Summary(long ok, long failed, long p50Ms, long p99Ms, long steadyMaxGapMs, long eventMaxGapMs,
            long firstFailMs, long lastFailMs, long recoveredMs) {
    }

    public record Result(String protocol, String scenario, Summary writes, Summary reads, long acked, long present,
            long lost, long unacked, Map<String, Long> errors, String eventNote, String startupError) {
    }

    private static final int WRITERS = 3;
    private static final int READERS = 2;
    /** WARP_TEST_BROWNOUT_WARPS=3 runs that many Warp instances sharing one config database and one backend set; clients are spread
     * across them. */
    private static final int WARPS = Math.max(1, Integer.parseInt(System.getenv().getOrDefault("WARP_TEST_BROWNOUT_WARPS", "1")));
    private static final long PACE_MS = 20;
    /** WARP_TEST_BROWNOUT_QUICK=true shortens the phases to find setup mistakes fast; the numbers are then not comparable. */
    private static final boolean QUICK = "true".equalsIgnoreCase(System.getenv("WARP_TEST_BROWNOUT_QUICK"));

    /** Every listener that has a fixed default port; a second Warp on the same host would fail to bind them. The workload's own frontend
     * is then given its port by {@code configure}, which overrides these. */
    private static final List<String> FRONTEND_PORT_VARS = List.of("WARP_A2A_PORT", "WARP_MCP_PORT", "WARP_HTTP_PORT",
            "WARP_AMQPWIRE_PORT", "WARP_AWSWIRE_PORT", "WARP_AZBLOBWIRE_PORT", "WARP_AZQUEUEWIRE_PORT", "WARP_AZTABLEWIRE_PORT",
            "WARP_BIGTABLEWIRE_PORT", "WARP_BOLTWIRE_PORT", "WARP_COSMOSWIRE_PORT", "WARP_CQLWIRE_PORT", "WARP_DATASTOREWIRE_PORT",
            "WARP_DYNAMOWIRE_PORT", "WARP_FIRESTOREWIRE_PORT", "WARP_GCSWIRE_PORT", "WARP_GREMLINWIRE_PORT", "WARP_INFLUXWIRE_PORT",
            "WARP_KAFKAWIRE_PORT", "WARP_KINESISWIRE_PORT", "WARP_KMSWIRE_PORT", "WARP_MONGOWIRE_PORT", "WARP_MSSQLWIRE_PORT",
            "WARP_MYWIRE_PORT", "WARP_ORAWIRE_PORT", "WARP_OSWIRE_PORT", "WARP_PGWIRE_PORT",
            "WARP_PUBSUBWIRE_PORT", "WARP_PUBSUBWIRE_REST_PORT", "WARP_REDISWIRE_PORT", "WARP_S3WIRE_PORT", "WARP_SECRETSWIRE_PORT",
            "WARP_SNSWIRE_PORT", "WARP_SQSWIRE_PORT", "WARP_SSMWIRE_PORT", "WARP_STSWIRE_PORT");

    /** Gives every listener of this Warp its own free port, so several instances can share one host. */
    public static void separatePorts(WarpProcess.Builder b) throws java.io.IOException {
        for (String portVar : FRONTEND_PORT_VARS) {
            b.env(portVar, String.valueOf(LocalPostgres.freePort()));
        }
    }

    private BrownoutHarness() {
    }

    // ---------------------------------------------------------------------------------------------------

    public static Result run(Workload workload, String scenario, String pgBin, Path base) throws Exception {
        Path dir = Files.createTempDirectory(base, scenario + "-" + workload.name());
        LocalPostgres cfg = LocalPostgres.primary(pgBin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(pgBin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        try {
            replica = LocalPostgres.replicaOf(pgBin, dir, "replica", primary, LocalPostgres.freePort());
            String spec = "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote";
            List<WarpProcess> warps = new ArrayList<>();
            try {
                // The first instance seeds warp_config from the environment; the others find it and share it (and the config database).
                for (int i = 0; i < WARPS; i++) {
                    WarpProcess.Builder b = WarpProcess.builder()
                            .pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                            .env("WARP_BACKENDS", spec)
                            .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                            .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                            .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                            .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                            .env("WARP_QOS_RATE_PER_SEC", "1000000")
                            .env("WARP_QOS_BURST", "1000000")
                            .env("WARP_ADMIN_TOKEN", TOKEN)
                            .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                            .env("WARP_OTEL_ENDPOINT", "disabled");
                    if (WARPS > 1) { // several instances on one host: no frontend may sit on a fixed default port
                        separatePorts(b);
                    }
                    StoreConfig stores = new StoreConfig();
                    workload.configure(b, stores);
                    if (stores.spec() != null) {
                        b.env("WARP_BACKEND_STORES", stores.spec());
                    }
                    warps.add(b.start());
                }
                return drive(workload, scenario, warps, primary, replica);
            } catch (Exception startup) {
                return new Result(workload.name(), scenario, null, null, 0, 0, 0, 0, Map.of(), "", String.valueOf(startup.getMessage()));
            } finally {
                warps.forEach(WarpProcess::close);
            }
        } finally {
            if (replica != null) {
                replica.stop("fast");
            }
            primary.stop("immediate");
            cfg.stop("immediate");
        }
    }

    private static Result drive(Workload workload, String scenario, List<WarpProcess> warps, LocalPostgres primary,
            LocalPostgres replica) throws Exception {
        WarpProcess warp = warps.get(0);
        boolean oneDown = scenario.endsWith("-one-down");
        boolean staleInstance = scenario.equals("switchover-stale-instance");
        if ((oneDown || staleInstance) && warps.size() < 3) {
            throw new IllegalStateException(scenario + " needs WARP_TEST_BROWNOUT_WARPS >= 3");
        }
        // in the one-down scenario the last instance carries no clients: it is the one that dies before the primary does
        List<WarpProcess> clientWarps = oneDown || staleInstance ? warps.subList(0, warps.size() - 1) : warps;
        workload.prepare(warp);
        long startNanos = System.nanoTime();
        AtomicBoolean stop = new AtomicBoolean();
        ConcurrentLinkedQueue<long[]> writes = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<long[]> reads = new ConcurrentLinkedQueue<>();
        Set<Long> acked = ConcurrentHashMap.newKeySet();
        Map<String, AtomicLong> errors = new ConcurrentHashMap<>();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            threads.add(worker("w" + w, stop, workload, clientWarps.get(w % clientWarps.size()), true, w, startNanos, writes, acked, errors));
        }
        for (int r = 0; r < READERS; r++) {
            threads.add(worker("r" + r, stop, workload, clientWarps.get((w0(r)) % clientWarps.size()), false, 100 + r, startNanos, reads, acked, errors));
        }
        threads.forEach(Thread::start);
        Thread.sleep(QUICK ? 3_000 : 12_000);

        long eventMs = (System.nanoTime() - startNanos) / 1_000_000;
        String eventNote;
        long postEvent;
        if ("switchover".equals(scenario) || staleInstance) {
            if (staleInstance) {
                warps.get(warps.size() - 1).pause(); // frozen across the switchover, so it comes back with an out-of-date idea of the primary
            }
            long t0 = System.nanoTime();
            String res = http("POST", "http://localhost:" + warp.metricsPort() + "/api/failover/pg/switchover",
                    "{\"target\":\"" + replica.url() + "\"}");
            eventNote = (staleInstance ? "one Warp instance paused across the switchover, resumed 8 s later; " : "")
                    + "switchover API took " + (System.nanoTime() - t0) / 1_000_000 + " ms: " + res;
            if (staleInstance) {
                Thread.sleep(8_000);
                warps.get(warps.size() - 1).resume();
            }
            postEvent = QUICK ? 6_000 : 25_000;
        } else {
            if (oneDown) {
                warps.get(warps.size() - 1).close();
                Thread.sleep(1500);
            }
            primary.stop("immediate");
            eventNote = (oneDown ? "one of " + warps.size() + " Warp instances killed, then " : "") + "primary crashed (pg_ctl -m immediate)";
            postEvent = QUICK ? 15_000 : 45_000;
        }
        Thread.sleep(postEvent);
        String instances = warps.size() > 1 ? "; " + instanceViews(warps, replica) : "";
        stop.set(true);
        for (Thread t : threads) {
            t.join(20_000);
        }
        Set<Long> present;
        try {
            present = workload.presentIds(warp);
        } catch (Exception e) {
            errors.computeIfAbsent("durability check failed: " + abbreviate(e), k -> new AtomicLong()).incrementAndGet();
            present = Set.of();
        }
        Set<Long> lost = new java.util.HashSet<>(acked);
        lost.removeAll(present);
        Set<Long> unacked = new java.util.HashSet<>(present);
        unacked.removeAll(acked);
        Map<String, Long> errs = new TreeMap<>();
        errors.forEach((k, v) -> errs.put(k, v.get()));
        return new Result(workload.name(), scenario, summarize(writes, eventMs), summarize(reads, eventMs), acked.size(),
                present.size(), lost.size(), unacked.size(), errs,
                eventNote + instances + "; writable afterwards: " + (replica.writable() ? "former replica" : primary.writable() ? "original primary" : "NONE"),
                null);
    }

    /** What each Warp instance believes after the event: whether it points at the new primary, and which failover events it recorded. */
    private static String instanceViews(List<WarpProcess> warps, LocalPostgres newPrimary) {
        StringBuilder sb = new StringBuilder();
        int following = 0;
        int reachable = 0;
        for (int i = 0; i < warps.size(); i++) {
            sb.append(i == 0 ? "" : ", ").append("instance ").append(i).append(": ");
            try {
                var root = com.google.gson.JsonParser.parseString(http("GET", "http://localhost:" + warps.get(i).metricsPort()
                        + "/api/failover", null)).getAsJsonObject();
                reachable++;
                String primaryUrl = "";
                for (var g : root.getAsJsonArray("groups")) {
                    for (var n : g.getAsJsonObject().getAsJsonArray("nodes")) {
                        if ("primary".equals(n.getAsJsonObject().get("configuredRole").getAsString())) {
                            primaryUrl = n.getAsJsonObject().get("url").getAsString();
                        }
                    }
                }
                boolean follows = primaryUrl.contains(":" + newPrimary.port() + "/");
                following += follows ? 1 : 0;
                Map<String, Integer> kinds = new TreeMap<>();
                for (var e : root.getAsJsonArray("events")) {
                    kinds.merge(e.getAsJsonObject().get("kind").getAsString(), 1, Integer::sum);
                }
                sb.append(follows ? "follows the new primary" : "STILL ON " + primaryUrl).append(" ").append(kinds);
            } catch (Exception e) {
                sb.append("down");
            }
        }
        return following + " of " + reachable + " reachable instances follow the new primary [" + sb + "]";
    }

    private static int w0(int readerIndex) {
        return WRITERS + readerIndex;
    }

    private static Thread worker(String name, AtomicBoolean stop, Workload workload, WarpProcess warp, boolean writer,
            int index, long startNanos, ConcurrentLinkedQueue<long[]> ops, Set<Long> acked, Map<String, AtomicLong> errors) {
        Thread t = new Thread(() -> {
            BrownoutHarness.Client c = null;
            long seq = 0;
            while (!stop.get()) {
                long t0 = System.nanoTime();
                long id = 1_000_000_000L + index * 10_000_000L + (++seq);
                boolean ok = false;
                try {
                    if (c == null) {
                        c = workload.open(warp);
                    }
                    if (writer) {
                        c.write(id);
                        acked.add(id);
                    } else {
                        c.read(1_000_000_000L + (seq % 50));
                    }
                    ok = true;
                } catch (Exception | LinkageError e) {
                    // a LinkageError (a client library on the wrong classpath) must count as a failed op, not silently end the worker
                    errors.computeIfAbsent((writer ? "write: " : "read: ") + abbreviate(e), k -> new AtomicLong()).incrementAndGet();
                    try {
                        if (c != null) {
                            c.close();
                        }
                    } catch (Exception ignored) {
                        // dropping it anyway
                    }
                    c = null;
                    sleep(100);
                }
                ops.add(new long[] {(System.nanoTime() - startNanos) / 1_000_000, (System.nanoTime() - t0) / 1000, ok ? 1 : 0});
                sleep(PACE_MS);
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

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String abbreviate(Throwable e) {
        String m = String.valueOf(e.getMessage()).replaceAll("\\s+", " ");
        String text = e.getClass().getSimpleName() + ": " + (m.length() > 110 ? m.substring(0, 110) : m);
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root != e) { // a wrapper like "Failed to construct kafka producer" says nothing without its cause
            String rm = String.valueOf(root.getMessage()).replaceAll("\\s+", " ");
            text += " <- " + root.getClass().getSimpleName() + ": " + (rm.length() > 140 ? rm.substring(0, 140) : rm);
        }
        return text;
    }

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
        for (int i = 1; i < okTimes.size(); i++) {
            long gap = okTimes.get(i) - okTimes.get(i - 1);
            if (okTimes.get(i) < eventMs) {
                steadyGap = Math.max(steadyGap, gap);
            } else {
                eventGap = Math.max(eventGap, gap);
            }
        }
        long recovered = -1;
        if (lastFail >= 0) {
            for (long t : okTimes) {
                if (t > lastFail) {
                    recovered = t;
                    break;
                }
            }
        }
        return new Summary(ok, failed, lat.isEmpty() ? 0 : lat.get(lat.size() / 2),
                lat.isEmpty() ? 0 : lat.get((int) Math.min(lat.size() - 1, Math.floor(lat.size() * 0.99))), steadyGap, eventGap,
                firstFail, lastFail, recovered);
    }

    public static String http(String method, String url, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).header("Authorization", "Bearer " + TOKEN)
                .header("Content-Type", "application/json");
        b = "POST".equals(method) ? b.POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body)) : b.GET();
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString()).body();
    }

    // ---------------------------------------------------------------------------------------------------

    public static String reportRow(Result r) {
        if (r.startupError() != null) {
            return String.format("| %s | %s | STARTUP/RUN ERROR: %s | | | | | | | |%n", r.protocol(), r.scenario(), r.startupError());
        }
        Summary w = r.writes();
        Summary rd = r.reads();
        String window = w.firstFailMs() < 0 ? "none" : (w.lastFailMs() - w.firstFailMs()) + " ms (recovered +"
                + (w.recoveredMs() < 0 ? "NEVER" : String.valueOf(w.recoveredMs() - w.firstFailMs())) + ")";
        return String.format("| %s | %s | %d | %d | %d | %d | %s | %d | %d | %d | %d | %d |%n", r.protocol(), r.scenario(),
                w.ok(), w.failed(), w.steadyMaxGapMs(), w.eventMaxGapMs(), window, rd.ok(), rd.failed(), r.acked(), r.lost(), r.unacked());
    }

    public static final String REPORT_HEADER = "| protocol | scenario | writes ok | writes failed | steady max gap ms | max gap around event ms | "
            + "failed window | reads ok | reads failed | acked | lost | extra |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n";

    /** Connection helper for workloads that talk to Postgres directly for setup. */
    public static Connection pgConn(LocalPostgres pg) throws Exception {
        return pg.conn();
    }
}
