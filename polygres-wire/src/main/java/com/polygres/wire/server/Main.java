package com.polygres.wire.server;

import com.polygres.wire.cluster.CacheStage;
import com.polygres.wire.cluster.PolyWireCluster;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.DialectTranslationStage;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.RouterStage;
import com.polygres.wire.core.StatsCollectorStage;
import com.polygres.wire.http.admin.MetricsServer;
import com.polygres.wire.mywire.MySqlWireSessionHandler;
import com.polygres.wire.orawire.session.SessionHandler;
import com.polygres.wire.pgwire.PgBackendPool;
import com.polygres.wire.pgwire.PgWireSessionHandler;
import com.polygres.wire.telemetry.PolyWireTelemetry;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap: starts the three frontend listeners (pgwire, mywire, orawire), each wired through a
 * shared pipeline onto a single Postgres backend (configured via the existing ORAPG_PG_* env
 * vars, defaults localhost:5432/postgres). Second pass — QoS admission control, cache-aside
 * result caching, and stats/OTel observability (deliberately skipped in the first pass, see
 * commit 7676603) are now wired in, in ARCHITECTURE.md §5's stage order: Router -&gt; Qos -&gt;
 * DialectTranslation -&gt; Cache -&gt; StatsCollector. Qos runs early (right after routing resolves
 * workloadClass/targetBackend, before any translation/execution work) so a rejected statement
 * does the least possible work. Cache runs after DialectTranslation (needs a resolved
 * targetBackend so cache keys never collide across backends, and around the execution step it
 * wraps so a hit can short-circuit it) and, per CacheStage's own javadoc, before
 * StatsCollectorStage. Live-verified consequence of that ordering, not originally anticipated:
 * because {@code CacheStage.handle} returns directly on a hit without calling {@code
 * next.proceed}, a cache hit never reaches StatsCollectorStage — {@code /metrics}' {@code
 * polywire_statements_total} undercounts served statements by exactly the cache-hit count
 * relative to {@code polywire_qos_admitted_total} (QosControlStage runs upstream of both Cache
 * and Stats, so it sees every admitted statement including hits). A real, documented narrow-slice
 * gap, not a bug to silently paper over — accepted because ARCHITECTURE.md's stage-order
 * contract for CacheStage is otherwise correct (cache key freedom from execution latency, hit
 * short-circuiting the real backend round-trip) and moving StatsCollector ahead of Cache would
 * cost the "hits are fast, misses are not" latency-histogram signal instead. Still not wired: the
 * gRPC/HTTPS admin frontends from Omnigate's ProxyServer.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.parse(args);

        BackendRegistry backendRegistry = BackendRegistry.fromConfig(null, null);

        PolyWireTelemetry telemetry = PolyWireTelemetry.fromEnv();
        if (telemetry != null) {
            log.info("OTel export enabled (POLYWIRE_OTEL_ENDPOINT); set POLYWIRE_OTEL_ENDPOINT=disabled to turn off");
        }

        // QoS: POLYWIRE_QOS_RATE_PER_SEC/_BURST/_MAX_WAIT_MS, deliberately low local-dev defaults
        // (5 req/s, burst 5) so admission rejection is trivial to trigger in live testing with a
        // tight psql loop; a sane production-shaped starting point is documented right here for
        // anyone deploying this for real: POLYWIRE_QOS_RATE_PER_SEC=200 POLYWIRE_QOS_BURST=400
        // (matches QosControlStage.fromConfig's own internal default of 200/400 when unset) is a
        // reasonable per-node starting point for a modest OLTP workload; tune from there.
        String qosRate = System.getenv().getOrDefault("POLYWIRE_QOS_RATE_PER_SEC", "5");
        String qosBurst = System.getenv().getOrDefault("POLYWIRE_QOS_BURST", "5");
        String qosMaxWait = System.getenv("POLYWIRE_QOS_MAX_WAIT_MS");
        String qosClassLimits = System.getenv("POLYWIRE_QOS_CLASS_LIMITS");
        String qosPoolWaitThreshold = System.getenv("POLYWIRE_QOS_POOL_WAIT_THRESHOLD");
        QosControlStage qosStage = QosControlStage.fromConfig(
                qosRate, qosBurst, qosMaxWait, qosClassLimits, qosPoolWaitThreshold, telemetry);
        log.info("QoS admission control: rate={}/s burst={} maxWaitMs={} (POLYWIRE_QOS_RATE_PER_SEC/_BURST/_MAX_WAIT_MS; "
                        + "production-shaped starting point: rate=200 burst=400)",
                qosRate, qosBurst, qosMaxWait == null ? "0" : qosMaxWait);

        // Cache: off by default (matches CacheStage/Omnigate's own posture) — only wired in when
        // POLYWIRE_CACHE_TABLES names at least one table. Backed by a single-node embedded Ignite
        // instance; POLYWIRE_CLUSTER_ENABLED=true would additionally join a real cluster, but a
        // single node is all CacheStage needs to function.
        PolyWireCluster cluster = PolyWireCluster.fromEnv();
        String cacheTables = System.getenv("POLYWIRE_CACHE_TABLES");
        String cacheTtlMs = System.getenv("POLYWIRE_CACHE_TTL_MS");
        PolyWireCluster cacheCluster = cluster.enabled() ? cluster
                : (cacheTables != null && !cacheTables.isBlank() ? startLocalCacheCluster() : cluster);
        CacheStage cacheStage = CacheStage.fromConfigOrNull(cacheCluster, cacheTables, cacheTtlMs);
        if (cacheStage != null) {
            log.info("result cache enabled: tables=[{}] ttlMs={}", cacheTables,
                    cacheTtlMs == null ? "30000" : cacheTtlMs);
        } else {
            log.info("result cache disabled (set POLYWIRE_CACHE_TABLES to enable)");
        }

        StatsCollectorStage statsStage = new StatsCollectorStage(telemetry);

        List<PipelineStage> stages = new ArrayList<>();
        stages.add(new RouterStage());
        stages.add(qosStage);
        stages.add(new DialectTranslationStage(backendRegistry));
        if (cacheStage != null) {
            stages.add(cacheStage);
        }
        stages.add(statsStage);
        List<PipelineStage> pipelineStages = List.copyOf(stages);

        PgBackendPool backendPool = new PgBackendPool(options);

        int metricsPort = parseIntEnv("POLYWIRE_METRICS_PORT", 19090);
        MetricsServer metricsServer = new MetricsServer(metricsPort, statsStage, qosStage);
        metricsServer.start();

        ExecutorService sessionExecutor = Executors.newCachedThreadPool();
        ExecutorService listenerExecutor = Executors.newCachedThreadPool();

        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor));
        acceptOraWireLoop(options, backendPool, pipelineStages, backendRegistry, sessionExecutor);
    }

    /**
     * A cache table was configured but full clustering wasn't opted into (POLYWIRE_CLUSTER_ENABLED
     * unset) — start a single-node embedded Ignite instance anyway, purely so CacheStage has
     * somewhere to put entries. Real multi-node clustering still requires POLYWIRE_CLUSTER_ENABLED.
     */
    private static PolyWireCluster startLocalCacheCluster() {
        return PolyWireCluster.startSingleNodeForCacheOnly();
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private static void acceptPgWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("polywire listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
                    options.pgWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new PgWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Postgres wire listener on port {} failed", options.pgWireListenPort(), e);
        }
    }

    private static void acceptMySqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.myWireListenPort())) {
            log.info("polywire listening for TCP (MySQL wire) on port {}, proxying to postgres {}:{}/{}",
                    options.myWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new MySqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("MySQL wire listener on port {} failed", options.myWireListenPort(), e);
        }
    }

    private static void acceptOraWireLoop(ServerOptions options, PgBackendPool backendPool,
            List<PipelineStage> pipelineStages, BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.listenPort())) {
            log.info("polywire listening for TCP (Oracle wire) on port {}, proxying to postgres {}:{}/{}",
                    options.listenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new SessionHandler(clientSocket, backendPool, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Oracle wire listener on port {} failed", options.listenPort(), e);
        }
    }

    private Main() {
    }
}
