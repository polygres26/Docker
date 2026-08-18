package com.polygres.wire.server;

import com.polygres.wire.cluster.CacheStage;
import com.polygres.wire.cluster.PolyWireCluster;
import com.polygres.wire.config.ConfigStore;
import com.polygres.wire.config.PolyWireConfig;
import com.polygres.wire.config.TranslationCacheStore;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.DialectTranslationStage;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.RollupStage;
import com.polygres.wire.core.RouterStage;
import com.polygres.wire.core.StatsCollectorStage;
import com.polygres.wire.dynamowire.DynamoWireServer;
import com.polygres.wire.grpc.PolyWireGrpcServer;
import com.polygres.wire.http.admin.MetricsServer;
import com.polygres.wire.mongowire.MongoWireSessionHandler;
import com.polygres.wire.mssqlwire.session.MssqlWireSessionHandler;
import com.polygres.wire.mywire.MySqlWireSessionHandler;
import com.polygres.wire.orawire.session.SessionHandler;
import com.polygres.wire.pgwire.PgBackendPool;
import com.polygres.wire.pgwire.PgWireSessionHandler;
import com.polygres.wire.rollup.RollupConfig;
import com.polygres.wire.rollup.RollupDefinition;
import com.polygres.wire.rollup.RollupRefreshJob;
import com.polygres.wire.rollup.RollupStore;
import com.polygres.wire.telemetry.PolyWireTelemetry;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
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
 *
 * <p>Third pass — {@code RollupStage} (rollup/materialized-view query rewriting) is now wired in
 * too. Off by default, matching {@code CacheStage}'s own posture: only active when
 * {@code POLYWIRE_ROLLUP_DEFINITIONS_FILE} names a YAML file of rollup definitions (see {@code
 * RollupConfig}'s javadoc for the schema). Omnigate originally read this YAML from a DB-backed
 * {@code ConfigStore}; PolyWire has no equivalent config-store table wired up (and adding one just
 * for this single blob would be disproportionate), so a plain file path is used instead — simplest
 * thing that lets an operator define/redefine rollups without a rebuild. Every configured
 * definition is materialized once via {@code RollupRefreshJob.refreshNow} synchronously at startup
 * (not left to the first scheduled interval) so the rollup table exists and has data before the
 * first query that could match it arrives; {@code scheduleAll()} then takes over recurring
 * refreshes. <b>Pipeline position</b>: {@code RollupStage}'s own javadoc calls for running it after
 * {@code AccessControlStage}/{@code FirewallStage} (so any row-filter predicate a policy already
 * injected into {@code sqlText} is present by the time substitution is attempted) — neither of
 * those stages is wired into this pipeline at all yet, so the next-closest analogous slot is used:
 * right after {@code DialectTranslationStage}, which is also where {@code targetBackend} first
 * becomes resolved (the same reason {@code CacheStage} sits there) and, not incidentally, also the
 * earliest point at which the final backend-native SQL text a rollup's regex/Calcite matching needs
 * to see is actually available. Placed <em>before</em> {@code CacheStage}: a rollup-accelerated
 * result is itself eligible to be cache-cached (no reason not to layer the two), and if a plain
 * cache-key hit exists it's cheaper than even attempting Calcite substitution, but the two stages
 * are independent enough that either order is defensible — this order was picked so a rollup miss
 * still gets a chance at a cache hit on its now-rewritten... no: {@code CacheStage} keys off the
 * original {@code sqlText}, so what actually matters here is that {@code RollupStage} runs first so
 * its accelerated, much-cheaper query is what a cache entry (if any) would otherwise have avoided
 * recomputing anyway — either stage can independently short-circuit the rest of the pipeline.
 *
 * <p>Fourth pass — TLS. All four client-facing frontends (orawire, pgwire, mywire, gRPC) now
 * support TLS, backed by one shared PKCS12 keystore ({@code POLYWIRE_TLS_KEYSTORE}/
 * {@code POLYWIRE_TLS_KEYSTORE_PASSWORD}, ported from Omnigate's Oracle-only {@code ORAPG_TLS_*}
 * and renamed for consistency). orawire's TCPS listener is a straight port of Omnigate's
 * {@code ProxyServer} TLS handling: a second, TLS-only {@code ServerSocket} (built via
 * {@link TlsSupport#buildTlsFactory}) accepting the exact same {@link SessionHandler}, since TLS
 * is a full-socket wrap done before any protocol bytes — no session-handler code needs to know
 * about TLS at all. <b>pgwire and mywire are both different on purpose</b>: real Postgres wire
 * protocol v3 has its own in-band {@code SSLRequest} negotiation (every real client/driver,
 * including {@code psql sslmode=require}, speaks it on the <em>same</em> port a plaintext client
 * uses), and MySQL's protocol has an equivalent {@code CLIENT_SSL} capability-flag negotiation
 * (spoken by e.g. {@code pymysql}'s {@code ssl=} option). An earlier version of this change gave
 * both a separate always-TLS listener port instead (matching orawire's raw-full-socket-wrap
 * pattern) — live-testing with real {@code psql} and {@code pymysql} clients showed that doesn't
 * work for either: both protocols' real client libraries always perform their plaintext
 * negotiation handshake first, regardless of which port they're pointed at, so a
 * raw-TLS-from-byte-zero port is never what a real client actually sends. {@code
 * PgWireSessionHandler}/{@code MySqlWireSessionHandler} instead answer their protocol's own
 * negotiation message and upgrade the socket in place via {@code SSLSocketFactory} when TLS is
 * configured — see {@code PgWireSessionHandler#readStartupMessage}'s and {@code
 * MySqlWireSessionHandler#performHandshake}'s javadoc. There are deliberately no separate
 * {@code POLYWIRE_PGWIRE_TLS_PORT}/{@code POLYWIRE_MYWIRE_TLS_PORT} listeners in {@code Main} as
 * a result; both protocols' single plain-looking ports transparently serve plaintext and (when
 * {@code POLYWIRE_TLS_KEYSTORE} is set) TLS clients, exactly like real Postgres/MySQL servers.
 * gRPC ({@code PolyWireGrpcServer}) is
 * started here for the first time at all (it was never wired into {@code Main} before this pass)
 * on {@code POLYWIRE_GRPC_PORT} (default 7070, unchanged), plus a second TLS listener on
 * {@code POLYWIRE_GRPC_TLS_PORT} (default 17071) built from the same shared keystore via a
 * {@code KeyManagerFactory} handed straight to Netty's {@code SslContextBuilder} — no PEM
 * extraction or temp files needed, see {@link TlsSupport} and {@link PolyWireGrpcServer}.
 *
 * <p>Fifth pass — {@code mssqlwire} (SQL Server TDS) is now a fifth peer of pgwire/mywire/orawire
 * in every sense this class wires up, not just query round-tripping: it shares the exact same
 * {@code pipelineStages} list built above (Router → Qos → DialectTranslation → Cache → Stats), so
 * QoS admission control and result caching apply to TDS traffic with zero changes here — see
 * {@code MssqlWireSessionHandler}'s constructor. Its TLS story is the third shape in this codebase
 * (after orawire's separate-TCPS-port wrap and pgwire/mywire's raw-socket in-band upgrade):
 * MS-TDS's PRELOGIN {@code ENCRYPTION} option also negotiates in-band on the same plain-looking
 * port, but — found live against real {@code mssql-jdbc} — the TLS handshake bytes themselves are
 * additionally required to be wrapped in ordinary TDS packets while the handshake is in flight, a
 * wrinkle specific enough to TDS that it needed its own {@code SSLEngine}-driven channel ({@code
 * com.polygres.wire.mssqlwire.frontend.TdsTlsChannel}) rather than reusing pgwire/mywire's simpler
 * raw {@code SSLSocket} wrap. All three of TLS, T-SQL dialect translation (see {@link
 * com.polygres.wire.core.DialectTranslations#normalizeSqlServer}), and QoS/cache sharing are
 * live-verified end-to-end against real {@code mssql-jdbc}.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.parse(args);

        // com.polygres.wire.config.ConfigStore: the hot-reloadable config tier (QoS limits, cache
        // TTLs, backend/routing rules, rollup definitions) now lives in the polywire_config table
        // on the SAME bootstrap Postgres connection every frontend already uses (ORAPG_PG_*) --
        // not a second, differently-configured connection. That bootstrap connection itself stays
        // a plain env-var read (chicken-and-egg: a Postgres connection is needed before dynamic
        // config *from* Postgres can be read at all) -- see ConfigStore's and PolyWireConfig's
        // class javadoc for the full bootstrap-vs-dynamic split this implements.
        ConfigStore configStore = new ConfigStore(options.pgHost(), options.pgPort(), options.pgDatabase(),
                options.pgUser(), options.pgPassword());
        configStore.ensureSchema();
        ConfigStore.Version initialVersion = configStore.readLatest().orElse(null);
        if (initialVersion == null) {
            // First-run compatibility: no operator has to pre-populate polywire_config before ever
            // starting PolyWire for the first time -- fall back to today's env-var-derived config
            // and publish it as the cluster's first version so every node converges on the same
            // starting point (a second node racing this on its own first boot just produces a
            // higher-numbered version with equivalent content -- readLatest() picks the newest
            // either way, never a torn one, per ConfigStore's insert-only design).
            PolyWireConfig bootstrapDefault = PolyWireConfig.fromEnvDefaults();
            long version = configStore.write(bootstrapDefault);
            initialVersion = new ConfigStore.Version(version, bootstrapDefault, java.time.Instant.now());
            log.info("config: polywire_config was empty -- published version {} from today's env-var defaults", version);
        } else {
            log.info("config: starting from polywire_config version {} (created {})",
                    initialVersion.version(), initialVersion.createdAt());
        }
        java.util.concurrent.atomic.AtomicReference<ConfigStore.Version> currentConfigVersion =
                new java.util.concurrent.atomic.AtomicReference<>(initialVersion);
        PolyWireConfig config = initialVersion.payload();

        // POLYWIRE_BACKENDS/POLYWIRE_SHARD_BACKENDS: named backends RouterStage rules and
        // RollupStage definitions can target, beyond the one connection each frontend already
        // opens for itself via ORAPG_PG_*. Now sourced from polywire_config (hot-reloadable), not
        // a static env var -- BackendRegistry#reload keeps this same instance current in place.
        // Synthetic "default" entry built from the same ORAPG_PG_* values every frontend already
        // connects with directly -- gives RouterStage/DialectTranslationStage a real, registered
        // targetBackend to fall back to in single-backend deployments (no POLYWIRE_BACKENDS set),
        // instead of targetBackend staying null and DialectTranslationStage never firing for the
        // default path. See BackendRegistry#fromConfig(String, String, BackendTarget)'s javadoc.
        BackendTarget defaultBackendTarget = new BackendTarget(BackendRegistry.DEFAULT_BACKEND_NAME,
                "jdbc:postgresql://" + options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase(),
                options.pgUser(), options.pgPassword());
        BackendRegistry backendRegistry = BackendRegistry.fromConfig(
                config.backends(), config.shardBackends(), defaultBackendTarget);

        PolyWireTelemetry telemetry = PolyWireTelemetry.fromEnv();
        if (telemetry != null) {
            log.info("OTel export enabled (POLYWIRE_OTEL_ENDPOINT); set POLYWIRE_OTEL_ENDPOINT=disabled to turn off");
        }

        // QoS: rate/burst/maxWait/classLimits/poolWaitThreshold now come from polywire_config
        // (hot-reloadable -- see PolyWireConfig), not POLYWIRE_QOS_* env vars directly; those env
        // vars still supply the *bootstrap default* the very first time a cluster starts with an
        // empty polywire_config table (deliberately low local-dev defaults, 5 req/s burst 5, so
        // admission rejection is trivial to trigger in live testing with a tight psql loop). A
        // sane production-shaped starting point is documented right here for anyone deploying this
        // for real: rate=200 burst=400 (matches QosControlStage.fromConfig's own internal default
        // when unset) is a reasonable per-node starting point for a modest OLTP workload.
        String qosRate = config.qosRatePerSec();
        String qosBurst = config.qosBurst();
        String qosMaxWait = config.qosMaxWaitMs();
        String qosClassLimits = config.qosClassLimits();
        String qosPoolWaitThreshold = config.qosPoolWaitThreshold();
        QosControlStage qosStage = QosControlStage.fromConfig(
                qosRate, qosBurst, qosMaxWait, qosClassLimits, qosPoolWaitThreshold, telemetry);
        log.info("QoS admission control: rate={}/s burst={} maxWaitMs={} (from polywire_config version {}; "
                        + "production-shaped starting point: rate=200 burst=400)",
                qosRate, qosBurst, qosMaxWait == null ? "0" : qosMaxWait, initialVersion.version());

        // Cache: off by default (matches CacheStage/Omnigate's own posture) — only wired in when
        // polywire_config's cacheTables names at least one table. Backed by a single-node embedded
        // Ignite instance; POLYWIRE_CLUSTER_ENABLED=true would additionally join a real cluster,
        // but a single node is all CacheStage needs to function.
        PolyWireCluster cluster = PolyWireCluster.fromEnv();
        String cacheTables = config.cacheTables();
        String cacheTtlMs = config.cacheTtlMs();

        // dynamowire/mongowire exact-key caches: unlike CacheStage above, default ON — see
        // DynamoCache/MongoCache's class javadocs for why a key-value/exact-_id cache doesn't
        // carry CacheStage's arbitrary-SQL staleness risk. Opt out per-frontend with
        // POLYWIRE_DYNAMOWIRE_CACHE_ENABLED=false / POLYWIRE_MONGOWIRE_CACHE_ENABLED=false.
        boolean dynamoCacheEnabled = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "true"));
        String dynamoCacheTtlMs = System.getenv("POLYWIRE_DYNAMOWIRE_CACHE_TTL_MS");
        boolean mongoCacheEnabled = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "true"));
        String mongoCacheTtlMs = System.getenv("POLYWIRE_MONGOWIRE_CACHE_TTL_MS");

        boolean needsLocalIgniteForKvCache = dynamoCacheEnabled || mongoCacheEnabled
                || (cacheTables != null && !cacheTables.isBlank());
        PolyWireCluster cacheCluster = cluster.enabled() ? cluster
                : (needsLocalIgniteForKvCache ? startLocalCacheCluster() : cluster);
        CacheStage cacheStage = CacheStage.fromConfigOrNull(cacheCluster, cacheTables, cacheTtlMs);
        if (cacheStage != null) {
            log.info("result cache enabled: tables=[{}] ttlMs={}", cacheTables,
                    cacheTtlMs == null ? "30000" : cacheTtlMs);
        } else {
            log.info("result cache disabled (set POLYWIRE_CACHE_TABLES to enable)");
        }

        com.polygres.wire.dynamowire.DynamoCache dynamoCache = dynamoCacheEnabled
                ? com.polygres.wire.dynamowire.DynamoCache.create(cacheCluster, dynamoCacheTtlMs)
                : null;
        log.info("dynamowire GetItem cache: {} (POLYWIRE_DYNAMOWIRE_CACHE_ENABLED, default on; "
                        + "exact-key GetItem only, not Query/Scan) ttlMs={}",
                dynamoCacheEnabled ? "enabled" : "disabled", dynamoCacheTtlMs == null ? "30000" : dynamoCacheTtlMs);

        com.polygres.wire.mongowire.MongoCache mongoCache = mongoCacheEnabled
                ? com.polygres.wire.mongowire.MongoCache.create(cacheCluster, mongoCacheTtlMs)
                : null;
        log.info("mongowire find cache: {} (POLYWIRE_MONGOWIRE_CACHE_ENABLED, default on; "
                        + "exact-_id find only, not filtered find) ttlMs={}",
                mongoCacheEnabled ? "enabled" : "disabled", mongoCacheTtlMs == null ? "30000" : mongoCacheTtlMs);

        // Rollup: definitions now come from polywire_config's rollupDefinitionsYaml (hot-
        // reloadable), falling back to the legacy POLYWIRE_ROLLUP_DEFINITIONS_FILE env var (a
        // plain YAML file) only if the config version carries no rollup YAML of its own -- keeps
        // existing file-based deployments working unchanged. RollupStore/RollupRefreshJob/
        // RollupStage are now constructed unconditionally (even with zero definitions) rather than
        // only when something was configured, so a config update that adds the *first* rollup
        // definition later doesn't need a restart: the pipeline already contains a RollupStage
        // wired to a store that can be RollupStore#reload'ed in place.
        String rollupYaml = config.rollupDefinitionsYaml();
        if (rollupYaml == null || rollupYaml.isBlank()) {
            String rollupDefinitionsFile = System.getenv("POLYWIRE_ROLLUP_DEFINITIONS_FILE");
            if (rollupDefinitionsFile != null && !rollupDefinitionsFile.isBlank()) {
                rollupYaml = Files.readString(Path.of(rollupDefinitionsFile));
            }
        }
        List<RollupDefinition> initialRollupDefinitions = RollupConfig.parse(rollupYaml);
        RollupStore rollupStore = new RollupStore(initialRollupDefinitions);
        RollupRefreshJob rollupRefreshJob = new RollupRefreshJob(backendRegistry, rollupStore);
        // Materialize every rollup synchronously now -- a query run immediately after startup must
        // not depend on the first scheduled refresh interval having fired yet.
        for (RollupDefinition def : initialRollupDefinitions) {
            try {
                rollupRefreshJob.refreshNow(def);
                log.info("rollup: \"{}\" materialized at startup (table {})", def.name(), def.rollupTableName());
            } catch (Exception e) {
                log.warn("rollup: startup materialization failed for \"{}\", it will stay stale until its "
                        + "next scheduled refresh ({})", def.name(), e.toString());
            }
        }
        rollupRefreshJob.scheduleAll();
        RollupStage rollupStage = new RollupStage(rollupStore, backendRegistry);
        log.info("rollup acceleration: {} definition(s) from polywire_config version {}",
                initialRollupDefinitions.size(), initialVersion.version());

        StatsCollectorStage statsStage = new StatsCollectorStage(telemetry);

        // RouterStage rules: schemaRules ("schema1:backend1,schema2:backend2"), predicateRules
        // ("bindIndex:value:backend,..."), valueShardRules ("bindIndex:type:params|...", type =
        // hash/consistent/list/range via ShardingStrategy -- see RouterStage.fromConfig's javadoc
        // for the full grammar), and shardTables ("schema1,schema2" -- scatter-gathers across
        // shardBackends when no schema/predicate/value-shard rule already routed the query). All
        // sourced from polywire_config (hot-reloadable) now, all optional; unset means the
        // previous zero-rule behavior (see RouterStage's javadoc).
        List<PipelineStage> stages = new ArrayList<>();
        RouterStage routerStage = RouterStage.fromConfig(
                config.routerSchemaRules(),
                config.routerPredicateRules(),
                config.routerValueShardRules(),
                config.routerShardTables(),
                backendRegistry);
        log.info("router: {} schema rule(s), {} predicate rule(s), {} value-shard rule(s), {} shard-table rule(s)",
                routerStage.schemaRules().size(), routerStage.predicateRules().size(),
                routerStage.valueShardRules().size(), routerStage.shardRules().size());
        stages.add(routerStage);
        stages.add(qosStage);
        TranslationCacheStore translationCacheStore = new TranslationCacheStore(options.pgHost(), options.pgPort(),
                options.pgDatabase(), options.pgUser(), options.pgPassword());
        translationCacheStore.ensureSchema();
        stages.add(new DialectTranslationStage(backendRegistry, translationCacheStore));
        stages.add(rollupStage);
        if (cacheStage != null) {
            stages.add(cacheStage);
        }
        stages.add(statsStage);
        List<PipelineStage> pipelineStages = List.copyOf(stages);

        // Hot-reload: subscribe to polywire_config's LISTEN/NOTIFY channel. On every new version,
        // re-apply it to every already-constructed stage in place (see each stage's own
        // #reconfigure javadoc for why that's safe without restarting or interrupting in-flight
        // sessions) rather than rebuilding pipelineStages -- the List<PipelineStage> reference
        // itself never changes after this point, only what each stage inside it does on its next
        // handle() call. cacheStage staying null when no table was ever configured at startup, and
        // BackendRegistry-driven target additions for schemas RouterStage didn't know about yet,
        // are the two narrow-slice limitations of this pass -- see the module README/commit
        // message for the honest list of what's deferred.
        configStore.listen(newVersion -> {
            currentConfigVersion.set(newVersion);
            PolyWireConfig c = newVersion.payload();
            log.info("config: applying polywire_config version {} in place", newVersion.version());
            QosControlStage parsedQos = QosControlStage.fromConfig(c.qosRatePerSec(), c.qosBurst(),
                    c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(), telemetry);
            qosStage.reconfigure(parsedQos.defaultLimit(), parsedQos.classLimits(), parsedQos.poolWaitThreshold());
            routerStage.reconfigure(c.routerSchemaRules(), c.routerPredicateRules(),
                    c.routerValueShardRules(), c.routerShardTables());
            backendRegistry.reload(c.backends(), c.shardBackends());
            if (cacheStage != null) {
                cacheStage.reconfigure(c.cacheTables(), c.cacheTtlMs());
            }
            List<RollupDefinition> newRollups = RollupConfig.parse(c.rollupDefinitionsYaml());
            rollupStore.reload(newRollups);
            for (RollupDefinition def : newRollups) {
                try {
                    rollupRefreshJob.refreshNow(def);
                } catch (Exception e) {
                    log.warn("rollup: reload materialization failed for \"{}\" ({})", def.name(), e.toString());
                }
            }
            rollupRefreshJob.scheduleAll();
            log.info("config: version {} applied (qos rate={}/s burst={}, {} router rule set(s), "
                            + "{} backend(s), cache={}, {} rollup definition(s))",
                    newVersion.version(), c.qosRatePerSec(), c.qosBurst(),
                    routerStage.schemaRules().size() + routerStage.predicateRules().size()
                            + routerStage.valueShardRules().size() + routerStage.shardRules().size(),
                    backendRegistry.all().size(), cacheStage != null, newRollups.size());
        });

        PgBackendPool backendPool = new PgBackendPool(options);

        int metricsPort = parseIntEnv("POLYWIRE_METRICS_PORT", 19090);
        MetricsServer metricsServer = new MetricsServer(metricsPort, statsStage, qosStage, currentConfigVersion::get);
        metricsServer.start();

        ExecutorService sessionExecutor = Executors.newCachedThreadPool();
        ExecutorService listenerExecutor = Executors.newCachedThreadPool();

        // gRPC: not wired into Main at all before this pass (see class javadoc). Started
        // unconditionally on POLYWIRE_GRPC_PORT (plaintext); a second TLS listener on
        // POLYWIRE_GRPC_TLS_PORT is added only when a keystore is configured.
        PolyWireGrpcServer grpcServer = new PolyWireGrpcServer(options, pipelineStages, backendRegistry);
        grpcServer.start();
        log.info("polywire listening for gRPC on port {}", options.grpcPort());

        SSLServerSocketFactory tlsFactory = null;
        if (options.tlsEnabled()) {
            tlsFactory = TlsSupport.buildTlsFactory(options);
            log.info("TLS enabled (POLYWIRE_TLS_KEYSTORE={}): orawire TCPS on {}, pgwire+mywire negotiate TLS "
                            + "in-band on their existing plain ports ({}, {})",
                    options.tlsKeystorePath(), options.tlsPort(), options.pgWireListenPort(), options.myWireListenPort());

            final SSLServerSocketFactory finalTlsFactory = tlsFactory;
            listenerExecutor.submit(() -> acceptOraWireTlsLoop(options, finalTlsFactory, backendPool, pipelineStages, backendRegistry, sessionExecutor));

            grpcServer.startTls();
            log.info("polywire listening for gRPC TLS on port {}", options.grpcTlsPort());
        } else {
            log.info("TLS disabled (set POLYWIRE_TLS_KEYSTORE to enable orawire TCPS / pgwire+mywire in-band TLS / gRPC TLS)");
        }

        // POLYWIRE_AUTH_MODE=postgres_roles (default: shared_secret, CredentialStore's single dev
        // credential, unchanged) -- see PgRoleAuthCache's javadoc for why only pgwire/mssqlwire
        // can use it (both collect the client's password as cleartext already) and not
        // orawire/mywire (real challenge-response protocols a Postgres password hash can't drive).
        final com.polygres.wire.auth.PgRoleAuthCache roleAuthCache =
                "postgres_roles".equals(System.getenv("POLYWIRE_AUTH_MODE"))
                        ? new com.polygres.wire.auth.PgRoleAuthCache(options) : null;
        if (roleAuthCache != null) {
            log.info("auth: POLYWIRE_AUTH_MODE=postgres_roles -- pgwire/mssqlwire logins verified against "
                    + "real pg_authid role passwords (refreshed every {}s), not CredentialStore's shared secret",
                    parseIntEnv("POLYWIRE_AUTH_REFRESH_SECONDS", 30));
        }
        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor));
        listenerExecutor.submit(() -> acceptMssqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache));
        listenerExecutor.submit(() -> acceptMongoWireLoop(options, sessionExecutor, mongoCache));

        // dynamowire: DynamoDB's real client protocol is HTTP/JSON (SigV4-signed POST requests
        // naming the operation via X-Amz-Target), not a raw TCP wire protocol, so it doesn't fit
        // the acceptXxxLoop(ServerSocket) pattern the other frontends use -- it's an embedded
        // Jetty HTTP server instead. See DynamoWireServer's class javadoc for the execution-path
        // and auth-scope decisions. Own port, independent of pipelineStages (see that javadoc for
        // why it bypasses the SQL pipeline entirely).
        int dynamoWirePort = parseIntEnv("POLYWIRE_DYNAMOWIRE_PORT", 18000);
        DynamoWireServer dynamoWireServer = new DynamoWireServer(dynamoWirePort, options.pgHost(), options.pgPort(),
                options.pgDatabase(), options.pgUser(), options.pgPassword(), dynamoCache);
        dynamoWireServer.start();
        log.info("polywire listening for DynamoDB HTTP/JSON (dynamowire) on port {}", dynamoWirePort);

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
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("polywire listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
                    options.pgWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new PgWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache));
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

    private static void acceptMssqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache) {
        try (ServerSocket serverSocket = new ServerSocket(options.mssqlWireListenPort())) {
            log.info("polywire listening for TCP (SQL Server TDS wire) on port {}, proxying to postgres {}:{}/{}",
                    options.mssqlWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new MssqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache));
            }
        } catch (IOException e) {
            log.error("SQL Server TDS wire listener on port {} failed", options.mssqlWireListenPort(), e);
        }
    }

    /**
     * mongowire: its own listener port (POLYWIRE_MONGOWIRE_PORT, default 27017 — MongoDB's real
     * conventional default, unclaimed by any other frontend here), and deliberately its own
     * un-pipelined path straight to a plain JDBC Postgres connection (see
     * MongoWireSessionHandler's class javadoc for why) rather than sharing pipelineStages/
     * backendRegistry the way the four SQL-text frontends above do.
     */
    private static void acceptMongoWireLoop(ServerOptions options, ExecutorService sessionExecutor,
            com.polygres.wire.mongowire.MongoCache mongoCache) {
        int mongoPort = parseIntEnv("POLYWIRE_MONGOWIRE_PORT", 27017);
        String pgUrl = "jdbc:postgresql://" + options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase();
        try (ServerSocket serverSocket = new ServerSocket(mongoPort)) {
            log.info("polywire listening for TCP (MongoDB wire) on port {}, proxying to postgres {} "
                    + "(find/insert/update/delete only -- no aggregation pipeline, see MongoWireSessionHandler)",
                    mongoPort, pgUrl);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new MongoWireSessionHandler(clientSocket, pgUrl, options.pgUser(), options.pgPassword(), mongoCache));
            }
        } catch (IOException e) {
            log.error("MongoDB wire listener on port {} failed", mongoPort, e);
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

    /**
     * TCPS: same session handler as {@link #acceptOraWireLoop}, just fed by an
     * {@code SSLServerSocket} instead of a plain one — TLS is a full-socket wrap done before any
     * TNS bytes are exchanged (ported from Omnigate's {@code ProxyServer}), so no protocol-
     * specific TLS code is needed here or in {@link SessionHandler} itself.
     */
    private static void acceptOraWireTlsLoop(ServerOptions options, SSLServerSocketFactory tlsFactory,
            PgBackendPool backendPool, List<PipelineStage> pipelineStages, BackendRegistry backendRegistry,
            ExecutorService sessionExecutor) {
        try (SSLServerSocket serverSocket = (SSLServerSocket) tlsFactory.createServerSocket(options.tlsPort())) {
            log.info("polywire listening for TCPS (Oracle wire over TLS) on port {}, proxying to postgres {}:{}/{}",
                    options.tlsPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new SessionHandler(clientSocket, backendPool, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Oracle wire TCPS listener on port {} failed", options.tlsPort(), e);
        }
    }

    private Main() {
    }
}
