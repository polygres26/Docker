package com.sayonora.wire.server;

import com.sayonora.wire.capture.WorkloadCaptureStage;
import com.sayonora.wire.cluster.CacheStage;
import com.sayonora.wire.cluster.WarpCluster;
import com.sayonora.wire.config.ConfigStore;
import com.sayonora.wire.config.WarpConfig;
import com.sayonora.wire.config.TranslationCacheStore;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.DialectTranslationStage;
import com.sayonora.wire.core.FirewallStage;
import com.sayonora.wire.core.PipelineStage;
import com.sayonora.wire.core.QosControlStage;
import com.sayonora.wire.core.RollupStage;
import com.sayonora.wire.core.RouterStage;
import com.sayonora.wire.core.SchemaFederationStage;
import com.sayonora.wire.core.StatsCollectorStage;
import com.sayonora.wire.dynamowire.DynamoWireServer;
import com.sayonora.wire.grpc.WarpGrpcServer;
import com.sayonora.wire.http.admin.MetricsServer;
import com.sayonora.wire.mongowire.MongoWireSessionHandler;
import com.sayonora.wire.mssqlwire.session.MssqlWireSessionHandler;
import com.sayonora.wire.mywire.MySqlWireSessionHandler;
import com.sayonora.wire.orawire.session.SessionHandler;
import com.sayonora.wire.pgwire.PgBackendPool;
import com.sayonora.wire.pgwire.PgWireSessionHandler;
import com.sayonora.wire.rollup.RollupConfig;
import com.sayonora.wire.rollup.RollupDefinition;
import com.sayonora.wire.rollup.RollupRefreshJob;
import com.sayonora.wire.rollup.RollupStore;
import com.sayonora.wire.telemetry.WarpTelemetry;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.parse(args);

        ConfigStore configStore = new ConfigStore(options);
        configStore.ensureSchema();
        com.sayonora.wire.config.NodeRegistry.ensureSchema(options);

        // License-tier instance cap -- checked here, before ANYTHING else starts (no config
        // bootstrap, no backend registry, no accept loop), since a Developer-tier install already
        // at its instance cap should refuse to come up at all, not partially start and then reject
        // connections. Counts every OTHER currently-live instance via the same warp_nodes
        // table NodeRegistry.start() will begin heartbeating into further down -- this instance's
        // own row doesn't exist yet, so +1 for itself is exact, not an estimate.
        com.sayonora.wire.license.License license = com.sayonora.wire.license.License.current();
        if (license.tier() == com.sayonora.wire.license.LicenseTier.DEVELOPER) {
            int liveElsewhere = com.sayonora.wire.config.NodeRegistry.countLive(options);
            int max = license.maxInstances();
            if (liveElsewhere + 1 > max) {
                log.error("license: refusing to start -- Developer edition is capped at {} Warp "
                        + "instance(s), and {} are already live (this would be number {}). Stop one of "
                        + "the running instances, or set WARP_LICENSE_KEY to an Enterprise license, "
                        + "which has no instance limit. See the Pricing section of the docs.",
                        max, liveElsewhere, liveElsewhere + 1);
                System.exit(1);
                return;
            }
            log.info("license: Developer edition -- {} of {} instance slot(s) now in use, capped at "
                    + "{} concurrent connections/instance and {} Postgres backend(s). Free forever; "
                    + "not licensed for commercial production use. See the Pricing section of the docs.",
                    liveElsewhere + 1, max, license.maxConnectionsPerInstance(), license.maxBackends());
        } else {
            log.info("license: Enterprise edition verified for '{}'{} -- no Developer-tier limits apply",
                    license.licensedTo(), license.expiresAt() == null ? " (perpetual)" : " (expires " + license.expiresAt() + ")");
        }
        ConfigStore.Version initialVersion = configStore.readLatest().orElse(null);
        if (initialVersion == null) {
            
            WarpConfig bootstrapDefault = WarpConfig.fromEnvDefaults();
            long version = configStore.write(bootstrapDefault);
            initialVersion = new ConfigStore.Version(version, bootstrapDefault, java.time.Instant.now());
            log.info("config: warp_config was empty -- published version {} from today's env-var defaults", version);
        } else {
            log.info("config: starting from warp_config version {} (created {})",
                    initialVersion.version(), initialVersion.createdAt());
        }
        java.util.concurrent.atomic.AtomicReference<ConfigStore.Version> currentConfigVersion =
                new java.util.concurrent.atomic.AtomicReference<>(initialVersion);
        WarpConfig config = initialVersion.payload();

        BackendTarget defaultBackendTarget = new BackendTarget(BackendRegistry.DEFAULT_BACKEND_NAME,
                "jdbc:postgresql://" + options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase(),
                options.pgUser(), options.pgPassword(), options);

        // Native-backend-mode targets: one named BackendTarget per protocol actually running in
        // native mode, so the shared pipeline (FirewallStage/QosControlStage/RouterStage/
        // CacheStage -- all dialect-agnostic already) can run against these statements instead of
        // the previous JdbcBackendExecutor-direct bypass. DialectTranslationStage already no-ops
        // when a Statement's sourceDialect equals its resolved BackendTarget's dialect() (sniffed
        // from the jdbc:mysql:/jdbc:sqlserver: URL prefix below), which is exactly the same-dialect
        // case each protocol's own native-mode session handler pins its statements to (see
        // MySqlWireSessionHandler/MssqlWireSessionHandler). orawire's own native mode isn't wired
        // in here yet -- it still bypasses SQL parsing entirely via NativeSessionRelay, a separate,
        // larger lift tracked on its own.
        // Deliberately the 4-arg BackendTarget constructor (no failoverOptions) -- passing
        // `options` as failoverOptions (as defaultBackendTarget above does) makes BackendTarget#
        // borrow() short-circuit straight to PgConnections.open(failoverOptions), ignoring this
        // target's own jdbcUrl/user/password entirely and reconnecting to POSTGRES regardless.
        // Confirmed live as a real bug while building this: with failoverOptions set, every native-
        // mode query silently ran against Postgres and failed with a Postgres "relation does not
        // exist" mapped through DialectErrorMessages into a SQL-Server-flavored "Invalid object
        // name" -- misleadingly looking like a real SQL Server response while never having reached
        // the real backend at all. These targets have no failover backend of their own; a plain
        // pooled connection via BackendConnectionPools (the same path MssqlBackendConnections/
        // MySqlBackendConnections already used) is exactly right.
        Map<String, BackendTarget> nativeBackendTargets = new LinkedHashMap<>();
        if (options.mywireNativeBackend()) {
            nativeBackendTargets.put(BackendRegistry.MYSQL_NATIVE_DEFAULT_NAME, new BackendTarget(
                    BackendRegistry.MYSQL_NATIVE_DEFAULT_NAME,
                    "jdbc:mysql://" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase(),
                    options.mysqlUser(), options.mysqlPassword()));
        }
        if (options.mssqlwireNativeBackend()) {
            nativeBackendTargets.put(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME, new BackendTarget(
                    BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                    "jdbc:sqlserver://" + options.mssqlHost() + ":" + options.mssqlPort()
                            + ";databaseName=" + options.mssqlDatabase() + ";encrypt=false;trustServerCertificate=true",
                    options.mssqlUser(), options.mssqlPassword()));
        }
        // Dual-port mode's OWN target, under a DIFFERENT reserved name than the block above --
        // see BackendRegistry#MYSQL_NATIVE_DUAL_PORT_NAME/MSSQL_NATIVE_DUAL_PORT_NAME's own
        // javadoc for why: registering it under the SAME name RouterStage's dialect-based fallback
        // already treats as "this protocol's own native default" would make that fallback
        // ambiguously hijack the TRANSLATED listener's OWN statements too (both listeners' sessions
        // build the identical SourceDialect-tagged Statement) -- a real bug, found live, that
        // silently misrouted an ordinary translated-mode CREATE TABLE straight to the real native
        // backend instead of Postgres. Registered independent of the global toggle above --
        // dual-port mode is opt-in via its own port var, unrelated to WARP_MYWIRE_BACKEND/
        // WARP_MSSQLWIRE_BACKEND.
        if (options.mywireNativeListenPort() != 0) {
            nativeBackendTargets.put(BackendRegistry.MYSQL_NATIVE_DUAL_PORT_NAME, new BackendTarget(
                    BackendRegistry.MYSQL_NATIVE_DUAL_PORT_NAME,
                    "jdbc:mysql://" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase(),
                    options.mysqlUser(), options.mysqlPassword()));
        }
        if (options.mssqlwireNativeListenPort() != 0) {
            nativeBackendTargets.put(BackendRegistry.MSSQL_NATIVE_DUAL_PORT_NAME, new BackendTarget(
                    BackendRegistry.MSSQL_NATIVE_DUAL_PORT_NAME,
                    "jdbc:sqlserver://" + options.mssqlHost() + ":" + options.mssqlPort()
                            + ";databaseName=" + options.mssqlDatabase() + ";encrypt=false;trustServerCertificate=true",
                    options.mssqlUser(), options.mssqlPassword()));
        }
        BackendRegistry backendRegistry = BackendRegistry.fromConfig(
                config.backends(), config.shardBackends(), config.backendSets(), config.backendGroups(),
                defaultBackendTarget, nativeBackendTargets);
        logSchemaDiscoveryConflicts(backendRegistry);

        // Closes the gap flagged by a competitive comparison against ShardingSphere: a coordinator
        // crash between an XA transaction's commit decision and every branch actually applying it
        // used to leave that branch prepared (holding locks) at its backend forever. Run before any
        // client connection is accepted, not lazily on first use -- an in-doubt branch has already
        // been holding locks since before this restart, so there's no reason to wait.
        com.sayonora.wire.xa.XaRecoveryLog xaRecoveryLog = new com.sayonora.wire.xa.XaRecoveryLog(options);
        xaRecoveryLog.ensureSchema();
        com.sayonora.wire.xa.XaRecovery.recover(xaRecoveryLog, backendRegistry);

        // Unplanned-failure half of the switchover design (see BackendHealthChecker's javadoc);
        // WARP_BACKEND_HEALTH_CHECK_SECONDS=0 (or a negative value) opts out entirely --
        // there's no forced-on default for something that adds a background connection attempt
        // against every configured backend on a timer.
        long healthCheckSeconds = parseLongEnv("WARP_BACKEND_HEALTH_CHECK_SECONDS", 15);
        if (healthCheckSeconds > 0 && !backendRegistry.isEmpty()) {
            // "Acceptable data loss" for an unplanned failover -- see BackendHealthChecker's
            // javadoc on maxAcceptableFailoverLagSeconds. Unset (null) means no lag check at all,
            // same behavior as before this existed.
            String maxLagRaw = System.getenv("WARP_FAILOVER_MAX_LAG_SECONDS");
            Double maxLagSeconds = (maxLagRaw == null || maxLagRaw.isBlank()) ? null : Double.valueOf(maxLagRaw);
            new com.sayonora.wire.core.BackendHealthChecker(backendRegistry, healthCheckSeconds, maxLagSeconds).start();
        }

        WarpTelemetry telemetry = WarpTelemetry.fromEnv();
        if (telemetry != null) {
            log.info("OTel export enabled (WARP_OTEL_ENDPOINT); set WARP_OTEL_ENDPOINT=disabled to turn off");
        }

        String qosRate = config.qosRatePerSec();
        String qosBurst = config.qosBurst();
        String qosMaxWait = config.qosMaxWaitMs();
        String qosClassLimits = config.qosClassLimits();
        String qosPoolWaitThreshold = config.qosPoolWaitThreshold();
        QosControlStage qosStage = QosControlStage.fromConfig(
                qosRate, qosBurst, qosMaxWait, qosClassLimits, qosPoolWaitThreshold, telemetry);
        log.info("QoS admission control: rate={}/s burst={} maxWaitMs={} (from warp_config version {}; "
                        + "production-shaped starting point: rate=200 burst=400)",
                qosRate, qosBurst, qosMaxWait == null ? "0" : qosMaxWait, initialVersion.version());

        WarpCluster cluster = WarpCluster.fromEnv();
        String cacheTables = config.cacheTables();
        String cacheTtlMs = config.cacheTtlMs();

        boolean dynamoCacheEnabled = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_DYNAMOWIRE_CACHE_ENABLED", "true"));
        String dynamoCacheTtlMs = System.getenv("WARP_DYNAMOWIRE_CACHE_TTL_MS");
        boolean mongoCacheEnabled = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_MONGOWIRE_CACHE_ENABLED", "true"));
        String mongoCacheTtlMs = System.getenv("WARP_MONGOWIRE_CACHE_TTL_MS");

        boolean needsLocalIgniteForKvCache = dynamoCacheEnabled || mongoCacheEnabled
                || (cacheTables != null && !cacheTables.isBlank());
        WarpCluster cacheCluster = cluster.enabled() ? cluster
                : (needsLocalIgniteForKvCache ? startLocalCacheCluster() : cluster);
        CacheStage cacheStage = CacheStage.fromConfigOrNull(cacheCluster, cacheTables, cacheTtlMs);
        if (cacheStage != null) {
            log.info("result cache enabled: tables=[{}] ttlMs={}", cacheTables,
                    cacheTtlMs == null ? "30000" : cacheTtlMs);
        } else {
            log.info("result cache disabled (set WARP_CACHE_TABLES to enable)");
        }

        // One shared, cross-protocol RowCache (see its own javadoc) for BOTH dynamowire's GetItem
        // and mongowire's exact-_id find -- previously two separate cache instances (DynamoCache/
        // MongoCache), each invisible to the other and to SQL. Now a single Ignite cache region:
        // dynamowire's PutItem/GetItem, mongowire's insertOne/find, and CacheStage's own SQL-side
        // row-cache fast path (wired below, once each protocol's own table-lookup exists) all
        // read/write the exact same entries for the same underlying row.
        //
        // Only one TTL now, not two: WARP_DYNAMOWIRE_CACHE_TTL_MS wins when set (falling back
        // to WARP_MONGOWIRE_CACHE_TTL_MS if only that one is), since they'd otherwise disagree
        // about the TTL of entries in the one cache region they share -- a real, disclosed
        // narrowing versus the old two-independent-TTLs behavior, not a silent one.
        com.sayonora.wire.cluster.RowCache rowCache = (dynamoCacheEnabled || mongoCacheEnabled)
                ? com.sayonora.wire.cluster.RowCache.create(cacheCluster,
                        dynamoCacheTtlMs != null ? dynamoCacheTtlMs : mongoCacheTtlMs)
                : null;
        log.info("dynamowire GetItem cache: {} (WARP_DYNAMOWIRE_CACHE_ENABLED, default on; "
                        + "exact-key GetItem only, not Query/Scan; shared with a matching SQL "
                        + "SELECT-by-primary-key via pgwire/mywire/mssqlwire/orawire, and with "
                        + "mongowire's own exact-_id find on the same underlying table) ttlMs={}",
                dynamoCacheEnabled ? "enabled" : "disabled", dynamoCacheTtlMs == null ? "30000" : dynamoCacheTtlMs);
        com.sayonora.wire.cluster.RowCache dynamoCache = dynamoCacheEnabled ? rowCache : null;

        log.info("mongowire find cache: {} (WARP_MONGOWIRE_CACHE_ENABLED, default on; "
                        + "exact-_id find only, not filtered find; shared with a matching SQL "
                        + "SELECT-by-id via pgwire/mywire/mssqlwire/orawire, and with dynamowire's "
                        + "own GetItem on the same underlying table) ttlMs={}",
                mongoCacheEnabled ? "enabled" : "disabled", mongoCacheTtlMs == null ? "30000" : mongoCacheTtlMs);
        com.sayonora.wire.cluster.RowCache mongoCache = mongoCacheEnabled ? rowCache : null;

        String rollupYaml = config.rollupDefinitionsYaml();
        if (rollupYaml == null || rollupYaml.isBlank()) {
            String rollupDefinitionsFile = System.getenv("WARP_ROLLUP_DEFINITIONS_FILE");
            if (rollupDefinitionsFile != null && !rollupDefinitionsFile.isBlank()) {
                rollupYaml = Files.readString(Path.of(rollupDefinitionsFile));
            }
        }
        List<RollupDefinition> initialRollupDefinitions = RollupConfig.parse(rollupYaml);
        RollupStore rollupStore = new RollupStore(initialRollupDefinitions);
        RollupRefreshJob rollupRefreshJob = new RollupRefreshJob(backendRegistry, rollupStore);
        
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
        log.info("rollup acceleration: {} definition(s) from warp_config version {}",
                initialRollupDefinitions.size(), initialVersion.version());

        // One SqlMetricsCollector for the whole process -- shared with DynamoWireServer and the
        // mongowire session loop below (see StatsCollectorStage's javadoc for why those two feed
        // it directly at their own dispatch point instead of going through this stage's handle()
        // like the SQL wire protocols do) so the traffic dashboard and every metrics export path
        // reflect all six wire protocols, not just the four that share the pipeline.
        com.sayonora.wire.core.SqlMetricsCollector sqlMetrics = new com.sayonora.wire.core.SqlMetricsCollector();
        if (cacheStage != null) {
            cacheStage.setSqlMetrics(sqlMetrics);
        }
        StatsCollectorStage statsStage = new StatsCollectorStage(telemetry, sqlMetrics);
        if (telemetry != null) {
            telemetry.attachSqlMetrics(statsStage::sqlMetricsSnapshot);
        }

        List<PipelineStage> stages = new ArrayList<>();

        com.sayonora.wire.config.FirewallRuleStore firewallRuleStore = new com.sayonora.wire.config.FirewallRuleStore(options);
        firewallRuleStore.ensureSchema();
        List<FirewallStage.Rule> initialFirewallRules;
        try {
            initialFirewallRules = firewallRuleStore.readRules();
        } catch (Exception e) {
            log.warn("firewall: failed to read initial rules from warp_firewall_rules, starting with zero "
                    + "rules (default ALLOW) until the next successful read: {}", e.getMessage());
            initialFirewallRules = List.of();
        }
        FirewallStage firewallStage = new FirewallStage(initialFirewallRules);
        firewallRuleStore.listen(firewallStage::reloadRules);
        log.info("firewall: {} rule(s) loaded from warp_firewall_rules", initialFirewallRules.size());
        stages.add(firewallStage);

        boolean captureEnabled = "true".equalsIgnoreCase(
                System.getenv().getOrDefault("WARP_CAPTURE_ENABLED", "false"));
        int captureBufferSize = parseIntEnv("WARP_CAPTURE_BUFFER_SIZE", 20_000);
        com.sayonora.wire.capture.WorkloadCaptureBuffer captureBuffer = captureEnabled
                ? new com.sayonora.wire.capture.WorkloadCaptureBuffer(java.util.UUID.randomUUID().toString(), captureBufferSize)
                : null;
        if (captureBuffer != null) {
            stages.add(new WorkloadCaptureStage(captureBuffer));
        }
        log.info("workload capture: {} (set WARP_CAPTURE_ENABLED=true to record every statement, in "
                + "arrival order, to an in-memory ring buffer of {} entries, readable via GET /api/capture "
                + "and merged across every live instance by WorkloadReplayer)",
                captureEnabled ? "enabled" : "disabled", captureBufferSize);

        RouterStage routerStage = RouterStage.fromConfig(
                config.routerSchemaRules(),
                config.routerPredicateRules(),
                config.routerValueShardRules(),
                config.routerShardTables(),
                config.routerTableShards(),
                backendRegistry);
        log.info("router: {} schema rule(s), {} predicate rule(s), {} value-shard rule(s) "
                        + "({} bind-param-indexed, {} literal-column), {} shard-table rule(s), "
                        + "{} declarative table-shard rule(s)",
                routerStage.schemaRules().size(), routerStage.predicateRules().size(),
                routerStage.valueShardRules().size() + routerStage.valueShardColumnRules().size(),
                routerStage.valueShardRules().size(), routerStage.valueShardColumnRules().size(),
                routerStage.shardRules().size(), routerStage.tableShardRules().size());
        // Real row-count statistics + plan history for BOTH federation stages (ShardJoinExecutor,
        // reached later via RoutingBackendExecutor, and schemaFederationStage right below) -- one
        // shared StatisticsStore/SqlPlanStore instance for the whole process, set onto routerStage
        // (always present) so RouterStage.statisticsStoreIn/planStoreIn can hand the same instances
        // to every protocol's own RoutingBackendExecutor. See StatisticsStore/SqlPlanStore/
        // StatisticsScheduler's own javadoc for why each is scoped the way it is.
        // `cluster` (real multi-instance clustering only, WARP_CLUSTER_ENABLED=true) -- not
        // `cacheCluster`, which also activates for the default single-node cache-only Ignite grid:
        // a "unified view across instances" only has meaning with genuine cross-instance clustering.
        long federationStatsTtlMillis = com.sayonora.wire.core.StatisticsStore.ttlFromEnvOrDefaultPublic();
        com.sayonora.wire.core.StatisticsStore federationStatisticsStore =
                new com.sayonora.wire.core.StatisticsStore(cluster, federationStatsTtlMillis);
        com.sayonora.wire.core.SqlPlanStore federationPlanStore =
                com.sayonora.wire.core.SqlPlanStore.fromConfig(System.getenv("WARP_FEDERATION_PLAN_HISTORY"), cluster);
        routerStage.setFederationSupport(federationStatisticsStore, federationPlanStore);
        com.sayonora.wire.core.StatisticsScheduler statisticsScheduler = com.sayonora.wire.core.StatisticsScheduler
                .startIfConfigured(backendRegistry, routerStage.shardRules(), routerStage.schemaRules(), federationStatisticsStore);
        log.info("federation plan history: {} (set WARP_FEDERATION_PLAN_HISTORY=<capacity> to enable, "
                + "0/unset disables; statistics collection: {})",
                federationPlanStore == null ? "disabled" : "enabled",
                statisticsScheduler == null ? "on-demand only (set WARP_STATS_REFRESH_INTERVAL_MINUTES for a background refresh)"
                        : "background refresh enabled");

        // Before routerStage, not after -- see SchemaFederationStage's own javadoc: a statement
        // that references two different WARP_ROUTER_SCHEMA_RULES-routed backends has to be
        // federated BEFORE RouterStage.resolveBackend ever narrows it down to just one of them.
        SchemaFederationStage schemaFederationStage = SchemaFederationStage.fromConfigOrNull(
                routerStage, backendRegistry, federationStatisticsStore, federationPlanStore);
        if (schemaFederationStage != null) {
            stages.add(schemaFederationStage);
            log.info("schema federation: enabled ({} schema rule(s) -- a query referencing 2+ of their backends "
                    + "in one statement is federated via Calcite instead of routed to a single one)",
                    routerStage.schemaRules().size());
        }
        stages.add(routerStage);
        stages.add(qosStage);
        TranslationCacheStore translationCacheStore = new TranslationCacheStore(options);
        translationCacheStore.ensureSchema();
        com.sayonora.wire.core.TranslationLlmClient initialLlmClient = com.sayonora.wire.core.TranslationLlmClient
                .fromConfig(config.llmProvider(), config.llmApiKey(), config.llmBaseUrl(), config.llmModel());
        DialectTranslationStage dialectTranslationStage = new DialectTranslationStage(
                backendRegistry, new com.sayonora.wire.core.TranslationCache(), initialLlmClient, translationCacheStore);
        log.info("dialect translation LLM fallback: provider={} ({})", config.llmProvider() == null ? "(env/default)" : config.llmProvider(),
                initialLlmClient == null ? "disabled" : "enabled");
        stages.add(dialectTranslationStage);
        stages.add(rollupStage);
        if (cacheStage != null) {
            stages.add(cacheStage);
        }
        stages.add(statsStage);
        // Last, deliberately -- see QueryRepairStage's own javadoc for why it needs to wrap only
        // the terminal executor call, nothing else in the pipeline. Shares the same LLM client/
        // config surface dialectTranslationStage does; off by default, unlike translation.
        boolean queryRepairEnabled = "true".equalsIgnoreCase(System.getenv(com.sayonora.wire.core.QueryRepairStage.ENABLED_ENV));
        com.sayonora.wire.core.QueryRepairStage queryRepairStage =
                new com.sayonora.wire.core.QueryRepairStage(queryRepairEnabled, initialLlmClient);
        log.info("query repair (LLM self-healing on real backend SQL errors): {} (set {}=true to enable; "
                        + "uses the same LLM provider config as dialect translation)",
                queryRepairEnabled ? "enabled" : "disabled", com.sayonora.wire.core.QueryRepairStage.ENABLED_ENV);
        stages.add(queryRepairStage);
        List<PipelineStage> pipelineStages = List.copyOf(stages);

        // Deterministic detection (a real per-protocol rate vs. its own EMA baseline), optional
        // LLM narration -- see AnomalyDetectionScheduler's own javadoc. Reads dialectTranslationStage's
        // llmClient() fresh every cycle (a Supplier, not a captured reference) so a later
        // PUT /api/llm-config change is picked up without restarting this scheduler too.
        com.sayonora.wire.core.AnomalyDetectionScheduler anomalyScheduler = com.sayonora.wire.core.AnomalyDetectionScheduler
                .startIfConfigured(statsStage, dialectTranslationStage::llmClient);
        log.info("anomaly detection: {} (set WARP_ANOMALY_SCAN_INTERVAL_MINUTES=<minutes> to enable; "
                + "LLM narration is optional on top of that and uses the same LLM provider config)",
                anomalyScheduler == null ? "disabled" : "enabled");

        PgBackendPool backendPool = new PgBackendPool(options);

        com.sayonora.wire.acl.ClientAcl clientAcl = com.sayonora.wire.acl.ClientAcl.parse(config.aclRules());
        com.sayonora.wire.acl.ConnectionGate connectionGate = com.sayonora.wire.acl.ConnectionGate.create(
                clientAcl, "true".equalsIgnoreCase(config.aclPpv2Enabled()),
                com.sayonora.wire.acl.ConnectionGate.parseTrustedProxies(config.aclTrustedProxies()));

        com.sayonora.wire.http.auth.AccessContextResolver oauth = com.sayonora.wire.http.auth.AccessContextResolver.create(
                config.oauthIssuer(), config.oauthAudience(), config.oauthUserIdClaim(), config.oauthRolesClaim());

        int metricsPort = parseIntEnv("WARP_METRICS_PORT", 19090);
        // WARP_ADMIN_WEB_DIR: path to the built wire/web SPA (its `dist/`). Opt-in, same
        // pattern as advisor's SAYONORA_DMS_WEB_DIR -- unset means API-only.
        String adminWebDir = System.getenv("WARP_ADMIN_WEB_DIR");
        // Constructed here (before both MetricsServer and the MCP server below, whichever order
        // they end up in) so both share the exact same instance -- MetricsServer reads it,
        // WarpMcpServer writes to it, from its single tools/call dispatch point.
        com.sayonora.wire.mcp.McpMetricsCollector mcpMetrics = new com.sayonora.wire.mcp.McpMetricsCollector();

        // Constructed here (before MetricsServer, which reads it for GET /api/audit) so the same
        // instance is shared with every session handler that records into it below -- pgwire/
        // mssqlwire's DB_LOGIN_SUCCEEDED/DB_LOGIN_FAILED events, and (indirectly, via
        // AccessControlStage if that's ever wired in) row-filter/column-mask decisions. Without
        // WARP_AUDIT_LOG_FILE/WARP_AUDIT_LOG_DB configured, events still land in the
        // in-memory ring (readable via /api/audit) but aren't durable across a restart.
        com.sayonora.wire.audit.AuditLog auditLog = com.sayonora.wire.audit.AuditLog.fromEnv();

        MetricsServer metricsServer = new MetricsServer(metricsPort, statsStage, qosStage, currentConfigVersion::get,
                connectionGate, oauth, firewallRuleStore, configStore, backendRegistry, dialectTranslationStage,
                adminWebDir, options, mcpMetrics, captureBuffer, auditLog, xaRecoveryLog, federationPlanStore);
        metricsServer.setQueryRepairStage(queryRepairStage);
        metricsServer.setAnomalyScheduler(anomalyScheduler);
        metricsServer.start();

        // Deployment-topology visibility: a ~10s heartbeat row on the config-primary Postgres,
        // read back via GET /api/nodes -- see NodeRegistry's javadoc. "dev" is a placeholder;
        // there's no existing warp release-version constant anywhere else in the codebase to
        // reuse (mongowire/MCP each stamp their own unrelated protocol-version strings).
        com.sayonora.wire.config.NodeRegistry nodeRegistry =
                new com.sayonora.wire.config.NodeRegistry(options, metricsPort, "dev");
        nodeRegistry.start();

        ExecutorService sessionExecutor = Executors.newCachedThreadPool();
        ExecutorService listenerExecutor = Executors.newCachedThreadPool();

        WarpGrpcServer grpcServer = new WarpGrpcServer(options, pipelineStages, backendRegistry, connectionGate);
        grpcServer.start();
        log.info("warp listening for gRPC on port {}", options.grpcPort());

        if (options.tlsEnabled()) {
            
            SSLSocketFactory tlsSocketFactory = TlsSupport.buildTlsContext(options).getSocketFactory();
            log.info("TLS enabled (WARP_TLS_KEYSTORE={}): orawire TCPS on {}, pgwire+mywire negotiate TLS "
                            + "in-band on their existing plain ports ({}, {})",
                    options.tlsKeystorePath(), options.tlsPort(), options.pgWireListenPort(), options.myWireListenPort());

            listenerExecutor.submit(() -> acceptOraWireTlsLoop(options, tlsSocketFactory, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate, auditLog));

            grpcServer.startTls();
            log.info("warp listening for gRPC TLS on port {}", options.grpcTlsPort());
        } else {
            log.info("TLS disabled (set WARP_TLS_KEYSTORE to enable orawire TCPS / pgwire+mywire in-band TLS / gRPC TLS)");
        }

        final com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache =
                "postgres_roles".equals(System.getenv("WARP_AUTH_MODE"))
                        ? new com.sayonora.wire.auth.PgRoleAuthCache(options) : null;
        if (roleAuthCache != null) {
            log.info("auth: WARP_AUTH_MODE=postgres_roles -- pgwire/mssqlwire logins verified against "
                    + "real pg_authid role passwords (refreshed every {}s), not CredentialStore's shared secret",
                    parseIntEnv("WARP_AUTH_REFRESH_SECONDS", 30));
        }

        // auditLog (constructed earlier, shared with MetricsServer's /api/audit) is wired into
        // every login attempt on the two protocols that authenticate a real, distinguishable
        // Postgres role -- see PgWireSessionHandler/MssqlWireSessionHandler's
        // DB_LOGIN_SUCCEEDED/DB_LOGIN_FAILED events, and JdbcBackendExecutor's native-RLS
        // session-context propagation, which uses that same real identity.
        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate, auditLog));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, connectionGate));
        listenerExecutor.submit(() -> acceptMssqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate, auditLog));
        // Second, independent listener per backend for running BOTH native-passthrough and
        // dialect-translated (JDBC) modes from the SAME Warp process at once -- disabled (0) by
        // default, so a deployment that never sets WARP_MYWIRE_NATIVE_PORT/
        // WARP_MSSQLWIRE_NATIVE_PORT/WARP_ORAWIRE_NATIVE_PORT (orawire's own native listener is
        // submitted further down, right before its primary loop's own blocking call) sees no
        // change at all -- see ServerOptions#withMywireNativeListener/withMssqlwireNativeListener/
        // withOracleNativeListener's own javadoc for exactly what the copied config flips.
        if (options.mywireNativeListenPort() != 0) {
            listenerExecutor.submit(() -> acceptMySqlWireLoop(options.withMywireNativeListener(), pipelineStages, backendRegistry, sessionExecutor, connectionGate));
        }
        if (options.mssqlwireNativeListenPort() != 0) {
            listenerExecutor.submit(() -> acceptMssqlWireLoop(options.withMssqlwireNativeListener(), pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate, auditLog));
        }
        listenerExecutor.submit(() -> acceptMongoWireLoop(options, sessionExecutor, mongoCache, connectionGate, sqlMetrics, backendRegistry));
        // Cross-protocol row-cache sharing, mongowire's half: unlike dynamowire's PgItemStore,
        // PostgresDocumentStore's physical-table registry is a static field (it's constructed
        // fresh per client session, so nothing else could be shared across sessions) -- so this
        // can be wired immediately, with no server-instance handle to wait on.
        if (cacheStage != null && mongoCache != null) {
            cacheStage.setRowCache(mongoCache);
            cacheStage.setMongoRowTableLookup(com.sayonora.wire.mongowire.MongoWireSessionHandler::isKnownMongoTable);
            log.info("cross-protocol row cache: SQL SELECT-by-id against a mongowire collection now shares its cache entry");
        }
        int boltWirePort = parseIntEnv("WARP_BOLTWIRE_PORT", 7687);
        listenerExecutor.submit(() -> acceptBoltWireLoop(boltWirePort, backendRegistry, sessionExecutor, connectionGate));

        int dynamoWirePort = parseIntEnv("WARP_DYNAMOWIRE_PORT", 18000);

        com.sayonora.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials =
                com.sayonora.wire.dynamowire.auth.AwsIamCredentialStore.create(config.awsIamCredentials());
        // Sharded when WARP_SHARD_BACKENDS is configured (the same shard group SQL's
        // value-shard rules already use) -- item storage splits across it by partition-key hash,
        // same as real DynamoDB's own partition model. Falls back to the single implicit
        // backend automatically when no shard group is configured -- see PgItemStore's javadoc.
        // Wrapped so a dynamowire-only misconfiguration (e.g. its catalog backend unreachable)
        // logs loudly and leaves dynamowire off instead of taking down every other wire protocol
        // this process serves -- an unhandled exception here used to kill the whole main thread.
        try {
            DynamoWireServer dynamoWireServer = new DynamoWireServer(dynamoWirePort, backendRegistry, dynamoCache,
                    connectionGate, oauth, awsIamCredentials, sqlMetrics);
            dynamoWireServer.start();
            log.info("warp listening for DynamoDB HTTP/JSON (dynamowire) on port {}", dynamoWirePort);
            // Cross-protocol row-cache sharing: CacheStage was built before dynamowire existed
            // (both need constructing before either can be wired to the other), so this closes
            // the loop -- a SELECT-by-primary-key against a dynamowire-backed table via
            // pgwire/mywire/mssqlwire/orawire can now hit the exact same RowCache entry GetItem
            // populated, and vice versa. See CacheStage#tryRowCacheLookup's own javadoc.
            if (cacheStage != null && dynamoCache != null) {
                cacheStage.setRowCache(dynamoCache);
                cacheStage.setDynamoRowTableLookup(physicalTable -> {
                    com.sayonora.wire.dynamowire.TableSchema schema = dynamoWireServer.store().lookupByPhysicalTable(physicalTable);
                    return schema == null ? null : schema.hasSortKey();
                });
                log.info("cross-protocol row cache: SQL SELECT-by-primary-key against a dynamowire table now shares its cache entry");
            }
        } catch (Exception e) {
            log.error("dynamowire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring dynamowire back.",
                    dynamoWirePort, e);
        }

        int sqsWirePort = parseIntEnv("WARP_SQSWIRE_PORT", 9324);
        // Sharded by queue name across backendRegistry.shardGroup(), same mechanism as
        // dynamowire/mongowire -- see PgQueueStore's javadoc. Wrapped the same way dynamowire is:
        // a sqswire-only misconfiguration logs loudly and leaves sqswire off, without affecting
        // any other wire protocol this process serves.
        try {
            com.sayonora.wire.sqswire.SqsWireServer sqsWireServer = new com.sayonora.wire.sqswire.SqsWireServer(
                    sqsWirePort, backendRegistry, connectionGate, sqlMetrics);
            sqsWireServer.start();
            log.info("warp listening for Amazon SQS HTTP/JSON (sqswire) on port {}", sqsWirePort);
        } catch (Exception e) {
            log.error("sqswire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring sqswire back.",
                    sqsWirePort, e);
        }

        int osWirePort = parseIntEnv("WARP_OSWIRE_PORT", 9200);
        // V1: OpenSearch-compatible _search/documents/_bulk backed by plain Postgres -- see
        // OpenSearchWireServer's javadoc for exactly what's covered and the internal Search IR
        // (SearchRequest) this is staged to let a future Qdrant adapter reuse. Wrapped the same
        // way dynamowire/sqswire are: an oswire-only misconfiguration logs loudly and leaves
        // oswire off, without affecting any other wire protocol this process serves.
        try {
            com.sayonora.wire.oswire.OpenSearchWireServer osWireServer = new com.sayonora.wire.oswire.OpenSearchWireServer(
                    osWirePort, backendRegistry, connectionGate, oauth, sqlMetrics);
            osWireServer.start();
            log.info("warp listening for OpenSearch HTTP/JSON (oswire) on port {}", osWirePort);
        } catch (Exception e) {
            log.error("oswire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring oswire back.",
                    osWirePort, e);
        }

        int influxWirePort = parseIntEnv("WARP_INFLUXWIRE_PORT", 8086);
        // V1: InfluxDB line-protocol writes plus a narrow SHOW MEASUREMENTS / SELECT * FROM
        // <measurement> read path, backed by plain Postgres (or a real TimescaleDB hypertable when
        // detected on the backend -- see PgTimeSeriesStore's javadoc for that dual code path).
        // Wrapped the same way oswire/dynamowire/sqswire are: an influxwire-only misconfiguration
        // logs loudly and leaves influxwire off, without affecting any other wire protocol.
        try {
            com.sayonora.wire.influxwire.InfluxWireServer influxWireServer =
                    new com.sayonora.wire.influxwire.InfluxWireServer(
                            influxWirePort, backendRegistry, connectionGate, oauth, sqlMetrics);
            influxWireServer.start();
            log.info("warp listening for InfluxDB HTTP/JSON (influxwire) on port {}", influxWirePort);
        } catch (Exception e) {
            log.error("influxwire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring influxwire back.",
                    influxWirePort, e);
        }

        int mcpPort = parseIntEnv("WARP_MCP_PORT", 18010);
        com.sayonora.wire.mcp.WarpMcpServer mcpServer = new com.sayonora.wire.mcp.WarpMcpServer(
                mcpPort, options, pipelineStages, backendRegistry, connectionGate, System.getenv("WARP_MCP_TOOLS"),
                oauth, mcpMetrics, auditLog, dialectTranslationStage::llmClient);
        mcpServer.start();
        log.info("warp listening for MCP (Model Context Protocol) on port {}", mcpPort);

        // A real A2A (Agent2Agent) frontend -- the gap found auditing Warp's own architecture
        // diagram against what was actually implemented (MCP was real, A2A was zero lines of
        // code). See A2AServer's own javadoc for scope: one JSON-RPC method (message/send,
        // synchronous), delegating to the SAME governed natural-language-to-SQL pipeline MCP's
        // own query_natural_language tool already uses -- not a separate, less-governed path.
        int a2aPort = parseIntEnv("WARP_A2A_PORT", 18020);
        String a2aPublicUrl = System.getenv().getOrDefault("WARP_A2A_PUBLIC_URL", "http://localhost:" + a2aPort + "/");
        com.sayonora.wire.a2a.A2AServer a2aServer = new com.sayonora.wire.a2a.A2AServer(
                a2aPort, a2aPublicUrl, mcpServer, connectionGate, oauth);
        a2aServer.start();
        log.info("warp listening for A2A (Agent2Agent Protocol) on port {}", a2aPort);

        configStore.listen(newVersion -> {
            currentConfigVersion.set(newVersion);
            WarpConfig c = newVersion.payload();
            log.info("config: applying warp_config version {} in place", newVersion.version());
            QosControlStage parsedQos = QosControlStage.fromConfig(c.qosRatePerSec(), c.qosBurst(),
                    c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(), telemetry);
            qosStage.reconfigure(parsedQos.defaultLimit(), parsedQos.classLimits(), parsedQos.poolWaitThreshold());
            // backendRegistry reloads BEFORE routerStage reconfigures: a table-shard rule's
            // "backends" field can name a WARP_BACKEND_SETS set, expanded using whatever
            // sets are live in the registry at the moment routerStage rebuilds its rules (see
            // RouterStage#expandBackendSets) -- reconfiguring first would expand against the
            // sets from BEFORE this same config version, one version stale.
            backendRegistry.reload(c.backends(), c.shardBackends(), c.backendSets(), c.backendGroups());
            routerStage.reconfigure(c.routerSchemaRules(), c.routerPredicateRules(),
                    c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards());
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
            
            clientAcl.reload(c.aclRules());
            connectionGate.reload("true".equalsIgnoreCase(c.aclPpv2Enabled()),
                    com.sayonora.wire.acl.ConnectionGate.parseTrustedProxies(c.aclTrustedProxies()));
            oauth.reload(c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim());
            awsIamCredentials.reload(c.awsIamCredentials());
            dialectTranslationStage.reconfigureLlm(c.llmProvider(), c.llmApiKey(), c.llmBaseUrl(), c.llmModel());
            queryRepairStage.reconfigureLlm(c.llmProvider(), c.llmApiKey(), c.llmBaseUrl(), c.llmModel());
            log.info("config: version {} applied (qos rate={}/s burst={}, {} router rule set(s), "
                            + "{} backend(s), cache={}, {} rollup definition(s), acl={} rule(s), "
                            + "oauth={}, awsIam={} credential(s))",
                    newVersion.version(), c.qosRatePerSec(), c.qosBurst(),
                    routerStage.schemaRules().size() + routerStage.predicateRules().size()
                            + routerStage.valueShardRules().size() + routerStage.valueShardColumnRules().size()
                            + routerStage.shardRules().size() + routerStage.tableShardRules().size(),
                    backendRegistry.all().size(), cacheStage != null, newRollups.size(),
                    clientAcl.hasRules() ? "some" : "0", c.oauthIssuer() == null ? "disabled" : "enabled",
                    awsIamCredentials.isEnabled() ? "some" : "0");
        });

        // Second, independent orawire listener for running native Oracle passthrough and
        // dialect-translated (JDBC) mode from the SAME Warp process at once -- see
        // ServerOptions#withOracleNativeListener's own javadoc. Submitted here, BEFORE the
        // primary loop's own blocking call just below, so it actually starts (that call never
        // returns).
        if (options.oracleNativeListenPort() != 0) {
            listenerExecutor.submit(() -> acceptOraWireLoop(options.withOracleNativeListener(), backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate, auditLog));
        }
        acceptOraWireLoop(options, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate, auditLog);
    }

    /**
     * Real proactive conflict detection, at startup -- the gap raised directly: a plain-group
     * table-name conflict should be "flagged as soon as discovery finds them," not only when some
     * later {@code query_federated} call happens to reference the colliding table. Runs the same
     * {@link com.sayonora.wire.core.BackendCatalogDiscovery} a live MCP call would, once, here.
     * Loud, not fatal -- a real, disclosed narrowing for a first implementation: a genuinely live,
     * periodic re-check (matching {@code StatisticsScheduler}'s own background-refresh pattern) is
     * a real follow-up, not built yet; this catches the conflict an operator can act on before any
     * real traffic hits it, which is the common case (a config mistake made once, not one that
     * appears fresh mid-flight from an unrelated schema migration).
     */
    private static void logSchemaDiscoveryConflicts(BackendRegistry backendRegistry) {
        var discovered = com.sayonora.wire.core.BackendCatalogDiscovery.discoverAll(backendRegistry);
        var byName = com.sayonora.wire.core.BackendCatalogDiscovery.byTableNameLowercase(discovered);
        int conflicts = 0;
        for (var entry : byName.entrySet()) {
            if (entry.getValue().size() < 2) {
                continue;
            }
            try {
                com.sayonora.wire.core.BackendCatalogDiscovery.resolveUnambiguous(entry.getValue(), backendRegistry);
            } catch (IllegalStateException conflict) {
                conflicts++;
                log.warn("schema auto-discovery: table \"{}\" is a real conflict -- {}. This table is "
                        + "unusable via query_federated's plain-table-name auto-discovery until resolved "
                        + "(rename one, or mark the right WARP_BACKEND_GROUPS group \":sharded\" if this "
                        + "collision is actually intentional).", entry.getKey(), conflict.getMessage());
            }
        }
        if (conflicts > 0) {
            log.warn("schema auto-discovery: {} table-name conflict(s) found across registered backends "
                    + "at startup -- see the warning(s) above for which table(s) and why", conflicts);
        }
    }

    private static WarpCluster startLocalCacheCluster() {
        return WarpCluster.startSingleNodeForCacheOnly();
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    /** Every TCP session handler ({@code PgWireSessionHandler}, {@code MySqlWireSessionHandler},
     * etc.) gets submitted through here rather than a bare {@code sessionExecutor.submit(handler)}
     * -- {@code connectionGate.acceptTcp} increments its live-connection count on accept (see its
     * javadoc), and this is the one place that guarantees the matching {@code release()} on every
     * exit path (normal return, an uncaught exception, doesn't matter), without needing to touch
     * any individual session handler's own {@code run()} method. */
    private static void submitSession(ExecutorService sessionExecutor, com.sayonora.wire.acl.ConnectionGate connectionGate,
            Runnable session) {
        sessionExecutor.submit(() -> {
            try {
                session.run();
            } finally {
                connectionGate.release();
            }
        });
    }

    private static void acceptPgWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache, com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("warp listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
                    options.pgWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate, new PgWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache, auditLog));
            }
        } catch (IOException e) {
            log.error("Postgres wire listener on port {} failed", options.pgWireListenPort(), e);
        }
    }

    // V1: real Bolt (binary TCP, PackStream-framed) so a genuine Neo4j client driver can point at
    // Warp directly -- see com.sayonora.wire.boltwire.BoltWireSessionHandler's own javadoc for
    // exactly what's covered (Phase 1: handshake/HELLO/RUN/PULL/RECORD/SUCCESS/GOODBYE against a
    // narrow "RETURN <literal>" Cypher subset, proven against a real captured Neo4j session) and
    // what's still Phase 2+ (real MATCH/pattern-matching Cypher-to-SQL translation). Same
    // TCP-accept-loop shape as pgwire/mywire/mssqlwire/mongowire above, not the Jetty HTTP pattern
    // oswire/dynamowire/sqswire/influxwire use, since Bolt is a real binary protocol, not HTTP/JSON.
    private static void acceptBoltWireLoop(int port, BackendRegistry backendRegistry,
            ExecutorService sessionExecutor, com.sayonora.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("warp listening for Bolt (Neo4j wire) on port {}", port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate,
                        new com.sayonora.wire.boltwire.BoltWireSessionHandler(clientSocket, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Bolt (boltwire) listener on port {} failed -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring boltwire back.", port, e);
        }
    }

    private static void acceptMySqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.sayonora.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.myWireListenPort())) {
            log.info("warp listening for TCP (MySQL wire) on port {}, proxying to postgres {}:{}/{}",
                    options.myWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate, new MySqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("MySQL wire listener on port {} failed", options.myWireListenPort(), e);
        }
    }

    private static void acceptMssqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache, com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.mssqlWireListenPort())) {
            log.info("warp listening for TCP (SQL Server TDS wire) on port {}, proxying to postgres {}:{}/{}",
                    options.mssqlWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate, new MssqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache, auditLog));
            }
        } catch (IOException e) {
            log.error("SQL Server TDS wire listener on port {} failed", options.mssqlWireListenPort(), e);
        }
    }

    private static void acceptMongoWireLoop(ServerOptions options, ExecutorService sessionExecutor,
            com.sayonora.wire.cluster.RowCache mongoCache, com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.core.SqlMetricsCollector sqlMetrics, BackendRegistry backendRegistry) {
        int mongoPort = parseIntEnv("WARP_MONGOWIRE_PORT", 27017);
        try (ServerSocket serverSocket = new ServerSocket(mongoPort)) {
            log.info("warp listening for TCP (MongoDB wire) on port {} "
                    + "(find/insert/update/delete, plus a real [$match][$group][$sort][$limit][$project] "
                    + "aggregate pipeline -- see MongoAggregationTranslator for its exact scope)",
                    mongoPort);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate, new MongoWireSessionHandler(clientSocket, backendRegistry, mongoCache, sqlMetrics));
            }
        } catch (IOException e) {
            log.error("MongoDB wire listener on port {} failed", mongoPort, e);
        }
    }

    private static void acceptOraWireLoop(ServerOptions options, PgBackendPool backendPool,
            List<PipelineStage> pipelineStages, BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.sayonora.wire.acl.ConnectionGate connectionGate, com.sayonora.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.listenPort())) {
            log.info("warp listening for TCP (Oracle wire) on port {}, proxying to postgres {}:{}/{}",
                    options.listenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate, new SessionHandler(clientSocket, backendPool, options, pipelineStages, backendRegistry, auditLog));
            }
        } catch (IOException e) {
            log.error("Oracle wire listener on port {} failed", options.listenPort(), e);
        }
    }

    private static void acceptOraWireTlsLoop(ServerOptions options, SSLSocketFactory tlsSocketFactory,
            PgBackendPool backendPool, List<PipelineStage> pipelineStages, BackendRegistry backendRegistry,
            ExecutorService sessionExecutor, com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.tlsPort())) {
            log.info("warp listening for TCPS (Oracle wire over TLS) on port {}, proxying to postgres {}:{}/{}",
                    options.tlsPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket plainSocket = serverSocket.accept();
                plainSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(plainSocket)) {
                    continue;
                }
                SSLSocket tlsSocket = (SSLSocket) tlsSocketFactory.createSocket(
                        plainSocket, null, plainSocket.getPort(), true);
                tlsSocket.setUseClientMode(false);
                submitSession(sessionExecutor, connectionGate, new SessionHandler(tlsSocket, backendPool, options, pipelineStages, backendRegistry, auditLog));
            }
        } catch (IOException e) {
            log.error("Oracle wire TCPS listener on port {} failed", options.tlsPort(), e);
        }
    }

    private Main() {
    }
}
