package com.nexagres.wire.server;

import com.nexagres.wire.capture.WorkloadCaptureStage;
import com.nexagres.wire.cluster.CacheStage;
import com.nexagres.wire.cluster.PolyWireCluster;
import com.nexagres.wire.config.ConfigStore;
import com.nexagres.wire.config.PolyWireConfig;
import com.nexagres.wire.config.TranslationCacheStore;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.BackendTarget;
import com.nexagres.wire.core.DialectTranslationStage;
import com.nexagres.wire.core.FirewallStage;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.core.QosControlStage;
import com.nexagres.wire.core.RollupStage;
import com.nexagres.wire.core.RouterStage;
import com.nexagres.wire.core.SchemaFederationStage;
import com.nexagres.wire.core.StatsCollectorStage;
import com.nexagres.wire.dynamowire.DynamoWireServer;
import com.nexagres.wire.grpc.PolyWireGrpcServer;
import com.nexagres.wire.http.admin.MetricsServer;
import com.nexagres.wire.mongowire.MongoWireSessionHandler;
import com.nexagres.wire.mssqlwire.session.MssqlWireSessionHandler;
import com.nexagres.wire.mywire.MySqlWireSessionHandler;
import com.nexagres.wire.orawire.session.SessionHandler;
import com.nexagres.wire.pgwire.PgBackendPool;
import com.nexagres.wire.pgwire.PgWireSessionHandler;
import com.nexagres.wire.rollup.RollupConfig;
import com.nexagres.wire.rollup.RollupDefinition;
import com.nexagres.wire.rollup.RollupRefreshJob;
import com.nexagres.wire.rollup.RollupStore;
import com.nexagres.wire.telemetry.PolyWireTelemetry;
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
        com.nexagres.wire.config.NodeRegistry.ensureSchema(options);

        // License-tier instance cap -- checked here, before ANYTHING else starts (no config
        // bootstrap, no backend registry, no accept loop), since a Developer-tier install already
        // at its instance cap should refuse to come up at all, not partially start and then reject
        // connections. Counts every OTHER currently-live instance via the same polywire_nodes
        // table NodeRegistry.start() will begin heartbeating into further down -- this instance's
        // own row doesn't exist yet, so +1 for itself is exact, not an estimate.
        com.nexagres.wire.license.License license = com.nexagres.wire.license.License.current();
        if (license.tier() == com.nexagres.wire.license.LicenseTier.DEVELOPER) {
            int liveElsewhere = com.nexagres.wire.config.NodeRegistry.countLive(options);
            int max = license.maxInstances();
            if (liveElsewhere + 1 > max) {
                log.error("license: refusing to start -- Developer edition is capped at {} PolyWire "
                        + "instance(s), and {} are already live (this would be number {}). Stop one of "
                        + "the running instances, or set POLYWIRE_LICENSE_KEY to an Enterprise license, "
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

        BackendTarget defaultBackendTarget = new BackendTarget(BackendRegistry.DEFAULT_BACKEND_NAME,
                "jdbc:postgresql://" + options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase(),
                options.pgUser(), options.pgPassword(), options);
        BackendRegistry backendRegistry = BackendRegistry.fromConfig(
                config.backends(), config.shardBackends(), defaultBackendTarget);

        // Closes the gap flagged by a competitive comparison against ShardingSphere: a coordinator
        // crash between an XA transaction's commit decision and every branch actually applying it
        // used to leave that branch prepared (holding locks) at its backend forever. Run before any
        // client connection is accepted, not lazily on first use -- an in-doubt branch has already
        // been holding locks since before this restart, so there's no reason to wait.
        com.nexagres.wire.xa.XaRecoveryLog xaRecoveryLog = new com.nexagres.wire.xa.XaRecoveryLog(options);
        xaRecoveryLog.ensureSchema();
        com.nexagres.wire.xa.XaRecovery.recover(xaRecoveryLog, backendRegistry);

        // Unplanned-failure half of the switchover design (see BackendHealthChecker's javadoc);
        // POLYWIRE_BACKEND_HEALTH_CHECK_SECONDS=0 (or a negative value) opts out entirely --
        // there's no forced-on default for something that adds a background connection attempt
        // against every configured backend on a timer.
        long healthCheckSeconds = parseLongEnv("POLYWIRE_BACKEND_HEALTH_CHECK_SECONDS", 15);
        if (healthCheckSeconds > 0 && !backendRegistry.isEmpty()) {
            // "Acceptable data loss" for an unplanned failover -- see BackendHealthChecker's
            // javadoc on maxAcceptableFailoverLagSeconds. Unset (null) means no lag check at all,
            // same behavior as before this existed.
            String maxLagRaw = System.getenv("POLYWIRE_FAILOVER_MAX_LAG_SECONDS");
            Double maxLagSeconds = (maxLagRaw == null || maxLagRaw.isBlank()) ? null : Double.valueOf(maxLagRaw);
            new com.nexagres.wire.core.BackendHealthChecker(backendRegistry, healthCheckSeconds, maxLagSeconds).start();
        }

        PolyWireTelemetry telemetry = PolyWireTelemetry.fromEnv();
        if (telemetry != null) {
            log.info("OTel export enabled (POLYWIRE_OTEL_ENDPOINT); set POLYWIRE_OTEL_ENDPOINT=disabled to turn off");
        }

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

        PolyWireCluster cluster = PolyWireCluster.fromEnv();
        String cacheTables = config.cacheTables();
        String cacheTtlMs = config.cacheTtlMs();

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

        com.nexagres.wire.dynamowire.DynamoCache dynamoCache = dynamoCacheEnabled
                ? com.nexagres.wire.dynamowire.DynamoCache.create(cacheCluster, dynamoCacheTtlMs)
                : null;
        log.info("dynamowire GetItem cache: {} (POLYWIRE_DYNAMOWIRE_CACHE_ENABLED, default on; "
                        + "exact-key GetItem only, not Query/Scan) ttlMs={}",
                dynamoCacheEnabled ? "enabled" : "disabled", dynamoCacheTtlMs == null ? "30000" : dynamoCacheTtlMs);

        com.nexagres.wire.mongowire.MongoCache mongoCache = mongoCacheEnabled
                ? com.nexagres.wire.mongowire.MongoCache.create(cacheCluster, mongoCacheTtlMs)
                : null;
        log.info("mongowire find cache: {} (POLYWIRE_MONGOWIRE_CACHE_ENABLED, default on; "
                        + "exact-_id find only, not filtered find) ttlMs={}",
                mongoCacheEnabled ? "enabled" : "disabled", mongoCacheTtlMs == null ? "30000" : mongoCacheTtlMs);

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

        // One SqlMetricsCollector for the whole process -- shared with DynamoWireServer and the
        // mongowire session loop below (see StatsCollectorStage's javadoc for why those two feed
        // it directly at their own dispatch point instead of going through this stage's handle()
        // like the SQL wire protocols do) so the traffic dashboard and every metrics export path
        // reflect all six wire protocols, not just the four that share the pipeline.
        com.nexagres.wire.core.SqlMetricsCollector sqlMetrics = new com.nexagres.wire.core.SqlMetricsCollector();
        if (cacheStage != null) {
            cacheStage.setSqlMetrics(sqlMetrics);
        }
        StatsCollectorStage statsStage = new StatsCollectorStage(telemetry, sqlMetrics);
        if (telemetry != null) {
            telemetry.attachSqlMetrics(statsStage::sqlMetricsSnapshot);
        }

        List<PipelineStage> stages = new ArrayList<>();

        com.nexagres.wire.config.FirewallRuleStore firewallRuleStore = new com.nexagres.wire.config.FirewallRuleStore(options);
        firewallRuleStore.ensureSchema();
        List<FirewallStage.Rule> initialFirewallRules;
        try {
            initialFirewallRules = firewallRuleStore.readRules();
        } catch (Exception e) {
            log.warn("firewall: failed to read initial rules from polywire_firewall_rules, starting with zero "
                    + "rules (default ALLOW) until the next successful read: {}", e.getMessage());
            initialFirewallRules = List.of();
        }
        FirewallStage firewallStage = new FirewallStage(initialFirewallRules);
        firewallRuleStore.listen(firewallStage::reloadRules);
        log.info("firewall: {} rule(s) loaded from polywire_firewall_rules", initialFirewallRules.size());
        stages.add(firewallStage);

        boolean captureEnabled = "true".equalsIgnoreCase(
                System.getenv().getOrDefault("POLYWIRE_CAPTURE_ENABLED", "false"));
        int captureBufferSize = parseIntEnv("POLYWIRE_CAPTURE_BUFFER_SIZE", 20_000);
        com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer = captureEnabled
                ? new com.nexagres.wire.capture.WorkloadCaptureBuffer(java.util.UUID.randomUUID().toString(), captureBufferSize)
                : null;
        if (captureBuffer != null) {
            stages.add(new WorkloadCaptureStage(captureBuffer));
        }
        log.info("workload capture: {} (set POLYWIRE_CAPTURE_ENABLED=true to record every statement, in "
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
        // `cluster` (real multi-instance clustering only, POLYWIRE_CLUSTER_ENABLED=true) -- not
        // `cacheCluster`, which also activates for the default single-node cache-only Ignite grid:
        // a "unified view across instances" only has meaning with genuine cross-instance clustering.
        long federationStatsTtlMillis = com.nexagres.wire.core.StatisticsStore.ttlFromEnvOrDefaultPublic();
        com.nexagres.wire.core.StatisticsStore federationStatisticsStore =
                new com.nexagres.wire.core.StatisticsStore(cluster, federationStatsTtlMillis);
        com.nexagres.wire.core.SqlPlanStore federationPlanStore =
                com.nexagres.wire.core.SqlPlanStore.fromConfig(System.getenv("POLYWIRE_FEDERATION_PLAN_HISTORY"), cluster);
        routerStage.setFederationSupport(federationStatisticsStore, federationPlanStore);
        com.nexagres.wire.core.StatisticsScheduler statisticsScheduler = com.nexagres.wire.core.StatisticsScheduler
                .startIfConfigured(backendRegistry, routerStage.shardRules(), routerStage.schemaRules(), federationStatisticsStore);
        log.info("federation plan history: {} (set POLYWIRE_FEDERATION_PLAN_HISTORY=<capacity> to enable, "
                + "0/unset disables; statistics collection: {})",
                federationPlanStore == null ? "disabled" : "enabled",
                statisticsScheduler == null ? "on-demand only (set POLYWIRE_STATS_REFRESH_INTERVAL_MINUTES for a background refresh)"
                        : "background refresh enabled");

        // Before routerStage, not after -- see SchemaFederationStage's own javadoc: a statement
        // that references two different POLYWIRE_ROUTER_SCHEMA_RULES-routed backends has to be
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
        com.nexagres.wire.core.TranslationLlmClient initialLlmClient = com.nexagres.wire.core.TranslationLlmClient
                .fromConfig(config.llmProvider(), config.llmApiKey(), config.llmBaseUrl(), config.llmModel());
        DialectTranslationStage dialectTranslationStage = new DialectTranslationStage(
                backendRegistry, new com.nexagres.wire.core.TranslationCache(), initialLlmClient, translationCacheStore);
        log.info("dialect translation LLM fallback: provider={} ({})", config.llmProvider() == null ? "(env/default)" : config.llmProvider(),
                initialLlmClient == null ? "disabled" : "enabled");
        stages.add(dialectTranslationStage);
        stages.add(rollupStage);
        if (cacheStage != null) {
            stages.add(cacheStage);
        }
        stages.add(statsStage);
        List<PipelineStage> pipelineStages = List.copyOf(stages);

        PgBackendPool backendPool = new PgBackendPool(options);

        com.nexagres.wire.acl.ClientAcl clientAcl = com.nexagres.wire.acl.ClientAcl.parse(config.aclRules());
        com.nexagres.wire.acl.ConnectionGate connectionGate = com.nexagres.wire.acl.ConnectionGate.create(
                clientAcl, "true".equalsIgnoreCase(config.aclPpv2Enabled()),
                com.nexagres.wire.acl.ConnectionGate.parseTrustedProxies(config.aclTrustedProxies()));

        com.nexagres.wire.http.auth.AccessContextResolver oauth = com.nexagres.wire.http.auth.AccessContextResolver.create(
                config.oauthIssuer(), config.oauthAudience(), config.oauthUserIdClaim(), config.oauthRolesClaim());

        int metricsPort = parseIntEnv("POLYWIRE_METRICS_PORT", 19090);
        // POLYWIRE_ADMIN_WEB_DIR: path to the built wire/web SPA (its `dist/`). Opt-in, same
        // pattern as advisor's NEXAGRES_ADVISOR_WEB_DIR -- unset means API-only.
        String adminWebDir = System.getenv("POLYWIRE_ADMIN_WEB_DIR");
        // Constructed here (before both MetricsServer and the MCP server below, whichever order
        // they end up in) so both share the exact same instance -- MetricsServer reads it,
        // PolyWireMcpServer writes to it, from its single tools/call dispatch point.
        com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics = new com.nexagres.wire.mcp.McpMetricsCollector();

        // Constructed here (before MetricsServer, which reads it for GET /api/audit) so the same
        // instance is shared with every session handler that records into it below -- pgwire/
        // mssqlwire's DB_LOGIN_SUCCEEDED/DB_LOGIN_FAILED events, and (indirectly, via
        // AccessControlStage if that's ever wired in) row-filter/column-mask decisions. Without
        // POLYWIRE_AUDIT_LOG_FILE/POLYWIRE_AUDIT_LOG_DB configured, events still land in the
        // in-memory ring (readable via /api/audit) but aren't durable across a restart.
        com.nexagres.wire.audit.AuditLog auditLog = com.nexagres.wire.audit.AuditLog.fromEnv();

        MetricsServer metricsServer = new MetricsServer(metricsPort, statsStage, qosStage, currentConfigVersion::get,
                connectionGate, oauth, firewallRuleStore, configStore, backendRegistry, dialectTranslationStage,
                adminWebDir, options, mcpMetrics, captureBuffer, auditLog, xaRecoveryLog, federationPlanStore);
        metricsServer.start();

        // Deployment-topology visibility: a ~10s heartbeat row on the config-primary Postgres,
        // read back via GET /api/nodes -- see NodeRegistry's javadoc. "dev" is a placeholder;
        // there's no existing polywire release-version constant anywhere else in the codebase to
        // reuse (mongowire/MCP each stamp their own unrelated protocol-version strings).
        com.nexagres.wire.config.NodeRegistry nodeRegistry =
                new com.nexagres.wire.config.NodeRegistry(options, metricsPort, "dev");
        nodeRegistry.start();

        ExecutorService sessionExecutor = Executors.newCachedThreadPool();
        ExecutorService listenerExecutor = Executors.newCachedThreadPool();

        PolyWireGrpcServer grpcServer = new PolyWireGrpcServer(options, pipelineStages, backendRegistry, connectionGate);
        grpcServer.start();
        log.info("polywire listening for gRPC on port {}", options.grpcPort());

        if (options.tlsEnabled()) {
            
            SSLSocketFactory tlsSocketFactory = TlsSupport.buildTlsContext(options).getSocketFactory();
            log.info("TLS enabled (POLYWIRE_TLS_KEYSTORE={}): orawire TCPS on {}, pgwire+mywire negotiate TLS "
                            + "in-band on their existing plain ports ({}, {})",
                    options.tlsKeystorePath(), options.tlsPort(), options.pgWireListenPort(), options.myWireListenPort());

            listenerExecutor.submit(() -> acceptOraWireTlsLoop(options, tlsSocketFactory, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate, auditLog));

            grpcServer.startTls();
            log.info("polywire listening for gRPC TLS on port {}", options.grpcTlsPort());
        } else {
            log.info("TLS disabled (set POLYWIRE_TLS_KEYSTORE to enable orawire TCPS / pgwire+mywire in-band TLS / gRPC TLS)");
        }

        final com.nexagres.wire.auth.PgRoleAuthCache roleAuthCache =
                "postgres_roles".equals(System.getenv("POLYWIRE_AUTH_MODE"))
                        ? new com.nexagres.wire.auth.PgRoleAuthCache(options) : null;
        if (roleAuthCache != null) {
            log.info("auth: POLYWIRE_AUTH_MODE=postgres_roles -- pgwire/mssqlwire logins verified against "
                    + "real pg_authid role passwords (refreshed every {}s), not CredentialStore's shared secret",
                    parseIntEnv("POLYWIRE_AUTH_REFRESH_SECONDS", 30));
        }

        // auditLog (constructed earlier, shared with MetricsServer's /api/audit) is wired into
        // every login attempt on the two protocols that authenticate a real, distinguishable
        // Postgres role -- see PgWireSessionHandler/MssqlWireSessionHandler's
        // DB_LOGIN_SUCCEEDED/DB_LOGIN_FAILED events, and JdbcBackendExecutor's native-RLS
        // session-context propagation, which uses that same real identity.
        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate, auditLog));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, connectionGate));
        listenerExecutor.submit(() -> acceptMssqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate, auditLog));
        listenerExecutor.submit(() -> acceptMongoWireLoop(options, sessionExecutor, mongoCache, connectionGate, sqlMetrics, backendRegistry));
        int boltWirePort = parseIntEnv("POLYWIRE_BOLTWIRE_PORT", 7687);
        listenerExecutor.submit(() -> acceptBoltWireLoop(boltWirePort, backendRegistry, sessionExecutor, connectionGate));

        int dynamoWirePort = parseIntEnv("POLYWIRE_DYNAMOWIRE_PORT", 18000);

        com.nexagres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials =
                com.nexagres.wire.dynamowire.auth.AwsIamCredentialStore.create(config.awsIamCredentials());
        // Sharded when POLYWIRE_SHARD_BACKENDS is configured (the same shard group SQL's
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
            log.info("polywire listening for DynamoDB HTTP/JSON (dynamowire) on port {}", dynamoWirePort);
        } catch (Exception e) {
            log.error("dynamowire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring dynamowire back.",
                    dynamoWirePort, e);
        }

        int sqsWirePort = parseIntEnv("POLYWIRE_SQSWIRE_PORT", 9324);
        // Sharded by queue name across backendRegistry.shardGroup(), same mechanism as
        // dynamowire/mongowire -- see PgQueueStore's javadoc. Wrapped the same way dynamowire is:
        // a sqswire-only misconfiguration logs loudly and leaves sqswire off, without affecting
        // any other wire protocol this process serves.
        try {
            com.nexagres.wire.sqswire.SqsWireServer sqsWireServer = new com.nexagres.wire.sqswire.SqsWireServer(
                    sqsWirePort, backendRegistry, connectionGate, sqlMetrics);
            sqsWireServer.start();
            log.info("polywire listening for Amazon SQS HTTP/JSON (sqswire) on port {}", sqsWirePort);
        } catch (Exception e) {
            log.error("sqswire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring sqswire back.",
                    sqsWirePort, e);
        }

        int osWirePort = parseIntEnv("POLYWIRE_OSWIRE_PORT", 9200);
        // V1: OpenSearch-compatible _search/documents/_bulk backed by plain Postgres -- see
        // OpenSearchWireServer's javadoc for exactly what's covered and the internal Search IR
        // (SearchRequest) this is staged to let a future Qdrant adapter reuse. Wrapped the same
        // way dynamowire/sqswire are: an oswire-only misconfiguration logs loudly and leaves
        // oswire off, without affecting any other wire protocol this process serves.
        try {
            com.nexagres.wire.oswire.OpenSearchWireServer osWireServer = new com.nexagres.wire.oswire.OpenSearchWireServer(
                    osWirePort, backendRegistry, connectionGate, oauth, sqlMetrics);
            osWireServer.start();
            log.info("polywire listening for OpenSearch HTTP/JSON (oswire) on port {}", osWirePort);
        } catch (Exception e) {
            log.error("oswire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring oswire back.",
                    osWirePort, e);
        }

        int influxWirePort = parseIntEnv("POLYWIRE_INFLUXWIRE_PORT", 8086);
        // V1: InfluxDB line-protocol writes plus a narrow SHOW MEASUREMENTS / SELECT * FROM
        // <measurement> read path, backed by plain Postgres (or a real TimescaleDB hypertable when
        // detected on the backend -- see PgTimeSeriesStore's javadoc for that dual code path).
        // Wrapped the same way oswire/dynamowire/sqswire are: an influxwire-only misconfiguration
        // logs loudly and leaves influxwire off, without affecting any other wire protocol.
        try {
            com.nexagres.wire.influxwire.InfluxWireServer influxWireServer =
                    new com.nexagres.wire.influxwire.InfluxWireServer(
                            influxWirePort, backendRegistry, connectionGate, oauth, sqlMetrics);
            influxWireServer.start();
            log.info("polywire listening for InfluxDB HTTP/JSON (influxwire) on port {}", influxWirePort);
        } catch (Exception e) {
            log.error("influxwire failed to start on port {} -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring influxwire back.",
                    influxWirePort, e);
        }

        int mcpPort = parseIntEnv("POLYWIRE_MCP_PORT", 18010);
        com.nexagres.wire.mcp.PolyWireMcpServer mcpServer = new com.nexagres.wire.mcp.PolyWireMcpServer(
                mcpPort, options, pipelineStages, backendRegistry, connectionGate, System.getenv("POLYWIRE_MCP_TOOLS"),
                oauth, mcpMetrics);
        mcpServer.start();
        log.info("polywire listening for MCP (Model Context Protocol) on port {}", mcpPort);

        configStore.listen(newVersion -> {
            currentConfigVersion.set(newVersion);
            PolyWireConfig c = newVersion.payload();
            log.info("config: applying polywire_config version {} in place", newVersion.version());
            QosControlStage parsedQos = QosControlStage.fromConfig(c.qosRatePerSec(), c.qosBurst(),
                    c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(), telemetry);
            qosStage.reconfigure(parsedQos.defaultLimit(), parsedQos.classLimits(), parsedQos.poolWaitThreshold());
            routerStage.reconfigure(c.routerSchemaRules(), c.routerPredicateRules(),
                    c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards());
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
            
            clientAcl.reload(c.aclRules());
            connectionGate.reload("true".equalsIgnoreCase(c.aclPpv2Enabled()),
                    com.nexagres.wire.acl.ConnectionGate.parseTrustedProxies(c.aclTrustedProxies()));
            oauth.reload(c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim());
            awsIamCredentials.reload(c.awsIamCredentials());
            dialectTranslationStage.reconfigureLlm(c.llmProvider(), c.llmApiKey(), c.llmBaseUrl(), c.llmModel());
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

        acceptOraWireLoop(options, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate, auditLog);
    }

    private static PolyWireCluster startLocalCacheCluster() {
        return PolyWireCluster.startSingleNodeForCacheOnly();
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
    private static void submitSession(ExecutorService sessionExecutor, com.nexagres.wire.acl.ConnectionGate connectionGate,
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
            com.nexagres.wire.auth.PgRoleAuthCache roleAuthCache, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("polywire listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
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
    // PolyWire directly -- see com.nexagres.wire.boltwire.BoltWireSessionHandler's own javadoc for
    // exactly what's covered (Phase 1: handshake/HELLO/RUN/PULL/RECORD/SUCCESS/GOODBYE against a
    // narrow "RETURN <literal>" Cypher subset, proven against a real captured Neo4j session) and
    // what's still Phase 2+ (real MATCH/pattern-matching Cypher-to-SQL translation). Same
    // TCP-accept-loop shape as pgwire/mywire/mssqlwire/mongowire above, not the Jetty HTTP pattern
    // oswire/dynamowire/sqswire/influxwire use, since Bolt is a real binary protocol, not HTTP/JSON.
    private static void acceptBoltWireLoop(int port, BackendRegistry backendRegistry,
            ExecutorService sessionExecutor, com.nexagres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("polywire listening for Bolt (Neo4j wire) on port {}", port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                submitSession(sessionExecutor, connectionGate,
                        new com.nexagres.wire.boltwire.BoltWireSessionHandler(clientSocket, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Bolt (boltwire) listener on port {} failed -- every other wire protocol is still up. "
                    + "Fix the config (see the cause below) and restart to bring boltwire back.", port, e);
        }
    }

    private static void acceptMySqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.nexagres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.myWireListenPort())) {
            log.info("polywire listening for TCP (MySQL wire) on port {}, proxying to postgres {}:{}/{}",
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
            com.nexagres.wire.auth.PgRoleAuthCache roleAuthCache, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.mssqlWireListenPort())) {
            log.info("polywire listening for TCP (SQL Server TDS wire) on port {}, proxying to postgres {}:{}/{}",
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
            com.nexagres.wire.mongowire.MongoCache mongoCache, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.core.SqlMetricsCollector sqlMetrics, BackendRegistry backendRegistry) {
        int mongoPort = parseIntEnv("POLYWIRE_MONGOWIRE_PORT", 27017);
        try (ServerSocket serverSocket = new ServerSocket(mongoPort)) {
            log.info("polywire listening for TCP (MongoDB wire) on port {} "
                    + "(find/insert/update/delete only -- no aggregation pipeline, see MongoWireSessionHandler)",
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
            com.nexagres.wire.acl.ConnectionGate connectionGate, com.nexagres.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.listenPort())) {
            log.info("polywire listening for TCP (Oracle wire) on port {}, proxying to postgres {}:{}/{}",
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
            ExecutorService sessionExecutor, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.audit.AuditLog auditLog) {
        try (ServerSocket serverSocket = new ServerSocket(options.tlsPort())) {
            log.info("polywire listening for TCPS (Oracle wire over TLS) on port {}, proxying to postgres {}:{}/{}",
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
