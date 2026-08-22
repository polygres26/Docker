package com.polygres.wire.server;

import com.polygres.wire.cluster.CacheStage;
import com.polygres.wire.cluster.PolyWireCluster;
import com.polygres.wire.config.ConfigStore;
import com.polygres.wire.config.PolyWireConfig;
import com.polygres.wire.config.TranslationCacheStore;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.DialectTranslationStage;
import com.polygres.wire.core.FirewallStage;
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
        com.polygres.wire.core.SqlMetricsCollector sqlMetrics = new com.polygres.wire.core.SqlMetricsCollector();
        StatsCollectorStage statsStage = new StatsCollectorStage(telemetry, sqlMetrics);
        if (telemetry != null) {
            telemetry.attachSqlMetrics(statsStage::sqlMetricsSnapshot);
        }

        List<PipelineStage> stages = new ArrayList<>();

        com.polygres.wire.config.FirewallRuleStore firewallRuleStore = new com.polygres.wire.config.FirewallRuleStore(options);
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
        TranslationCacheStore translationCacheStore = new TranslationCacheStore(options);
        translationCacheStore.ensureSchema();
        stages.add(new DialectTranslationStage(backendRegistry, translationCacheStore));
        stages.add(rollupStage);
        if (cacheStage != null) {
            stages.add(cacheStage);
        }
        stages.add(statsStage);
        List<PipelineStage> pipelineStages = List.copyOf(stages);

        PgBackendPool backendPool = new PgBackendPool(options);

        com.polygres.wire.acl.ClientAcl clientAcl = com.polygres.wire.acl.ClientAcl.parse(config.aclRules());
        com.polygres.wire.acl.ConnectionGate connectionGate = com.polygres.wire.acl.ConnectionGate.create(
                clientAcl, "true".equalsIgnoreCase(config.aclPpv2Enabled()),
                com.polygres.wire.acl.ConnectionGate.parseTrustedProxies(config.aclTrustedProxies()));

        com.polygres.wire.http.auth.AccessContextResolver oauth = com.polygres.wire.http.auth.AccessContextResolver.create(
                config.oauthIssuer(), config.oauthAudience(), config.oauthUserIdClaim(), config.oauthRolesClaim());

        int metricsPort = parseIntEnv("POLYWIRE_METRICS_PORT", 19090);
        MetricsServer metricsServer = new MetricsServer(metricsPort, statsStage, qosStage, currentConfigVersion::get, connectionGate, oauth, firewallRuleStore, configStore, backendRegistry);
        metricsServer.start();

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

            listenerExecutor.submit(() -> acceptOraWireTlsLoop(options, tlsSocketFactory, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate));

            grpcServer.startTls();
            log.info("polywire listening for gRPC TLS on port {}", options.grpcTlsPort());
        } else {
            log.info("TLS disabled (set POLYWIRE_TLS_KEYSTORE to enable orawire TCPS / pgwire+mywire in-band TLS / gRPC TLS)");
        }

        final com.polygres.wire.auth.PgRoleAuthCache roleAuthCache =
                "postgres_roles".equals(System.getenv("POLYWIRE_AUTH_MODE"))
                        ? new com.polygres.wire.auth.PgRoleAuthCache(options) : null;
        if (roleAuthCache != null) {
            log.info("auth: POLYWIRE_AUTH_MODE=postgres_roles -- pgwire/mssqlwire logins verified against "
                    + "real pg_authid role passwords (refreshed every {}s), not CredentialStore's shared secret",
                    parseIntEnv("POLYWIRE_AUTH_REFRESH_SECONDS", 30));
        }

        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, connectionGate));
        listenerExecutor.submit(() -> acceptMssqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor, roleAuthCache, connectionGate));
        listenerExecutor.submit(() -> acceptMongoWireLoop(options, sessionExecutor, mongoCache, connectionGate, sqlMetrics));

        int dynamoWirePort = parseIntEnv("POLYWIRE_DYNAMOWIRE_PORT", 18000);

        com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials =
                com.polygres.wire.dynamowire.auth.AwsIamCredentialStore.create(config.awsIamCredentials());
        // Sharded when POLYWIRE_SHARD_BACKENDS is configured (the same shard group SQL's
        // value-shard rules already use) -- item storage splits across it by partition-key hash,
        // same as real DynamoDB's own partition model. Falls back to the single implicit
        // backend automatically when no shard group is configured -- see PgItemStore's javadoc.
        DynamoWireServer dynamoWireServer = new DynamoWireServer(dynamoWirePort, backendRegistry, dynamoCache,
                connectionGate, oauth, awsIamCredentials, sqlMetrics);
        dynamoWireServer.start();
        log.info("polywire listening for DynamoDB HTTP/JSON (dynamowire) on port {}", dynamoWirePort);

        int mcpPort = parseIntEnv("POLYWIRE_MCP_PORT", 18010);
        com.polygres.wire.mcp.PolyWireMcpServer mcpServer = new com.polygres.wire.mcp.PolyWireMcpServer(
                mcpPort, options, pipelineStages, backendRegistry, connectionGate, System.getenv("POLYWIRE_MCP_TOOLS"), oauth);
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
            
            clientAcl.reload(c.aclRules());
            connectionGate.reload("true".equalsIgnoreCase(c.aclPpv2Enabled()),
                    com.polygres.wire.acl.ConnectionGate.parseTrustedProxies(c.aclTrustedProxies()));
            oauth.reload(c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim());
            awsIamCredentials.reload(c.awsIamCredentials());
            log.info("config: version {} applied (qos rate={}/s burst={}, {} router rule set(s), "
                            + "{} backend(s), cache={}, {} rollup definition(s), acl={} rule(s), "
                            + "oauth={}, awsIam={} credential(s))",
                    newVersion.version(), c.qosRatePerSec(), c.qosBurst(),
                    routerStage.schemaRules().size() + routerStage.predicateRules().size()
                            + routerStage.valueShardRules().size() + routerStage.shardRules().size(),
                    backendRegistry.all().size(), cacheStage != null, newRollups.size(),
                    clientAcl.hasRules() ? "some" : "0", c.oauthIssuer() == null ? "disabled" : "enabled",
                    awsIamCredentials.isEnabled() ? "some" : "0");
        });

        acceptOraWireLoop(options, backendPool, pipelineStages, backendRegistry, sessionExecutor, connectionGate);
    }

    private static PolyWireCluster startLocalCacheCluster() {
        return PolyWireCluster.startSingleNodeForCacheOnly();
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private static void acceptPgWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache, com.polygres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("polywire listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
                    options.pgWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                sessionExecutor.submit(new PgWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache));
            }
        } catch (IOException e) {
            log.error("Postgres wire listener on port {} failed", options.pgWireListenPort(), e);
        }
    }

    private static void acceptMySqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.myWireListenPort())) {
            log.info("polywire listening for TCP (MySQL wire) on port {}, proxying to postgres {}:{}/{}",
                    options.myWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                sessionExecutor.submit(new MySqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("MySQL wire listener on port {} failed", options.myWireListenPort(), e);
        }
    }

    private static void acceptMssqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache, com.polygres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.mssqlWireListenPort())) {
            log.info("polywire listening for TCP (SQL Server TDS wire) on port {}, proxying to postgres {}:{}/{}",
                    options.mssqlWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                sessionExecutor.submit(new MssqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry, roleAuthCache));
            }
        } catch (IOException e) {
            log.error("SQL Server TDS wire listener on port {} failed", options.mssqlWireListenPort(), e);
        }
    }

    private static void acceptMongoWireLoop(ServerOptions options, ExecutorService sessionExecutor,
            com.polygres.wire.mongowire.MongoCache mongoCache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        int mongoPort = parseIntEnv("POLYWIRE_MONGOWIRE_PORT", 27017);
        String pgUrl = "jdbc:postgresql://" + options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase();
        try (ServerSocket serverSocket = new ServerSocket(mongoPort)) {
            log.info("polywire listening for TCP (MongoDB wire) on port {}, proxying to postgres {} "
                    + "(find/insert/update/delete only -- no aggregation pipeline, see MongoWireSessionHandler)",
                    mongoPort, pgUrl);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                sessionExecutor.submit(new MongoWireSessionHandler(clientSocket, pgUrl, options.pgUser(), options.pgPassword(), mongoCache, sqlMetrics));
            }
        } catch (IOException e) {
            log.error("MongoDB wire listener on port {} failed", mongoPort, e);
        }
    }

    private static void acceptOraWireLoop(ServerOptions options, PgBackendPool backendPool,
            List<PipelineStage> pipelineStages, BackendRegistry backendRegistry, ExecutorService sessionExecutor,
            com.polygres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.listenPort())) {
            log.info("polywire listening for TCP (Oracle wire) on port {}, proxying to postgres {}:{}/{}",
                    options.listenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(clientSocket)) {
                    continue;
                }
                sessionExecutor.submit(new SessionHandler(clientSocket, backendPool, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Oracle wire listener on port {} failed", options.listenPort(), e);
        }
    }

    private static void acceptOraWireTlsLoop(ServerOptions options, SSLSocketFactory tlsSocketFactory,
            PgBackendPool backendPool, List<PipelineStage> pipelineStages, BackendRegistry backendRegistry,
            ExecutorService sessionExecutor, com.polygres.wire.acl.ConnectionGate connectionGate) {
        try (ServerSocket serverSocket = new ServerSocket(options.tlsPort())) {
            log.info("polywire listening for TCPS (Oracle wire over TLS) on port {}, proxying to postgres {}:{}/{}",
                    options.tlsPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket plainSocket = serverSocket.accept();
                if (!connectionGate.acceptTcp(plainSocket)) {
                    continue;
                }
                SSLSocket tlsSocket = (SSLSocket) tlsSocketFactory.createSocket(
                        plainSocket, null, plainSocket.getPort(), true);
                tlsSocket.setUseClientMode(false);
                sessionExecutor.submit(new SessionHandler(tlsSocket, backendPool, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Oracle wire TCPS listener on port {} failed", options.tlsPort(), e);
        }
    }

    private Main() {
    }
}
