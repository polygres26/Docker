package com.sayonora.warp.config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConfigStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigStore.class);
    private static final String CHANNEL = "warp_config_changed";

    public record Version(long version, WarpConfig payload, java.time.Instant createdAt) {
    }

    private final com.sayonora.warp.server.ServerOptions options;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;
    /** The newest config version handed to the listener. A notification sent while the LISTEN connection was down is never redelivered by
     * Postgres, so on every (re)connect and on a timer the latest version is read and delivered when it is newer than this. */
    private final java.util.concurrent.atomic.AtomicLong lastDelivered = new java.util.concurrent.atomic.AtomicLong();
    /** How often to compare the stored version with the delivered one even without a notification; 0 turns the poll off. */
    private final long pollMillis = pollMillisFromEnv();

    private static long pollMillisFromEnv() {
        try {
            String v = System.getenv("WARP_CONFIG_POLL_SECONDS");
            return (v == null || v.isBlank() ? 30 : Long.parseLong(v.trim())) * 1000;
        } catch (NumberFormatException e) {
            return 30_000;
        }
    }

    public ConfigStore(com.sayonora.warp.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_config ("
                    + "version bigserial PRIMARY KEY, "
                    + "payload jsonb NOT NULL, "
                    + "created_at timestamptz NOT NULL DEFAULT now())");
            st.execute("CREATE OR REPLACE FUNCTION warp_config_notify() RETURNS trigger AS $$ "
                    + "BEGIN PERFORM pg_notify('" + CHANNEL + "', NEW.version::text); RETURN NEW; END; "
                    + "$$ LANGUAGE plpgsql");
            st.execute("DROP TRIGGER IF EXISTS warp_config_notify_trigger ON warp_config");
            st.execute("CREATE TRIGGER warp_config_notify_trigger AFTER INSERT ON warp_config "
                    + "FOR EACH ROW EXECUTE FUNCTION warp_config_notify()");
        }
    }

    public long write(WarpConfig config) throws SQLException {
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options);
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO warp_config (payload) VALUES (?::jsonb) RETURNING version")) {
            ps.setString(1, encryptSecretFields(config).toJson());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public Optional<Version> readLatest() throws SQLException {
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT version, payload::text, created_at FROM warp_config "
                                + "ORDER BY version DESC LIMIT 1")) {
            if (!rs.next()) {
                return Optional.empty();
            }
            long version = rs.getLong(1);
            WarpConfig payload = decryptSecretFields(WarpConfig.fromJson(rs.getString(2)));
            java.time.Instant createdAt = rs.getTimestamp(3).toInstant();
            return Optional.of(new Version(version, payload, createdAt));
        }
    }

    // Only these three fields ever carry a credential -- backends' "name=url|user|password" spec
    // embeds a literal password inline, awsIamCredentials is exactly what it sounds like, and
    // llmApiKey is the dialect-translation LLM fallback's API key. Everything else in
    // WarpConfig (QoS, router rules, ACL, OAuth issuer/audience/claims, llmProvider/llmBaseUrl/
    // llmModel) is config, not secret, and stays as plain JSON so the column's still a real jsonb
    // document a human can read by eye. See com.sayonora.warp.secrets.FieldCipher's javadoc for the
    // encv1:-prefix / plaintext-passthrough scheme that makes this a no-op migration.
    private static WarpConfig encryptSecretFields(WarpConfig c) {
        return new WarpConfig(
                c.qosRatePerSec(), c.qosBurst(), c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(),
                c.cacheTables(), c.cacheTtlMs(),
                com.sayonora.warp.secrets.FieldCipher.encrypt(c.backends()), c.shardBackends(), c.backendSets(),
                c.routerSchemaRules(), c.routerPredicateRules(), c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards(),
                c.rollupDefinitionsYaml(),
                c.aclRules(), c.aclPpv2Enabled(), c.aclTrustedProxies(),
                c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim(),
                com.sayonora.warp.secrets.FieldCipher.encrypt(c.awsIamCredentials()),
                c.llmProvider(), com.sayonora.warp.secrets.FieldCipher.encrypt(c.llmApiKey()),
                c.llmBaseUrl(), c.llmModel(), c.backendGroups(),
                c.backendDescriptions(), c.backendGroupDescriptions(), c.mcpEndpoints(),
                c.backendStores(), c.backendSetNames(), c.connectionRoutes(), c.mcpUpstreams(), c.storeFrontendSets(),
                c.otlpExportOverride(), c.prometheusScrapeOverride(), c.accessPolicy());
    }

    private static WarpConfig decryptSecretFields(WarpConfig c) {
        return new WarpConfig(
                c.qosRatePerSec(), c.qosBurst(), c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(),
                c.cacheTables(), c.cacheTtlMs(),
                com.sayonora.warp.secrets.FieldCipher.decrypt(c.backends()), c.shardBackends(), c.backendSets(),
                c.routerSchemaRules(), c.routerPredicateRules(), c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards(),
                c.rollupDefinitionsYaml(),
                c.aclRules(), c.aclPpv2Enabled(), c.aclTrustedProxies(),
                c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim(),
                com.sayonora.warp.secrets.FieldCipher.decrypt(c.awsIamCredentials()),
                c.llmProvider(), com.sayonora.warp.secrets.FieldCipher.decrypt(c.llmApiKey()),
                c.llmBaseUrl(), c.llmModel(), c.backendGroups(),
                c.backendDescriptions(), c.backendGroupDescriptions(), c.mcpEndpoints(),
                c.backendStores(), c.backendSetNames(), c.connectionRoutes(), c.mcpUpstreams(), c.storeFrontendSets(),
                c.otlpExportOverride(), c.prometheusScrapeOverride(), c.accessPolicy());
    }

    /** Calls {@code callback} for every config version newer than {@code appliedVersion} (the one the process started from), whether it
     * arrived as a notification, was found when the LISTEN connection came back, or was found by the periodic poll. */
    public void listen(long appliedVersion, Consumer<Version> callback) throws SQLException {
        if (!listening.compareAndSet(false, true)) {
            throw new IllegalStateException("listen() already called on this ConfigStore");
        }
        lastDelivered.set(appliedVersion);
        listenExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "warp-config-listen");
            t.setDaemon(true);
            return t;
        });
        listenExecutor.submit(() -> listenLoop(callback));
    }

    private void listenLoop(Consumer<Version> callback) {
        while (listening.get()) {
            try {
                Connection conn = com.sayonora.warp.pgwire.PgConnections.openRaw(options);
                this.listenConnection = conn;
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                log.info("config: LISTEN {} established on a dedicated connection", CHANNEL);
                // anything written while this connection was down (or before it first existed) was never notified to us
                catchUp(callback, "after the LISTEN connection was (re)established");
                PGConnection pgConn = conn.unwrap(PGConnection.class);
                long lastPoll = System.currentTimeMillis();
                while (listening.get() && !conn.isClosed()) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null && notifications.length > 0) {
                        log.info("config: received {} notification(s) on {}, re-reading latest version",
                                notifications.length, CHANNEL);
                        try {
                            readLatest().ifPresent(v -> deliver(v, callback));
                        } catch (SQLException e) {
                            log.warn("config: failed to re-read latest version after notification", e);
                        }
                    }
                    if (pollMillis > 0 && System.currentTimeMillis() - lastPoll >= pollMillis) {
                        lastPoll = System.currentTimeMillis();
                        catchUp(callback, "by the periodic poll (a notification was missed)");
                    }
                }
            } catch (SQLException e) {
                if (listening.get()) {
                    log.warn("config: LISTEN connection failed, retrying in 2s", e);
                    sleep(2000);
                }
            }
        }
    }

    /** Delivers the latest stored version when it is newer than the last one delivered. */
    private void catchUp(Consumer<Version> callback, String how) {
        try {
            long stored = latestVersion();
            if (stored > lastDelivered.get()) {
                readLatest().ifPresent(v -> {
                    if (v.version() > lastDelivered.get()) {
                        log.warn("config: found version {} newer than the applied {} {}; applying it", v.version(), lastDelivered.get(), how);
                    }
                    deliver(v, callback);
                });
            }
        } catch (SQLException e) {
            log.warn("config: could not compare the stored config version {}: {}", how, e.toString());
        }
    }

    private synchronized void deliver(Version v, Consumer<Version> callback) {
        long previous = lastDelivered.get();
        if (v.version() <= previous) {
            return;
        }
        lastDelivered.set(v.version());
        try {
            callback.accept(v);
        } catch (RuntimeException e) {
            lastDelivered.set(previous); // applying it failed: the next poll or notification tries again
            log.error("config: applying version {} failed; will retry", v.version(), e);
        }
    }

    private long latestVersion() throws SQLException {
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT coalesce(max(version), 0) FROM warp_config")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        listening.set(false);
        Connection conn = listenConnection;
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                
            }
        }
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }
}
