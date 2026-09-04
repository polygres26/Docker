package com.sayonora.wire.config;

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

    private final com.sayonora.wire.server.ServerOptions options;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;

    public ConfigStore(com.sayonora.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = com.sayonora.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
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
        try (Connection conn = com.sayonora.wire.pgwire.PgConnections.open(options);
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
        try (Connection conn = com.sayonora.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement();
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
    // document a human can read by eye. See com.sayonora.wire.secrets.FieldCipher's javadoc for the
    // encv1:-prefix / plaintext-passthrough scheme that makes this a no-op migration.
    private static WarpConfig encryptSecretFields(WarpConfig c) {
        return new WarpConfig(
                c.qosRatePerSec(), c.qosBurst(), c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(),
                c.cacheTables(), c.cacheTtlMs(),
                com.sayonora.wire.secrets.FieldCipher.encrypt(c.backends()), c.shardBackends(), c.backendGroups(),
                c.routerSchemaRules(), c.routerPredicateRules(), c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards(),
                c.rollupDefinitionsYaml(),
                c.aclRules(), c.aclPpv2Enabled(), c.aclTrustedProxies(),
                c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim(),
                com.sayonora.wire.secrets.FieldCipher.encrypt(c.awsIamCredentials()),
                c.llmProvider(), com.sayonora.wire.secrets.FieldCipher.encrypt(c.llmApiKey()),
                c.llmBaseUrl(), c.llmModel());
    }

    private static WarpConfig decryptSecretFields(WarpConfig c) {
        return new WarpConfig(
                c.qosRatePerSec(), c.qosBurst(), c.qosMaxWaitMs(), c.qosClassLimits(), c.qosPoolWaitThreshold(),
                c.cacheTables(), c.cacheTtlMs(),
                com.sayonora.wire.secrets.FieldCipher.decrypt(c.backends()), c.shardBackends(), c.backendGroups(),
                c.routerSchemaRules(), c.routerPredicateRules(), c.routerValueShardRules(), c.routerShardTables(), c.routerTableShards(),
                c.rollupDefinitionsYaml(),
                c.aclRules(), c.aclPpv2Enabled(), c.aclTrustedProxies(),
                c.oauthIssuer(), c.oauthAudience(), c.oauthUserIdClaim(), c.oauthRolesClaim(),
                com.sayonora.wire.secrets.FieldCipher.decrypt(c.awsIamCredentials()),
                c.llmProvider(), com.sayonora.wire.secrets.FieldCipher.decrypt(c.llmApiKey()),
                c.llmBaseUrl(), c.llmModel());
    }

    public void listen(Consumer<Version> callback) throws SQLException {
        if (!listening.compareAndSet(false, true)) {
            throw new IllegalStateException("listen() already called on this ConfigStore");
        }
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
                Connection conn = com.sayonora.wire.pgwire.PgConnections.openRaw(options);
                this.listenConnection = conn;
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                log.info("config: LISTEN {} established on a dedicated connection", CHANNEL);
                PGConnection pgConn = conn.unwrap(PGConnection.class);
                while (listening.get() && !conn.isClosed()) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null && notifications.length > 0) {
                        log.info("config: received {} notification(s) on {}, re-reading latest version",
                                notifications.length, CHANNEL);
                        try {
                            readLatest().ifPresent(callback::accept);
                        } catch (SQLException e) {
                            log.warn("config: failed to re-read latest version after notification", e);
                        }
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
