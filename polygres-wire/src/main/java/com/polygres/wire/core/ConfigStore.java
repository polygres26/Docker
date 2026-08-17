package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Persisted, live-editable config — the DB half of "config lives in a database, with a UI/API to
 * update it" (ARCHITECTURE.md config-store section). One row per top-level config key (the exact
 * same key names/string formats each {@code fromConfig(String)} already parses, e.g.
 * {@code POLYWIRE_BACKENDS}), so no existing parser needed to change — only where the string comes
 * from.
 *
 * <p><b>Opt-in, like everything else config-shaped in this codebase</b>: only constructed when
 * {@code POLYWIRE_CONFIG_DB} is set ({@code GatewayComponents.fromEnv}); if unset, no
 * {@code ConfigStore} exists at all and every config value falls back to its pre-existing
 * env-var/default behavior, completely unchanged.
 *
 * <p><b>Precedence</b>: a row present in this table always wins over the env var of the same name,
 * which in turn wins over the compiled-in default (empty/unconfigured) — see
 * {@code GatewayComponents#resolveConfig}.
 *
 * <p><b>Connection</b>: reuses {@link BackendConnectionPools#borrow} under the fixed pool key
 * {@link #POOL_KEY} rather than adding a second pooling mechanism — same Hikari-backed pool every
 * real backend uses, just keyed as the gateway's own control connection instead of a query-routing
 * target. {@code ApiRoutes#metricsJson} filters this key out of the public pool snapshot so it
 * doesn't masquerade as a queryable backend.
 *
 * <p><b>No migration framework</b>: nothing in this repo uses Flyway/Liquibase/etc., so the schema
 * bootstrap here is a single hand-rolled {@code CREATE TABLE IF NOT EXISTS}, run once per process
 * from the constructor — proportionate to one table, one shape, forever (adding real migrations is
 * future work if/when this store grows past a handful of keys).
 */
public final class ConfigStore {

    /** Fixed {@link BackendConnectionPools} key for the control-DB connection — never a real query-routing target. */
    public static final String POOL_KEY = "polywire-control";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public ConfigStore(String jdbcUrl, String user, String password) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS polywire_config ("
                    + "config_key VARCHAR(255) PRIMARY KEY, "
                    + "config_value TEXT, "
                    + "updated_at TIMESTAMP, "
                    + "updated_by VARCHAR(255))");
        }
    }

    public Optional<String> get(String key) throws SQLException {
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT config_value FROM polywire_config WHERE config_key = ?")) {
            statement.setString(1, key);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.ofNullable(rs.getString(1)) : Optional.empty();
            }
        }
    }

    public Map<String, String> getAll() throws SQLException {
        Map<String, String> all = new LinkedHashMap<>();
        try (Connection connection = borrow();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT config_key, config_value FROM polywire_config")) {
            while (rs.next()) {
                all.put(rs.getString(1), rs.getString(2));
            }
        }
        return all;
    }

    /** Upsert — one row per {@code key}, last write wins. {@code updatedBy} is whatever identity the caller (typically the authenticated API caller) resolved, purely for audit/display. */
    public void put(String key, String value, String updatedBy) throws SQLException {
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE polywire_config SET config_value = ?, updated_at = ?, updated_by = ? WHERE config_key = ?")) {
            statement.setString(1, value);
            statement.setTimestamp(2, Timestamp.from(Instant.now()));
            statement.setString(3, updatedBy);
            statement.setString(4, key);
            if (statement.executeUpdate() > 0) {
                return;
            }
        }
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO polywire_config (config_key, config_value, updated_at, updated_by) VALUES (?, ?, ?, ?)")) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.setTimestamp(3, Timestamp.from(Instant.now()));
            statement.setString(4, updatedBy);
            statement.executeUpdate();
        }
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, user, password);
    }
}
