package com.polygres.advisor.llm;

import com.polygres.advisor.core.BackendConnectionPools;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

/**
 * Persisted LLM configuration for the two {@link LlmRole}s, backed by the same embedded HSQLDB
 * database {@link com.polygres.advisor.core.ConnectionStore} uses (same pool key, same file) --
 * one more table in the existing control-plane store rather than a new storage mechanism.
 * Exactly one row per role, upserted -- there's no "list," only "get PRIMARY" / "get JUDGE."
 */
public class LlmSettingsStore {

    private static final String POOL_KEY = "polygres-control";
    private final String jdbcUrl;

    public LlmSettingsStore() {
        String dataDir = System.getenv().getOrDefault("POLYGRES_DATA_DIR",
            System.getProperty("user.home") + "/.polygres");
        this.jdbcUrl = "jdbc:hsqldb:file:" + dataDir + "/polygres-store;shutdown=true";
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS llm_settings ("
                + "role VARCHAR(16) PRIMARY KEY, "
                + "provider_type VARCHAR(16), "
                + "api_key VARCHAR(2048), "
                + "base_url VARCHAR(512), "
                + "model VARCHAR(256), "
                + "enabled BOOLEAN, "
                + "updated_at VARCHAR(64))");
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize LLM settings store at " + jdbcUrl, e);
        }
    }

    public LlmSettings get(LlmRole role) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement("SELECT * FROM llm_settings WHERE role = ?")) {
            ps.setString(1, role.name());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return fromRow(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read LLM settings for " + role, e);
        }
        return new LlmSettings(role); // not configured yet -- role-appropriate default (see LlmSettings ctor)
    }

    public LlmSettings save(LlmRole role, LlmProviderType providerType, String apiKey, String baseUrl, String model, boolean enabled) {
        LlmSettings existing = get(role);
        LlmSettings updated = new LlmSettings(role);
        updated.providerType = providerType;
        // Blank apiKey on save keeps the existing stored key -- same "browser never has the real
        // secret to send back" reasoning as ConnectionStore#update.
        updated.apiKey = (apiKey != null && !apiKey.isBlank()) ? apiKey : existing.apiKey;
        updated.baseUrl = baseUrl;
        updated.model = model;
        updated.enabled = enabled;
        updated.updatedAt = Instant.now().toString();

        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement(
                 "MERGE INTO llm_settings USING (VALUES(?)) AS src(role) ON llm_settings.role = src.role "
               + "WHEN MATCHED THEN UPDATE SET provider_type = ?, api_key = ?, base_url = ?, model = ?, enabled = ?, updated_at = ? "
               + "WHEN NOT MATCHED THEN INSERT (role, provider_type, api_key, base_url, model, enabled, updated_at) "
               + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, role.name());
            ps.setString(2, updated.providerType.name());
            ps.setString(3, updated.apiKey);
            ps.setString(4, updated.baseUrl);
            ps.setString(5, updated.model);
            ps.setBoolean(6, updated.enabled);
            ps.setString(7, updated.updatedAt);
            ps.setString(8, role.name());
            ps.setString(9, updated.providerType.name());
            ps.setString(10, updated.apiKey);
            ps.setString(11, updated.baseUrl);
            ps.setString(12, updated.model);
            ps.setBoolean(13, updated.enabled);
            ps.setString(14, updated.updatedAt);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not save LLM settings for " + role, e);
        }
        return updated;
    }

    private LlmSettings fromRow(ResultSet rs) throws SQLException {
        LlmSettings s = new LlmSettings();
        s.role = LlmRole.valueOf(rs.getString("role"));
        s.providerType = LlmProviderType.valueOf(rs.getString("provider_type"));
        s.apiKey = rs.getString("api_key");
        s.baseUrl = rs.getString("base_url");
        s.model = rs.getString("model");
        s.enabled = rs.getBoolean("enabled");
        s.updatedAt = rs.getString("updated_at");
        return s;
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, "SA", "");
    }
}
