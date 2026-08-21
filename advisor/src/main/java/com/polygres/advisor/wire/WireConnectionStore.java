package com.polygres.advisor.wire;

import com.polygres.advisor.core.BackendConnectionPools;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Where PolyAdvisor should find PolyWire's admin API and the bearer token to authenticate with
 * it -- one row, same embedded HSQLDB store {@link com.polygres.advisor.llm.LlmSettingsStore}
 * uses. This is what makes the combined UI possible: the browser only ever talks to Advisor's own
 * backend (already gated by {@link com.polygres.advisor.http.auth.AdminAuth}'s session), which
 * then makes the real call to PolyWire's admin port server-to-server -- no CORS, no second auth
 * system exposed to the browser.
 */
public class WireConnectionStore {

    private static final String POOL_KEY = "polygres-control";
    private final String jdbcUrl;

    public WireConnectionStore() {
        String dataDir = System.getenv().getOrDefault("POLYGRES_DATA_DIR",
                System.getProperty("user.home") + "/.polygres");
        this.jdbcUrl = "jdbc:hsqldb:file:" + dataDir + "/polygres-store;shutdown=true";
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS wire_connection ("
                    + "id INTEGER PRIMARY KEY, "
                    + "admin_url VARCHAR(512), "
                    + "admin_token VARCHAR(512))");
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize Wire connection store at " + jdbcUrl, e);
        }
    }

    public record WireConnection(String adminUrl, String adminToken) {
        public boolean configured() {
            return adminUrl != null && !adminUrl.isBlank() && adminToken != null && !adminToken.isBlank();
        }
    }

    public WireConnection get() {
        try (Connection connection = borrow();
                PreparedStatement ps = connection.prepareStatement("SELECT admin_url, admin_token FROM wire_connection WHERE id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new WireConnection(rs.getString("admin_url"),
                            com.polygres.advisor.secrets.FieldCipher.decrypt(rs.getString("admin_token")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read Wire connection settings", e);
        }
        return new WireConnection(null, null);
    }

    public WireConnection save(String adminUrl, String adminToken) {
        WireConnection existing = get();
        // Blank adminToken on save keeps the existing stored token -- same "browser never has the
        // real secret to send back" convention as LlmSettingsStore#save.
        String tokenToStore = (adminToken != null && !adminToken.isBlank()) ? adminToken : existing.adminToken();
        String encryptedToken = com.polygres.advisor.secrets.FieldCipher.encrypt(tokenToStore);
        try (Connection connection = borrow();
                PreparedStatement ps = connection.prepareStatement(
                        "MERGE INTO wire_connection USING (VALUES(1)) AS src(id) ON wire_connection.id = src.id "
                                + "WHEN MATCHED THEN UPDATE SET admin_url = ?, admin_token = ? "
                                + "WHEN NOT MATCHED THEN INSERT (id, admin_url, admin_token) VALUES (1, ?, ?)")) {
            ps.setString(1, adminUrl);
            ps.setString(2, encryptedToken);
            ps.setString(3, adminUrl);
            ps.setString(4, encryptedToken);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not save Wire connection settings", e);
        }
        return new WireConnection(adminUrl, tokenToStore);
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, "SA", "");
    }
}
