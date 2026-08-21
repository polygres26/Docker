package com.polygres.advisor.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * CRUD store for {@link ConnectionRecord}s -- the admin's "set of connections to various data
 * sources" -- backed by an embedded HSQLDB database file, not a hand-rolled JSON file. Same
 * control-plane-lives-in-a-real-database shape as Omnigate's {@code com.omnigate.core.ConfigStore},
 * scaled down to one table; this is also the natural place to add more control-plane state later
 * (scan history, saved reports) without inventing a second storage mechanism.
 *
 * <p>Location: {@code POLYGRES_DATA_DIR} env var, defaulting to {@code ~/.polygres}; the database
 * file lives at {@code <dir>/polygres-store}. Uses {@link BackendConnectionPools} under a fixed
 * pool key, same as every other connection in this app, rather than a separate pooling mechanism.
 *
 * <p>{@code password} is encrypted at rest (AES-256-GCM) via {@link com.polygres.advisor.secrets.FieldCipher}
 * whenever {@code POLYGRES_ENCRYPTION_KEY} is set on this process -- opt-in, with a loud startup
 * warning if it isn't, and backward-compatible with rows written before the key existed (they
 * stay plaintext until the next save). {@link ConnectionRecord#redacted()} is what keeps the
 * (decrypted) value from ever round-tripping back to the browser regardless.
 */
public class ConnectionStore {

    private static final String POOL_KEY = "polygres-control";

    private final String jdbcUrl;

    public ConnectionStore() {
        String dataDir = System.getenv().getOrDefault("POLYGRES_DATA_DIR",
            System.getProperty("user.home") + "/.polygres");
        this.jdbcUrl = "jdbc:hsqldb:file:" + dataDir + "/polygres-store;shutdown=true";
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS connections ("
                + "id VARCHAR(64) PRIMARY KEY, "
                + "name VARCHAR(255), "
                + "jdbc_url VARCHAR(1024), "
                + "user_name VARCHAR(255), "
                + "password VARCHAR(1024), "
                + "created_at VARCHAR(64))");
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize connection store at " + jdbcUrl, e);
        }
    }

    public List<ConnectionRecord> list() {
        List<ConnectionRecord> records = new ArrayList<>();
        try (Connection connection = borrow();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM connections ORDER BY created_at")) {
            while (rs.next()) {
                records.add(fromRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not list connections", e);
        }
        return records;
    }

    public Optional<ConnectionRecord> get(String id) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement("SELECT * FROM connections WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(fromRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read connection " + id, e);
        }
    }

    public ConnectionRecord create(String name, String jdbcUrl, String user, String password) {
        ConnectionRecord record = new ConnectionRecord(name, jdbcUrl, user, password);
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement(
                 "INSERT INTO connections (id, name, jdbc_url, user_name, password, created_at) VALUES (?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, record.id);
            ps.setString(2, record.name);
            ps.setString(3, record.jdbcUrl);
            ps.setString(4, record.user);
            ps.setString(5, com.polygres.advisor.secrets.FieldCipher.encrypt(record.password));
            ps.setString(6, record.createdAt);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not create connection", e);
        }
        return record;
    }

    /** Blank {@code password} keeps the existing stored credential -- see {@link ConnectionRecord} javadoc. */
    public Optional<ConnectionRecord> update(String id, String name, String jdbcUrl, String user, String password) {
        Optional<ConnectionRecord> existingOpt = get(id);
        if (existingOpt.isEmpty()) return Optional.empty();
        ConnectionRecord existing = existingOpt.get();
        existing.name = name;
        existing.jdbcUrl = jdbcUrl;
        existing.user = user;
        if (password != null && !password.isBlank()) {
            existing.password = password;
        }
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement(
                 "UPDATE connections SET name = ?, jdbc_url = ?, user_name = ?, password = ? WHERE id = ?")) {
            ps.setString(1, existing.name);
            ps.setString(2, existing.jdbcUrl);
            ps.setString(3, existing.user);
            ps.setString(4, com.polygres.advisor.secrets.FieldCipher.encrypt(existing.password));
            ps.setString(5, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not update connection " + id, e);
        }
        return Optional.of(existing);
    }

    public boolean delete(String id) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement("DELETE FROM connections WHERE id = ?")) {
            ps.setString(1, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Could not delete connection " + id, e);
        }
    }

    private ConnectionRecord fromRow(ResultSet rs) throws SQLException {
        ConnectionRecord record = new ConnectionRecord();
        record.id = rs.getString("id");
        record.name = rs.getString("name");
        record.jdbcUrl = rs.getString("jdbc_url");
        record.user = rs.getString("user_name");
        record.password = com.polygres.advisor.secrets.FieldCipher.decrypt(rs.getString("password"));
        record.createdAt = rs.getString("created_at");
        return record;
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, "SA", "");
    }
}
