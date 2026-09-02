package com.sayonora.wire.core;

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

public final class ConfigStore {

    public static final String POOL_KEY = "warp-control";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public ConfigStore(String jdbcUrl, String user, String password) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS warp_config ("
                    + "config_key VARCHAR(255) PRIMARY KEY, "
                    + "config_value TEXT, "
                    + "updated_at TIMESTAMP, "
                    + "updated_by VARCHAR(255))");
        }
    }

    public Optional<String> get(String key) throws SQLException {
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT config_value FROM warp_config WHERE config_key = ?")) {
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
                ResultSet rs = statement.executeQuery("SELECT config_key, config_value FROM warp_config")) {
            while (rs.next()) {
                all.put(rs.getString(1), rs.getString(2));
            }
        }
        return all;
    }

    public void put(String key, String value, String updatedBy) throws SQLException {
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE warp_config SET config_value = ?, updated_at = ?, updated_by = ? WHERE config_key = ?")) {
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
                        "INSERT INTO warp_config (config_key, config_value, updated_at, updated_by) VALUES (?, ?, ?, ?)")) {
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
