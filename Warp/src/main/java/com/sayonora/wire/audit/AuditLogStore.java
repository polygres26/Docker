package com.sayonora.wire.audit;

import com.sayonora.wire.core.BackendConnectionPools;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class AuditLogStore {

    public static final String POOL_KEY = "warp-audit";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private long nextSeq;
    private String lastHash;

    public AuditLogStore(String jdbcUrl, String user, String password) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS warp_audit_log ("
                    + "seq_num BIGINT PRIMARY KEY, "
                    + "ts TIMESTAMP, "
                    + "event_type VARCHAR(64), "
                    + "user_id VARCHAR(255), "
                    + "summary TEXT, "
                    + "details TEXT, "
                    + "prev_hash VARCHAR(64), "
                    + "row_hash VARCHAR(64) NOT NULL)");
        }
        try (Connection connection = borrow();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT seq_num, row_hash FROM warp_audit_log ORDER BY seq_num DESC LIMIT 1")) {
            if (rs.next()) {
                this.nextSeq = rs.getLong(1) + 1;
                this.lastHash = rs.getString(2);
            } else {
                this.nextSeq = 1;
                this.lastHash = "";
            }
        }
    }

    public synchronized void append(AuditEvent event) throws SQLException {
        String detailsJson = AuditLog.toJson(event).getAsJsonObject("details").toString();
        String rowHash = hash(lastHash, event.timestamp().toString(), event.type().name(),
                event.userId(), event.summary(), detailsJson);
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO warp_audit_log (seq_num, ts, event_type, user_id, summary, details, "
                                + "prev_hash, row_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setLong(1, nextSeq);
            statement.setTimestamp(2, Timestamp.from(event.timestamp()));
            statement.setString(3, event.type().name());
            statement.setString(4, event.userId());
            statement.setString(5, event.summary());
            statement.setString(6, detailsJson);
            statement.setString(7, lastHash);
            statement.setString(8, rowHash);
            statement.executeUpdate();
        }
        nextSeq++;
        lastHash = rowHash;
    }

    public List<AuditEvent> recent(int limit) throws SQLException {
        List<AuditEvent> result = new ArrayList<>();
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT ts, event_type, user_id, summary, details FROM warp_audit_log "
                                + "ORDER BY seq_num DESC LIMIT ?")) {
            statement.setInt(1, limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(new AuditEvent(rs.getTimestamp(1).toInstant(),
                            AuditEvent.Type.valueOf(rs.getString(2)), rs.getString(3), rs.getString(4),
                            detailsFromJson(rs.getString(5))));
                }
            }
        }
        return result;
    }

    private static java.util.Map<String, String> detailsFromJson(String json) {
        if (json == null || json.isBlank()) {
            return java.util.Map.of();
        }
        java.util.Map<String, String> details = new java.util.LinkedHashMap<>();
        com.google.gson.JsonObject obj = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        for (var entry : obj.entrySet()) {
            details.put(entry.getKey(), entry.getValue().getAsString());
        }
        return details;
    }

    public record VerificationResult(boolean valid, Long brokenAtSeq) {
        public static final VerificationResult VALID = new VerificationResult(true, null);
    }

    public VerificationResult verifyChain() throws SQLException {
        String expectedPrevHash = "";
        try (Connection connection = borrow();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT seq_num, ts, event_type, user_id, summary, details, prev_hash, row_hash "
                                + "FROM warp_audit_log ORDER BY seq_num ASC")) {
            while (rs.next()) {
                long seqNum = rs.getLong(1);
                String storedPrevHash = rs.getString(7);
                String storedRowHash = rs.getString(8);
                if (!expectedPrevHash.equals(storedPrevHash)) {
                    return new VerificationResult(false, seqNum);
                }
                String recomputed = hash(storedPrevHash, rs.getTimestamp(2).toInstant().toString(),
                        rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6));
                if (!recomputed.equals(storedRowHash)) {
                    return new VerificationResult(false, seqNum);
                }
                expectedPrevHash = storedRowHash;
            }
        }
        return VerificationResult.VALID;
    }

    static String hash(String... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(String.join("|", parts).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] bytes = digest.digest();
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, user, password);
    }
}
