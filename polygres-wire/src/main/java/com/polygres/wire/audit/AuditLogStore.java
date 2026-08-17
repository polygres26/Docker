package com.polygres.wire.audit;

import com.polygres.wire.core.BackendConnectionPools;
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

/**
 * Durable, queryable, hash-chained audit trail — the DB half of "audit logs live in a database,"
 * the same shape {@code com.polygres.wire.core.ConfigStore} already established for "config lives in a
 * database": opt-in (only constructed when {@code POLYWIRE_AUDIT_LOG_DB} is set), reuses
 * {@link BackendConnectionPools} under a fixed pool key rather than a second pooling mechanism, one
 * hand-rolled {@code CREATE TABLE IF NOT EXISTS} with no migration framework (proportionate to one
 * table, same as {@code ConfigStore}'s own stated rationale).
 *
 * <p><b>Tamper-evident, not tamper-proof</b> — an important distinction, stated plainly rather than
 * oversold. Every row's {@code row_hash} is {@code SHA-256(prev_hash | ts | type | user_id |
 * summary | details)}, chaining each row to the one before it. {@link #verifyChain()} re-walks the
 * whole table and recomputes every hash; any row whose stored content was edited, or any row
 * deleted from the middle of the chain, breaks the recomputed hash for every row after it —
 * detectable, not preventable. This is a self-contained integrity check the app can run itself; it
 * is not a substitute for the DB-level hardening real tamper-*resistance* needs (a dedicated
 * append-only DB role with {@code INSERT} but not {@code UPDATE}/{@code DELETE} granted — a
 * deployment/DBA concern documented in {@code ARCHITECTURE.md}, not something this class can
 * enforce against its own credential, since the same credential that could be restricted could
 * also un-restrict itself).
 *
 * <p><b>Ordering</b>: {@code seq_num} is an app-assigned monotonic counter, not a DB-native
 * auto-increment/serial/identity column — deliberately, so the same schema works unchanged across
 * every JDBC backend this codebase already supports (Postgres/MySQL/Oracle spell "auto-increment
 * primary key" three different ways; a plain {@code BIGINT} does not need any of them). Seeded from
 * {@code MAX(seq_num)} at construction, then an in-process counter — single-writer per process is
 * assumed the same way {@link AuditLog#record} already synchronizes; multiple PolyWire nodes
 * writing to the same audit table concurrently would need a real distributed sequence (future work,
 * not attempted here — see {@code com.polygres.wire.cluster.PolyWireCluster#nextSequence} for the
 * pattern this would borrow from if it's ever needed).
 */
public final class AuditLogStore {

    public static final String POOL_KEY = "polywire-audit";

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
            statement.execute("CREATE TABLE IF NOT EXISTS polywire_audit_log ("
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
                        "SELECT seq_num, row_hash FROM polywire_audit_log ORDER BY seq_num DESC LIMIT 1")) {
            if (rs.next()) {
                this.nextSeq = rs.getLong(1) + 1;
                this.lastHash = rs.getString(2);
            } else {
                this.nextSeq = 1;
                this.lastHash = "";
            }
        }
    }

    /** Appends one row, chained to whatever the last-appended row's hash was (in-process; see class javadoc's "Ordering" section for the single-writer assumption). */
    public synchronized void append(AuditEvent event) throws SQLException {
        String detailsJson = AuditLog.toJson(event).getAsJsonObject("details").toString();
        String rowHash = hash(lastHash, event.timestamp().toString(), event.type().name(),
                event.userId(), event.summary(), detailsJson);
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO polywire_audit_log (seq_num, ts, event_type, user_id, summary, details, "
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

    /** Most recent {@code limit} events, newest first — {@code details} round-trips through the same JSON shape {@link AuditLog#toJson} produces, so a DB-backed read looks identical to an in-memory-ring-buffer one. */
    public List<AuditEvent> recent(int limit) throws SQLException {
        List<AuditEvent> result = new ArrayList<>();
        try (Connection connection = borrow();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT ts, event_type, user_id, summary, details FROM polywire_audit_log "
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

    /** Re-walks the entire table in {@code seq_num} order, recomputing each row's hash from its stored content and the previous row's stored {@code row_hash}, and compares against what's actually stored. See class javadoc's "Tamper-evident, not tamper-proof" section for exactly what this does and doesn't guarantee. */
    public VerificationResult verifyChain() throws SQLException {
        String expectedPrevHash = "";
        try (Connection connection = borrow();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT seq_num, ts, event_type, user_id, summary, details, prev_hash, row_hash "
                                + "FROM polywire_audit_log ORDER BY seq_num ASC")) {
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

    /** Package-visible so its determinism/sensitivity can be unit-tested directly, without needing a live database — same "expose the pure piece" convention {@code CacheStage.extractWriteTarget} already uses; {@link #append}/{@link #recent}/{@link #verifyChain} themselves need a real JDBC connection and are covered by live verification instead (same precedent {@code ConfigStore} already sets — no JUnit coverage of its own DB-touching methods either). */
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
            throw new IllegalStateException("SHA-256 unavailable", e); // every JDK ships this; unreachable in practice
        }
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, user, password);
    }
}
