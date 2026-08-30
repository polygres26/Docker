package com.nexagres.migration.checkpoint;

import com.nexagres.migration.core.StateStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Durable resume-point storage for every {@link com.nexagres.migration.core.Source} in this
 * project -- one row per source (e.g. {@code "mongo:mydb.orders"}), in the TARGET Postgres (the
 * same database the migration writes into, not a separate control-plane database), so a worker
 * restarted after a crash resumes exactly where it left off instead of either replaying the whole
 * source from scratch or silently skipping whatever changed while it was down.
 *
 * <p>Deliberately connects to the target Postgres DIRECTLY (not through {@link
 * com.nexagres.migration.sink.PolywireGrpcSink}) -- a checkpoint is migration-infrastructure
 * bookkeeping, not customer data, and doesn't need Polywire's firewall/cache/QoS semantics
 * applied to it; going through the full pipeline for this would only add latency and risk
 * (a firewall rule that happens to match {@code polywire_cdc_checkpoints} would break resumability
 * itself) for no real benefit.
 */
public final class CdcCheckpointStore implements StateStore {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public CdcCheckpointStore(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = open(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_cdc_checkpoints ("
                    + "source_key text PRIMARY KEY, "
                    + "resume_token jsonb NOT NULL, "
                    + "events_applied bigint NOT NULL DEFAULT 0, "
                    + "updated_at timestamptz NOT NULL DEFAULT now())");
        }
    }

    @Override
    public String load(String sourceKey) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT resume_token FROM polywire_cdc_checkpoints WHERE source_key = ?")) {
            ps.setString(1, sourceKey);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** One round trip per call, deliberately simple for v1 -- a real high-throughput source may
     * eventually want this batched (every N events or every T seconds) rather than after every
     * single change event, but correctness (never advancing the checkpoint past an event that
     * wasn't actually applied yet) matters far more than shaving round trips here, and every
     * apply path in this project is already idempotent by id, so a slightly-stale checkpoint
     * after a crash just means a few harmless replayed upserts, never lost data. */
    @Override
    public void save(String sourceKey, String resumeTokenJson) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_cdc_checkpoints (source_key, resume_token, events_applied, updated_at) "
                                + "VALUES (?, ?::jsonb, 1, now()) "
                                + "ON CONFLICT (source_key) DO UPDATE SET resume_token = EXCLUDED.resume_token, "
                                + "events_applied = polywire_cdc_checkpoints.events_applied + 1, updated_at = now()")) {
            ps.setString(1, sourceKey);
            ps.setString(2, resumeTokenJson);
            ps.executeUpdate();
        }
    }

    /** For an admin/CLI status view -- {@code null} if this source has no checkpoint row. */
    public Long eventsApplied(String sourceKey) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT events_applied FROM polywire_cdc_checkpoints WHERE source_key = ?")) {
            ps.setString(1, sourceKey);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }
}
