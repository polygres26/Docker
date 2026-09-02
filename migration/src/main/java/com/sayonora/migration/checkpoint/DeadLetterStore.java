package com.sayonora.migration.checkpoint;

import com.sayonora.migration.core.ChangeEvent;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Where a {@link ChangeEvent} lands after {@link com.sayonora.migration.sink.ResilientSink}
 * exhausts its retries on it -- Phase 3 of this session's migration plan ("dead-letter handling:
 * a failed ChangeEvent currently throws and kills the worker; needs a retry-then-dead-letter path
 * so one bad row doesn't stall a whole partition"). Same "own Postgres table on the target" pattern
 * as {@link CdcCheckpointStore}: a dead letter is migration-infrastructure bookkeeping (evidence
 * of a write that never landed), not customer data, so this connects directly, not through {@link
 * com.sayonora.migration.sink.WarpGrpcSink} -- the same reasoning {@link CdcCheckpointStore}'s
 * own javadoc gives for why IT bypasses the gRPC sink too.
 *
 * <p>Deliberately just a durable record, not a retry queue of its own -- replaying a dead-lettered
 * event (once its underlying cause, usually a real data problem on the source, e.g. a value that
 * doesn't fit the target column type, is fixed) is a real, separately scoped follow-up (a "replay
 * dead letters" CLI command), not built here. What matters for v1 is that ONE bad row can no
 * longer take down an entire partition's worker thread or the whole change feed -- it gets
 * recorded and skipped, not silently dropped and not fatal.
 */
public final class DeadLetterStore {

    private static final Gson GSON = new Gson();

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public DeadLetterStore(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = open(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS migration_dead_letters ("
                    + "id bigserial PRIMARY KEY, "
                    + "sql text NOT NULL, "
                    + "params jsonb NOT NULL, "
                    + "error_message text NOT NULL, "
                    + "attempts integer NOT NULL, "
                    + "failed_at timestamptz NOT NULL DEFAULT now())");
        }
    }

    /** Records one permanently-failed event. Never throws {@code SQLException} back to the caller
     * on a best-effort basis would defeat the point (a dead letter that silently failed to record
     * is worse than the original failure), so this DOES propagate a write failure here -- a caller
     * (see {@code ResilientSink}) that can't even record the dead letter has a real, separate
     * infrastructure problem (the target Postgres itself is unreachable) worth surfacing loudly,
     * not one more thing to swallow. */
    public void record(ChangeEvent event, String errorMessage, int attempts) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO migration_dead_letters (sql, params, error_message, attempts) "
                                + "VALUES (?, ?::jsonb, ?, ?)")) {
            ps.setString(1, event.sql());
            ps.setString(2, GSON.toJson(event.params()));
            ps.setString(3, errorMessage == null ? "(no message)" : errorMessage);
            ps.setInt(4, attempts);
            ps.executeUpdate();
        }
    }

    /** For an admin/CLI/Advisor status view. */
    public long count() throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM migration_dead_letters");
                var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
