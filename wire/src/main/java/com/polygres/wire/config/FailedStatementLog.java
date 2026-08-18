package com.polygres.wire.config;

import com.polygres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable, queryable record of statements PolyWire failed to service — either because
 * {@code DialectTranslationStage} could never turn them into valid SQL for the target backend
 * ({@link FailureType#UNTRANSLATABLE}), or because the backend itself rejected a real, translated
 * statement ({@link FailureType#BACKEND_ERROR}). Before this class, both cases just became a
 * client-facing error response with nothing durable recorded anywhere — no way for an operator to
 * later ask "what needs manual migration attention?" across every session that ever hit one.
 *
 * <p>Reuses the same Postgres backend PolyWire already treats as real infrastructure for
 * {@link ConfigStore}'s {@code polywire_config} table, rather than standing up a separate logging
 * store — same "reuse what's already real" reasoning as that class's javadoc.
 *
 * <h2>Schema</h2>
 * <pre>{@code
 * CREATE TABLE polywire_failed_statements (
 *     id                     bigserial PRIMARY KEY,
 *     occurred_at            timestamptz NOT NULL DEFAULT now(),
 *     dialect                text NOT NULL,
 *     sql_text               text NOT NULL,
 *     failure_type           text NOT NULL,
 *     sql_state              text,
 *     native_error_returned  integer,
 *     message                text
 * );
 * }</pre>
 *
 * <p><b>Best-effort, never on the client's critical path</b>: {@link #record} catches and
 * locally logs any failure to write the failure record itself (connection down, schema missing,
 * whatever) rather than letting a logging problem cascade into the client's actual error
 * response — a client that already hit a real failure must still get its error reply either way.
 */
public final class FailedStatementLog {

    private static final Logger log = LoggerFactory.getLogger(FailedStatementLog.class);

    public enum FailureType {
        /** {@code DialectTranslationStage}/{@code DialectTranslations} could not produce valid SQL
         * for the target dialect -- deterministic rules and the LLM fallback both came up empty.
         * We never even sent this to the backend. */
        UNTRANSLATABLE,
        /** We translated successfully and sent real SQL to the backend, and the backend itself
         * rejected it (a real {@link SQLException} with a real SQLState). */
        BACKEND_ERROR
    }

    private final com.polygres.wire.server.ServerOptions options;

    public FailedStatementLog(com.polygres.wire.server.ServerOptions options) {
        this.options = options;
    }

    /** Idempotent -- safe to call on every node's startup. */
    public void ensureSchema() {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_failed_statements ("
                    + "id bigserial PRIMARY KEY, "
                    + "occurred_at timestamptz NOT NULL DEFAULT now(), "
                    + "dialect text NOT NULL, "
                    + "sql_text text NOT NULL, "
                    + "failure_type text NOT NULL, "
                    + "sql_state text, "
                    + "native_error_returned integer, "
                    + "message text)");
        } catch (SQLException e) {
            log.warn("failed-statement log: could not ensure polywire_failed_statements schema exists"
                    + " -- failure recording will keep failing best-effort until this is fixed", e);
        }
    }

    /**
     * Records one failed statement. Never throws -- any problem writing the record itself is
     * caught and logged locally, never propagated, so this can never cause (or mask) the real
     * client-facing error response that triggered it.
     */
    public void record(SourceDialect dialect, String sqlText, FailureType failureType,
            String sqlState, Integer nativeErrorReturned, String message) {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_failed_statements "
                                + "(dialect, sql_text, failure_type, sql_state, native_error_returned, message) "
                                + "VALUES (?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, dialect == null ? null : dialect.name());
            ps.setString(2, sqlText);
            ps.setString(3, failureType.name());
            ps.setString(4, sqlState);
            if (nativeErrorReturned == null) {
                ps.setNull(5, java.sql.Types.INTEGER);
            } else {
                ps.setInt(5, nativeErrorReturned);
            }
            ps.setString(6, message);
            ps.executeUpdate();
        } catch (Exception e) {
            // Best-effort: a logging failure must never cascade into (or mask) the real
            // client-facing error response that triggered this call.
            log.warn("failed-statement log: could not record failure ({}): {}", failureType, e.getMessage());
        }
    }

}
