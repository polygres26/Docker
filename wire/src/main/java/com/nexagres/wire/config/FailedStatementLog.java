package com.nexagres.wire.config;

import com.nexagres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FailedStatementLog {

    private static final Logger log = LoggerFactory.getLogger(FailedStatementLog.class);

    public enum FailureType {
        
        UNTRANSLATABLE,
        
        BACKEND_ERROR
    }

    private final com.nexagres.wire.server.ServerOptions options;

    public FailedStatementLog(com.nexagres.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() {
        try (Connection conn = com.nexagres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_failed_statements ("
                    + "id bigserial PRIMARY KEY, "
                    + "occurred_at timestamptz NOT NULL DEFAULT now(), "
                    + "dialect text NOT NULL, "
                    + "sql_text text NOT NULL, "
                    + "failure_type text NOT NULL, "
                    + "sql_state text, "
                    + "native_error_returned integer, "
                    + "message text)");
        } catch (SQLException e) {
            log.warn("failed-statement log: could not ensure warp_failed_statements schema exists"
                    + " -- failure recording will keep failing best-effort until this is fixed", e);
        }
    }

    public void record(SourceDialect dialect, String sqlText, FailureType failureType,
            String sqlState, Integer nativeErrorReturned, String message) {
        try (Connection conn = com.nexagres.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO warp_failed_statements "
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
            
            log.warn("failed-statement log: could not record failure ({}): {}", failureType, e.getMessage());
        }
    }

}
