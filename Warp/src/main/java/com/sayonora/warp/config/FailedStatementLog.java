package com.sayonora.warp.config;

import com.sayonora.warp.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FailedStatementLog {

    private static final Logger log = LoggerFactory.getLogger(FailedStatementLog.class);

    public enum FailureType {
        
        UNTRANSLATABLE,
        
        BACKEND_ERROR
    }

    private final com.sayonora.warp.server.ServerOptions options;

    // Both the schema check and every record() now run on ONE shared background thread, never on the caller:
    //  * ensureSchema() used to run a CREATE TABLE IF NOT EXISTS on every new client connection, on the
    //    listener's accept thread (the handlers are constructed there) -- a saturated backend pool froze
    //    accepting for the whole listener. It is now once per process and off-thread.
    //  * record() borrows a backend connection of its own (it cannot use the failing session's: that may be
    //    inside an aborted transaction). Inline, a failing statement on a pinned session would wait for the
    //    pool while the pool is exactly what is exhausted; queued, it never blocks the session, and a full
    //    queue just drops the entry (best-effort by design -- see the catch below).
    // Keyed by backend (host:port/db) so several backends in one JVM each get their table.
    private static final java.util.Set<String> SCHEMA_ENSURED = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final java.util.Set<String> SCHEMA_ATTEMPT_QUEUED = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final ThreadPoolExecutor WRITER = newWriter();

    private static ThreadPoolExecutor newWriter() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1000), r -> {
                    Thread t = new Thread(r, "warp-failed-statement-log");
                    t.setDaemon(true);
                    return t;
                }, new ThreadPoolExecutor.DiscardPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public FailedStatementLog(com.sayonora.warp.server.ServerOptions options) {
        this.options = options;
    }

    /** Idempotent, once per process, never blocks the caller (nor touches the backend pool on it). If the
     * check fails (backend not reachable yet) the next record() retries it. */
    public void ensureSchema() {
        if (SCHEMA_ENSURED.contains(backendKey()) || !SCHEMA_ATTEMPT_QUEUED.add(backendKey())) {
            return;
        }
        WRITER.execute(this::ensureSchemaNow);
    }

    private String backendKey() {
        return options.pgHost() + ":" + options.pgPort() + "/" + options.pgDatabase();
    }

    private void ensureSchemaNow() {
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_failed_statements ("
                    + "id bigserial PRIMARY KEY, "
                    + "occurred_at timestamptz NOT NULL DEFAULT now(), "
                    + "dialect text NOT NULL, "
                    + "sql_text text NOT NULL, "
                    + "failure_type text NOT NULL, "
                    + "sql_state text, "
                    + "native_error_returned integer, "
                    + "message text)");
            SCHEMA_ENSURED.add(backendKey());
        } catch (SQLException e) {
            log.warn("failed-statement log: could not ensure warp_failed_statements schema exists"
                    + " -- failure recording will keep failing best-effort until this is fixed", e);
        } finally {
            SCHEMA_ATTEMPT_QUEUED.remove(backendKey());
        }
    }

    public void record(SourceDialect dialect, String sqlText, FailureType failureType,
            String sqlState, Integer nativeErrorReturned, String message) {
        WRITER.execute(() -> recordNow(dialect, sqlText, failureType, sqlState, nativeErrorReturned, message));
    }

    private void recordNow(SourceDialect dialect, String sqlText, FailureType failureType,
            String sqlState, Integer nativeErrorReturned, String message) {
        if (!SCHEMA_ENSURED.contains(backendKey())) {
            ensureSchemaNow();
        }
        try (Connection conn = com.sayonora.warp.pgwire.PgConnections.open(options);
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
