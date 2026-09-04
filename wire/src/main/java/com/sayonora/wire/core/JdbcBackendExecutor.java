package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcBackendExecutor implements BackendExecutor {

    private static final Logger log = LoggerFactory.getLogger(JdbcBackendExecutor.class);

    private Connection connection;
    private final com.sayonora.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer;

    /** Real, found-live optimization: {@code execute()} used to {@code prepareStatement}/close a
     * brand-new {@link PreparedStatement} on every single call, even for the exact same SQL text
     * repeated hundreds of times (e.g. a hot parameterized {@code SELECT ... WHERE id = ?}) --
     * which meant pgjdbc's own server-side prepare (activated only once the SAME {@code
     * PreparedStatement} object has actually been executed {@code prepareThreshold} times, 5 by
     * default) could never trigger: every call started a fresh object with its own
     * execution-count back at zero. Postgres re-parsed and re-planned the identical statement
     * from scratch on every call, forever.
     *
     * <p>Fixed by keeping one {@code PreparedStatement} per (connection, exact SQL text) and
     * reusing it across calls -- {@code bindParams} still get rebound fresh every time via {@code
     * setObject} in {@link #executeOnPreparedStatement}, only the parse/plan is what gets reused.
     * Bounded to {@link #STATEMENT_CACHE_MAX_SIZE} entries, LRU-evicted, closing the evicted
     * statement -- an unbounded cache would leak server-side prepared statements for genuinely
     * one-off/ad-hoc SQL (e.g. a client that embeds literal values directly in the text instead of
     * binding them, so every call is a distinct string) that's never repeated. On any {@link
     * SQLException} from execution, the offending entry is evicted and closed rather than kept --
     * defensive: a statement object that just failed isn't assumed safe to hand back out. {@link
     * #rebind} (a failover/reconnect) closes and clears every cached entry, since a {@code
     * PreparedStatement} is only ever valid against the connection that created it. */
    private static final int STATEMENT_CACHE_MAX_SIZE = 256;
    private final Map<String, PreparedStatement> statementCache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
            if (size() > STATEMENT_CACHE_MAX_SIZE) {
                closeQuietly(eldest.getValue());
                return true;
            }
            return false;
        }
    };

    public JdbcBackendExecutor(Connection connection) {
        this(connection, null);
    }

    public JdbcBackendExecutor(Connection connection, com.sayonora.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        this.connection = connection;
        this.nativeRlsInitializer = nativeRlsInitializer;
    }

    public void rebind(Connection connection) {
        closeAllCachedStatements();
        this.connection = connection;
    }

    @Override
    public ExecutionResult execute(Statement statement) throws SQLException {

        if (nativeRlsInitializer != null
                && (!statement.accessContext().isAnonymous() || nativeRlsInitializer.runEvenWhenAnonymous())) {
            nativeRlsInitializer.initialize(connection, statement.accessContext());
        }
        String sqlText = stripTrailingSemicolon(statement.sqlText());
        PreparedStatement stmt = statementCache.get(sqlText);
        if (stmt == null) {
            // RETURN_GENERATED_KEYS is harmless to request unconditionally -- pgjdbc (the only
            // backend driver this executor targets) only appends its RETURNING machinery for
            // INSERT statements; for everything else getGeneratedKeys() just comes back empty and
            // executeOnPreparedStatement()'s own read of it below is a no-op. Real gap this closes:
            // without it, a MySQL/Oracle app relying on the backend to assign an auto-increment /
            // sequence-based primary key got LAST_INSERT_ID()/getGeneratedKeys() == 0 always.
            stmt = connection.prepareStatement(sqlText, java.sql.Statement.RETURN_GENERATED_KEYS);
            statementCache.put(sqlText, stmt);
        }
        try {
            return executeOnPreparedStatement(stmt, statement.bindParams());
        } catch (SQLException e) {
            statementCache.remove(sqlText);
            closeQuietly(stmt);
            throw e;
        }
    }

    private void closeAllCachedStatements() {
        for (PreparedStatement stmt : statementCache.values()) {
            closeQuietly(stmt);
        }
        statementCache.clear();
    }

    private static void closeQuietly(PreparedStatement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            log.debug("jdbc backend executor: failed to close a cached PreparedStatement -- harmless, "
                    + "the connection's own close will reclaim it regardless", e);
        }
    }

    static ExecutionResult executeOnPreparedStatement(PreparedStatement stmt, List<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            stmt.setObject(i + 1, coerce(binds.get(i)));
        }
        boolean hasResultSet = stmt.execute();
        if (hasResultSet) {
            try (ResultSet rs = stmt.getResultSet()) {
                return readResultSet(rs);
            }
        }
        long generatedKey = 0;
        try (ResultSet keys = stmt.getGeneratedKeys()) {
            if (keys.next()) {
                generatedKey = keys.getLong(1);
            }
        } catch (SQLException ignoredNoGeneratedKeysSupport) {
            // Some backends/drivers throw rather than return an empty ResultSet for statements
            // that never asked for generated keys in a way they support -- treat identically to
            // "no generated key", since that's exactly what it means here.
        }
        return ExecutionResult.ofUpdate(Math.max(stmt.getUpdateCount(), 0), generatedKey);
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.stripTrailing();
        return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : sql;
    }

    private static Object coerce(Object value) {
        if (!(value instanceof String s) || s.isEmpty()) {
            return value;
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException ignoredNotAnInteger) {
            
        }
        try {
            return new java.math.BigDecimal(s);
        } catch (NumberFormatException ignoredNotANumber) {
            return s;
        }
    }

    private static ExecutionResult readResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List<ColumnInfo> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new ColumnInfo(md.getColumnLabel(i), md.getColumnType(i), md.getPrecision(i), md.getScale(i),
                    md.getColumnDisplaySize(i), md.isNullable(i) != ResultSetMetaData.columnNoNulls));
        }
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                row.add(rs.wasNull() ? null : materializeLob(value));
            }
            rows.add(row);
        }
        return ExecutionResult.ofQuery(columns, rows);
    }

    /** A CLOB/BLOB column's {@code getObject} returns a real JDBC {@link java.sql.Clob}/{@link
     * java.sql.Blob} locator object, not its content -- every wire protocol's own row-encoding
     * expects a plain value (a String, a byte[], a number) it already knows how to write, not a
     * locator it would have to separately stream. Materializing here, at the one place every
     * protocol's query results pass through, means a real backend CLOB/BLOB column round-trips as
     * plain text/bytes everywhere (orawire included, where {@code RequestLoop.toColumnMetadata}
     * maps {@code Types.CLOB}/{@code Types.BLOB} to VARCHAR2/RAW accordingly) instead of failing
     * or producing a locator's generic {@code toString()} garbage. Deliberately eager/whole-value
     * (no streaming) -- real LOB streaming is a materially larger, separate piece of work; this
     * covers the common case of a LOB column that comfortably fits in memory. */
    private static Object materializeLob(Object value) throws SQLException {
        if (value instanceof java.sql.Clob clob) {
            long length = clob.length();
            return length == 0 ? "" : clob.getSubString(1, (int) length);
        }
        if (value instanceof java.sql.Blob blob) {
            long length = blob.length();
            return length == 0 ? new byte[0] : blob.getBytes(1, (int) length);
        }
        return value;
    }
}
