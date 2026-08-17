package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic JDBC terminal executor (ARCHITECTURE.md §5.4 "Backend Adapter
 * Layer"): works against any JDBC connection regardless of vendor, since
 * {@link Statement#sqlText()} is expected to already use "?" placeholders
 * and {@link Statement#bindParams()} to already be positional — dialect
 * translation and bind-syntax rewriting happen in earlier pipeline stages,
 * not here.
 */
public final class JdbcBackendExecutor implements BackendExecutor {

    private Connection connection;
    private final com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer;

    public JdbcBackendExecutor(Connection connection) {
        this(connection, null);
    }

    /**
     * {@code nativeRlsInitializer}: when non-null, {@link #execute} calls it before every
     * statement whose {@link Statement#accessContext()} isn't
     * {@link AccessContext#ANONYMOUS} — populates the real backend session's native RLS/VPD
     * context (§3.6 of {@code docs/design/end-user-data-access-security.md}) so the database's
     * own row-security engine enforces from it. {@code null} (the 1-arg constructor) is today's
     * unchanged behavior — this is purely additive, opt-in per executor instance the same way
     * every other optional stage in this codebase is opt-in per config.
     */
    public JdbcBackendExecutor(Connection connection, com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        this.connection = connection;
        this.nativeRlsInitializer = nativeRlsInitializer;
    }

    /**
     * RTT optimization (ARCHITECTURE.md §11): lets a single {@code JdbcBackendExecutor} — and the
     * {@code StatementPipeline}/{@code RoutingBackendExecutor} wrapping it — be built <em>once</em>
     * per client session and reused for every statement, instead of all three being reconstructed
     * fresh per statement just to bind that statement's freshly-borrowed connection. Every
     * session-scoped frontend ({@code pgwire}/{@code mywire}/{@code grpc}/{@code orawire}'s
     * autocommit paths) now borrows a connection, calls this, then runs the same cached pipeline —
     * removing a `StatementPipeline` + `RoutingBackendExecutor` + `JdbcBackendExecutor` allocation
     * and a full stage-chain rebuild on every single statement. Each session-scoped frontend lives
     * in its own package (com.polygres.wire.pgwire/mywire/grpc/orawire), so this has to be public — but
     * it's meant to be called only from that frontend's own per-statement call site, immediately
     * before {@code StatementPipeline.execute}, not from arbitrary code holding a reference.
     */
    public void rebind(Connection connection) {
        this.connection = connection;
    }

    @Override
    public ExecutionResult execute(Statement statement) throws SQLException {
        // FederationStage.stripTrailingSemicolon, reused (same package): a trailing ';' — routine,
        // tolerated as a no-op by every real backend driver here (Oracle/Postgres/MySQL) — is
        // outright rejected by Calcite's own SQL parser ("parse failed: Encountered \";\"..."), the
        // driver behind any GENERIC_REST backend (S3SchemaFactory/RestSchemaFactory), whether
        // reached via FederationStage's own federated connection or, found live, via this plain
        // single-backend path the moment a GENERIC_REST backend is routed to directly (not
        // federated) — NL2SQL routinely appends a trailing ';' out of habit, tolerated everywhere
        // else, so this crashed a query that had nothing else wrong with it. Stripped
        // unconditionally rather than only for GENERIC_REST targets — a harmless no-op for every
        // other backend, and this executor is intentionally dialect-agnostic (see its own javadoc),
        // so branching on dialect here would be new coupling for no real benefit.
        if (nativeRlsInitializer != null && !statement.accessContext().isAnonymous()) {
            nativeRlsInitializer.initialize(connection, statement.accessContext());
        }
        String sqlText = stripTrailingSemicolon(statement.sqlText());
        try (PreparedStatement stmt = connection.prepareStatement(sqlText)) {
            return executeOnPreparedStatement(stmt, statement.bindParams());
        }
    }

    /**
     * Shared bind/execute/read-back logic, factored out so {@code FederationStage}'s embedded-Planner
     * path (see its javadoc on {@code POLYWIRE_FEDERATION_EMBEDDED_PLANNER}) can run a {@code
     * PreparedStatement} it obtained from Calcite's {@code RelRunner} (built from an
     * already-optimized {@code RelNode}, not from parsing SQL text a second time) through the exact
     * same bind-coercion and result-reading code every other backend already uses — no duplicated
     * logic, no second bug surface. Does not close {@code stmt}; the caller owns its lifecycle either
     * way (this method's own {@link #execute} caller uses try-with-resources around the same call).
     */
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
        return ExecutionResult.ofUpdate(Math.max(stmt.getUpdateCount(), 0));
    }

    /**
     * Frontends whose wire format is text-only (the native gRPC driver —
     * ARCHITECTURE.md §5.9) hand every bind value over as a String, having
     * lost the client's original Java type. Binding a numeric-looking
     * String as-is makes pgJDBC send it typed varchar, which Postgres
     * won't implicitly cast against a numeric column in an INSERT/UPDATE
     * — so numeric-looking strings are opportunistically promoted back to
     * Long/BigDecimal here. Frontends that already carry typed values
     * (Oracle's NUMBER/DATE bind decoding) are untouched, since this only
     * fires when the incoming value is a String to begin with.
     */
    /** Ported from {@code FederationStage.stripTrailingSemicolon} (Polywire) — PolyWire has no
     * Calcite-backed GENERIC_REST backend, but this is still a harmless no-op for every real
     * backend driver, so it's kept as a small standalone helper rather than pulled in with the
     * whole (excluded) federation stage. */
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
            // fall through
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
                row.add(rs.wasNull() ? null : value);
            }
            rows.add(row);
        }
        return ExecutionResult.ofQuery(columns, rows);
    }
}
