package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * The "run one statement through the real pipeline, on demand, outside any wire-protocol
 * frontend" pattern — originally inline in {@code ApiRoutes.query} (backing {@code /api/query} in
 * the admin console), pulled out so the MCP frontend's {@code run_query} tool
 * ({@code com.polygres.wire.mcp.PolyWireMcpServer}) can reuse the exact same execution path instead of
 * re-deriving it. Both callers still open their own {@link Connection} (a borrow from the default
 * backend pool) and format the result their own way — this only owns the shared middle: build a
 * one-shot {@link StatementPipeline}, run the statement, normalize success/failure into one shape.
 */
public final class AdHocQueryRunner {

    public record Result(boolean success, boolean isQuery, List<String> columns, List<List<Object>> rows,
            long updateCount, String sqlState, String error) {

        public static Result ofSuccess(ExecutionResult execResult) {
            return new Result(true, execResult.isQuery(), execResult.columnNames(), execResult.rows(),
                    execResult.updateCount(), null, null);
        }

        public static Result ofError(SQLException e) {
            return new Result(false, false, List.of(), List.of(), 0,
                    e.getSQLState() == null ? "58000" : e.getSQLState(),
                    e.getMessage() == null ? "backend error" : e.getMessage());
        }
    }

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, AccessContext.ANONYMOUS);
    }

    /** {@code accessContext}: the end user calling {@code /api/query} (see {@code AccessContextResolver}) — carried onto the {@link Statement} for {@code AccessControlStage} to enforce against. */
    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, AccessContext accessContext) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, accessContext, null);
    }

    /** {@code nativeRlsInitializer}: §3.6 of the design doc — non-null enables native backend RLS/VPD pass-through instead of (in addition to) {@code AccessControlStage}'s app-level SQL rewriting; see {@code JdbcBackendExecutor}'s javadoc. */
    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, AccessContext accessContext,
            com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        try {
            backend.setAutoCommit(true);
            StatementPipeline pipeline = new StatementPipeline(sharedStages,
                    new RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(backend, nativeRlsInitializer)));
            Statement statement = new Statement(tenantId, SourceDialect.POLYWIRE_NATIVE, sql, List.of(), "default", null, accessContext);
            return Result.ofSuccess(pipeline.execute(statement));
        } catch (SQLException e) {
            return Result.ofError(e);
        }
    }

    private AdHocQueryRunner() {
    }
}
