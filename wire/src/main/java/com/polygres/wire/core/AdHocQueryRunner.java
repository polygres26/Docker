package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, AccessContext accessContext) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, List.of(), accessContext, null);
    }

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, List<Object> bindParams, AccessContext accessContext) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, bindParams, accessContext, null);
    }

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, AccessContext accessContext,
            com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, List.of(), accessContext, nativeRlsInitializer);
    }

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, List<Object> bindParams, AccessContext accessContext,
            com.polygres.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        try {
            backend.setAutoCommit(true);
            StatementPipeline pipeline = new StatementPipeline(sharedStages,
                    new RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(backend, nativeRlsInitializer)));
            Statement statement = new Statement(tenantId, SourceDialect.MCP, sql, bindParams, "default", null, accessContext);
            return Result.ofSuccess(pipeline.execute(statement));
        } catch (SQLException e) {
            return Result.ofError(e);
        }
    }

    private AdHocQueryRunner() {
    }
}
