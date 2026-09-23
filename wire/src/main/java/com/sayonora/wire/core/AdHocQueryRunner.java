package com.sayonora.wire.core;

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
            com.sayonora.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, List.of(), accessContext, nativeRlsInitializer);
    }

    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, List<Object> bindParams, AccessContext accessContext,
            com.sayonora.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer) {
        return run(backend, sharedStages, backendRegistry, tenantId, sql, bindParams, accessContext, nativeRlsInitializer,
                null, SourceDialect.MCP, null);
    }

    /**
     * The full form: runs {@code sql} through the whole shared pipeline (firewall, capture,
     * federation, router, QoS, translation, rollup, cache, stats, repair) against {@code backend},
     * optionally pinned and scoped.
     *
     * <p>{@code pinnedBackend} (nullable) is the registered name of the backend {@code backend}
     * is a connection to; the Statement's {@code targetBackend} is pre-set to it (so
     * {@link RouterStage} honours it instead of resolving a rule) and
     * {@link RoutingBackendExecutor#withDefaultExecutorBackendName} makes that name run on
     * {@code backend} itself. {@code sourceDialect} should equal the pinned target's real
     * dialect so {@link DialectTranslationStage} no-ops. {@code scope} (nullable) is enforced at
     * every backend-resolution point -- see {@link BackendScope}.
     *
     * <p>Every older overload delegates here with {@code (null, SourceDialect.MCP, null)}: an
     * identical Statement to what they always built, and a {@code defaultExecutorBackendName}
     * of {@code "default"}, so their behavior is byte-for-byte unchanged.
     */
    public static Result run(Connection backend, List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            String tenantId, String sql, List<Object> bindParams, AccessContext accessContext,
            com.sayonora.wire.core.access.NativeRlsSessionInitializer nativeRlsInitializer,
            String pinnedBackend, SourceDialect sourceDialect, BackendScope scope) {
        try {
            backend.setAutoCommit(true);
            List<PipelineStage> stages = scope == null ? sharedStages : withoutCrossBackendCaches(sharedStages);
            StatementPipeline pipeline = new StatementPipeline(stages,
                    new RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(backend, nativeRlsInitializer),
                            null, RouterStage.shardRulesIn(sharedStages), RouterStage.tableShardRulesIn(sharedStages))
                            .withFederationSupport(RouterStage.statisticsStoreIn(sharedStages), RouterStage.planStoreIn(sharedStages))
                            .withDefaultExecutorBackendName(pinnedBackend));
            Statement statement = new Statement(tenantId, sourceDialect, sql, bindParams, "default", pinnedBackend,
                    accessContext, scope);
            return Result.ofSuccess(pipeline.execute(statement));
        } catch (SQLException e) {
            return Result.ofError(e);
        }
    }

    /**
     * Drops {@code CacheStage} from the stage list for a SCOPED statement. Why: CacheStage's
     * result-cache key ({@code tenant|targetBackend|sql|binds|access}) does distinguish the
     * routed backend, but its opt-in single-row cache ({@code RowCache.key(table, pk, sk)}) is
     * keyed on the physical table name alone -- no backend, no tenant -- so a scoped caller could
     * be served a row previously cached from a same-named table on a backend outside its scope.
     * That cache is shared cross-protocol by design (dynamowire GetItem), so rather than change
     * its key here the whole stage is simply skipped for scoped traffic. RollupStage isn't
     * filtered: it checks the rollup definition's own backend against the scope itself (see
     * RollupStage#handle). Unscoped callers (every wire frontend, MCP Postgres+ALL) keep the
     * list untouched.
     */
    private static List<PipelineStage> withoutCrossBackendCaches(List<PipelineStage> sharedStages) {
        List<PipelineStage> filtered = new java.util.ArrayList<>(sharedStages.size());
        for (PipelineStage stage : sharedStages) {
            if (!(stage instanceof com.sayonora.wire.cluster.CacheStage)) {
                filtered.add(stage);
            }
        }
        return filtered;
    }

    private AdHocQueryRunner() {
    }
}
