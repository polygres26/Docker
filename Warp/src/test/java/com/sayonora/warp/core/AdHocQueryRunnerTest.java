package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Zero-change guarantee for {@link AdHocQueryRunner}'s pre-existing overloads (what MCP's
 * Postgres + ALL-scope path and every other existing caller use): they must build the exact
 * Statement they always did -- tenant as given, {@link SourceDialect#MCP}, no pinned target, no
 * {@link BackendScope} -- and the full overload must carry pin/dialect/scope through.
 */
class AdHocQueryRunnerTest {

    /** A Connection that accepts setAutoCommit and nothing else -- the runner never touches it
     * when the capturing stage below short-circuits the pipeline. */
    private static Connection inertConnection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("setAutoCommit")) {
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class CapturingStage implements PipelineStage {
        Statement seen;

        @Override
        public ExecutionResult handle(Statement statement, PipelineChain next) {
            seen = statement;
            return ExecutionResult.ofQuery(List.of(), List.of());
        }
    }

    private static BackendRegistry emptyRegistry() {
        return BackendRegistry.fromConfig(null, null);
    }

    @Test
    void legacyOverloadBuildsAnUnpinnedUnscopedMcpDialectStatement() {
        CapturingStage stage = new CapturingStage();

        AdHocQueryRunner.Result result = AdHocQueryRunner.run(inertConnection(), List.of(stage), emptyRegistry(),
                "default", "SELECT 1", List.of(7), AccessContext.ANONYMOUS);

        assertTrue(result.success());
        assertEquals("default", stage.seen.tenantId());
        assertEquals(SourceDialect.MCP, stage.seen.sourceDialect());
        assertEquals("SELECT 1", stage.seen.sqlText());
        assertEquals(List.of(7), stage.seen.bindParams());
        assertNull(stage.seen.targetBackend());
        assertNull(stage.seen.backendScope());
    }

    @Test
    void fullOverloadCarriesPinDialectAndScope() {
        CapturingStage stage = new CapturingStage();
        BackendScope scope = BackendScope.single("mcp-native");

        AdHocQueryRunner.run(inertConnection(), List.of(stage), emptyRegistry(), "default", "SELECT 1 FROM dual",
                List.of(), AccessContext.ANONYMOUS, null, "mcp-native", SourceDialect.ORACLE, scope);

        assertEquals("mcp-native", stage.seen.targetBackend());
        assertEquals(SourceDialect.ORACLE, stage.seen.sourceDialect());
        assertEquals(scope, stage.seen.backendScope());
    }

    @Test
    void aScopeRefusalSurfacesAsANormalErrorResultWith42501() {
        // No stages: the terminal RoutingBackendExecutor's own check is what refuses this.
        AdHocQueryRunner.Result result = AdHocQueryRunner.run(inertConnection(), List.of(), emptyRegistry(),
                "default", "SELECT 1", List.of(), AccessContext.ANONYMOUS, null, "secrets", SourceDialect.MCP,
                BackendScope.single("backend_b"));

        assertTrue(!result.success());
        assertEquals("42501", result.sqlState());
        assertTrue(result.error().contains("\"secrets\""), result.error());
    }
}
