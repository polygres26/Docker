package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Terminal {@link BackendScope} enforcement in {@link RoutingBackendExecutor}, plus the
 * {@code defaultExecutorBackendName} generalisation of the old "default"-means-supplied-connection
 * special case. Backends are registered as {@code staticExtraTargets} (same reasoning as
 * {@link RouterStageBackendSetExpansionTest}) with unreachable URLs: every assertion below is
 * about a decision made BEFORE any connection would be opened, and a test that accidentally
 * reached {@code BackendConnectionPools} would fail loudly on the bogus host rather than pass.
 */
class RoutingBackendExecutorScopeTest {

    private static final class CountingExecutor implements BackendExecutor {
        int calls;
        Statement last;

        @Override
        public ExecutionResult execute(Statement statement) {
            calls++;
            last = statement;
            return ExecutionResult.ofQuery(List.of(), List.of());
        }
    }

    private static BackendRegistry registry(String shardGroupSpec, String... names) {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        for (String name : names) {
            targets.put(name, new BackendTarget(name, "jdbc:postgresql://unreachable.invalid:1/" + name, "u", "p"));
        }
        return BackendRegistry.fromConfig(null, shardGroupSpec, null, null, targets);
    }

    private static Statement statement(String sql, String target, BackendScope scope) {
        return new Statement("t", SourceDialect.MCP, sql, List.of(), "default", target, AccessContext.ANONYMOUS, scope);
    }

    @Test
    void aTargetEqualToTheDefaultExecutorBackendNameRunsOnTheSuppliedExecutorNotTheRegistry() throws SQLException {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(registry(null, "mcp-native", "default"), supplied)
                .withDefaultExecutorBackendName("mcp-native");

        executor.execute(statement("SELECT 1 FROM dual", "mcp-native", BackendScope.single("mcp-native")));

        assertEquals(1, supplied.calls);
        assertEquals("mcp-native", supplied.last.targetBackend());
    }

    @Test
    void withoutTheOverrideAPinnedNonDefaultTargetWouldGoToTheRegistry() {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(registry(null, "mcp-native", "default"), supplied);

        // Unscoped, so the scope check can't be what stops it: the registry path tries to open a
        // real connection to the bogus host and fails -- proving the override above is what
        // keeps the pinned name on the supplied connection.
        assertThrows(Exception.class, () -> executor.execute(statement("SELECT 1", "mcp-native", null)));
        assertEquals(0, supplied.calls);
    }

    @Test
    void anOutOfScopeConcreteTargetIsRefusedBeforeAnyConnectionIsOpened() {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(registry(null, "backend_b", "secrets"), supplied);

        SQLException e = assertThrows(SQLException.class, () -> executor.execute(
                statement("SELECT value FROM secrets", "secrets", BackendScope.single("backend_b"))));

        assertEquals("42501", e.getSQLState());
        assertTrue(e.getMessage().contains("\"secrets\""), e.getMessage());
        assertEquals(0, supplied.calls);
    }

    @Test
    void aNullTargetIsCheckedAsTheDefaultExecutorBackendName() {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(registry(null, "backend_b", "default"), supplied);

        SQLException e = assertThrows(SQLException.class, () -> executor.execute(
                statement("SELECT 1", null, BackendScope.single("backend_b"))));
        assertEquals("42501", e.getSQLState());
        assertTrue(e.getMessage().contains("\"default\""), e.getMessage());
        assertEquals(0, supplied.calls);
    }

    @Test
    void scatterAllWithAnOutOfScopeShardMemberIsRefusedBeforeAnyShardIsOpened() {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(
                registry("shardA,shardB", "shardA", "shardB"), supplied);

        SQLException e = assertThrows(SQLException.class, () -> executor.execute(statement(
                "SELECT * FROM orders", RoutingBackendExecutor.SCATTER_ALL,
                new BackendScope(Set.of("shardA"), "group:only_a"))));

        assertEquals("42501", e.getSQLState());
        assertTrue(e.getMessage().contains("\"shardB\""), e.getMessage());
        assertEquals(0, supplied.calls);
    }

    @Test
    void unscopedNullTargetStillUsesTheSuppliedExecutorExactlyAsBefore() throws SQLException {
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(registry(null, "default", "other"), supplied);

        executor.execute(statement("SELECT 1", null, null));
        executor.execute(statement("SELECT 1", "default", null));

        assertEquals(2, supplied.calls);
    }
}
