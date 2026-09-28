package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A connection routed at connect time must never reach a backend outside its route, through ANY path
 * the pipeline has: an unqualified statement, a rule-routed one, scatter-gather, a cursor, a prepared
 * statement (just a Statement with binds), and a fallback to the global default. Backends are
 * unreachable hosts: every refusal below happens BEFORE a connection would be opened.
 */
class ConnectionRouteEnforcementTest {

    private static final class CountingExecutor implements BackendExecutor {
        int calls;

        @Override
        public ExecutionResult execute(Statement statement) {
            calls++;
            return ExecutionResult.ofQuery(List.of(), List.of());
        }
    }

    private static BackendRegistry registry() {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        for (String n : List.of("default", "a1", "a2", "b1", "b2")) {
            targets.put(n, new BackendTarget(n, "jdbc:postgresql://unreachable.invalid:1/" + n, "u", "p"));
        }
        return BackendRegistry.fromConfig(null, "a1,b1", null, "seta:plain=a1,a2|setb:plain=b1,b2", null, targets);
    }

    private static Statement base(String sql, List<Object> binds) {
        return Statement.of(SourceDialect.POSTGRES, sql, binds);
    }

    private static ConnectionRoute route(BackendRegistry r, String name) {
        return new ConnectionRouter(r, ConnectionRouter.Mode.STRICT).resolve("postgres", name, "u");
    }

    @Test
    void aBackendRouteRefusesEveryTargetButItsOwn() {
        BackendRegistry reg = registry();
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(reg, supplied);
        ConnectionRoute a1 = route(reg, "a1");

        for (String other : List.of("a2", "b1", "b2", "default")) {
            SQLException e = assertThrows(SQLException.class, () -> executor.execute(
                    a1.apply(base("SELECT 1", List.of())).withRouting("default", other)), other);
            assertEquals("42501", e.getSQLState(), other);
        }
        // prepared statement: same Statement with binds, same enforcement
        assertThrows(SQLException.class, () -> executor.execute(
                a1.apply(base("SELECT * FROM t WHERE id = ?", List.of(7))).withRouting("default", "b1")));
        // scatter across a shard group that contains a member outside the route
        SQLException scatter = assertThrows(SQLException.class, () -> executor.execute(
                a1.apply(base("SELECT * FROM orders", List.of())).withRouting("default", RoutingBackendExecutor.SCATTER_ALL)));
        assertEquals("42501", scatter.getSQLState());
        assertEquals(0, supplied.calls);
    }

    @Test
    void aBackendRoutePinsAnUnqualifiedStatementToItsBackendNotTheDefault() {
        BackendRegistry reg = registry();
        Statement pinned = route(reg, "b2").apply(base("SELECT 1", List.of()));
        assertEquals("b2", pinned.targetBackend());
        // the pin survives RouterStage: no rule is consulted
        RouterStage router = new RouterStage(List.of(), List.of(), List.of(), List.of(), List.of(), reg);
        String[] seen = new String[1];
        assertThrows(SQLException.class, () -> router.handle(pinned.withRouting("default", "a1"), s -> {
            seen[0] = s.targetBackend();
            return ExecutionResult.ofQuery(List.of(), List.of());
        }), "a rule-style redirect to a1 is refused early by RouterStage under a b2 route");
        assertEquals(null, seen[0]);
    }

    @Test
    void aSetRouteFallsBackToTheSetsOwnDefaultNotTheGlobalDefault() throws SQLException {
        BackendRegistry reg = registry();
        RouterStage router = new RouterStage(List.of(), List.of(), List.of(), List.of(), List.of(), reg);
        String[] seen = new String[1];
        router.handle(route(reg, "setb").apply(base("SELECT 1", List.of())), s -> {
            seen[0] = s.targetBackend();
            return ExecutionResult.ofQuery(List.of(), List.of());
        });
        assertEquals("b1", seen[0], "no rule matched: the first member of the set, never \"default\"");
    }

    @Test
    void aSetRouteRefusesMembersOfOtherSetsAndScatterAcrossThem() {
        BackendRegistry reg = registry();
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(reg, supplied);
        ConnectionRoute seta = route(reg, "seta");

        assertThrows(SQLException.class, () -> executor.execute(
                seta.apply(base("SELECT 1", List.of())).withRouting("default", "b1")));
        // shard group a1,b1 spans both sets: scatter under a set route is refused, not silently narrowed
        SQLException e = assertThrows(SQLException.class, () -> executor.execute(
                seta.apply(base("SELECT * FROM orders", List.of())).withRouting("default", RoutingBackendExecutor.SCATTER_ALL)));
        assertEquals("42501", e.getSQLState());
        assertTrue(e.getMessage().contains("\"b1\""), e.getMessage());
        assertEquals(0, supplied.calls);
    }

    @Test
    void aTargetlessStatementUnderASetRouteRunsOnTheSetsDefaultNotTheSuppliedConnection() {
        BackendRegistry reg = registry();
        CountingExecutor supplied = new CountingExecutor();
        RoutingBackendExecutor executor = new RoutingBackendExecutor(reg, supplied);
        // RouterStage absent from the chain: the executor still must not fall onto the default connection
        assertThrows(Exception.class, () -> executor.execute(route(reg, "setb").apply(base("SELECT 1", List.of()))));
        assertEquals(0, supplied.calls, "went to b1 (unreachable here), never to the supplied default executor");
    }

    @Test
    void aSetContainingDefaultKeepsTheNaturalDefaultAndNeedsNoOverride() {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        for (String n : List.of("default", "x1")) {
            targets.put(n, new BackendTarget(n, "jdbc:postgresql://unreachable.invalid:1/" + n, "u", "p"));
        }
        BackendRegistry reg = BackendRegistry.fromConfig(null, null, null, "main:plain=default,x1", null, targets);
        assertEquals(null, route(reg, "main").scope().defaultBackend());
        assertTrue(route(reg, "main").scope().permits("default"));
    }
}
