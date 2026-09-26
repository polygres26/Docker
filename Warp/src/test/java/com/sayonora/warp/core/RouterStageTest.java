package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the gap found by re-running the competitive comparison against ShardingSphere
 * after shipping earlier fixes: {@link RouterStage.ValueShardRule} only ever indexed
 * {@code statement.bindParams()}, so a client that sent the sharding value as a plain SQL literal
 * (very common -- psql, simple-query mode, many ORMs) instead of a bind parameter never matched a
 * value-shard rule and silently fell through to the wrong (default) backend. No error, wrong
 * data/wrong shard. {@link RouterStage.ValueShardColumnRule} (routed via
 * {@link ValueShardLiteralMatcher}) closes that gap; these tests exercise it through
 * {@code RouterStage.fromConfig}'s actual config-string parsing and {@code handle()}'s actual
 * routing decision, not the matcher in isolation, so a wiring mistake between the two would fail
 * here too.
 */
class RouterStageTest {

    private static Statement select(String sql) {
        return new Statement("t", SourceDialect.POSTGRES, sql, List.of(), "default", null, AccessContext.ANONYMOUS);
    }

    private static Statement selectWithBinds(String sql, List<Object> binds) {
        return new Statement("t", SourceDialect.POSTGRES, sql, binds, "default", null, AccessContext.ANONYMOUS);
    }

    private static String routedBackend(RouterStage router, Statement statement) throws SQLException {
        String[] captured = new String[1];
        router.handle(statement, s -> {
            captured[0] = s.targetBackend();
            return ExecutionResult.ofQuery(List.of(), List.of());
        });
        return captured[0];
    }

    @Test
    void configWithAPurelyNumericKeyIsTheExistingBindIndexBehaviorUnchanged() {
        RouterStage router = RouterStage.fromConfig(null, null, "0:hash:shardA,shardB", null);
        assertEquals(1, router.valueShardRules().size());
        assertEquals(0, router.valueShardColumnRules().size());
        assertEquals(0, router.valueShardRules().get(0).bindIndex());
    }

    @Test
    void configWithAColumnNameKeyParsesAsALiteralRuleNotABindIndexRule() {
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:hash:shardA,shardB", null);
        assertEquals(0, router.valueShardRules().size());
        assertEquals(1, router.valueShardColumnRules().size());
        assertEquals("tenant_id", router.valueShardColumnRules().get(0).columnName());
    }

    @Test
    void literalTenantIdInSimpleQuerySqlIsRoutedByTheSameStrategyAsABoundValueWouldBe() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:list:shardA=42;shardB=99", null);

        String backend = routedBackend(router, select("SELECT * FROM orders WHERE tenant_id = 42"));

        assertEquals("shardA", backend, "a literal tenant_id=42 (no bind params at all -- simple-query mode) "
                + "must route the same as a bound tenant_id=42 would");
    }

    @Test
    void quotedStringLiteralIsAlsoMatched() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, "region:list:shardA=east;shardB=west", null);

        String backend = routedBackend(router, select("SELECT * FROM orders WHERE region = 'east'"));

        assertEquals("shardA", backend);
    }

    @Test
    void noMatchingLiteralFallsThroughWithoutError() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:list:shardA=42", null);

        // No tenant_id predicate at all in this statement -- must not throw, just fall through to
        // whatever the next rule (or the unambiguous-default fallback, here none configured) says.
        String backend = routedBackend(router, select("SELECT * FROM orders"));

        assertNull(backend);
    }

    @Test
    void aBoundValueStillTakesTheExistingBindIndexPathWhenBothRuleTypesAreConfigured() throws SQLException {
        // Both a legacy numeric bind-index rule and a new column-literal rule configured together
        // (a real deployment migrating between client types) -- the bind-index rule must still work
        // exactly as before for a client that DOES use bind parameters.
        RouterStage router = RouterStage.fromConfig(null, null, "0:list:shardA=42|tenant_id:list:shardA=42", null);

        String backend = routedBackend(router,
                selectWithBinds("SELECT * FROM orders WHERE tenant_id = $1", List.of(42)));

        assertEquals("shardA", backend);
    }

    // -- ValueShardColumnRule / TableShardRule bind-parameter gap (ValueShardLiteralMatcher#findBindValue) --
    //
    // A ValueShardColumnRule/TableShardRule is configured BY COLUMN NAME specifically so an operator
    // doesn't need to know a client's bind ordinal up front -- but until this fix, both rule types
    // only ever matched a LITERAL value in the SQL text (ValueShardLiteralMatcher#findLiteralValue),
    // never a real "column = ?" bind parameter. That's the overwhelming majority of real driver
    // traffic (JDBC PreparedStatement, psycopg2 parameterized queries, any ORM), so a query shaped
    // exactly like the sharding rule is meant to prune never got pruned -- it silently fell through
    // to scatter-gather across every shard member instead. These tests prove the fix through
    // RouterStage.fromConfig's real config parsing and handle()'s real routing decision, same
    // discipline as the literal-path tests above.

    @Test
    void valueShardColumnRuleMatchesABoundParameterNotJustALiteral() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:list:shardA=42;shardB=99", null);

        String backend = routedBackend(router,
                selectWithBinds("SELECT * FROM orders WHERE tenant_id = ?", List.of(42)));

        assertEquals("shardA", backend, "a real prepared-statement bind (tenant_id = ?) must route "
                + "the same as a literal tenant_id = 42 already does");
    }

    @Test
    void valueShardColumnRuleBindMatchUsesThePlaceholdersOwnPositionNotAlwaysTheFirstBind() throws SQLException {
        // The sharding column's own "?" is the SECOND placeholder in this statement -- proves the
        // match is positional (counts placeholders left-to-right), not "always bind index 0".
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:list:shardA=42;shardB=99", null);

        String backend = routedBackend(router,
                selectWithBinds("SELECT * FROM orders WHERE status = ? AND tenant_id = ?", List.of("active", 42)));

        assertEquals("shardA", backend);
    }

    @Test
    void tableShardRuleMatchesABoundParameterNotJustALiteral() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:list:customer_id:shardA=42;shardB=99", null);

        String backend = routedBackend(router,
                selectWithBinds("SELECT * FROM orders WHERE customer_id = ?", List.of(42)));

        assertEquals("shardA", backend, "WARP_TABLE_SHARDS must prune to one shard for a real bind "
                + "parameter, not only an inlined literal");
    }

    @Test
    void tableShardRuleStillPrunesOnALiteralUnchanged() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:list:customer_id:shardA=42;shardB=99", null);

        String backend = routedBackend(router, select("SELECT * FROM orders WHERE customer_id = 99"));

        assertEquals("shardB", backend);
    }

    @Test
    void tableShardRuleWithNoMatchingBindOrLiteralScattersAcrossEveryShardInstead() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:list:customer_id:shardA=42;shardB=99", null);

        // A full-table query with no shard-key predicate at all -- correctly can't be pruned to one
        // shard, so it takes TableShardRule's own documented scatter-gather path, not a silent
        // fall-through to null (which would mean "no route decision," wrongly implying no rule
        // matched the table at all).
        String backend = routedBackend(router, select("SELECT count(*) FROM orders"));

        assertEquals(RoutingBackendExecutor.SCATTER_ALL, backend);
    }

    // -- BackendScope enforcement (early check, before QoS/translation/caching) --

    private static Statement scoped(String sql, BackendScope scope) {
        return select(sql).withBackendScope(scope);
    }

    @Test
    void schemaRuleResolvingOutsideTheScopeIsRefusedWith42501() {
        RouterStage router = RouterStage.fromConfig("secrets_db:secrets", null, null, null);
        Statement statement = scoped("SELECT value FROM secrets_db.secrets",
                new BackendScope(java.util.Set.of("backend_b"), "group:team_alpha"));

        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                () -> routedBackend(router, statement));
        assertEquals("42501", e.getSQLState());
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("\"secrets\""), e.getMessage());
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("group:team_alpha"), e.getMessage());
    }

    @Test
    void schemaRuleResolvingInsideTheScopeProceedsNormally() throws SQLException {
        RouterStage router = RouterStage.fromConfig("orders_db:backend_b", null, null, null);
        Statement statement = scoped("SELECT * FROM orders_db.orders", BackendScope.single("backend_b"));

        assertEquals("backend_b", routedBackend(router, statement));
    }

    @Test
    void noRuleFallbackToTheDefaultBackendIsRefusedWhenDefaultIsOutsideTheScope() {
        RouterStage router = RouterStage.fromConfig(null, null, null, null);
        Statement statement = scoped("SELECT * FROM orders", BackendScope.single("backend_b"));

        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                () -> routedBackend(router, statement));
        assertEquals("42501", e.getSQLState());
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("\"default\""), e.getMessage());
    }

    @Test
    void aPreSetPinnedTargetInsideTheScopeIsHonouredUnchanged() throws SQLException {
        RouterStage router = RouterStage.fromConfig("orders_db:elsewhere", null, null, null);
        Statement statement = new Statement("t", SourceDialect.ORACLE, "SELECT * FROM orders_db.orders", List.of(),
                "default", "mcp-native", AccessContext.ANONYMOUS, BackendScope.single("mcp-native"));

        assertEquals("mcp-native", routedBackend(router, statement));
    }

    @Test
    void scatterAllIsNotCheckedByTheRouterItself() throws SQLException {
        RouterStage router = RouterStage.fromConfig(null, null, null, null);
        Statement statement = new Statement("t", SourceDialect.POSTGRES, "SELECT * FROM orders", List.of(),
                "default", RoutingBackendExecutor.SCATTER_ALL, AccessContext.ANONYMOUS, BackendScope.single("backend_b"));

        // The shard membership is only known to RoutingBackendExecutor, which checks each member.
        assertEquals(RoutingBackendExecutor.SCATTER_ALL, routedBackend(router, statement));
    }

    @Test
    void unscopedStatementsAreNeverChecked() throws SQLException {
        RouterStage router = RouterStage.fromConfig("secrets_db:secrets", null, null, null);
        assertEquals("secrets", routedBackend(router, select("SELECT value FROM secrets_db.secrets")));
    }
}
