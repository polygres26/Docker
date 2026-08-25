package com.polygres.wire.core;

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
}
