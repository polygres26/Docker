package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for letting a {@code WARP_TABLE_SHARDS}/{@code WARP_ROUTER_VALUE_SHARD_RULES}
 * hash/consistent-hash "backends" field name a {@link BackendRegistry} backend GROUP instead of
 * (or alongside) individual backend names -- see {@link RouterStage#expandBackendSets}. Backends
 * are registered via {@code staticExtraTargets} (bypassing {@code BackendRegistry.fromConfig}'s
 * spec-parsing path entirely, WARP_TRUSTED_BACKEND_HOSTS checks and Developer-edition backend cap
 * included) purely so this test can freely register five DIFFERENT-engine backends -- Postgres,
 * Oracle, MySQL, SQL Server, and MongoDB -- the exact scenario a "group my backends" feature
 * exists for, without the count itself becoming what the test is actually about.
 */
class RouterStageBackendSetExpansionTest {

    private static Map<String, BackendTarget> fiveEngineTargets() {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        targets.put("pg", new BackendTarget("pg", "jdbc:postgresql://h:5432/db", "u", "p"));
        targets.put("ora", new BackendTarget("ora", "jdbc:oracle:thin:@h:1521:xe", "u", "p"));
        targets.put("mysql", new BackendTarget("mysql", "jdbc:mysql://h:3306/db", "u", "p"));
        targets.put("mssql", new BackendTarget("mssql", "jdbc:sqlserver://h:1433;databaseName=master", "u", "p"));
        targets.put("mongo", new BackendTarget("mongo", "jdbc:postgresql://h:5432/mongoshadow", "u", "p"));
        return targets;
    }

    private static BackendRegistry registryWithGroup(String groupSpec) {
        return BackendRegistry.fromConfig(null, null, groupSpec, null, fiveEngineTargets());
    }

    private static ShardingStrategy.HashStrategy hashStrategyOf(RouterStage router, String table) {
        return router.tableShardRules().stream()
                .filter(r -> r.tableName().equals(table))
                .findFirst().orElseThrow()
                .strategy() instanceof ShardingStrategy.HashStrategy hash ? hash : null;
    }

    @Test
    void aTableShardHashRuleNamingAGroupExpandsToAllFiveEngines() {
        BackendRegistry registry = registryWithGroup("all-engines=pg,ora,mysql,mssql,mongo");
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:hash:customer_id:all-engines", registry);

        ShardingStrategy.HashStrategy strategy = hashStrategyOf(router, "orders");
        assertEquals(List.of("pg", "ora", "mysql", "mssql", "mongo"), strategy.backends());
    }

    @Test
    void aPlainBackendNameAlongsideAGroupNameExpandsAndDeduplicates() {
        BackendRegistry registry = registryWithGroup("sql-engines=pg,ora,mysql,mssql");
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:hash:customer_id:sql-engines,mongo,pg", registry);

        ShardingStrategy.HashStrategy strategy = hashStrategyOf(router, "orders");
        // "pg" appears both inside the group and again as a literal token -- de-duplicated, first
        // occurrence order kept, "mongo" (not in the group) appended as its own plain backend.
        assertEquals(List.of("pg", "ora", "mysql", "mssql", "mongo"), strategy.backends());
    }

    @Test
    void aPlainBackendNameWithNoMatchingGroupIsUnaffected() {
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:hash:customer_id:pg,ora", registryWithGroup(null));

        ShardingStrategy.HashStrategy strategy = hashStrategyOf(router, "orders");
        assertEquals(List.of("pg", "ora"), strategy.backends());
    }

    @Test
    void groupExpansionAppliesToConsistentHashToo() {
        BackendRegistry registry = registryWithGroup("all-engines=pg,ora,mysql,mssql,mongo");
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:consistent:customer_id:all-engines", registry);

        assertTrue(router.tableShardRules().get(0).strategy() instanceof ShardingStrategy.ConsistentHashStrategy);
    }

    @Test
    void groupExpansionDoesNotApplyToListRangeOrDateStrategiesWhoseFieldNamesOneBackendPerEntry() {
        // "all-engines" here is deliberately used as a literal VALUE, not a backend list -- list/
        // range/date strategies name exactly one backend per value/range entry, so there's no
        // "whole field is a backend set" position for group expansion to act on. This just proves
        // expandBackendSets is scoped to hash/consistent and never touches this grammar at all.
        BackendRegistry registry = registryWithGroup("all-engines=pg,ora,mysql,mssql,mongo");
        RouterStage router = RouterStage.fromConfig(null, null, null, null,
                "orders:list:region:pg=east,west", registry);

        assertTrue(router.tableShardRules().get(0).strategy() instanceof ShardingStrategy.ListStrategy);
    }

    @Test
    void groupExpansionAlsoAppliesToValueShardRulesHashStrategy() {
        BackendRegistry registry = registryWithGroup("all-engines=pg,ora,mysql,mssql,mongo");
        RouterStage router = RouterStage.fromConfig(null, null, "tenant_id:hash:all-engines", null, null, registry);

        ShardingStrategy.HashStrategy strategy =
                router.valueShardColumnRules().get(0).strategy() instanceof ShardingStrategy.HashStrategy hash
                        ? hash : null;
        assertEquals(List.of("pg", "ora", "mysql", "mssql", "mongo"), strategy.backends());
    }
}
