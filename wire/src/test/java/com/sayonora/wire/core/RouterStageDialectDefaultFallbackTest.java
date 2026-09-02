package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link RouterStage}'s generalized no-rule-matched fallback (the change that
 * let mywire/mssqlwire's native-mode session handlers stop hardcoding a
 * {@code Statement.withRouting(_, "mysql-native")}-style pin and instead let {@code RouterStage}
 * resolve every statement the same way, including native ones -- see
 * {@code MySqlWireSessionHandler}/{@code MssqlWireSessionHandler}'s native-mode branches).
 *
 * <p>The fallback, in order: (1) if the statement's source dialect has a RESERVED native-mode
 * default name ({@link BackendRegistry#MYSQL_NATIVE_DEFAULT_NAME} etc.) and that exact name is
 * registered, use it; (2) otherwise fall back to the registered {@code "default"} backend
 * (translate into it), same as every dialect without its own native target has always done.
 *
 * <p>Matching is deliberately by RESERVED NAME, not "the sole backend of a matching dialect" --
 * an earlier version of this fallback tried the latter and was caught (by
 * {@link #aRealMySqlShardRegisteredForAnUnrelatedProtocolDoesNotHijackADifferentDefaultModeSessionOfTheSameDialect()}
 * below, written specifically to catch it) silently rerouting an ordinary default-mode session to
 * an unrelated same-dialect backend an operator registered via {@code WARP_BACKENDS} purely for
 * router-rule-driven sharding on a DIFFERENT protocol (see
 * {@code ShardingAcrossBackendEnginesIntegrationTest}) -- with no rule and no operator intent
 * behind it.
 */
class RouterStageDialectDefaultFallbackTest {

    private static Statement statement(SourceDialect dialect, String sql) {
        return new Statement("t", dialect, sql, List.of(), "default", null, AccessContext.ANONYMOUS);
    }

    private static RouterStage routerOver(BackendRegistry registry) {
        return new RouterStage(List.of(), List.of(), List.of(), List.of(), List.of(), registry);
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
    void solePostgresBackendIsStillTheDefaultForAPostgresSourcedStatement() throws SQLException {
        BackendRegistry registry = new BackendRegistry(
                Map.of("default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p")), List.of());
        assertEquals("default", routedBackend(routerOver(registry), statement(SourceDialect.POSTGRES, "SELECT 1")));
    }

    @Test
    void theReservedMssqlNativeDefaultIsUsedForASqlServerSourcedStatementWithNoRule() throws SQLException {
        BackendRegistry registry = new BackendRegistry(Map.of(
                "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
                BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME, new BackendTarget(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                        "jdbc:sqlserver://h:1433;databaseName=master", "u", "p")
        ), List.of());
        assertEquals(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                routedBackend(routerOver(registry), statement(SourceDialect.SQL_SERVER, "SELECT 1")));
    }

    @Test
    void aPostgresSourcedStatementStillDefaultsToPostgresWhenAnUnrelatedNativeTargetIsAlsoRegistered() throws SQLException {
        BackendRegistry registry = new BackendRegistry(Map.of(
                "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
                BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME, new BackendTarget(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                        "jdbc:sqlserver://h:1433;databaseName=master", "u", "p")
        ), List.of());
        assertEquals("default", routedBackend(routerOver(registry), statement(SourceDialect.POSTGRES, "SELECT 1")));
    }

    @Test
    void multipleNonReservedSqlServerTargetsRequireAnExplicitRuleAndFallBackToDefaultWithoutOne() throws SQLException {
        BackendRegistry registry = new BackendRegistry(Map.of(
                "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
                "mssql-a", new BackendTarget("mssql-a", "jdbc:sqlserver://a:1433;databaseName=master", "u", "p"),
                "mssql-b", new BackendTarget("mssql-b", "jdbc:sqlserver://b:1433;databaseName=master", "u", "p")
        ), List.of());
        // Neither non-reserved target is picked automatically -- no reserved "mssql-native" name
        // is registered here, so this falls back to the shared Postgres default (translated)
        // rather than guessing between two equally-plausible real SQL Server backends.
        assertEquals("default", routedBackend(routerOver(registry), statement(SourceDialect.SQL_SERVER, "SELECT 1")));
    }

    @Test
    void aRealMySqlShardRegisteredForAnUnrelatedProtocolDoesNotHijackADifferentDefaultModeSessionOfTheSameDialect() throws SQLException {
        // The regression this fallback must NOT reintroduce: an operator registers a real MySQL
        // backend under an arbitrary name (here "mysql-shard-2") purely for WARP_TABLE_SHARDS-
        // style sharding reachable via pgwire -- completely unrelated to mywire's OWN native-mode
        // toggle, which is off in this scenario (no "mysql-native" reserved name registered at
        // all). A mywire session in its ordinary default/translating mode still produces
        // SourceDialect.MYSQL statements (mywire always reports MYSQL as its source dialect,
        // native mode or not) -- those must still translate into Postgres, NOT get silently
        // rerouted to "mysql-shard-2" just because it happens to be the only MYSQL-dialect
        // backend registered.
        BackendRegistry registry = new BackendRegistry(Map.of(
                "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
                "mysql-shard-2", new BackendTarget("mysql-shard-2", "jdbc:mysql://h:3306/db", "u", "p")
        ), List.of());
        assertEquals("default", routedBackend(routerOver(registry), statement(SourceDialect.MYSQL, "SELECT 1 FROM accounts")));
    }

    @Test
    void aConfiguredRuleStillTakesPriorityOverTheReservedNativeDefault() throws SQLException {
        BackendRegistry registry = new BackendRegistry(Map.of(
                "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
                BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME, new BackendTarget(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                        "jdbc:sqlserver://h:1433;databaseName=master", "u", "p"),
                "mssql-other", new BackendTarget("mssql-other", "jdbc:sqlserver://o:1433;databaseName=master", "u", "p")
        ), List.of());
        RouterStage router = new RouterStage(
                List.of(new RouterStage.SchemaRule("hr", java.util.regex.Pattern.compile("hr\\.", java.util.regex.Pattern.CASE_INSENSITIVE), "mssql-other")),
                List.of(), List.of(), List.of(), List.of(), registry);
        assertEquals("mssql-other", routedBackend(router, statement(SourceDialect.SQL_SERVER, "SELECT * FROM hr.employees")));
        // A statement the schema rule doesn't match still falls through to the reserved native
        // default ("mssql-native"), not the rule's own backend ("mssql-other").
        assertEquals(BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
                routedBackend(router, statement(SourceDialect.SQL_SERVER, "SELECT * FROM other_table")));
    }
}
