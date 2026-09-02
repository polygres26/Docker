package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Standalone coverage confirming {@link FirewallStage} is exactly as dialect-agnostic as its
 * {@code handle()} implementation looks -- it matches purely on {@code Statement.sqlText()},
 * never inspects {@code sourceDialect()} or {@code targetBackend()}, so the same rule set applies
 * identically whether a statement is headed for the default dialect-translated Postgres backend
 * or a same-dialect native backend (Oracle/MySQL/SQL Server -- see {@code MssqlWireSessionHandler}
 * and {@code MySqlWireSessionHandler}'s native-mode branches, which now route statements through
 * this same stage instead of bypassing it). No standalone unit test existed for this stage's
 * {@code handle()} logic before this; only an HTTP-admin integration test for the unrelated
 * natural-language rule-drafting endpoint did.
 */
class FirewallStageTest {

    private static Statement statement(SourceDialect dialect, String sql, String targetBackend) {
        return new Statement("t", dialect, sql, List.of(), "default", targetBackend, AccessContext.ANONYMOUS);
    }

    private static ExecutionResult proceed(FirewallStage firewall, Statement statement) throws SQLException {
        return firewall.handle(statement, s -> ExecutionResult.ofQuery(List.of(), List.of()));
    }

    @Test
    void denyRuleRejectsAMatchingStatementRegardlessOfSourceDialect() {
        FirewallStage firewall = new FirewallStage(List.of(
                new FirewallStage.Rule(1, 100, FirewallStage.Action.DENY, null, null,
                        Pattern.compile("DROP", Pattern.CASE_INSENSITIVE), "no drops")));

        for (SourceDialect dialect : List.of(SourceDialect.POSTGRES, SourceDialect.ORACLE,
                SourceDialect.MYSQL, SourceDialect.SQL_SERVER)) {
            Statement stmt = statement(dialect, "DROP TABLE accounts", null);
            assertThrows(SQLException.class, () -> proceed(firewall, stmt),
                    "deny rule should reject a DROP regardless of source dialect (" + dialect + ")");
        }
    }

    @Test
    void allowedStatementProceedsRegardlessOfTargetBackendName() throws SQLException {
        FirewallStage firewall = new FirewallStage(List.of(
                new FirewallStage.Rule(1, 100, FirewallStage.Action.DENY, null, null,
                        Pattern.compile("DROP", Pattern.CASE_INSENSITIVE), "no drops")));

        // "mysql-native"/"mssql-native" are the native-backend-mode target names Main.java
        // registers -- confirming the firewall's decision doesn't change based on which backend
        // (Postgres default, or a same-dialect native target) a statement is pinned to.
        for (String targetBackend : new String[] { null, "default", "mysql-native", "mssql-native" }) {
            Statement stmt = statement(SourceDialect.MYSQL, "SELECT 1", targetBackend);
            assertDoesNotThrow(() -> proceed(firewall, stmt));
        }
    }

    @Test
    void stackedQueryIsRejectedRegardlessOfSourceDialect() {
        FirewallStage firewall = new FirewallStage(List.of());
        for (SourceDialect dialect : List.of(SourceDialect.POSTGRES, SourceDialect.SQL_SERVER)) {
            Statement stmt = statement(dialect, "SELECT 1; DROP TABLE accounts", "mssql-native");
            assertThrows(SQLException.class, () -> proceed(firewall, stmt));
        }
    }
}
