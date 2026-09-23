package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The MCP native-mode contract that lets the shared pipeline govern real Oracle/MySQL/SQL Server
 * SQL: a statement pinned to the registered {@link BackendRegistry#MCP_NATIVE_DEFAULT_NAME}
 * target and tagged with that target's REAL dialect must pass {@link DialectTranslationStage}
 * untouched (its same-dialect no-op), so nothing is ever translated Postgres-ward.
 */
class DialectTranslationStagePinnedNativeTargetTest {

    private static BackendRegistry registryWithNativeOracle() {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        targets.put(BackendRegistry.MCP_NATIVE_DEFAULT_NAME, new BackendTarget(
                BackendRegistry.MCP_NATIVE_DEFAULT_NAME, "jdbc:oracle:thin:@unreachable.invalid:1521/xe", "u", "p"));
        return BackendRegistry.fromConfig(null, null, null, null, targets);
    }

    @Test
    void oracleSqlPinnedToTheOracleNativeTargetIsPassedThroughUnchanged() throws SQLException {
        DialectTranslationStage stage = new DialectTranslationStage(registryWithNativeOracle());
        String oracleSql = "SELECT SYSDATE, NVL(name, 'n/a') FROM widgets WHERE ROWNUM <= 5";
        Statement pinned = new Statement("default", SourceDialect.ORACLE, oracleSql, List.of(), "default",
                BackendRegistry.MCP_NATIVE_DEFAULT_NAME, AccessContext.ANONYMOUS,
                BackendScope.single(BackendRegistry.MCP_NATIVE_DEFAULT_NAME));

        Statement[] seen = new Statement[1];
        stage.handle(pinned, s -> {
            seen[0] = s;
            return ExecutionResult.ofQuery(List.of(), List.of());
        });

        assertSame(pinned, seen[0]);
        assertEquals(oracleSql, seen[0].sqlText());
        assertEquals(BackendScope.single(BackendRegistry.MCP_NATIVE_DEFAULT_NAME), seen[0].backendScope());
    }
}
