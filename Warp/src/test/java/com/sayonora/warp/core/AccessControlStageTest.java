package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.access.AccessPolicy;
import com.sayonora.warp.core.access.AccessPolicyYamlConfig;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The real, previously-config-less row-filter/column-masking engine -- now reachable end to end
 * from a YAML policy string exactly as {@code Main} wires it up from {@code WarpConfig#accessPolicy}
 * (see {@code AccessPolicyYamlConfig#parse} + {@code AccessControlStage#reloadPolicy}). */
class AccessControlStageTest {

    private static Statement stmt(String sql, AccessContext ctx) {
        return new Statement("t", SourceDialect.POSTGRES, sql, List.of(), null, null, ctx);
    }

    private static ExecutionResult rows() {
        return ExecutionResult.ofQuery(List.of(new ColumnInfo("v", Types.VARCHAR, 0, 0, 0, false)), List.of(List.of("x")));
    }

    @Test
    void emptyPolicyIsAPureNoOpPassThrough() throws SQLException {
        AccessControlStage stage = new AccessControlStage(AccessPolicy.EMPTY);
        Statement in = stmt("select * from orders", AccessContext.ANONYMOUS);
        boolean[] called = {false};
        stage.handle(in, s -> {
            called[0] = true;
            assertEquals(in, s, "an empty policy must not touch the statement at all");
            return rows();
        });
        assertTrue(called[0]);
    }

    @Test
    void rowFilterInjectsWhereClauseWhenAttributePresent() throws SQLException {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                row_filters:
                  - table_pattern: 'orders'
                    filter_column: tenant_id
                    required_attribute: tenant
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        AccessContext ctx = new AccessContext("alice", java.util.Set.of(), Map.of("tenant", "acme"));
        Statement[] seen = new Statement[1];
        stage.handle(stmt("select * from orders", ctx), s -> {
            seen[0] = s;
            return rows();
        });
        assertTrue(seen[0].sqlText().toLowerCase().contains("tenant_id"), "expected an injected tenant_id filter, got: " + seen[0].sqlText());
        assertTrue(seen[0].bindParams().contains("acme"), "expected the filter value bound as a param, got: " + seen[0].bindParams());
    }

    @Test
    void rowFilterBypassedByRoleLeavesSqlUnchanged() throws SQLException {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                row_filters:
                  - table_pattern: 'orders'
                    filter_column: tenant_id
                    required_attribute: tenant
                    bypass_roles: [admin]
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        AccessContext admin = new AccessContext("root", java.util.Set.of("admin"), Map.of());
        Statement original = stmt("select * from orders", admin);
        Statement[] seen = new Statement[1];
        stage.handle(original, s -> {
            seen[0] = s;
            return rows();
        });
        assertEquals(original.sqlText(), seen[0].sqlText());
    }

    @Test
    void rowFilterFailsClosedWhenRequiredAttributeMissing() {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                row_filters:
                  - table_pattern: 'orders'
                    filter_column: tenant_id
                    required_attribute: tenant
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        Statement in = stmt("select * from orders", new AccessContext("bob", java.util.Set.of(), Map.of()));
        SQLException e = assertThrows(SQLException.class, () -> stage.handle(in, s -> rows()));
        assertEquals("42501", e.getSQLState());
    }

    @Test
    void columnGrantMasksWhenConfiguredToMaskInsteadOfDeny() throws SQLException {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                column_grants:
                  - table_pattern: 'employees'
                    columns: [ssn]
                    required_attribute: clearance
                    allowed_values: [high]
                    on_violation: mask
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        Statement in = stmt("select ssn from employees", new AccessContext("bob", java.util.Set.of(), Map.of()));
        Statement[] seen = new Statement[1];
        stage.handle(in, s -> {
            seen[0] = s;
            return rows();
        });
        assertTrue(seen[0].sqlText().toLowerCase().contains("null"), "expected ssn to be masked to NULL, got: " + seen[0].sqlText());
    }

    @Test
    void columnGrantDeniesWhenEntitlementMissingAndOnViolationIsDeny() {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                column_grants:
                  - table_pattern: 'employees'
                    columns: [ssn]
                    required_attribute: clearance
                    allowed_values: [high]
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        Statement in = stmt("select ssn from employees", new AccessContext("bob", java.util.Set.of(), Map.of()));
        SQLException e = assertThrows(SQLException.class, () -> stage.handle(in, s -> rows()));
        assertEquals("42501", e.getSQLState());
    }

    @Test
    void columnGrantSatisfiedByAttributeLeavesSqlUnchanged() throws SQLException {
        AccessPolicy policy = AccessPolicyYamlConfig.parse("""
                column_grants:
                  - table_pattern: 'employees'
                    columns: [ssn]
                    required_attribute: clearance
                    allowed_values: [high]
                """);
        AccessControlStage stage = new AccessControlStage(policy);
        Statement in = stmt("select ssn from employees", new AccessContext("carol", java.util.Set.of(), Map.of("clearance", "high")));
        Statement[] seen = new Statement[1];
        stage.handle(in, s -> {
            seen[0] = s;
            return rows();
        });
        assertEquals(in.sqlText(), seen[0].sqlText());
    }

    @Test
    void reloadPolicyTakesEffectImmediatelyForTheNextStatement() throws SQLException {
        AccessControlStage stage = new AccessControlStage(AccessPolicy.EMPTY);
        stage.reloadPolicy(AccessPolicyYamlConfig.parse("""
                row_filters:
                  - table_pattern: 'orders'
                    filter_column: tenant_id
                    required_attribute: tenant
                """));
        Statement in = stmt("select * from orders", new AccessContext("dave", java.util.Set.of(), Map.of("tenant", "beta")));
        Statement[] seen = new Statement[1];
        stage.handle(in, s -> {
            seen[0] = s;
            return rows();
        });
        assertTrue(seen[0].bindParams().contains("beta"), "expected the filter value bound as a param, got: " + seen[0].bindParams());
    }
}
