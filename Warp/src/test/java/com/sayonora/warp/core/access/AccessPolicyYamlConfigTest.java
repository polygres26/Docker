package com.sayonora.warp.core.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The YAML authoring format for {@link AccessPolicy}, now that it's actually reachable from
 * config (see WarpConfig#accessPolicy / AccessControlStage) instead of being parsed only in this
 * test -- this is the format an operator types into the admin UI's policy editor. */
class AccessPolicyYamlConfigTest {

    @Test
    void blankOrNullYamlIsAnEmptyPolicy() {
        assertTrue(AccessPolicyYamlConfig.parse("").isEmpty());
        assertTrue(AccessPolicyYamlConfig.parse("   \n").isEmpty());
    }

    @Test
    void parsesRowFiltersAndColumnGrantsWithAllFields() {
        String yaml = """
                row_filters:
                  - table_pattern: '^orders$'
                    filter_column: tenant_id
                    required_attribute: tenant
                    bypass_roles: [admin, auditor]
                    values_for_unfiltered: [global]
                column_grants:
                  - table_pattern: '^employees$'
                    columns: [ssn, salary]
                    required_attribute: clearance
                    allowed_values: [high]
                    on_violation: mask
                """;
        AccessPolicy policy = AccessPolicyYamlConfig.parse(yaml);
        assertEquals(1, policy.rowFilters().size());
        AccessPolicy.RowFilter rf = policy.rowFilters().get(0);
        assertTrue(rf.tablePattern().matcher("orders").matches());
        assertEquals("tenant_id", rf.filterColumn());
        assertEquals("tenant", rf.requiredAttribute());
        assertEquals(java.util.List.of("admin", "auditor"), rf.bypassRoles());
        assertEquals(java.util.List.of("global"), rf.valuesForUnfiltered());

        assertEquals(1, policy.columnGrants().size());
        AccessPolicy.ColumnGrant cg = policy.columnGrants().get(0);
        assertTrue(cg.tablePattern().matcher("employees").matches());
        assertEquals(java.util.List.of("ssn", "salary"), cg.columns());
        assertEquals("clearance", cg.requiredAttribute());
        assertEquals(java.util.List.of("high"), cg.allowedValues());
        assertEquals(AccessPolicy.OnViolation.MASK, cg.onViolation());
    }

    @Test
    void columnGrantDefaultsToDenyWhenOnViolationOmitted() {
        String yaml = """
                column_grants:
                  - table_pattern: 'employees'
                    columns: [ssn]
                    required_attribute: clearance
                """;
        AccessPolicy.ColumnGrant cg = AccessPolicyYamlConfig.parse(yaml).columnGrants().get(0);
        assertEquals(AccessPolicy.OnViolation.DENY, cg.onViolation());
    }

    @Test
    void missingRequiredFieldFailsCleanlyNotWithANullPointer() {
        String yaml = """
                row_filters:
                  - filter_column: tenant_id
                    required_attribute: tenant
                """;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AccessPolicyYamlConfig.parse(yaml));
        assertTrue(e.getMessage().contains("table_pattern"));
    }

    @Test
    void invalidRegexFailsCleanly() {
        String yaml = """
                row_filters:
                  - table_pattern: '['
                    filter_column: c
                    required_attribute: a
                """;
        assertThrows(java.util.regex.PatternSyntaxException.class, () -> AccessPolicyYamlConfig.parse(yaml));
    }

    @Test
    void notAMappingAtTopLevelFailsCleanly() {
        assertThrows(IllegalArgumentException.class, () -> AccessPolicyYamlConfig.parse("- just\n- a\n- list\n"));
    }
}
