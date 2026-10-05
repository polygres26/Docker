package com.sayonora.warp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** The accessPolicy field specifically -- the newest addition to WarpConfig's flat-string-field
 * list (see AccessControlStage/AccessPolicyYamlConfig), verifying it survives the same hand-rolled
 * toJson/fromJson round trip every other field already relies on. */
class WarpConfigTest {

    @Test
    void accessPolicyDefaultsToNull() {
        assertNull(WarpConfig.fromEnvDefaults().accessPolicy());
    }

    @Test
    void withAccessPolicyRoundTripsThroughToJsonFromJson() {
        String yaml = "row_filters:\n  - table_pattern: 'orders'\n    filter_column: tenant_id\n    required_attribute: tenant\n";
        WarpConfig c = WarpConfig.fromEnvDefaults().withAccessPolicy(yaml);
        assertEquals(yaml, c.accessPolicy());

        WarpConfig reloaded = WarpConfig.fromJson(c.toJson());
        assertEquals(yaml, reloaded.accessPolicy());
    }

    @Test
    void withAccessPolicyNullClearsIt() {
        WarpConfig c = WarpConfig.fromEnvDefaults().withAccessPolicy("row_filters: []\n").withAccessPolicy(null);
        assertNull(c.accessPolicy());
    }

    @Test
    void withAccessPolicyLeavesEveryOtherFieldUntouched() {
        WarpConfig base = WarpConfig.fromEnvDefaults().withConnectionRoutes("[{\"x\":1}]");
        WarpConfig updated = base.withAccessPolicy("row_filters: []\n");
        assertEquals(base.connectionRoutes(), updated.connectionRoutes());
        assertEquals(base.cacheTables(), updated.cacheTables());
        assertEquals(base.backends(), updated.backends());
    }
}
