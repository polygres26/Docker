package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@code WARP_BACKEND_GROUPS} -- the mandatory, exactly-one-group-per-backend
 * partition {@link BackendRegistry#groupInfoFor} exposes, deliberately a SEPARATE mechanism from
 * the older, overlap-permitting {@code WARP_BACKEND_SETS} (see {@link
 * BackendRegistry#UNGROUPED_GROUP_NAME}'s own field javadoc for why).
 */
class BackendGroupsTest {

    private static final String BACKENDS = "pg1=jdbc:postgresql://h:5432/db|u|p"
            + ";pg2=jdbc:postgresql://h:5432/db2|u|p"
            + ";pg3=jdbc:postgresql://h:5432/db3|u|p";

    @Test
    void everyBackendHasAGroupEvenWithNoneDeclared() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null, null, null, null, Map.of());
        for (String name : java.util.List.of("pg1", "pg2", "pg3")) {
            BackendRegistry.BackendGroupInfo info = registry.groupInfoFor(name);
            assertEquals(BackendRegistry.UNGROUPED_GROUP_NAME, info.name());
            assertFalse(info.sharded(), "the synthetic ungrouped group must always be plain");
        }
    }

    @Test
    void aDeclaredGroupDefaultsToPlain() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null, null,
                "warehouse=pg1,pg2", null, Map.of());
        assertEquals(new BackendRegistry.BackendGroupInfo("warehouse", false), registry.groupInfoFor("pg1"));
        assertEquals(new BackendRegistry.BackendGroupInfo("warehouse", false), registry.groupInfoFor("pg2"));
        assertEquals(BackendRegistry.UNGROUPED_GROUP_NAME, registry.groupInfoFor("pg3").name());
    }

    @Test
    void aShardedQualifierMarksTheWholeGroupSharded() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null, null,
                "orders_shards:sharded=pg1,pg2", null, Map.of());
        assertTrue(registry.groupInfoFor("pg1").sharded());
        assertTrue(registry.groupInfoFor("pg2").sharded());
        assertEquals("orders_shards", registry.groupInfoFor("pg1").name());
    }

    @Test
    void aBackendInTwoDeclaredGroupsIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> BackendRegistry.fromConfig(BACKENDS, null, null,
                        "a=pg1,pg2|b=pg2,pg3", null, Map.of()));
        assertTrue(e.getMessage().contains("pg2") && e.getMessage().contains("exactly one group"),
                "got: " + e.getMessage());
    }

    @Test
    void anUnknownQualifierIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> BackendRegistry.fromConfig(BACKENDS, null, null, "g:bogus=pg1", null, Map.of()));
    }

    @Test
    void reusingTheReservedUngroupedNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> BackendRegistry.fromConfig(BACKENDS, null, null,
                        BackendRegistry.UNGROUPED_GROUP_NAME + "=pg1", null, Map.of()));
    }

    @Test
    void warpBackendSetsAndWarpBackendGroupsAreIndependentMechanisms() {
        // The SAME overlapping-membership shape WARP_BACKEND_SETS explicitly allows (see
        // RouterStageBackendSetExpansionTest) must NOT be rejected just because WARP_BACKEND_GROUPS
        // also happens to be configured in the same call -- they don't interact at all.
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null,
                "pair-a=pg1,pg2|pair-b=pg2,pg3", "solo=pg1", null, Map.of());
        assertEquals(java.util.List.of("pg1", "pg2"), registry.backendSets().get("pair-a"));
        assertEquals(java.util.List.of("pg2", "pg3"), registry.backendSets().get("pair-b"));
        assertEquals("solo", registry.groupInfoFor("pg1").name());
        assertEquals(BackendRegistry.UNGROUPED_GROUP_NAME, registry.groupInfoFor("pg2").name());
    }
}
