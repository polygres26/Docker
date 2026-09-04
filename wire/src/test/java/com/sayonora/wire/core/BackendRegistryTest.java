package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@code WARP_BACKEND_GROUPS} parsing/validation -- a named, reusable set of
 * backend names an admin defines once (possibly spanning multiple engines: a Postgres, an
 * Oracle, a MySQL, a SQL Server, and a MongoDB backend all in one group) and then references BY
 * NAME wherever a backend list would otherwise be typed out by hand (see
 * {@code RouterStageTest}'s {@code tableShardBackendsField...} tests for the consuming side).
 */
class BackendRegistryTest {

    // Exactly 3 spec-registered backends -- BackendRegistry.fromConfig enforces the Developer-
    // edition backend-count cap (License.DEVELOPER_MAX_BACKENDS == 3) against spec entries before
    // staticExtraTargets, and no WARP_LICENSE_KEY is set in the test environment, so a 4th here
    // would silently be skipped rather than actually testing group validation against it. Three
    // different engines (Postgres/Oracle/MySQL) is already enough to prove a group can mix
    // engines; SQL Server and MongoDB are exercised the same way in
    // RouterStageTest's group-expansion tests, which register backends via a
    // staticExtraTargets-bypassing path instead.
    private static final String BACKENDS = "pg=jdbc:postgresql://h:5432/db|u|p"
            + ";ora=jdbc:oracle:thin:@h:1521:xe|u|p"
            + ";mysql=jdbc:mysql://h:3306/db|u|p";

    @Test
    void aGroupCanMixEnginesAndIsExposedByName() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null,
                "all-engines=pg,ora,mysql", null, Map.of());
        assertEquals(Map.of("all-engines", List.of("pg", "ora", "mysql")), registry.backendGroups());
    }

    @Test
    void multipleGroupsAreIndependent() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null,
                "pair-a=pg,ora|pair-b=ora,mysql", null, Map.of());
        assertEquals(List.of("pg", "ora"), registry.backendGroups().get("pair-a"));
        assertEquals(List.of("ora", "mysql"), registry.backendGroups().get("pair-b"));
    }

    @Test
    void noGroupsConfiguredIsAnEmptyMapNotNull() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null, null, null, Map.of());
        assertTrue(registry.backendGroups().isEmpty());
    }

    @Test
    void aGroupNamingAnUnregisteredBackendFailsLoudAtConstruction() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                BackendRegistry.fromConfig(BACKENDS, null, "bad-group=pg,nonexistent", null, Map.of()));
        assertTrue(e.getMessage().contains("nonexistent"), e.getMessage());
    }

    @Test
    void aGroupNameCollidingWithARealBackendNameFailsLoud() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                BackendRegistry.fromConfig(BACKENDS, null, "pg=ora,mysql", null, Map.of()));
        assertTrue(e.getMessage().contains("pg"), e.getMessage());
    }

    @Test
    void anEntryMissingEqualsFailsLoud() {
        assertThrows(IllegalArgumentException.class, () ->
                BackendRegistry.fromConfig(BACKENDS, null, "not-a-valid-entry", null, Map.of()));
    }

    @Test
    void anEmptyGroupFailsLoud() {
        assertThrows(IllegalArgumentException.class, () ->
                BackendRegistry.fromConfig(BACKENDS, null, "empty=", null, Map.of()));
    }

    @Test
    void reloadPicksUpChangedGroups() {
        BackendRegistry registry = BackendRegistry.fromConfig(BACKENDS, null, "g=pg,ora", null, Map.of());
        assertEquals(List.of("pg", "ora"), registry.backendGroups().get("g"));
        registry.reload(BACKENDS, null, "g=pg,ora,mysql");
        assertEquals(List.of("pg", "ora", "mysql"), registry.backendGroups().get("g"));
    }
}
