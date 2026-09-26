package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BackendScopeTest {

    @Test
    void permitsExactMembersOnly() {
        BackendScope scope = new BackendScope(Set.of("backend_b", "backend_c"), "group:team");
        assertTrue(scope.permits("backend_b"));
        assertTrue(scope.permits("backend_c"));
        assertFalse(scope.permits("default"));
        assertFalse(scope.permits("backend_a"));
    }

    @Test
    void nullAndBlankNamesAreCheckedAsTheDefaultBackend() {
        assertTrue(BackendScope.single("default").permits(null));
        assertTrue(BackendScope.single("default").permits(""));
        assertFalse(BackendScope.single("backend_b").permits(null));
        assertFalse(BackendScope.single("backend_b").permits(" "));
    }

    @Test
    void checkThrows42501NamingBothTheBackendAndTheScopeLabel() {
        BackendScope scope = new BackendScope(Set.of("backend_b"), "group:team_alpha");
        SQLException e = assertThrows(SQLException.class, () -> BackendScope.check(scope, "secrets"));
        assertEquals("42501", e.getSQLState());
        assertTrue(e.getMessage().contains("\"secrets\""), e.getMessage());
        assertTrue(e.getMessage().contains("group:team_alpha"), e.getMessage());
        assertTrue(e.getMessage().contains("caller's scope"), e.getMessage());
    }

    @Test
    void checkOnANullTargetReportsTheDefaultName() {
        SQLException e = assertThrows(SQLException.class,
                () -> BackendScope.check(BackendScope.single("backend_b"), null));
        assertTrue(e.getMessage().contains("\"default\""), e.getMessage());
    }

    @Test
    void nullScopeIsANoOpForAnyName() {
        assertDoesNotThrow(() -> BackendScope.check(null, "anything"));
        assertDoesNotThrow(() -> BackendScope.check(null, null));
    }

    @Test
    void inScopeCheckDoesNotThrow() {
        assertDoesNotThrow(() -> BackendScope.check(BackendScope.single("mcp-native"), "mcp-native"));
    }
}
