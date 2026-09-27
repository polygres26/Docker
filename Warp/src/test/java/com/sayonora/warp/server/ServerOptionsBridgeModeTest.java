package com.sayonora.warp.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

/**
 * Additive-only coverage for {@link ServerOptions.OracleBackendMode#BRIDGE}: the new enum value
 * exists alongside JDBC/NATIVE without disturbing either, and {@link
 * ServerOptions#oracleBridgePoolSize()} has the documented default. (Parsing
 * {@code WARP_ORACLE_BACKEND_MODE} itself is exercised indirectly by the existing
 * {@code ServerOptions.parse}/{@code ServerOptions.defaultOptions} call sites -- this class avoids
 * mutating process environment variables, which {@code System.getenv} does not support overriding
 * in-process without a native-image-specific trick this codebase does not otherwise use.)
 */
final class ServerOptionsBridgeModeTest {

    @Test
    void bridgeIsADistinctThirdValueFromJdbcAndNative() {
        assertNotEquals(ServerOptions.OracleBackendMode.JDBC, ServerOptions.OracleBackendMode.BRIDGE);
        assertNotEquals(ServerOptions.OracleBackendMode.NATIVE, ServerOptions.OracleBackendMode.BRIDGE);
        assertEquals(3, ServerOptions.OracleBackendMode.values().length);
    }

    @Test
    void defaultOracleBridgePoolSizeIsTen() {
        assertEquals(10, ServerOptions.oracleBridgePoolSize());
    }
}
