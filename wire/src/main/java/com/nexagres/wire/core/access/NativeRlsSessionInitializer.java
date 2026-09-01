package com.nexagres.wire.core.access;

import com.nexagres.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

public interface NativeRlsSessionInitializer {

    void initialize(Connection connection, AccessContext accessContext) throws SQLException;

    /**
     * JdbcBackendExecutor skips {@link #initialize} entirely for an anonymous AccessContext
     * (no RBAC/OAuth configured) -- a real, correct optimization for the RLS/VPD-context-
     * propagating initializers, since there's no per-user identity worth setting for an
     * anonymous session. Found live, via a real orawire+sqlcl connection with plain username/
     * password auth (no WARP_AUTH_MODE): db_emulation was silently never applied at all,
     * because that anonymous-session guard sits in front of every initializer indiscriminately,
     * including ours, whose job (making pg_oracle's unqualified names resolve) has nothing to
     * do with per-user identity and must run on every orawire session regardless. Override to
     * {@code true} for exactly that case -- default stays {@code false} so every existing
     * initializer's anonymous-skipping behavior is unchanged.
     */
    default boolean runEvenWhenAnonymous() {
        return false;
    }
}
