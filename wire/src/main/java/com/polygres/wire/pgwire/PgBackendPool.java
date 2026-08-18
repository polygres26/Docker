package com.polygres.wire.pgwire;

import com.polygres.wire.core.LazyPooledConnection;
import com.polygres.wire.orawire.frontend.ConnectDescriptor;
import com.polygres.wire.server.ServerOptions;

/**
 * Hands out backend connections to Postgres via pgJDBC, for the Oracle
 * frontend's manual-commit session model. Returns a {@link
 * LazyPooledConnection}, not a live {@link java.sql.Connection} — no
 * physical connection is actually borrowed from {@link
 * com.polygres.wire.core.BackendConnectionPools} until the session's first
 * statement runs, and it's returned to the shared pool at each
 * COMMIT/ROLLBACK rather than held for the whole session (mirrors
 * pgadapter's one-JDBC-connection-per-proxied-session model only in spirit
 * now — the physical connection underneath is shared and reused).
 */
public final class PgBackendPool {

    private final ServerOptions options;

    public PgBackendPool(ServerOptions options) {
        this.options = options;
    }

    public LazyPooledConnection borrowConnection(ConnectDescriptor descriptor, String username) {
        // TODO: map descriptor.serviceName() to a target database, and
        // forward the O5LOGON-authenticated credentials instead of a fixed
        // backend user.
        return new LazyPooledConnection(() -> PgConnections.open(options), username);
    }
}
