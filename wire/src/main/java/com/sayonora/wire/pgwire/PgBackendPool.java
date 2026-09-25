package com.sayonora.wire.pgwire;

import com.sayonora.wire.core.LazyPooledConnection;
import com.sayonora.wire.core.SessionConnectionLease;
import com.sayonora.wire.orawire.frontend.ConnectDescriptor;
import com.sayonora.wire.server.ServerOptions;

public final class PgBackendPool {

    private final ServerOptions options;

    public PgBackendPool(ServerOptions options) {
        this.options = options;
    }

    public LazyPooledConnection borrowConnection(ConnectDescriptor descriptor, String username) {
        
        // openForSession (not open): the tenant search_path / db_emulation this session applies are reconciled per
        // physical connection by the session itself, so leftovers must not be stripped on every borrow.
        return new LazyPooledConnection(() -> PgConnections.openForSession(options), username,
                PgConnections.evictor(options), SessionConnectionLease.multiplexingEnabledByEnv());
    }
}
