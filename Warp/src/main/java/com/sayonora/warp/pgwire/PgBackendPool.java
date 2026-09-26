package com.sayonora.warp.pgwire;

import com.sayonora.warp.core.LazyPooledConnection;
import com.sayonora.warp.core.SessionConnectionLease;
import com.sayonora.warp.orawire.frontend.ConnectDescriptor;
import com.sayonora.warp.server.ServerOptions;

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
