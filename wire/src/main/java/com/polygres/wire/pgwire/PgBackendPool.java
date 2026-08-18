package com.polygres.wire.pgwire;

import com.polygres.wire.core.LazyPooledConnection;
import com.polygres.wire.orawire.frontend.ConnectDescriptor;
import com.polygres.wire.server.ServerOptions;

public final class PgBackendPool {

    private final ServerOptions options;

    public PgBackendPool(ServerOptions options) {
        this.options = options;
    }

    public LazyPooledConnection borrowConnection(ConnectDescriptor descriptor, String username) {
        
        return new LazyPooledConnection(() -> PgConnections.open(options), username);
    }
}
