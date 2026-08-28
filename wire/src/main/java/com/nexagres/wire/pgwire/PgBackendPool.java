package com.nexagres.wire.pgwire;

import com.nexagres.wire.core.LazyPooledConnection;
import com.nexagres.wire.orawire.frontend.ConnectDescriptor;
import com.nexagres.wire.server.ServerOptions;

public final class PgBackendPool {

    private final ServerOptions options;

    public PgBackendPool(ServerOptions options) {
        this.options = options;
    }

    public LazyPooledConnection borrowConnection(ConnectDescriptor descriptor, String username) {
        
        return new LazyPooledConnection(() -> PgConnections.open(options), username);
    }
}
