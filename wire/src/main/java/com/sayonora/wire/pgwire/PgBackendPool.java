package com.sayonora.wire.pgwire;

import com.sayonora.wire.core.LazyPooledConnection;
import com.sayonora.wire.orawire.frontend.ConnectDescriptor;
import com.sayonora.wire.server.ServerOptions;

public final class PgBackendPool {

    private final ServerOptions options;

    public PgBackendPool(ServerOptions options) {
        this.options = options;
    }

    public LazyPooledConnection borrowConnection(ConnectDescriptor descriptor, String username) {
        
        return new LazyPooledConnection(() -> PgConnections.open(options), username);
    }
}
