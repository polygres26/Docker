package com.polygres.wire.jdbc;

import com.polygres.wire.grpc.proto.QueryServiceGrpc;
import io.grpc.ManagedChannel;
import java.lang.reflect.Proxy;
import java.sql.Connection;

/** A {@link Connection} proxy: every "statement" over this connection shares the same gRPC channel/stub. */
final class PolyWireConnection {

    static Connection create(ManagedChannel channel, String username, String password) {
        QueryServiceGrpc.QueryServiceBlockingStub stub = QueryServiceGrpc.newBlockingStub(channel);
        boolean[] autoCommit = {true};

        UnsupportedInvocationHandler handler = new UnsupportedInvocationHandler("Connection");
        handler.on("prepareStatement", args -> PolyWireStatement.create(stub, username, password, (String) args[0]));
        handler.on("createStatement", args -> PolyWireStatement.create(stub, username, password, null));
        handler.on("close", args -> {
            channel.shutdown();
            return null;
        });
        handler.on("isClosed", args -> channel.isShutdown());
        handler.on("getAutoCommit", args -> autoCommit[0]);
        handler.on("setAutoCommit", args -> {
            autoCommit[0] = (Boolean) args[0];
            return null;
        }); // no server-side transaction state yet — see ARCHITECTURE.md §5.9/§6 Phase 2 (XA/2PC)
        handler.on("commit", args -> null);
        handler.on("rollback", args -> null);

        return (Connection) Proxy.newProxyInstance(
                PolyWireConnection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private PolyWireConnection() {
    }
}
