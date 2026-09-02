package com.sayonora.wire.grpc;

import io.grpc.Attributes;

final class GrpcProxyProtocol {

    static final Attributes.Key<java.net.InetAddress> PROXIED_REMOTE_ADDRESS =
            Attributes.Key.create("com.sayonora.wire.grpc.proxied-remote-address");

    private GrpcProxyProtocol() {
    }
}
