package com.sayonora.wire.grpc;

import com.sayonora.wire.acl.ClientAcl;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AclInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AclInterceptor.class);

    private final ClientAcl acl;

    AclInterceptor(ClientAcl acl) {
        this.acl = acl;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!acl.hasRules()) {
            return next.startCall(call, headers);
        }
        InetAddress proxied = call.getAttributes().get(GrpcProxyProtocol.PROXIED_REMOTE_ADDRESS);
        InetAddress remoteAddress = proxied;
        Object loggedPeer = proxied;
        if (remoteAddress == null) {
            SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            remoteAddress = remote instanceof InetSocketAddress isa ? isa.getAddress() : null;
            loggedPeer = remote;
        }
        if (remoteAddress == null || !acl.isAllowed(remoteAddress)) {
            log.warn("ACL: rejecting gRPC call from {}", loggedPeer);
            call.close(Status.PERMISSION_DENIED.withDescription("connection rejected by ACL"), headers);
            return new ServerCall.Listener<>() {
            };
        }
        return next.startCall(call, headers);
    }
}
