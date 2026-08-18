package com.polygres.wire.grpc;

import com.polygres.wire.acl.ClientAcl;
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

/**
 * {@link ClientAcl} for gRPC -- same rules as every TCP frontend
 * ({@code com.polygres.wire.acl.ConnectionGate}), but enforced per-call via {@link ServerInterceptor}
 * rather than at socket-accept time.
 *
 * <p><b>Why an interceptor, not a raw-socket gate like the other frontends</b>: gRPC's own accept
 * path is grpc-netty-shaded's, not the plain {@code ServerSocket} loop every other frontend in
 * this project uses (see {@code Main}'s {@code acceptXxxLoop} methods) -- there's no equivalent
 * accept-then-construct-session-handler moment to hook the same way.
 * {@code io.grpc.ServerTransportFilter#transportReady} looked like the natural connection-level
 * equivalent, but grpc-core's own bytecode (decompiled directly, not assumed from docs) calls it
 * with no surrounding {@code try/catch} -- throwing from it is not a documented, safe way to abort
 * a transport, so this project doesn't rely on that. A {@link ServerInterceptor} reading
 * {@link Grpc#TRANSPORT_ATTR_REMOTE_ADDR} and closing the call with {@link Status#PERMISSION_DENIED}
 * is real, unambiguously-supported grpc-java API instead -- the practical effect is the same from a
 * rejected client's perspective (every RPC on that connection is refused, immediately, before
 * {@link com.polygres.wire.grpc.QueryServiceImpl} ever sees it), just enforced per-call rather than
 * once at the TCP accept.
 *
 * <p><b>No PPv2 support</b> -- unlike the TCP frontends, this only ever evaluates the raw gRPC
 * transport's own peer address, matching orawire's TLS-listener scope limit for the same
 * PPv2-needs-a-plaintext-leading-byte reason (see {@code Main#acceptOraWireTlsLoop}'s javadoc);
 * gRPC here is always plaintext-or-TLS-terminated-by-PolyWire-itself, same shape.
 */
final class AclInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AclInterceptor.class);

    private final ClientAcl acl;

    AclInterceptor(ClientAcl acl) {
        this.acl = acl;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (acl == ClientAcl.DISABLED) {
            return next.startCall(call, headers);
        }
        SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        InetAddress remoteAddress = remote instanceof InetSocketAddress isa ? isa.getAddress() : null;
        if (remoteAddress == null || !acl.isAllowed(remoteAddress)) {
            log.warn("ACL: rejecting gRPC call from {}", remote);
            call.close(Status.PERMISSION_DENIED.withDescription("connection rejected by ACL"), headers);
            return new ServerCall.Listener<>() {
            };
        }
        return next.startCall(call, headers);
    }
}
