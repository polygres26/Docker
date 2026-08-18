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
 * <p><b>PPv2 support</b>: when {@code POLYWIRE_ACL_PPV2_ENABLED=true}, {@link PpV2ProtocolNegotiator}
 * already rejected any disallowed connection at the negotiation layer -- before any call could
 * ever reach this interceptor -- and carries the resolved PPv2 source address via
 * {@link GrpcProxyProtocol#PROXIED_REMOTE_ADDRESS} (not {@code Grpc.TRANSPORT_ATTR_REMOTE_ADDR};
 * see that key's javadoc for why). This interceptor re-checks it anyway, defense-in-depth, same as
 * every other double-checked rejection path in this project (e.g. {@code
 * PgWireSessionHandler}'s Extended Query error handling) -- cheap, and correct even if a future
 * change adds another way to reach this interceptor without going through the negotiator. Falls
 * back to the raw {@code Grpc.TRANSPORT_ATTR_REMOTE_ADDR} peer when PPv2 isn't enabled for this
 * listener, same as before PPv2 support existed.
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
