package com.polygres.wire.grpc;

import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;

/**
 * Not started by {@code Main} at all before this pass — this is the first working gRPC frontend
 * for PolyWire. Plaintext listener on {@link ServerOptions#grpcPort()} plus, when a TLS keystore
 * is configured (same shared {@code POLYWIRE_TLS_KEYSTORE} as orawire/pgwire/mywire), a second
 * TLS listener on {@link ServerOptions#grpcTlsPort()} started via {@link #startTls()}.
 *
 * <p>TLS wiring: grpc-netty-shaded's {@code GrpcSslContexts}/{@code SslContextBuilder} accept a
 * plain {@link KeyManagerFactory} directly ({@code SslContextBuilder.forServer(KeyManagerFactory)})
 * — the same one {@link TlsSupport#buildKeyManagerFactory} builds from the shared PKCS12
 * keystore for the plain-socket protocols' {@code SSLContext}. That means no PEM cert/key
 * extraction or temp files are needed here, even though {@code GrpcSslContexts.forServer} in the
 * gRPC docs is usually shown taking a cert-chain/key-file pair — going through
 * {@code SslContextBuilder} directly instead sidesteps that requirement entirely.
 */
public final class PolyWireGrpcServer {

    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final com.polygres.wire.acl.ConnectionGate connectionGate;
    private final Server server;
    private Server tlsServer;

    public PolyWireGrpcServer(ServerOptions options, List<PipelineStage> sharedStages,
            com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(options, sharedStages, backendRegistry, com.polygres.wire.acl.ConnectionGate.DISABLED);
    }

    /**
     * {@code connectionGate}: same instance every TCP frontend shares -- {@link AclInterceptor}
     * enforces its {@link com.polygres.wire.acl.ClientAcl} per-call (see that class's javadoc for
     * why an interceptor, not an accept-time hook), and when {@link
     * com.polygres.wire.acl.ConnectionGate#ppv2Enabled()} is set, {@link PpV2ProtocolNegotiator}
     * wraps the real plaintext/TLS negotiator to read a PPv2 preamble before any HTTP/2 byte is
     * parsed -- the gRPC analogue of what the TCP frontends' own accept-time PPv2 read does.
     */
    public PolyWireGrpcServer(ServerOptions options, List<PipelineStage> sharedStages,
            com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.acl.ConnectionGate connectionGate) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.connectionGate = connectionGate;
        NettyServerBuilder builder = NettyServerBuilder.forPort(options.grpcPort())
                .addService(new QueryServiceImpl(options, sharedStages, backendRegistry))
                .intercept(new ConnectionLimitInterceptor())
                .intercept(new AclInterceptor(connectionGate.acl()));
        if (connectionGate.ppv2Enabled()) {
            builder.protocolNegotiator(new PpV2ProtocolNegotiator(
                    io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators.serverPlaintext(),
                    connectionGate.acl(), connectionGate.trustedProxies()));
        }
        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
    }

    /** Builds and starts the TLS listener with a real {@link SslContext}. Only call when {@link ServerOptions#tlsEnabled()}. */
    public void startTls() throws IOException, GeneralSecurityException {
        KeyManagerFactory kmf = TlsSupport.buildKeyManagerFactory(options);
        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();
        // PPv2 not supported on gRPC's TLS listener -- real, documented scope limit, not an
        // oversight. Verified live (a real PPv2-primed raw-socket relay in front of a real TLS
        // gRPC client) that wrapping InternalProtocolNegotiators.serverTls(sslContext) in
        // PpV2ProtocolNegotiator the same way the plaintext listener does silently breaks the TLS
        // handshake -- the connection just hangs with no server-side exception logged, unlike
        // every other failure mode in this class which fails loud and fast. Root cause not fully
        // diagnosed (likely ServerTlsHandler's own initialization assumptions being violated by
        // installing it into the pipeline via replace() mid-stream rather than as the pipeline's
        // original first handler) -- rather than ship a feature that silently doesn't work, this
        // listener always uses plain sslContext(), same as before PPv2 support existed, regardless
        // of POLYWIRE_ACL_PPV2_ENABLED. IP/CIDR ClientAcl (via AclInterceptor, using the raw
        // Grpc.TRANSPORT_ATTR_REMOTE_ADDR peer) still applies normally.
        NettyServerBuilder builder = NettyServerBuilder.forPort(options.grpcTlsPort())
                .sslContext(sslContext)
                .addService(new QueryServiceImpl(options, sharedStages, backendRegistry))
                .intercept(new ConnectionLimitInterceptor())
                .intercept(new AclInterceptor(connectionGate.acl()));
        tlsServer = builder.build();
        tlsServer.start();
    }

    public void stop() {
        server.shutdownNow();
        if (tlsServer != null) {
            tlsServer.shutdownNow();
        }
    }
}
