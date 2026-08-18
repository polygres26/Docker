package com.polygres.wire.grpc;

import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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
    private final Server server;
    private Server tlsServer;

    public PolyWireGrpcServer(ServerOptions options, List<PipelineStage> sharedStages,
            com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.server = ServerBuilder.forPort(options.grpcPort())
                .addService(new QueryServiceImpl(options, sharedStages, backendRegistry))
                .intercept(new ConnectionLimitInterceptor())
                .build();
    }

    public void start() throws IOException {
        server.start();
    }

    /** Builds and starts the TLS listener with a real {@link SslContext}. Only call when {@link ServerOptions#tlsEnabled()}. */
    public void startTls() throws IOException, GeneralSecurityException {
        KeyManagerFactory kmf = TlsSupport.buildKeyManagerFactory(options);
        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();
        tlsServer = NettyServerBuilder.forPort(options.grpcTlsPort())
                .sslContext(sslContext)
                .addService(new QueryServiceImpl(options, sharedStages, backendRegistry))
                .intercept(new ConnectionLimitInterceptor())
                .build();
        tlsServer.start();
    }

    public void stop() {
        server.shutdownNow();
        if (tlsServer != null) {
            tlsServer.shutdownNow();
        }
    }
}
