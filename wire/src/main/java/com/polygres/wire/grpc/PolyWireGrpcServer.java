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

    public void startTls() throws IOException, GeneralSecurityException {
        KeyManagerFactory kmf = TlsSupport.buildKeyManagerFactory(options);
        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();
        
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
