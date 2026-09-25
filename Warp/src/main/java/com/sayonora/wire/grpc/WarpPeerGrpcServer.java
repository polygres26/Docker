package com.sayonora.wire.grpc;

import com.sayonora.wire.server.TlsSupport;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 1a's node-to-node listener for {@code WarpPeerService} -- deliberately a SEPARATE server
 * (own port, own {@code NettyServerBuilder}) from {@code WarpGrpcServer}'s client-facing {@code
 * QueryService}, and deliberately TLS-ONLY with no plaintext variant: this service is inherently
 * trusting another Warp NODE, not authenticating an end user, and there is no safe "unauthenticated
 * peer" mode the way {@code WarpGrpcServer}'s plaintext client port has (that port's trust model --
 * IP allowlist + downstream query auth -- isn't sufficient for handing a peer join data and letting
 * it inject rows back). Real mutual TLS: the SAME keystore file is loaded as both this server's own
 * identity AND its trust store (via {@link TlsSupport#loadMutualTlsManagers}, the same "one file,
 * one shared CA" shape {@code cluster/WarpCluster} already uses for Ignite's peer TLS) with {@link
 * ClientAuth#REQUIRE} -- a connecting peer without a certificate signed by that same CA is rejected
 * at the TLS handshake itself, before any RPC ever reaches {@link WarpPeerServiceImpl}.
 *
 * <p>Deliberately a SEPARATE cert/env-var pair from BOTH Ignite's {@code WARP_TLS_KEYSTORE} and the
 * client-facing gRPC/orawire TLS ({@code ServerOptions.tlsKeystorePath}) -- so the cache cluster,
 * the client-facing endpoint, and this query-execution peer mesh are three independently
 * configured/revocable trust boundaries, never conflated.
 *
 * <p>{@link #fromEnvOrNull()} returns {@code null} (the server simply never starts) when {@code
 * WARP_PEER_TLS_KEYSTORE} isn't set -- the same "absent means the feature doesn't exist" shape
 * {@code WARP_CLUSTER_ENABLED}/{@code SchemaFederationStage.fromConfigOrNull} already use.
 */
public final class WarpPeerGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(WarpPeerGrpcServer.class);

    private final int port;
    private final Server server;

    private WarpPeerGrpcServer(int port, Server server) {
        this.port = port;
        this.server = server;
    }

    public static WarpPeerGrpcServer fromEnvOrNull() throws GeneralSecurityException, IOException {
        String keystorePath = System.getenv("WARP_PEER_TLS_KEYSTORE");
        if (keystorePath == null || keystorePath.isBlank()) {
            return null;
        }
        String keystorePassword = System.getenv("WARP_PEER_TLS_KEYSTORE_PASSWORD");
        int port = parseIntEnv("WARP_PEER_GRPC_PORT", 7072);
        return create(port, keystorePath, keystorePassword);
    }

    public static WarpPeerGrpcServer create(int port, String keystorePath, String keystorePassword)
            throws GeneralSecurityException, IOException {
        TlsSupport.MutualTlsManagers managers = TlsSupport.loadMutualTlsManagers(keystorePath, keystorePassword);
        SslContext sslContext = GrpcSslContexts.configure(
                        SslContextBuilder.forServer(managers.keyManagerFactory())
                                .trustManager(managers.trustManagerFactory())
                                .clientAuth(ClientAuth.REQUIRE))
                .build();
        Server server = NettyServerBuilder.forPort(port)
                .sslContext(sslContext)
                .addService(new WarpPeerServiceImpl())
                .build();
        return new WarpPeerGrpcServer(port, server);
    }

    /** A real, mutually-authenticated client channel to a peer's {@link #fromEnvOrNull} listener --
     * the SAME keystore file (this node's own identity, and the CA it trusts peers against) feeds
     * BOTH directions, exactly as real mTLS requires. Used by the Phase 1b coordinator-side
     * dispatch (not yet built) and directly by {@code WarpPeerServiceIntegrationTest} to prove the
     * whole handshake end-to-end. */
    public static io.grpc.ManagedChannel openPeerChannel(String host, int port, String keystorePath, String keystorePassword)
            throws GeneralSecurityException, IOException {
        TlsSupport.MutualTlsManagers managers = TlsSupport.loadMutualTlsManagers(keystorePath, keystorePassword);
        SslContext sslContext = GrpcSslContexts.forClient()
                .keyManager(managers.keyManagerFactory())
                .trustManager(managers.trustManagerFactory())
                .build();
        return NettyChannelBuilder.forAddress(host, port)
                .sslContext(sslContext)
                .build();
    }

    public int port() {
        return port;
    }

    public void start() throws IOException {
        server.start();
        log.info("warp peer service (node-to-node, mutual TLS) listening on port {}", port);
    }

    public void stop() {
        server.shutdownNow();
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
