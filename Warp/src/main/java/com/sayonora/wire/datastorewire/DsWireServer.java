package com.sayonora.wire.datastorewire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import com.sayonora.wire.firestorewire.GrpcRestMux;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * datastorewire: the Google Cloud Datastore v1 API -- gRPC and REST/JSON on ONE port (default 8081, like the official emulator) --
 * with entities in the {@code datastore} store of the backend set's Postgres hosts. See {@link DsService} (operations),
 * {@link DsQuery} (queries), {@link DsGql} (GQL subset), {@link DsStore} (storage).
 */
public final class DsWireServer {

    private static final Logger log = LoggerFactory.getLogger(DsWireServer.class);

    /** Metrics hook shared by the gRPC and REST adapters. */
    interface Hooks {
        void record(String op, boolean write, long nanos);
    }

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final Server server;

    public DsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(port, registry, gate, metrics, DsConfig.fromEnv());
    }

    DsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, DsConfig cfg) {
        this.registry = registry;
        this.metrics = metrics;
        DsService svc = new DsService(new DsStore(registry));
        Hooks hooks = this::record;
        ServerInterceptor auth = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (gate != null && gate.acl().hasRules()) {
                    var remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                    if (!(remote instanceof InetSocketAddress isa) || !gate.acl().isAllowed(isa.getAddress())) {
                        call.close(Status.PERMISSION_DENIED.withDescription("client address not allowed"), new Metadata());
                        return new ServerCall.Listener<>() {
                        };
                    }
                }
                String a = headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
                if (!cfg.authorized(a)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("Request had invalid authentication credentials. Expected OAuth 2 "
                            + "access token, login cookie or other valid authentication credential."), new Metadata());
                    return new ServerCall.Listener<>() {
                    };
                }
                return next.startCall(call, headers);
            }
        };
        this.server = NettyServerBuilder.forPort(port)
                .addService(new DsGrpc(svc, hooks))
                .intercept(auth)
                .maxInboundMessageSize(32 << 20)
                .protocolNegotiator(new GrpcRestMux(InternalProtocolNegotiators.serverPlaintext(), new DsRest(svc, cfg, hooks)))
                .build();
    }

    private void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.DATASTORE);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("datastorewire", backend, write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, op, nanos, nanos);
    }

    public void start() throws Exception {
        server.start();
        log.info("warp datastorewire (gRPC + REST) listening on port {}", server.getPort());
    }

    public int port() {
        return server.getPort();
    }

    public void stop() {
        server.shutdownNow();
    }
}
