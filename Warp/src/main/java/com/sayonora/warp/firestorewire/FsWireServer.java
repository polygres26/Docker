package com.sayonora.warp.firestorewire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * firestorewire: the Google Cloud Firestore v1 API (native mode) -- gRPC and REST/JSON on ONE port (default 8080, like the official
 * emulator) -- with documents in the {@code firestore} store of the backend set's Postgres hosts. See {@link FsService} (operations),
 * {@link FsQuery} (structured queries), {@link FsWrites} (commits and transforms), {@link FsStreams} (Write and Listen).
 */
public final class FsWireServer {

    private static final Logger log = LoggerFactory.getLogger(FsWireServer.class);

    /** Metrics hook shared by the gRPC and REST adapters. */
    interface Hooks {
        void record(String op, boolean write, long nanos);
    }

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final FsStore store;
    private final FsStreams streams;
    private final Server server;
    private final int port;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "firestorewire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public FsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(port, registry, gate, metrics, FsConfig.fromEnv());
    }

    FsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, FsConfig cfg) {
        this.port = port;
        this.registry = registry;
        this.metrics = metrics;
        this.store = new FsStore(registry, cfg.historySeconds);
        FsService svc = new FsService(store);
        this.streams = new FsStreams(svc, store);
        Hooks hooks = this::record;
        FsRest rest = new FsRest(svc, cfg, hooks);
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
        NettyServerBuilder b = NettyServerBuilder.forPort(port)
                .addService(new FsGrpc(svc, streams, hooks))
                .intercept(auth)
                .maxInboundMessageSize(32 << 20)
                .protocolNegotiator(new GrpcRestMux(InternalProtocolNegotiators.serverPlaintext(), rest));
        this.server = b.build();
    }

    private void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.FIRESTORE);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("firestorewire", backend, write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, op, nanos, nanos);
    }

    public void start() throws Exception {
        server.start();
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (registry != null && !registry.storeHosts(StoreType.FIRESTORE).isEmpty()) {
                    store.prune();
                }
            } catch (RuntimeException e) {
                log.debug("firestorewire prune failed: {}", e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
        log.info("warp firestorewire (gRPC + REST) listening on port {}", server.getPort());
    }

    public int port() {
        return server.getPort();
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        streams.close();
        server.shutdownNow();
    }
}
