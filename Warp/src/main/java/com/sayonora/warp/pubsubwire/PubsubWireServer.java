package com.sayonora.warp.pubsubwire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pubsubwire: the Google Cloud Pub/Sub frontend. gRPC (Publisher, Subscriber incl. StreamingPull, SchemaService, IAMPolicy) on
 * one port (default 8085 like the official emulator) and the REST/JSON API on another (default 8087), both backed by the
 * {@code pubsub} store of the backend set's Postgres hosts. See {@link PsService}, {@link PsStore}, {@link PsStreams},
 * {@link PsPush}. Nothing holds a pooled connection while a client waits.
 */
public final class PubsubWireServer {

    private static final Logger log = LoggerFactory.getLogger(PubsubWireServer.class);

    private final int grpcPort;
    private final int restPort;
    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final PsConfig cfg;
    private final PsService service;
    private final PsStreams streams;
    private final PsPush push;
    private final PsRest rest;
    private final Server grpc;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pubsubwire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public PubsubWireServer(int grpcPort, int restPort, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(grpcPort, restPort, registry, gate, metrics, PsConfig.fromEnv());
    }

    PubsubWireServer(int grpcPort, int restPort, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, PsConfig cfg) {
        this.grpcPort = grpcPort;
        this.restPort = restPort;
        this.registry = registry;
        this.metrics = metrics;
        this.cfg = cfg;
        PsShards shards = new PsShards(registry);
        this.service = new PsService(new PsStore(shards), cfg);
        this.streams = new PsStreams(service);
        this.push = new PsPush(service);
        PsRpc rpc = new PsRpc(service);
        PsGrpc.Recorder rec = this::record;
        NettyServerBuilder b = NettyServerBuilder.forPort(grpcPort).maxInboundMessageSize(20 * 1024 * 1024)
                .permitKeepAliveTime(1, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(true)
                .intercept(PsGrpc.auth(cfg.tokens));
        new PsGrpc(rpc, streams, rec).services().forEach(b::addService);
        if (gate != null) {
            b.intercept(new com.sayonora.warp.grpc.AclInterceptor(gate.acl()));
        }
        this.grpc = b.build();
        this.rest = restPort > 0 ? new PsRest(rpc, cfg, rec, gate) : null;
    }

    private void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.PUBSUB);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("pubsubwire", backend, write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, op, nanos, nanos);
    }

    public void start() throws Exception {
        grpc.start();
        if (rest != null) {
            rest.start(restPort);
        }
        push.start();
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (registry != null && !registry.storeHosts(StoreType.PUBSUB).isEmpty()) {
                    service.sweep();
                }
            } catch (RuntimeException e) {
                log.debug("pubsubwire sweep failed: {}", e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
        log.info("warp pubsubwire listening on gRPC port {}{}", grpc.getPort(), rest != null ? " and REST port " + restPort : "");
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        push.stop();
        streams.stop();
        if (rest != null) {
            rest.stop();
        }
        grpc.shutdownNow();
    }

    public int grpcPort() {
        return grpc.getPort();
    }
}
