package com.sayonora.wire.bigtablewire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * bigtablewire: the Google Cloud Bigtable frontend. gRPC only (google.bigtable.v2.Bigtable data API and
 * google.bigtable.admin.v2.BigtableTableAdmin) on one port (default 8088; the official emulator uses 8086), backed by the
 * {@code bigtable} store of the backend set's Postgres hosts. See {@link BtData}, {@link BtAdmin}, {@link BtStore}.
 */
public final class BigtableWireServer {

    private static final Logger log = LoggerFactory.getLogger(BigtableWireServer.class);

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final BtConfig cfg;
    private final BtAdmin admin;
    private final Server grpc;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "bigtablewire-gc");
        t.setDaemon(true);
        return t;
    });

    public BigtableWireServer(int grpcPort, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(grpcPort, registry, gate, metrics, BtConfig.fromEnv());
    }

    BigtableWireServer(int grpcPort, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, BtConfig cfg) {
        this.registry = registry;
        this.metrics = metrics;
        this.cfg = cfg;
        BtShards shards = new BtShards(registry);
        BtStore store = new BtStore(shards);
        this.admin = new BtAdmin(store);
        BtData data = new BtData(store, admin, cfg);
        NettyServerBuilder b = NettyServerBuilder.forPort(grpcPort).maxInboundMessageSize(256 * 1024 * 1024)
                .permitKeepAliveTime(1, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(true)
                .intercept(BtGrpc.auth(cfg.tokens));
        new BtGrpc(data, admin, this::record).services().forEach(b::addService);
        if (gate != null) {
            b.intercept(new com.sayonora.wire.grpc.AclInterceptor(gate.acl()));
        }
        this.grpc = b.build();
    }

    private void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.BIGTABLE);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("bigtablewire", backend, write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, op, nanos, nanos);
    }

    public void start() throws Exception {
        grpc.start();
        if (cfg.gcIntervalSeconds > 0) {
            sweeper.scheduleWithFixedDelay(() -> {
                try {
                    if (registry != null && !registry.storeHosts(StoreType.BIGTABLE).isEmpty()) {
                        admin.gcSweep();
                    }
                } catch (RuntimeException e) {
                    log.debug("bigtablewire gc sweep failed: {}", e.getMessage());
                }
            }, cfg.gcIntervalSeconds, cfg.gcIntervalSeconds, TimeUnit.SECONDS);
        }
        log.info("warp bigtablewire listening on gRPC port {}", grpc.getPort());
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        grpc.shutdownNow();
    }

    public int grpcPort() {
        return grpc.getPort();
    }
}
