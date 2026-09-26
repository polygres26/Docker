package com.sayonora.wire.kafkawire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafkawire: the Apache Kafka frontend. Unmodified Kafka clients (Java, librdkafka/confluent-kafka, kafka-python, kcat, the console
 * tools) connect to it; the log lives in the {@code kafka} store of the backend set's Postgres hosts (a partition on the host owning
 * hash(topic-partition), metadata and group state on the first host). See docs/WARP_GUIDE.md, "The Kafka store (kafkawire)".
 */
public final class KafkaWireServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaWireServer.class);

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final ConnectionGate gate;
    private final KafkaStore store;
    private final GroupCoordinator groups;
    private final KafkaBroker broker;
    private final boolean authRequired;
    private final CredentialStore credentials = new CredentialStore();
    private final int port;
    private final long sweepMs;
    private final int maxRequestBytes;
    private volatile ServerSocket serverSocket;
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "kafkawire-session");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "kafkawire-background");
        t.setDaemon(true);
        return t;
    });

    public KafkaWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this.port = port;
        this.registry = registry;
        this.metrics = metrics;
        this.gate = gate == null ? ConnectionGate.DISABLED : gate;
        this.store = new KafkaStore(registry, envLong("WARP_KAFKAWIRE_METADATA_TTL_MS", 1000));
        this.groups = new GroupCoordinator(store, (int) envLong("WARP_KAFKAWIRE_GROUP_MIN_SESSION_TIMEOUT_MS", 6000),
                (int) envLong("WARP_KAFKAWIRE_GROUP_MAX_SESSION_TIMEOUT_MS", 1_800_000), envLong("WARP_KAFKAWIRE_GROUP_INITIAL_REBALANCE_DELAY_MS", 0));
        String a = System.getenv("WARP_KAFKAWIRE_AUTH");
        String creds = System.getenv("WARP_AUTH_CREDENTIALS");
        this.authRequired = a != null ? "true".equalsIgnoreCase(a) : creds != null && !creds.isBlank();
        this.sweepMs = envLong("WARP_KAFKAWIRE_SWEEP_MS", 300_000);
        this.maxRequestBytes = (int) envLong("WARP_KAFKAWIRE_MAX_REQUEST_BYTES", 100L * 1024 * 1024);
        String host = System.getenv("WARP_KAFKAWIRE_ADVERTISED_HOST");
        String advPort = System.getenv("WARP_KAFKAWIRE_ADVERTISED_PORT");
        this.advertisedHost = host == null || host.isBlank() ? "localhost" : host.trim();
        this.advertisedPortEnv = advPort == null || advPort.isBlank() ? 0 : Integer.parseInt(advPort.trim());
        this.broker = new KafkaBroker(store, groups, advertisedHost, 0, !"false".equalsIgnoreCase(System.getenv("WARP_KAFKAWIRE_AUTO_CREATE")),
                (int) envLong("WARP_KAFKAWIRE_NUM_PARTITIONS", 1), authRequired);
    }

    private final String advertisedHost;
    private final int advertisedPortEnv;
    private KafkaBroker liveBroker;

    static long envLong(String n, long d) {
        String v = System.getenv(n);
        try {
            return v == null || v.isBlank() ? d : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }

    KafkaBroker broker() {
        return liveBroker == null ? broker : liveBroker;
    }

    KafkaStore store() {
        return store;
    }

    boolean authRequired() {
        return authRequired;
    }

    int maxRequestBytes() {
        return maxRequestBytes;
    }

    boolean checkPassword(String user, byte[] password) {
        byte[] expected = credentials.lookupPassword(user);
        return expected != null && MessageDigest.isEqual(expected, password);
    }

    void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        List<String> h = registry.storeHosts(StoreType.KAFKA);
        String backend = h.isEmpty() ? "default" : h.size() == 1 ? h.get(0) : String.join(",", h);
        metrics.recordOperation("kafkawire", backend, write ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
                op, nanos, nanos);
    }

    public int port() {
        ServerSocket s = serverSocket;
        return s == null ? port : s.getLocalPort();
    }

    public void start() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(port), 1024);
        serverSocket = ss;
        int adv = advertisedPortEnv > 0 ? advertisedPortEnv : ss.getLocalPort();
        // the broker needs the port it really listens on for its advertised endpoint
        this.liveBroker = new KafkaBroker(store, groups, advertisedHost, adv, broker.autoCreate, broker.defaultPartitions, authRequired);
        Thread t = new Thread(this::acceptLoop, "kafkawire-accept");
        t.setDaemon(true);
        t.start();
        scheduler.scheduleWithFixedDelay(this::registerSelf, 0, 3, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(groups::sweep, 200, 200, TimeUnit.MILLISECONDS);
        if (sweepMs > 0) {
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    if (store.available()) {
                        int n = store.sweep(System.currentTimeMillis());
                        if (n > 0) {
                            log.debug("kafkawire retention removed {} batches", n);
                        }
                    }
                } catch (RuntimeException e) {
                    log.debug("kafkawire retention sweep failed: {}", e.getMessage());
                }
            }, sweepMs, sweepMs, TimeUnit.MILLISECONDS);
        }
        log.info("warp kafkawire listening on port {} (advertised {}:{})", ss.getLocalPort(), advertisedHost, adv);
    }

    private void registerSelf() {
        try {
            if (store.available() && liveBroker != null) {
                liveBroker.nodeId = store.registerBroker(advertisedHost, liveBroker.advertisedPort);
            }
        } catch (RuntimeException e) {
            log.debug("kafkawire broker registration failed: {}", e.getMessage());
        }
    }

    public void close() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        if (liveBroker != null) {
            store.unregister(liveBroker.nodeId);
        }
        scheduler.shutdownNow();
        sessions.shutdownNow();
    }

    private void acceptLoop() {
        ServerSocket ss = serverSocket;
        while (!ss.isClosed()) {
            Socket client;
            try {
                client = ss.accept();
            } catch (IOException e) {
                if (ss.isClosed()) {
                    return;
                }
                continue;
            }
            boolean gated = false;
            try {
                client.setTcpNoDelay(true);
                if (!gate.acceptTcp(client)) {
                    continue;
                }
                gated = true;
                sessions.execute(new KafkaSession(this, client, gate::release));
                gated = false;
            } catch (Exception e) {
                log.warn("kafkawire: dropping a connection that could not be served: {}", e.toString());
                if (gated) {
                    gate.release();
                }
                try {
                    client.close();
                } catch (IOException ignored) {
                    // gone
                }
            }
        }
    }
}
