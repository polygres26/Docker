package com.sayonora.warp.cqlwire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.auth.CredentialStore;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cqlwire: the Apache Cassandra frontend (CQL native protocol v3/v4). Amazon Keyspaces and Cosmos DB's Cassandra API expose the same
 * protocol, so unmodified Cassandra drivers connect. Data lives in the {@code cql} store of the backend set's Postgres hosts:
 * a partition (all its rows) on the host owning hash(partition key). See docs/WARP_GUIDE.md, "The Cassandra store (cqlwire)".
 */
public final class CqlWireServer {

    private static final Logger log = LoggerFactory.getLogger(CqlWireServer.class);

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final ConnectionGate gate;
    private final Engine engine;
    private final boolean authRequired;
    private final CredentialStore credentials = new CredentialStore();
    private final int port;
    private final long sweepMs;
    private volatile ServerSocket serverSocket;
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "cqlwire-session");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cqlwire-ttl-sweeper");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, Parser.Parsed> parseCache = new ConcurrentHashMap<>();
    private final Map<String, Engine.Prepared> prepared = new ConcurrentHashMap<>();

    public CqlWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this.port = port;
        this.registry = registry;
        this.metrics = metrics;
        this.gate = gate == null ? ConnectionGate.DISABLED : gate;
        this.engine = new Engine(new CqlShards(registry), envLong("WARP_CQLWIRE_SCHEMA_TTL_MS", 1000));
        String a = System.getenv("WARP_CQLWIRE_AUTH");
        String creds = System.getenv("WARP_AUTH_CREDENTIALS");
        this.authRequired = a != null ? "true".equalsIgnoreCase(a) : creds != null && !creds.isBlank();
        this.sweepMs = envLong("WARP_CQLWIRE_SWEEP_MS", 5000);
    }

    static long envLong(String n, long d) {
        String v = System.getenv(n);
        try {
            return v == null || v.isBlank() ? d : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }

    static String clusterName() {
        String v = System.getenv("WARP_CQLWIRE_CLUSTER_NAME");
        return v == null || v.isBlank() ? "Warp Cluster" : v;
    }

    static String releaseVersion() {
        String v = System.getenv("WARP_CQLWIRE_RELEASE_VERSION");
        return v == null || v.isBlank() ? "4.0.0" : v;
    }

    Engine engine() {
        return engine;
    }

    boolean authRequired() {
        return authRequired;
    }

    boolean checkPassword(String user, byte[] password) {
        byte[] expected = credentials.lookupPassword(user);
        return expected != null && MessageDigest.isEqual(expected, password);
    }

    Parser.Parsed parse(String q) {
        Parser.Parsed p = parseCache.get(q);
        if (p == null) {
            p = Parser.parseFull(q);
            if (parseCache.size() > 2048) {
                parseCache.clear();
            }
            parseCache.put(q, p);
        }
        return p;
    }

    Engine.Prepared prepare(String q, ClientState cs) {
        Engine.Prepared p = engine.prepare(q, cs);
        byte[] id = CqlSession.md5((cs.keyspace == null ? "" : cs.keyspace) + q);
        p.idBytes = id;
        p.id = CqlType.hex(id);
        if (prepared.size() > 10000) {
            prepared.clear();
        }
        prepared.put(p.id, p);
        return p;
    }

    Engine.Prepared prepared(byte[] id) {
        return prepared.get(CqlType.hex(id));
    }

    void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        List<String> h = registry.storeHosts(StoreType.CQL);
        String backend = h.isEmpty() ? "default" : h.size() == 1 ? h.get(0) : String.join(",", h);
        metrics.recordOperation("cqlwire", backend, write ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
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
        Thread t = new Thread(this::acceptLoop, "cqlwire-accept");
        t.setDaemon(true);
        t.start();
        if (sweepMs > 0) {
            sweeper.scheduleWithFixedDelay(() -> {
                try {
                    if (!registry.storeHosts(StoreType.CQL).isEmpty()) {
                        for (String h : engine.shards.allHosts()) {
                            engine.store.sweep(h);
                        }
                    }
                } catch (RuntimeException e) {
                    log.debug("cqlwire TTL sweep failed: {}", e.getMessage());
                }
            }, sweepMs, sweepMs, TimeUnit.MILLISECONDS);
        }
        log.info("warp cqlwire listening on port {}", ss.getLocalPort());
    }

    public void close() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        sweeper.shutdownNow();
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
                sessions.execute(new CqlSession(this, client, gate::release));
                gated = false;
            } catch (Exception e) {
                log.warn("cqlwire: dropping a connection that could not be served: {}", e.toString());
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
