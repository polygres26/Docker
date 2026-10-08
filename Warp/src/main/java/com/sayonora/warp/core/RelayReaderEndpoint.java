package com.sayonora.warp.core;

import com.sayonora.warp.orawire.backend.NativeSessionRelay;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-replica routing for the raw-byte relay modes (Oracle, MySQL and SQL Server {@code RELAY}). A relay never parses the protocol, so it
 * cannot tell a read from a write per statement; the choice is made per connection instead, by which port the client dials. This extra
 * listener is the "reader endpoint": each connection is relayed to a lag-eligible replica of a registry backend (the same eligibility,
 * round-robin and quarantine as statement-level routing, see {@link ReplicaRouter}), and when none is reachable it goes to the primary
 * (default) or is refused. Writes sent to this port reach a replica and fail with the database's own read-only error.
 *
 * <p>No read-your-writes: a session on this port can read behind its own writes by up to the replica's lag allowance. Replica roles follow
 * the failover monitor, because the replicas are read from the registry at each connection.
 */
public final class RelayReaderEndpoint implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RelayReaderEndpoint.class);

    private final String label;
    private final int listenPort;
    private final String backendName;
    private final BackendRegistry registry;
    private final ReplicaRouter router;
    private final int defaultDbPort;
    private final boolean fallbackToPrimary;
    private final int connectTimeoutMillis;
    private volatile ServerSocket server;
    private ExecutorService sessions;

    public RelayReaderEndpoint(String label, int listenPort, String backendName, BackendRegistry registry, int defaultDbPort,
            boolean fallbackToPrimary) {
        this(label, listenPort, backendName, registry, registry.replicaRouter(), defaultDbPort, fallbackToPrimary, 3000);
    }

    RelayReaderEndpoint(String label, int listenPort, String backendName, BackendRegistry registry, ReplicaRouter router,
            int defaultDbPort, boolean fallbackToPrimary, int connectTimeoutMillis) {
        this.label = label;
        this.listenPort = listenPort;
        this.backendName = backendName;
        this.registry = registry;
        this.router = router;
        this.defaultDbPort = defaultDbPort;
        this.fallbackToPrimary = fallbackToPrimary;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /** The port actually bound (useful when 0 was requested). */
    public int port() {
        return server == null ? listenPort : server.getLocalPort();
    }

    public void start() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(listenPort));
        server = ss;
        sessions = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "warp-relay-reader-" + label);
            t.setDaemon(true);
            return t;
        });
        Thread acceptor = new Thread(() -> {
            while (!ss.isClosed()) {
                try {
                    Socket client = ss.accept();
                    sessions.submit(() -> serve(client));
                } catch (IOException e) {
                    if (!ss.isClosed()) {
                        log.warn("relay reader {}: accept failed: {}", label, e.toString());
                    }
                }
            }
        }, "warp-relay-reader-accept-" + label);
        acceptor.setDaemon(true);
        acceptor.start();
        log.info("relay reader endpoint for {} listening on port {} (replicas of backend '{}'{})", label, ss.getLocalPort(), backendName,
                fallbackToPrimary ? ", primary as fallback" : ", refusing when no replica is available");
    }

    private void serve(Socket client) {
        try (client) {
            Socket backend = connectToReplica();
            if (backend == null && fallbackToPrimary) {
                backend = connectTo(registry.resolveForRouting(backendName), "primary");
            }
            if (backend == null) {
                log.warn("relay reader {}: no replica of '{}' is available{}; closing the connection", label, backendName,
                        fallbackToPrimary ? " and the primary is unreachable" : "");
                return;
            }
            NativeSessionRelay.relay(client, backend);
        } catch (IOException e) {
            log.debug("relay reader {}: session ended: {}", label, e.toString());
        }
    }

    /** A connected socket to the next eligible replica, quarantining ones that cannot be reached; null when there is none. */
    Socket connectToReplica() {
        int attempts = router.replicasOf(backendName).size();
        for (int i = 0; i < attempts; i++) {
            ReplicaRouter.Replica replica = router.pick(backendName);
            if (replica == null) {
                return null;
            }
            Socket s = connectTo(replica.target(), "replica " + replica.id());
            if (s != null) {
                return s;
            }
            router.quarantine(replica, "relay reader could not connect");
        }
        return null;
    }

    private Socket connectTo(BackendTarget target, String what) {
        if (target == null) {
            return null;
        }
        try {
            JdbcHostPort hp = JdbcHostPort.parse(target.jdbcUrl(), defaultDbPort);
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress(hp.host(), hp.port()), connectTimeoutMillis);
                return s;
            } catch (IOException e) {
                s.close();
                log.warn("relay reader {}: cannot connect to {} {}:{}: {}", label, what, hp.host(), hp.port(), e.toString());
                return null;
            }
        } catch (IOException | RuntimeException e) {
            log.warn("relay reader {}: bad target for {}: {}", label, what, e.toString());
            return null;
        }
    }

    @Override
    public void close() {
        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        if (sessions != null) {
            sessions.shutdownNow();
        }
    }
}
