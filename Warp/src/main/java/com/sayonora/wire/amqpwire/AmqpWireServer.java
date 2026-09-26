package com.sayonora.wire.amqpwire;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * amqpwire: the AMQP 0-9-1 frontend (what RabbitMQ clients - pika, amqplib, the RabbitMQ Java client - speak). Exchanges and
 * bindings live in the {@code amqp} store of the backend set's first Postgres host, every queue and its messages on the host owning
 * hash(vhost, queue name). See docs/WARP_GUIDE.md, "The AMQP store (amqpwire)".
 */
public final class AmqpWireServer {

    private static final Logger log = LoggerFactory.getLogger(AmqpWireServer.class);

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final ConnectionGate gate;
    private final AmqpConfig cfg;
    private final AmqpBroker broker;
    private final CredentialStore credentials = new CredentialStore();
    private final int port;
    private volatile ServerSocket serverSocket;
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "amqpwire-session");
        t.setDaemon(true);
        return t;
    });

    public AmqpWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this.port = port;
        this.registry = registry;
        this.metrics = metrics;
        this.gate = gate == null ? ConnectionGate.DISABLED : gate;
        this.cfg = AmqpConfig.fromEnv();
        this.broker = new AmqpBroker(new AmqpShards(registry), cfg);
    }

    boolean authRequired() {
        return cfg.authRequired;
    }

    boolean authenticate(String user, byte[] password) {
        if (!cfg.authRequired) {
            return true;
        }
        if (user == null || password == null) {
            return false;
        }
        byte[] expected = credentials.lookupPassword(user);
        return expected != null && MessageDigest.isEqual(expected, password);
    }

    void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        List<String> h = registry.storeHosts(StoreType.AMQP);
        String backend = h.isEmpty() ? "default" : h.size() == 1 ? h.get(0) : String.join(",", h);
        metrics.recordOperation("amqpwire", backend, write ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
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
        Thread t = new Thread(this::acceptLoop, "amqpwire-accept");
        t.setDaemon(true);
        t.start();
        broker.start();
        log.info("warp amqpwire listening on port {}", ss.getLocalPort());
    }

    public void close() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        broker.stop();
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
                if (!gate.acceptTcp(client)) {
                    continue;
                }
                gated = true;
                sessions.execute(new AmqpConnection(this, broker, client, gate::release));
                gated = false;
            } catch (Exception e) {
                log.warn("amqpwire: dropping a connection that could not be served: {}", e.toString());
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
