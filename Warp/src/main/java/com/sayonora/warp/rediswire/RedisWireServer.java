package com.sayonora.warp.rediswire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rediswire -- a Redis (RESP2/RESP3) frontend whose data lives in Postgres: keys, strings, hashes, lists, sets, sorted
 * sets, streams, HyperLogLog, bitmaps, geo, pub/sub, blocking commands and transactions. One store table family
 * ({@code warp_redis_*}) per Postgres host of the backend set that enabled the {@code redis} store; keys are spread over
 * the hosts by Redis Cluster hash slot (CRC16 with {hash tags}). See docs/WARP_GUIDE.md, "The Redis store (rediswire)".
 */
public final class RedisWireServer {

    private static final Logger log = LoggerFactory.getLogger(RedisWireServer.class);

    private final RedisStore store;
    private final Cmd.Registry registry = Cmd.buildRegistry();
    private final ConnectionGate gate;
    private final RedisOptions options;
    private volatile ServerSocket serverSocket;
    private Thread acceptThread;
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "rediswire-session");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService pushes = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "rediswire-push");
        t.setDaemon(true);
        return t;
    });

    /** Production constructor: hosts come from {@code registry}'s {@code redis} store. */
    public RedisWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(RedisOptions.fromEnv(port), new RegistryBackends(registry, metrics), gate);
    }

    RedisWireServer(RedisOptions options, Backends backends, ConnectionGate gate) {
        this.options = options;
        this.store = new RedisStore(backends, options);
        this.gate = gate == null ? ConnectionGate.DISABLED : gate;
    }

    RedisStore store() {
        return store;
    }

    public int port() {
        ServerSocket s = serverSocket;
        return s == null ? options.port() : s.getLocalPort();
    }

    public void start() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(options.port()), 1024);
        serverSocket = ss;
        store.startSweeper();
        acceptThread = new Thread(this::acceptLoop, "rediswire-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        log.info("rediswire: {} commands registered", registry.size());
    }

    public void close() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        for (Session s : store.clients.values()) {
            s.kill();
        }
        store.close();
        sessions.shutdownNow();
        pushes.shutdownNow();
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
                if (store.clients.size() >= options.maxClients()) {
                    store.rejectedConnections.incrementAndGet();
                    client.getOutputStream().write("-ERR max number of clients reached\r\n".getBytes(StandardCharsets.US_ASCII));
                    client.close();
                    gate.release();
                    continue;
                }
                Session session = new Session(store, registry, client, pushes, gate::release);
                sessions.execute(session);
                gated = false;
            } catch (Exception e) {
                log.warn("rediswire: dropping a connection that could not be served: {}", e.toString());
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
