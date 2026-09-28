package com.sayonora.warp.rediswire;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process-wide state of the Redis frontend: backends, hub, config, client registry, counters, expiry sweeper. */
final class RedisStore {

    private static final Logger log = LoggerFactory.getLogger(RedisStore.class);

    private final Backends backends;
    private final RedisOptions options;
    private final Hub hub;
    private final Set<String> schemaDone = ConcurrentHashMap.newKeySet();
    private final Set<String> knownHosts = ConcurrentHashMap.newKeySet();
    final Map<String, String> config = new ConcurrentHashMap<>();
    final Map<Long, Session> clients = new ConcurrentHashMap<>();
    final AtomicLong clientIds = new AtomicLong();
    final AtomicLong totalConnections = new AtomicLong();
    final AtomicLong totalCommands = new AtomicLong();
    final AtomicLong expiredKeys = new AtomicLong();
    final AtomicLong rejectedConnections = new AtomicLong();
    final long startMillis = System.currentTimeMillis();
    final String runId = UUID.randomUUID().toString().replace("-", "") + "0000000000";
    final String clusterNodeId = UUID.randomUUID().toString().replace("-", "").substring(0, 32) + "00000000";
    final SlowLog slowLog = new SlowLog();
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "rediswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    RedisStore(Backends backends, RedisOptions options) {
        this.backends = backends;
        this.options = options;
        this.hub = new Hub(this);
        String[][] defaults = {
            {"maxmemory", "0"}, {"maxmemory-policy", "noeviction"}, {"save", ""}, {"appendonly", "no"},
            {"timeout", "0"}, {"tcp-keepalive", "300"}, {"databases", String.valueOf(options.databases())},
            {"notify-keyspace-events", ""}, {"slowlog-log-slower-than", "10000"}, {"slowlog-max-len", "128"},
            {"hz", "10"}, {"maxclients", String.valueOf(options.maxClients())}, {"port", String.valueOf(options.port())},
            {"bind", "*"}, {"dir", "/data"}, {"dbfilename", "dump.rdb"}, {"list-max-listpack-size", "-2"},
            {"list-max-ziplist-size", "-2"}, {"hash-max-listpack-entries", "128"}, {"hash-max-listpack-value", "64"},
            {"hash-max-ziplist-entries", "128"}, {"hash-max-ziplist-value", "64"}, {"set-max-intset-entries", "512"},
            {"set-max-listpack-entries", "128"}, {"set-max-listpack-value", "64"}, {"zset-max-listpack-entries", "128"},
            {"zset-max-listpack-value", "64"}, {"zset-max-ziplist-entries", "128"}, {"zset-max-ziplist-value", "64"},
            {"stream-node-max-bytes", "4096"}, {"stream-node-max-entries", "100"}, {"lazyfree-lazy-user-del", "no"},
            {"appendfsync", "everysec"}, {"maxmemory-samples", "5"}, {"repl-backlog-size", "1048576"},
            {"tracking-table-max-keys", "1000000"}, {"proto-max-bulk-len", "536870912"}, {"lua-time-limit", "5000"},
            {"busy-reply-threshold", "5000"}, {"cluster-enabled", "yes"}, {"latency-tracking", "yes"},
            {"activedefrag", "no"}, {"lfu-log-factor", "10"}, {"lfu-decay-time", "1"}, {"loglevel", "notice"},
            {"requirepass", options.password() == null ? "" : options.password()}, {"acllog-max-len", "128"},
            {"client-output-buffer-limit", "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"},
            {"replica-read-only", "yes"}, {"min-replicas-to-write", "0"}, {"maxmemory-clients", "0"},
            {"latency-monitor-threshold", "0"}, {"close-on-oom", "no"},
        };
        for (String[] d : defaults) {
            config.put(d[0], d[1]);
        }
    }

    /** Current AUTH password ({@code requirepass}), or null when none is required. */
    String password() {
        String p = config.get("requirepass");
        return p == null || p.isEmpty() ? null : p;
    }

    Backends backends() {
        return backends;
    }

    RedisOptions options() {
        return options;
    }

    Hub hub() {
        return hub;
    }

    Set<String> knownHosts() {
        return knownHosts;
    }

    /** A pooled connection to {@code host}, bootstrapping the schema on first use. */
    Connection open(String host) throws SQLException {
        if (schemaDone.add(host)) {
            try {
                backends.ensureSchema(host);
            } catch (SQLException | RuntimeException e) {
                schemaDone.remove(host);
                throw e;
            }
        }
        knownHosts.add(host);
        return backends.open(host);
    }

    void startSweeper() {
        long every = Math.max(50, options.sweepMs());
        sweeper.scheduleWithFixedDelay(this::sweepOnce, every, every, TimeUnit.MILLISECONDS);
    }

    /** Deletes expired keys on every known host (bounded batches so a mass expiry never holds long locks). */
    void sweepOnce() {
        for (String host : knownHosts) {
            try (Connection c = backends.open(host); Statement st = c.createStatement()) {
                long now = System.currentTimeMillis();
                int n;
                do {
                    n = st.executeUpdate("DELETE FROM warp_redis_keys WHERE (db, k) IN (SELECT db, k FROM warp_redis_keys "
                            + "WHERE exp IS NOT NULL AND exp <= " + now + " LIMIT 1000)");
                    expiredKeys.addAndGet(n);
                } while (n >= 1000);
                st.executeUpdate("DELETE FROM warp_redis_pubsub WHERE ts < now() - interval '60 seconds'");
            } catch (SQLException | RuntimeException e) {
                log.debug("rediswire: sweep on {} failed: {}", host, e.toString());
            }
        }
    }

    void close() {
        sweeper.shutdownNow();
        hub.close();
    }

    /** Id used by NOTIFY/LISTEN to wake clients blocked on a key: db plus the key's SHA-1. */
    static String wakeId(int db, byte[] key) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-1").digest(key);
            return db + ":" + Base64.getEncoder().withoutPadding().encodeToString(d);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Ring buffer behind SLOWLOG. */
    static final class SlowLog {
        private final java.util.ArrayDeque<Object[]> entries = new java.util.ArrayDeque<>();
        private long nextId;

        synchronized void add(long micros, long unixSec, byte[][] args, String client, String name, int max) {
            entries.addFirst(new Object[] {nextId++, unixSec, micros, args, client, name});
            while (entries.size() > Math.max(0, max)) {
                entries.removeLast();
            }
        }

        synchronized List<Object[]> get(int count) {
            List<Object[]> out = new java.util.ArrayList<>();
            for (Object[] e : entries) {
                if (out.size() >= count) {
                    break;
                }
                out.add(e);
            }
            return out;
        }

        synchronized int len() {
            return entries.size();
        }

        synchronized void reset() {
            entries.clear();
        }
    }

    Map<String, String> sortedConfig() {
        return new TreeMap<>(config);
    }
}
