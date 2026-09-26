package com.sayonora.wire.rediswire;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-connection, cross-node signalling on top of Postgres LISTEN/NOTIFY, with ONE dedicated listener connection per
 * (Warp process, shard host). Two channels: {@code warp_redis_keys} wakes clients blocked in BLPOP/BZPOPMIN/XREAD BLOCK...
 * when a key received elements, and {@code warp_redis_ps} carries pub/sub messages to subscribers on every node.
 * Waiting and subscribed clients never hold a pooled connection: they park on a monitor / receive pushes.
 */
final class Hub {

    private static final Logger log = LoggerFactory.getLogger(Hub.class);
    static final String CH_KEYS = "warp_redis_keys";
    static final String CH_PUBSUB = "warp_redis_ps";
    /** NOTIFY payloads are limited to 8000 bytes; larger pub/sub messages go through warp_redis_pubsub. */
    static final int MAX_INLINE = 6000;

    /** byte[] as a map key. */
    static final class BK {
        final byte[] b;
        private final int h;

        BK(byte[] b) {
            this.b = b;
            this.h = Arrays.hashCode(b);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof BK k && Arrays.equals(b, k.b);
        }

        @Override
        public int hashCode() {
            return h;
        }
    }

    /** A blocked client: signalled when any key it waits on gets elements. */
    static final class Waiter {
        private boolean signaled;
        List<String> ids = List.of();

        synchronized void signal() {
            signaled = true;
            notifyAll();
        }

        /** Waits up to {@code ms}; true when signalled. Clears the flag. */
        synchronized boolean await(long ms) throws InterruptedException {
            long end = System.nanoTime() + ms * 1_000_000L;
            while (!signaled) {
                long left = (end - System.nanoTime()) / 1_000_000L;
                if (left <= 0) {
                    return false;
                }
                wait(left);
            }
            signaled = false;
            return true;
        }
    }

    private final RedisStore store;
    /** per wake id, blocked clients in the order they blocked (Redis serves them first come, first served). */
    private final Map<String, java.util.concurrent.ConcurrentLinkedDeque<Waiter>> waiters = new ConcurrentHashMap<>();
    private final Map<String, Listener> listeners = new ConcurrentHashMap<>();
    /** host + channel -> subscribers; host + pattern -> subscribers. */
    final Map<String, Map<BK, Set<Session>>> channels = new ConcurrentHashMap<>();
    final Map<String, Map<BK, Set<Session>>> patterns = new ConcurrentHashMap<>();
    final Map<String, Map<BK, Set<Session>>> shardChannels = new ConcurrentHashMap<>();
    private volatile boolean closed;

    Hub(RedisStore store) {
        this.store = store;
    }

    // ------------------------------------------------------------------------------------------
    // blocked-client wake-ups
    // ------------------------------------------------------------------------------------------

    Waiter register(String host, List<String> wakeIds) {
        ensureListener(host);
        Waiter w = new Waiter();
        w.ids = wakeIds;
        for (String id : wakeIds) {
            waiters.computeIfAbsent(id, k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).addLast(w);
        }
        return w;
    }

    /** True when {@code w} is the longest-blocked client of at least one of its keys: only then may it take elements. */
    boolean mayProceed(Waiter w) {
        for (String id : w.ids) {
            java.util.concurrent.ConcurrentLinkedDeque<Waiter> q = waiters.get(id);
            if (q != null && q.peekFirst() == w) {
                return true;
            }
        }
        return false;
    }

    void unregister(Waiter w, List<String> wakeIds) {
        for (String id : wakeIds) {
            java.util.concurrent.ConcurrentLinkedDeque<Waiter> q = waiters.get(id);
            if (q != null) {
                q.remove(w);
                Waiter next = q.peekFirst();
                if (next == null) {
                    waiters.remove(id, q);
                } else {
                    next.signal(); // the next one in line re-checks: elements may be left over
                }
            }
        }
    }

    void signalKey(String id) {
        java.util.concurrent.ConcurrentLinkedDeque<Waiter> q = waiters.get(id);
        if (q != null) {
            for (Waiter w : q) {
                w.signal();
            }
        }
    }

    private void signalAll() {
        for (java.util.concurrent.ConcurrentLinkedDeque<Waiter> q : waiters.values()) {
            for (Waiter w : q) {
                w.signal();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // pub/sub registry
    // ------------------------------------------------------------------------------------------

    private static Map<BK, Set<Session>> reg(Map<String, Map<BK, Set<Session>>> m, String host) {
        return m.computeIfAbsent(host, h -> new ConcurrentHashMap<>());
    }

    void subscribe(String host, byte[] name, Session s, int kind) {
        ensureListener(host);
        Map<String, Map<BK, Set<Session>>> m = kind == 0 ? channels : kind == 1 ? patterns : shardChannels;
        reg(m, host).computeIfAbsent(new BK(name), k -> new CopyOnWriteArraySet<>()).add(s);
    }

    void unsubscribe(String host, byte[] name, Session s, int kind) {
        Map<String, Map<BK, Set<Session>>> m = kind == 0 ? channels : kind == 1 ? patterns : shardChannels;
        Map<BK, Set<Session>> r = m.get(host);
        if (r == null) {
            return;
        }
        BK k = new BK(name);
        Set<Session> set = r.get(k);
        if (set != null) {
            set.remove(s);
            if (set.isEmpty()) {
                r.remove(k, set);
            }
        }
    }

    /** Local subscribers a message on {@code channel} reaches (direct + pattern). */
    int localReceivers(String host, byte[] channel, boolean sharded) {
        int n = 0;
        Map<BK, Set<Session>> r = (sharded ? shardChannels : channels).get(host);
        if (r != null) {
            Set<Session> set = r.get(new BK(channel));
            n += set == null ? 0 : set.size();
        }
        if (!sharded) {
            Map<BK, Set<Session>> p = patterns.get(host);
            if (p != null) {
                for (Map.Entry<BK, Set<Session>> e : p.entrySet()) {
                    if (Glob.matches(e.getKey().b, channel)) {
                        n += e.getValue().size();
                    }
                }
            }
        }
        return n;
    }

    /** Encodes a pub/sub message as a NOTIFY payload, or {@code null} when it is too big to inline. */
    static String encodeInline(boolean sharded, byte[] channel, byte[] msg) {
        int est = (channel.length + msg.length) * 4 / 3 + 8;
        if (est > MAX_INLINE) {
            return null;
        }
        Base64.Encoder e = Base64.getEncoder();
        return (sharded ? "s" : "p") + e.encodeToString(channel) + ":" + e.encodeToString(msg);
    }

    private void deliver(String host, boolean sharded, byte[] channel, byte[] msg) {
        Map<BK, Set<Session>> r = (sharded ? shardChannels : channels).get(host);
        if (r != null) {
            Set<Session> set = r.get(new BK(channel));
            if (set != null) {
                for (Session s : set) {
                    s.pushMessage(sharded ? "smessage" : "message", null, channel, msg);
                }
            }
        }
        if (!sharded) {
            Map<BK, Set<Session>> p = patterns.get(host);
            if (p != null) {
                for (Map.Entry<BK, Set<Session>> e : p.entrySet()) {
                    if (Glob.matches(e.getKey().b, channel)) {
                        for (Session s : e.getValue()) {
                            s.pushMessage("pmessage", e.getKey().b, channel, msg);
                        }
                    }
                }
            }
        }
    }

    private void onNotification(Listener l, PGNotification n) {
        String p = n.getParameter();
        if (CH_KEYS.equals(n.getName())) {
            signalKey(p);
            return;
        }
        if (p == null || p.isEmpty()) {
            return;
        }
        try {
            char kind = p.charAt(0);
            boolean sharded = kind == 's' || kind == 'S';
            if (kind == 'p' || kind == 's') {
                int colon = p.indexOf(':');
                Base64.Decoder d = Base64.getDecoder();
                deliver(l.host, sharded, d.decode(p.substring(1, colon)), d.decode(p.substring(colon + 1)));
            } else {
                long id = Long.parseLong(p.substring(1));
                try (PreparedStatement ps = l.lookup.prepareStatement("SELECT ch, msg FROM warp_redis_pubsub WHERE id = ?")) {
                    ps.setLong(1, id);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            deliver(l.host, sharded, rs.getBytes(1), rs.getBytes(2));
                        }
                    }
                }
            }
        } catch (RuntimeException | SQLException e) {
            log.warn("rediswire: bad pub/sub notification on {}: {}", l.host, e.toString());
        }
    }

    // ------------------------------------------------------------------------------------------
    // listeners
    // ------------------------------------------------------------------------------------------

    void ensureListener(String host) {
        if (closed) {
            return;
        }
        Listener l = listeners.computeIfAbsent(host, h -> {
            Listener nl = new Listener(h);
            nl.thread.start();
            return nl;
        });
        // a subscription is only real once LISTEN is active: wait for it (bounded) so no message is missed
        long end = System.nanoTime() + 5_000_000_000L;
        while (!l.ready && System.nanoTime() < end && !closed) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    boolean hasListener(String host) {
        Listener l = listeners.get(host);
        return l != null && l.ready;
    }

    /** Number of dedicated LISTEN connections this process holds (one per host). */
    int listenerCount() {
        return listeners.size();
    }

    void close() {
        closed = true;
        for (Listener l : listeners.values()) {
            l.stop();
        }
        listeners.clear();
    }

    private final class Listener {
        final String host;
        final Thread thread;
        volatile Connection conn;
        volatile Connection lookup;
        volatile boolean ready;
        volatile boolean running = true;

        Listener(String host) {
            this.host = host;
            this.thread = new Thread(this::loop, "rediswire-listen-" + host);
            this.thread.setDaemon(true);
        }

        void stop() {
            running = false;
            closeQuietly(conn);
            closeQuietly(lookup);
            thread.interrupt();
        }

        private void closeQuietly(Connection c) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // shutting down
                }
            }
        }

        private void loop() {
            boolean first = true;
            while (running && !closed) {
                try {
                    store.backends().ensureSchema(host);
                    Connection c = store.backends().openDedicated(host);
                    conn = c;
                    lookup = store.backends().openDedicated(host);
                    try (Statement st = c.createStatement()) {
                        st.execute("LISTEN " + CH_KEYS);
                        st.execute("LISTEN " + CH_PUBSUB);
                    }
                    ready = true;
                    if (!first) {
                        signalAll(); // notifications sent while disconnected were lost: blocked clients re-check
                    }
                    first = false;
                    PGConnection pg = c.unwrap(PGConnection.class);
                    while (running && !closed && !c.isClosed()) {
                        PGNotification[] ns = pg.getNotifications(1000);
                        if (ns != null) {
                            for (PGNotification n : ns) {
                                onNotification(this, n);
                            }
                        }
                    }
                } catch (SQLException | RuntimeException e) {
                    ready = false;
                    if (running && !closed) {
                        log.warn("rediswire: LISTEN connection to {} failed ({}); retrying", host, e.toString());
                        closeQuietly(conn);
                        closeQuietly(lookup);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            return;
                        }
                    }
                }
            }
        }
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> emptyIds() {
        return new ArrayList<>();
    }
}
