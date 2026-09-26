package com.sayonora.wire.firestorewire;

import com.sayonora.wire.cluster.CacheTriggerInstaller;
import com.sayonora.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Listen wake-up hub: one dedicated (non-pooled) {@code LISTEN warp_firestore} connection per Postgres host of the store, shared
 * by every Listen stream of this Warp node. A commit on ANY Warp node runs {@code pg_notify} inside its transaction, so streams learn
 * of changes made through other nodes; a 5 s fallback poll covers a lost notification or a reconnect. Streams hold no database
 * connection while idle -- they are woken, run one short query on a pooled connection, and let it go.
 */
final class FsHub {

    private static final Logger log = LoggerFactory.getLogger(FsHub.class);

    private final FsStore store;
    private final Map<Waker, FsNames.Db> subscribers = new ConcurrentHashMap<>();
    private final Map<String, Thread> listeners = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "firestorewire-hub-timer");
        t.setDaemon(true);
        return t;
    });

    FsHub(FsStore store) {
        this.store = store;
        timer.scheduleWithFixedDelay(this::tick, 3, 3, TimeUnit.SECONDS);
    }

    void subscribe(Waker w, FsNames.Db db) {
        subscribers.put(w, db);
        ensureListeners(store.hosts(db));
    }

    void unsubscribe(Waker w) {
        subscribers.remove(w);
    }

    private void tick() {
        try {
            if (!subscribers.isEmpty()) {
                for (FsNames.Db d : new java.util.HashSet<>(subscribers.values())) {
                    ensureListeners(store.hosts(d));
                }
                for (Waker w : subscribers.keySet()) {
                    w.wake();
                }
            }
        } catch (RuntimeException e) {
            log.debug("firestorewire hub tick: {}", e.toString());
        }
    }

    private void ensureListeners(List<String> hosts) {
        for (String h : hosts) {
            listeners.computeIfAbsent(h, k -> {
                Thread t = new Thread(() -> listenLoop(k), "firestorewire-listen-" + k);
                t.setDaemon(true);
                t.start();
                return t;
            });
        }
    }

    private void listenLoop(String host) {
        while (!closed.get()) {
            Connection c = null;
            try {
                BackendTarget target = store.target(host);
                c = CacheTriggerInstaller.openDedicatedConnection(target);
                try (Statement st = c.createStatement()) {
                    st.execute("LISTEN " + FsStore.CHANNEL);
                }
                PGConnection pg = c.unwrap(PGConnection.class);
                for (Waker w : subscribers.keySet()) {
                    w.wake();
                }
                while (!closed.get() && !c.isClosed()) {
                    PGNotification[] ns = pg.getNotifications(1000);
                    if (ns != null && ns.length > 0) {
                        Set<String> dbs = new java.util.HashSet<>();
                        for (PGNotification n : ns) {
                            dbs.add(n.getParameter());
                        }
                        for (var e : subscribers.entrySet()) {
                            if (dbs.contains(e.getValue().name())) {
                                e.getKey().wake();
                            }
                        }
                    }
                }
            } catch (SQLException | RuntimeException e) {
                if (!closed.get()) {
                    log.debug("firestorewire LISTEN on {} failed ({}), retrying", host, e.toString());
                }
            } finally {
                if (c != null) {
                    try {
                        c.close();
                    } catch (SQLException ignored) {
                        // gone
                    }
                }
            }
            if (!closed.get()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }


    interface Waker {
        void wake();
    }

    void close() {
        closed.set(true);
        timer.shutdownNow();
        for (Thread t : listeners.values()) {
            t.interrupt();
        }
    }
}
