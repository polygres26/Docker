package com.sayonora.warp.core;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The hold that makes moving slots between shards safe in this process. While slots of a table are being copied, writes whose key falls in
 * those slots wait (other slots are untouched); while the old shard still holds copies of rows that have moved, scatter reads of the table
 * wait so they cannot see a row twice. Statements already admitted are counted so the mover can wait until they have finished. The wait is
 * bounded: past {@code WARP_RESHARD_GATE_WAIT_SECONDS} (default 120) the statement fails with {@code ERR_SHARD_RESHARDING} instead of hanging.
 */
public final class ReshardGate {

    public static final ReshardGate INSTANCE = new ReshardGate();

    private final Map<String, Set<Integer>> frozen = new ConcurrentHashMap<>();
    private final Set<String> scatterBlocked = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> inflight = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inflightScatter = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private volatile boolean active;

    private static String key(String table) {
        String t = table.toLowerCase(Locale.ROOT);
        return t.contains(".") ? t.substring(t.lastIndexOf('.') + 1) : t;
    }

    /** Cheap check on the hot path: false when nothing is being moved. */
    public boolean active() {
        return active;
    }

    private void refreshActive() {
        active = !frozen.isEmpty() || !scatterBlocked.isEmpty();
    }

    public void freeze(String table, Collection<Integer> slots) {
        synchronized (lock) {
            frozen.put(key(table), Set.copyOf(slots));
            refreshActive();
        }
    }

    public void thaw(String table) {
        synchronized (lock) {
            frozen.remove(key(table));
            refreshActive();
            lock.notifyAll();
        }
    }

    public void blockScatter(String table, boolean blocked) {
        synchronized (lock) {
            if (blocked) {
                scatterBlocked.add(key(table));
            } else {
                scatterBlocked.remove(key(table));
            }
            refreshActive();
            lock.notifyAll();
        }
    }

    /**
     * Admits a write to {@code table}; {@code slots} are the slots its keys fall in, or null when that cannot be told (a broadcast UPDATE/DELETE),
     * which waits while any slot is frozen. Returns a token to close when the statement is done, or null when no hold applies.
     */
    public AutoCloseable admitWrite(String table, Collection<Integer> slots) throws SQLException {
        String k = key(table);
        AtomicInteger n = inflight.computeIfAbsent(k, x -> new AtomicInteger());
        AutoCloseable done = () -> {
            n.decrementAndGet();
            synchronized (lock) {
                lock.notifyAll();
            }
        };
        // Count first, then look at the freeze: a freeze that is not visible here was set after this count, so the mover's drain wait sees it.
        n.incrementAndGet();
        if (!active || !blocks(k, slots)) {
            return done;
        }
        n.decrementAndGet();
        long deadline = System.currentTimeMillis() + waitMillis();
        synchronized (lock) {
            while (blocks(k, slots)) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    throw ErrorCatalog.sqlException("ERR_SHARD_RESHARDING", table);
                }
                try {
                    lock.wait(Math.min(left, 500));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw ErrorCatalog.sqlException("ERR_SHARD_RESHARDING", table);
                }
            }
            n.incrementAndGet();
            return done;
        }
    }

    private boolean blocks(String k, Collection<Integer> slots) {
        Set<Integer> f = frozen.get(k);
        if (f == null || f.isEmpty()) {
            return false;
        }
        if (slots == null) {
            return true;
        }
        for (int s : slots) {
            if (f.contains(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Admits a scatter read of {@code table}: it waits while scatter reads are blocked, and is counted until it finishes so the mover can wait
     * for the ones admitted before the block (a read admitted just before the block may still work out its shard list after the new map is live).
     */
    public AutoCloseable admitScatterRead(String table) throws SQLException {
        String k = key(table);
        AtomicInteger n = inflightScatter.computeIfAbsent(k, x -> new AtomicInteger());
        AutoCloseable done = () -> {
            n.decrementAndGet();
            synchronized (lock) {
                lock.notifyAll();
            }
        };
        n.incrementAndGet();
        if (!active || !scatterBlocked.contains(k)) {
            return done;
        }
        n.decrementAndGet();
        long deadline = System.currentTimeMillis() + waitMillis();
        synchronized (lock) {
            while (scatterBlocked.contains(k)) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    throw ErrorCatalog.sqlException("ERR_SHARD_RESHARDING", table);
                }
                try {
                    lock.wait(Math.min(left, 500));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw ErrorCatalog.sqlException("ERR_SHARD_RESHARDING", table);
                }
            }
            n.incrementAndGet();
            return done;
        }
    }

    /** Waits until every scatter read admitted to {@code table} has finished; false on timeout. */
    public boolean awaitScatterDrained(String table, long timeoutMillis) {
        return awaitCount(inflightScatter.get(key(table)), timeoutMillis);
    }

    private boolean awaitCount(AtomicInteger n, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (lock) {
            while (n != null && n.get() > 0) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    return false;
                }
                try {
                    lock.wait(Math.min(left, 200));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /** Waits until every write admitted to {@code table} before the freeze has finished; false on timeout. */
    public boolean awaitDrained(String table, long timeoutMillis) {
        return awaitCount(inflight.get(key(table)), timeoutMillis);
    }

    private static long waitMillis() {
        try {
            String v = System.getenv("WARP_RESHARD_GATE_WAIT_SECONDS");
            return (v == null || v.isBlank() ? 120 : Long.parseLong(v.trim())) * 1000L;
        } catch (NumberFormatException e) {
            return 120_000;
        }
    }
}
