package com.sayonora.warp.cluster;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.SourceDialect;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LISTEN half of Warp's out-of-band cache invalidation: one dedicated, long-lived Postgres
 * connection per Postgres backend, parked in {@code LISTEN warp_cache_invalidate}, turning each
 * notify the {@link CacheTriggerInstaller}-installed triggers emit into a {@link
 * CacheStage#invalidateTable} / {@link RowCache#invalidateRows} call -- so a write that never went
 * through Warp (a migration, another application, psql) stops being served stale from the cache
 * within notify-delivery latency instead of until TTL.
 *
 * <p>Modelled directly on {@code ConfigStore#listenLoop} (same daemon-thread-per-connection, same
 * 5 s {@code getNotifications} poll, same close-the-connection-to-unblock shutdown), with three
 * things that loop doesn't need:
 * <ul>
 *   <li><b>Full clear on every (re)connect.</b> NOTIFY is not durable: any write committed while
 *       this connection was down produced a notification nobody received, and there's no way to
 *       replay it -- so every successful {@code LISTEN} is followed by {@link CacheStage#clearAll}
 *       and {@link RowCache#clearAll}. Rate-limited to one clear per {@value #CLEAR_MIN_INTERVAL_MS}
 *       ms per backend: a flapping backend reconnecting every 2 s would otherwise wipe the cache
 *       faster than it can refill, which is strictly worse than no cache at all.</li>
 *   <li><b>A raw connection, never a pooled one.</b> See {@link
 *       CacheTriggerInstaller#openDedicatedConnection}; plus {@code tcpKeepAlive} so a
 *       half-open connection through a NAT/firewall that silently dropped it is detected rather
 *       than sitting in {@code getNotifications} forever hearing nothing.</li>
 *   <li><b>{@link #reconcile}</b> for config reloads that add or remove backends: a removed
 *       backend's connection is closed from the outside (which is what unblocks its thread), and
 *       a new Postgres backend gets its own listener started.</li>
 * </ul>
 *
 * <p>Non-Postgres backends are skipped with an INFO log -- Postgres-only in this version.
 */
public final class CacheInvalidationListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationListener.class);

    static final String CHANNEL = CacheTriggerInstaller.CHANNEL;
    private static final int POLL_TIMEOUT_MS = 5000;
    private static final long RECONNECT_DELAY_MS = 2000;
    static final long CLEAR_MIN_INTERVAL_MS = 10_000;

    /** What a trigger sent -- {@code keys} is {@code null} for a table-level notify (and for a
     * rows-mode one that degraded because too many rows were touched); otherwise each element is
     * {@code [pk, sk]} with a {@code null} sk when the table has no sort-key column. */
    public record Payload(int version, String schema, String table, String op, List<String[]> keys) {

        /** @return {@code null} when {@code raw} isn't a JSON object carrying at least a readable
         *     {@code table} -- the caller decides what "malformed" costs (a table-level invalidation
         *     when the table is readable, a full clear otherwise). Never throws. */
        public static Payload parse(String raw) {
            try {
                JsonElement el = JsonParser.parseString(raw);
                if (!el.isJsonObject()) {
                    return null;
                }
                JsonObject o = el.getAsJsonObject();
                String table = string(o, "table");
                if (table == null || table.isBlank()) {
                    return null;
                }
                List<String[]> keys = null;
                JsonElement k = o.get("keys");
                if (k != null && k.isJsonArray()) {
                    keys = new ArrayList<>();
                    for (JsonElement row : k.getAsJsonArray()) {
                        if (!row.isJsonArray()) {
                            continue;
                        }
                        JsonArray pair = row.getAsJsonArray();
                        if (pair.isEmpty() || pair.get(0).isJsonNull()) {
                            continue;
                        }
                        String pk = pair.get(0).getAsString();
                        String sk = pair.size() > 1 && !pair.get(1).isJsonNull() ? pair.get(1).getAsString() : null;
                        keys.add(new String[] {pk, sk});
                    }
                }
                int version = o.has("v") && o.get("v").isJsonPrimitive() ? o.get("v").getAsInt() : 1;
                return new Payload(version, string(o, "schema"), table, string(o, "op"), keys);
            } catch (RuntimeException malformed) {
                return null;
            }
        }

        private static String string(JsonObject o, String field) {
            JsonElement e = o.get(field);
            return e == null || e.isJsonNull() || !e.isJsonPrimitive() ? null : e.getAsString();
        }
    }

    /** The two cache surfaces a notify lands on, abstracted so the payload-to-invalidation logic
     * is unit-testable with a fake in place of a real Ignite-backed CacheStage/RowCache. */
    public interface Target {
        void invalidateTable(String schema, String table);

        /** @see CacheStage#rowCacheKeyFor */
        String rowCacheKeyFor(String schema, String table);

        void invalidateRows(String physicalTable, List<String[]> keys);

        void clearAll(String reason);
    }

    private static final class RealTarget implements Target {
        private final CacheStage cacheStage;
        private final RowCache rowCache;

        RealTarget(CacheStage cacheStage, RowCache rowCache) {
            this.cacheStage = cacheStage;
            this.rowCache = rowCache;
        }

        @Override
        public void invalidateTable(String schema, String table) {
            if (cacheStage != null) {
                cacheStage.invalidateTable(schema, table);
            }
        }

        @Override
        public String rowCacheKeyFor(String schema, String table) {
            return cacheStage == null || rowCache == null ? null : cacheStage.rowCacheKeyFor(schema, table);
        }

        @Override
        public void invalidateRows(String physicalTable, List<String[]> keys) {
            if (rowCache != null) {
                rowCache.invalidateRows(physicalTable, keys);
            }
        }

        @Override
        public void clearAll(String reason) {
            if (cacheStage != null) {
                cacheStage.clearAll(reason);
            }
            if (rowCache != null) {
                rowCache.clearAll(reason);
            }
        }
    }

    /** One backend's listener: its thread, its current connection (closed from outside to unblock
     * the thread), and its own clear rate-limit clock. */
    private final class BackendListener {
        final BackendTarget target;
        final AtomicBoolean running = new AtomicBoolean(true);
        volatile Connection connection;
        volatile long lastClearAtMs = Long.MIN_VALUE;
        volatile int suppressedClears;
        final Thread thread;

        BackendListener(BackendTarget target) {
            this.target = target;
            this.thread = new Thread(() -> listenLoop(this), "warp-cache-invalidate-" + target.name());
            this.thread.setDaemon(true);
        }

        void stop() {
            running.set(false);
            Connection c = connection;
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // closing is exactly what unblocks getNotifications; nothing else to do
                }
            }
            thread.interrupt();
        }
    }

    private final BackendRegistry backendRegistry;
    private final Target target;
    private final Map<String, BackendListener> listeners = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CacheInvalidationListener(BackendRegistry backendRegistry, CacheStage cacheStage, RowCache rowCache) {
        this(backendRegistry, new RealTarget(cacheStage, rowCache));
    }

    CacheInvalidationListener(BackendRegistry backendRegistry, Target target) {
        this.backendRegistry = backendRegistry;
        this.target = target;
    }

    /** Starts one listener per Postgres backend currently in the registry. */
    public synchronized void start() {
        reconcile();
    }

    /**
     * Brings the running listeners in line with the registry: called from {@code Main}'s
     * config-reload callback right after {@code backendRegistry.reload(...)}. A backend that
     * disappeared has its connection closed (unblocking and ending its thread); a new Postgres
     * backend gets a listener. Unchanged backends are left alone -- their LISTEN connection and,
     * more importantly, their cache contents survive the reload.
     */
    public synchronized void reconcile() {
        if (closed.get()) {
            return;
        }
        Set<String> wanted = new HashSet<>();
        for (BackendTarget t : backendRegistry.all()) {
            if (t.dialect() != SourceDialect.POSTGRES) {
                if (!listeners.containsKey(t.name())) {
                    log.info("cache invalidation: backend \"{}\" is {} -- LISTEN/NOTIFY is Postgres-only, its cached "
                            + "results expire by TTL only", t.name(),
                            t.dialect() == null ? "an unrecognized JDBC URL" : t.dialect());
                }
                continue;
            }
            wanted.add(t.name());
            listeners.computeIfAbsent(t.name(), name -> {
                BackendListener l = new BackendListener(t);
                l.thread.start();
                return l;
            });
        }
        for (String name : new ArrayList<>(listeners.keySet())) {
            if (!wanted.contains(name)) {
                BackendListener l = listeners.remove(name);
                log.info("cache invalidation: backend \"{}\" removed by config reload -- stopping its LISTEN connection", name);
                l.stop();
            }
        }
    }

    /** Backend names with a listener thread currently registered -- for tests and reconcile. */
    public Set<String> activeBackends() {
        return Set.copyOf(listeners.keySet());
    }

    private void listenLoop(BackendListener l) {
        boolean firstConnect = true;
        while (l.running.get() && !closed.get()) {
            try {
                Connection conn = CacheTriggerInstaller.openDedicatedConnection(l.target);
                l.connection = conn;
                if (!l.running.get()) {
                    // Raced with stop(): it may have closed a connection we hadn't assigned yet.
                    conn.close();
                    return;
                }
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                if (firstConnect) {
                    log.info("cache invalidation: LISTEN {} established on backend \"{}\" (dedicated connection)",
                            CHANNEL, l.target.name());
                } else {
                    log.warn("cache invalidation: LISTEN {} re-established on backend \"{}\" -- notifies sent while "
                            + "disconnected were lost, so the caches are being cleared in full", CHANNEL, l.target.name());
                }
                clearAllRateLimited(l, firstConnect ? "LISTEN established on backend \"" + l.target.name() + "\""
                        : "LISTEN reconnected on backend \"" + l.target.name() + "\"");
                firstConnect = false;
                PGConnection pgConn = conn.unwrap(PGConnection.class);
                while (l.running.get() && !closed.get() && !conn.isClosed()) {
                    PGNotification[] notifications = pgConn.getNotifications(POLL_TIMEOUT_MS);
                    if (notifications != null) {
                        for (PGNotification n : notifications) {
                            apply(l.target.name(), n.getParameter());
                        }
                    }
                }
            } catch (SQLException e) {
                if (l.running.get() && !closed.get()) {
                    log.warn("cache invalidation: LISTEN connection to backend \"{}\" failed ({}), retrying in {}ms",
                            l.target.name(), e.getMessage(), RECONNECT_DELAY_MS);
                    sleep(RECONNECT_DELAY_MS);
                }
            } catch (RuntimeException e) {
                // e.g. a vault: password reference whose resolver is down -- same retry, not a
                // dead thread.
                if (l.running.get() && !closed.get()) {
                    log.warn("cache invalidation: listener for backend \"{}\" hit {} -- retrying in {}ms",
                            l.target.name(), e.toString(), RECONNECT_DELAY_MS);
                    sleep(RECONNECT_DELAY_MS);
                }
            } finally {
                Connection c = l.connection;
                l.connection = null;
                if (c != null) {
                    try {
                        c.close();
                    } catch (SQLException ignored) {
                        // already gone
                    }
                }
            }
        }
    }

    private void clearAllRateLimited(BackendListener l, String reason) {
        long now = System.currentTimeMillis();
        if (now - l.lastClearAtMs < CLEAR_MIN_INTERVAL_MS) {
            l.suppressedClears++;
            log.warn("cache invalidation: backend \"{}\" reconnected again within {}ms of the last full clear -- "
                    + "collapsing this one ({} suppressed so far); entries cached in between may be stale until TTL",
                    l.target.name(), CLEAR_MIN_INTERVAL_MS, l.suppressedClears);
            return;
        }
        l.lastClearAtMs = now;
        l.suppressedClears = 0;
        target.clearAll(reason);
    }

    /** Applies one notify -- package-private so the unit test can drive it without a socket. */
    void apply(String backendName, String raw) {
        Payload p = Payload.parse(raw);
        if (p == null) {
            String table = extractTableLeniently(raw);
            if (table != null) {
                log.warn("cache invalidation: malformed notify from backend \"{}\" ({}) -- invalidating table \"{}\" as a whole",
                        backendName, abbreviate(raw), table);
                target.invalidateTable(null, table);
            } else {
                log.warn("cache invalidation: unreadable notify from backend \"{}\" ({}) -- clearing all caches to be safe",
                        backendName, abbreviate(raw));
                target.clearAll("unreadable notify on backend \"" + backendName + "\"");
            }
            return;
        }
        // The result cache indexes by whatever the SELECT literally wrote ("orders" or
        // "public.orders") -- invalidateTable handles both spellings. Always done, even for a
        // rows-level notify: a rows-level write still changes what any SELECT over that table
        // returns; only the ROW cache gets the finer-grained treatment.
        target.invalidateTable(p.schema(), p.table());
        String rowTable = target.rowCacheKeyFor(p.schema(), p.table());
        if (rowTable == null) {
            return;
        }
        if (p.keys() == null) {
            // Table-level (or degraded rows-level) notify on a row-cached table: there's no
            // per-table index in RowCache, so the only safe move is a full clear.
            log.debug("cache invalidation: {} on row-cached table {} carried no keys -- clearing the row cache", p.op(), rowTable);
            target.clearAll(p.op() + " on row-cached table " + rowTable + " (no keys in notify)");
            return;
        }
        target.invalidateRows(rowTable, p.keys());
        String lower = CacheStage.normalizeTable(rowTable);
        if (!lower.equals(rowTable)) {
            // mongowire keys case-preserved, CacheStage's SQL fast path lower-cases -- see
            // CacheStage#rowCacheKeyFor. Both spellings are valid live entries for the same row.
            target.invalidateRows(lower, p.keys());
        }
    }

    private static final java.util.regex.Pattern LENIENT_TABLE = java.util.regex.Pattern.compile(
            "\"table\"\\s*:\\s*\"([^\"]+)\"");

    private static String extractTableLeniently(String raw) {
        if (raw == null) {
            return null;
        }
        java.util.regex.Matcher m = LENIENT_TABLE.matcher(raw);
        return m.find() ? m.group(1) : null;
    }

    private static String abbreviate(String raw) {
        if (raw == null) {
            return "null";
        }
        return raw.length() > 120 ? raw.substring(0, 120) + "..." : raw;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (BackendListener l : listeners.values()) {
            l.stop();
        }
        listeners.clear();
    }
}
