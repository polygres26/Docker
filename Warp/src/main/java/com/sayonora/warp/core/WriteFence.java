package com.sayonora.warp.core;

import java.sql.SQLException;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes the window in which a Warp instance that has not yet heard about a switchover keeps
 * writing to the old primary. Every config change (a switchover is one) is a new, increasing
 * {@code warp_config} version, so that version is the term: an instance may write only while its
 * applied version equals the newest stored one, and only while it has confirmed that against the
 * config database recently.
 *
 * <p>A background thread reads the newest stored version every {@code refreshMillis} and starts a
 * catch-up when this instance is behind. {@link #check()} runs before each non-read statement:
 * it refuses when the last confirmation is older than {@code maxStalenessMillis} (the config
 * database is unreachable -- the partitioned-instance case) or when the newest version seen is
 * still ahead of the applied one after one inline catch-up. The window for a stale write is
 * therefore about {@code refreshMillis} with a reachable config database and at most
 * {@code maxStalenessMillis} without one, instead of the config poll interval.
 *
 * <p>Fail-closed by design: with the fence on, writes depend on the config database being
 * reachable. Off by default ({@code WARP_WRITE_FENCE=true}). The fence lives in the instance, so
 * it works the same on every backend engine.
 */
public final class WriteFence {

    private static final Logger log = LoggerFactory.getLogger(WriteFence.class);

    /** What the fence needs from the config store. */
    public interface Source {
        long latestVersion() throws Exception;

        long appliedVersion();

        void catchUp();
    }

    private final Source source;
    private final long refreshMillis;
    private final long maxStalenessMillis;
    private final LongSupplier clock;
    private volatile long lastConfirmed;
    private volatile long latestSeen;
    private volatile boolean running;
    private Thread thread;

    public WriteFence(Source source, long refreshMillis, long maxStalenessMillis, LongSupplier clock) {
        this.source = source;
        this.refreshMillis = Math.max(50, refreshMillis);
        this.maxStalenessMillis = Math.max(this.refreshMillis * 2, maxStalenessMillis);
        this.clock = clock;
        this.lastConfirmed = clock.getAsLong();
    }

    public static WriteFence fromEnv(Source source) {
        String on = System.getenv("WARP_WRITE_FENCE");
        if (on == null || !on.equalsIgnoreCase("true")) {
            return null;
        }
        return new WriteFence(source, envLong("WARP_WRITE_FENCE_REFRESH_MILLIS", 500),
                envLong("WARP_WRITE_FENCE_MAX_STALENESS_MILLIS", 5000), System::currentTimeMillis);
    }

    private static long envLong(String name, long dflt) {
        String v = System.getenv(name);
        try {
            return v == null || v.isBlank() ? dflt : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> {
            while (running) {
                refresh();
                try {
                    Thread.sleep(refreshMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "warp-write-fence");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    /** One confirmation against the config database; visible for tests. */
    void refresh() {
        try {
            latestSeen = source.latestVersion();
            lastConfirmed = clock.getAsLong();
            if (latestSeen > source.appliedVersion()) {
                source.catchUp();
            }
        } catch (Exception e) {
            log.debug("write fence: could not confirm the config version: {}", e.toString());
        }
    }

    /** Throws when a write must not proceed. */
    public void check() throws SQLException {
        long age = clock.getAsLong() - lastConfirmed;
        if (age > maxStalenessMillis) {
            throw ErrorCatalog.sqlException("ERR_WRITE_FENCE_UNCONFIRMED", age, maxStalenessMillis);
        }
        if (latestSeen > source.appliedVersion()) {
            source.catchUp();
            if (latestSeen > source.appliedVersion()) {
                throw ErrorCatalog.sqlException("ERR_WRITE_FENCE_BEHIND", latestSeen, source.appliedVersion());
            }
        }
    }
}
