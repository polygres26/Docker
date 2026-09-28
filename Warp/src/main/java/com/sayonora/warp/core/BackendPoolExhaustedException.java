package com.sayonora.warp.core;

import java.sql.SQLException;

/**
 * Thrown by {@link BackendConnectionPools#borrow} when every connection of Warp's own backend pool
 * (Warp -> Postgres) is checked out and none was returned within {@code WARP_POOL_CONNECT_TIMEOUT_MS}.
 *
 * <p>Deliberately a distinct type carrying SQLSTATE {@code 53300} ({@code too_many_connections}) so
 * every wire frontend's existing SQLSTATE-to-native-error mapping turns it into that protocol's own
 * "too many connections" error (ORA-00018, MySQL 1040, Postgres 53300, ...) while the message still
 * names what really happened -- Warp's backend pool, how long the client waited, and which knobs
 * fix it -- instead of the driver's opaque "Connection is not available" text.
 */
public final class BackendPoolExhaustedException extends SQLException {

    /** Substring every rendering layer can key off (see {@link DialectErrorMessages#render}). */
    public static final String WARP_BACKEND_MARKER = "Warp backend";
    public static final String MESSAGE_MARKER = WARP_BACKEND_MARKER + " connection pool exhausted";

    private final long waitedMillis;
    private final int maxPoolSize;

    public BackendPoolExhaustedException(String poolName, long waitedMillis, int maxPoolSize, int active,
            int waiting, Throwable cause) {
        super(MESSAGE_MARKER + ": waited " + waitedMillis + "ms for one of " + maxPoolSize
                + " pooled backend connections (" + active + " in use, " + waiting + " other request(s) waiting; pool \""
                + poolName + "\"). Every connection is held by an open transaction or by session state that pins it, "
                + "or the pool is too small for the load: raise WARP_POOL_MAX_SIZE, shorten transactions, or raise "
                + "WARP_POOL_CONNECT_TIMEOUT_MS to wait longer.", "53300", cause);
        this.waitedMillis = waitedMillis;
        this.maxPoolSize = maxPoolSize;
    }

    public long waitedMillis() {
        return waitedMillis;
    }

    public int maxPoolSize() {
        return maxPoolSize;
    }
}
