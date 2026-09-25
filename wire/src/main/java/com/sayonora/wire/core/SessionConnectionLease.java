package com.sayonora.wire.core;

import com.sayonora.wire.core.access.PhysicalSessionState;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Many-to-few connection multiplexing for the translated (JDBC) frontends -- the pgwire / mywire /
 * mssqlwire / orawire / boltwire session handlers -- in the style of PgBouncer's transaction pooling.
 *
 * <p>A client session used to borrow ONE pooled backend connection at its first statement and keep it
 * until it disconnected, so {@code WARP_POOL_MAX_SIZE} idle clients starved everyone else. A session now
 * owns a lease instead: the backend connection is borrowed when a statement actually needs one
 * ({@link #acquire}) and handed back to the pool as soon as the work is done ({@link #releaseIfIdle}),
 * unless the session holds state that lives on that physical connection. Then the lease is <b>pinned</b>
 * and keeps the connection.
 *
 * <h3>Pin triggers</h3>
 * <ul>
 *   <li><b>Transaction</b> ({@link #begin}, until {@link #commit}/{@link #rollback}): an explicit
 *       BEGIN/START TRANSACTION, JDBC autocommit=false, MySQL {@code SET autocommit=0}, SQL Server
 *       IMPLICIT_TRANSACTIONS, Oracle's implicit transaction after an uncommitted write. Reversible.</li>
 *   <li><b>Cursor</b> ({@link #pinCursor}/{@link #unpinCursor}): a SQL-level {@code DECLARE ... CURSOR}
 *       that has not been CLOSEd. Reversible. (Protocol-level portals/result streams do not need a pin:
 *       Warp fully materialises a result before releasing, so nothing remains on the backend.)</li>
 *   <li><b>Session state</b> ({@link #pinSessionState}): state created on the backend that only exists on
 *       that physical connection -- non-LOCAL {@code SET}/{@code RESET}, {@code SET ROLE}, temp tables,
 *       session advisory locks, {@code LISTEN}, SQL-level {@code PREPARE}, sequence {@code currval},
 *       session-scoped functions (see {@link SessionStatePins}). <b>Irreversible</b>: the session stays
 *       pinned until it disconnects, because Warp cannot prove the state is gone.</li>
 * </ul>
 *
 * <h3>Guarantees</h3>
 * <ul>
 *   <li>A connection is only ever released outside a transaction; if an open one is found anyway it is
 *       rolled back first, like HikariCP does on close.</li>
 *   <li>What Warp applied to the physical connection (RLS identity, db_emulation, tenant search_path) is
 *       tracked per physical connection ({@link PhysicalSessionState}) and reconciled by the executor at
 *       every statement, so identity never leaks between sessions sharing a connection.</li>
 *   <li>{@code WARP_MULTIPLEX_SESSIONS=false} makes every lease permanently pinned after its first
 *       borrow, i.e. the old hold-for-the-session behaviour.</li>
 * </ul>
 *
 * Not thread-safe: one instance belongs to one session thread.
 */
public final class SessionConnectionLease implements AutoCloseable {

    /** Opens (borrows from the pool) a backend connection. */
    @FunctionalInterface
    public interface Opener {
        Connection open() throws SQLException;
    }

    /** {@code WARP_MULTIPLEX_SESSIONS}; anything but "false" (case-insensitive) means on. Read once. */
    private static final boolean ENV_ENABLED = !"false".equalsIgnoreCase(System.getenv("WARP_MULTIPLEX_SESSIONS"));

    public static boolean multiplexingEnabledByEnv() {
        return ENV_ENABLED;
    }

    private final Opener opener;
    private final java.util.function.Consumer<Connection> evictor;
    private final boolean multiplex;
    private Connection connection;
    private boolean inTransaction;
    private int openCursors;
    private String sessionStateReason;
    // Plain session settings (SET name = value) the client made, name -> statement, in order. They do not pin:
    // they are recorded here and replayed onto whichever physical connection the session gets next
    // (syncSettings), while PhysicalSessionState remembers what each physical connection carries so an unchanged
    // connection costs nothing.
    private final java.util.LinkedHashMap<String, String> settings = new java.util.LinkedHashMap<>();
    private long borrows;
    private long releases;

    public SessionConnectionLease(Opener opener) {
        this(opener, null, ENV_ENABLED);
    }

    public SessionConnectionLease(Opener opener, boolean multiplex) {
        this(opener, null, multiplex);
    }

    /** @param evictor removes a connection from the pool instead of returning it (used when session state
     *         could not be reset); may be null */
    public SessionConnectionLease(Opener opener, java.util.function.Consumer<Connection> evictor, boolean multiplex) {
        this.opener = opener;
        this.evictor = evictor;
        this.multiplex = multiplex;
    }

    /** The backend connection for the next statement: the held one, or a fresh borrow. */
    public Connection acquire() throws SQLException {
        Connection held = connection;
        if (held != null) {
            return held;
        }
        Connection fresh = opener.open();
        borrows++;
        try {
            if (!fresh.getAutoCommit()) {
                fresh.setAutoCommit(true);
            }
            syncSettings(fresh);
        } catch (SQLException | RuntimeException e) {
            // a connection we failed to bring to the session's state must not go back to the pool as is
            if (evictor != null) {
                evictor.accept(fresh);
            }
            try {
                fresh.close();
            } catch (SQLException ignored) {
                // already evicted
            }
            throw e;
        }
        connection = fresh;
        return fresh;
    }

    /** Makes {@code fresh} carry exactly this session's recorded settings: nothing to do when it already does
     * (the usual case), otherwise {@code RESET ALL} (which also drops what Warp itself had applied -- identity,
     * emulation, tenant path -- so that is marked clean/unknown accordingly) and a replay of the session's own. */
    private void syncSettings(Connection fresh) throws SQLException {
        PhysicalSessionState.State st = PhysicalSessionState.of(fresh);
        if (!st.settingsUnknown && st.settings.equals(settings)) {
            return;
        }
        try (java.sql.Statement stmt = fresh.createStatement()) {
            if (st.hasSettings()) {
                stmt.execute("RESET ALL");
                st.clearWarpApplied();
            }
            if (!settings.isEmpty()) {
                stmt.execute(String.join(";", settings.values()));
            }
        }
        st.settings = snapshotOfSettings();
        st.settingsUnknown = false;
    }

    private java.util.Map<String, String> snapshotOfSettings() {
        return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(settings));
    }

    /** The client ran a plain {@code SET name = value} that succeeded on the held connection: remember it
     * for replay instead of pinning. Call while the connection is still held (before releaseIfIdle). */
    public void recordSetting(String name, String statement) {
        settings.put(name, statement);
        settingsChangedOnHeldConnection(name);
    }

    /** {@code RESET name} ({@code "*"} = RESET ALL) succeeded on the held connection. */
    public void forgetSetting(String name) {
        if ("*".equals(name)) {
            settings.clear();
        } else {
            settings.remove(name);
        }
        settingsChangedOnHeldConnection(name);
    }

    private void settingsChangedOnHeldConnection(String name) {
        Connection c = connection;
        if (c == null) {
            return;
        }
        PhysicalSessionState.State st = PhysicalSessionState.of(c);
        st.settings = snapshotOfSettings();
        st.settingsUnknown = false;
        if ("*".equals(name)) {
            st.clearWarpApplied(); // RESET ALL also dropped the identity / emulation / tenant path Warp had set
        } else if (name.startsWith("warp.")) {
            st.rlsUnknown = true;  // a client must not be able to keep an identity it set itself: re-assert ours
        }
    }

    public boolean hasSettings() {
        return !settings.isEmpty();
    }

    public boolean isHeld() {
        return connection != null;
    }

    /** Starts a transaction on the (borrowed if needed) connection and pins it until commit/rollback. */
    public Connection begin() throws SQLException {
        Connection c = acquire();
        c.setAutoCommit(false);
        inTransaction = true;
        return c;
    }

    public boolean inTransaction() {
        return inTransaction;
    }

    /** Commits the open transaction (a no-op if none was begun on this lease), returns to autocommit and
     * unpins. Does not release; call {@link #releaseIfIdle} once the statement's response is built. */
    public void commit() throws SQLException {
        Connection c = connection;
        boolean open = inTransaction;
        inTransaction = false;
        openCursors = 0; // non-HOLD cursors die with the transaction
        if (c != null && open) {
            c.commit();
            c.setAutoCommit(true);
        }
    }

    public void rollback() throws SQLException {
        Connection c = connection;
        boolean open = inTransaction;
        inTransaction = false;
        openCursors = 0;
        if (c != null && open) {
            try {
                c.rollback();
            } finally {
                // a rolled-back transaction also rolls back SET/set_config issued inside it
                PhysicalSessionState.invalidate(c);
            }
            c.setAutoCommit(true);
        }
    }

    public void pinCursor() {
        openCursors++;
    }

    public void unpinCursor() {
        if (openCursors > 0) {
            openCursors--;
        }
    }

    /** Irreversible: keeps the current physical connection for the rest of the session. */
    public void pinSessionState(String reason) {
        if (sessionStateReason == null) {
            sessionStateReason = reason;
        }
    }

    public boolean isPinned() {
        return !multiplex || inTransaction || openCursors > 0 || sessionStateReason != null;
    }

    /** Why the lease cannot be released right now, or {@code null} if it can. */
    public String pinReason() {
        if (!multiplex) {
            return "multiplexing disabled (WARP_MULTIPLEX_SESSIONS=false)";
        }
        if (inTransaction) {
            return "open transaction";
        }
        if (openCursors > 0) {
            return "open cursor";
        }
        return sessionStateReason;
    }

    public boolean multiplexing() {
        return multiplex;
    }

    /** Returns the connection to the pool unless the session is pinned. Cheap no-op if nothing is held. */
    public void releaseIfIdle() {
        if (connection != null && !isPinned()) {
            release();
        }
    }

    private void release() {
        Connection c = connection;
        connection = null;
        releases++;
        try {
            if (!c.getAutoCommit()) {
                // never hand a connection with an open transaction to the next borrower
                c.rollback();
                PhysicalSessionState.invalidate(c);
                c.setAutoCommit(true);
            }
        } catch (SQLException e) {
            PhysicalSessionState.invalidate(c);
        } finally {
            try {
                c.close();
            } catch (SQLException ignored) {
                // the pool evicts a connection it cannot reset
            }
        }
        inTransaction = false;
    }

    /** Session end: always releases, rolling back anything still open. A session that created backend
     * session state (see {@link #pinSessionState}) first has that state reset so it cannot leak to the next
     * borrower of the physical connection; if the reset fails the connection is evicted instead. */
    @Override
    public void close() {
        Connection c = connection;
        if (c == null) {
            return;
        }
        boolean poisoned = false;
        if (sessionStateReason != null) {
            try {
                if (!c.getAutoCommit()) {
                    c.rollback();
                    c.setAutoCommit(true);
                }
                SessionStateReset.reset(c);
            } catch (SQLException | RuntimeException e) {
                poisoned = true;
            }
        }
        if (poisoned && evictor != null) {
            connection = null;
            releases++;
            try {
                evictor.accept(c);
            } finally {
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // already evicted
                }
            }
            return;
        }
        release();
    }

    public long borrowCount() {
        return borrows;
    }

    public long releaseCount() {
        return releases;
    }
}
