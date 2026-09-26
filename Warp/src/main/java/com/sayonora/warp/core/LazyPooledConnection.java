package com.sayonora.warp.core;

import com.sayonora.warp.core.access.PhysicalSessionState;
import com.sayonora.warp.core.access.SessionStateReconciler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * orawire's backend connection for one client session: opened lazily on first use, in manual-commit mode
 * (Oracle's implicit transaction), and returned to the pool when the transaction ends (COMMIT/ROLLBACK) --
 * and, with connection multiplexing (WARP_MULTIPLEX_SESSIONS, see {@link SessionConnectionLease} for the model
 * and the pin list), also as soon as a statement finished without leaving a transaction or session state on it
 * ({@link #releaseIfIdle}: RequestLoop runs autocommit statements and plain reads in autocommit mode and
 * releases afterwards, while an uncommitted write keeps the connection until COMMIT/ROLLBACK).
 */
public final class LazyPooledConnection implements AutoCloseable {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection open() throws SQLException;
    }

    private final ConnectionSupplier supplier;
    private final String schemaUsername;
    private final Consumer<Connection> evictor;
    private final boolean multiplex;
    private Connection current;
    private boolean sessionStatePinned;

    public LazyPooledConnection(ConnectionSupplier supplier, String schemaUsername) {
        this(supplier, schemaUsername, null, SessionConnectionLease.multiplexingEnabledByEnv());
    }

    public LazyPooledConnection(ConnectionSupplier supplier, String schemaUsername, Consumer<Connection> evictor,
            boolean multiplex) {
        this.supplier = supplier;
        this.schemaUsername = schemaUsername;
        this.evictor = evictor;
        this.multiplex = multiplex;
    }

    public static LazyPooledConnection alreadyOpen(Connection connection) {
        // multiplexing off: the caller owns this connection and it must stay open for the wrapper's whole life
        LazyPooledConnection wrapper = new LazyPooledConnection(() -> connection, null, null, false);
        wrapper.current = connection;
        return wrapper;
    }

    public boolean multiplexing() {
        return multiplex;
    }

    public Connection get() throws SQLException {
        if (current == null) {
            Connection opened = supplier.open();
            try {
                if (schemaUsername != null) {
                    String schema = schemaUsername.toLowerCase();
                    if (!SAFE_IDENTIFIER.matcher(schema).matches()) {
                        throw ErrorCatalog.sqlException("ERR_UNSUPPORTED_SCHEMA_USERNAME", schemaUsername);
                    }
                    // Recorded per physical connection: a connection that already carries this tenant's
                    // search_path (the usual case when the pool hands back the same one) costs no round trip.
                    // Done in autocommit mode, before the implicit transaction begins, so it never leaves a
                    // transaction open behind it.
                    SessionStateReconciler.ensureTenantPath(opened, PhysicalSessionState.of(opened), schema);
                }
                opened.setAutoCommit(false);
            } catch (SQLException | RuntimeException e) {
                try {
                    opened.close();
                } catch (SQLException ignored) {
                    // pool evicts
                }
                throw e;
            }
            current = opened;
        }
        return current;
    }

    public void commit() throws SQLException {
        if (current != null) {
            current.commit();
            releaseUnlessPinned();
        }
    }

    public void rollback() throws SQLException {
        if (current != null) {
            try {
                current.rollback();
            } finally {
                // a rolled-back transaction also rolls back SET/set_config issued inside it
                PhysicalSessionState.invalidate(current);
            }
            releaseUnlessPinned();
        }
    }

    /** The session created backend session state (sequence currval, DBMS_OUTPUT, ALTER SESSION, temp
     * table, ...): keep this physical connection until the session ends, also across COMMIT/ROLLBACK (see
     * {@link SessionStatePins}). No effect with multiplexing off, which keeps the historical release-on-commit
     * behaviour exactly. */
    public void pinSessionState() {
        if (multiplex) {
            sessionStatePinned = true;
        }
    }

    /** Returns the connection to the pool now, if one is held and the session is not pinned. The CALLER
     * guarantees no transaction is open on it (RequestLoop's bookkeeping); it is rolled back defensively
     * if one is found anyway. No effect with multiplexing off. */
    public void releaseIfIdle() throws SQLException {
        if (current != null && multiplex && !sessionStatePinned) {
            release(false);
        }
    }

    public boolean isOpen() {
        return current != null;
    }

    @Override
    public void close() throws SQLException {
        if (current == null) {
            return;
        }
        if (sessionStatePinned) {
            Connection c = current;
            try {
                if (!c.getAutoCommit()) {
                    c.rollback();
                    c.setAutoCommit(true);
                }
                SessionStateReset.reset(c);
            } catch (SQLException | RuntimeException e) {
                if (evictor != null) {
                    current = null;
                    try {
                        evictor.accept(c);
                    } finally {
                        c.close();
                    }
                    return;
                }
            }
        }
        release(true);
    }

    private void releaseUnlessPinned() throws SQLException {
        if (!(multiplex && sessionStatePinned)) {
            release(false);
        }
    }

    /** @param mayHaveOpenTransaction true at session end, where a transaction (and the SET/set_config Warp ran
     *         inside it) may still be open and is rolled back -- so what Warp recorded as applied to the
     *         physical connection is no longer trustworthy. The other callers (after commit/rollback, or after
     *         an autocommit statement) leave nothing open; the rollback below is then a client-side no-op. */
    private void release(boolean mayHaveOpenTransaction) throws SQLException {
        Connection toClose = current;
        current = null;
        try {
            if (!toClose.getAutoCommit()) {
                // never hand a connection with an open transaction to the next borrower
                toClose.rollback();
                if (mayHaveOpenTransaction) {
                    PhysicalSessionState.invalidate(toClose);
                }
                toClose.setAutoCommit(true);
            }
        } catch (SQLException e) {
            PhysicalSessionState.invalidate(toClose);
        } finally {
            toClose.close();
        }
    }
}
