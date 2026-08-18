package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * A session's handle to its backend connection under manual-commit
 * semantics (Oracle wire's explicit COMMIT/ROLLBACK function codes — see
 * {@code com.polygres.wire.orawire.session.RequestLoop}): no physical connection
 * is borrowed from the pool until the first statement of a transaction
 * actually runs ({@link #get()}), and it's returned to the pool
 * (via {@code Connection.close()}) the moment that transaction ends
 * ({@link #commit()}/{@link #rollback()}) — not held for the whole client
 * session. An idle client between transactions costs zero backend
 * connections, same as an idle client that hasn't queried at all yet.
 *
 * <p>{@code schemaUsername}, when non-null, re-issues {@code SET
 * search_path} on every fresh borrow — required because the pool is shared
 * across sessions logged in as different Oracle usernames, so a physical
 * connection handed back by the pool could have any prior borrower's schema
 * still set (see {@code PgBackendPool}'s original per-session version of
 * this same mapping).
 */
public final class LazyPooledConnection implements AutoCloseable {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection open() throws SQLException;
    }

    private final ConnectionSupplier supplier;
    private final String schemaUsername;
    private Connection current;

    public LazyPooledConnection(ConnectionSupplier supplier, String schemaUsername) {
        this.supplier = supplier;
        this.schemaUsername = schemaUsername;
    }

    /**
     * Wraps a connection that's already open and not pool-managed (e.g. an
     * XA branch from {@link com.polygres.wire.xa.XaBackendFactory}, which has
     * its own enlist/prepare/commit lifecycle that doesn't mix with generic
     * pooling — see that class's javadoc). {@link #commit()}/{@link
     * #rollback()} delegate straight through instead of releasing anything
     * back to a pool.
     */
    public static LazyPooledConnection alreadyOpen(Connection connection) {
        LazyPooledConnection wrapper = new LazyPooledConnection(() -> connection, null);
        wrapper.current = connection;
        return wrapper;
    }

    public Connection get() throws SQLException {
        if (current == null) {
            current = supplier.open();
            current.setAutoCommit(false);
            if (schemaUsername != null) {
                String schema = schemaUsername.toLowerCase();
                if (!SAFE_IDENTIFIER.matcher(schema).matches()) {
                    throw new SQLException("unsupported username as schema name: " + schemaUsername);
                }
                try (Statement stmt = current.createStatement()) {
                    stmt.execute("SET search_path TO \"" + schema + "\", public");
                }
            }
        }
        return current;
    }

    public void commit() throws SQLException {
        if (current != null) {
            current.commit();
            release();
        }
    }

    public void rollback() throws SQLException {
        if (current != null) {
            current.rollback();
            release();
        }
    }

    @Override
    public void close() throws SQLException {
        if (current != null) {
            release();
        }
    }

    private void release() throws SQLException {
        Connection toClose = current;
        current = null;
        toClose.close(); // returns to the pool — see BackendConnectionPools
    }
}
