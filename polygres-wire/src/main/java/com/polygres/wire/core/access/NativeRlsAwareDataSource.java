package com.polygres.wire.core.access;

import com.polygres.wire.core.AccessContext;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Makes native RLS/VPD pass-through ({@link NativeRlsSessionInitializer}) work for {@code
 * FederationStage}'s federated Calcite connection, where {@code com.polygres.wire.core.JdbcBackendExecutor}'s own
 * "call it once on the one connection I'm about to run the statement on" approach doesn't apply —
 * a federated query's real backend connections are opened and closed internally by Calcite's JDBC
 * adapter (per real backend, per query execution), never handed to {@code FederationStage} itself
 * to call {@link NativeRlsSessionInitializer#initialize} on directly.
 *
 * <p>Wraps the real per-backend {@link DataSource} {@code FederationStage} already builds
 * ({@code JdbcSchema.dataSource(...)}): every {@link #getConnection()}/{@link
 * #getConnection(String, String)} call opens the real connection as before, then — if {@code
 * contextSupplier} currently has a non-{@link AccessContext#isAnonymous() anonymous} context to
 * give it (see {@code FederationStage}'s {@code CURRENT_ACCESS_CONTEXT} {@code ThreadLocal},
 * which it sets for the duration of executing one statement) — runs {@code initializer} against
 * that freshly opened connection before handing it back. Calcite's JDBC adapter opens a fresh
 * connection per query execution against this backend (confirmed: it doesn't hold one open across
 * separate federated queries), so this fires correctly on every real query, not just the first one
 * — no caching/skip logic needed here.
 *
 * <p>{@code contextSupplier} rather than a fixed {@link AccessContext}: a single wrapped {@link
 * DataSource} instance lives for the lifetime of one cached federated connection (see {@code
 * FederationStage}'s {@code connectionCache}, keyed by backend set, not by caller), and is reused
 * across many different callers' statements over that connection's lifetime — the context has to
 * be resolved fresh at connection-open time, not baked in at wrap time.
 */
public final class NativeRlsAwareDataSource implements DataSource {

    private final DataSource delegate;
    private final NativeRlsSessionInitializer initializer;
    private final Supplier<AccessContext> contextSupplier;

    public NativeRlsAwareDataSource(DataSource delegate, NativeRlsSessionInitializer initializer,
            Supplier<AccessContext> contextSupplier) {
        this.delegate = delegate;
        this.initializer = initializer;
        this.contextSupplier = contextSupplier;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return initializeAndReturn(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return initializeAndReturn(delegate.getConnection(username, password));
    }

    private Connection initializeAndReturn(Connection connection) throws SQLException {
        AccessContext context = contextSupplier.get();
        if (context != null && !context.isAnonymous()) {
            initializer.initialize(connection, context);
        }
        return connection;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }
}
