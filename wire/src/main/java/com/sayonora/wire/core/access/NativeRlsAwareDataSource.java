package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.sql.DataSource;

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
