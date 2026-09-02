package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

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
                    throw ErrorCatalog.sqlException("ERR_UNSUPPORTED_SCHEMA_USERNAME", schemaUsername);
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
        toClose.close();
    }
}
