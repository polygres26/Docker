package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.SQLException;

public record BackendTarget(String name, String jdbcUrl, String user, String password,
        com.polygres.wire.server.ServerOptions failoverOptions) {

    public BackendTarget(String name, String jdbcUrl, String user, String password) {
        this(name, jdbcUrl, user, password, null);
    }

    public SourceDialect dialect() {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return SourceDialect.POSTGRES;
        }
        return null;
    }

    public Connection open() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(true);
        return connection;
    }

    public Connection openManualCommit() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(false);
        return connection;
    }

    private Connection borrow() throws SQLException {
        if (failoverOptions != null) {
            return com.polygres.wire.pgwire.PgConnections.open(failoverOptions);
        }
        
        return BackendConnectionPools.borrow(BackendConnectionPools.poolKeyFor(jdbcUrl, user), jdbcUrl, user, password);
    }
}
