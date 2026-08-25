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

    /** As {@link #open()}, but prefers this target's configured standby for the connection --
     * see {@link com.polygres.wire.pgwire.PgConnections#openForRead}. Falls back to {@link #open()}
     * when this target has no {@code failoverOptions} (i.e. no standby concept at all, the plain
     * {@code BackendConnectionPools}-only path) -- there's nothing to prefer in that case. */
    public Connection openPreferringStandby() throws SQLException {
        if (failoverOptions == null) {
            return open();
        }
        Connection connection = com.polygres.wire.pgwire.PgConnections.openForRead(failoverOptions);
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

        // password may be a "vault:..."/"cyberark:..." reference, not a literal -- resolved on
        // every borrow (not cached on this record) so a rotated secret takes effect on the next
        // connection attempt rather than requiring a restart. A literal password round-trips
        // through SecretRef.parse/SecretResolver.resolve as a no-op.
        String resolvedPassword = com.polygres.wire.secrets.SecretResolver.resolve(password);
        return BackendConnectionPools.borrow(BackendConnectionPools.poolKeyFor(jdbcUrl, user), jdbcUrl, user, resolvedPassword);
    }
}
