package com.nexagres.advisor.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * One source database connection Advisor has been pointed at for scanning. Adapted from
 * Omnigate's {@code com.omnigate.core.BackendTarget} (~/Projects/Omnigate) -- same shape
 * (name + jdbcUrl + credentials, dialect inferred from the URL scheme, connections borrowed
 * from a shared per-target pool) trimmed to the dialects Advisor actually deals with and
 * with routing-specific methods (openManualCommit, XA) dropped since Advisor only ever reads.
 */
public record BackendTarget(String name, String jdbcUrl, String user, String password) {

    /** Inferred from the {@code jdbc:...:} URL prefix; {@code null} if unrecognized. */
    public SourceDialect dialect() {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(Locale.ROOT);
        if (url.startsWith("jdbc:oracle:")) {
            return SourceDialect.ORACLE;
        }
        if (url.startsWith("jdbc:mysql:")) {
            return SourceDialect.MYSQL;
        }
        if (url.startsWith("jdbc:mariadb:")) {
            return SourceDialect.MARIADB;
        }
        if (url.startsWith("jdbc:postgresql:")) {
            return SourceDialect.POSTGRES;
        }
        if (url.startsWith("jdbc:sqlserver:")) {
            return SourceDialect.SQL_SERVER;
        }
        return null;
    }

    /**
     * Read-only autocommit connection borrowed from this target's shared pool. {@code password}
     * may be a {@code vault:...}/{@code cyberark:...} secret reference instead of a literal --
     * resolved here, on every open, so a rotated secret takes effect on the next connection
     * rather than requiring a restart (see {@code com.nexagres.advisor.secrets.SecretResolver}).
     * A literal password round-trips through unchanged.
     */
    public Connection open() throws SQLException {
        String resolvedPassword = com.nexagres.advisor.secrets.SecretResolver.resolve(password);
        Connection connection = BackendConnectionPools.borrow(name, jdbcUrl, user, resolvedPassword);
        connection.setAutoCommit(true);
        connection.setReadOnly(true);
        return connection;
    }
}
