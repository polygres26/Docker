package com.polygres.wire.xa;

import com.polygres.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.postgresql.xa.PGXADataSource;

public final class XaBackendFactory {

    public record XaBranch(Connection connection, XAResource resource, XAConnection xaConnection) {
    }

    public static XaBranch open(BackendTarget target) throws SQLException {
        String url = target.jdbcUrl();
        if (url == null || !url.startsWith("jdbc:postgresql:")) {
            throw new SQLException("XA unsupported for backend \"" + target.name()
                    + "\": not a Postgres JDBC URL (" + url + ") — PolyWire's XA coordinator is Postgres-only");
        }
        return openInternal(url, target.user(), target.password());
    }

    /** As {@link #open(BackendTarget)}, but connects directly to a captured jdbcUrl/user/password
     * instead of resolving them from a {@code BackendRegistry} entry by name -- see {@code
     * XaRecoveryLog.Branch}'s javadoc for why crash recovery needs this: a backend NAME can be
     * repointed to a different physical target after a branch was prepared against it (a
     * switchover, a credential rotation, an operator editing {@code POLYWIRE_BACKENDS}), and
     * recovery must reconnect to the exact target the branch is actually still prepared on, not
     * whatever that name currently resolves to. */
    public static XaBranch openDirect(String jdbcUrl, String user, String password) throws SQLException {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:postgresql:")) {
            throw new SQLException("XA unsupported: not a Postgres JDBC URL (" + jdbcUrl
                    + ") — PolyWire's XA coordinator is Postgres-only");
        }
        return openInternal(jdbcUrl, user, password);
    }

    private static XaBranch openInternal(String url, String user, String password) throws SQLException {
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl(url);
        if (user != null) {
            dataSource.setUser(user);
            dataSource.setPassword(password);
        }
        XAConnection xaConnection = dataSource.getXAConnection();
        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);
        return new XaBranch(connection, xaConnection.getXAResource(), xaConnection);
    }

    private XaBackendFactory() {
    }
}
