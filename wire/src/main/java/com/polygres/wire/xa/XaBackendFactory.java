package com.polygres.wire.xa;

import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.ErrorCatalog;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import oracle.jdbc.xa.client.OracleXADataSource;
import org.postgresql.xa.PGXADataSource;

/**
 * Real {@code XAResource}-based 2PC branch construction, using each engine's own vendor-provided
 * {@code XADataSource} implementation -- the same real, proven shape the sibling Omnigate project's
 * own {@code XaBackendFactory} uses (its javadoc: "using the vendor-provided XADataSource
 * implementations every major JDBC driver ships ... rather than hand-rolling XA support").
 *
 * <p><b>Postgres and Oracle today, matching {@link com.polygres.wire.core.BackendDriverRegistry}'s
 * own currently-supported engine list</b> -- adding a real, standard {@code XADataSource} (e.g.
 * Microsoft's {@code SQLServerXADataSource}) is a real, scoped follow-up as SQL Server becomes a
 * supported backend engine. MySQL/MariaDB is a genuinely different, harder case: Omnigate's own
 * javadoc found live that the MariaDB JDBC driver ships no usable {@code XADataSource} at all, so a
 * MySQL/MariaDB backend is expected to stay best-effort-only (independent, non-coordinated commits)
 * rather than a real XA participant, not a bug to fix here later.
 */
public final class XaBackendFactory {

    public record XaBranch(Connection connection, XAResource resource, XAConnection xaConnection) {
    }

    public static XaBranch open(BackendTarget target) throws SQLException {
        String url = target.jdbcUrl();
        XADataSource dataSource = xaDataSourceFor(url);
        if (dataSource == null) {
            throw ErrorCatalog.sqlException("ERR_XA_UNSUPPORTED_ENGINE", target.name(), url);
        }
        return openInternal(dataSource, url, target.user(), target.password());
    }

    /** As {@link #open(BackendTarget)}, but connects directly to a captured jdbcUrl/user/password
     * instead of resolving them from a {@code BackendRegistry} entry by name -- see {@code
     * XaRecoveryLog.Branch}'s javadoc for why crash recovery needs this: a backend NAME can be
     * repointed to a different physical target after a branch was prepared against it (a
     * switchover, a credential rotation, an operator editing {@code POLYWIRE_BACKENDS}), and
     * recovery must reconnect to the exact target the branch is actually still prepared on, not
     * whatever that name currently resolves to. */
    public static XaBranch openDirect(String jdbcUrl, String user, String password) throws SQLException {
        XADataSource dataSource = xaDataSourceFor(jdbcUrl);
        if (dataSource == null) {
            throw ErrorCatalog.sqlException("ERR_XA_UNSUPPORTED_ENGINE_DIRECT", jdbcUrl);
        }
        return openInternal(dataSource, jdbcUrl, user, password);
    }

    /** @return a real, configured (but not yet connected) vendor {@code XADataSource} for {@code
     *     url}'s own engine, or {@code null} for an engine with no real XA support wired up --
     *     mirrors {@link com.polygres.wire.core.BackendDriverRegistry}'s own URL-prefix dispatch
     *     shape, kept as a SEPARATE lookup rather than merged into it: a backend can be a fine
     *     plain read/write or federation target without being a real XA participant (MySQL/MariaDB
     *     today -- see this class's own javadoc), so "supports this engine at all" and "supports
     *     this engine for XA" are genuinely different questions. */
    private static XADataSource xaDataSourceFor(String url) throws SQLException {
        String lower = url == null ? "" : url.toLowerCase(java.util.Locale.ROOT);
        if (lower.startsWith("jdbc:postgresql:")) {
            return new PGXADataSource();
        }
        if (lower.startsWith("jdbc:oracle:")) {
            return new OracleXADataSource();
        }
        return null;
    }

    private static XaBranch openInternal(XADataSource dataSource, String url, String user, String password)
            throws SQLException {
        if (dataSource instanceof PGXADataSource pg) {
            pg.setUrl(url);
        } else if (dataSource instanceof OracleXADataSource oracle) {
            oracle.setURL(url);
        }
        if (user != null) {
            if (dataSource instanceof PGXADataSource pg) {
                pg.setUser(user);
                pg.setPassword(password);
            } else if (dataSource instanceof OracleXADataSource oracle) {
                oracle.setUser(user);
                oracle.setPassword(password);
            }
        }
        XAConnection xaConnection = dataSource.getXAConnection();
        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);
        return new XaBranch(connection, xaConnection.getXAResource(), xaConnection);
    }

    private XaBackendFactory() {
    }
}
