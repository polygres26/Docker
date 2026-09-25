package com.sayonora.wire.xa;

import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ErrorCatalog;
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
 * <p><b>Postgres, Oracle, SQL Server, and MySQL/MariaDB today</b> -- matching {@link
 * com.sayonora.wire.core.BackendDriverRegistry}'s own currently-supported engine list. Every one
 * of these ships a real vendor {@code XADataSource}: {@code PGXADataSource}, {@code
 * OracleXADataSource}, Microsoft's {@code SQLServerXADataSource}, and (this project's own real,
 * Oracle-published MySQL Connector/J, NOT the MariaDB driver the sibling Omnigate project found
 * live has no usable {@code XADataSource} at all) {@code MysqlXADataSource}. All four have been
 * live-verified end to end -- a real prepare+commit against a real instance of each engine, not
 * assumed from the mere existence of the vendor class.
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
     * switchover, a credential rotation, an operator editing {@code WARP_BACKENDS}), and
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
     *     mirrors {@link com.sayonora.wire.core.BackendDriverRegistry}'s own URL-prefix dispatch
     *     shape, kept as a SEPARATE lookup rather than merged into it: a backend can in principle
     *     be a fine plain read/write or federation target without being a real XA participant,
     *     even though today every {@link com.sayonora.wire.core.BackendDriverRegistry}-supported
     *     engine also has real XA support -- "supports this engine at all" and "supports this
     *     engine for XA" are still genuinely different questions, kept as different lookups. */
    private static XADataSource xaDataSourceFor(String url) throws SQLException {
        String lower = url == null ? "" : url.toLowerCase(java.util.Locale.ROOT);
        if (lower.startsWith("jdbc:postgresql:")) {
            return new PGXADataSource();
        }
        if (lower.startsWith("jdbc:oracle:")) {
            return new OracleXADataSource();
        }
        if (lower.startsWith("jdbc:sqlserver:")) {
            return new SQLServerXADataSource();
        }
        if (lower.startsWith("jdbc:mysql:") || lower.startsWith("jdbc:mariadb:")) {
            return new MysqlXADataSource();
        }
        return null;
    }

    private static XaBranch openInternal(XADataSource dataSource, String url, String user, String password)
            throws SQLException {
        setUrl(dataSource, url);
        if (user != null) {
            setCredentials(dataSource, user, password);
        }
        XAConnection xaConnection = dataSource.getXAConnection();
        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);
        return new XaBranch(connection, xaConnection.getXAResource(), xaConnection);
    }

    private static void setUrl(XADataSource dataSource, String url) {
        if (dataSource instanceof PGXADataSource pg) {
            pg.setUrl(url);
        } else if (dataSource instanceof OracleXADataSource oracle) {
            oracle.setURL(url);
        } else if (dataSource instanceof SQLServerXADataSource mssql) {
            mssql.setURL(url);
        } else if (dataSource instanceof MysqlXADataSource mysql) {
            mysql.setUrl(url);
        }
    }

    private static void setCredentials(XADataSource dataSource, String user, String password) {
        if (dataSource instanceof PGXADataSource pg) {
            pg.setUser(user);
            pg.setPassword(password);
        } else if (dataSource instanceof OracleXADataSource oracle) {
            oracle.setUser(user);
            oracle.setPassword(password);
        } else if (dataSource instanceof SQLServerXADataSource mssql) {
            mssql.setUser(user);
            mssql.setPassword(password);
        } else if (dataSource instanceof MysqlXADataSource mysql) {
            mysql.setUser(user);
            mysql.setPassword(password);
        }
    }

    private XaBackendFactory() {
    }
}
