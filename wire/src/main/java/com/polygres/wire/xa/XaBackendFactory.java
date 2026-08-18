package com.polygres.wire.xa;

import com.polygres.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.postgresql.xa.PGXADataSource;

/**
 * Opens one XA branch (a {@link Connection} for running statements plus the {@link XAResource}
 * that coordinates its commit/rollback) against a real Postgres {@link BackendTarget}, using
 * PostgreSQL's own JDBC driver's {@code PGXADataSource} rather than hand-rolling XA support.
 *
 * <p>Ported from {@code com.omnigate.xa.XaBackendFactory}, which additionally branched on JDBC
 * URL prefix to also support {@code oracle.jdbc.xa.client.OracleXADataSource}. That branch is cut
 * entirely here: every {@link BackendTarget} in PolyWire is a Postgres instance (see
 * {@link BackendTarget#dialect()} — always {@code SourceDialect#POSTGRES} or {@code null}), so
 * there is no second dialect to dispatch on. A target whose URL isn't recognized as Postgres
 * fails fast with a clear error instead of silently trying a driver that isn't a dependency here.
 */
public final class XaBackendFactory {

    public record XaBranch(Connection connection, XAResource resource, XAConnection xaConnection) {
    }

    /** One XA branch against a real Postgres {@link BackendTarget} (a {@code POLYWIRE_BACKENDS} entry). */
    public static XaBranch open(BackendTarget target) throws SQLException {
        String url = target.jdbcUrl();
        if (url == null || !url.startsWith("jdbc:postgresql:")) {
            throw new SQLException("XA unsupported for backend \"" + target.name()
                    + "\": not a Postgres JDBC URL (" + url + ") — PolyWire's XA coordinator is Postgres-only");
        }
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl(url);
        if (target.user() != null) {
            dataSource.setUser(target.user());
            dataSource.setPassword(target.password());
        }
        XAConnection xaConnection = dataSource.getXAConnection();
        Connection connection = xaConnection.getConnection();
        // Required, not optional: per the XA contract a connection enlisted via XAResource.start()
        // must not autocommit on its own -- the transaction manager (XaTransaction, here) owns the
        // commit boundary via prepare()/commit(). Found live to matter: without this, a statement
        // run on this connection commits itself immediately and durably regardless of what the
        // later XA end/prepare/commit/rollback sequence does, defeating atomicity entirely (a
        // failed sibling branch's rollback no longer undoes this one -- the whole point of 2PC).
        connection.setAutoCommit(false);
        return new XaBranch(connection, xaConnection.getXAResource(), xaConnection);
    }

    private XaBackendFactory() {
    }
}
