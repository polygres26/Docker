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
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl(url);
        if (target.user() != null) {
            dataSource.setUser(target.user());
            dataSource.setPassword(target.password());
        }
        XAConnection xaConnection = dataSource.getXAConnection();
        Connection connection = xaConnection.getConnection();
        
        connection.setAutoCommit(false);
        return new XaBranch(connection, xaConnection.getXAResource(), xaConnection);
    }

    private XaBackendFactory() {
    }
}
