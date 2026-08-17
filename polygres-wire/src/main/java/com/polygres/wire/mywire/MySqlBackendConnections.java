package com.polygres.wire.mywire;

import com.polygres.wire.core.BackendConnectionPools;
import com.polygres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Opens a JDBC connection to a real MySQL/MariaDB backend, via the
 * mariadb-java-client dependency already present for {@code
 * POLYWIRE_BACKENDS} target support (see pom.xml's comment on that
 * dependency). Mirrors {@link com.polygres.wire.pgwire.PgConnections#open}'s
 * shape (pooled via {@link BackendConnectionPools}, one pool key per
 * host/port/database/user) but deliberately without that class's
 * failover/standby machinery — {@code ORAPG_MYWIRE_BACKEND=mysql} is a new,
 * narrower mode (see {@link ServerOptions#mywireNativeBackend()}'s javadoc)
 * that hasn't needed it yet.
 */
public final class MySqlBackendConnections {

    public static Connection open(ServerOptions options) throws SQLException {
        String url = "jdbc:mariadb://" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase();
        String poolKey = "mysql:" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase()
                + "/" + options.mysqlUser();
        return BackendConnectionPools.borrow(poolKey, url, options.mysqlUser(), options.mysqlPassword());
    }

    private MySqlBackendConnections() {
    }
}
