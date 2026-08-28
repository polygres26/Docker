package com.nexagres.wire.mywire;

import com.nexagres.wire.core.BackendConnectionPools;
import com.nexagres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.SQLException;

public final class MySqlBackendConnections {

    public static Connection open(ServerOptions options) throws SQLException {
        String url = "jdbc:mariadb://" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase();
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.mysqlUser());
        return BackendConnectionPools.borrow(poolKey, url, options.mysqlUser(), options.mysqlPassword());
    }

    private MySqlBackendConnections() {
    }
}
