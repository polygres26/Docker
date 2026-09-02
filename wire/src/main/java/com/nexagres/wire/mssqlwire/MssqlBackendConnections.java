package com.nexagres.wire.mssqlwire;

import com.nexagres.wire.core.BackendConnectionPools;
import com.nexagres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.SQLException;

/** As {@code MySqlBackendConnections}, for {@code WARP_MSSQLWIRE_BACKEND=sqlserver}: a pooled
 * connection straight to a real SQL Server instance instead of the dialect-translated Postgres
 * backend every other mssqlwire connection uses. {@code encrypt=false;trustServerCertificate=true}
 * matches this project's own real-SQL-Server test fixtures ({@code RealAzureSqlEdge}) -- a
 * production deployment pointed at a real SQL Server with a real certificate would want these
 * default to their real secure values instead, same tradeoff {@code WARP_PG_SSLMODE}/{@code
 * WARP_PG_SSLROOTCERT} already make explicit for the Postgres side. */
public final class MssqlBackendConnections {

    public static Connection open(ServerOptions options) throws SQLException {
        String url = "jdbc:sqlserver://" + options.mssqlHost() + ":" + options.mssqlPort()
                + ";databaseName=" + options.mssqlDatabase() + ";encrypt=false;trustServerCertificate=true";
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.mssqlUser());
        return BackendConnectionPools.borrow(poolKey, url, options.mssqlUser(), options.mssqlPassword());
    }

    private MssqlBackendConnections() {
    }
}
