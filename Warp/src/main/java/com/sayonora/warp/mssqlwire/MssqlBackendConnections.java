package com.sayonora.warp.mssqlwire;

import com.sayonora.warp.core.BackendConnectionPools;
import com.sayonora.warp.server.ServerOptions;
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

    // Lazily built, process-wide: BRIDGE mode (ServerOptions.MssqlBackendMode#BRIDGE) shares one
    // small, bounded pool across every client session -- see MssqlBridgePool, mirroring
    // MySqlBackendConnections' own bridgePool() field.
    private static volatile MssqlBridgePool bridgePool;

    private static MssqlBridgePool bridgePool(ServerOptions options) {
        MssqlBridgePool pool = bridgePool;
        if (pool == null) {
            synchronized (MssqlBackendConnections.class) {
                pool = bridgePool;
                if (pool == null) {
                    pool = MssqlBridgePool.fromServerOptions(options);
                    bridgePool = pool;
                }
            }
        }
        return pool;
    }

    /** The one JDBC URL both {@link #open} and {@code Main}'s registered native targets build
     * from -- one source, so they can't drift (same URL + user => same pool key). */
    public static String jdbcUrl(ServerOptions options) {
        return "jdbc:sqlserver://" + options.mssqlHost() + ":" + options.mssqlPort()
                + ";databaseName=" + options.mssqlDatabase() + ";encrypt=false;trustServerCertificate=true";
    }

    /** BRIDGE mode (see {@link ServerOptions.MssqlBackendMode#BRIDGE}) uses a dedicated, bounded
     * {@link MssqlBridgePool} instead of the generic {@link BackendConnectionPools} every other
     * caller of this method shares -- mirrors {@code MySqlBackendConnections}'s own split. */
    public static Connection open(ServerOptions options) throws SQLException {
        if (options.mssqlBackendMode() == ServerOptions.MssqlBackendMode.BRIDGE) {
            return bridgePool(options).checkout();
        }
        String url = jdbcUrl(options);
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.mssqlUser());
        return BackendConnectionPools.borrow(poolKey, url, options.mssqlUser(), options.mssqlPassword());
    }

    private MssqlBackendConnections() {
    }
}
