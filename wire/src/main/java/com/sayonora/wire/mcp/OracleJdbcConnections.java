package com.sayonora.wire.mcp;

import com.sayonora.wire.core.BackendConnectionPools;
import com.sayonora.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.SQLException;

/** As {@code MySqlBackendConnections}/{@code MssqlBackendConnections}, for {@code
 * WARP_MCP_BACKEND=oracle}: a pooled connection straight to a real Oracle instance instead of the
 * dialect-translated Postgres backend the MCP frontend uses by default. Lives under {@code mcp}
 * rather than {@code orawire} because it's a plain ojdbc11 JDBC connection (the same driver
 * BackendConnectionPools already pools every other JDBC backend through) -- orawire's own native
 * mode is a much deeper thing, a real TTC/TNS-level pass-through ({@code NativeOracleExecutor})
 * needed because orawire emulates Oracle's own wire protocol byte-for-byte; MCP has no such wire
 * protocol to emulate; it's an HTTP/JSON-RPC tool interface that just needs a real JDBC connection
 * to run SQL through, the same way this class's mywire/mssqlwire siblings do for their own native
 * modes. Uses its own gateway-held {@code WARP_ORACLE_USER}/{@code WARP_ORACLE_PASSWORD}
 * credential (see {@code ServerOptions.McpBackendMode}'s own javadoc for why MCP needs one where
 * orawire's native mode doesn't). */
public final class OracleJdbcConnections {

    /** The one JDBC URL both {@link #open} and {@code Main}'s registered
     * {@code BackendRegistry#MCP_NATIVE_DEFAULT_NAME} target build from -- one source, so the
     * two can't drift (same URL + user => same {@code BackendConnectionPools} pool key). */
    public static String jdbcUrl(ServerOptions options) {
        return "jdbc:oracle:thin:@" + options.oracleHost() + ":" + options.oraclePort()
                + "/" + options.oracleServiceName();
    }

    public static Connection open(ServerOptions options) throws SQLException {
        String url = jdbcUrl(options);
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.oracleUser());
        return BackendConnectionPools.borrow(poolKey, url, options.oracleUser(), options.oraclePassword());
    }

    private OracleJdbcConnections() {
    }
}
