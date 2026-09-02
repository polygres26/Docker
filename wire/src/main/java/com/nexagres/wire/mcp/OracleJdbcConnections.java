package com.nexagres.wire.mcp;

import com.nexagres.wire.core.BackendConnectionPools;
import com.nexagres.wire.server.ServerOptions;
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

    public static Connection open(ServerOptions options) throws SQLException {
        String url = "jdbc:oracle:thin:@" + options.oracleHost() + ":" + options.oraclePort()
                + "/" + options.oracleServiceName();
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.oracleUser());
        return BackendConnectionPools.borrow(poolKey, url, options.oracleUser(), options.oraclePassword());
    }

    private OracleJdbcConnections() {
    }
}
