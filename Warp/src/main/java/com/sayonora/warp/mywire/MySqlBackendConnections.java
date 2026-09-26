package com.sayonora.warp.mywire;

import com.sayonora.warp.core.BackendConnectionPools;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.SQLException;

public final class MySqlBackendConnections {

    /** Real, previously-undiscovered bug fixed here, found live while writing the first real
     * end-to-end test for this class: this used to build a {@code jdbc:mariadb://} URL, but the
     * only MySQL-family JDBC driver this project actually depends on (see {@code pom.xml}'s own
     * pinned {@code mysql-connector-j}) is {@code com.mysql.cj.jdbc.Driver}, whose own {@code
     * acceptsURL} only recognizes {@code jdbc:mysql:} -- {@code jdbc:mariadb:} needs the separate
     * {@code org.mariadb.jdbc.Driver} class, which isn't on this project's classpath at all. The
     * mismatch surfaced as HikariCP's pool failing to initialize ("Driver ... claims to not accept
     * jdbcUrl"), which in turn aborted the mywire session mid-query with a raw connection reset
     * rather than a real MySQL error packet -- confirmed live, not a hypothetical. */
    /** The one JDBC URL both {@link #open} and {@code Main}'s registered native targets build
     * from -- one source, so they can't drift (same URL + user => same pool key). */
    public static String jdbcUrl(ServerOptions options) {
        return "jdbc:mysql://" + options.mysqlHost() + ":" + options.mysqlPort() + "/" + options.mysqlDatabase();
    }

    public static Connection open(ServerOptions options) throws SQLException {
        String url = jdbcUrl(options);
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.mysqlUser());
        return BackendConnectionPools.borrow(poolKey, url, options.mysqlUser(), options.mysqlPassword());
    }

    private MySqlBackendConnections() {
    }
}
