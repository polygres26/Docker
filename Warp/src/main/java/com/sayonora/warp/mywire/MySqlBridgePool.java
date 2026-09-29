package com.sayonora.warp.mywire;

import com.sayonora.warp.server.ServerOptions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * mywire's version of {@link com.sayonora.warp.orawire.backend.OracleBridgePool}: {@link
 * ServerOptions.MySqlBackendMode#BRIDGE}'s real, bounded, shared JDBC connection pool to a real
 * MySQL backend -- many client sessions (one per {@code MySqlWireSessionHandler}) to a few real
 * physical MySQL connections, unlike RELAY's ({@link
 * com.sayonora.warp.orawire.backend.NativeSessionRelay}) 1 raw socket per session. Each {@link
 * #checkout()} hands back a dynamic proxy {@link Connection} whose {@code close()} first resets
 * whatever session state this slice resets, then delegates to the underlying pooled connection's
 * own {@code close()} -- same shape as Oracle's bridge pool, see that class's javadoc for the
 * fuller rationale (HikariCP already a dependency, dynamic-proxy-must-delegate-to-Hikari's-own-
 * connection lesson, etc.), not repeated here.
 *
 * <h2>What is reset on checkout/return, and what is NOT</h2>
 * Reset: any open transaction is rolled back, and every {@link Statement} opened through this
 * connection since its last checkout is closed (closing any open cursor/ResultSet with it). Unlike
 * Oracle's Bridge pool, there is no PL/SQL-package-variable-state equivalent to reset here -- MySQL
 * has no server-side package/global-variable construct comparable to Oracle's. Still NOT reset:
 * session variables set via {@code SET @var := ...} or {@code SET SESSION ...}, temporary tables
 * created on this connection (real MySQL: {@code CREATE TEMPORARY TABLE} is connection-scoped and
 * would leak to the next borrower exactly like Oracle Bridge's temp-table gap), and any {@code
 * SET NAMES}/charset state -- a real, disclosed limitation carried over from the same design
 * tradeoff Oracle's Bridge pool already documents.
 */
public final class MySqlBridgePool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MySqlBridgePool.class);

    private final DataSource dataSource;
    private final Runnable onClose;
    private final int maxSize;
    private volatile boolean closed;

    public MySqlBridgePool(DataSource dataSource, int maxSize) {
        this(dataSource, maxSize, null);
    }

    MySqlBridgePool(DataSource dataSource, int maxSize, Runnable onClose) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("MySQL bridge pool size must be positive, got " + maxSize);
        }
        this.dataSource = dataSource;
        this.maxSize = maxSize;
        this.onClose = onClose;
    }

    /** Builds the pool from Bridge-mode configuration: {@code WARP_MYSQL_HOST}/{@code
     * WARP_MYSQL_PORT}/{@code WARP_MYSQL_DATABASE} for the target, a single shared {@code
     * WARP_MYSQL_USER}/{@code WARP_MYSQL_PASSWORD} service account for every client session (same
     * shared-identity tradeoff as Oracle's Bridge pool), and {@code WARP_MYSQL_BRIDGE_POOL_SIZE}
     * (default 10) as Hikari's {@code maximumPoolSize}. Reuses {@link MySqlBackendConnections#jdbcUrl}
     * so this pool and any other MySQL-backend code path build the identical JDBC URL. */
    public static MySqlBridgePool fromServerOptions(ServerOptions options) {
        int size = ServerOptions.mysqlBridgePoolSize();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(MySqlBackendConnections.jdbcUrl(options));
        config.setUsername(options.mysqlUser());
        config.setPassword(options.mysqlPassword());
        config.setMaximumPoolSize(size);
        config.setPoolName("warp-mysql-bridge");
        // Warp manages autoCommit/transaction boundaries itself (matching every other pooled
        // backend connection in this codebase) -- Hikari must not second-guess that on borrow.
        config.setAutoCommit(true);
        HikariDataSource ds = new HikariDataSource(config);
        return new MySqlBridgePool(ds, size, ds::close);
    }

    public int maxSize() {
        return maxSize;
    }

    public Connection checkout() throws SQLException {
        if (closed) {
            throw new SQLException("MySQL bridge pool is closed");
        }
        Connection raw = dataSource.getConnection();
        try {
            if (!raw.getAutoCommit()) {
                raw.rollback();
            }
        } catch (SQLException | RuntimeException e) {
            closeQuietly(raw);
            throw e;
        }
        return wrap(raw);
    }

    private Connection wrap(Connection raw) {
        PooledConnectionHandler handler = new PooledConnectionHandler(raw);
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(
                MySqlBridgePool.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private void returnConnection(Connection raw, Set<Statement> openStatements) {
        for (Statement s : openStatements) {
            try {
                s.close();
            } catch (SQLException e) {
                log.warn("MySQL bridge pool: failed to close a statement/cursor on return: {}", e.getMessage());
            }
        }
        try {
            if (!raw.getAutoCommit()) {
                raw.rollback();
            }
        } catch (SQLException e) {
            log.warn("MySQL bridge pool: connection failed to reset cleanly on return: {}", e.getMessage());
        } finally {
            closeQuietly(raw);
        }
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException ignored) {
            // already broken; nothing more to do
        }
    }

    @Override
    public void close() {
        closed = true;
        if (onClose != null) {
            onClose.run();
        }
    }

    private final class PooledConnectionHandler implements InvocationHandler {
        private final Connection delegate;
        private final Set<Statement> openStatements =
                Collections.newSetFromMap(new IdentityHashMap<>());
        private volatile boolean returned;

        PooledConnectionHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (name.equals("close")) {
                if (!returned) {
                    returned = true;
                    returnConnection(delegate, openStatements);
                }
                return null;
            }
            if (name.equals("isClosed")) {
                return returned;
            }
            try {
                Object result = method.invoke(delegate, args);
                if (result instanceof Statement && isStatementFactoryMethod(name)) {
                    openStatements.add((Statement) result);
                }
                return result;
            } catch (InvocationTargetException e) {
                throw e.getCause() != null ? e.getCause() : e;
            }
        }

        private boolean isStatementFactoryMethod(String name) {
            return name.equals("createStatement") || name.equals("prepareStatement") || name.equals("prepareCall");
        }
    }
}
