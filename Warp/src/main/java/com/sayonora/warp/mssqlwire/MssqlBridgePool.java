package com.sayonora.warp.mssqlwire;

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
 * mssqlwire's version of {@link com.sayonora.warp.orawire.backend.OracleBridgePool}/{@link
 * com.sayonora.warp.mywire.MySqlBridgePool}: {@link ServerOptions.MssqlBackendMode#BRIDGE}'s real,
 * bounded, shared JDBC connection pool to a real SQL Server backend -- many client sessions to a
 * few real physical connections, unlike RELAY's ({@link
 * com.sayonora.warp.orawire.backend.NativeSessionRelay}) 1 raw socket per session. See {@code
 * MySqlBridgePool}'s own javadoc for the fuller HikariCP/dynamic-proxy rationale, not repeated
 * here.
 *
 * <h2>What is reset on checkout/return, and what is NOT</h2>
 * Reset: any open transaction is rolled back, every {@link Statement} opened through this
 * connection since its last checkout is closed. Still NOT reset: SQL Server session-level {@code
 * SET} options ({@code SET DATEFORMAT}, {@code SET ANSI_NULLS}, etc.), temp tables ({@code
 * #temp}), and CONTEXT_INFO -- the same disclosed-limitation shape Oracle's/MySQL's own bridge
 * pools already carry, for the same reason (out of scope for this first slice).
 */
public final class MssqlBridgePool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MssqlBridgePool.class);

    private final DataSource dataSource;
    private final Runnable onClose;
    private final int maxSize;
    private volatile boolean closed;

    public MssqlBridgePool(DataSource dataSource, int maxSize) {
        this(dataSource, maxSize, null);
    }

    MssqlBridgePool(DataSource dataSource, int maxSize, Runnable onClose) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("SQL Server bridge pool size must be positive, got " + maxSize);
        }
        this.dataSource = dataSource;
        this.maxSize = maxSize;
        this.onClose = onClose;
    }

    /** Builds the pool from Bridge-mode configuration: {@code WARP_MSSQL_HOST}/{@code
     * WARP_MSSQL_PORT}/{@code WARP_MSSQL_DATABASE} for the target, a single shared {@code
     * WARP_MSSQL_USER}/{@code WARP_MSSQL_PASSWORD} service account for every client session, and
     * {@code WARP_MSSQL_BRIDGE_POOL_SIZE} (default 10) as Hikari's {@code maximumPoolSize}. Reuses
     * {@link MssqlBackendConnections#jdbcUrl} so this pool and any other SQL Server-backend code
     * path build the identical JDBC URL. */
    public static MssqlBridgePool fromServerOptions(ServerOptions options) {
        int size = ServerOptions.mssqlBridgePoolSize();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(MssqlBackendConnections.jdbcUrl(options));
        config.setUsername(options.mssqlUser());
        config.setPassword(options.mssqlPassword());
        config.setMaximumPoolSize(size);
        config.setPoolName("warp-mssql-bridge");
        config.setAutoCommit(true);
        HikariDataSource ds = new HikariDataSource(config);
        return new MssqlBridgePool(ds, size, ds::close);
    }

    public int maxSize() {
        return maxSize;
    }

    public Connection checkout() throws SQLException {
        if (closed) {
            throw new SQLException("SQL Server bridge pool is closed");
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
                MssqlBridgePool.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private void returnConnection(Connection raw, Set<Statement> openStatements) {
        for (Statement s : openStatements) {
            try {
                s.close();
            } catch (SQLException e) {
                log.warn("SQL Server bridge pool: failed to close a statement/cursor on return: {}", e.getMessage());
            }
        }
        try {
            if (!raw.getAutoCommit()) {
                raw.rollback();
            }
        } catch (SQLException e) {
            log.warn("SQL Server bridge pool: connection failed to reset cleanly on return: {}", e.getMessage());
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
