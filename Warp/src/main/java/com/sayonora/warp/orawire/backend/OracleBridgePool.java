package com.sayonora.warp.orawire.backend;

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
 * Bridge mode's ({@link ServerOptions.OracleBackendMode#BRIDGE}) real, bounded, shared JDBC
 * connection pool to Oracle -- many client sessions (one per orawire {@code SessionHandler}) to a
 * few real physical Oracle connections, unlike NATIVE's ({@link NativeSessionRelay}) 1 raw socket
 * per session. Each {@code checkout()} hands back a dynamic proxy {@link Connection} whose {@code
 * close()} first resets whatever session state this slice can cheaply and reliably reset (see the
 * class javadoc section below), then delegates the real {@code close()} to the underlying pool
 * connection, returning it there exactly like an ordinary pooled JDBC connection.
 *
 * <h2>Why HikariCP instead of Oracle UCP or a fully hand-rolled pool</h2>
 * HikariCP is already a compile-scope dependency of this project (see Warp/pom.xml's {@code
 * HikariCP} artifact, used elsewhere in Warp) and is already cached in the local Maven repository,
 * so it needs no new dependency and works fully offline. Oracle UCP was considered first (it is
 * Oracle's own pool, with Oracle-specific validation/harvesting/labeling support), but ojdbc11 does
 * not transitively pull in {@code ucp.jar} -- confirmed via a read-only {@code mvn
 * dependency:tree} against the real repo; UCP ships as its own separate Maven artifact ({@code
 * com.oracle.database.jdbc:ucp}), not declared anywhere in this project's pom.xml -- so using it
 * would mean adding a brand-new production dependency for this one feature slice. A fully
 * hand-rolled pool (bounded queue + semaphore, no borrowed library at all) was the first cut of
 * this class, but real bounded-pool concerns this slice would otherwise have to reinvent --
 * blocking admission with a timeout, connection validation, safely handing out a connection whose
 * {@code close()} really does return it to the pool rather than leaking the physical connection --
 * are exactly what a mature pool library is for. Sister project {@code
 * /Users/kumarrajamani/Projects/orawire} (a separate, independent codebase, NOT part of this repo)
 * hit and fixed a real connection leak in almost this exact spot (its {@code
 * OWHikariPoolManagerImpl}, commit {@code ab4c432}: "the reserved path carried a null proxy and the
 * physical connection was never returned, exhausting the pool after two sessions") -- the concrete
 * lesson taken from it here is that {@link #wrap} must always keep and delegate to Hikari's OWN
 * connection proxy (never a bare reference to something Hikari doesn't itself track), so that this
 * class's {@code close()} really does call back into Hikari's own pooled-connection close path
 * rather than silently dropping the physical connection. This class does not depend on that sister
 * project's code (its TTC/protocol-parsing and auth packages are explicitly NOT reused -- Warp's
 * own {@code RequestLoop}/{@code ExecuteRequestReader}/{@code ResponseWriter} machinery is kept for
 * that); see NOTES.md for the fuller writeup of what was and was not adapted from it.
 *
 * <h2>What is reset on checkout/return, and what is NOT</h2>
 * Reset, on every return to the pool (and defensively again on checkout, in case a caller's {@code
 * close()} was skipped by a crash/interrupt): any open transaction is rolled back, and every {@link
 * Statement} (or subtype: {@code PreparedStatement}/{@code CallableStatement}) opened through this
 * connection since its last checkout is closed (closing an open {@code ResultSet}/cursor along
 * with it, per the JDBC contract). See NOTES.md for the explicit list of Oracle session state this
 * slice does NOT reset (ALTER SESSION settings, DBMS_SESSION package state, temp table contents,
 * NLS settings, sequence caches, etc) -- full session reset would need either a real {@code
 * DBMS_SESSION.RESET_PACKAGE}-style call against a live Oracle instance (out of scope: no live
 * Oracle in this slice) or a driver-level reset hook this class does not attempt.
 */
public final class OracleBridgePool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OracleBridgePool.class);

    private final DataSource dataSource;
    private final Runnable onClose;
    private final int maxSize;
    private volatile boolean closed;

    /** Test/general constructor: any {@link DataSource} (a real {@link HikariDataSource}, or a
     * fake bounded {@link DataSource} in unit tests -- see {@code OracleBridgePoolTest}). {@code
     * maxSize} is recorded only for {@link #maxSize()}/documentation; the actual admission bound is
     * enforced by whatever {@code dataSource} does inside {@code getConnection()} (a real Hikari
     * pool blocks there once {@code maximumPoolSize} connections are checked out). */
    public OracleBridgePool(DataSource dataSource, int maxSize) {
        this(dataSource, maxSize, null);
    }

    OracleBridgePool(DataSource dataSource, int maxSize, Runnable onClose) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Oracle bridge pool size must be positive, got " + maxSize);
        }
        this.dataSource = dataSource;
        this.maxSize = maxSize;
        this.onClose = onClose;
    }

    /** Builds the pool from the ordinary Bridge-mode configuration: {@code WARP_ORACLE_HOST}/
     * {@code WARP_ORACLE_PORT}/{@code WARP_ORACLE_SERVICE} for the target, and a single shared
     * {@code WARP_ORACLE_USER}/{@code WARP_ORACLE_PASSWORD} service account for every client
     * session (see NOTES.md's credential-sharing tradeoff section for why: pooled connections
     * generally cannot be shared across client sessions authenticated as different real Oracle
     * users, so Bridge mode -- like Adapt mode's own Postgres pool -- picks one shared backend
     * identity rather than a real per-client Oracle login). {@code WARP_ORACLE_BRIDGE_POOL_SIZE}
     * (default 10) becomes Hikari's {@code maximumPoolSize}. */
    public static OracleBridgePool fromServerOptions(ServerOptions options) {
        int size = ServerOptions.oracleBridgePoolSize();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@//" + options.oracleHost() + ":" + options.oraclePort()
                + "/" + options.oracleServiceName());
        config.setUsername(options.oracleUser());
        config.setPassword(options.oraclePassword());
        config.setMaximumPoolSize(size);
        config.setPoolName("warp-oracle-bridge");
        // Warp manages autoCommit/transaction boundaries itself (see resetForReuse/handleExecute in
        // RequestLoop, same as every other backend connection this codebase pools) -- Hikari must
        // not second-guess that on borrow.
        config.setAutoCommit(true);
        HikariDataSource ds = new HikariDataSource(config);
        return new OracleBridgePool(ds, size, ds::close);
    }

    public int maxSize() {
        return maxSize;
    }

    public Connection checkout() throws SQLException {
        if (closed) {
            throw new SQLException("Oracle bridge pool is closed");
        }
        Connection raw = dataSource.getConnection();
        try {
            resetForReuse(raw);
        } catch (SQLException | RuntimeException e) {
            closeQuietly(raw);
            throw e;
        }
        return wrap(raw);
    }

    /** Resets session state that this slice can cheaply and reliably reset before a connection
     * (freshly opened, or reused from the pool) is handed to a client session: rolls back any open
     * transaction. See class javadoc for what is intentionally NOT reset. */
    private void resetForReuse(Connection connection) throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.rollback();
        }
    }

    private Connection wrap(Connection raw) {
        PooledConnectionHandler handler = new PooledConnectionHandler(raw);
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(
                OracleBridgePool.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    /** Resets a checked-out connection (rollback plus closing every {@link Statement} the session
     * opened, releasing any open cursors with it) and returns it to the underlying pool by calling
     * its own real {@code close()} -- see class javadoc's leak-lesson note on why the REAL delegate
     * connection, never a null/placeholder, must be the one closed here. A connection that fails to
     * reset is still closed (so the underlying pool can evict/replace it) rather than risking state
     * bleeding to the next borrower, but the failure is logged. */
    private void returnConnection(Connection raw, Set<Statement> openStatements) {
        for (Statement s : openStatements) {
            try {
                s.close();
            } catch (SQLException e) {
                log.warn("Oracle bridge pool: failed to close a statement/cursor on return: {}", e.getMessage());
            }
        }
        try {
            if (!raw.getAutoCommit()) {
                raw.rollback();
            }
        } catch (SQLException e) {
            log.warn("Oracle bridge pool: connection failed to reset cleanly on return: {}", e.getMessage());
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

    /** Dynamic proxy over a real (or fake, in tests) {@link Connection}: every method delegates
     * straight through except {@code close()} (resets then returns the connection to the pool
     * instead of a bare close -- see {@link #returnConnection}) and the statement-creating methods
     * (tracked so {@code close()} can close them all first). */
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
