package com.polygres.wire.orawire.backend;

import com.polygres.wire.server.ServerOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes a statement against the real Oracle backend via the ordinary ojdbc11 thin driver
 * (real O5LOGON, real EXECUTE — nothing about the driver itself changes) while capturing the raw
 * bytes the backend sent back, via {@link NativeByteCaptureProxy}. See that class's javadoc and
 * {@link ServerOptions.OracleBackendMode#NATIVE}'s javadoc for why this exists: {@code
 * java.sql.ResultSetMetaData} never exposes several backend-computed TTC fields a real client's
 * own response still needs, so this bypasses the JDBC abstraction for the response side while
 * still using it for the (already correct, already working) request side.
 *
 * <p>Narrow slice, first version: one dedicated (non-pooled) connection per executor instance —
 * correlating the capture proxy's FIFO-accepted sessions with the right {@code java.sql.Connection}
 * is only unambiguous with a single connection at a time (see {@link NativeByteCaptureProxy}'s
 * class javadoc); a pooled/concurrent version needs a stronger correlation key. Only statements
 * whose result exhausts within the first EXECUTE response are supported — no FETCH continuation
 * (see {@link RequestLoop}'s caller for how that's gated) — a large-result-set query returning
 * more rows would need one implemented on top of the same capture mechanism.
 */
public final class NativeOracleExecutor implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(NativeOracleExecutor.class.getName());

    private static volatile NativeByteCaptureProxy sharedProxy;

    private static synchronized NativeByteCaptureProxy proxy() {
        if (sharedProxy == null) {
            try {
                sharedProxy = NativeByteCaptureProxy.start();
                LOG.info("native Oracle backend byte-capture proxy listening on 127.0.0.1:" + sharedProxy.getPort());
            } catch (Exception e) {
                throw new IllegalStateException("failed to start NativeByteCaptureProxy", e);
            }
        }
        return sharedProxy;
    }

    private final ServerOptions options;
    private final String username;
    private final String password;
    private Connection connection;
    // One SOCKS session per underlying TCP connection, established once at login and reused for
    // every statement run on it — a JDBC Connection does not open a new socket per query. Fixed
    // live: the first version of this class called expectNextSession() again per execute() call,
    // which just hung forever (nothing new to accept — the driver reuses the existing socket).
    private NativeByteCaptureProxy.CapturedSession session;
    // The currently-open query cursor, if any — kept open (not drained/closed) across execute()
    // so a later fetchMore() can pull the next batch through the SAME real backend cursor,
    // capturing that round trip's own raw bytes too. Closed by closeCursor(), which every new
    // execute() calls first (matches RequestLoop's own one-active-cursor-per-session model).
    private Statement openStatement;
    private ResultSet openResultSet;

    public NativeOracleExecutor(ServerOptions options, String username, String password) {
        this.options = options;
        this.username = username;
        this.password = password;
    }

    /** Result of a native execute/fetch: the DESCRIBE_INFO/ROW_DATA/terminator bytes the real
     *  backend actually sent, TNS framing already stripped, ready to relay to PolyWire's own
     *  frontend client (still needs re-wrapping in PolyWire's own TNS packet header — see
     *  RequestLoop). {@code hasMoreRows} is a best-effort hint (see fetchMore's javadoc) for
     *  whether a later OFETCH is worth routing here at all — the relayed bytes' own terminator
     *  shape is what actually tells the frontend client whether to ask again, same as the
     *  reconstruction path already did. */
    public record NativeQueryResult(byte[] ttcPayload, boolean isQuery, long updateCount, boolean hasMoreRows) {
    }

    public NativeQueryResult execute(String sql, int prefetchRows) throws SQLException {
        NativeByteCaptureProxy captureProxy = proxy();
        ensureConnected(captureProxy);
        closeCursor();
        // Any bytes from connection setup/prior calls are irrelevant to THIS call — the same
        // captured session (one per underlying socket, see its field javadoc) is reused and
        // cleared before every execute, not re-acquired.
        session.clear();

        openStatement = connection.createStatement();
        if (prefetchRows > 0) {
            openStatement.setFetchSize(prefetchRows);
        }
        boolean isResultSet = openStatement.execute(sql);
        long updateCount = isResultSet ? -1 : openStatement.getUpdateCount();
        boolean hasMoreRows = false;
        if (isResultSet) {
            openResultSet = openStatement.getResultSet();
            // RESOLVED, found live: without forcing at least one real read here, there's a race —
            // the driver may not have actually finished receiving the backend's own inline row
            // batch by the time we snapshot captured bytes below (TCP arrival is async; execute()
            // returning doesn't itself guarantee the whole inline batch is on the wire yet), so
            // the snapshot could be a truncated mid-response slice. A real client (ojdbc11
            // against a real backend) hung waiting for the rest of a response it never fully
            // received — this exact "partial capture" shape. rs.next() forces the driver to
            // actually read (and thus fully receive) whatever was sent unprompted; it does NOT
            // trigger a premature extra wire fetch here since the whole inline batch is already
            // buffered from EXECUTE — safe to call before fetchMore ever runs.
            hasMoreRows = openResultSet.next();
        } else {
            closeCursor();
        }
        byte[] raw = session.snapshotServerBytes();
        byte[] payload = NativeTtcFrameUtil.stripFraming(raw);
        return new NativeQueryResult(payload, isResultSet, updateCount, hasMoreRows);
    }

    /**
     * Continues the SAME real backend cursor left open by {@link #execute}, capturing the raw
     * bytes of whatever real FETCH round trip {@code ResultSet.next()} triggers once the
     * driver's own internal row buffer (populated by the last EXECUTE or fetchMore call) is
     * exhausted. Best-effort alignment, not exact: ojdbc11's own internal prefetch/buffering
     * decides when it actually goes back to the wire, which isn't necessarily byte-for-byte
     * "one real FETCH per fetchMore() call" — {@code fetchArraySize} is applied as the driver's
     * own fetch size (best available lever to influence that), not a hard per-call guarantee.
     */
    public NativeQueryResult fetchMore(int fetchArraySize) throws SQLException {
        if (openResultSet == null) {
            throw new SQLException("fetchMore() with no open native cursor");
        }
        session.clear();
        if (fetchArraySize > 0) {
            openResultSet.setFetchSize(fetchArraySize);
        }
        boolean exhausted = false;
        int consumed = 0;
        while (consumed < Math.max(1, fetchArraySize)) {
            if (!openResultSet.next()) {
                exhausted = true;
                break;
            }
            consumed++;
        }
        byte[] raw = session.snapshotServerBytes();
        byte[] payload = NativeTtcFrameUtil.stripFraming(raw);
        if (exhausted) {
            closeCursor();
        }
        return new NativeQueryResult(payload, true, -1, !exhausted);
    }

    public void closeCursor() {
        if (openResultSet != null) {
            try {
                openResultSet.close();
            } catch (SQLException ignored) {
            }
            openResultSet = null;
        }
        if (openStatement != null) {
            try {
                openStatement.close();
            } catch (SQLException ignored) {
            }
            openStatement = null;
        }
    }

    private void ensureConnected(NativeByteCaptureProxy captureProxy) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            return;
        }
        String url = "jdbc:oracle:thin:@%s:%d/%s".formatted(
                options.oracleHost(), options.oraclePort(), options.oracleServiceName());
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        // Routes this connection's actual TCP traffic through our local capture proxy —
        // the driver itself does a completely normal SOCKS5 CONNECT + O5LOGON, unaware
        // anything is different. See NativeByteCaptureProxy's javadoc.
        props.setProperty("oracle.net.socksProxyHost", "127.0.0.1");
        props.setProperty("oracle.net.socksProxyPort", String.valueOf(captureProxy.getPort()));

        // expectNextSession() blocks until the SOCKS accept fires — which only happens once the
        // driver actually opens its socket, i.e. inside DriverManager.getConnection() below. Must
        // run the connect on its own thread and wait on it concurrently with expectNextSession(),
        // not call expectNextSession() first (that deadlocks: nothing to accept yet).
        var connectFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                return DriverManager.getConnection(url, props);
            } catch (SQLException e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
        try {
            session = captureProxy.expectNextSession();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted waiting for native capture session", e);
        }
        try {
            connection = connectFuture.join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof SQLException sqlEx) {
                throw sqlEx;
            }
            throw new SQLException("native connect failed", e.getCause());
        }
        connection.setAutoCommit(true);
        session.clear();
    }

    @Override
    public void close() {
        closeCursor();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.log(Level.FINE, "error closing native Oracle connection", e);
            }
            connection = null;
        }
    }
}
