package com.nexagres.wire.orawire.backend;

import com.nexagres.wire.core.ErrorCatalog;
import com.nexagres.wire.server.ServerOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    
    private NativeByteCaptureProxy.CapturedSession session;
    
    private Statement openStatement;
    private ResultSet openResultSet;

    public NativeOracleExecutor(ServerOptions options, String username, String password) {
        this.options = options;
        this.username = username;
        this.password = password;
    }

    public record NativeQueryResult(byte[] ttcPayload, boolean isQuery, long updateCount, boolean hasMoreRows) {
    }

    public NativeQueryResult execute(String sql, int prefetchRows) throws SQLException {
        NativeByteCaptureProxy captureProxy = proxy();
        ensureConnected(captureProxy);
        closeCursor();
        
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
            
            hasMoreRows = openResultSet.next();
        } else {
            closeCursor();
        }
        byte[] raw = session.snapshotServerBytes();
        byte[] payload = NativeTtcFrameUtil.stripFraming(raw);
        return new NativeQueryResult(payload, isResultSet, updateCount, hasMoreRows);
    }

    public NativeQueryResult fetchMore(int fetchArraySize) throws SQLException {
        if (openResultSet == null) {
            throw ErrorCatalog.sqlException("ERR_NATIVE_NO_OPEN_CURSOR");
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
        
        props.setProperty("oracle.net.socksProxyHost", "127.0.0.1");
        props.setProperty("oracle.net.socksProxyPort", String.valueOf(captureProxy.getPort()));

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
            throw ErrorCatalog.sqlExceptionWithCause("ERR_NATIVE_CAPTURE_INTERRUPTED", e);
        }
        try {
            connection = connectFuture.join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof SQLException sqlEx) {
                throw sqlEx;
            }
            throw ErrorCatalog.sqlExceptionWithCause("ERR_NATIVE_CONNECT_FAILED", e.getCause());
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
