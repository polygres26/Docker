package com.polygres.wire.pgwire;

import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.core.UntranslatableQueryException;
import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PgWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PgWireSessionHandler.class);

    private final Socket clientSocket;
    
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    
    private final com.polygres.wire.auth.PgRoleAuthCache roleAuthCache;

    private boolean authenticate(String username, String presentedPassword) {
        if (roleAuthCache != null) {
            return roleAuthCache.verify(username, presentedPassword);
        }
        byte[] expected = credentials.lookupPassword(username);
        return expected != null && presentedPassword.equals(new String(expected, StandardCharsets.UTF_8));
    }
    
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    
    private final com.polygres.wire.core.RoutingBackendExecutor routingExecutor;
    private final StatementPipeline pipeline;
    
    private final FailedStatementLog failedStatementLog;

    private Connection sessionConnection;
    private final Map<String, String> preparedStatements = new LinkedHashMap<>();
    private final Map<String, Portal> portals = new LinkedHashMap<>();
    private boolean skipUntilSync;
    
    private volatile String lastExtendedSql;

    private static final Pattern DOLLAR_PARAM = Pattern.compile("\\$(\\d+)");

    private record Portal(String sqlText, ExecutionResult result, int[] nextRow) {
        Portal(String sqlText, ExecutionResult result) {
            this(sqlText, result, new int[1]);
        }
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(clientSocket, options, sharedStages, backendRegistry, null);
    }

    public PgWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.routingExecutor = new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor);
        this.pipeline = new StatementPipeline(sharedStages, routingExecutor);
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
        this.roleAuthCache = roleAuthCache;
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(activeSocket.getOutputStream());

            StartupStreams startup = performStartup(in, out);
            if (startup == null) {
                return;
            }
            queryLoop(startup.in(), startup.out());
        } catch (java.io.EOFException e) {
            
        } catch (Exception e) {
            log.warn("pgwire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close();
            } catch (IOException ignoredOnSessionTeardown) {
                
            }
            try {
                routingExecutor.endTransaction(false);
            } catch (SQLException ignoredOnSessionTeardown) {
                
            }
            if (sessionConnection != null) {
                try {
                    sessionConnection.close();
                } catch (SQLException ignoredOnSessionTeardown) {
                    
                }
            }
        }
    }

    private Connection sessionConnection() throws SQLException {
        if (sessionConnection == null) {
            sessionConnection = PgConnections.open(options);
            sessionConnection.setAutoCommit(true);
        }
        return sessionConnection;
    }

    private boolean handleTransactionControl(Connection connection, String sql) throws SQLException {
        
        String stripped = sql.strip();
        if (stripped.endsWith(";")) {
            stripped = stripped.substring(0, stripped.length() - 1).stripTrailing();
        }
        String verb = stripped.split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT);
        switch (verb) {
            case "BEGIN", "START" -> {
                connection.setAutoCommit(false);
                routingExecutor.beginTransaction();
            }
            case "COMMIT", "END" -> {
                connection.commit();
                connection.setAutoCommit(true);
                routingExecutor.endTransaction(true);
            }
            case "ROLLBACK" -> {
                connection.rollback();
                connection.setAutoCommit(true);
                routingExecutor.endTransaction(false);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private static char readyForQueryStatus(Connection connection) {
        try {
            return connection != null && !connection.getAutoCommit() ? 'T' : 'I';
        } catch (SQLException ignoredStatusIsBestEffort) {
            return 'I';
        }
    }

    private record StartupStreams(DataInputStream in, DataOutputStream out) {
    }

    private StartupStreams performStartup(DataInputStream in, DataOutputStream out) throws IOException {
        StartupMessage startup = readStartupMessage(in, out);
        if (startup == null) {
            return null;
        }
        in = startup.in();
        out = startup.out();
        Map<String, String> params = startup.params();
        String username = params.getOrDefault("user", "");

        PgMessages.writeAuthCleartextPassword(out);
        int type = in.readUnsignedByte();
        if (type != 'p') {
            throw new IOException("expected PasswordMessage, got type " + type);
        }
        int len = in.readInt();
        byte[] body = new byte[len - 4];
        in.readFully(body);
        String password = new String(body, 0, body.length - 1, StandardCharsets.UTF_8);

        if (!authenticate(username, password)) {
            PgMessages.writeErrorAndReady(out, "28P01", "password authentication failed for user \"" + username + "\"");
            return null;
        }

        PgMessages.writeAuthOk(out);
        PgMessages.writeParameterStatus(out, "server_version", "14.0 (polywire)");
        PgMessages.writeParameterStatus(out, "client_encoding", "UTF8");
        PgMessages.writeBackendKeyData(out);
        PgMessages.writeReadyForQuery(out, 'I');
        out.flush();
        return new StartupStreams(in, out);
    }

    private record StartupMessage(DataInputStream in, DataOutputStream out, Map<String, String> params) {
    }

    private StartupMessage readStartupMessage(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            int len = in.readInt();
            int code = in.readInt();
            if (code == PgMessages.SSL_REQUEST_CODE) {
                if (options.tlsEnabled()) {
                    out.writeByte('S');
                    out.flush();
                    try {
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                        SSLSocketFactory factory = sslContext.getSocketFactory();
                        SSLSocket sslSocket = (SSLSocket) factory.createSocket(activeSocket, null, activeSocket.getPort(), true);
                        sslSocket.setUseClientMode(false);
                        sslSocket.startHandshake();
                        activeSocket = sslSocket;
                        in = new DataInputStream(sslSocket.getInputStream());
                        out = new DataOutputStream(sslSocket.getOutputStream());
                    } catch (GeneralSecurityException e) {
                        throw new IOException("pgwire TLS upgrade failed", e);
                    }
                } else {
                    out.writeByte('N');
                    out.flush();
                }
                continue;
            }
            if (code == PgMessages.GSSENC_REQUEST_CODE) {
                out.writeByte('N');
                out.flush();
                continue;
            }
            if (code != PgMessages.PROTOCOL_VERSION_3_0) {
                throw new IOException("unsupported startup protocol version: " + code);
            }
            byte[] rest = new byte[len - 8];
            in.readFully(rest);
            return new StartupMessage(in, out, parseStartupParams(rest));
        }
    }

    private Map<String, String> parseStartupParams(byte[] rest) {
        Map<String, String> params = new LinkedHashMap<>();
        int i = 0;
        while (i < rest.length && rest[i] != 0) {
            int keyEnd = indexOfNul(rest, i);
            String key = new String(rest, i, keyEnd - i, StandardCharsets.UTF_8);
            int valStart = keyEnd + 1;
            int valEnd = indexOfNul(rest, valStart);
            String value = new String(rest, valStart, valEnd - valStart, StandardCharsets.UTF_8);
            params.put(key, value);
            i = valEnd + 1;
        }
        return params;
    }

    private static int indexOfNul(byte[] data, int from) {
        for (int i = from; i < data.length; i++) {
            if (data[i] == 0) {
                return i;
            }
        }
        return data.length;
    }

    private void queryLoop(DataInputStream in, DataOutputStream out) throws IOException {
        while (true) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);

            switch (type) {
                case 'X' -> { return; }
                case 'Q' -> executeSimpleQuery(out, new String(body, 0, body.length - 1, StandardCharsets.UTF_8));
                case 'P' -> dispatchExtended(out, () -> handleParse(out, body));
                case 'B' -> dispatchExtended(out, () -> handleBind(out, body));
                case 'D' -> dispatchExtended(out, () -> handleDescribe(out, body));
                case 'E' -> dispatchExtended(out, () -> handleExecute(out, body));
                case 'C' -> dispatchExtended(out, () -> handleClose(out, body));
                case 'H' -> out.flush();
                case 'S' -> handleSync(out);
                default -> throw new IOException("unsupported pgwire message type: " + (char) type);
            }
        }
    }

    @FunctionalInterface
    private interface ExtendedStep {
        void run() throws IOException, SQLException;
    }

    private void dispatchExtended(DataOutputStream out, ExtendedStep step) throws IOException {
        if (skipUntilSync) {
            return;
        }
        try {
            step.run();
        } catch (SQLException e) {
            
            routingExecutor.markTransactionFailed();
            if (lastExtendedSql != null) {
                recordFailure(lastExtendedSql, e);
            }
            PgMessages.writeErrorResponse(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
            skipUntilSync = true;
        }
    }

    private void handleSync(DataOutputStream out) throws IOException {
        skipUntilSync = false;
        PgMessages.writeReadyForQuery(out, readyForQueryStatus(sessionConnection));
        out.flush();
    }

    private void handleParse(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        String stmtName = r.readCString();
        String sql = r.readCString();
        int numParamTypes = r.readInt16();
        r.skip(numParamTypes * 4);
        preparedStatements.put(stmtName, sql);
        PgMessages.writeParseComplete(out);
    }

    private void handleBind(DataOutputStream out, byte[] body) throws IOException, SQLException {
        PgBodyReader r = new PgBodyReader(body);
        String portalName = r.readCString();
        String stmtName = r.readCString();
        int numParamFormats = r.readInt16();
        int[] paramFormats = new int[numParamFormats];
        for (int i = 0; i < numParamFormats; i++) {
            paramFormats[i] = r.readInt16();
        }
        int numParams = r.readInt16();
        List<Object> rawParams = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            int valueLen = r.readInt32();
            if (valueLen == -1) {
                rawParams.add(null);
                continue;
            }
            int format = numParamFormats == 0 ? 0 : paramFormats[Math.min(i, numParamFormats - 1)];
            if (format != 0) {
                throw new IOException("binary-format bind parameters are not supported (param " + (i + 1) + ")");
            }
            rawParams.add(new String(r.readBytes(valueLen), StandardCharsets.UTF_8));
        }
        int numResultFormats = r.readInt16();
        r.skip(numResultFormats * 2);

        String sql = preparedStatements.get(stmtName);
        if (sql == null) {
            throw new IOException("no such prepared statement: " + stmtName);
        }
        lastExtendedSql = sql;

        Connection backend = sessionConnection();
        if (handleTransactionControl(backend, sql)) {
            portals.put(portalName, new Portal(sql, ExecutionResult.ofUpdate(0)));
            PgMessages.writeBindComplete(out);
            return;
        }

        List<Object> orderedBinds = new ArrayList<>();
        String jdbcSql = rewriteDollarParams(sql, rawParams, orderedBinds);

        terminalExecutor.rebind(backend);
        Statement statement = Statement.of(SourceDialect.POSTGRES, jdbcSql, orderedBinds);
        ExecutionResult result = pipeline.execute(statement);
        portals.put(portalName, new Portal(sql, result));
        PgMessages.writeBindComplete(out);
    }

    private static String rewriteDollarParams(String sql, List<Object> rawParams, List<Object> orderedBinds) {
        Matcher matcher = DOLLAR_PARAM.matcher(sql);
        StringBuilder rewritten = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            rewritten.append(sql, last, matcher.start());
            rewritten.append('?');
            int index = Integer.parseInt(matcher.group(1)) - 1;
            orderedBinds.add(index >= 0 && index < rawParams.size() ? rawParams.get(index) : null);
            last = matcher.end();
        }
        rewritten.append(sql, last, sql.length());
        return rewritten.toString();
    }

    private void handleDescribe(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        char kind = (char) r.readByte();
        String name = r.readCString();
        if (kind == 'S') {
            
            PgMessages.writeParameterDescription(out, 0);
            PgMessages.writeNoData(out);
            return;
        }
        Portal portal = portals.get(name);
        if (portal == null) {
            throw new IOException("no such portal: " + name);
        }
        if (portal.result().isQuery()) {
            PgMessages.writeRowDescription(out, portal.result().columnNames(), portal.result().columnJdbcTypes());
        } else {
            PgMessages.writeNoData(out);
        }
    }

    private void handleExecute(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        String portalName = r.readCString();
        int maxRows = r.readInt32();
        Portal portal = portals.get(portalName);
        if (portal == null) {
            throw new IOException("no such portal: " + portalName);
        }
        if (!portal.result().isQuery()) {
            PgMessages.writeCommandComplete(out, commandTag(portal.sqlText(), (int) portal.result().updateCount()));
            return;
        }
        List<List<Object>> rows = portal.result().rows();
        int start = portal.nextRow()[0];
        int end = maxRows <= 0 ? rows.size() : Math.min(rows.size(), start + maxRows);
        for (int i = start; i < end; i++) {
            PgMessages.writeDataRow(out, rows.get(i));
        }
        portal.nextRow()[0] = end;
        if (end < rows.size()) {
            PgMessages.writePortalSuspended(out);
        } else {
            PgMessages.writeCommandComplete(out, "SELECT " + rows.size());
        }
    }

    private void handleClose(DataOutputStream out, byte[] body) throws IOException {
        PgBodyReader r = new PgBodyReader(body);
        char kind = (char) r.readByte();
        String name = r.readCString();
        if (kind == 'S') {
            preparedStatements.remove(name);
        } else {
            portals.remove(name);
        }
        PgMessages.writeCloseComplete(out);
    }

    private void executeSimpleQuery(DataOutputStream out, String sql) throws IOException {
        try {
            Connection backend = sessionConnection();
            if (handleTransactionControl(backend, sql)) {
                PgMessages.writeCommandComplete(out, sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT));
                PgMessages.writeReadyForQuery(out, readyForQueryStatus(backend));
                out.flush();
                return;
            }
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.POSTGRES, sql, List.of());
            ExecutionResult result = pipeline.execute(statement);
            if (result.isQuery()) {
                PgMessages.writeRowDescription(out, result.columnNames(), result.columnJdbcTypes());
                for (List<Object> row : result.rows()) {
                    PgMessages.writeDataRow(out, row);
                }
                PgMessages.writeCommandComplete(out, "SELECT " + result.rows().size());
            } else {
                PgMessages.writeCommandComplete(out, commandTag(sql, (int) result.updateCount()));
            }
            PgMessages.writeReadyForQuery(out, readyForQueryStatus(backend));
            out.flush();
        } catch (SQLException e) {
            
            routingExecutor.markTransactionFailed();
            recordFailure(sql, e);
            PgMessages.writeErrorAndReady(out, sqlState(e), e.getMessage() == null ? "backend error" : e.getMessage());
            out.flush();
        }
    }

    private void recordFailure(String sql, SQLException e) {
        if (e instanceof UntranslatableQueryException) {
            failedStatementLog.record(SourceDialect.POSTGRES, sql,
                    FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
        } else {
            failedStatementLog.record(SourceDialect.POSTGRES, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), null, e.getMessage());
        }
    }

    private static String sqlState(SQLException e) {
        String state = e.getSQLState();
        return state == null || state.isBlank() ? "58000" : state;
    }

    private static String commandTag(String sql, int updateCount) {
        String verb = sql.strip().split("\\s+", 2)[0].toUpperCase(java.util.Locale.ROOT);
        return switch (verb) {
            case "INSERT" -> "INSERT 0 " + updateCount;
            case "UPDATE" -> "UPDATE " + updateCount;
            case "DELETE" -> "DELETE " + updateCount;
            default -> verb;
        };
    }
}
