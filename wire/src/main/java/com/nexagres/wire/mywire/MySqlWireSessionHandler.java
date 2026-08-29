package com.nexagres.wire.mywire;

import com.nexagres.wire.auth.CredentialStore;
import com.nexagres.wire.config.FailedStatementLog;
import com.nexagres.wire.core.DialectErrorMessages;
import com.nexagres.wire.core.ExecutionResult;
import com.nexagres.wire.core.JdbcBackendExecutor;
import com.nexagres.wire.core.SourceDialect;
import com.nexagres.wire.core.SqlStateErrorMapper;
import com.nexagres.wire.core.Statement;
import com.nexagres.wire.core.StatementPipeline;
import com.nexagres.wire.core.UntranslatableQueryException;
import com.nexagres.wire.pgwire.PgConnections;
import com.nexagres.wire.server.ServerOptions;
import com.nexagres.wire.server.TlsSupport;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MySqlWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MySqlWireSessionHandler.class);
    private static final AtomicLong NEXT_CONNECTION_ID = new AtomicLong(1);

    private final Socket clientSocket;
    
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    
    // See com.nexagres.wire.core.access.MySqlPgEmulationSessionInitializer's own header comment --
    // the mywire equivalent of orawire's OraclePgEmulationSessionInitializer (RequestLoop.java):
    // `SET db_emulation = 'mysql'` once per statement (cheap/idempotent when already set, see
    // pg_oracle.c's own db_emulation_assign_hook), so pg_mysql's unqualified LAST_INSERT_ID(),
    // GROUP_CONCAT(...), DATE_FORMAT(...), etc. resolve without db/pg_mysql needing every caller
    // to schema-qualify mysql_catalog.* by hand.
    private final JdbcBackendExecutor terminalExecutor =
            new JdbcBackendExecutor(null, new com.nexagres.wire.core.access.MySqlPgEmulationSessionInitializer());
    private final StatementPipeline pipeline;
    private final com.nexagres.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    /** Session-scoped: statement handles are only meaningful within the connection that
     * PREPAREd them, same as real MySQL. */
    private final Map<Integer, PreparedStmt> preparedStatements = new HashMap<>();
    private final AtomicInteger nextStmtId = new AtomicInteger(1);

    private static final class PreparedStmt {
        final String sql;
        final int paramCount;
        // -1 sentinel in [0] means "no cached types yet" -- see MySqlBinaryProtocol.decodeExecuteParams.
        final int[] cachedParamTypes;
        boolean longDataRefused;

        PreparedStmt(String sql, int paramCount) {
            this.sql = sql;
            this.paramCount = paramCount;
            this.cachedParamTypes = new int[Math.max(paramCount, 1)];
            this.cachedParamTypes[0] = -1;
        }
    }

    public MySqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.nexagres.wire.core.PipelineStage> sharedStages, com.nexagres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new com.nexagres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor,
                new com.nexagres.wire.xa.XaRecoveryLog(options),
                com.nexagres.wire.core.RouterStage.shardRulesIn(sharedStages), com.nexagres.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                .withFederationSupport(com.nexagres.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                        com.nexagres.wire.core.RouterStage.planStoreIn(sharedStages)));
        this.sqlMetrics = com.nexagres.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            OutputStream out = activeSocket.getOutputStream();
            MySqlPacket packets = new MySqlPacket();

            HandshakeStreams handshake = performHandshake(in, out, packets);
            if (handshake == null) {
                return;
            }
            queryLoop(handshake.in(), handshake.out(), packets);
        } catch (java.io.EOFException e) {
            
        } catch (Exception e) {
            log.warn("mywire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close();
            } catch (IOException ignoredOnSessionTeardown) {
                
            }
        }
    }

    private record HandshakeStreams(DataInputStream in, OutputStream out) {
    }

    private HandshakeStreams performHandshake(DataInputStream in, OutputStream out, MySqlPacket packets) throws IOException {
        byte[] scramble = new byte[20];
        new SecureRandom().nextBytes(scramble);
        long connectionId = NEXT_CONNECTION_ID.getAndIncrement();
        packets.writePayload(out, MySqlMessages.handshakeV10(connectionId, scramble, options.tlsEnabled()));

        byte[] response = packets.readPayload(in);
        int clientCapabilities = (response[0] & 0xFF) | ((response[1] & 0xFF) << 8)
                | ((response[2] & 0xFF) << 16) | ((response[3] & 0xFF) << 24);
        if (options.tlsEnabled() && (clientCapabilities & MySqlMessages.CLIENT_SSL) != 0) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                SSLSocketFactory factory = sslContext.getSocketFactory();
                SSLSocket sslSocket = (SSLSocket) factory.createSocket(activeSocket, null, activeSocket.getPort(), true);
                sslSocket.setUseClientMode(false);
                sslSocket.startHandshake();
                activeSocket = sslSocket;
                in = new DataInputStream(sslSocket.getInputStream());
                out = sslSocket.getOutputStream();
            } catch (GeneralSecurityException e) {
                throw new IOException("mywire TLS upgrade failed", e);
            }
            response = packets.readPayload(in);
        }

        int[] pos = {0};
        pos[0] += 4;
        pos[0] += 4;
        pos[0] += 1;
        pos[0] += 23;
        String username = MySqlPacket.readNulString(response, pos);
        int authLen = response[pos[0]++] & 0xFF;
        // This first auth_response is NOT necessarily computed with mysql_native_password, even
        // though that's the only plugin this server declares in the handshake and the only one it
        // implements: modern client defaults (confirmed live -- MySQL Connector/J 8.4 and 9.1
        // both, despite the server's declared plugin) optimistically guess caching_sha2_password
        // for their very first response regardless, and only actually honor the server-declared
        // plugin after an explicit AuthSwitchRequest. So this value is discarded, not compared --
        // an AuthSwitchRequest is always sent next, and only the client's response to THAT (which
        // every compliant client, by spec, must compute with the requested plugin) is checked.
        pos[0] += authLen;

        packets.writePayload(out, MySqlMessages.authSwitchRequest("mysql_native_password", scramble));
        byte[] switchResponse = packets.readPayload(in);

        byte[] expected = credentials.lookupPassword(username);
        String expectedPassword = expected == null ? "" : new String(expected, StandardCharsets.UTF_8);
        byte[] expectedScramble = MySqlMessages.nativePasswordScramble(expectedPassword, scramble);
        if (!Arrays.equals(expectedScramble, switchResponse)) {
            packets.writePayload(out, MySqlMessages.errPacket(1045, "28000",
                    "Access denied for user '" + username + "'"));
            return null;
        }

        packets.writePayload(out, MySqlMessages.okPacket(0));
        return new HandshakeStreams(in, out);
    }

    private static final int COM_QUIT = 0x01;
    private static final int COM_INIT_DB = 0x02;
    private static final int COM_QUERY = 0x03;
    private static final int COM_STMT_PREPARE = 0x16;
    private static final int COM_STMT_EXECUTE = 0x17;
    private static final int COM_STMT_SEND_LONG_DATA = 0x18;
    private static final int COM_STMT_CLOSE = 0x19;
    private static final int COM_STMT_RESET = 0x1a;
    private static final int COM_PING = 0x0e;

    private void queryLoop(DataInputStream in, OutputStream out, MySqlPacket packets) throws IOException {
        while (true) {
            byte[] payload = packets.readPayload(in);
            if (payload.length == 0) {
                continue;
            }
            int command = payload[0] & 0xFF;
            switch (command) {
                case COM_QUIT -> {
                    return;
                }
                case COM_PING, COM_INIT_DB -> packets.writePayload(out, MySqlMessages.okPacket(0));
                case COM_QUERY -> {
                    String sql = new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8);
                    long rttStart = System.nanoTime();
                    executeQuery(out, packets, sql, List.of());
                    if (sqlMetrics != null) {
                        sqlMetrics.recordRtt(SourceDialect.MYSQL, sql, System.nanoTime() - rttStart);
                    }
                }
                case COM_STMT_PREPARE -> {
                    String sql = new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8);
                    handlePrepare(out, packets, sql);
                }
                case COM_STMT_EXECUTE -> handleExecute(out, packets, payload);
                case COM_STMT_SEND_LONG_DATA -> handleSendLongData(payload);
                case COM_STMT_CLOSE -> {
                    if (payload.length >= 5) {
                        preparedStatements.remove(readInt32LE(payload, 1));
                    }
                    // No response -- COM_STMT_CLOSE is one of the rare commands MySQL never
                    // acknowledges, matching real server behavior.
                }
                case COM_STMT_RESET -> {
                    if (payload.length >= 5) {
                        PreparedStmt stmt = preparedStatements.get(readInt32LE(payload, 1));
                        if (stmt != null) {
                            stmt.longDataRefused = false;
                        }
                    }
                    packets.writePayload(out, MySqlMessages.okPacket(0));
                }
                default -> packets.writePayload(out, MySqlMessages.errPacket(1047, "08S01",
                        "unsupported command: 0x" + Integer.toHexString(command)));
            }
        }
    }

    private void handleSendLongData(byte[] payload) {
        // COM_STMT_SEND_LONG_DATA streams a BLOB/CLOB parameter across multiple packets, sent
        // separately from -- and excluded entirely from -- the following EXECUTE's own parameter
        // value section. Correctly supporting it means buffering per-parameter chunks and having
        // EXECUTE's decode loop skip those parameter indices rather than read them from the wire,
        // which is real additional protocol-state tracking this pass doesn't implement. No
        // response is sent for this command either way (matches real MySQL); refusing here would
        // require synthesizing an out-of-band error the client isn't expecting. Instead: mark the
        // statement so the *next* EXECUTE fails cleanly instead of silently using a wrong/missing
        // value for that parameter.
        if (payload.length < 5) {
            return;
        }
        PreparedStmt stmt = preparedStatements.get(readInt32LE(payload, 1));
        if (stmt != null) {
            stmt.longDataRefused = true;
        }
    }

    private static int readInt32LE(byte[] data, int offset) {
        return (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8)
                | ((data[offset + 2] & 0xFF) << 16) | ((data[offset + 3] & 0xFF) << 24);
    }

    private void handlePrepare(OutputStream out, MySqlPacket packets, String sql) throws IOException {
        int paramCount = MySqlBinaryProtocol.countPlaceholders(sql);
        List<String> columnNames = List.of();
        if (sql.stripLeading().regionMatches(true, 0, "SELECT", 0, 6)) {
            // Best-effort only: prepare-time column metadata is used purely to size
            // COM_STMT_PREPARE_OK's num_columns declaration, NOT held across PREPARE->EXECUTE --
            // EXECUTE re-runs the full statement against a fresh connection (same as plain
            // COM_QUERY already does) and uses that real execution's own result columns for the
            // actual response. A metadata-fetch failure here just means PREPARE_OK declares 0
            // columns; some clients tolerate that better than others, but EXECUTE's response is
            // unaffected either way.
            try (Connection backend = options.mywireNativeBackend()
                    ? MySqlBackendConnections.open(options) : PgConnections.open(options);
                    PreparedStatement ps = backend.prepareStatement(sql)) {
                ResultSetMetaData md = ps.getMetaData();
                if (md != null) {
                    List<String> names = new ArrayList<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        names.add(md.getColumnLabel(i));
                    }
                    columnNames = names;
                }
            } catch (SQLException e) {
                log.debug("mywire: could not fetch prepare-time column metadata for \"{}\" -- "
                        + "PREPARE_OK will declare 0 columns, EXECUTE's response is unaffected: {}",
                        sql, e.getMessage());
            }
        }

        int stmtId = nextStmtId.getAndIncrement();
        preparedStatements.put(stmtId, new PreparedStmt(sql, paramCount));

        ByteArrayOutputStream okBody = new ByteArrayOutputStream();
        okBody.write(0x00);
        MySqlPacket.writeFixedInt(okBody, stmtId, 4);
        MySqlPacket.writeFixedInt(okBody, columnNames.size(), 2);
        MySqlPacket.writeFixedInt(okBody, paramCount, 2);
        okBody.write(0);
        MySqlPacket.writeFixedInt(okBody, 0, 2);
        packets.writePayload(out, okBody.toByteArray());

        for (int i = 0; i < paramCount; i++) {
            packets.writePayload(out, MySqlMessages.columnDefinition("?", Types.VARCHAR));
        }
        if (paramCount > 0) {
            packets.writePayload(out, MySqlMessages.eofPacket());
        }
        for (String name : columnNames) {
            packets.writePayload(out, MySqlMessages.columnDefinition(name, Types.VARCHAR));
        }
        if (!columnNames.isEmpty()) {
            packets.writePayload(out, MySqlMessages.eofPacket());
        }
    }

    private void handleExecute(OutputStream out, MySqlPacket packets, byte[] payload) throws IOException {
        int stmtId = readInt32LE(payload, 1);
        PreparedStmt stmt = preparedStatements.get(stmtId);
        if (stmt == null) {
            packets.writePayload(out, MySqlMessages.errPacket(1243, "HY000",
                    "Unknown prepared statement handle: " + stmtId));
            return;
        }
        if (stmt.longDataRefused) {
            packets.writePayload(out, MySqlMessages.errPacket(1235, "42000",
                    "COM_STMT_SEND_LONG_DATA parameters are not supported -- bind this value as a "
                            + "regular (non-streamed) parameter instead"));
            return;
        }

        List<Object> bindParams;
        try {
            int[] pos = {10}; // command(1) + stmt_id(4) + flags(1) + iteration_count(4)
            bindParams = stmt.paramCount == 0
                    ? List.of()
                    : MySqlBinaryProtocol.decodeExecuteParams(payload, pos, stmt.paramCount, stmt.cachedParamTypes);
        } catch (IOException e) {
            packets.writePayload(out, MySqlMessages.errPacket(1210, "HY000",
                    "could not decode COM_STMT_EXECUTE parameters: " + e.getMessage()));
            return;
        }

        long rttStart = System.nanoTime();
        executeQuery(out, packets, stmt.sql, bindParams, true);
        if (sqlMetrics != null) {
            sqlMetrics.recordRtt(SourceDialect.MYSQL, stmt.sql, System.nanoTime() - rttStart);
        }
    }

    private static final java.util.regex.Pattern SET_STATEMENT =
            java.util.regex.Pattern.compile("^\\s*set\\s+", java.util.regex.Pattern.CASE_INSENSITIVE);

    private void executeQuery(OutputStream out, MySqlPacket packets, String sql, List<Object> bindParams)
            throws IOException {
        executeQuery(out, packets, sql, bindParams, false);
    }

    /** {@code binaryResult} selects COM_STMT_EXECUTE's binary row/column-definition encoding
     * instead of COM_QUERY's plain text encoding -- see MySqlBinaryProtocol's class doc for why
     * every column is declared/encoded as VAR_STRING either way. */
    private void executeQuery(OutputStream out, MySqlPacket packets, String sql, List<Object> bindParams,
            boolean binaryResult) throws IOException {
        if (!options.mywireNativeBackend() && SET_STATEMENT.matcher(sql).find()) {
            packets.writePayload(out, MySqlMessages.okPacket(0));
            return;
        }
        if (!options.mywireNativeBackend() && MySqlSessionVariableQuery.matches(sql)) {
            MySqlSessionVariableQuery.SyntheticResult synthetic = MySqlSessionVariableQuery.synthesize(sql);
            packets.writePayload(out, columnCountPayload(synthetic.columnNames().size()));
            for (String name : synthetic.columnNames()) {
                packets.writePayload(out, MySqlMessages.columnDefinition(name, Types.VARCHAR));
            }
            packets.writePayload(out, MySqlMessages.eofPacket());
            packets.writePayload(out, binaryResult
                    ? MySqlBinaryProtocol.encodeRow(synthetic.row())
                    : MySqlMessages.textRow(synthetic.row()));
            packets.writePayload(out, MySqlMessages.eofPacket());
            return;
        }

        try (Connection backend = options.mywireNativeBackend()
                ? MySqlBackendConnections.open(options) : PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.MYSQL, sql, bindParams);
            ExecutionResult result;
            try {
                result = pipeline.execute(statement);
            } catch (UntranslatableQueryException e) {
                failedStatementLog.record(SourceDialect.MYSQL, sql,
                        FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
                packets.writePayload(out, MySqlMessages.errPacket(SqlStateErrorMapper.MYSQL_DEFAULT, e.getSQLState(),
                        e.getMessage() == null ? "statement could not be translated" : e.getMessage()));
                return;
            }
            if (result.isQuery()) {
                List<String> columnNames = result.columnNames();
                List<Integer> columnJdbcTypes = result.columnJdbcTypes();
                packets.writePayload(out, columnCountPayload(columnNames.size()));
                for (int i = 0; i < columnNames.size(); i++) {
                    int declaredType = binaryResult ? Types.VARCHAR : columnJdbcTypes.get(i);
                    packets.writePayload(out, MySqlMessages.columnDefinition(columnNames.get(i), declaredType));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
                for (List<Object> row : result.rows()) {
                    packets.writePayload(out, binaryResult ? MySqlBinaryProtocol.encodeRow(row) : MySqlMessages.textRow(row));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
            } else {
                packets.writePayload(out, MySqlMessages.okPacket(result.updateCount()));
            }
        } catch (SQLException e) {
            String state = sqlState(e);
            int nativeError = SqlStateErrorMapper.toMySqlError(state, e.getMessage());
            failedStatementLog.record(SourceDialect.MYSQL, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            // The client sees real MySQL error text ("Table 'x' doesn't exist", etc.) when a
            // dialect-native template exists for this SQLSTATE -- the audit log line above still
            // records Postgres's own raw message.
            String clientMessage = e.getMessage() == null ? "backend error"
                    : DialectErrorMessages.render(SourceDialect.MYSQL, state, e.getMessage());
            packets.writePayload(out, MySqlMessages.errPacket(nativeError, state, clientMessage));
        }
    }

    private static byte[] columnCountPayload(int count) {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        MySqlPacket.writeLenEncInt(b, count);
        return b.toByteArray();
    }

    private static String sqlState(SQLException e) {
        String state = e.getSQLState();
        return state == null || state.isBlank() ? "HY000" : state;
    }
}
