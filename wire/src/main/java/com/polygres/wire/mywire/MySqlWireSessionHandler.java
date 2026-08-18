package com.polygres.wire.mywire;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.SqlStateErrorMapper;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.core.UntranslatableQueryException;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
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
    
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    private final StatementPipeline pipeline;

    private final FailedStatementLog failedStatementLog;

    public MySqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<com.polygres.wire.core.PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor));
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
        byte[] authResponse = Arrays.copyOfRange(response, pos[0], pos[0] + authLen);

        byte[] expected = credentials.lookupPassword(username);
        String expectedPassword = expected == null ? "" : new String(expected, StandardCharsets.UTF_8);
        byte[] expectedScramble = MySqlMessages.nativePasswordScramble(expectedPassword, scramble);
        if (!Arrays.equals(expectedScramble, authResponse)) {
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
                    executeQuery(out, packets, sql);
                }
                default -> packets.writePayload(out, MySqlMessages.errPacket(1047, "08S01",
                        "unsupported command: 0x" + Integer.toHexString(command)));
            }
        }
    }

    private static final java.util.regex.Pattern SET_STATEMENT =
            java.util.regex.Pattern.compile("^\\s*set\\s+", java.util.regex.Pattern.CASE_INSENSITIVE);

    private void executeQuery(OutputStream out, MySqlPacket packets, String sql) throws IOException {
        if (!options.mywireNativeBackend() && SET_STATEMENT.matcher(sql).find()) {
            packets.writePayload(out, MySqlMessages.okPacket(0));
            return;
        }
        
        try (Connection backend = options.mywireNativeBackend()
                ? MySqlBackendConnections.open(options) : PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.MYSQL, sql, List.of());
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
                    packets.writePayload(out, MySqlMessages.columnDefinition(columnNames.get(i), columnJdbcTypes.get(i)));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
                for (List<Object> row : result.rows()) {
                    packets.writePayload(out, MySqlMessages.textRow(row));
                }
                packets.writePayload(out, MySqlMessages.eofPacket());
            } else {
                packets.writePayload(out, MySqlMessages.okPacket(result.updateCount()));
            }
        } catch (SQLException e) {
            String state = sqlState(e);
            int nativeError = SqlStateErrorMapper.toMySqlError(state);
            failedStatementLog.record(SourceDialect.MYSQL, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            packets.writePayload(out, MySqlMessages.errPacket(nativeError, state,
                    e.getMessage() == null ? "backend error" : e.getMessage()));
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
