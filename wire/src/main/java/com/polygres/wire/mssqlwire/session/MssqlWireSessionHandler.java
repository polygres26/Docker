package com.polygres.wire.mssqlwire.session;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.RoutingBackendExecutor;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.SqlStateErrorMapper;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.core.UntranslatableQueryException;
import com.polygres.wire.mssqlwire.frontend.Login7Handler;
import com.polygres.wire.mssqlwire.frontend.PreLoginHandshake;
import com.polygres.wire.mssqlwire.frontend.SqlBatchReader;
import com.polygres.wire.mssqlwire.frontend.TdsTlsChannel;
import com.polygres.wire.mssqlwire.frontend.TdsTokens;
import com.polygres.wire.mssqlwire.wireformat.TdsPacket;
import com.polygres.wire.mssqlwire.wireformat.TdsPacketType;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.server.TlsSupport;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MssqlWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MssqlWireSessionHandler.class);

    private final Socket clientSocket;
    
    private volatile Socket activeSocket;
    private final ServerOptions options;
    private final CredentialStore credentials = new CredentialStore();
    private final JdbcBackendExecutor terminalExecutor = new JdbcBackendExecutor(null);
    private final StatementPipeline pipeline;
    private final com.polygres.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    private final com.polygres.wire.auth.PgRoleAuthCache roleAuthCache;

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry) {
        this(clientSocket, options, sharedStages, backendRegistry, null);
    }

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache) {
        this.clientSocket = clientSocket;
        this.options = options;
        this.pipeline = new StatementPipeline(sharedStages,
                new RoutingBackendExecutor(backendRegistry, terminalExecutor,
                        new com.polygres.wire.xa.XaRecoveryLog(options)));
        this.sqlMetrics = com.polygres.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
        this.roleAuthCache = roleAuthCache;
    }

    private boolean authenticate(String username, String presentedPassword) {
        if (roleAuthCache != null) {
            return roleAuthCache.verify(username, presentedPassword);
        }
        byte[] expected = credentials.lookupPassword(username);
        return expected != null && new String(expected, java.nio.charset.StandardCharsets.UTF_8).equals(presentedPassword);
    }

    @Override
    public void run() {
        activeSocket = clientSocket;
        try {
            DataInputStream in = new DataInputStream(activeSocket.getInputStream());
            OutputStream out = activeSocket.getOutputStream();
            TdsPacket packets = new TdsPacket();

            HandshakeStreams streams = performHandshake(in, out, packets);
            if (streams == null) {
                return;
            }
            queryLoop(streams.in(), streams.out(), packets);
        } catch (java.io.EOFException e) {
            
        } catch (Exception e) {
            log.warn("mssqlwire session terminated: {}", e.getMessage(), e);
        } finally {
            try {
                activeSocket.close();
            } catch (IOException ignoredOnSessionTeardown) {
                
            }
        }
    }

    private record HandshakeStreams(DataInputStream in, OutputStream out) {
    }

    private HandshakeStreams performHandshake(DataInputStream in, OutputStream out, TdsPacket packets) throws IOException {
        TdsPacket.Message preloginReq = packets.readMessage(in);
        if (preloginReq.type() != TdsPacketType.PRE_LOGIN) {
            log.warn("mssqlwire: expected PRELOGIN (0x12), got 0x{}", Integer.toHexString(preloginReq.type()));
            return null;
        }
        Map<Integer, byte[]> clientOptions = PreLoginHandshake.parse(preloginReq.payload());
        byte requestedEncryption = PreLoginHandshake.requestedEncryption(clientOptions);
        boolean willUpgrade = options.tlsEnabled()
                && (requestedEncryption == PreLoginHandshake.ENCRYPT_ON
                        || requestedEncryption == PreLoginHandshake.ENCRYPT_REQUIRED);
        if (!options.tlsEnabled() && requestedEncryption == PreLoginHandshake.ENCRYPT_REQUIRED) {
            
            log.warn("mssqlwire: client requires encryption but TLS isn't configured (set POLYWIRE_TLS_KEYSTORE)");
            return null;
        }
        byte negotiatedEncryption = willUpgrade
                ? PreLoginHandshake.ENCRYPT_ON
                : PreLoginHandshake.ENCRYPT_NOT_SUPPORTED;
        
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, PreLoginHandshake.buildResponse(negotiatedEncryption));

        if (willUpgrade) {
            
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(TlsSupport.buildKeyManagerFactory(options).getKeyManagers(), null, null);
                
                TdsTlsChannel tls = new TdsTlsChannel(sslContext, activeSocket);
                tls.handshake();
                in = new DataInputStream(tls.inputStream());
                out = tls.outputStream();
            } catch (GeneralSecurityException e) {
                throw new IOException("mssqlwire TLS upgrade failed", e);
            }
        }

        TdsPacket.Message loginReq = packets.readMessage(in);
        if (loginReq.type() != TdsPacketType.LOGIN7) {
            log.warn("mssqlwire: expected LOGIN7 (0x10), got 0x{}", Integer.toHexString(loginReq.type()));
            return null;
        }
        Login7Handler.Credentials creds = Login7Handler.parse(loginReq.payload());
        if (creds.integratedSecurity()) {
            log.warn("mssqlwire: client requested Windows/SSPI auth, only SQL auth is supported");
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Windows authentication is not supported"));
            return null;
        }

        if (!authenticate(creds.userName(), creds.password())) {
            log.warn("mssqlwire: login failed for user '{}'", creds.userName());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Login failed for user '" + creds.userName() + "'"));
            return null;
        }

        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, TdsTokens.loginAck(creds.database()));
        return new HandshakeStreams(in, out);
    }

    private static final Pattern SET_STATEMENT = Pattern.compile("^\\s*set\\s+", Pattern.CASE_INSENSITIVE);

    private void queryLoop(DataInputStream in, OutputStream out, TdsPacket packets) throws IOException {
        while (true) {
            TdsPacket.Message msg = packets.readMessage(in);
            switch (msg.type()) {
                case TdsPacketType.SQL_BATCH -> {
                    String sql = SqlBatchReader.readSqlText(msg.payload());
                    long rttStart = System.nanoTime();
                    executeQuery(out, packets, sql);
                    if (sqlMetrics != null) {
                        sqlMetrics.recordRtt(SourceDialect.SQL_SERVER, sql, System.nanoTime() - rttStart);
                    }
                }
                case TdsPacketType.ATTENTION -> {
                    // DONE_ATTN, not DONE_FINAL -- see TdsTokens' javadoc on doneAttnStatus for
                    // why a plain DONE_FINAL here leaves real TDS clients hanging forever.
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    TdsTokens.writeDone(body, TdsTokens.doneAttnStatus(), 0, 0);
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
                }
                case TdsPacketType.RPC -> {
                    log.warn("mssqlwire: RPC batches not supported in this pass");
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                            TdsTokens.errorMessage(50000, "RPC/prepared-statement calls are not supported yet"));
                }
                default -> {
                    if (msg.payload().length == 0 && msg.type() == 0) {
                        return;
                    }
                    log.warn("mssqlwire: unsupported message type 0x{}", Integer.toHexString(msg.type()));
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                            TdsTokens.errorMessage(50000, "unsupported TDS message type 0x" + Integer.toHexString(msg.type())));
                }
            }
        }
    }

    private void executeQuery(OutputStream out, TdsPacket packets, String sql) throws IOException {
        if (SET_STATEMENT.matcher(sql).find()) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
            return;
        }
        
        try (Connection backend = PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.SQL_SERVER, sql, List.of());
            ExecutionResult result;
            try {
                result = pipeline.execute(statement);
            } catch (UntranslatableQueryException e) {
                failedStatementLog.record(SourceDialect.SQL_SERVER, sql,
                        FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
                packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                        TdsTokens.errorMessage(SqlStateErrorMapper.SQL_SERVER_DEFAULT,
                                e.getMessage() == null ? "statement could not be translated" : e.getMessage()));
                return;
            }

            ByteArrayOutputStream body = new ByteArrayOutputStream();
            if (result.isQuery()) {
                List<String> columnNames = result.columnNames();
                TdsTokens.writeColMetaData(body, columnNames);
                for (List<Object> row : result.rows()) {
                    TdsTokens.writeRow(body, row);
                }
                TdsTokens.writeDone(body, TdsTokens.doneCountStatus(), TdsTokens.curCmdSelect(), result.rows().size());
            } else {
                
                TdsTokens.writeDone(body, TdsTokens.doneCountStatus(), TdsTokens.curCmdFor(sql), result.updateCount());
            }
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
        } catch (SQLException e) {
            int nativeError = SqlStateErrorMapper.toSqlServerError(e.getSQLState());
            failedStatementLog.record(SourceDialect.SQL_SERVER, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(nativeError, e.getMessage() == null ? "backend error" : e.getMessage()));
        }
    }
}
