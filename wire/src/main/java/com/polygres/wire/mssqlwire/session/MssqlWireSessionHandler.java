package com.polygres.wire.mssqlwire.session;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.DialectErrorMessages;
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
import com.polygres.wire.mssqlwire.frontend.RpcRequestReader;
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
import java.util.regex.Matcher;
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
    private final JdbcBackendExecutor terminalExecutor;
    private final StatementPipeline pipeline;
    private final com.polygres.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    private final com.polygres.wire.auth.PgRoleAuthCache roleAuthCache;
    private final com.polygres.wire.audit.AuditLog auditLog;
    private volatile com.polygres.wire.core.AccessContext accessContext = com.polygres.wire.core.AccessContext.ANONYMOUS;

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry) {
        this(clientSocket, options, sharedStages, backendRegistry, null, null);
    }

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache) {
        this(clientSocket, options, sharedStages, backendRegistry, roleAuthCache, null);
    }

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            com.polygres.wire.auth.PgRoleAuthCache roleAuthCache, com.polygres.wire.audit.AuditLog auditLog) {
        this.clientSocket = clientSocket;
        this.options = options;
        // Same real-identity-into-native-RLS wiring as PgWireSessionHandler -- see its
        // constructor's javadoc for the full reasoning.
        this.terminalExecutor = new JdbcBackendExecutor(null, new com.polygres.wire.core.access.PostgresRlsSessionInitializer());
        this.pipeline = new StatementPipeline(sharedStages,
                new RoutingBackendExecutor(backendRegistry, terminalExecutor,
                        new com.polygres.wire.xa.XaRecoveryLog(options)));
        this.sqlMetrics = com.polygres.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
        this.roleAuthCache = roleAuthCache;
        this.auditLog = auditLog;
    }

    private boolean authenticate(String username, String presentedPassword) {
        boolean ok;
        if (roleAuthCache != null) {
            ok = roleAuthCache.verify(username, presentedPassword);
        } else {
            byte[] expected = credentials.lookupPassword(username);
            ok = expected != null && new String(expected, java.nio.charset.StandardCharsets.UTF_8).equals(presentedPassword);
        }
        if (ok && roleAuthCache != null) {
            accessContext = new com.polygres.wire.core.AccessContext(username, java.util.Set.of(), java.util.Map.of());
        }
        if (auditLog != null) {
            auditLog.record(ok
                    ? com.polygres.wire.audit.AuditEvent.of(com.polygres.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED,
                            username, "mssqlwire login succeeded for user \"" + username + "\""
                                    + (roleAuthCache != null ? " (real Postgres role)" : " (shared credential)"))
                    : com.polygres.wire.audit.AuditEvent.of(com.polygres.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED,
                            username, "mssqlwire login failed for user \"" + username + "\""));
        }
        return ok;
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

    // Real client behavior, not something this project's own hand-written tests would have
    // exercised: mssql-jdbc (and other TDS clients) manage a client-driven transaction by sending
    // literal batches like "IF @@TRANCOUNT > 0 COMMIT TRAN" for Connection#commit()/rollback(),
    // rather than relying on a TDS-level transaction descriptor. executeQuery always opens its
    // backend connection with autoCommit(true) (mssqlwire has no real cross-statement transaction
    // support today -- every statement commits itself, same limitation noted for pgwire session
    // pooling elsewhere in this codebase), so these batches are pure client-side bookkeeping with
    // nothing for the backend to actually do -- sending them through would either hit a syntax
    // error (Postgres doesn't understand T-SQL's IF/TRAN grammar) or, worse, silently execute
    // unrelated SQL if a translation happened to produce something parseable. No-op them the same
    // way SET_STATEMENT already is. Found live via MssqlJdbcIntegrationTest -- a real driver, not
    // a hand-constructed test payload, is what surfaced this.
    private static final Pattern TRANSACTION_CONTROL_STATEMENT = Pattern.compile(
            "^\\s*(?:IF\\s+@@TRANCOUNT|BEGIN\\s+TRAN|COMMIT\\s+TRAN|ROLLBACK\\s+TRAN|SAVE\\s+TRAN)",
            Pattern.CASE_INSENSITIVE);

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
                    long rttStart = System.nanoTime();
                    String rpcSql = handleRpc(out, packets, msg.payload());
                    if (sqlMetrics != null && rpcSql != null) {
                        sqlMetrics.recordRtt(SourceDialect.SQL_SERVER, rpcSql, System.nanoTime() - rttStart);
                    }
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
        executeQuery(out, packets, sql, List.of(), false);
    }

    /** {@code viaRpc} controls only the final DONE token's flavor (DONEPROC vs DONE) -- see
     * TdsTokens#writeDoneProc's javadoc for why an RPC (sp_executesql) response needs it. */
    private void executeQuery(OutputStream out, TdsPacket packets, String sql, List<Object> bindParams,
            boolean viaRpc) throws IOException {
        if (SET_STATEMENT.matcher(sql).find() || TRANSACTION_CONTROL_STATEMENT.matcher(sql).find()) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
            return;
        }

        try (Connection backend = PgConnections.open(options)) {
            backend.setAutoCommit(true);
            terminalExecutor.rebind(backend);
            Statement statement = Statement.of(SourceDialect.SQL_SERVER, sql, bindParams, accessContext);
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
                writeFinalDone(body, viaRpc, TdsTokens.curCmdSelect(), result.rows().size());
            } else {

                writeFinalDone(body, viaRpc, TdsTokens.curCmdFor(sql), result.updateCount());
            }
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
        } catch (SQLException e) {
            int nativeError = SqlStateErrorMapper.toSqlServerError(e.getSQLState());
            failedStatementLog.record(SourceDialect.SQL_SERVER, sql,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            // The client sees real SQL Server error text ("Invalid object name 'x'.", etc.) when
            // a dialect-native template exists for this SQLSTATE -- the audit log line above
            // still records Postgres's own raw message.
            String clientMessage = e.getMessage() == null ? "backend error"
                    : DialectErrorMessages.render(SourceDialect.SQL_SERVER, e.getSQLState(), e.getMessage());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(nativeError, clientMessage));
        }
    }

    private static void writeFinalDone(ByteArrayOutputStream body, boolean viaRpc, int curCmd, long rowCount) {
        if (viaRpc) {
            TdsTokens.writeDoneProc(body, TdsTokens.doneCountStatus(), curCmd, rowCount);
        } else {
            TdsTokens.writeDone(body, TdsTokens.doneCountStatus(), curCmd, rowCount);
        }
    }

    // Only sp_executesql is supported -- covers the overwhelming majority of real
    // parameterized-query traffic (JDBC PreparedStatement, .NET SqlCommand with parameters, most
    // ORMs). sp_prepare/sp_execute/sp_prepexec (server-side statement caching across round-trips)
    // and arbitrary stored-procedure calls are refused with a clean error rather than attempted --
    // a driver that probes one of those first sees an error, not the silent hang the old blanket
    // RPC rejection risked for anything that assumed *some* RPC response would come back.
    private static final int SP_EXECUTESQL_PROC_ID = 10;

    private static final Pattern NAMED_PARAM_PLACEHOLDER = Pattern.compile("@[A-Za-z_][A-Za-z0-9_]*");
    // Confirmed live against a real client (mssql-jdbc): the RPC parameters carrying the actual
    // bound values are sent UNNAMED (name field empty) -- only @stmt/@params carry real content,
    // and the @P0/@P1/... placeholders in @stmt are matched to bound values purely positionally,
    // in declaration order. This is why rewriteNamedParams below resolves a "@P<n>" placeholder by
    // numeric index first and only falls back to name matching for a driver that DOES set names.
    private static final Pattern POSITIONAL_PLACEHOLDER = Pattern.compile("(?i)^@P(\\d+)$");

    /** Returns the executed SQL text for RTT metrics, or {@code null} if nothing was executed
     * (decode failure / unsupported RPC / bad shape -- an error was already written to the
     * client in every such case). */
    private String handleRpc(OutputStream out, TdsPacket packets, byte[] payload) throws IOException {
        RpcRequestReader.RpcRequest request;
        try {
            request = RpcRequestReader.read(payload);
        } catch (IOException e) {
            log.warn("mssqlwire: could not decode RPC request: {}", e.getMessage());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "could not decode RPC request: " + e.getMessage()));
            return null;
        }
        boolean isExecSql = request.procId() == SP_EXECUTESQL_PROC_ID
                || "sp_executesql".equalsIgnoreCase(request.procName());
        if (!isExecSql) {
            String proc = request.procName() != null ? request.procName() : ("proc #" + request.procId());
            log.warn("mssqlwire: RPC call to {} not supported -- only sp_executesql is", proc);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "only sp_executesql RPC calls are supported "
                            + "(not stored procedures or sp_prepare/sp_execute/sp_prepexec)"));
            return null;
        }
        if (request.params().size() < 2 || !(request.params().get(0).value() instanceof String sql)) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "sp_executesql call missing a string @stmt parameter"));
            return null;
        }
        List<RpcRequestReader.RpcParam> boundParams = request.params().subList(2, request.params().size());
        List<Object> orderedBinds = new java.util.ArrayList<>();
        String jdbcSql;
        try {
            jdbcSql = rewriteNamedParams(sql, boundParams, orderedBinds);
        } catch (IOException e) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, TdsTokens.errorMessage(50000, e.getMessage()));
            return null;
        }
        executeQuery(out, packets, jdbcSql, orderedBinds, true);
        return jdbcSql;
    }

    /** Rewrites {@code @P0}/{@code @name}-style placeholders in {@code sql} to JDBC {@code ?}.
     * Every real client observed (mssql-jdbc, confirmed live) sends the actual bound-value RPC
     * parameters unnamed and matches them to {@code @stmt}'s placeholders purely by position/
     * declaration order -- so a {@code @P<n>} placeholder resolves directly to
     * {@code boundParams.get(n)} first. Falls back to matching by name for a driver that DOES set
     * names (the TDS spec allows it even though the one real client tested here doesn't use it).
     * A placeholder that resolves neither way is refused rather than guessed at, same "refuse over
     * silently wrong" convention as RouterStage/ScatterGatherAggregateMerge. */
    private static String rewriteNamedParams(String sql, List<RpcRequestReader.RpcParam> boundParams,
            List<Object> orderedBinds) throws IOException {
        Matcher matcher = NAMED_PARAM_PLACEHOLDER.matcher(sql);
        StringBuilder rewritten = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            String name = matcher.group();
            RpcRequestReader.RpcParam match = resolveBoundParam(name, boundParams);
            if (match == null) {
                throw new IOException("sp_executesql: no bound value found for placeholder " + name);
            }
            rewritten.append(sql, last, matcher.start());
            rewritten.append('?');
            orderedBinds.add(match.value());
            last = matcher.end();
        }
        rewritten.append(sql, last, sql.length());
        return rewritten.toString();
    }

    private static RpcRequestReader.RpcParam resolveBoundParam(String placeholderName,
            List<RpcRequestReader.RpcParam> boundParams) {
        Matcher positional = POSITIONAL_PLACEHOLDER.matcher(placeholderName);
        if (positional.matches()) {
            int index = Integer.parseInt(positional.group(1));
            if (index >= 0 && index < boundParams.size()) {
                return boundParams.get(index);
            }
        }
        for (RpcRequestReader.RpcParam p : boundParams) {
            if (placeholderName.equalsIgnoreCase(p.name())) {
                return p;
            }
        }
        return null;
    }
}
