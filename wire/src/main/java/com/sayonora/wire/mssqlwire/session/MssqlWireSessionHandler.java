package com.sayonora.wire.mssqlwire.session;

import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.config.FailedStatementLog;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.DialectErrorMessages;
import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.JdbcBackendExecutor;
import com.sayonora.wire.core.PipelineStage;
import com.sayonora.wire.core.RoutingBackendExecutor;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.SqlStateErrorMapper;
import com.sayonora.wire.core.Statement;
import com.sayonora.wire.core.StatementPipeline;
import com.sayonora.wire.core.UntranslatableQueryException;
import com.sayonora.wire.mssqlwire.frontend.Login7Handler;
import com.sayonora.wire.mssqlwire.frontend.PreLoginHandshake;
import com.sayonora.wire.mssqlwire.frontend.RpcRequestReader;
import com.sayonora.wire.mssqlwire.frontend.SqlBatchReader;
import com.sayonora.wire.mssqlwire.frontend.TdsTlsChannel;
import com.sayonora.wire.mssqlwire.frontend.TdsTokens;
import com.sayonora.wire.mssqlwire.wireformat.TdsPacket;
import com.sayonora.wire.mssqlwire.wireformat.TdsPacketType;
import com.sayonora.wire.pgwire.PgConnections;
import com.sayonora.wire.server.ServerOptions;
import com.sayonora.wire.server.TlsSupport;
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
    private final RoutingBackendExecutor routingExecutor;
    private final StatementPipeline pipeline;
    // Real gap found auditing this frontend for GA transparency: every statement used to open a
    // BRAND NEW backend connection, execute, and close -- meaning BEGIN TRAN/COMMIT TRAN/ROLLBACK
    // TRAN (already regex-matched below, but only to NO-OP them) had nowhere real to apply. Same
    // fix as pgwire/mywire already have: one connection lives for the whole session.
    private Connection sessionConnection;
    private boolean inTransaction;
    // Set by an "INSERT BULK <table> (...)" statement (real BCP's own literal-SQL_BATCH trigger --
    // see #INSERT_BULK's javadoc), consumed by the very next TdsPacketType.BULK_LOAD_BCP packet.
    // Session-scoped, one in flight at a time -- a real client always completes one bulk load
    // (INSERTBULK text, then its data packet(s)) before starting another, same assumption
    // preparedRpcStatements' handle table already makes about single-session sequencing.
    private volatile String pendingBulkLoadTable;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    private final FailedStatementLog failedStatementLog;

    private final com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache;
    private final com.sayonora.wire.audit.AuditLog auditLog;
    private volatile com.sayonora.wire.core.AccessContext accessContext = com.sayonora.wire.core.AccessContext.ANONYMOUS;

    // sp_prepare/sp_execute/sp_unprepare's own handle table -- server-side statement caching a
    // real client (mssql-jdbc confirmed live) switches to automatically after a few repeated
    // executions of the SAME PreparedStatement, instead of continuing to use sp_executesql every
    // time. Session-scoped (one connection's own handles, not shared or persisted), same lifetime
    // as a real SQL Server's own prepared-statement cache for a session. A plain incrementing int
    // is enough -- these handles are never compared against anything but this same map, on this
    // same connection, unlike a real backend's actual statement-cache identifiers.
    private final Map<Integer, String> preparedRpcStatements = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicInteger nextPreparedHandle = new java.util.concurrent.atomic.AtomicInteger(1);

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry) {
        this(clientSocket, options, sharedStages, backendRegistry, null, null);
    }

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache) {
        this(clientSocket, options, sharedStages, backendRegistry, roleAuthCache, null);
    }

    public MssqlWireSessionHandler(Socket clientSocket, ServerOptions options,
            List<PipelineStage> sharedStages, BackendRegistry backendRegistry,
            com.sayonora.wire.auth.PgRoleAuthCache roleAuthCache, com.sayonora.wire.audit.AuditLog auditLog) {
        this.clientSocket = clientSocket;
        this.options = options;
        // Same real-identity-into-native-RLS wiring as PgWireSessionHandler -- see its
        // constructor's javadoc for the full reasoning. MssqlPgEmulationSessionInitializer
        // delegates to PostgresRlsSessionInitializer for that and adds `SET db_emulation =
        // 'sqlserver'` on top, so pg_sqlserver's unqualified sys.tables/OBJECT_ID(...)/
        // SCOPE_IDENTITY() etc. resolve -- see that class's own javadoc.
        this.terminalExecutor = new JdbcBackendExecutor(null, new com.sayonora.wire.core.access.MssqlPgEmulationSessionInitializer());
        this.routingExecutor = new RoutingBackendExecutor(backendRegistry, terminalExecutor,
                new com.sayonora.wire.xa.XaRecoveryLog(options),
                com.sayonora.wire.core.RouterStage.shardRulesIn(sharedStages), com.sayonora.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                .withFederationSupport(com.sayonora.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                        com.sayonora.wire.core.RouterStage.planStoreIn(sharedStages));
        this.pipeline = new StatementPipeline(sharedStages, routingExecutor);
        this.sqlMetrics = com.sayonora.wire.core.StatsCollectorStage.findIn(sharedStages);
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
            accessContext = new com.sayonora.wire.core.AccessContext(username, java.util.Set.of(), java.util.Map.of());
        }
        if (auditLog != null) {
            auditLog.record(ok
                    ? com.sayonora.wire.audit.AuditEvent.of(com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED,
                            username, "mssqlwire login succeeded for user \"" + username + "\""
                                    + (roleAuthCache != null ? " (real Postgres role)" : " (shared credential)"))
                    : com.sayonora.wire.audit.AuditEvent.of(com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED,
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
            terminalExecutor.rebind(sessionConnection);
        }
        return sessionConnection;
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
            
            log.warn("mssqlwire: client requires encryption but TLS isn't configured (set WARP_TLS_KEYSTORE)");
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
            return performNtlmHandshake(in, out, packets, creds);
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

    /**
     * NTLMv2 continuation exchange for a Windows/SSPI login (LOGIN7's {@code fIntSecurity} bit
     * set) -- see {@link com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages}' javadoc for the
     * real wire shape (raw Type-2/Type-3 blobs in standalone {@code TdsPacketType.SSPI} packets,
     * confirmed live against mssql-jdbc's {@code authenticationScheme=NTLM}). The presented
     * password is verified via {@link com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages
     * #verifyNtlmV2Response} against {@link CredentialStore}'s plaintext credential -- the same
     * server-side-plaintext-needed structural reason orawire's O5LOGON and mongowire's SCRAM both
     * already depend on (an NTLMv2 response can only be verified by recomputing it from the real
     * password, never from a stored hash of some other shape).
     */
    private HandshakeStreams performNtlmHandshake(DataInputStream in, OutputStream out, TdsPacket packets,
            Login7Handler.Credentials creds) throws IOException {
        if (!com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.isType1Negotiate(creds.sspiBlob())) {
            log.warn("mssqlwire: client set fIntSecurity but sent no real NTLM Type-1 message -- "
                    + "Kerberos/SSPI-native Windows auth isn't supported, only NTLM");
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Windows authentication is only supported via NTLM"));
            return null;
        }
        byte[] serverChallenge = com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.randomServerChallenge();
        byte[] type2 = com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.buildType2Challenge(serverChallenge);
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, TdsTokens.sspiToken(type2));

        TdsPacket.Message type3Msg = packets.readMessage(in);
        if (type3Msg.type() != TdsPacketType.SSPI) {
            log.warn("mssqlwire: expected an SSPI (0x11) continuation packet with the client's NTLM "
                    + "Type-3 message, got 0x{}", Integer.toHexString(type3Msg.type()));
            return null;
        }
        com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.Type3Message type3;
        try {
            type3 = com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.parseType3(type3Msg.payload());
        } catch (IllegalArgumentException malformed) {
            log.warn("mssqlwire: malformed NTLM Type-3 message: {}", malformed.getMessage());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Login failed: malformed NTLM authenticate message"));
            return null;
        }

        byte[] expectedPassword = credentials.lookupPassword(type3.userName());
        boolean ok = expectedPassword != null
                && com.sayonora.wire.mssqlwire.frontend.auth.NtlmMessages.verifyNtlmV2Response(
                        type3.ntChallengeResponse(), serverChallenge,
                        new String(expectedPassword, java.nio.charset.StandardCharsets.UTF_8),
                        type3.userName(), type3.domain());
        if (auditLog != null) {
            auditLog.record(ok
                    ? com.sayonora.wire.audit.AuditEvent.of(com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_SUCCEEDED,
                            type3.userName(), "mssqlwire NTLM login succeeded for user \"" + type3.userName() + "\"")
                    : com.sayonora.wire.audit.AuditEvent.of(com.sayonora.wire.audit.AuditEvent.Type.DB_LOGIN_FAILED,
                            type3.userName(), "mssqlwire NTLM login failed for user \"" + type3.userName() + "\""));
        }
        if (!ok) {
            log.warn("mssqlwire: NTLM login failed for user '{}'", type3.userName());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(18456, "Login failed for user '" + type3.userName() + "'"));
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
    // NOT anchored to the start of the batch: confirmed live (WARP_DEBUG_MSSQL_SQL) that
    // mssql-jdbc's Connection#setAutoCommit(true) sends these concatenated in ONE batch --
    // "set implicit_transactions off IF @@TRANCOUNT > 0 COMMIT TRAN" -- so an anchored "must be
    // the very first token" pattern misses the COMMIT/ROLLBACK entirely once something else
    // (like the SET here) comes first in the same batch. IMPLICIT_TRANSACTIONS ON/OFF is
    // mssql-jdbc's OWN Connection#setAutoCommit(false/true) implementation -- SQL Server's native
    // "next statement implicitly opens a transaction" mode, not a TDS-level flag -- confirmed the
    // same way mywire's own SET autocommit=0/1 finding was.
    private static final Pattern TRANSACTION_CONTROL_STATEMENT = Pattern.compile(
            "IF\\s+@@TRANCOUNT|BEGIN\\s+TRAN|COMMIT\\s+TRAN|ROLLBACK\\s+TRAN|SAVE\\s+TRAN|IMPLICIT_TRANSACTIONS",
            Pattern.CASE_INSENSITIVE);

    /** Real gap found live (same audit that found mywire's identical one): the comment this
     * pattern used to justify no-op'ing every match is now out of date -- mssql-jdbc's
     * "IF @@TRANCOUNT &gt; 0 COMMIT/ROLLBACK TRAN" idiom for {@code Connection#commit()}/
     * {@code rollback()}, and its {@code SET IMPLICIT_TRANSACTIONS ON/OFF} for {@code
     * Connection#setAutoCommit(false/true)}, need a REAL backend connection in manual-commit mode
     * to mean anything; with the old per-statement-fresh-connection design there wasn't one, so
     * no-op was the only safe choice. Now that {@link #sessionConnection()} is session-scoped,
     * these can (and must) actually run. Checked in this exact order -- ROLLBACK/COMMIT before
     * IMPLICIT_TRANSACTIONS ON -- because a real {@code setAutoCommit(true)} batch contains BOTH
     * "IMPLICIT_TRANSACTIONS OFF" and a trailing "COMMIT TRAN": the COMMIT is the real action,
     * the IMPLICIT_TRANSACTIONS OFF alone doesn't end anything by itself. {@code SAVE TRAN} (named
     * savepoints) still falls through to a no-op below (matched by {@link
     * #TRANSACTION_CONTROL_STATEMENT} but none of the checked verbs) -- not yet implemented, same
     * as before this fix; still correctly swallowed rather than reaching the (Postgres-translated)
     * backend as invalid T-SQL syntax. */
    private boolean handleTransactionControl(Connection connection, String sql) throws SQLException {
        if (!TRANSACTION_CONTROL_STATEMENT.matcher(sql).find()) {
            return false;
        }
        String upper = sql.toUpperCase(java.util.Locale.ROOT);
        if (upper.contains("ROLLBACK")) {
            if (inTransaction) {
                connection.rollback();
                connection.setAutoCommit(true);
                inTransaction = false;
                routingExecutor.endTransaction(false);
            }
        } else if (upper.contains("COMMIT")) {
            if (inTransaction) {
                connection.commit();
                connection.setAutoCommit(true);
                inTransaction = false;
                routingExecutor.endTransaction(true);
            }
        } else if (upper.contains("IMPLICIT_TRANSACTIONS") && upper.contains("ON")) {
            if (!inTransaction) {
                connection.setAutoCommit(false);
                inTransaction = true;
                routingExecutor.beginTransaction();
            }
        } else if (upper.contains("IMPLICIT_TRANSACTIONS")) {
            // "...OFF" with no COMMIT/ROLLBACK alongside it (the ROLLBACK/COMMIT branches above
            // already handle the common combined-batch case) -- nothing further to do; leaving
            // any currently-open transaction exactly as it is matches real SQL Server, where
            // turning implicit_transactions off doesn't itself end an already-open transaction.
        } else if (upper.contains("BEGIN")) {
            connection.setAutoCommit(false);
            inTransaction = true;
            routingExecutor.beginTransaction();
        }
        return true;
    }

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
                case TdsPacketType.BULK_LOAD_BCP -> handleBulkLoadPacket(out, packets, msg.payload());
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

    /**
     * {@code EXEC}/{@code EXECUTE proc arg1, arg2, ...} -- Postgres has no {@code EXEC} statement
     * at all, so forwarded untranslated this is a flat syntax error. Confirmed live to be a real,
     * common shape (not just a hypothetical one): mssql-jdbc's own client-side JDBC-escape
     * processing for {@code {call proc(?)}} builds exactly this text ("EXEC proc  ?") as an
     * sp_executesql {@code @stmt} -- so this rewrite, together with {@link #rewriteNamedParams}'s
     * bare-{@code ?} handling above, is what actually makes a real {@code CallableStatement} call
     * work, not just a hand-typed {@code EXEC} batch.
     *
     * <p>Rewritten to {@code SELECT * FROM proc(arg1, arg2, ...)} -- Postgres's own way to invoke
     * a {@code FUNCTION} and get its result rows back (see {@code
     * MssqlStoredProcedureCallIntegrationTest} for why this assumes an equivalently-named,
     * equivalently-positioned Postgres FUNCTION already exists; a bare {@code PROCEDURE} with no
     * return value still executes correctly through this same rewrite, just yields an empty
     * result set). Scope, deliberately narrow: only plain positional arguments are recognized --
     * SQL Server's {@code @name = value} named-argument form is left untouched (and still fails
     * exactly as before this fix) rather than guessed at.
     *
     * @return the rewritten SQL, or {@code null} if {@code sql} isn't an EXEC call at all.
     */
    private static final Pattern EXEC_CALL = Pattern.compile(
            "(?is)^\\s*EXEC(?:UTE)?\\s+([A-Za-z_][\\w.$#]*)\\s*(.*?)\\s*;?\\s*$");

    private static String rewriteExecCallToFunctionCall(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher m = EXEC_CALL.matcher(sql);
        if (!m.matches()) {
            return null;
        }
        String procName = m.group(1);
        String args = m.group(2);
        return "SELECT * FROM " + procName + "(" + args + ")";
    }

    /** {@code viaRpc} controls only the final DONE token's flavor (DONEPROC vs DONE) -- see
     * TdsTokens#writeDoneProc's javadoc for why an RPC (sp_executesql) response needs it. */
    private void executeQuery(OutputStream out, TdsPacket packets, String sql, List<Object> bindParams,
            boolean viaRpc) throws IOException {
        executeQuery(out, packets, sql, bindParams, viaRpc, null);
    }

    /** @param prepareHandleToReturn non-null only for {@code sp_prepexec} -- the handle its own
     *      {@code @handle OUTPUT} parameter names, written as a RETURNVALUE token ahead of the
     *      real query result in the SAME response message (a real client reads one continuous
     *      token stream per RPC call; a separate TDS message here would set the End-Of-Message
     *      flag after just the handle, telling the client the whole response was already done). */
    // {@code SET FMTONLY ON <select> [SET FMTONLY OFF]} -- SQL Server's legacy "describe the
    // result set's columns without actually running the query" mechanism, predating
    // sp_describe_first_result_set. Found live: SQLServerBulkCopy sends exactly this (wrapped in a
    // literal sp_executesql call, see BARE_SP_EXECUTESQL below) to learn its destination table's
    // real column shape before it ever gets to the actual BCP wire protocol -- without handling it,
    // bulk copy can't even start ("Unable to retrieve column metadata", the SELECT passed straight
    // through to Postgres, which has no FMTONLY concept and chokes on the literal T-SQL syntax).
    // Real SQL Server semantics: FMTONLY is a session-level setting that persists until explicitly
    // turned off; this codebase has no per-session query-execution-suppression state to model that
    // faithfully, so it's handled per-statement instead -- narrower than the real feature (a
    // FMTONLY ON with no matching OFF anywhere in the SAME batch, which is exactly what
    // SQLServerBulkCopy sends and the only shape found live), but correctly covers the one real
    // client behavior this project has actually observed.
    private static final Pattern FMTONLY_ON = Pattern.compile(
            "(?is)^\\s*SET\\s+FMTONLY\\s+ON\\s+(.*?)\\s*(?:;\\s*SET\\s+FMTONLY\\s+OFF\\s*;?)?\\s*$");

    // SQLServerBulkCopy's own literal-call shape for sp_executesql -- confirmed live: it sends
    // {@code sp_executesql N'<stmt>'} as plain SQL_BATCH text, with NO leading EXEC/EXECUTE keyword
    // (unlike {@link #EXEC_CALL}'s shape), so neither that rewrite nor DialectTranslationStage's
    // normal handling ever sees this as a call at all -- it falls straight through as literal T-SQL
    // and fails against Postgres ("syntax error at or near sp_executesql"). Only the single-N'...'-
    // literal-argument shape is unwrapped (real doubled-quote escaping inside the literal is
    // honored); sp_executesql's OWN parameter-list/bind-value arguments (its 2nd/3rd+ args, used
    // for a real parameterized call) are a materially different, RPC-shaped case already handled
    // by {@code RpcRequestReader}'s isExecSql path -- not this literal-SQL_BATCH shape at all.
    private static final Pattern BARE_SP_EXECUTESQL = Pattern.compile(
            "(?is)^\\s*sp_executesql\\s+N?'((?:[^']|'')*)'\\s*;?\\s*$");

    private static String unwrapBareSpExecuteSql(String sql) {
        Matcher m = BARE_SP_EXECUTESQL.matcher(sql);
        return m.matches() ? m.group(1).replace("''", "'") : sql;
    }

    // SQLServerBulkCopy's own second real metadata-discovery query (after the FMTONLY probe
    // above), confirmed live: an exact, fixed shape asking sys.columns for each destination
    // column's collation and computed-ness. Real SQL Server's sys.columns has no direct Postgres
    // analog outside the optional, not-always-installed pg_sqlserver emulation extension (see
    // MssqlPgEmulationSessionInitializer's own javadoc on that being best-effort) -- rather than
    // require that extension just for BCP to work, this recognizes the ONE exact query shape a
    // real client sends and answers it directly from information_schema.columns, matching real
    // driver behavior for the common case this project can actually verify: an ordinary table with
    // no exotic per-column COLLATE clause and no computed columns. A real collation-aware or
    // computed-column BCP target is a narrower case left unhandled (NULL collation, is_computed
    // always false) rather than guessed at.
    private static final Pattern BCP_SYS_COLUMNS_PROBE = Pattern.compile(
            "(?is)^\\s*select\\s+collation_name\\s*,\\s*is_computed\\s+from\\s+sys\\.columns\\s+"
                    + "where\\s+object_id\\s*=\\s*OBJECT_ID\\('([^']+)'\\)\\s+order\\s+by\\s+column_id\\s+ASC\\s*$");

    // Real BCP's own trigger, sent as plain SQL_BATCH text just like INSERT/CREATE TABLE (not RPC)
    // -- confirmed live via SQLServerBulkCopy: "INSERT BULK <table> ([col] TYPE [, ...])". Only the
    // table name is captured here; the authoritative column list/types arrive again, properly
    // wire-encoded, in the COLMETADATA-shaped header of the BULK_LOAD_BCP packet(s) that follow --
    // see #handleBulkLoadPacket.
    private static final Pattern INSERT_BULK = Pattern.compile(
            "(?is)^\\s*INSERT\\s+BULK\\s+([A-Za-z_][\\w.$#\\[\\]]*)\\s*\\(.*\\)\\s*$");

    private static String rewriteBcpSysColumnsProbe(String sql) {
        Matcher m = BCP_SYS_COLUMNS_PROBE.matcher(sql);
        if (!m.matches()) {
            return sql;
        }
        return "SELECT NULL AS collation_name, false AS is_computed FROM information_schema.columns "
                + "WHERE table_name = '" + m.group(1).replace("'", "''") + "' ORDER BY ordinal_position ASC";
    }

    private static String stripBrackets(String identifier) {
        return identifier.replace("[", "").replace("]", "");
    }

    /**
     * Real BCP data ({@code TdsPacketType.BULK_LOAD_BCP}), decoded by {@link
     * com.sayonora.wire.mssqlwire.frontend.BcpDataReader} -- see its own javadoc for the wire
     * shape. Each decoded row is inserted as a real, ordinary parameterized {@code INSERT}
     * through this session's normal {@link #executeQuery} path (same dialect-translation/routing
     * pipeline every other statement uses), one statement per row rather than a true batched/COPY
     * write -- correctness over raw bulk-load throughput for a first working implementation; a
     * genuinely large BCP load would be materially slower here than real SQL Server's own bulk
     * path, a real, disclosed limitation rather than a silent one.
     */
    private void handleBulkLoadPacket(OutputStream out, TdsPacket packets, byte[] payload) throws IOException {
        String table = pendingBulkLoadTable;
        pendingBulkLoadTable = null;
        com.sayonora.wire.mssqlwire.frontend.BcpDataReader.BcpResult bcp;
        try {
            bcp = com.sayonora.wire.mssqlwire.frontend.BcpDataReader.read(payload);
        } catch (IOException e) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "could not decode BCP data: " + e.getMessage()));
            return;
        }
        if (table == null) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "BCP data packet with no preceding INSERT BULK statement"));
            return;
        }
        String columnList = String.join(", ", bcp.columns().stream().map(c -> "[" + c.name() + "]").toList());
        String placeholders = String.join(", ", bcp.columns().stream().map(c -> "?").toList());
        String insertSql = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + placeholders + ")";
        int rowsInserted = 0;
        try {
            sessionConnection();
            for (List<Object> row : bcp.rows()) {
                Statement statement = Statement.of(SourceDialect.SQL_SERVER, insertSql, row, accessContext);
                pipeline.execute(statement);
                rowsInserted++;
            }
        } catch (SQLException e) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(SqlStateErrorMapper.SQL_SERVER_DEFAULT,
                            "BCP bulk load into '" + table + "' failed after " + rowsInserted + " row(s): "
                                    + (e.getMessage() == null ? "backend error" : e.getMessage())));
            return;
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, rowsInserted);
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
    }

    private void executeQuery(OutputStream out, TdsPacket packets, String sql, List<Object> bindParams,
            boolean viaRpc, Integer prepareHandleToReturn) throws IOException {
        sql = rewriteBcpSysColumnsProbe(unwrapBareSpExecuteSql(sql));
        Matcher insertBulk = INSERT_BULK.matcher(sql);
        if (insertBulk.matches()) {
            // A real client DOES wait for a response to INSERT BULK itself (confirmed live: no
            // response here hangs both sides -- this server waiting to read the next packet, the
            // client waiting for an ack of this one) before sending the actual
            // BULK_LOAD_BCP data packet(s) -- see #handleBulkLoadPacket. A plain DONE (same shape
            // as any other successful zero-row batch) is what a real client expects.
            pendingBulkLoadTable = stripBrackets(insertBulk.group(1));
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
            return;
        }
        Matcher fmtOnly = FMTONLY_ON.matcher(sql);
        if (fmtOnly.matches()) {
            // Metadata-only: wrap the real query so Postgres itself never returns any rows (no
            // separate "describe without executing" primitive exists over a plain JDBC
            // PreparedStatement/ResultSetMetaData path here), while still getting the query's real
            // column shape back -- exactly what FMTONLY ON is for.
            sql = "SELECT * FROM (" + fmtOnly.group(1) + ") AS warp_fmtonly_probe WHERE 1 = 0";
        }
        if (!options.mssqlwireNativeBackend()) {
            String execRewrite = rewriteExecCallToFunctionCall(sql);
            if (execRewrite != null) {
                sql = execRewrite;
            }
            try {
                if (handleTransactionControl(sessionConnection(), sql)) {
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
                    packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
                    return;
                }
            } catch (SQLException e) {
                packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                        TdsTokens.errorMessage(SqlStateErrorMapper.SQL_SERVER_DEFAULT,
                                e.getMessage() == null ? "backend error" : e.getMessage()));
                return;
            }
        }
        // A no-op against the (fake, dialect-translated) Postgres backend -- in native mode a SET
        // statement means something real against the real SQL Server backend and must actually
        // run, not be silently swallowed.
        if (!options.mssqlwireNativeBackend() && SET_STATEMENT.matcher(sql).find()) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            TdsTokens.writeDone(body, TdsTokens.doneFinalStatus(), 0, 0);
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
            return;
        }

        Statement statement = Statement.of(SourceDialect.SQL_SERVER, sql, bindParams, accessContext);
        try {
            ExecutionResult result;
            try {
                if (options.mssqlwireNativeBackend()) {
                    // No pin: leave targetBackend unset and let RouterStage resolve it like any
                    // other statement. Any configured WARP_ROUTER_*/WARP_TABLE_SHARDS rule can
                    // route this statement to ANY registered backend of ANY dialect (including a
                    // Postgres one -- correctly translated by DialectTranslationStage, since that
                    // stage keys off the RESOLVED target's own dialect, not this protocol's usual
                    // one). When no rule matches, RouterStage.resolveUnambiguousDefault() falls
                    // back to the sole registered SQL_SERVER-dialect backend if exactly one exists
                    // -- "mssql-native", registered by Main.java only when
                    // WARP_MSSQLWIRE_BACKEND=sqlserver is set -- which is exactly today's single-
                    // target behavior, just reached generically instead of via a hardcoded pin.
                    // Register a SECOND SQL Server target via WARP_BACKENDS and that fallback stops
                    // being unambiguous, requiring an explicit rule -- the whole point of this.
                    // DialectTranslationStage no-ops once resolved to a same-dialect target, so the
                    // client's real T-SQL still reaches a real SQL Server backend untouched, same as
                    // before. FirewallStage/QosControlStage/CacheStage still run unmodified -- none
                    // of them inspect dialect at all. RoutingBackendExecutor.execute() resolves any
                    // non-default named target via its own executeOnFreshConnection path -- a pooled
                    // connection via BackendTarget#open() and a plain JdbcBackendExecutor(Connection)
                    // with NO NativeRlsSessionInitializer, unlike this session's own terminalExecutor
                    // (bound to MssqlPgEmulationSessionInitializer, Postgres-only `SET db_emulation =
                    // 'sqlserver'` setup a real SQL Server backend has no use for and would reject).
                    //
                    // EXCEPT for dual-port mode's own native listener (see ServerOptions#
                    // mssqlwireNativeViaDualPort's own javadoc): there, this same ambiguous
                    // fallback would ALSO match the TRANSLATED listener's own statements once BOTH
                    // are registered at once (a real bug, found live) -- so that one case pins
                    // targetBackend explicitly to its own reserved dual-port name instead of
                    // relying on the fallback at all.
                    if (options.mssqlwireNativeViaDualPort()) {
                        statement = statement.withRouting(statement.workloadClass(),
                                com.sayonora.wire.core.BackendRegistry.MSSQL_NATIVE_DUAL_PORT_NAME);
                    }
                    result = pipeline.execute(statement);
                } else {
                    // One connection for the whole session (see sessionConnection()'s own
                    // javadoc, which also does the one-time terminalExecutor.rebind()), not a
                    // fresh one per statement -- required for BEGIN/COMMIT/ROLLBACK TRAN (handled
                    // above) to mean anything across statements.
                    sessionConnection();
                    result = pipeline.execute(statement);
                }
            } catch (UntranslatableQueryException e) {
                failedStatementLog.record(SourceDialect.SQL_SERVER, sql,
                        FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
                packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                        TdsTokens.errorMessage(SqlStateErrorMapper.SQL_SERVER_DEFAULT,
                                e.getMessage() == null ? "statement could not be translated" : e.getMessage()));
                return;
            }

            ByteArrayOutputStream body = new ByteArrayOutputStream();
            if (prepareHandleToReturn != null) {
                TdsTokens.writeReturnValueInt(body, 1, prepareHandleToReturn);
            }
            if (result.isQuery()) {
                TdsTokens.writeColMetaData(body, result.columns());
                for (List<Object> row : result.rows()) {
                    TdsTokens.writeRow(body, row, result.columns());
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

    // sp_executesql, sp_prepare, sp_execute, and sp_unprepare are the four RPC shapes actually
    // covering real parameterized-query traffic (JDBC PreparedStatement, .NET SqlCommand with
    // parameters, most ORMs) -- sp_executesql for a statement's first few executions, then a real
    // client (mssql-jdbc confirmed live) switches to sp_prepare (once) + sp_execute (every
    // execution after) + sp_unprepare (on close), the server-side statement-caching path.
    // sp_prepexec/sp_cursor*/arbitrary stored-procedure calls are still refused with a clean
    // error rather than attempted -- a driver that probes one of those first sees an error, not
    // the silent hang the old blanket RPC rejection risked for anything that assumed *some* RPC
    // response would come back. Proc IDs are the standard MS-TDS special-stored-procedure numbers
    // (2.2.6.6), not arbitrary constants.
    private static final int SP_EXECUTESQL_PROC_ID = 10;
    private static final int SP_PREPARE_PROC_ID = 11;
    private static final int SP_EXECUTE_PROC_ID = 12;
    private static final int SP_PREPEXEC_PROC_ID = 13;
    private static final int SP_UNPREPARE_PROC_ID = 15;

    // Confirmed live: mssql-jdbc's own client-side JDBC-escape processing for {@code
    // {call proc(?)}} (via CallableStatement) builds an sp_executesql @stmt whose text is
    // literally "EXEC proc  ?" -- a bare ODBC-style '?' marker, not an @P0-style named
    // placeholder -- alongside the real bound value as an ordinary unnamed RPC parameter. A bare
    // '?' is also matched here (see rewriteNamedParams's own handling of it) so this one real
    // client shape resolves correctly, not just the @P0/@name shapes.
    private static final Pattern NAMED_PARAM_PLACEHOLDER = Pattern.compile("@[A-Za-z_][A-Za-z0-9_]*|\\?");
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
        boolean isPrepare = request.procId() == SP_PREPARE_PROC_ID || "sp_prepare".equalsIgnoreCase(request.procName());
        boolean isExecute = request.procId() == SP_EXECUTE_PROC_ID || "sp_execute".equalsIgnoreCase(request.procName());
        boolean isPrepExec = request.procId() == SP_PREPEXEC_PROC_ID || "sp_prepexec".equalsIgnoreCase(request.procName());
        boolean isUnprepare = request.procId() == SP_UNPREPARE_PROC_ID || "sp_unprepare".equalsIgnoreCase(request.procName());
        if (isPrepare) {
            return handleSpPrepare(out, packets, request);
        }
        if (isExecute) {
            return handleSpExecute(out, packets, request);
        }
        if (isPrepExec) {
            return handleSpPrepExec(out, packets, request);
        }
        if (isUnprepare) {
            return handleSpUnprepare(out, packets, request);
        }
        if (!isExecSql) {
            if (request.procName() != null && !request.procName().isBlank()) {
                return handleUserDefinedProcCall(out, packets, request);
            }
            log.warn("mssqlwire: RPC call to proc #{} not supported -- only sp_executesql/sp_prepare/"
                    + "sp_execute/sp_prepexec/sp_unprepare/a named stored procedure are", request.procId());
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "only sp_executesql/sp_prepare/sp_execute/sp_prepexec/"
                            + "sp_unprepare/a named stored procedure call are supported (not a numeric "
                            + "system proc id)"));
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

    /**
     * A real, user-defined stored-procedure call ({@code {call myproc(?, ?)}}/{@code EXEC
     * myproc @p1, @p2} -- confirmed live via mssql-jdbc's {@code CallableStatement}: this arrives
     * as an ordinary RPC with {@code procName} set to the real procedure name and its arguments as
     * plain, unnamed, positional params, the exact same shape {@code sp_execute}'s bound params
     * already use). Previously refused outright with a clean error -- a real, common gap: any app
     * that calls its own stored procedures (not just ad-hoc parameterized SQL) got a hard failure
     * for every one of those calls.
     *
     * <p>Scope, deliberately narrow: this assumes the Postgres backend already has an
     * equivalently-named, equivalently-positioned {@code FUNCTION} (not a bare {@code PROCEDURE}
     * -- {@code SELECT * FROM name(...)} is what actually returns a result set for the common
     * "procedure that returns rows" case a real app relies on; a true void {@code PROCEDURE} with
     * no return value still executes correctly via this same call shape, just yields an empty
     * result set instead of a real error). OUT/INOUT parameters are NOT supported (there is no
     * data-dictionary lookup here the way {@code OracleProcedureCatalog} gives orawire's PL/SQL
     * path real parameter directions) -- every argument is sent as a plain IN value; a mismatch
     * (the real proc expects more args than an OUT parameter's caller sends actual values for)
     * surfaces as an ordinary backend SQL error, not silently wrong results.
     */
    private String handleUserDefinedProcCall(OutputStream out, TdsPacket packets, RpcRequestReader.RpcRequest request)
            throws IOException {
        String procName = request.procName();
        List<RpcRequestReader.RpcParam> params = request.params();
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(procName).append('(');
        List<Object> orderedBinds = new java.util.ArrayList<>(params.size());
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append('?');
            orderedBinds.add(params.get(i).value());
        }
        sql.append(')');
        String jdbcSql = sql.toString();
        executeQuery(out, packets, jdbcSql, orderedBinds, true);
        return jdbcSql;
    }

    /** {@code sp_prepare(@handle OUTPUT, @params, @stmt)} -- real SQL Server doesn't execute
     * anything at this step, it just hands back a handle the client uses in every later
     * {@code sp_execute} call for the same statement. Nothing here reaches a real backend either;
     * this only ever stores {@code @stmt}'s text against a new handle and returns it. */
    private String handleSpPrepare(OutputStream out, TdsPacket packets, RpcRequestReader.RpcRequest request)
            throws IOException {
        if (request.params().size() < 3 || !(request.params().get(2).value() instanceof String sql)) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "sp_prepare call missing a string @stmt parameter"));
            return null;
        }
        int handle = nextPreparedHandle.getAndIncrement();
        preparedRpcStatements.put(handle, sql);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        TdsTokens.writeReturnValueInt(body, 1, handle);
        TdsTokens.writeDoneProc(body, TdsTokens.doneFinalStatus(), 0, 0);
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
        return null; // Nothing executed against any backend -- no RTT sample to attribute.
    }

    /** {@code sp_execute(@handle, @P0, @P1, ...)} -- the handle names a statement this same
     * session's {@code sp_prepare} already stored; the rest is identical to
     * {@code sp_executesql}'s own bind-and-execute path, just against that remembered SQL text
     * instead of a freshly-supplied one. */
    private String handleSpExecute(OutputStream out, TdsPacket packets, RpcRequestReader.RpcRequest request)
            throws IOException {
        if (request.params().isEmpty() || !(request.params().get(0).value() instanceof Number handleValue)) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "sp_execute call missing an integer @handle parameter"));
            return null;
        }
        int handle = handleValue.intValue();
        String sql = preparedRpcStatements.get(handle);
        if (sql == null) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "sp_execute: no prepared statement for handle " + handle
                            + " (never sp_prepare'd on this connection, or already sp_unprepare'd)"));
            return null;
        }
        List<RpcRequestReader.RpcParam> boundParams = request.params().subList(1, request.params().size());
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

    /** {@code sp_prepexec(@handle OUTPUT, @params, @stmt, @P0, @P1, ...)} -- confirmed live as
     * the actual shape mssql-jdbc sends (not the separate {@code sp_prepare}+{@code sp_execute}
     * pair {@link #handleSpPrepare}/{@link #handleSpExecute} also support, for whichever client
     * DOES split them): prepare AND execute in the same round trip, remembering {@code @stmt}
     * under a new handle for any LATER {@code sp_execute} call the same statement reuses. */
    private String handleSpPrepExec(OutputStream out, TdsPacket packets, RpcRequestReader.RpcRequest request)
            throws IOException {
        if (request.params().size() < 3 || !(request.params().get(2).value() instanceof String sql)) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT,
                    TdsTokens.errorMessage(50000, "sp_prepexec call missing a string @stmt parameter"));
            return null;
        }
        int handle = nextPreparedHandle.getAndIncrement();
        preparedRpcStatements.put(handle, sql);
        List<RpcRequestReader.RpcParam> boundParams = request.params().subList(3, request.params().size());
        List<Object> orderedBinds = new java.util.ArrayList<>();
        String jdbcSql;
        try {
            jdbcSql = rewriteNamedParams(sql, boundParams, orderedBinds);
        } catch (IOException e) {
            packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, TdsTokens.errorMessage(50000, e.getMessage()));
            return null;
        }
        executeQuery(out, packets, jdbcSql, orderedBinds, true, handle);
        return jdbcSql;
    }

    /** {@code sp_unprepare(@handle)} -- releases a handle {@code sp_prepare} returned. A real
     * client calls this once it closes the {@code PreparedStatement} object; a handle that's
     * never explicitly unprepared just stays in {@link #preparedRpcStatements} for the rest of
     * the connection's lifetime, same as a real SQL Server session's own prepared-statement
     * cache would until the connection closes. */
    private String handleSpUnprepare(OutputStream out, TdsPacket packets, RpcRequestReader.RpcRequest request)
            throws IOException {
        if (!request.params().isEmpty() && request.params().get(0).value() instanceof Number handleValue) {
            preparedRpcStatements.remove(handleValue.intValue());
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        TdsTokens.writeDoneProc(body, TdsTokens.doneFinalStatus(), 0, 0);
        packets.writeMessage(out, TdsPacketType.TABULAR_RESULT, body.toByteArray());
        return null;
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
        int nextBarePlaceholder = 0;
        while (matcher.find()) {
            String name = matcher.group();
            RpcRequestReader.RpcParam match;
            if ("?".equals(name)) {
                if (nextBarePlaceholder >= boundParams.size()) {
                    throw new IOException("sp_executesql: more '?' placeholders than bound values sent");
                }
                match = boundParams.get(nextBarePlaceholder++);
            } else {
                match = resolveBoundParam(name, boundParams);
                if (match == null) {
                    throw new IOException("sp_executesql: no bound value found for placeholder " + name);
                }
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
