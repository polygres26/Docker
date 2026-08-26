package com.polygres.wire.orawire.session;

import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.ColumnInfo;
import com.polygres.wire.core.DialectErrorMessages;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.SqlStateErrorMapper;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.core.UntranslatableQueryException;
import com.polygres.wire.orawire.translator.BindVariableRewriter;
import com.polygres.wire.xa.XaTransaction;
import com.polygres.wire.orawire.translator.DualTableRewriter;
import com.polygres.wire.orawire.ttc.BindParam;
import com.polygres.wire.orawire.ttc.ColumnMetadata;
import com.polygres.wire.orawire.ttc.ExecuteRequest;
import com.polygres.wire.orawire.ttc.ExecuteRequestReader;
import com.polygres.wire.orawire.ttc.FetchRequest;
import com.polygres.wire.orawire.ttc.ResponseWriter;
import com.polygres.wire.orawire.ttc.TtcConstants;
import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import com.polygres.wire.server.ServerOptions;
import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RequestLoop {

    private static final Logger log = LoggerFactory.getLogger(RequestLoop.class);
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    private final TnsPacketReader reader;
    private final OutputStream out;
    private final com.polygres.wire.core.LazyPooledConnection pgConnection;
    private final com.polygres.wire.core.LazyPooledConnection oracleConnection;
    private final List<Connection> replicaConnections;
    private final XaTransaction xaTransaction;
    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final String oracleUsername;
    private final String oraclePassword;
    private com.polygres.wire.orawire.backend.NativeOracleExecutor nativeExecutor;
    private boolean nativeCursorOpen;

    private List<List<Object>> openRows;
    private List<ColumnMetadata> openColumns;
    private int fetchPosition;
    private int openCursorId = 0;
    private int nextCursorId = 1;

    // Set once this session's Execute parsing has needed ExecuteRequestReader's native-OCI
    // fallback (see readExecuteRequest) -- a real distributed-database-link connection's client,
    // confirmed live, not any client this codebase already handled correctly. Used to append the
    // extra trailer real Oracle sends after an Execute response for that same client shape (see
    // NATIVE_OCI_EXECUTE_TRAILER's javadoc); every other tested client's Execute responses already
    // work without it, so this stays opt-in per-session rather than unconditional.
    private boolean usedNativeOciExecuteFallback;

    private final Map<Integer, StatementSignature> statementSignatures = new HashMap<>();

    private volatile String lastSqlText;
    // Separate from lastSqlText (which stays the raw pre-rewrite Oracle text, e.g. bind
    // variables as ":1"/"name") specifically for RTT labeling -- recordRtt normalizes and looks
    // up the SAME per-fingerprint entry the exec-time path already created, and that path
    // records the REWRITTEN sql (rewritten.sql(), "?" placeholders) via Statement.of(). Using
    // lastSqlText there looked plausible and compiled fine, but silently missed every lookup
    // (different normalized string -> different map key), so orawire's per-query avgRttMs in
    // /api/metrics/summary always came back null despite the overall counter being correct --
    // found only by actually reading the API response, not from re-reading this code.
    private volatile String lastRewrittenSqlText;
    private final FailedStatementLog failedStatementLog;
    private final com.polygres.wire.core.SqlMetricsCollector sqlMetrics;

    // Built once per session and reused across every Execute, instead of a fresh
    // JdbcBackendExecutor + RoutingBackendExecutor + StatementPipeline object graph per call --
    // same pattern PgWireSessionHandler already uses. primaryConn is stable across a session's
    // Execute calls (LazyPooledConnection caches the physical Connection after its first open,
    // and dual/authoritativeIsOracle can't change mid-session), so rebind() is enough; no need to
    // rebuild the chain. This also fixes a latent gap: a fresh RoutingBackendExecutor per call
    // meant it could never accumulate its own cross-statement transaction/cursor routing state
    // within one session -- reusing one now matches how pgwire's already behaves.
    //
    // The NativeRlsSessionInitializer here is always PostgresRlsSessionInitializer, never
    // OracleVpdSessionInitializer, even though this is the Oracle wire protocol: every
    // SessionHandler.run() construction site actually reaching RequestLoop passes primaryConn =
    // pgConnection (Postgres JDBC, running the Oracle SQL DualTableRewriter-translated) -- the
    // real-Oracle-backend path (dualExecAuthority=ORACLE + oracleBackendMode=NATIVE) is
    // intercepted earlier, in SessionHandler.run(), by a raw TNS byte relay
    // (NativeSessionRelay) that never constructs a RequestLoop or JdbcBackendExecutor at all.
    // So the JDBC connection this executor's set_config(...) calls land on genuinely is
    // Postgres, and OracleVpdSessionInitializer's SYS_CONTEXT/DBMS_SESSION.SET_CONTEXT-shaped
    // calls would simply fail against it -- it stays reserved for a future NativeOracleExecutor
    // RLS path, which would need its own, separate wiring at a completely different layer.
    private final JdbcBackendExecutor terminalExecutor =
            new JdbcBackendExecutor(null, new com.polygres.wire.core.access.PostgresRlsSessionInitializer());
    private final StatementPipeline reusablePipeline;
    private final com.polygres.wire.core.AccessContext accessContext;

    private record StatementSignature(String sql, int[] bindTypes) {
    }

    public RequestLoop(TnsPacketReader reader, OutputStream out, com.polygres.wire.core.LazyPooledConnection pgConnection,
            com.polygres.wire.core.LazyPooledConnection oracleConnection, List<Connection> replicaConnections,
            XaTransaction xaTransaction,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(reader, out, pgConnection, oracleConnection, replicaConnections, xaTransaction, options, sharedStages,
                backendRegistry, null, null, com.polygres.wire.core.AccessContext.ANONYMOUS);
    }

    public RequestLoop(TnsPacketReader reader, OutputStream out, com.polygres.wire.core.LazyPooledConnection pgConnection,
            com.polygres.wire.core.LazyPooledConnection oracleConnection, List<Connection> replicaConnections,
            XaTransaction xaTransaction,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry,
            String oracleUsername, String oraclePassword) {
        this(reader, out, pgConnection, oracleConnection, replicaConnections, xaTransaction, options, sharedStages,
                backendRegistry, oracleUsername, oraclePassword, com.polygres.wire.core.AccessContext.ANONYMOUS);
    }

    public RequestLoop(TnsPacketReader reader, OutputStream out, com.polygres.wire.core.LazyPooledConnection pgConnection,
            com.polygres.wire.core.LazyPooledConnection oracleConnection, List<Connection> replicaConnections,
            XaTransaction xaTransaction,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry,
            String oracleUsername, String oraclePassword, com.polygres.wire.core.AccessContext accessContext) {
        this.accessContext = accessContext;
        this.reader = reader;
        this.out = out;
        this.pgConnection = pgConnection;
        this.oracleConnection = oracleConnection;
        this.replicaConnections = replicaConnections == null ? List.of() : replicaConnections;
        this.xaTransaction = xaTransaction;
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.oracleUsername = oracleUsername;
        this.oraclePassword = oraclePassword;
        this.failedStatementLog = new FailedStatementLog(options);
        this.failedStatementLog.ensureSchema();
        this.sqlMetrics = com.polygres.wire.core.StatsCollectorStage.findIn(sharedStages);
        this.reusablePipeline = new StatementPipeline(sharedStages,
                new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, terminalExecutor,
                new com.polygres.wire.xa.XaRecoveryLog(options)));
    }

    public void run() throws IOException {
        try {
            while (true) {
                TnsPacket packet = reader.readPacket();
                switch (packet.type()) {
                    case DATA -> {
                        
                        if (packet.payload().length == 0) {
                            continue;
                        }
                        if (handleData(packet)) {
                            return;
                        }
                    }
                    case MARKER -> handleMarker(packet);
                    case ABORT, NULL -> {
                        return;
                    }
                    default -> throw new IOException("Unexpected packet type in request loop: " + packet.type());
                }
            }
        } finally {
            if (nativeExecutor != null) {
                nativeExecutor.close();
            }
        }
    }

    private boolean handleData(TnsPacket packet) throws IOException {
        TtcReader r = new TtcReader(packet.payload());
        int messageType = r.readUint8();
        while (messageType == TtcConstants.MSG_TYPE_PIGGYBACK) {

            skipPiggyback(r);
            // A real distributed-database-link connection's native OCI client sends a piggyback as
            // the ENTIRE contents of its own TNS packet, with no FUNCTION call chained after it in
            // the same packet (confirmed live via a real capture against a real Oracle 23c
            // instance) -- unlike every other tested client, which always follows a piggyback with
            // a real function call in the same read. Nothing to reply to and no more bytes to read;
            // returning here (not logging off) lets the request loop go back to reading the next
            // packet instead of trying to read a messageType byte past the end of this one.
            if (!r.hasRemaining()) {
                return false;
            }
            messageType = r.readUint8();
        }
        if (messageType != TtcConstants.MSG_TYPE_FUNCTION) {
            throw new IOException("expected function-call message, got type " + messageType);
        }
        int functionCode = r.readUint8();
        int wireSequenceNumber = r.readUint8();

        // This field is normally a chunked (length-prefixed) UB8 -- real for every previously
        // tested client (JDBC/sqlplus/SQLcl), which always sends 0 here regardless of function
        // code, so a chunked-zero read (1 byte, value 0) has always been indistinguishable from
        // whatever the field's true width actually is. A real distributed-database-link
        // connection's native OCI client breaks that: confirmed live against a real Oracle 23c
        // instance, its own FUNC_FETCH request carries a real, varying 2-byte value here (a
        // per-call nonce/tag, not a length -- two separate live captures of the identical fetch
        // scenario had different values in exactly these 2 bytes, with every other byte of the
        // request byte-identical) that is NOT safely decodable as a chunked UB8 (its high byte
        // routinely exceeds any plausible chunk length, e.g. 0xf3, and reading it as one throws
        // trying to consume far more bytes than the packet has). See readNativeOciFetchRequest's
        // javadoc for the rest of this request's real, fixed-width layout for this client.
        if (usedNativeOciExecuteFallback && functionCode == TtcConstants.FUNC_FETCH) {
            r.skip(2);
        } else {
            r.readUb8();
        }
        int callNumber = wireSequenceNumber;

        TtcWriter w = new TtcWriter();
        boolean logoff = false;
        // RTT: wraps the WHOLE dispatch below through sendData() at the bottom of this method --
        // not just the handle* call -- since handle* only fills an in-memory TtcWriter buffer;
        // the actual socket write happens once, afterward, shared by every function code. Only
        // recorded for the statement-shaped function codes (Execute/Fetch/Reexecute) against
        // lastSqlText, which Execute sets and stays valid for a Fetch against the same open
        // cursor. Fetch gets its own honest sample here: unlike pgwire's Bind-vs-Execute split,
        // Fetch does no backend re-execution and has no client-paced gap of its own -- it's a
        // clean one-message-in-one-response-out cycle, just like Execute's initial batch.
        long rttStart = System.nanoTime();
        try {
            if (functionCode == TtcConstants.FUNC_EXECUTE) {
                handleExecute(readExecuteRequest(r, packet), w, callNumber);
            } else if (functionCode == TtcConstants.FUNC_FETCH) {
                handleFetch(usedNativeOciExecuteFallback ? readNativeOciFetchRequest(r) : FetchRequest.read(r), w,
                        callNumber);
            } else if (functionCode == TtcConstants.FUNC_REEXECUTE
                    || functionCode == TtcConstants.FUNC_REEXECUTE_AND_FETCH) {
                handleReexecute(r, w, callNumber, functionCode == TtcConstants.FUNC_REEXECUTE_AND_FETCH);
            } else if (functionCode == TtcConstants.FUNC_LOGOFF) {
                handleLogoff();
                ResponseWriter.writeSuccessEnd(w, 0, 0, callNumber);
                logoff = true;
            } else if (functionCode == TtcConstants.FUNC_COMMIT) {
                commitAll();
                ResponseWriter.writeSuccessEnd(w, 0, openCursorId, callNumber);
            } else if (functionCode == TtcConstants.FUNC_ROLLBACK) {
                if (xaTransaction != null) {
                    xaTransaction.rollback();
                } else {
                    pgConnection.rollback();
                    if (oracleConnection != null) {
                        runShadow(oracleConnection::rollback, "rollback");
                    }
                    for (Connection replica : replicaConnections) {
                        runShadow(replica::rollback, "rollback");
                    }
                }
                ResponseWriter.writeSuccessEnd(w, 0, openCursorId, callNumber);
            } else if (functionCode == FUNC_SESSION_NLS_SETUP) {
                // A real distributed-database-link connection's native OCI client sends TWO
                // structurally different calls under this same function code (67, undocumented
                // publicly), confirmed live against a real Oracle 23c instance:
                //   1. Right after the post-login banner exchange, carrying a literal
                //      `ALTER SESSION SET NLS_LANGUAGE=... TIME_ZONE=... SKIP_UNUSABLE_INDEXES=...`
                //      statement -- Oracle session-format/locale settings with no Postgres
                //      equivalent. Real Oracle answers with a structured reply that echoes each
                //      NLS_* setting back individually.
                //   2. Later, carrying a distributed-session identifier string of the shape
                //      `<SERVICE_NAME>[<version>][<n>]<SERVICE_NAME>.<hex>.<version>` (a global
                //      transaction/session cookie, not SQL text) -- answered with a different,
                //      much shorter structured reply.
                // Sending the first reply's bytes back for the second call (this codebase's first
                // attempt, since neither call is otherwise parsed at the TTC level) causes the same
                // TNS BREAK/RESET reaction every other structurally-wrong response in this series
                // has -- confirmed live. Distinguish by the one cheap, reliable signal available
                // without a real parser: whether the request text contains "ALTER SESSION".
                byte[] requestBytes = r.readRemaining();
                boolean isNlsAlterSession = indexOfAscii(requestBytes, "ALTER SESSION") >= 0;
                String responseB64 = isNlsAlterSession ? SESSION_NLS_SETUP_RESPONSE_B64
                        : SESSION_ID_REGISTER_RESPONSE_B64;
                w.writeRaw(java.util.Base64.getDecoder().decode(responseB64));
            } else if (functionCode == FUNC_UNKNOWN_202) {
                // Another real-dblink-client-only call this codebase has no parser for -- comes
                // right after FUNC_SESSION_NLS_SETUP, confirmed live against a real Oracle 23c
                // instance. Its request and response payloads are dominated by large opaque
                // base64-shaped blobs (~1KB), unlike every other call in this series, which strongly
                // suggests real per-session cryptographic or key-exchange material rather than
                // plain protocol/session metadata -- meaning replaying a fixed captured response
                // here (the strategy that worked for the two calls above) is a real risk: if the
                // client actually uses this content afterward (e.g. as key material for encrypting
                // later traffic), a fixed replayed value from a DIFFERENT real session won't match
                // and later traffic could silently corrupt rather than cleanly fail. Tried anyway,
                // specifically to learn how far a fixed reply gets before that risk materializes,
                // rather than guessing blind -- see this method's caller-side notes for what was
                // observed.
                w.writeRaw(java.util.Base64.getDecoder().decode(FUNC_UNKNOWN_202_RESPONSE_B64));
            } else if (usedNativeOciExecuteFallback && functionCode == FUNC_UNKNOWN_68) {
                // The real dblink native-OCI client's next call after a successful Execute response
                // (this whole series's dblink-compatibility milestone: the client accepted an
                // Execute response and moved on instead of reacting with a TNS BREAK/RESET, for the
                // first time) -- confirmed live against a real Oracle 23c instance. Content-wise
                // this looks like plain, non-cryptographic, non-session-specific numeric fields
                // (unlike FUNC_UNKNOWN_202), so replaying a fixed real response carries much less
                // risk here; done the same way as every other undocumented call in this series --
                // except this response DOES encode something query-specific (confirmed by diffing
                // two real captures against each other, a 2-column and a 3-column SELECT: a byte at
                // a fixed offset was 2 in one and 3 in the other, matching each capture's own column
                // count), so that one byte is patched per-response instead of just replayed
                // statically like the rest of the template.
                byte[] response = java.util.Base64.getDecoder().decode(FUNC_UNKNOWN_68_RESPONSE_B64);
                response[FUNC_UNKNOWN_68_COLUMN_COUNT_OFFSET] = (byte) openColumns.size();
                w.writeRaw(response);
            } else {
                throw new UnsupportedOperationException("unsupported TTC function code: " + functionCode);
            }
        } catch (UntranslatableQueryException e) {
            log.warn("statement could not be translated: {}", e.getMessage());
            failedStatementLog.record(SourceDialect.ORACLE, lastSqlText,
                    FailedStatementLog.FailureType.UNTRANSLATABLE, null, null, e.getMessage());
            rollbackAfterStatementError();
            ResponseWriter.writeErrorEnd(w, SqlStateErrorMapper.ORACLE_DEFAULT,
                    e.getMessage() == null ? "statement could not be translated" : e.getMessage(), openCursorId, callNumber);
        } catch (SQLException e) {
            log.warn("backend error executing statement: {}", e.getMessage());
            int nativeError = SqlStateErrorMapper.toOracleError(e.getSQLState(), e.getMessage());
            failedStatementLog.record(SourceDialect.ORACLE, lastSqlText,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            rollbackAfterStatementError();
            // The client sees the dialect-native ORA-xxxxx wording (real Oracle message text,
            // not Postgres's) when one exists for this SQLSTATE -- the audit log line above still
            // records Postgres's own raw message, since that's the more useful text for debugging
            // the real backend failure.
            String clientMessage = e.getMessage() == null ? "backend error"
                    : DialectErrorMessages.render(SourceDialect.ORACLE, e.getSQLState(), e.getMessage());
            ResponseWriter.writeErrorEnd(w, nativeError, clientMessage, openCursorId, callNumber);
        } catch (RuntimeException e) {
            
            log.warn("unexpected error executing statement: {}", e.toString(), e);
            rollbackAfterStatementError();
            ResponseWriter.writeErrorEnd(w, 942, e.getMessage() == null ? e.toString() : e.getMessage(), openCursorId, callNumber);
        }
        sendData(w.toByteArray());
        if (sqlMetrics != null && lastRewrittenSqlText != null && isStatementShaped(functionCode)) {
            sqlMetrics.recordRtt(SourceDialect.ORACLE, lastRewrittenSqlText, System.nanoTime() - rttStart);
        }
        return logoff;
    }

    /**
     * A real distributed-database-link connection's native OCI client sends Execute requests that
     * are NOT field-compatible with {@link ExecuteRequestReader#read}'s layout (tuned against
     * JDBC/sqlplus/SQLcl, confirmed live to keep working correctly against a real Oracle 23c
     * instance -- this fallback only ever triggers on a request shape none of those clients send).
     * On the {@link ArrayIndexOutOfBoundsException} that shape causes, retry with
     * {@link ExecuteRequestReader#readByScanningForSql}, a content-scanning reader built
     * specifically for a dblink-forwarded query's shape, against the packet's own raw bytes (not
     * {@code r}, whose position is already past recovery after a mid-parse failure).
     */
    private ExecuteRequest readExecuteRequest(TtcReader r, TnsPacket packet) {
        try {
            return ExecuteRequestReader.read(r);
        } catch (ArrayIndexOutOfBoundsException e) {
            log.info("Execute request didn't match the known field layout (real dblink native-OCI "
                    + "client?) -- falling back to scanning its raw bytes for a SQL statement: {}",
                    e.toString());
            usedNativeOciExecuteFallback = true;
            return ExecuteRequestReader.readByScanningForSql(packet.payload());
        }
    }

    /**
     * A real distributed-database-link connection's native OCI client's FUNC_FETCH request isn't
     * field-compatible with {@link FetchRequest#read}'s generic chunked-UB4-pair layout either --
     * confirmed live against a real Oracle 23c instance via two independent live captures of the
     * identical fetch scenario (this codebase's own dblink test, run twice): every byte of the
     * request was identical between the two runs except a 2-byte value right after the function
     * code/sequence number (already consumed by the caller as a raw, not chunked, skip -- see its
     * call site's comment) -- strong evidence of a real, fixed-width layout for this client, not
     * chunked fields whose byte count would itself vary run to run. Working out from that shared
     * structure: a 4-byte zero field, a 1-byte flag (0x0f in every capture, including a genuine
     * real Oracle-to-Oracle self-loop's own version of this same call -- a real protocol constant,
     * not session-specific data), another 4-byte zero field, then two raw (not chunked) 4-byte
     * big-endian values whose low byte varied sensibly between captures (a small integer in both
     * cases) -- read here as the cursor id and requested fetch row count respectively, the same
     * pair {@link FetchRequest#read} extracts for every other client, just encoded differently.
     */
    private FetchRequest readNativeOciFetchRequest(TtcReader r) {
        r.skip(4);
        r.readUint8();
        r.skip(4);
        long cursorId = r.readUint32BE();
        long fetchArraySize = r.readUint32BE();
        return new FetchRequest(cursorId, fetchArraySize);
    }

    // The tail a real Oracle server writes between the last column's DESCRIBE_INFO block and the
    // end of its Execute response, for this same real distributed-database-link client -- recovered
    // by diffing this exact 220-byte span from TWO separate real Oracle-to-Oracle self-loop
    // captures (a 2-column and a 3-column SELECT) against each other. (An earlier version of this
    // fix mis-measured this span as two separate pieces, 155+97 bytes, because it mis-measured the
    // preceding column blocks' own width as a fixed 90 bytes -- confirmed WRONG once a real 3-column
    // capture showed column width actually scales with the column's name length; see
    // writeColumnMetadataNativeOci. Fixing that column-width bug shifted where this tail actually
    // starts, which is why it's now one 220-byte span instead of two mismatched ones.) Only 11 of
    // the 220 bytes actually varied between the two real captures -- a few response-timestamp bytes
    // inside an embedded current-date field, what looks like a computed row-length/statistics pair,
    // and a handful more of what's likely a session nonce -- everything else, including whether the
    // response was for 2 or 3 columns, was byte-for-byte identical, which is why this is used as a
    // fixed template with no per-call patching at all. The un-patched varying bytes are left as this
    // template's own captured values rather than recomputed or randomized -- they were never what
    // made either real capture differ in whether the client accepted the response.
    //
    // Real Oracle's own DESCRIBE_INFO for this client has NO recognizable inline row-value bytes in
    // either real capture this was built from (no Oracle NUMBER-encoding markers anywhere) -- this
    // method's caller skips this codebase's own row-data writing entirely for this fallback path, on
    // the theory the client fetches rows via a separate real FETCH call instead of inline. (Tried in
    // isolation first -- numIters=0, no row writing, no tail template -- and made no difference on
    // its own; combined with this tail template is what's actually being tried here.)
    private static final String NATIVE_OCI_EXECUTE_TAIL_B64 =
        "BwAAAAd4fggaBBsoAQAAAOgfAABdAAAAXQAAAAAAAAAIBgAAAAAAAAAAAAcAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAQDAAAA3QsBAAAAAAAAAAAAAAcAHgADAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKAAAAAAAANgEAAAAAAAAAAAAAAAAAALA0WG/w6gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAHQ==";

    private void writeNativeOciExecuteTail(TtcWriter w) {
        w.writeRaw(java.util.Base64.getDecoder().decode(NATIVE_OCI_EXECUTE_TAIL_B64));
    }

    private static boolean isStatementShaped(int functionCode) {
        return functionCode == TtcConstants.FUNC_EXECUTE || functionCode == TtcConstants.FUNC_FETCH
                || functionCode == TtcConstants.FUNC_REEXECUTE || functionCode == TtcConstants.FUNC_REEXECUTE_AND_FETCH;
    }

    private void handleLogoff() throws SQLException {
        closeOpenCursor();
    }

    private void rollbackAfterStatementError() {
        try {
            pgConnection.rollback();
        } catch (SQLException rollbackFailure) {
            log.warn("rollback after statement error also failed: {}", rollbackFailure.getMessage());
        }
        
        if (oracleConnection != null) {
            try {
                oracleConnection.rollback();
            } catch (SQLException rollbackFailure) {
                log.warn("shadow (oracle) rollback after statement error also failed: {}", rollbackFailure.getMessage());
            }
        }
        for (Connection replica : replicaConnections) {
            try {
                replica.rollback();
            } catch (SQLException rollbackFailure) {
                log.warn("replica rollback after statement error also failed: {}", rollbackFailure.getMessage());
            }
        }
    }

    private void commitAll() throws SQLException {
        if (xaTransaction != null) {
            xaTransaction.commit();
        } else {
            pgConnection.commit();
            if (oracleConnection != null) {
                runShadow(oracleConnection::commit, "commit");
            }
            for (Connection replica : replicaConnections) {
                runShadow(replica::commit, "commit");
            }
        }
    }

    private void skipPiggyback(TtcReader r) throws IOException {
        int piggybackFunctionCode = r.readUint8();
        r.readUint8();
        r.readUb8();
        if (piggybackFunctionCode == FUNC_CLIENT_BANNER_REQUEST) {
            // A real distributed-database-link connection's native OCI client sends this piggyback
            // (function code 107, undocumented publicly; its payload includes a plain "SQL*Plus"
            // client-program-name string) right after login and, unlike every other piggyback this
            // codebase handles, actually expects a reply -- confirmed live via a real capture
            // against a real Oracle 23c instance: the real server replies with the connection
            // banner text sqlplus prints right after "Connected to:" (a real Oracle server sent
            // "Oracle AI Database 26ai Free Release 23.26.2.0.0 - ..."). Not replying at all left
            // the client waiting forever for this banner instead of proceeding to its real query.
            r.readRemaining();
            sendBanner();
        } else if (piggybackFunctionCode == TtcConstants.FUNC_CLOSE_CURSORS
                || piggybackFunctionCode == TtcConstants.FUNC_CANCEL_ALL) {

            r.readUint8();
            long numCursors = r.readUb4();
            for (long i = 0; i < numCursors; i++) {
                r.readUb4();
            }
            // A real distributed-database-link connection's native OCI client's FUNC_CLOSE_CURSORS
            // piggyback doesn't end here the way it does for JDBC/sqlplus/SQLcl -- confirmed live
            // against a real Oracle 23c instance via two independent real captures (a real
            // Oracle-to-Oracle self-loop and this codebase's own dblink test scenario), byte-length
            // identical despite carrying different session-specific values: with numCursors=0, 31
            // more bytes follow (an 8-byte -2/"invalid" cursor-id sentinel, then further opaque
            // session/call-tag fields whose exact field-by-field layout wasn't needed to get past
            // them -- only their fixed total width, confirmed the same across both captures) before
            // a real chained FUNCTION message -- a genuine second EXECUTE (function code 94) for the
            // dblink's actual remote-fetch query, reusing real column aliases (A1.ID, A1.AMOUNT)
            // instead of the first EXECUTE's plain "SELECT *". (An earlier version of this skip used
            // 21 bytes, from a hand-traced byte count that wrongly assumed readUb8()/readUb4() are
            // fixed-width -- they're actually Oracle's usual chunked length-prefixed encoding, e.g.
            // a lone 0x00 length byte for a zero value, not "8 bytes"/"4 bytes"; replaying the
            // parse with that corrected accounting is what found the true, live-confirmed gap of 31
            // bytes from here to the real FUNCTION tag.) Skipping straight past this piggyback
            // without also consuming these bytes left the caller's next messageType read landing on
            // padding mid-structure instead of that FUNCTION tag ("expected function-call message,
            // got type 0"). Scoped to the native-OCI Execute fallback specifically --
            // JDBC/sqlplus/SQLcl's own FUNC_CLOSE_CURSORS piggybacks have never been observed to
            // carry this trailing content, and a numCursors=0 close-cursors call from them really
            // does just end here.
            if (usedNativeOciExecuteFallback) {
                r.skip(31);
            }
        } else if (piggybackFunctionCode == TtcConstants.FUNC_CLIENT_FEATURES) {
            
            r.readUint8();
            long featureBytesLength = r.readUb4();
            r.readUint8();
            r.skip((int) featureBytesLength);
        } else if (piggybackFunctionCode == TtcConstants.FUNC_SET_END_TO_END_ATTR) {
            
            r.readUint8();
            r.readUint8();
            r.readUb4();
            r.readUint8();
            long clientIdentifierLength = r.readUb4();
            r.readUint8();
            long moduleLength = r.readUb4();
            r.readUint8();
            long actionLength = r.readUb4();
            r.readUint8();
            r.readUb4();
            r.readUint8();
            r.readUb4();
            r.readUint8();
            long clientInfoLength = r.readUb4();
            r.readUint8();
            r.readUb4();
            r.readUint8();
            r.readUb4();
            r.readUint8();
            long dbopLength = r.readUb4();
            
            r.readRawOrLengthPrefixedBytes((int) clientIdentifierLength);
            r.readRawOrLengthPrefixedBytes((int) moduleLength);
            r.readRawOrLengthPrefixedBytes((int) actionLength);
            r.readRawOrLengthPrefixedBytes((int) clientInfoLength);
            r.readRawOrLengthPrefixedBytes((int) dbopLength);
        } else {
            // An unrecognized piggyback function code. Rather than fail the whole session over a
            // best-effort piggyback this codebase doesn't have specific field-level parsing for,
            // consume the rest of this packet and move on -- every piggyback this client has been
            // observed sending occupies the entirety of its own TNS packet with nothing further to
            // read afterward (see the caller's hasRemaining() check), so discarding an unrecognized
            // one's trailing bytes is no worse than not understanding its fields would already be.
            r.readRemaining();
        }
    }

    // Function code of the "client banner request" piggyback -- see skipPiggyback's javadoc for
    // where this was discovered. Not present in TtcConstants alongside the documented piggyback
    // function codes since Oracle doesn't document this one publicly; kept local to where it's
    // used rather than added to that shared, otherwise-documented constant set.
    private static final int FUNC_CLIENT_BANNER_REQUEST = 107;

    // See the FUNC_SESSION_NLS_SETUP branch's comment above for what this call actually carries.
    private static final int FUNC_SESSION_NLS_SETUP = 67;

    // The exact real reply bytes captured from a genuine Oracle-to-Oracle self-loop dblink
    // session's response to this same call: echoes each NLS_* setting from the client's ALTER
    // SESSION statement back individually (as real Oracle values -- AMERICAN/AMERICA/AL32UTF8/etc,
    // not whatever the client actually asked for), plus a "SQL*Plus" client-name echo and the same
    // kind of fixed trailer/marker structure seen elsewhere in this series. Used verbatim rather
    // than reconstructed field-by-field: this call's real wire format is a different, undocumented
    // shape from the ordinary EXECUTE/DEFINE response format ResponseWriter otherwise builds, and
    // the client was confirmed live to need this specific structured content, not just any
    // well-formed acknowledgement.
    private static final String SESSION_NLS_SETUP_RESPONSE_B64 =
        "CAMAAAAAAAAAAAADAAAAAQABEgBhZjJjZmZmYSJGUkVFUERCMSICAETLdQAAAAAAFwUBABAXAAAAFgAAAAAIAAAACEFNRVJJQ0FOEAAAAAAABwAAAAdBTUVSSUNBCQAAAAAAAQAAAAEkAAAAAAAABwAAAAdBTUVSSUNBAQAAAAAAAgAAAAIuLAIAAAAAAAgAAAAIQUwzMlVURjgKAAAAAAAJAAAACUdSRUdPUklBTgwAAAAAAAkAAAAJREQtTU9OLVJSBwAAAAAACAAAAAhBTUVSSUNBTggAAAAAAAYAAAAGQklOQVJZCwAAAAAADgAAAA5ISC5NSS5TU1hGRiBBTTkAAAAAABgAAAAYREQtTU9OLVJSIEhILk1JLlNTWEZGIEFNOgAAAAAAEgAAABJISC5NSS5TU1hGRiBBTSBUWlI7AAAAAAAcAAAAHERELU1PTi1SUiBISC5NSS5TU1hGRiBBTSBUWlI8AAAAAAABAAAAASQ0AAAAAAAGAAAABkJJTkFSWTIAAAAAAAQAAAAEQllURT0AAAAAAAUAAAAFRkFMU0U+AAAAAAALAAAAC4AAAAA8PDyAAAAAowAGAAAABkFDVElWRQAAAAC7AAAAAAABAAAAAQCkAAgAAAAIU1FMKlBsdXMAAAAAuAAAAAAAAAAAALkAAAAAAAkBAAEAIwwd";

    // The second FUNC_SESSION_NLS_SETUP call's real reply -- see that branch's comment. This one
    // has one obviously session-specific field (a small hex/counter value near the front of the
    // real capture this was taken from); reusing it verbatim is the same accepted risk as
    // FUNC_UNKNOWN_202's response, on the same "confirm how far this gets, rather than block on a
    // full parser" basis.
    private static final String SESSION_ID_REGISTER_RESPONSE_B64 =
        "CAMAZxknAwAAAAAAAAAAAgBny3UAAAAAABcFAQAQAQAAABYEAAAABEhJR0gAAAAAzAAAAAAACQMAAAAlDB0=";

    /** Byte-substring search for ASCII text within a request payload, used only to distinguish
     * two otherwise-unparsed real shapes of the same undocumented function code -- see
     * FUNC_SESSION_NLS_SETUP's comment. Not a general-purpose text search: no encoding handling,
     * just literal ASCII byte matching, which is all that's needed for a fixed marker string. */
    private static int indexOfAscii(byte[] haystack, String needleAscii) {
        byte[] needle = needleAscii.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    // See the FUNC_UNKNOWN_202 branch's comment above.
    private static final int FUNC_UNKNOWN_202 = 202;

    private static final String FUNC_UNKNOWN_202_RESPONSE_B64 =
        "CNgARlp0YlE0Y0JDcWt2WEMvVWZ1QytDejdxZ1ZPcnlwSXRFb0V2aVBIN0Q0RXJoMkh1S1h4Wk1EK0FHd2F5cjZ2dkdNd1NjcEJ6" +
        "UVpJMnoxR1JVbUdMeGk1akV4MXVOZU8wZHNieGtjZGk3QmdVc3NBYnNlb1ZZL2QyS1R1UXVCdmhzVHlEcUJmd3gzbmd1dHg4QmNF" +
        "SlZ5djJiKzV2NVg3b1hORldDY3kwKyszaWJ0VnUzdHRjS2h3dmJCL3A5emphN2NleCtGMHNxcFpTMkxIMGdIdEMweVU92AJGZXNX" +
        "Z2pOaWhGUnk4Ym5aRzhiN0tBTVhrM3o1Ty9Xc28welI0V0hSVzRJMnhCV3FEdjNtbmRvWWdVc1FyS3dEc2JyTGhQN2hOalhaUnl6" +
        "TVV0MGhFNU16bjZ0SndUcXRLSGtNc2V6MmY5em9CblhiakZac3NCODB1THU4a1dkNDhJbXVyZjMwZXBXN0gveHY5UkxuVkNodUh5" +
        "MHJhVGlQZWpXUTdhZnUwSGlYdHNkQnJHaWp2K3AzSThONTRkdmlBU093akREUk4xVlpmN3MwT2V6UVY2QWRIaGdrRlk0NDhQQXJX" +
        "TXVmRnBBZmxsZnIwYVdmUkx3ZEpzQlVEeGFROGNEYml6TFFtSVRhOUpzMWFBTG9jNVA4U3RHVnBwYTNZSDhjZlQ5cmVTZzdhb2Ri" +
        "MjBTMHJ3L3dNUk9lUllMY3Q1RXZyaWh0WUI3VS9xRzdxejBBZmxMT0wvZjRhaVdJeFd3TEgvbU11ZlpHQ3c3VWhJUFVXclpaMFVI" +
        "b3NlYUF5ZFRrV045V1lXeXhrSFd2anZneWdWNnRjdEN2Nk9Qd29TTnlLV0hQZW15bHFaOFhEaEJpNHRJaUt0TnQzcStuVXNTZWRj" +
        "bUMrbnhMb0w1NXBmdExuQ0NXcU9EV1UyNHRRNGo4ak11T05VVGhmM0xpRndUUXF6K2FNN0xhMVNzY0dNTUdZR0ZkNEtHMHlhcVFV" +
        "YXAwRlVkYmkwWHkxN1JOZTQ2elVLZnozbk9naHZma0JYNkUzMkJvV2VibENKWC9yeDZrZVhVVTlHdXVWNzBlRUMyb21JdS9qeXVa" +
        "bzlHYVE4L2V5RzhQRTNkK0c3Zk1WWWZEZjNXcUR4cFNCWWdZTnUxL1N3UlhkYzAxazRlOXo4bTFGc214dUhGUFpOZkR4bWFEcGQ3" +
        "SDhxOVRLU2NDT1pmVlo5c0trdHhKMDJtdVZkMVh4N2NWWk15ZjFheVgxWWp2R0ZJPYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJAQAAACQMHQ==";

    // See the FUNC_UNKNOWN_68 branch's comment above.
    private static final int FUNC_UNKNOWN_68 = 68;

    // Real bytes from a 2-column SELECT's real capture -- matches this codebase's own dblink test
    // scenario's column count, so FUNC_UNKNOWN_68_COLUMN_COUNT_OFFSET's patch is a no-op against
    // this particular template; kept patched anyway since a 2-column response was confirmed WRONG
    // when it carried a real 3-column capture's own baked-in count (a real Oracle 23c client
    // reported ORA-02072, "distributed database network protocol mismatch", on that mismatch).
    private static final String FUNC_UNKNOWN_68_RESPONSE_B64 =
        "CAgAjec12rsAAABSAAAAAQAAAAIAAABkAAAAAgAAAAAAAAAEAAIAAAAJAwAAAMUSHQ==";
    private static final int FUNC_UNKNOWN_68_COLUMN_COUNT_OFFSET = 27;

    // The trailer bytes real Oracle sends after the banner's null terminator: a fixed prefix, a
    // 2-byte "varying" marker (randomized below, same pattern as the marker
    // O5LogonHandler.PHASE_ONE_TERMINATOR_VARYING_OFFSET/PHASE_TWO_RICH_CALLNUMBER_OFFSET both
    // have), then a final terminating byte -- confirmed byte-for-byte via a real capture against a
    // real Oracle 23c instance.
    private static final byte[] BANNER_TRAILER_PREFIX = { 0x00, 0x00, 0x17, 0x09, 0x01, 0x00, 0x00, 0x00 };
    private static final byte BANNER_TRAILER_TERMINATOR = 0x1d;

    private static final String STATIC_BANNER_PAYLOAD_B64 =
        "CFMAT3JhY2xlIEFJIERhdGFiYXNlIDI2YWkgRnJlZSBSZWxlYXNlIDIzLjI2LjIuMC4wIC0gRGV2ZWxvcCwgTGVhcm4sIGFuZCBSdW4gZm9yIEZyZWUAAAAXCQEAAAAiDB0=";

    private void sendBanner() throws IOException {
        byte[] payload = java.util.Base64.getDecoder().decode(STATIC_BANNER_PAYLOAD_B64);
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(reader.isLargeSdu()));
        out.flush();
    }

    /**
     * A real distributed-database-link connection's native OCI client sends TNS RESET markers
     * during the request loop (not just during login -- see {@code O5LogonHandler}'s own marker
     * handling and its javadoc for the login-time half of this investigation). This method used to
     * unconditionally echo a RESET marker straight back for every marker received, which, tested
     * live against a real Oracle 23c instance, causes an infinite marker ping-pong between this
     * side and the client (confirmed via a live byte capture running into hundreds of thousands of
     * frames in under a minute -- each side kept interpreting the other's echo as a fresh marker to
     * echo back).
     *
     * <p>The real reference client's own marker handler
     * ({@code NetworkChannelImpl.handleMarkerPacket}, case {@code NIQRMARK}/reset) only ever echoes
     * a reset back when {@code acknowledgeReset} was already set -- which only happens when
     * <i>that same side</i> sent the initiating break itself and is now waiting on the peer's
     * acknowledgment. PolyWire never initiates a break, so it should never be the one echoing a
     * reset either; simply consuming and ignoring the marker (matching the "silent skip" behavior
     * confirmed correct at login time) is the correct response here.
     */
    private void handleMarker(TnsPacket packet) throws IOException {
        // Intentionally no response -- see javadoc above.
    }

    private void handleExecute(ExecuteRequest request, TtcWriter w, int callNumber) throws SQLException {
        if (request.sqlText != null) {
            lastSqlText = request.sqlText;
        }
        closeOpenCursor();
        
        if (options.oracleBackendMode() == ServerOptions.OracleBackendMode.NATIVE
                && oracleConnection != null
                && options.dualExecAuthority() == ServerOptions.DualExecAuthority.ORACLE
                && request.sqlText != null
                && request.bindParams.isEmpty()) {
            handleExecuteNative(request, w);
            return;
        }
        
        if (request.sqlText == null && request.cursorId != 0) {
            StatementSignature cached = statementSignatures.get((int) request.cursorId);
            if (cached == null) {
                throw new IllegalStateException(
                        "EXECUTE for cursor_id=" + request.cursorId + " with no prior EXECUTE on this connection to reuse");
            }
            request = new ExecuteRequest(request.cursorId, cached.sql(), request.options, request.numIters,
                    request.bindParams);
        }
        
        int[] bindTypes = null;
        if (request.sqlText != null) {
            bindTypes = new int[request.bindParams.size()];
            for (int i = 0; i < bindTypes.length; i++) {
                bindTypes[i] = request.bindParams.get(i).oraTypeNum;
            }
        }
        boolean dual = oracleConnection != null;
        boolean authoritativeIsOracle = dual
                && options.dualExecAuthority() == ServerOptions.DualExecAuthority.ORACLE;

        boolean shadowEnabled = options.dualExecShadowEnabled();
        if (dual && shadowEnabled) {
            if (authoritativeIsOracle) {
                executeShadow(pgConnection::get, DualTableRewriter.rewrite(request.sqlText), request);
            } else {
                executeShadow(oracleConnection::get, request.sqlText, request);
            }
        }

        String primarySql = authoritativeIsOracle ? request.sqlText : DualTableRewriter.rewrite(request.sqlText);
        Connection primaryConn = authoritativeIsOracle ? oracleConnection.get() : pgConnection.get();

        if (shadowEnabled) {
            for (Connection replica : replicaConnections) {
                executeShadow(() -> replica, primarySql, request);
            }
        }

        BindVariableRewriter.Result rewritten = BindVariableRewriter.rewrite(primarySql);
        List<Object> binds = orderedBindValues(request.bindParams, rewritten.placeholderToBindIndex());
        Statement statement = Statement.of(SourceDialect.ORACLE, rewritten.sql(), binds, accessContext);
        lastRewrittenSqlText = rewritten.sql();
        terminalExecutor.rebind(primaryConn);

        // The client's own EXEC_OPTION_COMMIT bit says exactly what it wants for THIS statement
        // (oracledb sets it when the caller's connection.autocommit is True) -- when nothing else
        // needs primaryConn's explicit-transaction mode (no dual Oracle connection, no shadow
        // replicas, no XA transaction: none of which apply here since dual implies
        // authoritativeIsOracle or a different primaryConn entirely), honor that per-statement
        // instead of always running in manual-commit mode and paying for a separate real Postgres
        // COMMIT round trip afterward via commitAll(). Toggling autoCommit here costs nothing on
        // its own (pgjdbc only talks to the server for it when there's an open transaction to
        // close, and there never is one right before a freshly claimed statement) -- the
        // statement itself now commits as part of its own single round trip, same as pgwire's
        // plain autocommit INSERT. Found live: this was the single largest remaining cost between
        // orawire and pgwire/mywire/mssqlwire's write latency, once the translation-cache and
        // per-call pipeline-construction costs earlier in this investigation were already fixed.
        boolean wantsCommit = (request.options & TtcConstants.EXEC_OPTION_COMMIT) != 0;
        boolean useNativeAutocommit = wantsCommit && !dual && replicaConnections.isEmpty() && xaTransaction == null;
        ExecutionResult result;
        if (useNativeAutocommit) {
            primaryConn.setAutoCommit(true);
            try {
                result = reusablePipeline.execute(statement);
            } finally {
                primaryConn.setAutoCommit(false);
            }
        } else {
            result = reusablePipeline.execute(statement);
        }
        openCursorId = nextCursorId++;
        if (bindTypes != null) {
            statementSignatures.put(openCursorId, new StatementSignature(request.sqlText, bindTypes));
        }

        if (result.isQuery()) {
            openColumns = toColumnMetadata(result.columns());
            openRows = result.rows();
            fetchPosition = 0;
            ResponseWriter.writeDescribeInfo(w, openColumns, usedNativeOciExecuteFallback);

            // Always a plain success end, real row count included, even when this batch happens
            // to exhaust the cursor (fewer rows existed than request.numIters asked for). A real
            // Oracle client correctly infers "no more rows" from getting back fewer rows than it
            // requested -- it doesn't need an inline ORA-01403 on the very call that also handed
            // it real data. This used to call ResponseWriter.writeInlineExhaustionEnd, a
            // hardcoded, captured byte blob with error 1403 baked in and no row-count field at
            // all -- fine for python-oracledb's more forgiving parser, but a real bug against a
            // real ojdbc client: "ORA-01403: no data found" even though the row is right there,
            // confirmed live and reproducible (see docker/tests/java's OraWireTest javadoc for
            // how this was found). Genuine exhaustion -- a FETCH call made after the cursor is
            // already empty -- is unaffected and still correctly signaled via writeErrorEnd
            // below; writeInlineExhaustionEnd itself has been removed as dead code.
            if (usedNativeOciExecuteFallback) {
                writeNativeOciExecuteTail(w);
            } else {
                ResponseWriter.writeSuccessEnd(w, writeRows(w, request.numIters), openCursorId, callNumber);
            }
        } else {

            if (wantsCommit && !useNativeAutocommit) {
                commitAll();
            }
            ResponseWriter.writeSuccessEnd(w, result.updateCount(), openCursorId, callNumber);
        }
    }

    private void handleExecuteNative(ExecuteRequest request, TtcWriter w) throws SQLException {
        if (nativeExecutor == null) {
            nativeExecutor = new com.polygres.wire.orawire.backend.NativeOracleExecutor(
                    options, oracleUsername, oraclePassword);
        }
        com.polygres.wire.orawire.backend.NativeOracleExecutor.NativeQueryResult result =
                nativeExecutor.execute(request.sqlText, (int) request.numIters);
        nativeCursorOpen = result.isQuery() && result.hasMoreRows();
        w.writeRaw(result.ttcPayload());
    }

    private void handleFetchNative(FetchRequest request, TtcWriter w) throws SQLException {
        com.polygres.wire.orawire.backend.NativeOracleExecutor.NativeQueryResult result =
                nativeExecutor.fetchMore((int) request.fetchArraySize);
        nativeCursorOpen = result.hasMoreRows();
        w.writeRaw(result.ttcPayload());
    }

    private void handleReexecute(TtcReader r, TtcWriter w, int callNumber, boolean andFetch) throws SQLException {
        long cursorId = r.readUb4();
        
        long numIters = r.readUb4();
        r.readUb4();
        long options2 = r.readUb4();

        StatementSignature signature = statementSignatures.get((int) cursorId);
        if (signature == null) {
            throw new IllegalStateException(
                    "REEXECUTE for cursor_id=" + cursorId + " with no prior EXECUTE on this connection to reuse");
        }
        lastSqlText = signature.sql();

        List<BindParam> bindParams = signature.bindTypes().length > 0
                ? ExecuteRequestReader.readBindValueRow(r, signature.bindTypes())
                : List.of();

        long syntheticOptions = (andFetch ? TtcConstants.EXEC_OPTION_FETCH : 0)
                | ((options2 & TtcConstants.EXEC_OPTION_COMMIT_REEXECUTE) != 0 ? TtcConstants.EXEC_OPTION_COMMIT : 0);
        ExecuteRequest synthetic = new ExecuteRequest(0, signature.sql(), syntheticOptions,
                andFetch ? numIters : 0, bindParams);
        handleExecute(synthetic, w, callNumber);
    }

    private List<Object> orderedBindValues(List<BindParam> bindParams, int[] placeholderToBindIndex) {
        List<Object> ordered = new ArrayList<>(placeholderToBindIndex.length);
        for (int bindIndex : placeholderToBindIndex) {
            if (bindIndex >= bindParams.size()) {
                throw new IllegalStateException(
                        "SQL references more distinct bind variables than the client sent values for");
            }
            ordered.add(bindParams.get(bindIndex).value);
        }
        return ordered;
    }

    private static List<ColumnMetadata> toColumnMetadata(List<ColumnInfo> columns) {
        List<ColumnMetadata> result = new ArrayList<>(columns.size());
        for (ColumnInfo col : columns) {
            int oraType = switch (col.jdbcType()) {
                case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR -> TtcConstants.ORA_TYPE_NUM_VARCHAR;
                case Types.NUMERIC, Types.DECIMAL, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DOUBLE,
                        Types.FLOAT, Types.REAL ->
                    TtcConstants.ORA_TYPE_NUM_NUMBER;
                case Types.DATE, Types.TIMESTAMP -> TtcConstants.ORA_TYPE_NUM_DATE;
                default -> throw new UnsupportedOperationException(
                        "unsupported Postgres column type (jdbcType=" + col.jdbcType() + ") for column "
                                + col.name() + "; narrow slice supports VARCHAR2/NUMBER/DATE only");
            };
            int precision = oraType == TtcConstants.ORA_TYPE_NUM_NUMBER ? col.precision() : 0;
            int scale = oraType == TtcConstants.ORA_TYPE_NUM_NUMBER ? col.scale() : 0;
            long bufferSize = oraType == TtcConstants.ORA_TYPE_NUM_VARCHAR
                    ? Math.max(1, col.displaySize())
                    : (oraType == TtcConstants.ORA_TYPE_NUM_DATE ? 7 : 22);
            result.add(new ColumnMetadata(col.name(), oraType, precision, scale, bufferSize, col.nullable()));
        }
        return result;
    }

    private interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    private void executeShadow(ConnectionSupplier shadowConnSupplier, String shadowSql, ExecuteRequest request)
            throws SQLException {
        try {
            Connection shadowConn = shadowConnSupplier.get();
            BindVariableRewriter.Result rewritten = BindVariableRewriter.rewrite(shadowSql);
            try (PreparedStatement shadowStmt = shadowConn.prepareStatement(rewritten.sql())) {
                bindParamsDirect(shadowStmt, request.bindParams, rewritten.placeholderToBindIndex());
                if (request.isQuery()) {
                    try (ResultSet rs = shadowStmt.executeQuery()) {
                        while (rs.next()) {
                            
                        }
                    }
                } else {
                    shadowStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            if (options.dualExecRequireBoth()) {
                throw e;
            }
            log.warn("dual-exec shadow backend statement failed (non-authoritative, requireBoth=false): {}",
                    e.getMessage());
        }
    }

    private void bindParamsDirect(PreparedStatement stmt, List<BindParam> bindParams, int[] placeholderToBindIndex)
            throws SQLException {
        for (int i = 0; i < placeholderToBindIndex.length; i++) {
            stmt.setObject(i + 1, bindParams.get(placeholderToBindIndex[i]).value);
        }
    }

    private interface ThrowingAction {
        void run() throws SQLException;
    }

    private void runShadow(ThrowingAction action, String what) throws SQLException {
        try {
            action.run();
        } catch (SQLException e) {
            if (options.dualExecRequireBoth()) {
                throw e;
            }
            log.warn("dual-exec shadow backend {} failed (non-authoritative, requireBoth=false): {}",
                    what, e.getMessage());
        }
    }

    private void handleFetch(FetchRequest request, TtcWriter w, int callNumber) throws SQLException {
        if (nativeCursorOpen) {
            handleFetchNative(request, w);
            return;
        }
        if (openRows == null) {
            throw new IllegalStateException("fetch requested with no open cursor");
        }
        long rowsWritten = writeRows(w, request.fetchArraySize);
        // A real distributed-database-link connection's native OCI client's Execute response uses
        // this codebase's own static, real-capture-derived template (see writeNativeOciExecuteTail)
        // rather than the normal writeSuccessEnd this class uses for every other client -- meaning
        // the "cursor id" that template told the client about is whatever arbitrary value was baked
        // into that OTHER real capture, not this session's own openCursorId (a small internal
        // counter, e.g. 1 or 2, that this client was never actually told). Echoing our own
        // openCursorId back in this Fetch response instead of the cursor id the client itself just
        // sent (readNativeOciFetchRequest already recovered it as request.cursorId) left the client
        // silently stuck rather than erroring -- it seemingly can't match this response to the
        // cursor it's tracking. Echoing its own value back, the same thing real Oracle's server
        // always does, is correct regardless of whether this specific explanation is the full
        // story.
        int cursorIdForResponse = usedNativeOciExecuteFallback ? (int) request.cursorId : openCursorId;
        if (rowsWritten < request.fetchArraySize) {
            ResponseWriter.writeErrorEnd(w, TtcConstants.ERR_NO_DATA_FOUND, "no data found", cursorIdForResponse,
                    callNumber);
        } else {
            ResponseWriter.writeSuccessEnd(w, rowsWritten, cursorIdForResponse, callNumber);
        }
    }

    private long writeRows(TtcWriter w, long maxRows) {
        long count = 0;
        while (count < maxRows && fetchPosition < openRows.size()) {
            List<Object> row = openRows.get(fetchPosition++);
            ResponseWriter.writeRow(w, openColumns, row.toArray());
            count++;
        }
        return count;
    }

    private void closeOpenCursor() {
        openRows = null;
        openColumns = null;
        fetchPosition = 0;
        openCursorId = 0;
        if (nativeCursorOpen && nativeExecutor != null) {
            nativeExecutor.closeCursor();
        }
        nativeCursorOpen = false;
    }

    private void sendData(byte[] payload) throws IOException {
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(reader.isLargeSdu()));
        out.flush();
    }

}
