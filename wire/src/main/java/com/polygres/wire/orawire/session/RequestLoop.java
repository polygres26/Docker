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

    // Counts native-OCI-fallback Execute calls in this session. This client sends TWO Executes for
    // one query (see the piggyback/chained-FUNCTION comments elsewhere in this class): a first,
    // prepare-shaped one carrying no real data (confirmed live: real Oracle's own response to it is
    // DESCRIBE_INFO + the fixed tail only, no row content -- writing rows into it broke the client,
    // which reacted with a TNS BREAK/RESET), and a second, chained one that's the actual
    // remote-fetch call and DOES need real inline row data (see handleExecute's own comment). This
    // distinguishes the two without needing to understand what makes them different at the SQL/request
    // level -- only that the second native-OCI Execute in a session is the one that counts.
    private int nativeOciExecuteCount;

    // Whether this session's first native-OCI query has already fully completed (its Fetch found
    // genuinely nothing left -- see where this is set, right alongside nativeOciExecuteCount's own
    // per-query reset). Real Oracle's FUNC_UNKNOWN_68 response is a completely different, much
    // shorter shape for a query that reuses an already-established dblink connection than for the
    // very first query on it -- confirmed live diffing a real Oracle-to-Oracle self-loop capture of
    // two sequential queries on the same link. nativeOciExecuteCount itself can't distinguish these
    // two cases (it's already back to 1 by the time FUNC_UNKNOWN_68 runs, in *either* case, since it
    // resets per-query) -- this tracks the thing that actually differs: whether this is the first
    // query in the session or a later one.
    private boolean nativeOciFirstQueryComplete;

    // Which real native-OCI client this session is talking to -- set once, the first time
    // skipPiggyback sees the client-banner-request piggyback (see that method's own comment for
    // how the two are told apart there). Needed here too because the two client types differ in
    // WHEN they expect row data: a dblink client always sends a second, chained Execute (via a
    // FUNC_UNKNOWN_68 round trip) before it wants any row content, and its first Execute must stay
    // row-free (confirmed repeatedly, including a live TNS BREAK/RESET when this was tried); a
    // real SQL*Plus client, confirmed live via a real Oracle-to-Oracle capture showing its query's
    // actual result value already embedded in that capture's own single Execute response, never
    // sends that second chained call at all for a simple query -- it expects its one and only
    // Execute to carry the row directly.
    private boolean nativeOciDblinkClient;

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
        if (usedNativeOciExecuteFallback
                && (functionCode == TtcConstants.FUNC_FETCH || functionCode == TtcConstants.FUNC_LOGOFF)) {
            // Real bug, found live: this client's FUNC_LOGOFF carries the exact same kind of real,
            // varying opaque value here that FUNC_FETCH does (confirmed live: 0x5c as the would-be
            // chunk-length byte, again far too large to be a real chunk length, again crashing
            // trying to consume far more bytes than the packet has) -- the same underlying "this
            // field genuinely isn't chunked for this client" issue, not something specific to
            // FETCH. handleLogoff() itself reads nothing further from `r`, so the exact skip width
            // doesn't matter as long as it doesn't crash; reusing FETCH's confirmed-safe 2-byte
            // skip rather than guessing a LOGOFF-specific width that's never actually used.
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
                if (usedNativeOciExecuteFallback) {
                    // Real bytes, captured live from a real Oracle-to-Oracle self-loop's own
                    // response to this same client's LOGOFF: a STATUS message (tag 9), not the
                    // ERROR-tagged (tag 4) writeSuccessEnd this codebase's other clients get --
                    // confirmed live as its own distinct real shape, not just writeSuccessEnd with
                    // different field values.
                    w.writeRaw(NATIVE_OCI_LOGOFF_RESPONSE);
                } else {
                    ResponseWriter.writeSuccessEnd(w, 0, 0, callNumber);
                }
                logoff = true;
            } else if (functionCode == TtcConstants.FUNC_COMMIT) {
                commitAll();
                if (usedNativeOciExecuteFallback && !nativeOciDblinkClient) {
                    // A real SQL*Plus client's own FUNC_COMMIT arrives as the tail of the bundled
                    // close-cursors/set-end-to-end-attr/commit piggyback (see the scan-based
                    // recovery in skipPiggyback above), and a live side-by-side comparison against
                    // a genuine real Oracle-to-Oracle capture of this exact bundle found this
                    // codebase's normal writeSuccessEnd-based COMMIT reply is the wrong shape for
                    // it: real Oracle replies with the same compact STATUS ack (tag 9, zero
                    // retcode, trailing 0x1d) already established and confirmed live for this same
                    // client's own LOGOFF above -- not writeSuccessEnd's own ERROR-tagged (tag 4)
                    // shape. Sending the wrong shape here was confirmed live to be exactly why a
                    // real SQL*Plus client went silent after PolyWire's own COMMIT response instead
                    // of proceeding to its final close marker. A dblink native OCI client's own
                    // standalone COMMIT (never part of this bundle) is unaffected -- it keeps using
                    // the writeSuccessEnd shape below, already confirmed correct for it.
                    w.writeRaw(NATIVE_OCI_LOGOFF_RESPONSE);
                } else {
                    ResponseWriter.writeSuccessEnd(w, 0, openCursorId, callNumber);
                    if (usedNativeOciExecuteFallback) {
                        // Same real trailing byte handleFetch's own comment already documents for
                        // this client (every native-OCI response this investigation has captured
                        // ends in one more byte, 0x1d, after its own real content) --
                        // writeSuccessEnd is generic, hand-written code shared with
                        // JDBC/sqlplus/SQLcl and doesn't append it.
                        w.writeUint8(0x1d);
                    }
                }
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
                // Real bug, found live testing a second query reusing the same dblink connection
                // (a real Oracle client's normal, common usage pattern this series hadn't tried
                // until now): a real Oracle-to-Oracle self-loop capture of two sequential queries
                // on the same link shows this SAME call's real response is a completely different,
                // much shorter shape (22 payload bytes, not 49) the second time -- confirmed live
                // this was the actual cause of the second query hanging, not a coincidence: using
                // the first-query-shaped 49-byte response for it produced the same TNS BREAK/RESET
                // this whole series keeps finding whenever a response's real shape doesn't match
                // what the client expects. Tracked with nativeOciFirstQueryComplete rather than
                // nativeOciExecuteCount (already 1 again by this point in *either* case, since it
                // resets per-query -- see its own reset site) since what actually matters here is
                // "first query in this session" vs "not," not "how many Executes so far."
                byte[] response = java.util.Base64.getDecoder()
                        .decode(nativeOciFirstQueryComplete ? FUNC_UNKNOWN_68_REPEAT_RESPONSE_B64
                                : FUNC_UNKNOWN_68_RESPONSE_B64);
                if (!nativeOciFirstQueryComplete) {
                    response[FUNC_UNKNOWN_68_COLUMN_COUNT_OFFSET] = (byte) openColumns.size();
                }
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
            ExecuteRequest parsed = ExecuteRequestReader.read(r);
            // Real bug, found live against a real dblink client's Execute for a query shaped
            // differently from the "SELECT *" one this fallback was originally found and fixed
            // against ("SELECT id, amount FROM t@link" instead): this client's field layout
            // mismatch doesn't always throw ArrayIndexOutOfBoundsException the way the "SELECT *"
            // shape did -- for this shape, the structured reader ran to completion without any
            // exception at all, just silently landing on sqlPointer=0 ("not a fresh parse, no SQL
            // text follows" -- exactly the misreading readByScanningForSql's own javadoc already
            // warned this client triggers) with cursorId also 0. sqlText==null and cursorId==0
            // together are never legitimate for a real Execute: a genuine re-execute of a cached
            // statement (sqlText==null) always carries the real cursor id of the statement it's
            // reusing, never 0. Confirmed live: without this check, that combination reached
            // handleExecute's dual-table rewriting unguarded and threw a NullPointerException
            // trying to regex-match null SQL text. Retry via the same raw-scanning fallback used
            // for the exception case, against this same packet's own raw bytes (not `r`, whose
            // position reflects the structured reader's own, differently-wrong parse this time).
            if (parsed.sqlText == null && parsed.cursorId == 0) {
                log.info("Execute request parsed without error but produced no SQL text and no cursor "
                        + "id (real dblink native-OCI client, different query shape?) -- falling back "
                        + "to scanning its raw bytes for a SQL statement");
                usedNativeOciExecuteFallback = true;
                return ExecuteRequestReader.readByScanningForSql(packet.payload());
            }
            return parsed;
        } catch (ArrayIndexOutOfBoundsException | UnsupportedOperationException e) {
            // UnsupportedOperationException: real bug, found live -- the SAME real dblink client
            // sending the SAME query ("SELECT *") doesn't always hit the field layout mismatch the
            // same way run to run. Sometimes it's ArrayIndexOutOfBoundsException (this method's
            // original case), sometimes silently-wrong-but-no-exception (the sqlText==null check
            // above), and sometimes the structured reader's own explicit
            // "column defines not supported in narrow slice" guard trips instead (definesPointer
            // read as nonzero from what's actually still this same misaligned field layout, not a
            // real client-sent DEFINE). All three are the identical underlying problem -- this
            // client's Execute request just isn't shaped the way ExecuteRequestReader#read expects
            // -- so all three fall back to the same raw-scanning reader.
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

    // Real bug, found live testing a second query reusing the same dblink connection: relative
    // offset 35-37 of NATIVE_OCI_EXECUTE_TAIL_B64 is real, non-zero content (0x39 0xd2 0x76) that
    // real Oracle sends -- but ONLY in the chained (row-carrying) Execute response, confirmed by
    // diffing a real self-loop capture's two sequential queries' CHAINED responses against each
    // other (identical) as well as against their own PREPARE responses (zero at this same
    // position in both). An earlier version of this fix patched NATIVE_OCI_EXECUTE_TAIL_B64
    // itself, which -- since writeNativeOciExecuteTail (the prepare-shaped response, no rows) uses
    // the same base template -- wrongly leaked this value into the prepare response too, a
    // regression this session caught by re-diffing after the first fix and finding a NEW mismatch
    // introduced at this exact position in query 2's own prepare response. This byte-array copy +
    // patch, applied only inside writeNativeOciExecuteTailWithRows below, keeps the plain prepare
    // tail exactly as it always was.
    private static final byte[] NATIVE_OCI_EXECUTE_TAIL_CHAINED_PATCH = {0x39, (byte) 0xd2, 0x76};
    private static final int NATIVE_OCI_EXECUTE_TAIL_CHAINED_PATCH_OFFSET = 35;

    // Where a real row belongs *inside* NATIVE_OCI_EXECUTE_TAIL_B64, for the second (chained)
    // native-OCI Execute in a session -- see handleExecute's own comment for how this was found.
    // Confirmed live: a real capture's own tail-equivalent bytes match this template exactly for
    // their first 32 bytes, then a real row's own bytes appear, then the template's remaining 188
    // bytes match again (with a handful of session-specific bytes differing throughout, same as
    // this template's own known variance -- see its javadoc above) -- i.e. the row is spliced into
    // the middle of what looked, from the first (row-free) Execute, like one opaque fixed span.
    private static final int NATIVE_OCI_EXECUTE_TAIL_ROW_INSERTION_POINT = 32;

    // A second, separate 50-byte span that belongs between the row-insertion point above and the
    // row data itself -- found the same way, live diffing against a real capture: after inserting
    // rows directly at NATIVE_OCI_EXECUTE_TAIL_ROW_INSERTION_POINT, the real capture's row still
    // started 50 bytes later than this codebase's own output did, with these exact 50 bytes (almost
    // entirely zero, six leading non-zero bytes: a real, per-row descriptor of some kind, plausibly
    // OCI bind/define buffer-size housekeeping -- exact meaning not needed to get past it, same as
    // NATIVE_OCI_ROW_PREFIX) sitting in the real capture right where this codebase's output jumped
    // straight to the row. Only one real row has been captured so far, so -- like the row prefix --
    // this is a fixed, best-effort template rather than a field-by-field understanding.
    //
    // Real bug, found the same way and for the same reason as NATIVE_OCI_EXECUTE_TAIL_B64's own
    // offset-35-37 fix above (see its comment): relative offsets 28-31 and 44-47 here were also
    // left as this template's original zero, and are also real, constant (not query-scoped) values
    // real Oracle always sends -- confirmed identical across two sequential real queries on the
    // same link, diffed against each other. A second query over the same connection needs these to
    // be right; a first query apparently doesn't notice they're wrong.
    private static final byte[] NATIVE_OCI_PRE_ROW_BLOCK = java.util.Base64.getDecoder().decode(
        "BgEaAAIAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAEbH//8AAAAAAAAAAAAAAAAswSTuAAA=");

    private void writeNativeOciExecuteTailWithRows(TtcWriter w, long maxRows) {
        byte[] tail = java.util.Base64.getDecoder().decode(NATIVE_OCI_EXECUTE_TAIL_B64);
        System.arraycopy(NATIVE_OCI_EXECUTE_TAIL_CHAINED_PATCH, 0, tail,
                NATIVE_OCI_EXECUTE_TAIL_CHAINED_PATCH_OFFSET, NATIVE_OCI_EXECUTE_TAIL_CHAINED_PATCH.length);
        w.writeRaw(java.util.Arrays.copyOfRange(tail, 0, NATIVE_OCI_EXECUTE_TAIL_ROW_INSERTION_POINT));
        w.writeRaw(NATIVE_OCI_PRE_ROW_BLOCK);
        writeRows(w, maxRows);
        w.writeRaw(java.util.Arrays.copyOfRange(tail, NATIVE_OCI_EXECUTE_TAIL_ROW_INSERTION_POINT, tail.length));
    }

    // See the FUNC_LOGOFF branch's comment above for where this was captured.
    private static final byte[] NATIVE_OCI_LOGOFF_RESPONSE = java.util.Base64.getDecoder().decode("CQEAAAAAAB0=");

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
        if (usedNativeOciExecuteFallback && nativeOciDblinkClient
                && piggybackFunctionCode == TtcConstants.FUNC_CLOSE_CURSORS) {
            // A real distributed-database-link connection's native OCI client's FUNC_CLOSE_CURSORS
            // piggyback carries a real, varying opaque field right where every other piggyback type
            // (and the generic seq+UB8 preamble below, shared by all of them) safely assumes a
            // chunked UB8 that's always 0 -- confirmed live against a real Oracle 23c instance: two
            // earlier live captures of this exact call both happened to have 0 there, so an earlier
            // version of this method read the shared preamble as usual and then hand-derived a
            // 31-byte skip from that point to reach the real chained FUNCTION tag that follows (see
            // readNativeOciExecuteTail and the FUNC_CLOSE_CURSORS branch elsewhere in this class for
            // the same "chunked field that isn't really chunked for this client" pattern found in
            // FUNC_FETCH). A THIRD live capture (a different real Oracle 23c container, same
            // version) broke that assumption with a genuine nonzero value there -- attempting to
            // chunk-decode it desyncs everything downstream ("expected function-call message, got
            // type 255"). What stayed constant across all three captures, regardless of that field's
            // actual value, is the total distance from right after this piggyback's function code to
            // the real chained FUNCTION tag: 35 bytes. Skipping that fixed distance directly, without
            // trying to semantically parse any of the fields in between, sidesteps the whole fragile
            // chunked-vs-raw ambiguity the same way the DESCRIBE_INFO header/Execute tail templates
            // already do elsewhere in this class. Scoped to the native-OCI Execute fallback
            // specifically -- JDBC/sqlplus/SQLcl's own FUNC_CLOSE_CURSORS piggybacks are handled by
            // the ordinary numCursors-based parse below and have never been observed to need this.
            r.skip(35);
            return;
        }
        r.readUint8();
        r.readUb8();
        if (piggybackFunctionCode == FUNC_CLIENT_BANNER_REQUEST) {
            // A real distributed-database-link connection's native OCI client sends this piggyback
            // (function code 107, undocumented publicly; its payload includes a plain "SQL*Plus" or
            // "oracle" client-program-name string) right after login and, unlike every other
            // piggyback this codebase handles, actually expects a reply -- confirmed live via a
            // real capture against a real Oracle 23c instance: the real server replies with the
            // connection banner text sqlplus prints right after "Connected to:" (a real Oracle
            // server sent "Oracle AI Database 26ai Free Release 23.26.2.0.0 - ..."). Not replying
            // at all left the client waiting forever for this banner instead of proceeding to its
            // real query.
            //
            // The real banner content itself differs by client type -- confirmed live, not
            // guessed: a genuine SQL*Plus client needs the fuller two-line banner (see
            // STATIC_BANNER_PAYLOAD_B64's own javadoc) or it silently aborts with a TNS BREAK/RESET
            // after receiving the shorter one; a dblink native OCI client does the opposite --
            // sending it that same fuller banner is what makes IT abort instead, while the original
            // shorter, one-line banner is what it actually expects. Distinguishing them here by the
            // banner-request piggyback's own payload length (confirmed live: a dblink client's is
            // ~263 bytes, a real SQL*Plus client's is ~86) rather than by client-program-name text,
            // since that name was already discarded upstream by the time this call happens and
            // isn't worth re-plumbing just for this one branch.
            byte[] piggybackPayload = r.readRemaining();
            nativeOciDblinkClient = piggybackPayload.length > 150;
            sendBanner(nativeOciDblinkClient);
        } else if (piggybackFunctionCode == TtcConstants.FUNC_CLOSE_CURSORS
                || piggybackFunctionCode == TtcConstants.FUNC_CANCEL_ALL) {
            // The native-OCI dblink fallback case is handled entirely above, before the generic
            // seq+UB8 preamble. This ordinary numCursors-based parse was written for, and stays
            // correct for, a real SQL*Plus client's own STANDALONE close-cursors piggyback -- but a
            // real SQL*Plus client was confirmed live to also send a genuinely different, bundled
            // shape for this same function code: FUNC_CLOSE_CURSORS immediately followed, in the
            // very same TNS packet, by a FUNC_SET_END_TO_END_ATTR piggyback and then a real
            // FUNC_COMMIT function call -- confirmed via a real capture showing this exact
            // "close-cursors, set-end-to-end-attr, commit" bundle right after a query's first Fetch
            // finds no more rows. Its own internal field layout (a repeating 5-byte
            // call-sequence-looking marker precedes each sub-call in the bundle, not modeled
            // anywhere else in this codebase) wasn't reverse-engineered field-by-field -- instead,
            // scan forward in the raw remaining bytes for the next recognizable message boundary
            // (another piggyback tag, or a real FUNCTION tag paired with a function code this
            // codebase actually knows), the same "don't depend on exact field offsets" approach
            // {@link ExecuteRequestReader#readByScanningForSql} already uses for an analogous
            // real-Oracle-shape problem. Falls back to the plain numCursors-based skip above if no
            // recognizable boundary is found, so a genuinely standalone close-cursors call (the
            // common case this branch was originally written for) is unaffected.
            byte[] rest = r.readRemaining();
            int boundary = findNextMessageBoundary(rest);
            if (boundary >= 0) {
                r.skip(-(rest.length - boundary));
            } else {
                // No recognizable boundary found -- fall back to treating `rest` as the plain
                // numCursors-based body this branch originally expected, on the raw bytes already
                // read out rather than re-reading from `r` (which is now at end-of-buffer).
                TtcReader fallback = new TtcReader(rest);
                fallback.readUint8();
                long numCursors = fallback.readUb4();
                for (long i = 0; i < numCursors; i++) {
                    fallback.readUb4();
                }
            }
        } else if (piggybackFunctionCode == TtcConstants.FUNC_CLIENT_FEATURES) {
            
            r.readUint8();
            long featureBytesLength = r.readUb4();
            r.readUint8();
            r.skip((int) featureBytesLength);
        } else if (piggybackFunctionCode == TtcConstants.FUNC_SET_END_TO_END_ATTR) {
            // Same scan-first safety net as the FUNC_CLOSE_CURSORS branch above, added for the
            // same reason: confirmed live that a real SQL*Plus client can send this piggyback
            // bundled together with others in one packet (immediately after a FUNC_CLOSE_CURSORS
            // piggyback, immediately before a real FUNC_COMMIT call), a context this field-by-field
            // parse below was never verified against. Try the scan first since it's already
            // confirmed correct for that real bundled shape; fall back to the original precise
            // parse (unchanged, still what a standalone occurrence of this piggyback needs) only if
            // scanning finds nothing recognizable.
            byte[] rest = r.readRemaining();
            int boundary = findNextMessageBoundary(rest);
            if (boundary >= 0) {
                r.skip(-(rest.length - boundary));
            } else {
                TtcReader fr = new TtcReader(rest);
                fr.readUint8();
                fr.readUint8();
                fr.readUb4();
                fr.readUint8();
                long clientIdentifierLength = fr.readUb4();
                fr.readUint8();
                long moduleLength = fr.readUb4();
                fr.readUint8();
                long actionLength = fr.readUb4();
                fr.readUint8();
                fr.readUb4();
                fr.readUint8();
                fr.readUb4();
                fr.readUint8();
                long clientInfoLength = fr.readUb4();
                fr.readUint8();
                fr.readUb4();
                fr.readUint8();
                fr.readUb4();
                fr.readUint8();
                long dbopLength = fr.readUb4();

                fr.readRawOrLengthPrefixedBytes((int) clientIdentifierLength);
                fr.readRawOrLengthPrefixedBytes((int) moduleLength);
                fr.readRawOrLengthPrefixedBytes((int) actionLength);
                fr.readRawOrLengthPrefixedBytes((int) clientInfoLength);
                fr.readRawOrLengthPrefixedBytes((int) dbopLength);
            }
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

    // Function codes this codebase can meaningfully resume parsing at, if a scan lands on one --
    // deliberately conservative (only function codes this class has its own real, tested handling
    // for) rather than "any small integer," to keep a false-positive match unlikely. Used only by
    // findNextMessageBoundary's own scan, itself only reached for a piggyback shape whose exact
    // byte layout isn't modeled field-by-field (see that call site's own javadoc).
    // 68 = FUNC_UNKNOWN_68, 118/115 = the AUTH_PHASE_ONE/AUTH_PHASE_TWO function codes (see
    // AuthConstants in the auth package) -- written as literals here rather than referenced,
    // since both are declared later in this file / in another package respectively and this set
    // is built at class-init time before either would be reachable.
    private static final java.util.Set<Integer> KNOWN_RESUMABLE_FUNCTION_CODES = java.util.Set.of(
            TtcConstants.FUNC_EXECUTE, TtcConstants.FUNC_FETCH, TtcConstants.FUNC_COMMIT,
            TtcConstants.FUNC_ROLLBACK, TtcConstants.FUNC_LOGOFF, 68, 118, 115);

    // 107 = the client-banner-request piggyback, 67 = FUNC_SESSION_NLS_SETUP -- literals for the
    // same forward-reference reason as above.
    private static final java.util.Set<Integer> KNOWN_RESUMABLE_PIGGYBACK_CODES = java.util.Set.of(
            107, TtcConstants.FUNC_CLOSE_CURSORS, TtcConstants.FUNC_CANCEL_ALL,
            TtcConstants.FUNC_CLIENT_FEATURES, TtcConstants.FUNC_SET_END_TO_END_ATTR, 67);

    /**
     * Scans {@code data} for the next byte pair that looks like a genuine message boundary this
     * class knows how to resume parsing from: a real FUNCTION tag ({@link TtcConstants#MSG_TYPE_FUNCTION})
     * paired with a function code this class actually has handling for, or a PIGGYBACK tag
     * ({@link TtcConstants#MSG_TYPE_PIGGYBACK}) paired with a piggyback function code this class
     * actually has handling for. Returns the index of the tag byte itself (not the function-code
     * byte after it), or -1 if nothing recognizable is found. See this method's one call site for
     * why this exists instead of a precise field-by-field parse.
     */
    private static int findNextMessageBoundary(byte[] data) {
        for (int i = 0; i < data.length - 1; i++) {
            int tag = data[i] & 0xFF;
            int code = data[i + 1] & 0xFF;
            if (tag == TtcConstants.MSG_TYPE_FUNCTION && KNOWN_RESUMABLE_FUNCTION_CODES.contains(code)) {
                return i;
            }
            if (tag == TtcConstants.MSG_TYPE_PIGGYBACK && KNOWN_RESUMABLE_PIGGYBACK_CODES.contains(code)) {
                return i;
            }
        }
        return -1;
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

    // Real bytes from a real Oracle-to-Oracle self-loop capture's SECOND query on an
    // already-established dblink connection -- see the FUNC_UNKNOWN_68 branch's own comment for
    // why this exists as a separate template rather than reusing FUNC_UNKNOWN_68_RESPONSE_B64 for
    // every query. No column-count-like byte has been confirmed/patched in this one yet (this
    // codebase's own dblink test scenario happens to have 2 columns, matching what's baked into
    // this capture, the same kind of coincidental match FUNC_UNKNOWN_68_RESPONSE_B64's own comment
    // already flags for its column count byte) -- a different column count on a repeated query may
    // need this revisited the same way.
    private static final String FUNC_UNKNOWN_68_REPEAT_RESPONSE_B64 = "CAIAo5rUywcCAAABAAIJAwAAAJgAHQ==";

    // Real bug, found live testing a genuine SQL*Plus client (not just a dblink native OCI
    // connection): this template was missing the banner's second line entirely -- a real Oracle
    // server's banner text is "Oracle AI Database ... Free\nVersion 23.26.2.0.0", not just the
    // first line, confirmed byte-for-byte against a real Oracle 23c self-loop capture that went
    // past login all the way through a real query. A real SQL*Plus client silently aborts with a
    // TNS BREAK/RESET marker pair after receiving the truncated one-line version -- it doesn't
    // error visibly, it just never proceeds to send its own query, which is why this was hard to
    // spot without a capture that went all the way through a successful real query for comparison.
    //
    // Real, confirmed live: a dblink native OCI client does NOT want this fuller banner --
    // sending it this one makes THAT client abort instead (same BREAK/RESET symptom, opposite
    // trigger), while DBLINK_BANNER_PAYLOAD_B64 below (the original one-line template) is what it
    // actually expects. See skipPiggyback's own comment for how the two are told apart at the call
    // site. The whole template (length-prefix bytes included -- real Oracle appears to write the
    // text length as both a leading UB1 and, for this longer text, a duplicated length byte again
    // right before the text itself) is captured verbatim rather than re-derived, same as this
    // file's other static-template constants.
    private static final String SQLPLUS_BANNER_PAYLOAD_B64 =
        "CGcAZ09yYWNsZSBBSSBEYXRhYmFzZSAyNmFpIEZyZWUgUmVsZWFzZSAyMy4yNi4yLjAuMCAtIERldmVsb3AsIExlYXJuLCBhbmQgUnVuIGZvciBGcmVlClZlcnNpb24gMjMuMjYuMi4wLjAAIBoXCQEAAACjAB0=";

    // The original, shorter one-line banner template -- a dblink native OCI client's own expected
    // shape, confirmed to still work for that client after SQLPLUS_BANNER_PAYLOAD_B64 above was
    // found to break it (see that constant's own javadoc).
    private static final String DBLINK_BANNER_PAYLOAD_B64 =
        "CFMAT3JhY2xlIEFJIERhdGFiYXNlIDI2YWkgRnJlZSBSZWxlYXNlIDIzLjI2LjIuMC4wIC0gRGV2ZWxvcCwgTGVhcm4sIGFuZCBSdW4gZm9yIEZyZWUAAAAXCQEAAAAiDB0=";

    private void sendBanner(boolean dblinkClient) throws IOException {
        byte[] payload = java.util.Base64.getDecoder()
                .decode(dblinkClient ? DBLINK_BANNER_PAYLOAD_B64 : SQLPLUS_BANNER_PAYLOAD_B64);
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
            openColumns = toColumnMetadata(result.columns(), usedNativeOciExecuteFallback);
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
                nativeOciExecuteCount++;
                // Real bug, found live diffing this exact response against a real Oracle-to-Oracle
                // self-loop capture: real Oracle's own Execute response for the SECOND (chained --
                // see nativeOciExecuteCount's javadoc) Execute in a session embeds the query's
                // actual row data inline (confirmed live: a real capture's equivalent response has
                // this query's exact encoded values, id=1/amount=99.5, sitting in it), not just the
                // static, row-free NATIVE_OCI_EXECUTE_TAIL_B64 template this call site used to send
                // unconditionally. Two wrong guesses before finding the real shape: (1) rows
                // immediately before the whole static tail -- byte-diffing the result against the
                // real capture showed the real row sits INSIDE the tail's own byte range, not before
                // it; (2) writeSuccessEnd (the compositional, cursorId/rowcount-aware writer that's
                // correct for every OTHER client's row-returning Execute) instead of the tail
                // entirely -- also wrong, real Oracle really does still send this same tail template
                // for this client, just with a row spliced into it (see
                // writeNativeOciExecuteTailWithRows's own javadoc for exactly where and how that was
                // confirmed). The FIRST native-OCI Execute in a session (prepare-shaped, genuinely no
                // row content) keeps using the plain tail -- tried giving it rows/writeSuccessEnd too
                // and both broke it specifically, confirmed live via a TNS BREAK/RESET.
                // A dblink client's first Execute must stay row-free (it always sends a second,
                // chained Execute for the actual rows -- see nativeOciDblinkClient's own javadoc);
                // a real SQL*Plus client, confirmed live, never sends that second call and expects
                // its one and only Execute to carry the row directly.
                if (nativeOciExecuteCount > 1 || !nativeOciDblinkClient) {
                    writeNativeOciExecuteTailWithRows(w, request.numIters);
                } else {
                    writeNativeOciExecuteTail(w);
                }
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

    private static List<ColumnMetadata> toColumnMetadata(List<ColumnInfo> columns, boolean uppercaseNames) {
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
            // Real bug, found live diffing a native-OCI dblink client's DESCRIBE_INFO response
            // against a real Oracle-to-Oracle self-loop capture: real Oracle reports column names
            // in uppercase ("AMOUNT"), matching its own default unquoted-identifier convention --
            // this codebase's own column name comes straight from Postgres's catalog, which folds
            // the other way (lowercase), so it reached the client as "amount" unchanged. Scoped to
            // the native-OCI fallback specifically since it's the only client this has been
            // confirmed to matter for live; JDBC/sqlplus/SQLcl keep getting Postgres's own case
            // exactly as before.
            String name = uppercaseNames ? col.name().toUpperCase(java.util.Locale.ROOT) : col.name();
            result.add(new ColumnMetadata(name, oraType, precision, scale, bufferSize, col.nullable()));
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
        // NOTE: an earlier version of this method special-cased the native-OCI fallback to send a
        // success-end instead of ORA-01403 whenever a partial (but nonzero) row batch came back,
        // on the theory that mixing a real row with an immediate "no data found" in the same
        // response was what left this client silently stuck afterward. That theory didn't survive
        // testing (the same silent stall happened either way) and, more importantly, doesn't match
        // a real Oracle-to-Oracle self-loop capture of this exact call from earlier in this
        // investigation: the real server DID send a combined real-row + ORA-01403 response, and
        // the real client handled it fine, continuing on to send a further request afterward. So
        // this method's original, JDBC/sqlplus/SQLcl-shared rowsWritten<fetchArraySize=>error
        // behavior below is correct as-is and applies unconditionally again -- the actual next gap
        // is figuring out what that further real request is and replying to it, not this.
        if (rowsWritten < request.fetchArraySize) {
            if (usedNativeOciExecuteFallback && rowsWritten == 0) {
                // Real bug, found live diffing this exact response against a real Oracle-to-Oracle
                // self-loop capture (finally reproducible again after this investigation's self-loop
                // environment was fixed): a genuinely-empty Fetch's real response for this client
                // isn't just this codebase's generic writeErrorEnd shape with a fuller message --
                // it's a substantially longer (179-byte), differently-structured message the real
                // capture's own bytes never lined up with field-by-field (the same class of "real
                // Oracle sends more than writeErrorEnd/writeSuccessEnd construct" gap already found
                // for the Execute response -- see writeNativeOciExecuteTailWithRows). No cursor-id
                // echo point was found in it either (unlike Execute's tail, its only 0x07 byte is
                // part of a fixed span, not an echoed value) -- used verbatim rather than patched.
                // Real bug, found live: reusing the SAME dblink connection for a second query in
                // the same sqlplus session sends a differently-shaped exchange than the very first
                // query does (its own chained Execute arrives immediately, without a repeat of the
                // first-query-only prepare/describe/piggyback dance) -- but nativeOciExecuteCount
                // was a monotonic, session-wide counter that never reset, so by the second query it
                // was already >1, and the new query's own FIRST (prepare-shaped, no-data) Execute
                // wrongly got real row data spliced into it -- the exact same "TNS BREAK/RESET"
                // failure mode already confirmed live for a first Execute given rows it shouldn't
                // have. This point -- a genuinely-empty Fetch, the normal signal that a query's own
                // data delivery is fully done -- is the right place to reset it: whatever Execute
                // comes next belongs to a new query, not a continuation of this one.
                nativeOciExecuteCount = 0;
                nativeOciFirstQueryComplete = true;
                w.writeRaw(NATIVE_OCI_EMPTY_FETCH_RESPONSE);
                return;
            }
            // Real bug, found live diffing this exact response against a real Oracle-to-Oracle
            // self-loop capture: real Oracle's own end-of-fetch message text is the full
            // "ORA-01403: no data found\n" -- both the "ORA-01403: " prefix AND a trailing newline
            // this code was missing, not the bare "no data found" it used to send. Confirmed via
            // the length-prefixed message string's own byte length in the capture: 0x19=25, i.e.
            // exactly "ORA-01403: no data found\n".length() (24 without the newline, matching what
            // an intermediate version of this fix that added only the prefix produced and still
            // wasn't enough to un-stick the client -- the newline turned out to matter too).
            // JDBC/sqlplus/SQLcl apparently never cared (they key off the numeric error code, 1403,
            // not this string) enough for the difference to have been noticed before -- but is
            // otherwise a plain, harmless completeness fix worth making unconditionally: this
            // message goes to every client, native-OCI or not, and the fuller, more correct text
            // can only help.
            ResponseWriter.writeErrorEnd(w, TtcConstants.ERR_NO_DATA_FOUND, "ORA-01403: no data found\n",
                    cursorIdForResponse, callNumber);
        } else {
            ResponseWriter.writeSuccessEnd(w, rowsWritten, cursorIdForResponse, callNumber);
        }
        if (usedNativeOciExecuteFallback) {
            // Every real Oracle TTC message this investigation has captured -- this Fetch response,
            // the earlier Execute response, function 68's response, all of them -- ends in one more
            // byte, 0x1d, after its own last real content, confirmed by this codebase's own static
            // NATIVE_OCI_EXECUTE_TAIL_B64 template already carrying it verbatim as its last byte
            // (it's a raw real-capture copy, not hand-written). writeErrorEnd/writeSuccessEnd are
            // hand-written, generic code shared with JDBC/sqlplus/SQLcl and don't append it --
            // apparently harmless for them, but this pickier native-OCI client needs it: without it
            // here, it received an otherwise byte-correct Fetch response and never proceeded.
            w.writeUint8(0x1d);
        }
    }

    // Real bytes, captured live from a real Oracle-to-Oracle self-loop's response to a Fetch that
    // genuinely found zero rows (the normal case once the Execute response above already delivers
    // every row inline) -- see handleFetch's own comment for why this exists instead of
    // writeErrorEnd. Includes its own trailing 0x1d already, unlike writeErrorEnd/writeSuccessEnd.
    private static final byte[] NATIVE_OCI_EMPTY_FETCH_RESPONSE = java.util.Base64.getDecoder().decode(
        "BAMAAAAjAAEBAAAAewUAAAAABwAAAAMAIAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4AAAAAAAA2AQAAAAAAAAAAAAAAAAAAsBRFKrL1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAewUAAAEAAAAAAAAAAwAAAAAAAAAZT1JBLTAxNDAzOiBubyBkYXRhIGZvdW5kCh0=");

    private long writeRows(TtcWriter w, long maxRows) {
        long count = 0;
        while (count < maxRows && fetchPosition < openRows.size()) {
            List<Object> row = openRows.get(fetchPosition++);
            if (usedNativeOciExecuteFallback) {
                ResponseWriter.writeRowNativeOci(w, openColumns, row.toArray());
            } else {
                ResponseWriter.writeRow(w, openColumns, row.toArray());
            }
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
