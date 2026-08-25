package com.polygres.wire.orawire.session;

import com.polygres.wire.config.FailedStatementLog;
import com.polygres.wire.core.ColumnInfo;
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
            messageType = r.readUint8();
        }
        if (messageType != TtcConstants.MSG_TYPE_FUNCTION) {
            throw new IOException("expected function-call message, got type " + messageType);
        }
        int functionCode = r.readUint8();
        int wireSequenceNumber = r.readUint8();
        
        r.readUb8();
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
                handleExecute(ExecuteRequestReader.read(r), w, callNumber);
            } else if (functionCode == TtcConstants.FUNC_FETCH) {
                handleFetch(FetchRequest.read(r), w, callNumber);
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
            int nativeError = SqlStateErrorMapper.toOracleError(e.getSQLState());
            failedStatementLog.record(SourceDialect.ORACLE, lastSqlText,
                    FailedStatementLog.FailureType.BACKEND_ERROR, e.getSQLState(), nativeError, e.getMessage());
            rollbackAfterStatementError();
            ResponseWriter.writeErrorEnd(w, nativeError, e.getMessage() == null ? "backend error" : e.getMessage(), openCursorId, callNumber);
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

    private void skipPiggyback(TtcReader r) {
        int piggybackFunctionCode = r.readUint8();
        r.readUint8();
        r.readUb8();
        if (piggybackFunctionCode == TtcConstants.FUNC_CLOSE_CURSORS
                || piggybackFunctionCode == TtcConstants.FUNC_CANCEL_ALL) {
            
            r.readUint8();
            long numCursors = r.readUb4();
            for (long i = 0; i < numCursors; i++) {
                r.readUb4();
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
            throw new UnsupportedOperationException(
                    "unsupported piggyback function code: " + piggybackFunctionCode);
        }
    }

    private void handleMarker(TnsPacket packet) throws IOException {
        byte[] resetMarker = { 1, 0, TNS_MARKER_TYPE_RESET };
        TnsPacket response = new TnsPacket(TnsPacketType.MARKER, 0, resetMarker);
        out.write(response.encode(reader.isLargeSdu()));
        TnsPacket drainTerminator = new TnsPacket(TnsPacketType.DATA, 0, new byte[0]);
        out.write(drainTerminator.encode(reader.isLargeSdu()));
        out.flush();
    }

    private static final byte TNS_MARKER_TYPE_RESET = 2;

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
            ResponseWriter.writeDescribeInfo(w, openColumns);

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
            ResponseWriter.writeSuccessEnd(w, writeRows(w, request.numIters), openCursorId, callNumber);
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
        if (rowsWritten < request.fetchArraySize) {
            ResponseWriter.writeErrorEnd(w, TtcConstants.ERR_NO_DATA_FOUND, "no data found", openCursorId, callNumber);
        } else {
            ResponseWriter.writeSuccessEnd(w, rowsWritten, openCursorId, callNumber);
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
