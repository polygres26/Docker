package com.polygres.wire.orawire.session;

import com.polygres.wire.core.ColumnInfo;
import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
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

/**
 * Decodes TTC DATA packets carrying OEXEC/OFETCH calls, runs the (SQL
 * translated) statement against Postgres via JDBC, and writes the response
 * back in Oracle's TTC framing. Implements the narrow slice from
 * reference/ttc_execute_fetch_spec.md: one active cursor per session,
 * VARCHAR2/NUMBER/DATE columns and bind variables only.
 *
 * <p>The authoritative statement runs through the shared
 * {@link StatementPipeline} (firewall/router/stats/QoS — same one the
 * Postgres-wire and native gRPC frontends use), same as the other two
 * frontends. Since the pipeline's {@link ExecutionResult} fully
 * materializes rows rather than exposing a live JDBC cursor, OFETCH is
 * served by paginating {@link #openRows} in memory (see {@link #writeRows})
 * instead of holding a {@code ResultSet} open across EXECUTE/FETCH calls.
 *
 * <p>The dual-execution shadow-backend path ({@link #executeShadow}) is
 * <em>not</em> migrated onto the pipeline: it's a distinct concept
 * (replication/validation against a second backend chosen per statement)
 * that the pipeline doesn't model yet — ARCHITECTURE.md §6 Phase 2 folds
 * this into a first-class Replication/XA pipeline stage. Until then it
 * stays a direct JDBC call, unchanged from before this migration.
 *
 * <p>When {@code oracleConnection} is non-null (dual execution/replication
 * testing mode), every EXECUTE/COMMIT/ROLLBACK also runs against a real
 * Oracle backend alongside Postgres. One backend is "authoritative" (its
 * result is what's actually returned to the client, per
 * {@link ServerOptions#dualExecAuthority()}); the other is the "shadow"
 * backend, run for validation/replication only. {@link
 * ServerOptions#dualExecRequireBoth()} controls whether a shadow-backend
 * failure is tolerated (logged only) or surfaced to the client as a
 * failure of the whole statement.
 *
 * <p>By default there's still no real distributed-transaction coordination
 * between the two backends at COMMIT/ROLLBACK time — a COMMIT succeeding on
 * one and failing on the other can't be undone. {@code
 * ORAPG_DUAL_EXEC_XA_ENABLED} (surfaced here as a non-null
 * {@code xaTransaction}) replaces that with a real XA two-phase commit via
 * {@link XaCoordinatorStub} — see its javadoc for exactly what
 * "real" does and doesn't cover (no crash-recovery log).
 */
public final class RequestLoop {

    private static final Logger log = LoggerFactory.getLogger(RequestLoop.class);

    private final TnsPacketReader reader;
    private final OutputStream out;
    private final com.polygres.wire.core.LazyPooledConnection pgConnection;
    private final com.polygres.wire.core.LazyPooledConnection oracleConnection; // null unless the legacy 2-way dual-exec path is enabled for this session
    private final List<Connection> replicaConnections; // empty unless POLYWIRE_REPLICATION_BACKENDS is configured (generalized N-way path)
    private final XaTransaction xaTransaction; // non-null only when running under XA/2PC (either replication path)
    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final String oracleUsername; // only used by ORAPG_ORACLE_BACKEND_MODE=native, see handleExecuteNative
    private final String oraclePassword;
    private com.polygres.wire.orawire.backend.NativeOracleExecutor nativeExecutor; // lazily opened, one dedicated connection per session
    private boolean nativeCursorOpen; // true only between a native handleExecute leaving more rows and the FETCH that exhausts them

    // Rows are fully materialized at EXECUTE time (see class javadoc) and paginated from here.
    private List<List<Object>> openRows;
    private List<ColumnMetadata> openColumns;
    private int fetchPosition;
    private int openCursorId = 0;
    private int nextCursorId = 1;
    // Every terminator (success or error) carries a "callNumber" field that must match the
    // driver's own internal per-connection RPC counter (T4CTTIfun.receive()'s code=4 case
    // compares it and silently misroutes to handleOutOfSequenceError()/processError() on any
    // mismatch, corrupting the parse of whatever follows — see ResponseWriter.writeErrorEnd's
    // javadoc). RESOLVED: previously tracked independently here (a counter starting at 2,
    // incrementing once per top-level function-call message) — replaced by simply echoing the
    // driver's own wireSequenceNumber from each call's header (see handleData), since an
    // independent counter silently drifts from the driver's real one the moment a piggyback
    // (which consumes a call slot on the driver's side but was never counted here) appears
    // ahead of a call — see handleData's callNumber assignment for the live repro.
    // Per-cursor EXECUTE signature (original Oracle-dialect SQL text + per-position bind types),
    // keyed by the cursor_id the client itself assigns — needed so a later REEXECUTE/
    // REEXECUTE_AND_FETCH (see handleReexecute), which carries neither the SQL text nor bind-type
    // metadata, only a cursor_id and fresh bind VALUES, can look up the right statement to
    // re-run. Previously a single "last executed" pair of fields rather than a map — correct only
    // when the client's REEXECUTE calls are never interleaved with a DIFFERENT statement's
    // EXECUTE in between. Found live: python-oracledb caches statement handles PER SQL TEXT, so a
    // "SELECT COUNT(*) ...", "INSERT ...", "SELECT COUNT(*) ..." sequence (the common
    // read-write-read pattern, e.g. this project's own soe_client.py test) reuses the SELECT's
    // original cursor_id for its second call via REEXECUTE_AND_FETCH — by which point a
    // single-slot "last executed" would already hold the INSERT's 3-bind-position signature
    // instead of the SELECT's 0-bind one, corrupting {@link ExecuteRequestReader#readBindValueRow}
    // (an ArrayIndexOutOfBoundsException from trying to read bind values the wire never sent).
    // Not evicted on cursor close ({@link #closeOpenCursor}) — a real client can REEXECUTE a
    // cursor whose FETCH-side state (openRows/openColumns) has already been closed out from
    // under it (this is normal: each result set is read to exhaustion via OFETCH well before the
    // next EXECUTE/REEXECUTE arrives), so the signature must outlive that. Bounded by session
    // lifetime and cursor_id churn, not unbounded.
    private final Map<Integer, StatementSignature> statementSignatures = new HashMap<>();

    private record StatementSignature(String sql, int[] bindTypes) {
    }

    public RequestLoop(TnsPacketReader reader, OutputStream out, com.polygres.wire.core.LazyPooledConnection pgConnection,
            com.polygres.wire.core.LazyPooledConnection oracleConnection, List<Connection> replicaConnections,
            XaTransaction xaTransaction,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(reader, out, pgConnection, oracleConnection, replicaConnections, xaTransaction, options, sharedStages,
                backendRegistry, null, null);
    }

    // ORAPG_ORACLE_BACKEND_MODE=native needs a username/password to open its own dedicated
    // connection through NativeByteCaptureProxy (see handleExecuteNative) — the existing
    // constructor above is kept, delegating with nulls, so every JDBC-mode call site (which never
    // needs these) is unaffected.
    public RequestLoop(TnsPacketReader reader, OutputStream out, com.polygres.wire.core.LazyPooledConnection pgConnection,
            com.polygres.wire.core.LazyPooledConnection oracleConnection, List<Connection> replicaConnections,
            XaTransaction xaTransaction,
            ServerOptions options, List<PipelineStage> sharedStages, com.polygres.wire.core.BackendRegistry backendRegistry,
            String oracleUsername, String oraclePassword) {
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
    }

    public void run() throws IOException {
        try {
            while (true) {
                TnsPacket packet = reader.readPacket();
                switch (packet.type()) {
                    case DATA -> {
                        // The real Java thin driver can send a zero-payload DATA packet ahead of a
                        // real message elsewhere in the handshake (see O5LogonHandler's/
                        // ProtocolNegotiation's readNonEmptyPacket/readDataTypesRequest javadocs for
                        // the same pattern found live) — found here too, live, once a session
                        // actually reached RequestLoop for the first time this session: an empty
                        // packet right after a successful O5LOGON crashed handleData's very first
                        // readUint8() with an ArrayIndexOutOfBoundsException instead of being skipped
                        // like every other stage of the handshake already does.
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
            // Only ever non-null under ORAPG_ORACLE_BACKEND_MODE=native — see handleExecuteNative.
            if (nativeExecutor != null) {
                nativeExecutor.close();
            }
        }
    }

    /** Returns true if the session should end after this message (LOGOFF). */
    private boolean handleData(TnsPacket packet) throws IOException {
        TtcReader r = new TtcReader(packet.payload());
        int messageType = r.readUint8();
        while (messageType == TtcConstants.MSG_TYPE_PIGGYBACK) {
            // KNOWN GAP, found live but NOT fixed: ojdbc11 (confirmed via SQLcl) sends a
            // piggyback function code 120 immediately post-O5LOGON that this server doesn't
            // recognize — not in reference/ttc_execute_fetch_spec.md's python-oracledb-derived
            // list, and not traceable to a named T4CTTIfun subclass via javap against
            // ojdbc11.jar either. Captured live via a raw relay directly against real Oracle
            // (bypassing this server entirely): its payload embeds a COMPLETE SQL statement
            // (an NLS session-parameter bootstrap query, "select parameter,value from
            // nls_session_parameters union ..."), meaning this isn't a fire-and-forget
            // notification like CLOSE_CURSORS/CLIENT_FEATURES — the client waits for a real
            // response to it. Tried silently discarding the rest of the packet on an
            // unrecognized piggyback as a "safer than crashing" fallback; that was wrong —
            // it turned a clean, immediate connection close (this method's original behavior)
            // into a silent indefinite hang instead, strictly worse for a real client to
            // recover from. Reverted: an unread/misunderstood piggyback here still means the
            // rest of this packet cannot be safely parsed at all, so failing loudly and closing
            // the connection remains the right behavior until function code 120 gets a real
            // implementation (not just a skip) — it needs to actually run the embedded query
            // and answer it, the same as a normal EXECUTE.
            skipPiggyback(r);
            messageType = r.readUint8();
        }
        if (messageType != TtcConstants.MSG_TYPE_FUNCTION) {
            throw new IOException("expected function-call message, got type " + messageType);
        }
        int functionCode = r.readUint8();
        int wireSequenceNumber = r.readUint8();
        // token_num (UB8) — every function-call header (Message._write_function_code in
        // python-oracledb's own source) writes this extra trailing field once
        // ttc_field_version >= 23.1_ext1, which this server's real PROTOCOL capability
        // array (see ProtocolNegotiation) now clears. Read with readUb8() (universal
        // variable-length decode), not a fixed single-byte skip: this is the same field
        // O5LogonHandler's "unidentified leading byte" (see readUsernameAndSkipPairs)
        // turned out to be, which only happened to work there because token_num was 0
        // (a single 0x00 byte) in every capture seen so far — using the real variable
        // decode here is correct for any value, not just the empirically-observed one.
        r.readUb8();

        // RESOLVED (was a server-side independent counter, callCounter++, for every call except
        // FETCH which already special-cased reusing wireSequenceNumber): found live that a
        // second query on the same connection got T4CTTIfun.handleOutOfSequenceError even
        // though callCounter itself was incrementing correctly — because the piggyback ahead of
        // that second EXECUTE (a CLOSE_CURSORS the client sends for the prior statement's
        // cursor) consumes a call slot on the DRIVER's own internal counter that callCounter
        // never accounted for (skipPiggyback() deliberately doesn't advance it — see that
        // method's javadoc), so the two counters silently drift apart from the second call with
        // a piggyback ahead of it onward. The driver already tells the server exactly what value
        // it expects back, in wireSequenceNumber, right here in this same call's own header —
        // FETCH's existing special case was really just the general rule found once in a
        // narrower context; simplified to always echo the driver's own value instead of tracking
        // a second, independently-drifting counter.
        int callNumber = wireSequenceNumber;

        TtcWriter w = new TtcWriter();
        boolean logoff = false;
        try {
            if (functionCode == TtcConstants.FUNC_EXECUTE) {
                handleExecute(ExecuteRequestReader.read(r), w, callNumber);
            } else if (functionCode == TtcConstants.FUNC_FETCH) {
                handleFetch(FetchRequest.read(r), w, callNumber);
            } else if (functionCode == TtcConstants.FUNC_REEXECUTE
                    || functionCode == TtcConstants.FUNC_REEXECUTE_AND_FETCH) {
                handleReexecute(r, w, callNumber, functionCode == TtcConstants.FUNC_REEXECUTE_AND_FETCH);
            } else if (functionCode == TtcConstants.FUNC_LOGOFF) {
                // LogoffMessage sends no payload beyond the function-code header
                // (messages/base.pyx:735-736, no _write_message override) and
                // expects the generic response framing — a plain success
                // terminator is enough (messages/logoff.pyx has no custom
                // process() override either).
                handleLogoff();
                ResponseWriter.writeSuccessEnd(w, 0, 0, callNumber);
                logoff = true;
            } else if (functionCode == TtcConstants.FUNC_COMMIT) {
                // CommitMessage/RollbackMessage send no payload beyond the
                // function-code header either (messages/commit.pyx,
                // rollback.pyx — no _write_message override, same pattern
                // as LOGOFF); plain success terminator suffices.
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
        } catch (SQLException e) {
            log.warn("backend error executing statement: {}", e.getMessage());
            rollbackAfterStatementError();
            ResponseWriter.writeErrorEnd(w, 942, e.getMessage() == null ? "backend error" : e.getMessage(), openCursorId, callNumber);
        } catch (RuntimeException e) {
            // Belt-and-suspenders: an unexpected RuntimeException here (e.g. an unsupported
            // column type) used to propagate uncaught out of the request loop, silently dropping
            // the connection with no response and no logged cause — found live while debugging a
            // routed-execution response gap. A real backend/logic error should surface as a TTC
            // error the client can see, same as a checked SQLException does above.
            log.warn("unexpected error executing statement: {}", e.toString(), e);
            rollbackAfterStatementError();
            ResponseWriter.writeErrorEnd(w, 942, e.getMessage() == null ? e.toString() : e.getMessage(), openCursorId, callNumber);
        }
        sendData(w.toByteArray());
        return logoff;
    }

    private void handleLogoff() throws SQLException {
        closeOpenCursor();
    }

    /**
     * Real Oracle tolerates a failed statement mid-transaction — later statements in the same
     * session keep working. Postgres does not: once any statement inside a transaction block
     * errors, every subsequent statement is rejected with "current transaction is aborted,
     * commands ignored until end of transaction block" until an explicit ROLLBACK. Found live
     * running SQLcl (unlike sqlplus, SQLcl connects and runs real queries over ojdbc — see
     * ARCHITECTURE.md §5.5g for why real OCI clients still can't get this far) against a fresh
     * connection: several of SQLcl's own startup introspection statements reference
     * Oracle-only constructs ({@code dbms_metadata}, etc.) that don't translate to Postgres and
     * fail as expected — but without this rollback, that single expected failure silently broke
     * every real query afterward in the same session, including ones with nothing wrong with
     * them. A full ROLLBACK (not a per-statement SAVEPOINT) is the deliberate choice here: it's
     * the simple, safe fix that unblocks the session; it does mean any of *this* session's own
     * uncommitted work earlier in the same transaction is lost along with the failed statement,
     * not just the failed statement itself — a real gap from Oracle's actual per-statement
     * semantics, left as a known follow-up (SAVEPOINT-per-statement) rather than adding that
     * complexity blind.
     */
    private void rollbackAfterStatementError() {
        try {
            pgConnection.rollback();
        } catch (SQLException rollbackFailure) {
            log.warn("rollback after statement error also failed: {}", rollbackFailure.getMessage());
        }
        // Best-effort only here, unlike runShadow's normal dualExecRequireBoth()-gated throw —
        // we're already inside error recovery for the primary statement failure; a shadow
        // connection also failing to roll back shouldn't block writing the primary error response.
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

    /**
     * Commits the primary (pg) connection plus any dual-exec/replica shadow connections, or
     * drives the real 2PC path when XA is active. Used both by the explicit FUNC_COMMIT RPC and
     * by handleExecute's EXEC_OPTION_COMMIT handling (see that field's javadoc) — factored out
     * because both need the exact same "xaTransaction, else pg+oracle+replicas" fan-out.
     */
    private void commitAll() throws SQLException {
        if (xaTransaction != null) {
            xaTransaction.commit(); // real 2PC — see XaTransaction javadoc
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

    /**
     * Piggybacks are prepended to the same DATA packet as a real function
     * call (messages/base.pyx:749-778, _write_piggybacks) — e.g. the client
     * bundles a CLOSE_CURSORS piggyback ahead of LOGOFF to tell the server
     * which cached cursor IDs it can release. We don't cache statements
     * across executes (each EXECUTE closes the prior cursor already), so
     * there's nothing to act on — just consume the piggyback's own bytes so
     * the reader lands correctly on the real function-code header that
     * follows. Format confirmed via a live capture of a real client's
     * connection-close sequence.
     */
    private void skipPiggyback(TtcReader r) {
        int piggybackFunctionCode = r.readUint8();
        r.readUint8(); // sequence number
        // token_num (UB8) — python-oracledb's _write_piggyback_code (messages/base.pyx) has
        // the exact same ttc_field_version >= 23.1_ext_1-gated trailing field as
        // _write_function_code (see handleData's matching read for the main function
        // header's javadoc) — a separate call site, easy to miss fixing once and not the
        // other. Confirmed live: a piggybacked CLOSE_CURSORS ahead of a real EXECUTE
        // (python closing a cursor left over from a previous error) corrupted every field
        // read after it, eventually desyncing the connection entirely.
        r.readUb8();
        if (piggybackFunctionCode == TtcConstants.FUNC_CLOSE_CURSORS
                || piggybackFunctionCode == TtcConstants.FUNC_CANCEL_ALL) {
            // See FUNC_CANCEL_ALL's javadoc: OCANA (120) and CLOSE_CURSORS (105) are the same
            // wire shape (confirmed via javap against ojdbc11.jar's T4C8Oclose, which marshals
            // both from the same pointer+count+cursorIds code path) — found live stacked
            // together (OCANA closing 1 cursor, then CLOSE_CURSORS closing 2 more) ahead of an
            // ordinary EXECUTE, immediately post-O5LOGON.
            r.readUint8(); // pointer
            long numCursors = r.readUb4();
            for (long i = 0; i < numCursors; i++) {
                r.readUb4(); // cursor id
            }
        } else if (piggybackFunctionCode == TtcConstants.FUNC_CLIENT_FEATURES) {
            // ojdbc11-specific: a real client sends this (piggybacked ahead of LOGOFF, found
            // live) to advertise its own feature-flag string, e.g. "512,0,0,0". Ignored
            // content-wise (not applicable to this narrow slice) but must still be consumed:
            // an unread piggyback here desyncs the LOGOFF call right behind it exactly like an
            // unread CLOSE_CURSORS payload would, closing the connection with no response at
            // all instead of the clean logoff the client is waiting for. Structure reverse
            // engineered byte-by-byte from a live capture (not found documented anywhere in
            // this repo's specs): pointer(uint8), then a ub4-length-prefixed byte count (9 for
            // "512,0,0,0"), then one further byte whose role wasn't identified (always 0 in
            // every capture seen — possibly a chunking/continuation flag, not needed to just
            // skip past it), then that many raw feature-string bytes with no additional
            // length prefix of their own.
            r.readUint8(); // pointer
            long featureBytesLength = r.readUb4();
            r.readUint8(); // unidentified single byte, always 0 so far
            r.skip((int) featureBytesLength);
        } else if (piggybackFunctionCode == TtcConstants.FUNC_SET_END_TO_END_ATTR) {
            // MODULE/ACTION/CLIENT_IDENTIFIER/CLIENT_INFO/DBOP session-tagging attributes — see
            // TtcConstants.FUNC_SET_END_TO_END_ATTR's javadoc for why this exists and what broke
            // without it. Structure confirmed against python-oracledb's own source
            // (_write_end_to_end_piggyback, messages/base.pyx) rather than inferred from a raw
            // capture alone, since this shape is easy to get subtly wrong (five conditional
            // header/length pairs, only some of which are ever followed by string bytes) — see
            // FUNC_CLIENT_FEATURES above for what a misread here does to everything after it.
            // Every attribute is still discarded, not captured (not applicable to this narrow
            // slice — same scope cut as FUNC_CLIENT_FEATURES): only consumed so the reader lands
            // correctly on the real function-code header that follows. A length field is 0
            // whenever that attribute wasn't sent (whether because the client never modified it,
            // or modified it to null) — the real driver's own write-side logic guarantees that,
            // so the string-skip step below only needs the five length values, never the flags
            // word or a separate "was this one sent" check.
            r.readUint8(); // pointer (cidnam)
            r.readUint8(); // pointer (cidser)
            r.readUb4();   // flags (which attributes were modified) — unused, see above
            r.readUint8(); // pointer (client_identifier)
            long clientIdentifierLength = r.readUb4();
            r.readUint8(); // pointer (module)
            long moduleLength = r.readUb4();
            r.readUint8(); // pointer (action)
            long actionLength = r.readUb4();
            r.readUint8(); // pointer (cideci — always unset, no real client populates this)
            r.readUb4();   // length (cideci)
            r.readUint8(); // cidcct — always unset
            r.readUb4();   // cidecs — always unset
            r.readUint8(); // pointer (client_info)
            long clientInfoLength = r.readUb4();
            r.readUint8(); // pointer (cidkstk — always unset)
            r.readUb4();   // length (cidkstk)
            r.readUint8(); // pointer (cidktgt — always unset)
            r.readUb4();   // length (cidktgt)
            r.readUint8(); // pointer (dbop)
            long dbopLength = r.readUb4();
            // String bytes follow in this exact order — NOT a plain skip of the header's own
            // length: found live (mismatched by exactly 1 byte per string, desyncing everything
            // after) that each present string is preceded by a redundant inner length byte
            // matching the header length already read, the same "python-oracledb sometimes
            // repeats the length right before the raw bytes" shape TtcReader.readRawOrLengthPrefixedBytes
            // already exists to handle (confirmed against a live capture: module="billing-app"
            // arrived as 0x0b ("11", the redundant byte) followed by the 11 raw bytes, not 11
            // raw bytes alone).
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

    /**
     * The client sends a MARKER packet when it hits an unexpected condition
     * elsewhere (BREAK, then RESET) and needs to recover the connection state
     * before it can surface whatever error it caught to its own caller.
     * python-oracledb's Protocol._reset() (impl/thin/protocol.pyx) sends its
     * own RESET marker and then BLOCKS reading packets from us until it sees
     * a MARKER packet back whose payload's marker_type byte is
     * TNS_MARKER_TYPE_RESET (2) — confirmed live: leaving this a no-op left
     * the client hung forever in that wait, masking whatever real error
     * triggered the break in the first place (in every case seen so far, a
     * bug in this server's own response bytes elsewhere — the marker
     * exchange itself isn't the root cause, just what was blocking the real
     * error from reaching the surface). Echo a RESET marker back so the
     * client can complete its own cleanup and re-raise its real exception.
     */
    private void handleMarker(TnsPacket packet) throws IOException {
        byte[] resetMarker = { 1, 0, TNS_MARKER_TYPE_RESET };
        TnsPacket response = new TnsPacket(TnsPacketType.MARKER, 0, resetMarker);
        out.write(response.encode(reader.isLargeSdu()));
        // Protocol._reset()'s drain loop, after seeing our RESET marker, keeps calling
        // wait_for_packets_sync() as long as the current packet type is still MARKER — it
        // needs one more non-marker packet to terminate (real Oracle apparently follows its
        // own reset marker with an error/status unit describing the interrupted operation;
        // _reset() discards whatever it reads there without inspecting it, since the caller
        // re-raises its own already-caught exception regardless — so the payload's content
        // doesn't matter, only that some non-MARKER packet follows). An empty DATA packet
        // satisfies that unconditionally — confirmed live for python-oracledb.
        //
        // RULED OUT (tried live, this session, real OCI/sqlplus at the reader.isAnoEligible()
        // tier — the first real client ever to reach RequestLoop and hit this path): sending a
        // real TTC ERROR unit here instead of an empty DATA packet, on the theory real OCI's
        // reset-wait logic inspects the drain packet's content unlike python-oracledb's. Made no
        // observable difference — the client still hung indefinitely afterward with zero error
        // output even after 75+ seconds (not even a delayed re-raise). Reverted rather than carry
        // unverified complexity; the actual root cause of the hang is still open — see
        // ARCHITECTURE.md's O5LOGON rich-tier section.
        TnsPacket drainTerminator = new TnsPacket(TnsPacketType.DATA, 0, new byte[0]);
        out.write(drainTerminator.encode(reader.isLargeSdu()));
        out.flush();
    }

    private static final byte TNS_MARKER_TYPE_RESET = 2;

    private void handleExecute(ExecuteRequest request, TtcWriter w, int callNumber) throws SQLException {
        closeOpenCursor();
        // ORAPG_ORACLE_BACKEND_MODE=native: relay the real backend's raw response bytes instead
        // of reconstructing DESCRIBE_INFO/inline-exhaustion from JDBC metadata — see
        // ServerOptions.OracleBackendMode's javadoc and NativeOracleExecutor for why. Only
        // engages when Oracle is genuinely the authoritative backend (oracleConnection != null,
        // dualExecAuthority == ORACLE) — the Postgres/dual-exec-shadow paths are unaffected.
        // bindParams.isEmpty() gate found live: sqlcl's own internal session-setup queries use
        // real bind variables (e.g. ":OPTION") — NativeOracleExecutor runs raw SQL text through a
        // plain Statement, not a PreparedStatement, so a bound query sent through native mode
        // reached the backend as literal unbound placeholder text and failed with ORA-01008.
        // Falls through to the existing (unaffected, already-correct) JDBC path for these.
        if (options.oracleBackendMode() == ServerOptions.OracleBackendMode.NATIVE
                && oracleConnection != null
                && options.dualExecAuthority() == ServerOptions.DualExecAuthority.ORACLE
                && request.sqlText != null
                && request.bindParams.isEmpty()) {
            handleExecuteNative(request, w);
            return;
        }
        // Found live, this session: a cursorId != 0 EXECUTE with sqlText == null is NOT a narrow-
        // slice-unreached path — it's exactly what python-oracledb sends to re-execute a
        // non-query (INSERT/UPDATE/DELETE) statement whose shape hasn't changed, as a *plain*
        // FUNC_EXECUTE (not FUNC_REEXECUTE/REEXECUTE_AND_FETCH — that pair is apparently reserved
        // for queries). Confirmed via a live capture: the exact "insert, select_agg, insert"
        // sequence any read-write-read workload produces sends this shape for the second insert.
        // Previously left request.sqlText null all the way through to
        // DualTableRewriter.rewrite(null), an uncaught NullPointerException — now resolved the
        // same way handleReexecute resolves an explicit REEXECUTE: look up the cursor's earlier
        // signature by its real cursorId and substitute both the SQL text and bind types.
        if (request.sqlText == null && request.cursorId != 0) {
            StatementSignature cached = statementSignatures.get((int) request.cursorId);
            if (cached == null) {
                throw new IllegalStateException(
                        "EXECUTE for cursor_id=" + request.cursorId + " with no prior EXECUTE on this connection to reuse");
            }
            request = new ExecuteRequest(request.cursorId, cached.sql(), request.options, request.numIters,
                    request.bindParams);
        }
        // Remembered for a possible later REEXECUTE/REEXECUTE_AND_FETCH (or another sqlText-null
        // EXECUTE, see above) on this cursor — see handleReexecute and the statementSignatures
        // field's javadoc. Deferred: this EXECUTE's own cursor_id (openCursorId) isn't assigned
        // until after the pipeline runs below, so the actual map insert happens further down,
        // right after that assignment; only the bind-type extraction happens here.
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

        // Run the non-authoritative ("shadow") backend first and fully to
        // completion before touching the authoritative one, so a
        // requireBoth failure never leaves the authoritative side holding
        // an open cursor no one will finish reading — see class javadoc.
        // DualTableRewriter only applies on whichever branch actually targets
        // Postgres — Oracle understands "FROM DUAL" natively and must not have
        // it rewritten out from under it.
        // ORAPG_DUAL_EXEC_SHADOW_ENABLED=false lets a pure single-backend homogeneous session
        // (e.g. an Oracle client -> polywire -> real Oracle only, no Postgres involved at all —
        // see ServerOptions.dualExecShadowEnabled's javadoc) skip every shadow/replica execution
        // below, so the lazily-opened pgConnection this session would otherwise borrow is never
        // actually connected to and no live Postgres backend is required at runtime.
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

        // Generalized N-way replication (POLYWIRE_REPLICATION_BACKENDS): every configured replica gets the
        // same already-translated-to-Postgres SQL as the primary — replicas are assumed to share the
        // primary's dialect (Postgres), unlike the legacy 2-way path which translates specifically for
        // Oracle. A replica pointed at an Oracle backend would need its own per-branch translation, not
        // implemented here — a documented narrow-slice limit of the generalized path.
        if (shadowEnabled) {
            for (Connection replica : replicaConnections) {
                executeShadow(() -> replica, primarySql, request);
            }
        }

        BindVariableRewriter.Result rewritten = BindVariableRewriter.rewrite(primarySql);
        List<Object> binds = orderedBindValues(request.bindParams, rewritten.placeholderToBindIndex());
        Statement statement = Statement.of(SourceDialect.ORACLE, rewritten.sql(), binds);
        StatementPipeline pipeline = new StatementPipeline(sharedStages,
                new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(primaryConn)));
        ExecutionResult result = pipeline.execute(statement);
        openCursorId = nextCursorId++;
        if (bindTypes != null) {
            statementSignatures.put(openCursorId, new StatementSignature(request.sqlText, bindTypes));
        }

        if (result.isQuery()) {
            openColumns = toColumnMetadata(result.columns());
            openRows = result.rows();
            fetchPosition = 0;
            ResponseWriter.writeDescribeInfo(w, openColumns);
            // Inline prefetch is capped at the client's requested num_iters
            // (al8i4 "prefetch number of rows" field, spec §2.3) — writing
            // more rows than the client expects here desyncs its parser,
            // which responds by sending a break/reset MARKER and aborting;
            // confirmed via a live capture against a 14-row table (a 2-row
            // table never exposed this because it happened to equal the
            // driver's num_iters=2 default). Remaining rows are fetched via
            // separate OFETCH calls, already handled by handleFetch.
            // RESOLVED: when this inline prefetch exhausts the cursor in the SAME response as
            // EXECUTE's own row data (every available row fit within the client's requested
            // prefetch/array size), a real Oracle server does NOT send this branch's usual plain
            // success terminator — confirmed via a raw TCP relay directly against a real Oracle
            // Database 26ai Free instance (relay.py, real ojdbc11/SQLcl client, no packet-capture
            // tooling needed): even with a deliberately small requested array size, the real
            // server returned every row of a small result set PLUS an inline ORA-01403
            // "exhausted" signal in ONE response, via a message type this codebase hadn't
            // implemented (TNS_MSG_TYPE_PARAMETER = 8) — see writeInlineExhaustionEnd's javadoc.
            // A previous attempt at this fix sidestepped the unknown message type entirely by
            // deliberately holding back the last available row so a genuine follow-up OFETCH
            // would always be required (discovering exhaustion through the already-correct
            // msg-type-4 path in handleFetch below) — reverted: real Oracle never actually
            // produces that "partial, not-yet-exhausted, more via FETCH" shape for a case like
            // this, and forcing it manufactured a response shape ojdbc11 apparently doesn't
            // handle either, once decoding this real capture proved that theory wrong. Only the
            // fully-exhausted case is handled here — a genuinely partial inline prefetch (real
            // rows remain beyond request.numIters, e.g. a large result set) is unaffected and
            // keeps using the plain success terminator, unchanged from before.
            long totalAvailable = openRows.size();
            long rowsWritten = writeRows(w, request.numIters);
            if (rowsWritten == totalAvailable && rowsWritten <= request.numIters) {
                // Full formatted error text, not just "no data found" — confirmed live (see
                // ResponseWriter.INLINE_EXHAUSTION_PREFIX's javadoc) a real server sends the
                // complete "ORA-01403: ..." string with trailing newline here, not a bare message.
                ResponseWriter.writeInlineExhaustionEnd(w, openCursorId, callNumber, "ORA-01403: no data found\n");
            } else {
                ResponseWriter.writeSuccessEnd(w, rowsWritten, openCursorId, callNumber);
            }
        } else {
            // EXEC_OPTION_COMMIT ("commit on success", spec's al8i4 options bitmask) is how a
            // client in autocommit mode tells the server to commit THIS statement itself,
            // instead of sending a separate explicit FUNC_COMMIT RPC — that's the only commit
            // path python-oracledb's own test scripts ever exercised (calling conn.commit()
            // explicitly after every write), which is exactly why this went unnoticed until a
            // JDBC client with autocommit=true (ojdbc11's own default, and the common case) hit
            // it: this bit was already being read into ExecuteRequest.options and even had a
            // named constant, but nothing ever checked it, so no JDBC autocommit write was ever
            // actually committed to the backend — confirmed live: a fresh connection couldn't
            // see a row a first connection had just "successfully" inserted (getUpdateCount()
            // even reported 0 rather than 1, a second, already-documented, still-open bug — but
            // the far more serious defect was silent data loss underneath that wrong count).
            if ((request.options & TtcConstants.EXEC_OPTION_COMMIT) != 0) {
                commitAll();
            }
            ResponseWriter.writeSuccessEnd(w, result.updateCount(), openCursorId, callNumber);
        }
    }

    /**
     * ORAPG_ORACLE_BACKEND_MODE=native's execute path — see {@link ServerOptions.OracleBackendMode}
     * and {@link com.polygres.wire.orawire.backend.NativeOracleExecutor}'s javadoc for the full
     * rationale (JDBC's {@code ResultSetMetaData} never exposes several backend-computed TTC
     * fields a real OALL8 response includes, found live to be the actual root cause behind a
     * second-query-on-the-same-connection failure ResponseWriter's reconstruction couldn't fix no
     * matter how many real captures it was patched against). Relays the real backend's own
     * DESCRIBE_INFO/ROW_DATA/terminator bytes verbatim instead of rebuilding them.
     *
     * <p>Narrow slice, matching {@link com.polygres.wire.orawire.backend.NativeOracleExecutor}'s own
     * scope: no bind variables (checked by the caller before this is reached — falls through to
     * the JDBC path otherwise), no REEXECUTE, no cursor bookkeeping for a later CLOSE_CURSORS —
     * the relayed bytes carry the real backend's own cursor numbering, which this narrow slice
     * doesn't track. FETCH continuation IS supported (see {@link #handleFetchNative}) — added
     * after sqlcl's own internal setup queries (more rows than fit inline) hit "fetch requested
     * with no open cursor" the first time this mode was tried against a real client beyond
     * JDBC/python-oracledb.
     */
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

    /** See {@link #handleExecuteNative}'s javadoc — FETCH continuation for a native-mode cursor. */
    private void handleFetchNative(FetchRequest request, TtcWriter w) throws SQLException {
        com.polygres.wire.orawire.backend.NativeOracleExecutor.NativeQueryResult result =
                nativeExecutor.fetchMore((int) request.fetchArraySize);
        nativeCursorOpen = result.hasMoreRows();
        w.writeRaw(result.ttcPayload());
    }

    /**
     * TNS_FUNC_REEXECUTE / TNS_FUNC_REEXECUTE_AND_FETCH (python-oracledb's own statement-caching
     * fast path, execute.pyx's {@code _write_reexecute_message}): sent instead of a full EXECUTE
     * once a cursor has already been parsed and nothing about its shape has changed — the common
     * case for any loop that repeats {@code cur.execute()} with the same SQL text on a fresh
     * cursor. Carries only the cursor id, iteration/options words, and fresh bind VALUES — no SQL
     * text and no per-bind type metadata, both looked up from {@link #statementSignatures} by the
     * cursor_id this call itself carries — see that field's javadoc for why a single "last
     * executed" slot (this method's previous implementation) breaks on the common
     * read-write-read interleaving pattern.
     *
     * <p>Wire format (confirmed against python-oracledb's own source, not independently
     * captured): ub4 cursor_id, ub4 num_iters, ub4 options_1, ub4 options_2, then num_iters
     * ROW_DATA-tagged rows of bind values (see {@link ExecuteRequestReader#readBindValueRow}) —
     * only present at all if the statement takes bind variables. options_2 bit
     * {@link TtcConstants#EXEC_OPTION_COMMIT_REEXECUTE} means autocommit is on, same role as
     * {@link TtcConstants#EXEC_OPTION_COMMIT} on a full EXECUTE's single options word (see that
     * constant's use in handleExecute above) but a different bit in a different field. This
     * narrow slice doesn't support true array execution (num_iters > 1 with per-row distinct
     * binds, same limit {@link ExecuteRequestReader#readBindParams} already documents for a full
     * EXECUTE) — only the LAST row's bind values are kept and used.
     */
    private void handleReexecute(TtcReader r, TtcWriter w, int callNumber, boolean andFetch) throws SQLException {
        long cursorId = r.readUb4();
        // NOT the bind-row count (found live via a wire capture: a real python-oracledb client
        // sent num_iters=2 here — its cursor's prefetchrows — alongside exactly ONE ROW_DATA
        // row of bind values). This is the SAME "how many rows to inline-fetch" cap
        // handleExecute's own numIters parameter already means; the bind-row count is a
        // separate concept (python's self.num_execs, for true array execution — narrow slice
        // doesn't support that, same limit ExecuteRequestReader.readBindParams documents for a
        // full EXECUTE, so exactly one row is read below regardless of num_iters' value).
        long numIters = r.readUb4();
        r.readUb4(); // options_1 — carries no information handleExecute needs beyond andFetch (already known)
        long options2 = r.readUb4();

        StatementSignature signature = statementSignatures.get((int) cursorId);
        if (signature == null) {
            throw new IllegalStateException(
                    "REEXECUTE for cursor_id=" + cursorId + " with no prior EXECUTE on this connection to reuse");
        }

        List<BindParam> bindParams = signature.bindTypes().length > 0
                ? ExecuteRequestReader.readBindValueRow(r, signature.bindTypes())
                : List.of();

        long syntheticOptions = (andFetch ? TtcConstants.EXEC_OPTION_FETCH : 0)
                | ((options2 & TtcConstants.EXEC_OPTION_COMMIT_REEXECUTE) != 0 ? TtcConstants.EXEC_OPTION_COMMIT : 0);
        ExecuteRequest synthetic = new ExecuteRequest(0, signature.sql(), syntheticOptions,
                andFetch ? numIters : 0, bindParams);
        handleExecute(synthetic, w, callNumber);
    }

    /** placeholderToBindIndex[i] gives the wire bind-value index for the i-th "?" in the rewritten SQL. */
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

    /**
     * Runs {@code request} against the non-authoritative dual-exec backend,
     * for validation/replication only — its rows (if any) are drained and
     * discarded, never sent to the client. A failure is either swallowed
     * (logged) or rethrown depending on {@link ServerOptions#dualExecRequireBoth()}.
     * Direct JDBC, not the shared pipeline — see class javadoc.
     *
     * <p>{@code shadowConnSupplier}, not an already-open {@link Connection}:
     * with pooled shadow connections ({@link com.polygres.wire.core.LazyPooledConnection}),
     * acquiring the connection itself can fail (pool exhausted/timed out) exactly
     * like running a statement on it can — that failure needs the same
     * requireBoth-tolerant handling, not a bare exception straight to the client.
     */
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
                            // fully drain so runtime data errors, not just parse/plan
                            // errors, are caught here too
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
