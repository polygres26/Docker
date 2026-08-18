package com.polygres.wire.orawire.ttc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

/**
 * Writes the server->client response units for OEXEC/OFETCH: describe-info
 * (query only, first execute), row data, and the terminating status/error
 * unit. Spec §3/§4.2.
 *
 * KNOWN GAP: only the fields the client actually reads are written (see
 * spec §3.1/§3.3 "(skipped)" markers) — this server writes zero/empty
 * placeholders for those, which is safe because the reference driver never
 * reads them back, but has not been cross-checked against a real Oracle
 * server's byte-for-byte output.
 */
public final class ResponseWriter {

    // RESOLVED: these three were the actual cause of a fast ORA-17401 "[16, 0]" (msg-type-16,
    // i.e. this very message) protocol violation once the inline-exhaustion trailer fix above
    // stopped masking it behind a hang — found via the same same-session 3-capture diff technique
    // (SAL, EMPNO, SAL again against a real backend proxied through orawire's own working relay).
    // All three fields were previously written as empty/zero on the theory that the client
    // ignores them (matching the "(skipped)" spec markers elsewhere in this file) — wrong here:
    //   - the "skipped blob" is a real, non-empty, LENGTH-load-bearing field: a real server always
    //     sends 23 bytes (confirmed: identical content for two calls of the *same* SQL text,
    //     different content for a different SQL text, with what looks like a trailing
    //     OracleDateCodec-shaped timestamp — almost certainly an internal SQL-signature/cache-key
    //     hash this codebase has no way to reproduce). Writing 0 bytes instead of 23 shifts every
    //     field after it, corrupting the whole rest of the message — the actual root cause, not
    //     just a cosmetic gap. Content is very likely unchecked by the client (same "opaque,
    //     replay-verbatim" situation as O5LOGON_TERMINATOR_TAIL/INLINE_EXHAUSTION_* below), so a
    //     fixed-length filler of the right size is used rather than trying to reproduce the hash.
    //   - "max row size" is a real constant 22 in both captures (single-NUMBER-column queries),
    //     not skipped/zero.
    //   - the "unlabeled skipped byte" is a real constant 130 (0x82) in both captures, not 0.
    private static final byte[] DESCRIBE_INFO_BLOB_FILLER = new byte[23];
    private static final int DESCRIBE_INFO_MAX_ROW_SIZE = 22;
    private static final int DESCRIBE_INFO_TRAILING_BYTE = 130;

    public static void writeDescribeInfo(TtcWriter w, List<ColumnMetadata> columns) {
        w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
        w.writeBytesWithLength(DESCRIBE_INFO_BLOB_FILLER);
        w.writeUb4(DESCRIBE_INFO_MAX_ROW_SIZE);
        w.writeUb4(columns.size());
        if (!columns.isEmpty()) {
            w.writeUint8(DESCRIBE_INFO_TRAILING_BYTE);
        }
        for (int i = 0; i < columns.size(); i++) {
            writeColumnMetadata(w, columns.get(i), i);
        }
        // RESOLVED: NOT actually a "(skipped)" field, despite the spec's own marker — confirmed
        // live (same real-Oracle byte diff as the per-column fixes above) that a real server
        // always sends a genuine 7-byte OracleDateCodec-encoded current-date value here, never an
        // empty placeholder. python-oracledb's own client tolerates an empty one (never reads it
        // back — matches the spec's claim), but ojdbc11 (and by extension SQLcl, which is what
        // actually caught this) unmarshals it for real in T4CTTIdcb.receiveCommon and threw an
        // ArrayIndexOutOfBoundsException in T4CMAREngineNIO.buffer2Value on the empty bytes this
        // server used to send. Only the current wall-clock time is meaningfully "real" here — the
        // client has no way to validate it against anything, so any correctly-encoded date works.
        byte[] currentDate = OracleDateCodec.encode(java.time.LocalDateTime.now());
        w.writeUb4(currentDate.length);
        w.writeBytesWithLength(currentDate);
        w.writeUb4(0); // dcbflag
        w.writeUb4(0); // dcbmdbz
        w.writeUb4(0); // dcbmnpr
        w.writeUb4(0); // dcbmxpr
        w.writeBytesWithLength(new byte[0]); // dcbqcky
    }

    private static void writeColumnMetadata(TtcWriter w, ColumnMetadata col, int columnIndex) {
        w.writeUint8(col.oraTypeNum);
        w.writeUint8(0); // flags (skipped)
        // RESOLVED (was the "NUMBER-scale DESCRIBE gap", commit da2888e): precision and scale
        // are BOTH plain fixed single raw bytes, confirmed live and unchanged from the very first
        // version of this method — they were never actually the problem. Root-caused instead via
        // a live byte-for-byte diff against a real Oracle Database 23ai Free instance
        // (python-oracledb's PYO_DEBUG_PACKETS env var, no packet-capture tooling/root needed):
        // describing three adjacent NUMBER columns (DEPTNO NUMBER(2,0), SAL NUMBER(7,2),
        // EMPNO NUMBER(4,0)) side by side and diffing index-by-index against this server's own
        // output found two real, unrelated bugs further down this same method (see "max_size" and
        // "column position" below) that together made every column's metadata a variable number
        // of bytes longer than a real server's, depending on that column's own field values —
        // exactly the kind of bug that silently hangs a real client's DESCRIBE_INFO parse (SQLcl
        // confirmed live) instead of producing a visible error, since nothing here fails loudly.
        // RESOLVED: wire scale is NOT the same number JDBC's ResultSetMetaData.getScale() gives —
        // confirmed via the same real-backend diff as the class-level DESCRIBE_INFO note above:
        // NUMBER(7,2) SAL (JDBC scale=2) sent wire scale=1, NUMBER(4,0) EMPNO (JDBC scale=0) sent
        // wire scale=0 unchanged. Both data points fit a simple "biased by one once scale is
        // actually nonzero" rule; only two columns' worth of real evidence exist so this is a
        // best-effort fit, not a derived formula — flagged here rather than silently assumed
        // general. bufferSize below follows the same two real data points: real Oracle sent 278
        // for the has-decimals column and 22 (this codebase's pre-existing fixed NUMBER buffer
        // size) for the no-decimals one — so the fixed 22 default only needs overriding for the
        // decimal case.
        int wireScale = col.scale > 0 ? col.scale - 1 : col.scale;
        w.writeSb1(col.precision);
        w.writeSb1(wireScale);
        long wireBufferSize = col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER && col.scale > 0
                ? 278 : col.bufferSize;
        w.writeUb4(wireBufferSize);
        w.writeUb4(0); // max array elements
        w.writeUb8(0); // cont flags
        w.writeBytesWithLength(null); // oid, empty for scalar types
        w.writeUb2(0); // version
        w.writeUb2(0); // character set id
        // csfrm (character-set form) is SQLCS_IMPLICIT (1) for character types only — confirmed 0
        // for a real Oracle NUMBER column via the same live byte diff below; a fixed 1 here was
        // never actually exercised for a non-VARCHAR column before this fix (every DESCRIBE this
        // codebase's own tests happened to cover was character data), so it went unnoticed.
        w.writeUint8(col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR ? 1 : 0);
        // RESOLVED, same live diff as the class-level "NUMBER-scale DESCRIBE gap" note above:
        // this was previously ALSO writing col.bufferSize here (duplicating the real bufferSize
        // field above), which — because bufferSize is itself variable-length-encoded and 22
        // (this codebase's fixed NUMBER buffer size) needs 2 wire bytes, not 1 — made every
        // NUMBER/DATE column's metadata exactly 1 byte longer than a real Oracle server's, with
        // nothing to reveal the mismatch until the very next column's (or the trailer's) fields
        // silently misparsed. A real server sends 0 here — matches the "(skipped)" fields
        // immediately around it, not another copy of the real max_size.
        w.writeUb4(0); // max_size (skipped by client, NOT a duplicate of bufferSize above)
        w.writeUb4(0); // oaccolid (12.2+)
        w.writeUint8(col.nullsAllowed ? 1 : 0);
        // v7 length of name: a real server writes the column name's actual byte length as a
        // fixed raw byte here (confirmed live: 0x03 for "SAL") rather than an unconditional 0 —
        // still purely skipped/unread by every driver this codebase talks to (byte count is
        // identical either way, only the value changes), kept for fidelity with the real capture.
        w.writeUint8(col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        w.writeStrWithTwoLengths(col.name);
        w.writeStrWithTwoLengths(null); // schema
        w.writeStrWithTwoLengths(null); // object-type name
        // RESOLVED, same live diff as the class-level note above: this was previously an
        // unconditional 0, which only happens to be byte-count-correct for a query's FIRST column
        // (0-based position 0 encodes as a single 0x00 byte, same as our old hardcoded value) —
        // every column after the first has a genuinely nonzero 0-based position, which the
        // variable-length ub2 encoding this field actually uses needs 2 wire bytes for, not 1.
        // This was a second, independent source of the same "too few bytes written" failure mode
        // as the max_size bug below, affecting every multi-column DESCRIBE regardless of column
        // type — it happened to go unnoticed until now because the only multi-column queries this
        // codebase had verified live so far all still worked well enough for their own narrower
        // checks (or masked it against other coincidentally-compensating byte differences); a
        // dedicated column-by-column live diff is what actually exposed it.
        w.writeUb2(columnIndex);
        w.writeUb4(0); // uds_flags
        // Fields below are all unconditional now that ProtocolNegotiation advertises a real
        // server's field_version (27 — see PROTOCOL_RESPONSE_B64's javadoc), which clears
        // every one of these thresholds (23.1 / 23.1_ext_3 / 23.4). Confirmed live: without
        // them, python-oracledb's _process_metadata (messages/base.pyx) read straight into
        // the NEXT column's or the describe-info trailer's bytes expecting these fields,
        // producing DPY-5002 ("read integer of length N when expecting...").
        w.writeStrWithTwoLengths(null); // domain schema (>= 23.1)
        w.writeStrWithTwoLengths(null); // domain name (>= 23.1)
        w.writeUb4(0); // num_annotations (>= 23.1_ext_3) — 0 skips the whole inner block
        w.writeUb4(0); // vector dimensions (>= 23.4)
        w.writeUint8(0); // vector format (>= 23.4)
        w.writeUint8(0); // vector flags (>= 23.4)
    }

    public static void writeRow(TtcWriter w, List<ColumnMetadata> columns, Object[] values) {
        w.writeUint8(TtcConstants.MSG_TYPE_ROW_DATA);
        for (int i = 0; i < columns.size(); i++) {
            writeColumnValue(w, columns.get(i), values[i]);
        }
    }

    private static void writeColumnValue(TtcWriter w, ColumnMetadata col, Object value) {
        if (value == null) {
            w.writeUint8(0);
            return;
        }
        switch (col.oraTypeNum) {
            case TtcConstants.ORA_TYPE_NUM_VARCHAR -> w.writeStrWithLength(value.toString());
            case TtcConstants.ORA_TYPE_NUM_NUMBER -> {
                BigDecimal bd = value instanceof BigDecimal b ? b : new BigDecimal(value.toString());
                w.writeBytesWithLength(OracleNumberCodec.encode(bd));
            }
            case TtcConstants.ORA_TYPE_NUM_DATE -> {
                // rs.getObject() (unlike the narrower rs.getTimestamp()) returns
                // java.sql.Date for a Postgres DATE column and java.sql.Timestamp
                // for TIMESTAMP — both need handling here now that column values
                // come from the generic JdbcBackendExecutor (core.JdbcBackendExecutor)
                // shared across all three frontends, not a per-column-typed getter.
                LocalDateTime dt;
                if (value instanceof LocalDateTime d) {
                    dt = d;
                } else if (value instanceof java.sql.Timestamp ts) {
                    dt = ts.toLocalDateTime();
                } else if (value instanceof java.sql.Date d) {
                    dt = d.toLocalDate().atStartOfDay();
                } else {
                    throw new IllegalArgumentException("unsupported DATE value type: " + value.getClass());
                }
                w.writeBytesWithLength(OracleDateCodec.encode(dt));
            }
            default -> throw new UnsupportedOperationException("unsupported column type: " + col.oraTypeNum);
        }
    }

    /**
     * Writes a success terminator: error unit with error_num == 0. Spec §3.3.
     *
     * cursorId MUST be nonzero for a freshly-executed cursor: the client
     * only adopts cursor_id from this field when it's nonzero
     * (messages/base.pyx:1222-1223, `_process_error_info`) — sending 0 here
     * (as an earlier version of this method always did) leaves the client's
     * statement._cursor_id stuck at 0, which makes it refuse to even send a
     * later FETCH request (fetch.pyx raises ERR_CURSOR_HAS_BEEN_CLOSED
     * client-side without talking to the server at all). Confirmed via a
     * live capture: a real client's `for row in cursor` loop failed exactly
     * this way past the first EXECUTE's inline-prefetched rows.
     */
    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId) {
        writeSuccessEnd(w, rowcount, cursorId, 0);
    }

    /**
     * O5LOGON-specific success terminator. NOT derived field-by-field from
     * decompiled semantics — that was tried first (javap -c against
     * T4CTTIoer11.unmarshalAttributes()), fixed a real, confirmed structural
     * bug (this server's old shared writeSuccessEnd wrote 3 fields, plus a
     * trailing error_num/rowcount pair, that the real reader never consumes
     * at this position), and even after that fix, a live capture of a real
     * Oracle Database 23 Free instance's own successful O5LOGON phase-one
     * response (proxied through a byte-logging TCP relay, real ojdbc11
     * client, tcpdump-free) showed 5 more trailing zero bytes past where
     * the decompiled unmarshalAttributes() should have returned (KPDKV
     * count == 0) — meaning some caller-side or capability-gated read
     * (e.g. T4CTTIfun.processEOCS()'s conditional eocs field, gated on a
     * server compile-time capability bit this server's replayed capability
     * table may or may not advertise) accounts for bytes not visible in
     * that one method's bytecode alone, and chasing it further by static
     * analysis wasn't converging.
     *
     * So: this replays the real capture's exact terminator tail verbatim
     * (msgtype byte through the trailing padding), the same technique
     * already used for the DATA_TYPES response. The only field patched at
     * runtime is callNumber, at its confirmed byte offset (27, a
     * fixed-width UB1 — see FunctionCall/expectFunction's javadoc): the
     * real capture's own value there was 1, matching this server's own
     * call1.sequenceNumber() for the same first real call, so patching it
     * per-call rather than hardcoding is the conservative choice.
     */
    private static final byte[] O5LOGON_TERMINATOR_TAIL = HexFormat.of().parseHex(
            "0401010204df000000000000000000000000000000000000000100000000000000000000");
    // Tried live, this session: re-diffing this 36-byte tail against phase-two's own independently
    // captured terminator (PHASE_TWO_RESPONSE_B64) shows the byte at index 25 also varies between
    // the two real captures (0x01 here vs 0x02 there) — exactly "call #1" vs "call #2", which looked
    // like the signature of a real per-call callNumber field, suggesting 25 (not 27) was the field
    // this method should patch. Switching the patch to index 25 broke python-oracledb (hung
    // post-auth) — reverted back to 27, the original, live-verified-safe offset. Whatever's really
    // at index 25 is either not a client-checked callNumber after all, or matters in a way this
    // one-line move didn't correctly account for. Left at 27; ojdbc11's ORA-17401 remains open.
    private static final int O5LOGON_TERMINATOR_CALLNUMBER_OFFSET = 27;

    /**
     * REVERTED (root cause finally found and fixed): this used to append two extra zero bytes
     * here, reasoned as harmless >=14-gated oertyp2/oerchksm padding (see T4CTTIoer's
     * unmarshalAttributes() javap output). That reasoning was wrong in a way that took the
     * whole session to track down: "harmless if unread" is false for this driver. Confirmed via
     * a FRESH live capture against a real Oracle Database 23 Free instance (proxied through a
     * byte-logging TCP relay, same real ojdbc11 client, same negotiated ttc_field_version=27 as
     * this server advertises) that the real phase-one terminator is exactly 36 bytes —
     * O5LOGON_TERMINATOR_TAIL's own length, with NO trailing pair of zero bytes. This driver's
     * NIOPacket.readHeader() (javap -c against ojdbc11.jar) reuses a session-scoped read buffer
     * across receive() calls: if bytes remain unconsumed in it (buffer.remaining() > 0 after
     * the previous message) AND there happen to be at least 8 of them, it reads the NEXT
     * logical message's header directly out of that stale buffer content instead of performing
     * a fresh socket read — confirmed live via oracle.jdbc.diagnostic.enableLogging=true: the
     * driver's very next receive() call (for the phase-two response) logged no
     * readFromSocket/readPacketFromSocketChannel at all, immediately throwing "Received
     * unexpected TTC message code 0" — i.e. it read a stale zero byte left over from exactly
     * these two extra bytes, without ever looking at the actual phase-two response we'd sent.
     * Appending unread trailing bytes here doesn't get ignored — it corrupts the parse of
     * whatever this driver reads next on the same connection. Removed.
     */
    public static void writeO5LogonSuccessEnd(TtcWriter w, int cursorId, int callNumber) {
        byte[] tail = O5LOGON_TERMINATOR_TAIL.clone();
        tail[O5LOGON_TERMINATOR_CALLNUMBER_OFFSET] = (byte) callNumber;
        w.writeRaw(tail);
    }

    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0); // call_status
        w.writeUb2(0); // end-to-end seq#
        // KNOWN GAP, deliberately left at 0 (NOT the real DML row count): ojdbc11's
        // PreparedStatement.getUpdateCount() ultimately reads T4C8Oall.rowsProcessed, set from
        // T4CTTIoer11.getCurRowNumber() (javap -c against ojdbc11.jar) — this field, not the
        // later ub8 "rowcount" field python-oracledb's _process_error_info reads instead. Tried
        // writing the real rowcount here live: it DID show up correctly on the wire (verified
        // byte-for-byte) and fixed a single-threaded repro's update count, but caused a genuine
        // ORA-17401 protocol violation under broader/concurrent load-test traffic — some other
        // field's presence or interpretation is evidently gated on this value being nonzero in a
        // way not yet understood. Reverted rather than risk regressing the core connect/query/
        // fetch/logoff flow this server already gets right; the actual DML write always executes
        // correctly against the backend (confirmed directly), only the JDBC-visible update COUNT
        // is wrong. Left as a known, documented, non-blocking gap for a future session.
        //
        // Tried again in a later session, narrower this time: passing the real row count here
        // ONLY for a query's own (non-DML) success terminator, when it returns fewer rows than
        // the client's prefetch/array size without the cursor being exhausted — the exact shape
        // of a real, live SQLcl/ojdbc11 hang (identical "stuck reading, no error, nothing further
        // sent" signature as several other bugs fixed in the same investigation). Confirmed via
        // live testing this did NOT fix that hang either, so the field isn't what ojdbc11 needs
        // for this specific case — reverted again rather than carry the DML-load-test regression
        // risk above for no benefit. The real cause of the non-exhausted-partial-fetch hang is
        // still open.
        w.writeUb4(0); // current row number
        w.writeUb2(0); // legacy error number
        w.writeUb2(0); // array elem error
        w.writeUb2(0); // array elem error
        w.writeUb2(cursorId);
        w.writeSb1(0); // error_pos (sb2 in spec; sb1 sufficient here since always 0)
        w.writeUint8(0); // sql type
        w.writeUint8(0); // fatal?
        w.writeUint8(0); // flags
        w.writeUint8(0); // user cursor options
        w.writeUint8(0); // UPI parameter
        w.writeUint8(0); // flags2
        writeZeroRowid(w);
        w.writeUb4(0); // OS error
        w.writeUint8(0); // statement number
        w.writeUint8(callNumber); // call number — see this overload's javadoc
        w.writeUb2(0); // padding
        w.writeUb4(0); // success iters — NOT the DML row-count field, see "current row number" above
        w.writeBytesWithLength(null); // oerrdd
        w.writeUb2(0); // num batch error codes
        w.writeUb4(0); // num batch error offsets
        w.writeUb2(0); // num batch error messages
        w.writeUb4(0); // error_num == 0 => success
        w.writeUb8(rowcount);
        // Unconditional now that ProtocolNegotiation advertises field_version 27 (see its
        // PROTOCOL_RESPONSE_B64 javadoc), which clears the >= 20.1 gate
        // (messages/base.pyx:238-240) — confirmed live against python-oracledb's own source;
        // an earlier version of this method omitted these because the server used to pin
        // field_version at plain 12.2, where the client never read them at all.
        w.writeUb4(0); // sql type
        w.writeUb4(0); // server checksum
    }

    // RESOLVED (was "STILL NOT SUFFICIENT" below — root cause found): the old PREFIX/MIDDLE split
    // put the dynamic byte in the wrong place and the message text was wrong, both confirmed via a
    // proper 3-capture diff this time (not 2) — same connection, three sequential exhausting
    // SELECTs against a real Oracle backend proxied through orawire's own working relay
    // (TeeProxy on 127.0.0.1:1522, no packet-capture tooling needed), so every byte that varies
    // between the three captures is provably per-call, not per-session noise. Findings:
    //   1. The single dynamic byte (previously assumed to be one of several unidentified
    //      candidates, none confirmed) is unambiguous here: byte offset 21 stepped 0x08 -> 0x0a ->
    //      0x0c across the three calls, a clean +2-per-round-trip pattern consistent with a
    //      call-number/cursor-id counter — not any of the bytes the old two-capture diff flagged
    //      as "opaque, presumably unchecked". That old diff's two captures happened to only ever
    //      differ by 3 essentially-random bytes because they came from different sessions, which
    //      buries the real per-call signal in session-to-session noise; same-session diffing
    //      removes that confound.
    //   2. A real server's message text is "ORA-01403: no data found\n" (the full formatted
    //      error, WITH the ORA- prefix and trailing newline, 25 bytes) — not the bare "no data
    //      found" (13 bytes) this class previously sent. Confirmed via the same live captures.
    //   3. A real column's own precision/scale still perturbs a few bytes just after the
    //      dynamic one (confirmed by diffing a NUMBER(7,2) SAL capture against a NUMBER(4,0)
    //      EMPNO capture from the same session — they differ in exactly that region, by exactly
    //      the byte or two a differently-sized UNV-encoded precision/scale field would cost).
    //      Only the common small-precision NUMBER shape (matching SAL, this narrow slice's own
    //      typical case) is replayed verbatim below; a column whose precision/scale needs a
    //      wider UNV encoding than SAL's is not yet covered by this template — a known,
    //      documented remaining gap, not silently claimed as general.
    // RESOLVED: split PREFIX further — the byte right after this array's own length-byte (the
    // "01 01" pair that used to sit inside one opaque PREFIX blob) is the al8o4 array's own
    // 3rd element, decompiled from ojdbc11's real T4C8Oall.readRPA() as the field it stores into
    // `this.cursor`. Found live, this exact bug: since it was baked into PREFIX as a hardcoded
    // constant (matching whatever ONE real capture happened to use), every exhausted query's
    // response reported the SAME fixed cursor number regardless of the real cursor PolyWire
    // itself assigned — python-oracledb (which, unlike ojdbc11, trusts and remembers this value
    // for later REEXECUTE calls rather than just discarding it) legitimately reused that same
    // wrong number for a LATER, DIFFERENT statement's REEXECUTE, silently re-running the wrong
    // SQL and returning another query's rows with no error at all. Confirmed live: a 5-query
    // python-oracledb sequence (sal, empno, ename, sal-repeat, empno-repeat) returned SAL's rows
    // for the empno-repeat call — reproduced twice, does NOT reproduce against real Oracle
    // directly with the byte-identical wire request, confirming this server's own bug, not a
    // client quirk.
    private static final byte[] INLINE_EXHAUSTION_PREFIX_A = HexFormat.of().parseHex(
            "080106033c77ac0001");
    private static final byte[] INLINE_EXHAUSTION_PREFIX_B = HexFormat.of().parseHex(
            "0000000000000401010208");
    private static final byte[] INLINE_EXHAUSTION_MIDDLE = HexFormat.of().parseHex(
            "010202057b000001010003000000000000000000000000030001010000000002057b0102010300");

    /**
     * The response a real Oracle server sends when a query's own inline row-prefetch (the same
     * DATA packet as EXECUTE's own DESCRIBE_INFO/ROW_DATA, not a separate FETCH) happens to
     * exhaust the cursor — i.e. every row fit within the client's own requested prefetch/array
     * size. Found live: this is NOT the same case as {@link #writeErrorEnd}'s
     * TNS_ERR_NO_DATA_FOUND (used for a real client-issued OFETCH running dry, spec §4.2,
     * confirmed correct and unchanged) — a real server uses a completely different message type
     * (TNS_MSG_TYPE_PARAMETER = 8, not this class's usual msg-type-4/OER shape) for the
     * EXECUTE-fused case specifically. Sending a plain success terminator instead (this
     * codebase's original behavior) silently desyncs a real client (ojdbc11/SQLcl, confirmed
     * live) with no visible error — this was the last of several bugs blocking a real Oracle
     * JDBC client from completing even a trivial SELECT through this server. Only cursor_id
     * (single raw byte, per this live capture — Oracle cursor ids observed in these sessions
     * stay well under 256; a session long/busy enough to exceed that isn't handled by this
     * narrow slice) and the error message are dynamic; everything else replays verbatim.
     *
     * <p>STILL NOT SUFFICIENT on its own: confirmed live this genuinely is a strictly more
     * byte-accurate response (own cursor_id correctly substituted, structure otherwise matching
     * two independent real captures byte-for-byte outside a handful of unidentified bytes that
     * differ between those two real captures too, so are presumably opaque/unchecked) — but a
     * real SQLcl/ojdbc11 session still hangs immediately after receiving it, in exactly the same
     * "stuck reading, no error, sends nothing further" shape as every other bug fixed in this
     * investigation. Two adjacent real captures (different sessions, different call/cursor
     * counts) were diffed to separate "must patch per-session" fields from "opaque, safe to
     * replay verbatim" ones — only 3 bytes varied between them at all, none obviously matching
     * either capture's own call-count/cursor-id pattern, so they were left verbatim rather than
     * guessed at further. The likeliest remaining suspect is an unidentified call-number-analog
     * field somewhere in the verbatim prefix — msg-type 4 (OER) is separately documented
     * elsewhere in this file as validating callNumber and silently desyncing on a mismatch (see
     * writeErrorEnd's own javadoc); msg-type 8 plausibly has an analogous check this template
     * doesn't satisfy. Not resolved — needs either a capture varying ONLY the call number with
     * everything else held constant, or Oracle-side protocol documentation this codebase doesn't
     * have access to.
     */
    // RESOLVED: the single dynamic byte here is callNumber, not cursorId — confirmed live (a
    // second query on the same connection got T4CTTIfun.handleOutOfSequenceError, the exact
    // named check msg-type-4's own javadoc already documents for this class of bug; msg-type 8
    // needed the same treatment and never got it). cursorId and callNumber happen to both start
    // near 1 and increment once per statement in this narrow slice, which is exactly why passing
    // cursorId here "worked" for a connection's FIRST query (they coincide) and broke on the
    // second (RequestLoop's callCounter starts at 2, one ahead of cursorId's start at 1, so they
    // diverge from the second call on).
    public static void writeInlineExhaustionEnd(TtcWriter w, int cursorId, int callNumber, String message) {
        w.writeRaw(INLINE_EXHAUSTION_PREFIX_A);
        w.writeUint8(cursorId);
        w.writeRaw(INLINE_EXHAUSTION_PREFIX_B);
        w.writeUint8(callNumber);
        w.writeRaw(INLINE_EXHAUSTION_MIDDLE);
        w.writeStrWithLength(message);
    }

    /**
     * Writes an error terminator, e.g. TNS_ERR_NO_DATA_FOUND for fetch
     * exhaustion. Spec §3.3/§4.2. Also carries cursor_id (see
     * {@link #writeSuccessEnd} javadoc) — end-of-fetch is a normal, expected
     * condition on an otherwise-still-open cursor, so this must NOT reset
     * cursor_id to 0.
     */
    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId) {
        writeErrorEnd(w, errorNum, message, cursorId, 0);
    }

    /**
     * See writeSuccessEnd's javadoc on callNumber — this shares the exact same OER layout (the
     * error_num field further below is what actually flags failure vs success to the client),
     * so it needs the same per-connection call counter at the same relative position, at uint8
     * offset "statement number, call number" — previously hardcoded to 0 unconditionally here,
     * unlike writeSuccessEnd which at least took a parameter (itself frequently called with a
     * hardcoded 0 too). Found live: T4CTTIfun.receive()'s code=4 case compares this field
     * against the driver's own internal per-connection RPC counter and silently routes to
     * handleOutOfSequenceError() on any mismatch, which then re-parses this response as if every
     * field were an error field regardless of errorNum, corrupting downstream parsing (confirmed
     * live: a real ojdbc11 client crashed with an arraycopy bounds exception deep inside
     * T4CTTIoer11.processError while handling a normal EXECUTE response, immediately after
     * O5LOGON's own analogous callNumber bug was fixed).
     */
    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0); // current row number
        // errorNum ALSO belongs here (T4CTTIoer11.unmarshalAttributes()'s "retCode" field,
        // read as UB2 right after curRowNumber, javap -c against ojdbc11.jar) — not only in the
        // later ub4 "error_num" field further below that python's _process_error_info reads.
        // Found live: with retCode left hardcoded 0 here, a real ojdbc11 client read "Error : 0"
        // for what should have been ORA-01403 (fetch exhaustion) and threw a raw/unrecognized
        // exception straight to user code instead of transparently ending the ResultSet, even
        // though the callNumber field (this method's other recent fix) already matched and the
        // later error_num field correctly held 1403 — ojdbc11 evidently keys its own
        // success/error branching off THIS earlier field, not the later one. Writing the same
        // value in both places satisfies both drivers without touching python's already-working
        // read path.
        w.writeUb2(errorNum);
        w.writeUb2(0);
        w.writeUb2(0);
        w.writeUb2(cursorId);
        w.writeSb1(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        writeZeroRowid(w);
        w.writeUb4(0);
        w.writeUint8(0); // statement number
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(errorNum);
        w.writeUb8(0);
        // >= 20.1 fields — see writeSuccessEnd's note. Unlike there, these come BEFORE the
        // error message field below: python's _process_error_info reads sql_type/checksum
        // right after error_num/rowcount, and only afterward — gated on error_num != 0, which
        // every real caller of this method satisfies — reads the message itself.
        w.writeUb4(0); // sql type
        w.writeUb4(0); // server checksum
        w.writeStrWithLength(message);
    }

    /**
     * Writes an all-zero rowid the way the client actually parses it
     * (packet.pyx read_rowid): rba/partition_id/block_num/slot_num are
     * variable-length ub4/ub2/ub4/ub2 fields (1 byte each for zero), plus one
     * fixed raw skip byte in the middle — 5 bytes total, NOT a fixed 12-byte
     * struct. Writing a fixed 12-byte block (as an earlier version of this
     * method did) overshoots by 7 bytes and misaligns every field that
     * follows; confirmed via a live PYO_DEBUG_PACKETS capture showing a
     * backend SQL error's error_num landing as 0 client-side (so the client
     * never raised an exception) despite the server encoding it correctly.
     */
    private static void writeZeroRowid(TtcWriter w) {
        w.writeUb4(0); // rba
        w.writeUb2(0); // partition_id
        w.writeUint8(0); // skip_ub1 (raw byte, unused)
        w.writeUb4(0); // block_num
        w.writeUb2(0); // slot_num
    }

    private ResponseWriter() {
    }
}
