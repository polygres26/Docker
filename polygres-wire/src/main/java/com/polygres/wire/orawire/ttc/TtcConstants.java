package com.polygres.wire.orawire.ttc;

/**
 * TTC wire constants for the narrow execute/fetch slice, taken from
 * reference/ttc_execute_fetch_spec.md (itself derived from python-oracledb's
 * impl/thin/constants.pxi and impl/base_impl.pxd).
 *
 * Values marked "ASSUMED" were flagged UNRESOLVED in the spec (their literal
 * wasn't located in the reference source) and are filled in here from
 * well-known Oracle domain knowledge, not from a confirmed source citation.
 */
public final class TtcConstants {

    public static final int MSG_TYPE_FUNCTION = 3;
    public static final int MSG_TYPE_ERROR = 4;
    public static final int MSG_TYPE_ROW_HEADER = 6;
    public static final int MSG_TYPE_ROW_DATA = 7;
    public static final int MSG_TYPE_PARAMETER = 8;
    public static final int MSG_TYPE_STATUS = 9;
    public static final int MSG_TYPE_PIGGYBACK = 17;
    public static final int MSG_TYPE_WARNING = 15;
    public static final int MSG_TYPE_DESCRIBE_INFO = 16;
    public static final int MSG_TYPE_BIT_VECTOR = 21;
    public static final int MSG_TYPE_END_OF_RESPONSE = 29;
    // Confirmed live (real sqlplus <-> real Oracle 23ai MITM capture): the extended
    // PROTOCOL-equivalent message a real OCI client sends immediately after ANO negotiation
    // completes, at protocol version >=320. Same driver-name-string payload as MSG_TYPE_PROTOCOL
    // (see ProtocolNegotiation.MSG_TYPE_PROTOCOL) but with a longer, 9-byte fixed header (vs.
    // MSG_TYPE_PROTOCOL's 2-byte version+zero header) before the driver name starts — see
    // ProtocolNegotiation.readProtocolRequest.
    public static final int MSG_TYPE_PROTOCOL_EXTENDED = 34;

    public static final int FUNC_EXECUTE = 94;
    // Statement-caching "reexecute" fast path (python-oracledb's own execute.pyx
    // _write_message: skips the full parse/bind-metadata EXECUTE when the same cursor's
    // statement was already parsed and nothing about its shape has changed since — the common
    // case for any loop that repeats cur.execute() with the same SQL text and a fresh cursor).
    // REEXECUTE is for non-query DML/PL-SQL; REEXECUTE_AND_FETCH additionally inline-fetches
    // rows, used for a repeated query.
    public static final int FUNC_REEXECUTE = 4;
    public static final int FUNC_REEXECUTE_AND_FETCH = 78;
    // options_2 bit python-oracledb sets on a REEXECUTE(_AND_FETCH) request when the connection
    // is in autocommit mode — distinct field AND bit from EXEC_OPTION_COMMIT below, which only
    // applies to a full EXECUTE's single options word.
    public static final int EXEC_OPTION_COMMIT_REEXECUTE = 0x1;
    public static final int FUNC_FETCH = 5;
    public static final int FUNC_LOGOFF = 9;
    public static final int FUNC_COMMIT = 14;
    public static final int FUNC_ROLLBACK = 15;
    public static final int FUNC_CLOSE_CURSORS = 105;
    // ojdbc11's OCANA ("cancel", per T4CTTIfunCodes.OCANA in the decompiled driver) piggyback —
    // found live (SQLcl hanging immediately post-O5LOGON, no visible error). Decompiling
    // T4C8Oclose.doOCANA/marshal (javap -c against ojdbc11.jar) showed it's structurally
    // IDENTICAL to FUNC_CLOSE_CURSORS below (same class, same marshalPTR + marshalUB4(count) +
    // count*marshalUB4(cursorId) shape) — confirmed against a live capture, byte-exact: OCANA
    // closing 1 cursor, immediately followed by a FUNC_CLOSE_CURSORS piggyback closing 2 more,
    // immediately followed by a completely ordinary FUNC_EXECUTE this server already knows how
    // to parse. There is no embedded query or special response for this piggyback at all — it
    // was mistaken for one before decompiling, since skipping it entirely (unrecognized code)
    // desynced the reader well before ever reaching that ordinary EXECUTE.
    public static final int FUNC_CANCEL_ALL = 120;
    // ojdbc11's "Client features" piggyback (see FUN_CODE_DESCRIPTIONS in the decompiled
    // driver), sent ahead of LOGOFF in a live capture — not documented in this repo's specs.
    public static final int FUNC_CLIENT_FEATURES = 191;
    // TNS_FUNC_SET_END_TO_END_ATTR — python-oracledb's own name for it
    // (_write_end_to_end_piggyback, messages/base.pyx). Carries MODULE/ACTION/CLIENT_IDENTIFIER/
    // CLIENT_INFO/DBOP session-tagging attributes (a real client's connection.module/.action/
    // .client_identifier properties, or DBMS_APPLICATION_INFO — set routinely in enterprise
    // Oracle shops, sometimes automatically by a connection pool or ORM with no application code
    // involved). Found live: any client that sets one of these previously killed the whole
    // session with "unsupported piggyback function code: 135" the instant it was sent.
    public static final int FUNC_SET_END_TO_END_ATTR = 135;

    public static final int EXEC_OPTION_PARSE = 0x01;
    public static final int EXEC_OPTION_BIND = 0x08;
    public static final int EXEC_OPTION_EXECUTE = 0x20;
    public static final int EXEC_OPTION_FETCH = 0x40;
    public static final int EXEC_OPTION_COMMIT = 0x100;
    public static final int EXEC_OPTION_NOT_PLSQL = 0x8000;

    // ASSUMED: bit position not confirmed in constants.pxi (spec §7 item 3).
    public static final int EXEC_FLAGS_IMPLICIT_RESULTSET = 0x00000001;

    public static final int TNS_MAX_SHORT_LENGTH = 252;
    public static final int TNS_LONG_LENGTH_INDICATOR = 254;
    public static final int TNS_NULL_LENGTH_INDICATOR = 255;
    public static final int TNS_CHUNK_SIZE = 32767;

    // ASSUMED: Oracle's well-known ORA-01403 "no data found" number; the
    // literal used on the wire as TNS_ERR_NO_DATA_FOUND was not located in
    // the reviewed source (spec §7 item 2).
    public static final int ERR_NO_DATA_FOUND = 1403;

    public static final int ORA_TYPE_NUM_VARCHAR = 1;
    public static final int ORA_TYPE_NUM_NUMBER = 2;
    public static final int ORA_TYPE_NUM_DATE = 12;
    // Confirmed live: python-oracledb sends `bytes` binds as this type (23), raw byte
    // content with no further decoding needed — the length-prefix framing already
    // handled by TtcReader.readBytesWithLength gives back the exact bytes.
    public static final int ORA_TYPE_NUM_RAW = 23;
    // Confirmed live: ojdbc11 sends java.sql.Timestamp binds as this type (180, DTY_TIMESTAMP),
    // distinct from DATE (12) — python-oracledb's own bind-type mapping doesn't distinguish
    // them the same way (both round-tripped through DATE=12 in every capture seen so far), so
    // this was never exercised until a real JDBC load test used setTimestamp(). Wire format is
    // the same 7-byte struct as DATE plus optional trailing fractional-nanosecond bytes;
    // OracleDateCodec.decode already tolerates (silently ignores) any bytes past the first 7.
    public static final int ORA_TYPE_NUM_TIMESTAMP = 180;

    private TtcConstants() {
    }
}
