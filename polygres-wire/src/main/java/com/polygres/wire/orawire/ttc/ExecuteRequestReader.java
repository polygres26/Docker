package com.polygres.wire.orawire.ttc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses an OEXEC request body, mirroring the write-side layout documented
 * in reference/ttc_execute_fetch_spec.md §2.2. Bind variable metadata/value
 * layout was not in that spec (flagged out of scope) — confirmed instead
 * against a live capture of a real bound query (`... where ename = :1`):
 * for each of numParams bind vars, a column-metadata block (ora_type_num,
 * flag, precision=0, scale=0, buffer_size, max_num_elements=0, cont_flag
 * ub8=0, OID ub4=0, version ub2=0, charset_id ub2, csfrm, lob_prefetch_length
 * ub4=0, oaccolid ub4=0 [12.2+]) per messages/base.pyx:1413-1468
 * (_write_column_metadata), followed by a single TNS_MSG_TYPE_ROW_DATA(7)
 * tag and then each bind value using the same single-length-prefix encoding
 * as regular row data. No bind NAME is transmitted — correspondence to
 * ":1"/":name" tokens in the SQL text is purely positional, matching the
 * order the client's own local SQL parse assigned them (this server relies
 * on the same order via {@link com.polygres.wire.orawire.translator.BindVariableRewriter}).
 *
 * No DDL, no column defines, no subscription registration.
 *
 * The trailing "12.2" and "12.2_ext1" optional field groups both depend on
 * the ttc_field_version negotiated during PROTOCOL/DATA_TYPES (spec §1);
 * this reader now assumes a modern client sends both groups, matching the
 * real capability array ProtocolNegotiation now advertises (well above
 * 12.2_ext1) — see this class's readSqlText call site for the field-by-field
 * citation.
 */
public final class ExecuteRequestReader {

    public static ExecuteRequest read(TtcReader r) {
        long options = r.readUb4();
        long cursorId = r.readUb4();

        String sqlText = null;
        int sqlPointer = r.readUint8();
        long sqlLength = r.readUb4();
        boolean freshParse = sqlPointer != 0;

        r.readUint8(); // al8i4 pointer
        r.readUb4();   // al8i4 length, always 13
        r.readUint8(); // al8o4 pointer (always null)
        r.readUint8(); // al8o4l pointer (always null)
        r.readUb4();   // prefetch buffer size (unused)
        long numIters = r.readUb4();
        r.readUb4();   // TNS_MAX_LONG_LENGTH

        int bindsPointer = r.readUint8();
        long numParams = r.readUb4();

        r.readUint8(); // al8app
        r.readUint8(); // al8txn
        r.readUint8(); // al8txl
        r.readUint8(); // al8kv
        r.readUint8(); // al8kvl

        int definesPointer = r.readUint8();
        long numDefines = r.readUb4();
        if (definesPointer != 0 || numDefines != 0) {
            throw new UnsupportedOperationException("column defines not supported in narrow slice");
        }

        r.readUb4(); // registration id lsb
        r.readUint8(); // al8objlist
        r.readUint8(); // al8objlen (always 1)
        r.readUint8(); // al8blv
        r.readUb4();   // al8blvl
        r.readUint8(); // al8dnam
        r.readUb4();   // al8dnaml
        r.readUb4();   // registration id msb

        r.readUint8(); // arraydmlrowcounts pointer
        r.readUb4();   // num_execs (for arraydmlrowcounts)
        // Third field is ALWAYS present (both the true and false branches of
        // "if arraydmlrowcounts" write a trailing uint8, per spec §2.2) —
        // confirmed against a live capture of a real EXECUTE request; an
        // earlier version of this reader only read it conditionally, which
        // silently misaligned every field after it whenever this pointer
        // was 0 (the common case).
        r.readUint8();

        // assume ttc_field_version >= 12.2 (see class javadoc gap note)
        r.readUint8(); // al8sqlsig pointer
        r.readUb4();   // SQL signature length
        r.readUint8(); // SQL ID pointer
        r.readUb4();   // SQL ID allocated size
        r.readUint8(); // SQL ID length pointer
        // ttc_field_version >= 12.2_EXT1 fields — confirmed against python-oracledb's own
        // write-side source (messages/execute.pyx _write_execute_message: the `if
        // buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1` block writes 2 more
        // fields, chunk-ids pointer + count). This server didn't used to reach that threshold
        // (ProtocolNegotiation pinned field_version at plain 12.2 via an 8-byte placeholder
        // compile_caps array), so these were never sent/expected — but once that array was
        // replaced with a real server's full capability array (see ProtocolNegotiation's
        // PROTOCOL_RESPONSE_B64 javadoc), the negotiated version now clears 12.2_EXT1 too,
        // and both python-oracledb and ojdbc11 correspondingly start sending them. Confirmed
        // live: a real python-oracledb EXECUTE request for a bare "SELECT 1 FROM DUAL" (87-byte
        // payload) ran off the end of the buffer without these two fields, reading garbage into
        // every field that followed.
        r.readUint8(); // chunk-ids pointer
        r.readUb4();   // number of chunk ids

        // al8i4 array payload
        if (freshParse) {
            sqlText = readSqlText(r, (int) sqlLength);
            r.readUb4(); // al8i4[0] = 1 (parse flag)
        } else {
            r.readUb4(); // al8i4[0] = 0
        }
        r.readUb4(); // al8i4[1] execution count
        for (int i = 0; i < 4; i++) {
            r.readUb4(); // al8i4[2..5]
        }
        r.readUb4(); // al8i4[6]
        long isQueryField = r.readUb4(); // al8i4[7]
        r.readUb4(); // al8i4[8]
        r.readUb4(); // al8i4[9] exec_flags
        r.readUb4(); // al8i4[10] fetch orientation
        r.readUb4(); // al8i4[11] fetch pos
        r.readUb4(); // al8i4[12]

        List<BindParam> bindParams = Collections.emptyList();
        if (bindsPointer != 0 && numParams > 0) {
            bindParams = readBindParams(r, (int) numParams);
        }

        return new ExecuteRequest(cursorId, sqlText, options, numIters, bindParams);
    }

    private static List<BindParam> readBindParams(TtcReader r, int numParams) {
        int[] oraTypeNums = new int[numParams];
        for (int i = 0; i < numParams; i++) {
            oraTypeNums[i] = r.readUint8(); // ora_type_num
            r.readUint8(); // flag
            r.readUint8(); // precision (always 0)
            r.readUint8(); // scale (always 0)
            r.readUb4();   // buffer_size
            r.readUb4();   // max_num_elements
            r.readUb8();   // cont_flag
            r.readUb4();   // OID (no objtype support)
            r.readUb2();   // version
            r.readUb2();   // charset_id
            r.readUint8(); // csfrm
            r.readUb4();   // lob_prefetch_length
            r.readUb4();   // oaccolid (12.2+)
        }

        List<BindParam> params = readBindValueRow(r, oraTypeNums);
        return params;
    }

    /**
     * Reads one ROW_DATA-tagged row of bind VALUES ONLY, given the ora_type_num for each
     * position already known from elsewhere — no per-bind metadata block precedes it. Used both
     * by a full EXECUTE's own single row (factored out of {@link #readBindParams}) and by
     * REEXECUTE/REEXECUTE_AND_FETCH (RequestLoop.handleReexecute), whose whole point is skipping
     * that metadata block and reusing the types the ORIGINAL EXECUTE on this cursor already
     * declared.
     */
    public static List<BindParam> readBindValueRow(TtcReader r, int[] oraTypeNums) {
        int rowDataTag = r.readUint8();
        if (rowDataTag != TtcConstants.MSG_TYPE_ROW_DATA) {
            throw new UnsupportedOperationException(
                    "expected ROW_DATA tag for bind values, got " + rowDataTag
                            + " (multi-execution/array binds not supported)");
        }

        List<BindParam> params = new ArrayList<>(oraTypeNums.length);
        for (int oraTypeNum : oraTypeNums) {
            byte[] bytes = r.readBytesWithLength();
            Object value = decodeBindValue(oraTypeNum, bytes);
            params.add(new BindParam(oraTypeNum, value));
        }
        return params;
    }

    private static Object decodeBindValue(int oraTypeNum, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return switch (oraTypeNum) {
            case TtcConstants.ORA_TYPE_NUM_VARCHAR -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            case TtcConstants.ORA_TYPE_NUM_NUMBER -> OracleNumberCodec.decode(bytes);
            case TtcConstants.ORA_TYPE_NUM_DATE, TtcConstants.ORA_TYPE_NUM_TIMESTAMP -> OracleDateCodec.decode(bytes);
            // RAW: already just raw bytes once the length-prefix framing is stripped —
            // no further decoding, passed straight through to JDBC's setObject(byte[])
            // (RequestLoop.bindParamsDirect), which maps to Postgres bytea.
            case TtcConstants.ORA_TYPE_NUM_RAW -> bytes;
            default -> throw new UnsupportedOperationException("unsupported bind variable type: " + oraTypeNum);
        };
    }

    /**
     * NOT r.readBytesWithLength() (an earlier version of this method used that, which worked for
     * python-oracledb but crashed on a real ojdbc11 request with an arraycopy bounds exception):
     * the SQL text's length was ALREADY read a few fields earlier (the {@code sqlLength} ub4)
     * and, for ojdbc11, that's the field's one and only length — the bytes immediately follow
     * with no length byte of their own, same pattern found for O5LOGON's username field (see
     * O5LogonHandler.readUsernameAndSkipPairs's javadoc). python-oracledb, confirmed live, DOES
     * write a redundant extra length byte here matching read_bytes_with_length's shape, which is
     * exactly what readRawOrLengthPrefixedBytes already exists to tolerate either way of.
     */
    private static String readSqlText(TtcReader r, int sqlLength) {
        byte[] bytes = r.readRawOrLengthPrefixedBytes(sqlLength);
        return bytes == null ? "" : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
