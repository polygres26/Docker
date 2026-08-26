package com.polygres.wire.orawire.ttc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ExecuteRequestReader {

    public static ExecuteRequest read(TtcReader r) {
        long options = r.readUb4();
        long cursorId = r.readUb4();

        String sqlText = null;
        int sqlPointer = r.readUint8();
        long sqlLength = r.readUb4();
        boolean freshParse = sqlPointer != 0;

        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUint8();
        r.readUb4();
        long numIters = r.readUb4();
        r.readUb4();

        int bindsPointer = r.readUint8();
        long numParams = r.readUb4();

        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUint8();

        int definesPointer = r.readUint8();
        long numDefines = r.readUb4();
        if (definesPointer != 0 || numDefines != 0) {
            throw new UnsupportedOperationException("column defines not supported in narrow slice");
        }

        r.readUb4();
        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUb4();
        r.readUb4();

        r.readUint8();
        r.readUb4();

        r.readUint8();

        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUb4();
        r.readUint8();

        r.readUint8();
        r.readUb4();

        if (freshParse) {
            sqlText = readSqlText(r, (int) sqlLength);
            r.readUb4();
        } else {
            r.readUb4();
        }
        r.readUb4();
        for (int i = 0; i < 4; i++) {
            r.readUb4();
        }
        r.readUb4();
        long isQueryField = r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();

        List<BindParam> bindParams = Collections.emptyList();
        if (bindsPointer != 0 && numParams > 0) {
            bindParams = readBindParams(r, (int) numParams);
        }

        return new ExecuteRequest(cursorId, sqlText, options, numIters, bindParams);
    }

    private static List<BindParam> readBindParams(TtcReader r, int numParams) {
        int[] oraTypeNums = new int[numParams];
        for (int i = 0; i < numParams; i++) {
            oraTypeNums[i] = r.readUint8();
            r.readUint8();
            r.readUint8();
            r.readUint8();
            r.readUb4();
            r.readUb4();
            r.readUb8();
            r.readUb4();
            r.readUb2();
            r.readUb2();
            r.readUint8();
            r.readUb4();
            r.readUb4();
        }

        List<BindParam> params = readBindValueRow(r, oraTypeNums);
        return params;
    }

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

            case TtcConstants.ORA_TYPE_NUM_RAW -> bytes;
            default -> throw new UnsupportedOperationException("unsupported bind variable type: " + oraTypeNum);
        };
    }

    /**
     * Fallback Execute-request reader for a real distributed-database-link connection's native OCI
     * client, whose Execute request is NOT field-compatible with {@link #read}'s layout (tuned
     * against JDBC/sqlplus/SQLcl, all of which parse correctly with it -- confirmed by this
     * codebase's existing, passing test/integration coverage, none of it touched by adding this
     * method). Confirmed live against a real Oracle 23c instance: {@code sqlPointer} reads as 0
     * (which {@link #read} takes as "not a fresh parse, no SQL text follows") even on a real dblink
     * client's very first Execute of a brand-new statement, and the small fixed-count fields before
     * the SQL text don't line up with this client's actual field count either -- by the time
     * {@link #read} reaches where it expects the SQL text to start, it's already misaligned by
     * several fields, and running its field-by-field trace forward (temporary debug instrumentation,
     * not guessed) showed the very next read landing squarely on the SQL text's own first character
     * instead of a real length field, well before the eventual out-of-bounds crash several fields
     * further on. Reliably relocating the *correct* preceding length field looked like it would need
     * the same kind of extended live byte-capture investigation the rest of this dblink-compatibility
     * series has used -- ground truth this method sidesteps entirely by not depending on exact field
     * offsets or counts at all: it scans the raw request payload directly for a recognizable SQL
     * keyword at a plausible start of statement text, then takes the following run of dominantly
     * printable ASCII bytes as the statement. This is deliberately narrower than {@link #read}: no
     * bind variables, no column defines, no non-fresh-parse (re-execute) support -- exactly what a
     * dblink-forwarded remote query (a single, self-contained SELECT with no placeholders, generated
     * by Oracle's own dblink layer, not authored by an end user) actually needs. Only used as an
     * explicit fallback in {@code RequestLoop} when {@link #read} throws
     * {@link ArrayIndexOutOfBoundsException} parsing an Execute request -- something the existing
     * JDBC/sqlplus/SQLcl-shaped traffic this codebase already handles has never been observed to do.
     */
    public static ExecuteRequest readByScanningForSql(byte[] rawPayload) {
        int sqlStart = findSqlStatementStart(rawPayload);
        if (sqlStart < 0) {
            throw new UnsupportedOperationException(
                    "could not locate a SQL statement in this Execute request by scanning "
                            + "(fallback for a real dblink native-OCI client's non-JDBC-shaped request)");
        }
        int sqlEnd = sqlStart;
        while (sqlEnd < rawPayload.length && isLikelySqlTextByte(rawPayload[sqlEnd])) {
            sqlEnd++;
        }
        String sqlText = new String(rawPayload, sqlStart, sqlEnd - sqlStart,
                java.nio.charset.StandardCharsets.UTF_8).stripTrailing();
        // SELECT/WITH need EXEC_OPTION_FETCH set for ExecuteRequest.isQuery() to route this through
        // the fetch/cursor path instead of being treated as a plain DML execute -- ExecuteRequest
        // itself derives isQuery() purely from this options bit, and this fallback bypasses the
        // real field that would otherwise carry it, so it has to be inferred here instead.
        boolean isQuery = sqlText.regionMatches(true, 0, "SELECT", 0, "SELECT".length())
                || sqlText.regionMatches(true, 0, "WITH", 0, "WITH".length());
        long options = isQuery ? TtcConstants.EXEC_OPTION_FETCH : 0;
        // numIters (the requested prefetch row count) can't be recovered by this fallback -- the
        // real field is somewhere in the same misaligned region readByScanningForSql exists to
        // avoid depending on. A generous fixed count, rather than 1, keeps a typical
        // dblink-forwarded query's whole result set flowing back in this same response instead of
        // needing a real FETCH continuation this fallback also doesn't model. (0 -- no inline rows
        // at all, on the theory that this client fetches separately -- was also tried live against
        // a real Oracle 23c instance: no better, still the same TNS BREAK/RESET reaction, so this
        // stays at a real row count instead.)
        return new ExecuteRequest(0, sqlText, options, FALLBACK_NUM_ITERS, Collections.emptyList());
    }

    private static final long FALLBACK_NUM_ITERS = 100;

    private static final String[] SQL_STATEMENT_KEYWORDS =
            { "SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "MERGE", "BEGIN", "CALL" };

    private static int findSqlStatementStart(byte[] payload) {
        for (String keyword : SQL_STATEMENT_KEYWORDS) {
            byte[] needle = keyword.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            outer:
            for (int i = 0; i <= payload.length - needle.length; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (payload[i + j] != needle[j]) {
                        continue outer;
                    }
                }
                // Require the match to start a "word" (not be a substring of a longer identifier,
                // e.g. matching "SELECT" inside "MULTISELECT") -- the preceding byte, if any, must
                // not itself be a plain ASCII letter/digit/underscore.
                boolean wordStart = i == 0 || !isIdentifierByte(payload[i - 1]);
                if (wordStart) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean isIdentifierByte(byte b) {
        char c = (char) (b & 0xFF);
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static boolean isLikelySqlTextByte(byte b) {
        int v = b & 0xFF;
        // Printable ASCII plus tab -- real SQL text (including quoted identifiers/literals) stays
        // within this range; the first byte that falls outside it is real Oracle's own trailing
        // wire-format padding/fields, not statement content.
        return (v >= 0x20 && v < 0x7F) || v == '\t';
    }

    private static String readSqlText(TtcReader r, int sqlLength) {
        // Deliberately readRawBytes, NOT readRawOrLengthPrefixedBytes -- real bug, found live
        // testing against a genuine SQLcl client (not just JDBC): that method's "is there a
        // redundant length-prefix byte here?" check guesses by comparing the *next* byte's value
        // to sqlLength, and skips it if they match. For SQL text specifically that's unsound --
        // the next byte is the first character of the statement itself, an arbitrary ASCII value,
        // and when a statement happens to be exactly as many bytes long as the ASCII code of its
        // own first character (e.g. a 67-byte "CREATE TABLE ..." statement -- 'C' is 67), the
        // heuristic misfires, skips a real content byte, and misaligns every field parsed after
        // it until the buffer runs out (an ArrayIndexOutOfBoundsException several fields later,
        // nowhere near the actual bug). Confirmed empirically: a passing, unambiguous-length
        // request's trace shows readRawOrLengthPrefixedBytes's guess never actually triggers for
        // this field in practice (it consumes exactly sqlLength bytes with no skip) -- meaning the
        // guess is a live hazard with no evidenced benefit here, unlike O5LogonHandler's username
        // field (a separate call site, untouched), which is left as-is since there's no equivalent
        // evidence that call site's use of the same heuristic is unsafe.
        byte[] bytes = r.readRawBytes(sqlLength);
        return bytes == null ? "" : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
