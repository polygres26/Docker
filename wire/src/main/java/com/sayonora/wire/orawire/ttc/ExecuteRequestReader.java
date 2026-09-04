package com.sayonora.wire.orawire.ttc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ExecuteRequestReader {

    public static ExecuteRequest read(TtcReader r) {
        return read(r, cursorId -> null);
    }

    /** @param cachedBindTypesLookup resolves a previously-parsed cursor's bind types by cursor id
     *      -- needed for the real gap this parameter exists to close (see below); a lookup that
     *      always returns {@code null} (the no-arg {@link #read(TtcReader)} overload) is exactly
     *      as safe as this codebase's previous behavior for every OTHER shape.
     *
     *      <p>Real bug, found live via byte-level capture (see {@link ExecuteRequest#bindRows}'s
     *      own javadoc for the sibling array-execute finding from the same investigation): when a
     *      client re-executes an already-parsed cursor with NEW bind values (real ojdbc shape,
     *      confirmed live -- {@code cursorId != 0}, {@code sqlText == null}, this is a genuine
     *      {@code FUNC_EXECUTE}, NOT a {@code FUNC_REEXECUTE}, for reusing one statement across
     *      calls that aren't literally the same {@code PreparedStatement.addBatch()} run),
     *      {@code bindsPointer} reads as 0 (no FRESH descriptors, since the client already
     *      described them on the original parse) even though the wire genuinely still carries real
     *      {@code ROW_DATA}-tagged bind VALUE rows afterward, using the types from that original
     *      description. The old code's {@code bindsPointer != 0} gate treated "no fresh
     *      descriptors" as "no bind values at all" and silently left those real trailing bytes
     *      unread -- exactly the corruption {@code RequestLoop.handleReexecute} already had to
     *      solve for its own, textually-different code path (a real {@code FUNC_REEXECUTE}) by
     *      keeping the cached types from the original parse; this closes the same gap for the
     *      EXECUTE-shaped case of the identical real client behavior. */
    public static ExecuteRequest read(TtcReader r, java.util.function.LongFunction<int[]> cachedBindTypesLookup) {
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

        List<List<BindParam>> bindRows = List.of(Collections.<BindParam>emptyList());
        if (bindsPointer != 0 && numParams > 0) {
            bindRows = readBindParams(r, (int) numParams);
        } else if (!freshParse && r.hasRemaining()) {
            // See this method's own two-arg overload javadoc: a cursor-reuse Execute with NEW bind
            // values sends them without re-describing types (bindsPointer stays 0), so the only way
            // to know how to decode the real bytes still sitting in this message is the cursor's
            // ORIGINAL bind types, from its first (fresh-parse) Execute.
            int[] cachedTypes = cachedBindTypesLookup.apply(cursorId);
            if (cachedTypes != null && cachedTypes.length > 0) {
                bindRows = readAllBindValueRows(r, cachedTypes);
            }
        }

        return new ExecuteRequest(cursorId, sqlText, options, numIters, bindRows);
    }

    /** Reads the per-parameter descriptors ONCE (type/length/etc, sent once regardless of how many
     * rows follow), then every row of bind VALUES that actually follows -- see
     * {@link ExecuteRequest#bindRows}'s own javadoc for how live byte-capture confirmed rows are
     * self-delimited (each starts with its own {@code ROW_DATA} tag byte) rather than counted by
     * any field this reader has found, and why looping on that tag rather than trusting
     * {@code numIters} is the correct, confirmed-safe way to read them. */
    private static List<List<BindParam>> readBindParams(TtcReader r, int numParams) {
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

        return readAllBindValueRows(r, oraTypeNums);
    }

    /** Reads one row of bind values, then keeps reading more for as long as the wire genuinely has
     * another one -- see {@link ExecuteRequest#bindRows}'s javadoc for how live byte-capture
     * confirmed a real DML array-execute (ojdbc's {@code addBatch()}/{@code executeBatch()}) sends
     * N rows this way, each self-delimited by its own {@code ROW_DATA} ({@code 0x07}) tag with no
     * count field anywhere, rather than a single row plus a separate {@code numIters}-driven count
     * (confirmed live NOT to carry it: it read 0 for every Execute observed, batched or not). Used
     * both for a fresh-parse Execute's bind section ({@link #readBindParams}) and for REEXECUTE's
     * (see {@code RequestLoop#handleReexecute}), which shares the identical bind-value wire shape. */
    public static List<List<BindParam>> readAllBindValueRows(TtcReader r, int[] oraTypeNums) {
        List<List<BindParam>> rows = new ArrayList<>();
        rows.add(readBindValueRow(r, oraTypeNums));
        // A single row's own values end exactly where the next row's ROW_DATA tag would begin
        // (or where the message simply ends, for the overwhelmingly common single-row case) --
        // confirmed live: a genuine 2-row array-execute's second row is a second, complete
        // ROW_DATA-tagged block with NO separator and no count field anywhere before it, appended
        // immediately after the first row's last value byte.
        while (r.hasRemaining() && (r.peekRawBytes(0, 1)[0] & 0xFF) == TtcConstants.MSG_TYPE_ROW_DATA) {
            rows.add(readBindValueRow(r, oraTypeNums));
        }
        return rows;
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

            case TtcConstants.ORA_TYPE_NUM_RAW, TtcConstants.ORA_TYPE_NUM_BLOB -> bytes;
            // A CLOB bind value's own bytes are already length-prefixed/PLP-chunked the same way
            // VARCHAR's are (see TtcConstants.ORA_TYPE_NUM_CLOB's own javadoc) -- decoded
            // identically, just tagged with a different type code on the wire.
            case TtcConstants.ORA_TYPE_NUM_CLOB -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            // A REF CURSOR OUT parameter's own placeholder value -- see TtcConstants
            // .ORA_TYPE_NUM_CURSOR's own javadoc for why this decodes rather than refuses.
            case TtcConstants.ORA_TYPE_NUM_CURSOR -> bytes;
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

    // Real, disclosed leading keywords for every statement kind a client would send through
    // Execute -- deliberately NOT client-identity-based (see this method's own javadoc for why):
    // this is a check against the message's own decoded content, the same kind of signal
    // #isLikelySqlTextByte already uses elsewhere in this class, not who's connected.
    private static final java.util.Set<String> SQL_LEADING_KEYWORDS = java.util.Set.of(
            "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE",
            "GRANT", "REVOKE", "COMMIT", "ROLLBACK", "SET", "BEGIN", "DECLARE", "CALL", "EXPLAIN",
            "WITH", "ANALYZE", "COMMENT", "LOCK", "SAVEPOINT", "RENAME");

    private static boolean looksLikeRealSql(String candidate) {
        String trimmed = candidate.stripLeading();
        int end = 0;
        while (end < trimmed.length() && Character.isLetter(trimmed.charAt(end))) {
            end++;
        }
        return SQL_LEADING_KEYWORDS.contains(trimmed.substring(0, end).toUpperCase(java.util.Locale.ROOT));
    }

    /**
     * Real bug, found live against two genuine Oracle clients with opposite behavior at this exact
     * field: a real SQLcl session showed NO redundant length-prefix byte before SQL text (a fixed
     * {@code readRawBytes(sqlLength)} read was correct there -- see this method's git history for
     * the original investigation), but a real {@code python-oracledb} (thin-mode) session DOES send
     * one -- confirmed live via byte-level tracing: the byte immediately before a {@code "SELECT
     * ..."} statement's real content was {@code 0x26}, exactly equal to {@code sqlLength}, i.e. a
     * genuine redundant echo of the length just parsed, not a coincidental first-character match.
     * Skipping it unconditionally (the pre-regression behavior) broke SQLcl; never skipping it (the
     * regression this replaces) breaks python-oracledb. Neither client's own identity is available
     * here to key off of (and using one would be the wrong signal regardless -- Oracle's wire
     * protocol is opcode-driven, not client-driven), so this reads BOTH candidate windows -- with
     * the marker skipped and without -- and keeps whichever one actually decodes to a real SQL
     * statement (starts with a real SQL keyword), the one signal that's genuinely unambiguous
     * regardless of which client sent it. Falls back to the no-skip reading (this class's own
     * pre-fix default) on the practically-unreachable case where NEITHER candidate looks like real
     * SQL, so a genuinely novel client shape fails the same way the old unconditional read did,
     * not silently worse.
     */
    private static String readSqlText(TtcReader r, int sqlLength) {
        String withoutSkip = decode(r.peekRawBytes(0, sqlLength));
        if (looksLikeRealSql(withoutSkip)) {
            r.readRawBytes(sqlLength);
            return withoutSkip;
        }
        if (r.remaining() >= 1 + sqlLength) {
            String withSkip = decode(r.peekRawBytes(1, sqlLength));
            if (looksLikeRealSql(withSkip)) {
                r.readRawBytes(1 + sqlLength);
                return withSkip;
            }
        }
        r.readRawBytes(sqlLength);
        return withoutSkip;
    }

    private static String decode(byte[] bytes) {
        return bytes == null ? "" : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
