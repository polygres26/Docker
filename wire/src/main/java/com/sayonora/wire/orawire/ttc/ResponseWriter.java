package com.sayonora.wire.orawire.ttc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

public final class ResponseWriter {

    private static final byte[] DESCRIBE_INFO_BLOB_FILLER = new byte[23];
    private static final int DESCRIBE_INFO_MAX_ROW_SIZE = 22;
    private static final int DESCRIBE_INFO_TRAILING_BYTE = 130;

    // The DESCRIBE_INFO header (msgtype through the trailing byte right before the first column)
    // a real Oracle server sends the same real dblink native-OCI client this whole fallback series
    // is for -- 7 bytes longer than the plain all-zero-filler version above (this codebase's own,
    // correct for and unchanged for JDBC/sqlplus/SQLcl), confirmed via a real Oracle 23c self-loop
    // capture. Used verbatim (its own filler content isn't understood field-by-field, only that
    // its LENGTH matters) with just the column-count byte patched -- confirmed at the same offset
    // in a second real capture with a different column count.
    private static final byte[] NATIVE_OCI_DESCRIBE_INFO_HEADER = java.util.Base64.getDecoder().decode(
        "EBcAAADAxMIZMJdKtcUM0E2zdjOGeH4IGgQbKCwAAAACAAAAgg==");
    private static final int NATIVE_OCI_DESCRIBE_INFO_HEADER_COLUMN_COUNT_OFFSET = 32;
    // Real bug, found live diffing a single-column native-OCI response against a real
    // Oracle-to-Oracle self-loop capture of that exact scenario (this codebase's fix chain had
    // only ever been verified against a 2-column capture until now): this byte, previously left as
    // the 2-column template's own fixed 0x2c (44), isn't fixed at all -- the real single-column
    // capture had 0x16 (22) in the exact same position. 22 is this codebase's own per-NUMBER-column
    // bufferSize (see ColumnMetadata/toColumnMetadata), and 22*2=44 matches the 2-column template
    // exactly -- a total-row-byte-budget field, the sum of every column's own buffer size.
    private static final int NATIVE_OCI_DESCRIBE_INFO_HEADER_ROW_SIZE_OFFSET = 28;

    private static void writeDescribeInfoHeaderNativeOci(TtcWriter w, List<ColumnMetadata> columns) {
        byte[] header = NATIVE_OCI_DESCRIBE_INFO_HEADER.clone();
        long rowSize = 0;
        for (ColumnMetadata col : columns) {
            rowSize += col.bufferSize;
        }
        header[NATIVE_OCI_DESCRIBE_INFO_HEADER_ROW_SIZE_OFFSET] = (byte) rowSize;
        header[NATIVE_OCI_DESCRIBE_INFO_HEADER_COLUMN_COUNT_OFFSET] = (byte) columns.size();
        w.writeRaw(header);
    }

    public static void writeDescribeInfo(TtcWriter w, List<ColumnMetadata> columns) {
        writeDescribeInfo(w, columns, false);
    }

    /**
     * @param nativeOciColumnFormat Use {@link #writeColumnMetadataNativeOci} instead of the normal
     * (correct for, and unchanged for, JDBC/sqlplus/SQLcl) {@link #writeColumnMetadata} -- see that
     * method's javadoc. Only {@code RequestLoop}'s native-OCI fallback path should ever pass
     * {@code true} here; every other caller uses the default, two-arg overload.
     */
    public static void writeDescribeInfo(TtcWriter w, List<ColumnMetadata> columns,
            boolean nativeOciColumnFormat) {
        if (nativeOciColumnFormat) {
            writeDescribeInfoHeaderNativeOci(w, columns);
        } else {
            w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
            w.writeBytesWithLength(DESCRIBE_INFO_BLOB_FILLER);
            w.writeUb4(DESCRIBE_INFO_MAX_ROW_SIZE);
            w.writeUb4(columns.size());
            if (!columns.isEmpty()) {
                w.writeUint8(DESCRIBE_INFO_TRAILING_BYTE);
            }
        }
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            if (nativeOciColumnFormat && (col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER
                    || col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR)) {
                writeColumnMetadataNativeOci(w, col, i);
            } else {
                writeColumnMetadata(w, col, i);
            }
        }

        // The real dblink native-OCI client needs a completely different (and much larger) tail
        // here than the plain currentDate+zeros this codebase's own JDBC/sqlplus/SQLcl-correct
        // format writes -- see RequestLoop.writeNativeOciExecuteTail's javadoc, which writes that
        // tail (and skips this codebase's own row-data/success-end writing entirely) instead, right
        // after this method returns, only for that same fallback path.
        if (nativeOciColumnFormat) {
            return;
        }

        byte[] currentDate = OracleDateCodec.encode(java.time.LocalDateTime.now());
        w.writeUb4(currentDate.length);
        w.writeBytesWithLength(currentDate);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeBytesWithLength(new byte[0]);
    }

    private static void writeColumnMetadata(TtcWriter w, ColumnMetadata col, int columnIndex) {
        w.writeUint8(col.oraTypeNum);
        w.writeUint8(0);
        
        int wireScale = col.scale > 0 ? col.scale - 1 : col.scale;
        w.writeSb1(col.precision);
        w.writeSb1(wireScale);
        long wireBufferSize = col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER && col.scale > 0
                ? 278 : col.bufferSize;
        w.writeUb4(wireBufferSize);
        w.writeUb4(0);
        w.writeUb8(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb2(0);
        
        w.writeUint8(col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR ? 1 : 0);
        
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUint8(col.nullsAllowed ? 1 : 0);
        
        w.writeUint8(col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        w.writeStrWithTwoLengths(col.name);
        w.writeStrWithTwoLengths(null);
        w.writeStrWithTwoLengths(null);
        
        w.writeUb2(columnIndex);
        w.writeUb4(0);
        
        w.writeStrWithTwoLengths(null);
        w.writeStrWithTwoLengths(null);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(0);
    }

    // A real Oracle server's per-column DESCRIBE_INFO block for a real distributed-database-link
    // connection's native OCI client is a fixed 56-byte prefix, then the column name written at
    // its own real length (NOT padded or truncated to a fixed width -- an earlier version of this
    // fix assumed a fixed 6-byte name field, confirmed WRONG live: a real capture of a 5-char
    // column name ("PRICE") produced a block exactly 3 bytes longer than a 2-char one ("ID"), and
    // a 3-char one ("QTY") 1 byte longer again -- then a fixed 32-byte suffix (which holds the
    // 0-based column index, always at the same offset relative to the suffix's own start,
    // regardless of the name's length). writeColumnMetadata above produces a much shorter, 31-byte
    // block instead -- correct for, and confirmed live unaffected by this method's existence,
    // JDBC/sqlplus/SQLcl. Confirmed byte-for-byte against two real Oracle 23c self-loop captures
    // (a 2-column and a 3-column SELECT, three differently-named columns total): diffing the
    // fixed-width prefix/suffix portions against each other across all three real columns isolates
    // exactly which bytes are fixed/structural versus column-specific (the name length, written
    // three times in lockstep, and the column index) -- this template encodes that structure
    // directly rather than guessing at Oracle's internal field semantics for the rest.
    private static final byte[] NATIVE_OCI_COLUMN_PREFIX_NUMBER = java.util.Base64.getDecoder().decode(
        "AQIAAIEWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAECAgAAAAI=");

    // The VARCHAR-column equivalent of NATIVE_OCI_COLUMN_PREFIX_NUMBER above -- same real,
    // byte-for-byte-captured discipline, this time confirmed via two real Oracle 23c self-loop
    // captures of a single-VARCHAR2(20)-column dblink query, differing only in the column name
    // ("NAME" vs "X"). Structurally identical to the NUMBER prefix in every way that matters here
    // -- also exactly 56 bytes, also patched at the same three relative offsets for the name
    // length (this codebase's own real captures show that "write the length three times" pattern
    // repeats verbatim across both types, not something specific to NUMBER) -- but the fixed
    // content differs (byte 1 is the real oraTypeNum, 1 for VARCHAR vs. 2 for NUMBER; two more
    // bytes, at offsets 5 and 41, both carry the column's own declared buffer size -- confirmed by
    // both real captures independently showing 0x14 (20) at both positions for a VARCHAR2(20)
    // column; not independently confirmed the two positions vary separately since both real
    // captures happened to use the same declared width, but writing the real bufferSize at both is
    // the safer assumption than leaving either one as a guessed constant). This generalizes what
    // was previously a NUMBER-only native-OCI column format (see writeColumnMetadataNativeOci's
    // own javadoc) to also cover VARCHAR -- confirmed live to fix a real hang: a native-OCI client
    // sending any single-VARCHAR-column query (including, notably, SQL*Plus's own unavoidable
    // startup probe query, `select current_user`) previously got this codebase's plain,
    // JDBC-shaped column block here instead of this one, and silently aborted with a TNS
    // BREAK/RESET rather than ever showing an error.
    private static final byte[] NATIVE_OCI_COLUMN_PREFIX_VARCHAR = java.util.Base64.getDecoder().decode(
        "AQGAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGkDAQAUAAAA/j8AAAEEBAAAAAQ=");
    private static final int NATIVE_OCI_VARCHAR_BUFFER_SIZE_OFFSET_1 = 5;
    private static final int NATIVE_OCI_VARCHAR_BUFFER_SIZE_OFFSET_2 = 41;

    private static final byte[] NATIVE_OCI_COLUMN_SUFFIX = new byte[32];
    private static final int[] NATIVE_OCI_NAME_LENGTH_OFFSETS = { 50, 51, 55 };
    private static final int NATIVE_OCI_COLUMN_INDEX_SUFFIX_OFFSET = 12;

    private static void writeColumnMetadataNativeOci(TtcWriter w, ColumnMetadata col, int columnIndex) {
        byte[] prefix = (col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR
                ? NATIVE_OCI_COLUMN_PREFIX_VARCHAR
                : NATIVE_OCI_COLUMN_PREFIX_NUMBER).clone();
        if (col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR) {
            prefix[NATIVE_OCI_VARCHAR_BUFFER_SIZE_OFFSET_1] = (byte) col.bufferSize;
            prefix[NATIVE_OCI_VARCHAR_BUFFER_SIZE_OFFSET_2] = (byte) col.bufferSize;
        }
        byte[] nameBytes = col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int offset : NATIVE_OCI_NAME_LENGTH_OFFSETS) {
            prefix[offset] = (byte) nameBytes.length;
        }
        byte[] suffix = NATIVE_OCI_COLUMN_SUFFIX.clone();
        suffix[NATIVE_OCI_COLUMN_INDEX_SUFFIX_OFFSET] = (byte) columnIndex;
        w.writeRaw(prefix);
        w.writeRaw(nameBytes);
        w.writeRaw(suffix);
    }

    public static void writeRow(TtcWriter w, List<ColumnMetadata> columns, Object[] values) {
        w.writeUint8(TtcConstants.MSG_TYPE_ROW_DATA);
        for (int i = 0; i < columns.size(); i++) {
            writeColumnValue(w, columns.get(i), values[i]);
        }
    }

    // Confirmed live via a real Oracle 23c self-loop capture (a real ojdbc CallableStatement
    // call, one IN NUMBER + one OUT NUMBER parameter): the response's OUT-bind-carrying block is
    // NOT a DESCRIBE_INFO/ROW_DATA pair (a first attempt using that shape produced a real
    // ORA-17401 protocol violation against a real client, confirmed live) -- it's this fixed
    // 10-byte preamble, whose individual field meanings aren't independently confirmed (no public
    // TTC spec available to cross-check against), followed by a MSG_TYPE_DESCRIBE_INFO (0x10) tag
    // byte, then one ROW_DATA-tagged value per OUT parameter in call-position order. This exact
    // byte sequence for exactly ONE scalar OUT parameter is what was captured and is what's
    // shipped here -- untested/unconfirmed for more than one OUT parameter in the same call (see
    // RequestLoop#handlePlSqlExecute's own scope notes).
    private static final byte[] IO_VECTOR_PREAMBLE = { 0x05, 0x01, 0x02, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x20 };

    public static void writeOutBindValues(TtcWriter w, List<ColumnMetadata> outColumns, Object[] outValues) {
        w.writeUint8(TtcConstants.MSG_TYPE_IO_VECTOR);
        w.writeRaw(IO_VECTOR_PREAMBLE);
        w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
        w.writeUint8(TtcConstants.MSG_TYPE_ROW_DATA);
        for (int i = 0; i < outColumns.size(); i++) {
            writeColumnValue(w, outColumns.get(i), outValues[i]);
        }
        // The real capture's value bytes are followed by one more 0x00 byte before the response's
        // next section begins -- a terminator for this values list, not part of the value's own
        // encoding (confirmed: writeColumnValue's own length-prefixed NUMBER encoding for this
        // exact value is only 3 bytes, "02 c1 2b"; the real capture has a 4th, "00", right after).
        w.writeUint8(0);
    }

    // A real distributed-database-link connection's native OCI client's own real row data --
    // embedded inline in its Execute response, not a separate Fetch (see RequestLoop's
    // nativeOciExecuteCount javadoc for why) -- carries 4 extra bytes between the ROW_DATA tag and
    // the first column's value that plain writeRow above doesn't produce, confirmed live via a real
    // Oracle-to-Oracle self-loop capture's own equivalent 2-column row. A SEPARATE real capture of
    // the identical scenario with a single-column query proved this prefix isn't universal, though:
    // that row's own ROW_DATA tag is followed immediately by its one column's length-prefixed value
    // with no extra bytes at all -- while everything else in the response (the DESCRIBE_INFO
    // header/columns, the tail before and after the row) is otherwise structurally identical in
    // shape between the 1- and 2-column captures. So this prefix is real, but conditional on having
    // more than one column, not an unconditional part of a "native-OCI row." Its own meaning still
    // isn't understood field-by-field (a per-row descriptor of some kind, plausibly ROWID/slot-like
    // housekeeping data a real backing table would have and this codebase's own rows don't) -- kept
    // separate from writeRow, not merged into it, since every other tested client's real captures
    // have never shown it and JDBC/sqlplus/SQLcl's rows must stay exactly as they were.
    private static final byte[] NATIVE_OCI_ROW_PREFIX = { 0x0a, 0x2c, 0x01, 0x02 };

    // A real single-VARCHAR-column native-OCI row is NOT prefix-free the way a real
    // single-NUMBER-column row is (see NATIVE_OCI_ROW_PREFIX's own javadoc for that case) --
    // confirmed live via two real Oracle 23c self-loop captures of a single VARCHAR2(20) column,
    // differing only in the value's own length ("ABC" vs "ZZ"): both show a real 3-byte marker
    // {0x2c, 0x01, 0x01} between the ROW_DATA tag and the column's own length-prefixed value,
    // preceded by a 1-byte count -- confirmed to be the total byte length of everything from that
    // 3-byte marker through the end of the column's own encoded value (3 + the column value's own
    // writeColumnValue output length), not a fixed constant: it was 7 for "ABC" (3 + 4, since
    // "ABC"'s own length-prefixed encoding is itself 4 bytes) and 6 for "ZZ" (3 + 3). Scoped to
    // exactly the single-VARCHAR-column case for now (not blindly generalized to every VARCHAR row
    // regardless of column count, which hasn't been captured/confirmed) -- see this method's
    // dispatch below.
    private static final byte[] NATIVE_OCI_SINGLE_VARCHAR_ROW_MARKER = { 0x2c, 0x01, 0x01 };

    public static void writeRowNativeOci(TtcWriter w, List<ColumnMetadata> columns, Object[] values) {
        w.writeUint8(TtcConstants.MSG_TYPE_ROW_DATA);
        if (columns.size() == 1 && columns.get(0).oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR) {
            TtcWriter valueWriter = new TtcWriter();
            writeColumnValue(valueWriter, columns.get(0), values[0]);
            byte[] encodedValue = valueWriter.toByteArray();
            w.writeUint8(NATIVE_OCI_SINGLE_VARCHAR_ROW_MARKER.length + encodedValue.length);
            w.writeRaw(NATIVE_OCI_SINGLE_VARCHAR_ROW_MARKER);
            w.writeRaw(encodedValue);
            return;
        }
        if (columns.size() > 1) {
            w.writeRaw(NATIVE_OCI_ROW_PREFIX);
        }
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
                // A Postgres BOOLEAN column is mapped to NUMBER (real Oracle's own NUMBER(1)
                // convention -- see RequestLoop.toColumnMetadata) -- value.toString() on a real
                // Boolean is "true"/"false", which BigDecimal can't parse as a number at all, so
                // it needs converting to 1/0 explicitly rather than falling into the generic
                // toString() parse below.
                BigDecimal bd = value instanceof BigDecimal b ? b
                        : value instanceof Boolean bool ? BigDecimal.valueOf(bool ? 1 : 0)
                        : new BigDecimal(value.toString());
                w.writeBytesWithLength(OracleNumberCodec.encode(bd));
            }
            case TtcConstants.ORA_TYPE_NUM_DATE -> {
                
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

    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId) {
        writeSuccessEnd(w, rowcount, cursorId, 0);
    }

    private static final byte[] O5LOGON_TERMINATOR_TAIL = HexFormat.of().parseHex(
            "0401010204df000000000000000000000000000000000000000100000000000000000000");
    
    private static final int O5LOGON_TERMINATOR_CALLNUMBER_OFFSET = 27;

    public static void writeO5LogonSuccessEnd(TtcWriter w, int cursorId, int callNumber) {
        byte[] tail = O5LOGON_TERMINATOR_TAIL.clone();
        tail[O5LOGON_TERMINATOR_CALLNUMBER_OFFSET] = (byte) callNumber;
        w.writeRaw(tail);
    }

    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        
        w.writeUb4(0);
        w.writeUb2(0);
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
        w.writeUint8(0);
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb8(rowcount);
        
        w.writeUb4(0);
        w.writeUb4(0);
    }

    /**
     * A success end that ALSO carries an inline warning (real row data plus a trailing
     * error-number/message pair on the very same response), matching what a real Oracle server
     * sends when a fetch both returns its last real row(s) AND exhausts the cursor in the same
     * call -- confirmed via a real Oracle packet capture (ScratchRealOracleCaptureTest): its
     * response to a "SELECT ... WHERE id = ?" fetch that both hands back the one matching row AND
     * exhausts the cursor embeds "ORA-01403: no data found" as a trailing warning alongside the
     * real rowcount, not as a separate no-rows-at-all error response. This is a genuinely
     * different shape from the old, since-removed writeInlineExhaustionEnd: that one was a
     * hardcoded, captured byte blob with error 1403 baked in and NO row-count/row-data field at
     * all, so a real ojdbc client correctly read it as "zero rows" and raised ORA-01403 to the
     * application even though the row was legitimately there -- a real, reproducible bug (see
     * docker/tests/java's OraWireTest javadoc). This writer keeps writeSuccessEnd's real
     * rowcount field (the actual row(s) already written via writeRows/similar are unaffected;
     * this only changes the trailing status fields) while additionally carrying the errorNum and
     * message fields writeErrorEnd uses, so a real client sees both "here are your real rows" and
     * "the cursor is now exhausted" in one response, matching the real-Oracle capture exactly.
     */
    public static void writeSuccessEndWithWarning(TtcWriter w, long rowcount, int cursorId, int errorNum,
            String message, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0);
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
        w.writeUint8(0);
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(errorNum);
        w.writeUb8(rowcount);

        w.writeUb4(0);
        w.writeUb4(0);
        w.writeStrWithLength(message);
    }

    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId) {
        writeErrorEnd(w, errorNum, message, cursorId, 0);
    }

    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0);
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
        w.writeUint8(0);
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(errorNum);
        w.writeUb8(0);
        
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeStrWithLength(message);
    }

    private static void writeZeroRowid(TtcWriter w) {
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUb2(0);
    }

    private ResponseWriter() {
    }
}
