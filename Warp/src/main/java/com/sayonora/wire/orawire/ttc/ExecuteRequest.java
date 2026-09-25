package com.sayonora.wire.orawire.ttc;

import java.util.List;

public final class ExecuteRequest {
    public final long cursorId;
    public final String sqlText;
    public final long options;
    public final long numIters;
    /** The FIRST (and, outside a real DML array-execute, only) row of bind values -- kept as its
     * own field since most call sites only ever care about one row. Equal to
     * {@code bindRows.get(0)}, or empty if there are no binds at all. */
    public final List<BindParam> bindParams;
    /** One entry per row for a real array-execute (ojdbc's {@code PreparedStatement.addBatch()} +
     * {@code executeBatch()} being the ordinary way a client produces this shape). Real bug, found
     * live via a byte-level capture of a real ojdbc client's batch INSERT against orawire
     * (OracleReexecuteBindCountDebugTest): each row is its OWN self-delimited {@code ROW_DATA}
     * ({@code 0x07}) -tagged block, back-to-back with NO row-count field anywhere in the
     * message -- confirmed by diffing a genuinely 1-row Execute against a genuinely 2-row batch
     * Execute of the SAME statement, byte for byte identical up to and including the bind
     * descriptors, differing only in a second {@code 0x07}-tagged block appended after the first.
     * The (unrelated) {@code numIters} field this codebase's own {@code ExecuteRequestReader} also
     * reads was checked and ruled out live: it reads 0 for every Execute observed, single-row or
     * batched alike, so it does NOT carry this count. {@link ExecuteRequestReader#readBindParams}
     * reads rows until the wire genuinely runs out of another {@code ROW_DATA} tag to consume,
     * rather than depending on a count. Exactly one entry ({@code bindParams} itself) for every
     * other Execute -- confirmed this shape never occurs for a query (see the same reader's own
     * javadoc for why). */
    public final List<List<BindParam>> bindRows;

    /** Convenience for the (overwhelmingly common) single-bind-row case -- erasure means a second
     * constructor overloaded on {@code List<List<BindParam>>} would collide with this one, so this
     * is a static factory instead. */
    public static ExecuteRequest singleRow(long cursorId, String sqlText, long options, long numIters,
            List<BindParam> bindParams) {
        return new ExecuteRequest(cursorId, sqlText, options, numIters, List.of(bindParams));
    }

    public ExecuteRequest(long cursorId, String sqlText, long options, long numIters, List<List<BindParam>> bindRows) {
        this.cursorId = cursorId;
        this.sqlText = sqlText;
        this.options = options;
        this.numIters = numIters;
        this.bindRows = bindRows;
        this.bindParams = bindRows.isEmpty() ? List.of() : bindRows.get(0);
    }

    public boolean isQuery() {
        return (options & TtcConstants.EXEC_OPTION_FETCH) != 0;
    }
}
