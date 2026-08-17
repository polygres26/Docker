package com.polygres.wire.orawire.ttc;

import java.util.List;

/** Parsed OEXEC request from the client. */
public final class ExecuteRequest {
    public final long cursorId;
    public final String sqlText; // null if cursorId != 0 (re-execute of a cached cursor; not supported yet)
    public final long options;
    public final long numIters; // prefetch row count: how many rows the client expects inline in this response
    public final List<BindParam> bindParams; // empty if no bind variables

    public ExecuteRequest(long cursorId, String sqlText, long options, long numIters, List<BindParam> bindParams) {
        this.cursorId = cursorId;
        this.sqlText = sqlText;
        this.options = options;
        this.numIters = numIters;
        this.bindParams = bindParams;
    }

    public boolean isQuery() {
        return (options & TtcConstants.EXEC_OPTION_FETCH) != 0;
    }
}
