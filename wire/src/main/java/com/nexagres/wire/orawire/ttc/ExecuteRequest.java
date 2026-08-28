package com.nexagres.wire.orawire.ttc;

import java.util.List;

public final class ExecuteRequest {
    public final long cursorId;
    public final String sqlText;
    public final long options;
    public final long numIters;
    public final List<BindParam> bindParams;

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
