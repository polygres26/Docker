package com.polygres.wire.orawire.ttc;

/** One bind variable's declared type and value, parsed from an OEXEC request. */
public final class BindParam {
    public final int oraTypeNum;
    public final Object value; // null, String, java.math.BigDecimal, java.time.LocalDateTime, or byte[] (RAW)

    public BindParam(int oraTypeNum, Object value) {
        this.oraTypeNum = oraTypeNum;
        this.value = value;
    }
}
