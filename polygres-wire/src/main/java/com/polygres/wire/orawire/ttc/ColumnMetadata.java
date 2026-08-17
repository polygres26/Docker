package com.polygres.wire.orawire.ttc;

/** One column's describe-info, narrow slice: VARCHAR2, NUMBER, DATE. Spec §3.1.1. */
public final class ColumnMetadata {

    public final String name;
    public final int oraTypeNum;
    public final int precision;
    public final int scale;
    public final long bufferSize;
    public final boolean nullsAllowed;

    public ColumnMetadata(String name, int oraTypeNum, int precision, int scale,
            long bufferSize, boolean nullsAllowed) {
        this.name = name;
        this.oraTypeNum = oraTypeNum;
        this.precision = precision;
        this.scale = scale;
        this.bufferSize = bufferSize;
        this.nullsAllowed = nullsAllowed;
    }
}
