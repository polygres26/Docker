package com.sayonora.wire.orawire.ttc;

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
