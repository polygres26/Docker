package com.sayonora.wire.orawire.ttc;

public final class BindParam {
    public final int oraTypeNum;
    public final Object value;

    public BindParam(int oraTypeNum, Object value) {
        this.oraTypeNum = oraTypeNum;
        this.value = value;
    }
}
