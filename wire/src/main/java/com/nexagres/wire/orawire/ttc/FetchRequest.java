package com.nexagres.wire.orawire.ttc;

public final class FetchRequest {
    public final long cursorId;
    public final long fetchArraySize;

    public FetchRequest(long cursorId, long fetchArraySize) {
        this.cursorId = cursorId;
        this.fetchArraySize = fetchArraySize;
    }

    public static FetchRequest read(TtcReader r) {
        return new FetchRequest(r.readUb4(), r.readUb4());
    }
}
