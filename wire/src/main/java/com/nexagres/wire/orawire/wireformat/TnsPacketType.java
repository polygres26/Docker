package com.nexagres.wire.orawire.wireformat;

public enum TnsPacketType {
    CONNECT(1),
    ACCEPT(2),
    ACK(3),
    REFUSE(4),
    REDIRECT(5),
    DATA(6),
    NULL(7),
    ABORT(9),
    RESEND(11),
    MARKER(12),
    ATTENTION(13),
    CONTROL(14);

    private final int code;

    TnsPacketType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static TnsPacketType fromCode(int code) {
        for (TnsPacketType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown TNS packet type: " + code);
    }
}
