package com.sayonora.wire.boltwire;

/** Bolt message signature bytes (client to server and server to client). */
final class BoltMessages {

    private BoltMessages() {
    }

    // client -> server
    static final int HELLO = 0x01;
    static final int GOODBYE = 0x02;
    static final int RESET = 0x0F;
    static final int RUN = 0x10;
    static final int BEGIN = 0x11;
    static final int COMMIT = 0x12;
    static final int ROLLBACK = 0x13;
    static final int DISCARD = 0x2F;
    static final int PULL = 0x3F;
    static final int TELEMETRY = 0x54; // 5.4+
    static final int ROUTE = 0x66;     // 4.3+
    static final int LOGON = 0x6A;     // 5.1+
    static final int LOGOFF = 0x6B;    // 5.1+

    // server -> client
    static final int SUCCESS = 0x70;
    static final int RECORD = 0x71;
    static final int IGNORED = 0x7E;
    static final int FAILURE = 0x7F;
}
