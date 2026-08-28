package com.nexagres.wire.boltwire;

/** Real Bolt message signature bytes -- every one confirmed against a genuine Neo4j 5.26 server
 * capture (see {@link PackStream}'s own javadoc for the capture this investigation is grounded in),
 * not just the public spec. Bolt 4.4 (the version this handler negotiates -- see
 * {@link BoltWireSessionHandler}'s own javadoc for why) uses a single combined HELLO carrying auth
 * inline, unlike Bolt 4.3+/5.x's separate HELLO+LOGON split -- confirmed live by forcing a real
 * {@code neo4j} Python driver down to a classic 4-byte version handshake and observing it send the
 * older single-HELLO shape instead. */
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
    static final int LOGON = 0x6A; // Bolt 4.3+/5.x only -- not used on the 4.4 path this handler
                                    // negotiates, kept named here since a real capture showed it.

    // server -> client
    static final int SUCCESS = 0x70;
    static final int RECORD = 0x71;
    static final int IGNORED = 0x7E;
    static final int FAILURE = 0x7F;
}
