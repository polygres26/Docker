package com.polygres.wire.mssqlwire.wireformat;

/**
 * TDS packet header "Type" byte values (MS-TDS §2.2.3.1 / Babelfish's {@code
 * postgresql_modified_for_babelfish} TDS front-end reads/writes the same byte-for-byte framing
 * this enum documents; the public MS-TDS spec was cross-referenced for the exact numeric values
 * since the Babelfish TDS source directory could not be pinned down within this pass's budget —
 * see mssqlwire package javadoc). Only the subset this narrow first pass actually speaks/emits is
 * listed.
 */
public final class TdsPacketType {

    /** Client -> server: SQL_BATCH, plain-text SQL, no RPC/bind support (this pass's scope). */
    public static final byte SQL_BATCH = 0x01;

    /** Client -> server: RPC call (e.g. {@code sp_executesql}) — not handled by this pass; logged and rejected. */
    public static final byte RPC = 0x03;

    /** Server -> client: tabular result (COLMETADATA/ROW/DONE, or LOGINACK, or ERROR tokens). */
    public static final byte TABULAR_RESULT = 0x04;

    /** Client -> server: attention/cancel. */
    public static final byte ATTENTION = 0x06;

    /** Both directions: PRELOGIN handshake. */
    public static final byte PRE_LOGIN = 0x12;

    /** Client -> server: LOGIN7 (auth). */
    public static final byte LOGIN7 = 0x10;

    // ---- header Status byte flags ----
    public static final byte STATUS_NORMAL = 0x00;
    /** End Of Message — last (or only) physical packet of a logical TDS message. */
    public static final byte STATUS_EOM = 0x01;

    private TdsPacketType() {
    }
}
