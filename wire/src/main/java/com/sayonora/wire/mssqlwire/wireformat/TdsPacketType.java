package com.sayonora.wire.mssqlwire.wireformat;

public final class TdsPacketType {

    public static final byte SQL_BATCH = 0x01;

    public static final byte RPC = 0x03;

    public static final byte TABULAR_RESULT = 0x04;

    public static final byte ATTENTION = 0x06;

    public static final byte PRE_LOGIN = 0x12;

    public static final byte LOGIN7 = 0x10;

    // Carries a raw SSPI security-blob (NTLM/Kerberos) in EITHER direction of the post-LOGIN7
    // continuation exchange -- unlike LOGIN7/TABULAR_RESULT, the payload is the bare blob itself,
    // no TDS token wrapper. See NtlmMessages' javadoc for the exchange this type code is used in.
    public static final byte SSPI = 0x11;

    public static final byte STATUS_NORMAL = 0x00;
    
    public static final byte STATUS_EOM = 0x01;

    private TdsPacketType() {
    }
}
