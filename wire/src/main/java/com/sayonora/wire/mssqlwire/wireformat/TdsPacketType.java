package com.sayonora.wire.mssqlwire.wireformat;

public final class TdsPacketType {

    public static final byte SQL_BATCH = 0x01;

    public static final byte RPC = 0x03;

    public static final byte TABULAR_RESULT = 0x04;

    public static final byte ATTENTION = 0x06;

    public static final byte PRE_LOGIN = 0x12;

    public static final byte LOGIN7 = 0x10;

    public static final byte STATUS_NORMAL = 0x00;
    
    public static final byte STATUS_EOM = 0x01;

    private TdsPacketType() {
    }
}
