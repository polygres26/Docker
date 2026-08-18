package com.polygres.wire.mssqlwire.frontend;

import java.nio.charset.StandardCharsets;

public final class Login7Handler {

    public record Credentials(String hostName, String userName, String password, String appName,
            String serverName, String database, boolean integratedSecurity) {
    }

    private static final int OFFSET_LEN_BLOCK_START = 36;

    public static Credentials parse(byte[] payload) {
        byte optionFlags2 = payload[25];
        boolean integratedSecurity = (optionFlags2 & 0x80) != 0;

        int p = OFFSET_LEN_BLOCK_START;
        int ibHostName = readU16LE(payload, p); int cchHostName = readU16LE(payload, p + 2); p += 4;
        int ibUserName = readU16LE(payload, p); int cchUserName = readU16LE(payload, p + 2); p += 4;
        int ibPassword = readU16LE(payload, p); int cchPassword = readU16LE(payload, p + 2); p += 4;
        int ibAppName = readU16LE(payload, p); int cchAppName = readU16LE(payload, p + 2); p += 4;
        int ibServerName = readU16LE(payload, p); int cchServerName = readU16LE(payload, p + 2); p += 4;
        p += 4;
        p += 4;
        p += 4;
        int ibDatabase = readU16LE(payload, p); int cchDatabase = readU16LE(payload, p + 2); p += 4;

        String hostName = readUcs2String(payload, ibHostName, cchHostName);
        String userName = readUcs2String(payload, ibUserName, cchUserName);
        String password = cchPassword == 0 ? "" : decodePassword(payload, ibPassword, cchPassword);
        String appName = readUcs2String(payload, ibAppName, cchAppName);
        String serverName = readUcs2String(payload, ibServerName, cchServerName);
        String database = readUcs2String(payload, ibDatabase, cchDatabase);

        return new Credentials(hostName, userName, password, appName, serverName, database, integratedSecurity);
    }

    private static int readU16LE(byte[] data, int pos) {
        return (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8);
    }

    private static String readUcs2String(byte[] data, int byteOffset, int charLength) {
        if (charLength == 0) {
            return "";
        }
        return new String(data, byteOffset, charLength * 2, StandardCharsets.UTF_16LE);
    }

    private static String decodePassword(byte[] data, int byteOffset, int charLength) {
        int byteLen = charLength * 2;
        byte[] decoded = new byte[byteLen];
        for (int i = 0; i < byteLen; i++) {
            int b = data[byteOffset + i] & 0xFF;
            b = b ^ 0xA5;
            b = ((b & 0x0F) << 4) | ((b & 0xF0) >>> 4);
            decoded[i] = (byte) b;
        }
        return new String(decoded, StandardCharsets.UTF_16LE);
    }

    private Login7Handler() {
    }
}
