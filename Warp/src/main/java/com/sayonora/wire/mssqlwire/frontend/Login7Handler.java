package com.sayonora.wire.mssqlwire.frontend;

import java.nio.charset.StandardCharsets;

public final class Login7Handler {

    public record Credentials(String hostName, String userName, String password, String appName,
            String serverName, String database, boolean integratedSecurity, byte[] sspiBlob) {
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
        p += 4; // ibUnused/cbUnused (reserved -- see MS-TDS 2.2.6.4)
        p += 4; // ibCltIntName/cchCltIntName (client interface library name, e.g. "ODBC")
        p += 4; // ibLanguage/cchLanguage
        int ibDatabase = readU16LE(payload, p); int cchDatabase = readU16LE(payload, p + 2); p += 4;
        p += 6; // ClientID -- 6 raw bytes (a MAC address), not an offset/length pair
        // ibSSPI/cbSSPI -- present regardless of fIntSecurity, but only populated (non-zero) for a
        // real Windows/NTLM/Kerberos login; a plain SQL-auth login always sends cbSSPI=0 here.
        // Real client behavior confirmed live (mssql-jdbc, authenticationScheme=NTLM): carries the
        // raw NTLM Type-1 (Negotiate) message, no TDS wrapper -- see NtlmMessages' javadoc.
        int ibSspi = readU16LE(payload, p); int cbSspi = readU16LE(payload, p + 2); p += 4;

        String hostName = readUcs2String(payload, ibHostName, cchHostName);
        String userName = readUcs2String(payload, ibUserName, cchUserName);
        String password = cchPassword == 0 ? "" : decodePassword(payload, ibPassword, cchPassword);
        String appName = readUcs2String(payload, ibAppName, cchAppName);
        String serverName = readUcs2String(payload, ibServerName, cchServerName);
        String database = readUcs2String(payload, ibDatabase, cchDatabase);
        // cbSSPI == 0xFFFF is TDS's own escape for "the real length is in cbSSPILong instead" (a
        // 4-byte field further along the offset/length block, for an SSPI blob too big for a
        // 16-bit length) -- not read here since no real client sends an initial NTLM Negotiate
        // message anywhere near that size; refusing rather than guessing at that field's own
        // offset, same posture as this codebase's other "exotic case left unhandled" decisions.
        byte[] sspiBlob = (cbSspi == 0 || cbSspi == 0xFFFF)
                ? new byte[0]
                : java.util.Arrays.copyOfRange(payload, ibSspi, ibSspi + cbSspi);

        return new Credentials(hostName, userName, password, appName, serverName, database, integratedSecurity, sspiBlob);
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
