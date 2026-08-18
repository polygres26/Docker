package com.polygres.wire.mssqlwire.frontend;

import java.nio.charset.StandardCharsets;

/**
 * LOGIN7 (MS-TDS §2.2.6.4): the actual authentication/login message a TDS client sends right
 * after PRELOGIN. Only SQL auth is in scope here (Windows/Kerberos/SSPI auth is a distinct field
 * layout this pass doesn't parse — see {@code OptionFlags2}'s bit 7 below); enough of the fixed
 * header + variable-length offset/length block is parsed to extract username/password/database
 * and hand off to the session pipeline, the same narrow slice orawire's {@code O5LogonHandler}
 * extracts for Oracle's O5LOGON.
 *
 * <p>Fixed-length prefix (positions per MS-TDS §2.2.6.4, all multi-byte fields little-endian —
 * unlike the TDS packet header itself, which is big-endian):
 *
 * <pre>
 * 0   Length          (4)  total LOGIN7 message length
 * 4   TDSVersion      (4)
 * 8   PacketSize      (4)
 * 12  ClientProgVer   (4)
 * 16  ClientPID       (4)
 * 20  ConnectionID    (4)
 * 24  OptionFlags1    (1)
 * 25  OptionFlags2    (1)  bit7 = fIntSecurity (Windows auth requested — rejected here, see {@link #isIntegratedSecurity})
 * 26  TypeFlags       (1)
 * 27  OptionFlags3    (1)
 * 28  ClientTimZone   (4)
 * 30  ClientLCID      (4)
 * ... 34 onward: repeated (offset:2 LE, length:2 LE-in-characters) pairs for
 *     HostName, UserName, Password, AppName, ServerName, (Extension|Unused), CltIntName,
 *     Language, Database, ClientID(6 raw bytes, no offset/length pair), SSPI, AtchDBFile,
 *     ChangePassword, then a trailing SSPILong(4) length.
 * </pre>
 *
 * Password bytes on the wire are obfuscated (MS-TDS §2.2.6.4's "Password/Change Password" note):
 * each byte is XORed with 0xA5 and then nibble-swapped; {@link #decodePassword} reverses that.
 */
public final class Login7Handler {

    public record Credentials(String hostName, String userName, String password, String appName,
            String serverName, String database, boolean integratedSecurity) {
    }

    private static final int OFFSET_LEN_BLOCK_START = 36; // first offset/length pair (ibHostName)

    public static Credentials parse(byte[] payload) {
        byte optionFlags2 = payload[25];
        boolean integratedSecurity = (optionFlags2 & 0x80) != 0;

        int p = OFFSET_LEN_BLOCK_START;
        int ibHostName = readU16LE(payload, p); int cchHostName = readU16LE(payload, p + 2); p += 4;
        int ibUserName = readU16LE(payload, p); int cchUserName = readU16LE(payload, p + 2); p += 4;
        int ibPassword = readU16LE(payload, p); int cchPassword = readU16LE(payload, p + 2); p += 4;
        int ibAppName = readU16LE(payload, p); int cchAppName = readU16LE(payload, p + 2); p += 4;
        int ibServerName = readU16LE(payload, p); int cchServerName = readU16LE(payload, p + 2); p += 4;
        p += 4; // ibExtension/cbExtension (or ibUnused/cbUnused on older TDS versions) -- unused here
        p += 4; // ibCltIntName/cchCltIntName -- unused
        p += 4; // ibLanguage/cchLanguage -- unused
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
