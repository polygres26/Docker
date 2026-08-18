package com.polygres.wire.mssqlwire.frontend;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PRELOGIN (MS-TDS §2.2.6.4/2.2.6.5): the option-list handshake every TDS client sends before
 * LOGIN7. Wire shape (cross-checked against the public MS-TDS spec; see the {@code mssqlwire}
 * package javadoc for why the Babelfish C source itself couldn't be pinned down within this
 * pass's time budget):
 *
 * <pre>
 * option list: repeated { token(1) offset(2 BE) length(2 BE) } terminated by token 0xFF,
 *              followed immediately by each option's data laid out at those offsets.
 * </pre>
 *
 * Token 0x01 (ENCRYPTION) is the only field this pass's response logic inspects: TLS is out of
 * scope for this round (see the module-level task description), so the server always answers
 * {@code ENCRYPT_NOT_SUPPORTED} (0x02) regardless of what the client requested — a real client
 * (verified live against {@code mssql-jdbc}) accepts that and proceeds with a plaintext LOGIN7
 * rather than failing the handshake, as long as it didn't request {@code ENCRYPT_REQUIRED} (0x01).
 */
public final class PreLoginHandshake {

    private static final byte TOKEN_VERSION = 0x00;
    private static final byte TOKEN_ENCRYPTION = 0x01;
    private static final byte TOKEN_INSTOPT = 0x02;
    private static final byte TOKEN_THREADID = 0x03;
    private static final byte TOKEN_MARS = 0x04;
    private static final byte TOKEN_TERMINATOR = (byte) 0xFF;

    public static final byte ENCRYPT_NOT_SUPPORTED = 0x02;
    public static final byte ENCRYPT_REQUIRED = 0x01;

    private PreLoginHandshake() {
    }

    /** Parsed client PRELOGIN option list, keyed by token byte (unsigned). */
    public static Map<Integer, byte[]> parse(byte[] payload) {
        Map<Integer, byte[]> options = new LinkedHashMap<>();
        int pos = 0;
        while (pos < payload.length) {
            int token = payload[pos] & 0xFF;
            if (token == (TOKEN_TERMINATOR & 0xFF)) {
                break;
            }
            int offset = ((payload[pos + 1] & 0xFF) << 8) | (payload[pos + 2] & 0xFF);
            int length = ((payload[pos + 3] & 0xFF) << 8) | (payload[pos + 4] & 0xFF);
            byte[] data = new byte[length];
            System.arraycopy(payload, offset, data, 0, length);
            options.put(token, data);
            pos += 5;
        }
        return options;
    }

    /** Client-requested encryption mode (ENCRYPTION option, token 0x01), or NOT_SUPPORTED if absent/malformed. */
    public static byte requestedEncryption(Map<Integer, byte[]> clientOptions) {
        byte[] data = clientOptions.get((int) TOKEN_ENCRYPTION);
        return (data != null && data.length >= 1) ? data[0] : ENCRYPT_NOT_SUPPORTED;
    }

    /**
     * Builds the server's PRELOGIN response: VERSION (a made-up but well-formed server version),
     * ENCRYPTION (always NOT_SUPPORTED — see class javadoc), INSTOPT (empty), THREADID (0), MARS
     * (off), terminated by 0xFF.
     */
    public static byte[] buildResponse() {
        byte[] versionData = {0x0F, 0x00, 0x00, 0x07, 0x00, 0x00}; // 15.0.0.1792-ish; value is arbitrary but well-formed
        byte[] encryptionData = {ENCRYPT_NOT_SUPPORTED};
        byte[] instoptData = {0x00}; // empty instance name, NUL-terminated
        byte[] threadIdData = {0x00, 0x00, 0x00, 0x00};
        byte[] marsData = {0x00};

        byte[][] datas = {versionData, encryptionData, instoptData, threadIdData, marsData};
        byte[] tokens = {TOKEN_VERSION, TOKEN_ENCRYPTION, TOKEN_INSTOPT, TOKEN_THREADID, TOKEN_MARS};

        int headerLen = tokens.length * 5 + 1; // + terminator byte
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        int offset = headerLen;
        for (int i = 0; i < tokens.length; i++) {
            header.write(tokens[i]);
            header.write((offset >>> 8) & 0xFF);
            header.write(offset & 0xFF);
            header.write((datas[i].length >>> 8) & 0xFF);
            header.write(datas[i].length & 0xFF);
            body.writeBytes(datas[i]);
            offset += datas[i].length;
        }
        header.write(TOKEN_TERMINATOR);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(header.toByteArray());
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }
}
