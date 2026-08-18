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
 * Token 0x01 (ENCRYPTION) is the field this pass's TLS negotiation logic inspects. Real MS-TDS
 * encoding (cross-checked live against {@code mssql-jdbc}'s own {@code TDS.class} constant pool —
 * {@code javap -p -constants} on the shipped 12.10.0 jar — since the public spec's prose is easy
 * to transcribe wrong): {@code ENCRYPT_OFF=0x00}, {@code ENCRYPT_ON=0x01}, {@code
 * ENCRYPT_NOT_SUPPORTED=0x02}, {@code ENCRYPT_REQUIRED=0x03}. (An earlier pass of this file had
 * {@code ENCRYPT_REQUIRED} wrongly aliased to {@code 0x01} — that byte is actually {@code
 * ENCRYPT_ON} — which happened not to matter while the server unconditionally answered {@code
 * NOT_SUPPORTED} either way; it matters now that encryption is actually negotiated.)
 *
 * <p>When {@link com.polygres.wire.server.ServerOptions#tlsEnabled()} and the client requested
 * {@code ENCRYPT_ON} or {@code ENCRYPT_REQUIRED}, the server answers {@code ENCRYPT_ON} and the
 * caller ({@code MssqlWireSessionHandler}) upgrades the socket to TLS in place immediately after
 * this PRELOGIN response is flushed and before LOGIN7 is read — the same in-band-upgrade shape
 * pgwire's {@code SSLRequest}/mywire's {@code CLIENT_SSL} already use (see {@code
 * PgWireSessionHandler#readStartupMessage}'s javadoc for the general pattern this replicates).
 * When TLS isn't configured, the server still answers {@code ENCRYPT_NOT_SUPPORTED} regardless of
 * what the client requested, unless the client hard-requires it ({@code ENCRYPT_REQUIRED}), in
 * which case the handshake is failed cleanly rather than silently served in plaintext.
 */
public final class PreLoginHandshake {

    private static final byte TOKEN_VERSION = 0x00;
    private static final byte TOKEN_ENCRYPTION = 0x01;
    private static final byte TOKEN_INSTOPT = 0x02;
    private static final byte TOKEN_THREADID = 0x03;
    private static final byte TOKEN_MARS = 0x04;
    private static final byte TOKEN_TERMINATOR = (byte) 0xFF;

    public static final byte ENCRYPT_OFF = 0x00;
    public static final byte ENCRYPT_ON = 0x01;
    public static final byte ENCRYPT_NOT_SUPPORTED = 0x02;
    public static final byte ENCRYPT_REQUIRED = 0x03;

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
     * ENCRYPTION (the negotiated byte — see class javadoc), INSTOPT (empty), THREADID (0), MARS
     * (off), terminated by 0xFF.
     */
    public static byte[] buildResponse(byte negotiatedEncryption) {
        byte[] versionData = {0x0F, 0x00, 0x00, 0x07, 0x00, 0x00}; // 15.0.0.1792-ish; value is arbitrary but well-formed
        byte[] encryptionData = {negotiatedEncryption};
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
