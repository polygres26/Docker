package com.polygres.wire.mssqlwire.frontend;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public static byte requestedEncryption(Map<Integer, byte[]> clientOptions) {
        byte[] data = clientOptions.get((int) TOKEN_ENCRYPTION);
        return (data != null && data.length >= 1) ? data[0] : ENCRYPT_NOT_SUPPORTED;
    }

    public static byte[] buildResponse(byte negotiatedEncryption) {
        byte[] versionData = {0x0F, 0x00, 0x00, 0x07, 0x00, 0x00};
        byte[] encryptionData = {negotiatedEncryption};
        byte[] instoptData = {0x00};
        byte[] threadIdData = {0x00, 0x00, 0x00, 0x00};
        byte[] marsData = {0x00};

        byte[][] datas = {versionData, encryptionData, instoptData, threadIdData, marsData};
        byte[] tokens = {TOKEN_VERSION, TOKEN_ENCRYPTION, TOKEN_INSTOPT, TOKEN_THREADID, TOKEN_MARS};

        int headerLen = tokens.length * 5 + 1;
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
