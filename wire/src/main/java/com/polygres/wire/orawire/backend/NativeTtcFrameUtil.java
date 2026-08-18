package com.polygres.wire.orawire.backend;

public final class NativeTtcFrameUtil {

    private static final int TNS_TYPE_DATA = 6;
    private static final int DATA_HEADER_LENGTH = 10;

    private NativeTtcFrameUtil() {
    }

    public static byte[] stripFraming(byte[] raw) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(raw.length);
        int i = 0;
        while (i + 4 <= raw.length) {
            int length = ((raw[i] & 0xFF) << 24) | ((raw[i + 1] & 0xFF) << 16)
                    | ((raw[i + 2] & 0xFF) << 8) | (raw[i + 3] & 0xFF);
            if (length <= 0 || i + length > raw.length) {
                
                break;
            }
            int type = raw[i + 4] & 0xFF;
            int headerLen = (type == TNS_TYPE_DATA) ? DATA_HEADER_LENGTH : 8;
            if (type == TNS_TYPE_DATA && length >= headerLen) {
                out.write(raw, i + headerLen, length - headerLen);
            }
            i += length;
        }
        return out.toByteArray();
    }
}
