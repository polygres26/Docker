package com.polygres.wire.orawire.ttc;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public final class TtcWriter {

    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    public void writeUint8(int v) {
        buf.write(v & 0xFF);
    }

    public void writeSb1(int v) {
        buf.write(v & 0xFF);
    }

    public void writeUb2(int v) {
        writeVarBigEndian(v, 2);
    }

    public void writeUb4(long v) {
        writeVarBigEndian(v, 4);
    }

    public void writeUb8(long v) {
        writeVarBigEndian(v, 8);
    }

    public void writeUint16LE(int v) {
        buf.write(v & 0xFF);
        buf.write((v >> 8) & 0xFF);
    }

    public void writeUint16BE(int v) {
        buf.write((v >> 8) & 0xFF);
        buf.write(v & 0xFF);
    }

    public void writeUint32BE(long v) {
        buf.write((int) ((v >> 24) & 0xFF));
        buf.write((int) ((v >> 16) & 0xFF));
        buf.write((int) ((v >> 8) & 0xFF));
        buf.write((int) (v & 0xFF));
    }

    private void writeVarBigEndian(long v, int maxBytes) {
        if (v == 0) {
            writeUint8(0);
            return;
        }
        byte[] tmp = new byte[maxBytes];
        int n = 0;
        for (int i = maxBytes - 1; i >= 0; i--) {
            byte b = (byte) ((v >>> (8 * i)) & 0xFF);
            if (n > 0 || b != 0) {
                tmp[n++] = b;
            }
        }
        writeUint8(n);
        buf.write(tmp, 0, n);
    }

    public void writeBytesWithLength(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            writeUint8(0);
            return;
        }
        if (bytes.length > TtcConstants.TNS_MAX_SHORT_LENGTH) {
            writeUint8(TtcConstants.TNS_LONG_LENGTH_INDICATOR);
            int offset = 0;
            while (offset < bytes.length) {
                int chunkLen = Math.min(bytes.length - offset, TtcConstants.TNS_CHUNK_SIZE);
                writeUb4(chunkLen);
                buf.write(bytes, offset, chunkLen);
                offset += chunkLen;
            }
            writeUb4(0);
            return;
        }
        writeUint8(bytes.length);
        buf.write(bytes, 0, bytes.length);
    }

    public void writeStrWithLength(String s) {
        writeBytesWithLength(s == null ? null : s.getBytes(StandardCharsets.UTF_8));
    }

    public void writeStrWithTwoLengths(String s) {
        if (s == null) {
            writeUb4(0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeUb4(bytes.length);
        writeBytesWithLength(bytes);
    }

    public void writeRaw(byte[] bytes) {
        buf.write(bytes, 0, bytes.length);
    }

    public byte[] toByteArray() {
        return buf.toByteArray();
    }
}
