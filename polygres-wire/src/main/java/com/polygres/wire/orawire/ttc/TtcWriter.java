package com.polygres.wire.orawire.ttc;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Writes TTC primitive types into an in-memory buffer for a single DATA
 * packet payload. Counterpart to {@link TtcReader}; same encoding-shape
 * caveats apply (see that class's javadoc).
 */
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

    /** Fixed-width 2-byte little-endian, used by PROTOCOL/DATA_TYPES (not the variable-length ub2). */
    public void writeUint16LE(int v) {
        buf.write(v & 0xFF);
        buf.write((v >> 8) & 0xFF);
    }

    /** Fixed-width 2-byte big-endian, used by PROTOCOL/DATA_TYPES (not the variable-length ub2). */
    public void writeUint16BE(int v) {
        buf.write((v >> 8) & 0xFF);
        buf.write(v & 0xFF);
    }

    /** Fixed-width 4-byte big-endian, used by CONNECT/ACCEPT (not the variable-length ub4). */
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

    /**
     * bytes_with_length, both forms: short form (values &lt;= TNS_MAX_SHORT_LENGTH: 1-byte
     * length prefix, raw bytes) and long form (sentinel byte TNS_LONG_LENGTH_INDICATOR = 254,
     * then a sequence of (ub4 chunk_length, raw chunk bytes) pairs at TNS_CHUNK_SIZE each,
     * terminated by a ub4 value of 0) — per spec §5.1/§6.
     */
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

    /** Single-length-prefix form (e.g. SQL text via write_bytes_with_length). */
    public void writeStrWithLength(String s) {
        writeBytesWithLength(s == null ? null : s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Double-length-prefix form matching read_str_with_length (outer ub4
     * byte count, then the normal inner writeBytesWithLength encoding of
     * the same bytes) — used by column metadata name/schema/object-name
     * fields (base.pyx:351-353) and O5LOGON KV pair values. Confirmed
     * against a live capture after writeStrWithLength (single-prefix) was
     * initially and incorrectly used for column names, which desynced the
     * whole DESCRIBE_INFO parse on the real client.
     */
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
