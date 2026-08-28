package com.nexagres.wire.orawire.ttc;

import java.nio.charset.StandardCharsets;

public final class TtcReader {

    private final byte[] buf;
    private int pos;

    public TtcReader(byte[] buf) {
        this.buf = buf;
    }

    public boolean hasRemaining() {
        return pos < buf.length;
    }

    public int readUint8() {
        return buf[pos++] & 0xFF;
    }

    public int readSb1() {
        return buf[pos++];
    }

    public int readUb2() {
        int v = 0;
        int n = readUint8();
        for (int i = 0; i < n; i++) {
            v = (v << 8) | readUint8();
        }
        return v;
    }

    public long readUb4() {
        long v = 0;
        int n = readUint8();
        for (int i = 0; i < n; i++) {
            v = (v << 8) | readUint8();
        }
        return v;
    }

    public long readUb8() {
        long v = 0;
        int n = readUint8();
        for (int i = 0; i < n; i++) {
            v = (v << 8) | readUint8();
        }
        return v;
    }

    public int readUint16BE() {
        return (readUint8() << 8) | readUint8();
    }

    public long readUint32BE() {
        long v = 0;
        for (int i = 0; i < 4; i++) {
            v = (v << 8) | readUint8();
        }
        return v;
    }

    public byte[] readRawBytes(int n) {
        byte[] out = new byte[n];
        System.arraycopy(buf, pos, out, 0, n);
        pos += n;
        return out;
    }

    public byte[] readRawOrLengthPrefixedBytes(int knownLen) {
        if (knownLen > 0 && knownLen < 256 && hasRemaining() && (buf[pos] & 0xFF) == knownLen) {
            pos++;
        }
        return readRawBytes(knownLen);
    }

    /** Reads {@code len} bytes starting {@code offset} bytes past the current position, WITHOUT
     * consuming anything -- {@link #position()} is unchanged. Lets a caller inspect a candidate
     * read before committing to it (see {@code ExecuteRequestReader#readSqlText}'s own use, which
     * needs to try two different byte offsets and keep whichever one decodes to real SQL). */
    public byte[] peekRawBytes(int offset, int len) {
        byte[] out = new byte[len];
        System.arraycopy(buf, pos + offset, out, 0, len);
        return out;
    }

    public byte[] readRemaining() {
        byte[] out = new byte[buf.length - pos];
        System.arraycopy(buf, pos, out, 0, out.length);
        pos = buf.length;
        return out;
    }

    public byte[] readBytesWithLength() {
        int length = readUint8();
        if (length == 0 || length == TtcConstants.TNS_NULL_LENGTH_INDICATOR) {
            return null;
        }
        if (length == TtcConstants.TNS_LONG_LENGTH_INDICATOR) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            while (true) {
                long chunkLen = readUb4();
                if (chunkLen == 0) {
                    break;
                }
                out.write(readRawBytes((int) chunkLen), 0, (int) chunkLen);
            }
            return out.toByteArray();
        }
        byte[] out = new byte[length];
        System.arraycopy(buf, pos, out, 0, length);
        pos += length;
        return out;
    }

    public String readStrWithLength() {
        byte[] bytes = readBytesWithLength();
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    public void skip(int n) {
        pos += n;
    }

    public void skipBytesWithLength() {
        readBytesWithLength();
    }

    public int position() {
        return pos;
    }

    public int remaining() {
        return buf.length - pos;
    }
}
