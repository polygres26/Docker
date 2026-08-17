package com.polygres.wire.orawire.ttc;

import java.nio.charset.StandardCharsets;

/**
 * Reads TTC primitive types out of a single DATA packet's payload bytes.
 * Mirrors ReadBuffer's primitive decoders described in
 * reference/ttc_execute_fetch_spec.md §1/§5.
 *
 * Does not implement long-form chunked reads (length-prefix byte ==
 * TNS_LONG_LENGTH_INDICATOR) — that read-side counterpart is an explicit
 * UNRESOLVED gap in the spec (§7 item 1); values needing it raise.
 *
 * ub2/ub4/ub8 use TTC's standard variable-length integer encoding (a 1-byte
 * length prefix followed by that many big-endian bytes) — the spec cites
 * "write_ub4(...)" call sites but doesn't itself spell out this byte-level
 * shape, so this is carried over from general TTC/Net8 protocol knowledge,
 * not a line-cited part of the reference spec.
 */
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

    /** Fixed-width 2-byte big-endian, used by CONNECT/ACCEPT (not the variable-length ub2). */
    public int readUint16BE() {
        return (readUint8() << 8) | readUint8();
    }

    /** Fixed-width 4-byte big-endian, used by CONNECT/ACCEPT (not the variable-length ub4). */
    public long readUint32BE() {
        long v = 0;
        for (int i = 0; i < 4; i++) {
            v = (v << 8) | readUint8();
        }
        return v;
    }

    /** Reads exactly {@code n} raw bytes with no length prefix of its own — for fields whose
     * length was already given by a preceding SB4/UB4 field (e.g. O5LOGON's username: marshalled
     * via marshalCHR, which is plain bytes, not the length-prefixed bytes_with_length shape —
     * confirmed by decompiling ojdbc11.jar's T4CMAREngine.marshalCHR, not assumed). */
    public byte[] readRawBytes(int n) {
        byte[] out = new byte[n];
        System.arraycopy(buf, pos, out, 0, n);
        pos += n;
        return out;
    }

    /**
     * O5LOGON's username field is encoded differently by different real clients, found live: the
     * real Java thin driver (ojdbc11, Swingbench) writes it as plain raw bytes with the preceding
     * ub4 as its one length; python-oracledb writes it with a second, redundant inner
     * length-prefix byte ahead of the same raw bytes (the shape this server originally assumed,
     * before ojdbc11 testing showed that assumption doesn't hold universally). Both send the same
     * {@code knownLen} in the preceding ub4, so peeking at the very next byte tells them apart:
     * if it equals {@code knownLen} (only possible for the redundant-prefix shape, since raw
     * username content beginning with that exact byte value is not realistically ambiguous here),
     * skip it before reading; otherwise the raw bytes already start at the current position.
     */
    public byte[] readRawOrLengthPrefixedBytes(int knownLen) {
        if (knownLen > 0 && knownLen < 256 && hasRemaining() && (buf[pos] & 0xFF) == knownLen) {
            pos++; // redundant inner length byte — skip it (python-oracledb shape)
        }
        return readRawBytes(knownLen);
    }

    public byte[] readRemaining() {
        byte[] out = new byte[buf.length - pos];
        System.arraycopy(buf, pos, out, 0, out.length);
        pos = buf.length;
        return out;
    }

    /**
     * bytes_with_length, both forms: short form (length prefix 1-252, raw bytes follow) and
     * long form (length prefix == TNS_LONG_LENGTH_INDICATOR = 254, followed by a sequence of
     * (ub4 chunk_length, raw chunk bytes) pairs terminated by a ub4 value of 0) — per spec §5.1/§6.
     * The long-form write side (TtcWriter.writeBytesWithLength) always chunks at
     * TNS_CHUNK_SIZE, but nothing on this read side depends on that chunk size; it just
     * concatenates whatever chunk sizes actually arrive.
     */
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

    /** Skips a length-prefixed byte blob whose contents aren't needed. */
    public void skipBytesWithLength() {
        readBytesWithLength();
    }

    public int position() {
        return pos;
    }
}
