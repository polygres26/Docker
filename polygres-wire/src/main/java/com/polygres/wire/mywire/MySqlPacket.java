package com.polygres.wire.mywire;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * MySQL client/server protocol packet framing (3-byte little-endian length +
 * 1-byte sequence id) and the length-encoded int/string primitives used
 * throughout the wire format. Built from MySQL's own public protocol
 * documentation ("MySQL Client/Server Protocol"), not reverse-engineered
 * from any source — same approach as {@code com.polygres.wire.pgwire.PgMessages}.
 */
final class MySqlPacket {

    int sequenceId = -1; // pre-increment in writePayload means the first packet sent gets id 0

    byte[] readPayload(DataInputStream in) throws IOException {
        int len = in.readUnsignedByte() | (in.readUnsignedByte() << 8) | (in.readUnsignedByte() << 16);
        sequenceId = in.readUnsignedByte();
        byte[] payload = new byte[len];
        in.readFully(payload);
        return payload;
    }

    void writePayload(OutputStream out, byte[] payload) throws IOException {
        int len = payload.length;
        out.write(len & 0xFF);
        out.write((len >>> 8) & 0xFF);
        out.write((len >>> 16) & 0xFF);
        out.write(++sequenceId & 0xFF);
        out.write(payload);
        out.flush();
    }

    // ---- length-encoded primitives ----------------------------------------

    static long readLenEncInt(byte[] data, int[] pos) {
        int first = data[pos[0]++] & 0xFF;
        if (first < 0xfb) {
            return first;
        }
        if (first == 0xfc) {
            long v = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8);
            pos[0] += 2;
            return v;
        }
        if (first == 0xfd) {
            long v = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8) | ((data[pos[0] + 2] & 0xFF) << 16);
            pos[0] += 3;
            return v;
        }
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (long) (data[pos[0] + i] & 0xFF) << (8 * i);
        }
        pos[0] += 8;
        return v;
    }

    static String readLenEncString(byte[] data, int[] pos) {
        long len = readLenEncInt(data, pos);
        String s = new String(data, pos[0], (int) len, StandardCharsets.UTF_8);
        pos[0] += (int) len;
        return s;
    }

    static String readNulString(byte[] data, int[] pos) {
        int start = pos[0];
        while (data[pos[0]] != 0) {
            pos[0]++;
        }
        String s = new String(data, start, pos[0] - start, StandardCharsets.UTF_8);
        pos[0]++; // skip NUL
        return s;
    }

    static void writeLenEncInt(ByteArrayOutputStream out, long v) {
        if (v < 0xfb) {
            out.write((int) v);
        } else if (v < 0x10000) {
            out.write(0xfc);
            out.write((int) (v & 0xFF));
            out.write((int) ((v >>> 8) & 0xFF));
        } else if (v < 0x1000000) {
            out.write(0xfd);
            out.write((int) (v & 0xFF));
            out.write((int) ((v >>> 8) & 0xFF));
            out.write((int) ((v >>> 16) & 0xFF));
        } else {
            out.write(0xfe);
            for (int i = 0; i < 8; i++) {
                out.write((int) ((v >>> (8 * i)) & 0xFF));
            }
        }
    }

    static void writeLenEncString(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        writeLenEncInt(out, b.length);
        out.write(b, 0, b.length);
    }

    static void writeNulString(ByteArrayOutputStream out, String s) {
        out.write(s.getBytes(StandardCharsets.UTF_8), 0, s.getBytes(StandardCharsets.UTF_8).length);
        out.write(0);
    }

    static void writeFixedInt(ByteArrayOutputStream out, long v, int bytes) {
        for (int i = 0; i < bytes; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

}
