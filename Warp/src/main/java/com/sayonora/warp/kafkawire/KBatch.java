package com.sayonora.warp.kafkawire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.GZIPInputStream;

/**
 * A Kafka record batch (message format v2, magic 2) viewed in place. Warp keeps batches opaque: the broker validates the CRC32C, assigns
 * the base offset (bytes 0-7) and the partition leader epoch (bytes 12-15), neither of which the CRC covers, and returns the stored bytes on
 * fetch. Records are only decoded (uncompressed and gzip) for timestamp searches and the MCP tools.
 */
final class KBatch {

    static final int HEADER = 61;
    static final int COMPRESSION_MASK = 0x07;

    final byte[] buf;
    final int off;
    final int len;

    KBatch(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        this.len = len;
    }

    long baseOffset() {
        return getLong(off);
    }

    int batchLength() {
        return getInt(off + 8);
    }

    int magic() {
        return buf[off + 16];
    }

    int crc() {
        return getInt(off + 17);
    }

    int attributes() {
        return ((buf[off + 21] & 0xff) << 8) | (buf[off + 22] & 0xff);
    }

    int compression() {
        return attributes() & COMPRESSION_MASK;
    }

    boolean transactional() {
        return (attributes() & 0x10) != 0;
    }

    boolean control() {
        return (attributes() & 0x20) != 0;
    }

    boolean logAppendTime() {
        return (attributes() & 0x08) != 0;
    }

    int lastOffsetDelta() {
        return getInt(off + 23);
    }

    long firstTimestamp() {
        return getLong(off + 27);
    }

    long maxTimestamp() {
        return getLong(off + 35);
    }

    long producerId() {
        return getLong(off + 43);
    }

    int producerEpoch() {
        return (short) (((buf[off + 51] & 0xff) << 8) | (buf[off + 52] & 0xff));
    }

    int baseSequence() {
        return getInt(off + 53);
    }

    int recordCount() {
        return getInt(off + 57);
    }

    boolean crcOk() {
        CRC32C c = new CRC32C();
        c.update(buf, off + 21, len - 21);
        return (int) c.getValue() == crc();
    }

    private int getInt(int p) {
        return ((buf[p] & 0xff) << 24) | ((buf[p + 1] & 0xff) << 16) | ((buf[p + 2] & 0xff) << 8) | (buf[p + 3] & 0xff);
    }

    private long getLong(int p) {
        return ((long) getInt(p) << 32) | (getInt(p + 4) & 0xffffffffL);
    }

    /** The batch as its own byte array. */
    byte[] copy() {
        byte[] out = new byte[len];
        System.arraycopy(buf, off, out, 0, len);
        return out;
    }

    /** Splits a produce payload into batches; throws CORRUPT_MESSAGE / UNSUPPORTED_FOR_MESSAGE_FORMAT for anything that is not v2. */
    static List<KBatch> parse(byte[] records) {
        List<KBatch> out = new ArrayList<>();
        int p = 0;
        while (p < records.length) {
            if (records.length - p < 12) {
                throw new KafkaError(KafkaError.INVALID_RECORD, "partial batch header");
            }
            int bl = ((records[p + 8] & 0xff) << 24) | ((records[p + 9] & 0xff) << 16) | ((records[p + 10] & 0xff) << 8) | (records[p + 11] & 0xff);
            if (bl < HEADER - 12 || (long) p + 12 + bl > records.length) {
                throw new KafkaError(KafkaError.INVALID_RECORD, "invalid batch length " + bl);
            }
            KBatch b = new KBatch(records, p, 12 + bl);
            if (b.magic() != 2) {
                throw new KafkaError(KafkaError.INVALID_RECORD, "only record batch magic 2 is supported, got " + b.magic());
            }
            out.add(b);
            p += 12 + bl;
        }
        return out;
    }

    // ------------------------------------------------------------------ decoded records (uncompressed and gzip)

    record Rec(long offset, long timestamp, byte[] key, byte[] value, List<byte[][]> headers) {
    }

    /** @return the records, or null when the batch is compressed with snappy, lz4 or zstd (kept opaque). */
    List<Rec> records() {
        byte[] body;
        int bp;
        int bend;
        int comp = compression();
        if (comp == 0) {
            body = buf;
            bp = off + HEADER;
            bend = off + len;
        } else if (comp == 1) {
            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(buf, off + HEADER, len - HEADER))) {
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                in.transferTo(o);
                body = o.toByteArray();
            } catch (IOException e) {
                throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "bad gzip payload");
            }
            bp = 0;
            bend = body.length;
        } else {
            return null;
        }
        int[] pos = {bp};
        List<Rec> out = new ArrayList<>();
        long base = baseOffset();
        long ft = firstTimestamp();
        int n = recordCount();
        for (int i = 0; i < n; i++) {
            int rl = (int) varlong(body, pos);
            int rend = pos[0] + rl;
            pos[0]++; // attributes
            long tsd = varlong(body, pos);
            long od = varlong(body, pos);
            int kl = (int) varlong(body, pos);
            byte[] key = null;
            if (kl >= 0) {
                key = java.util.Arrays.copyOfRange(body, pos[0], pos[0] + kl);
                pos[0] += kl;
            }
            int vl = (int) varlong(body, pos);
            byte[] val = null;
            if (vl >= 0) {
                val = java.util.Arrays.copyOfRange(body, pos[0], pos[0] + vl);
                pos[0] += vl;
            }
            int hc = (int) varlong(body, pos);
            List<byte[][]> hs = new ArrayList<>();
            for (int h = 0; h < hc; h++) {
                int hkl = (int) varlong(body, pos);
                byte[] hk = java.util.Arrays.copyOfRange(body, pos[0], pos[0] + hkl);
                pos[0] += hkl;
                int hvl = (int) varlong(body, pos);
                byte[] hv = null;
                if (hvl >= 0) {
                    hv = java.util.Arrays.copyOfRange(body, pos[0], pos[0] + hvl);
                    pos[0] += hvl;
                }
                hs.add(new byte[][] {hk, hv});
            }
            pos[0] = rend;
            out.add(new Rec(base + od, ft + tsd, key, val, hs));
        }
        return out;
    }

    static long varlong(byte[] b, int[] pos) {
        long v = 0;
        int shift = 0;
        while (true) {
            int x = b[pos[0]++];
            v |= (long) (x & 0x7f) << shift;
            if ((x & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "bad varlong");
            }
        }
        return (v >>> 1) ^ -(v & 1);
    }

    static void putVarlong(ByteArrayOutputStream o, long v) {
        long z = (v << 1) ^ (v >> 63);
        while ((z & ~0x7fL) != 0) {
            o.write((int) ((z & 0x7f) | 0x80));
            z >>>= 7;
        }
        o.write((int) z);
    }

    // ------------------------------------------------------------------ building (MCP produce, tests)

    /** An uncompressed, non-idempotent batch of records ({@code key}/{@code value}/{@code headers} may be null), base offset 0. */
    static byte[] build(List<Rec> recs, long timestamp) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        long maxTs = Long.MIN_VALUE;
        long firstTs = timestamp;
        for (int i = 0; i < recs.size(); i++) {
            Rec r = recs.get(i);
            long ts = r.timestamp() > 0 ? r.timestamp() : timestamp;
            if (i == 0) {
                firstTs = ts;
            }
            maxTs = Math.max(maxTs, ts);
        }
        for (int i = 0; i < recs.size(); i++) {
            Rec r = recs.get(i);
            long ts = r.timestamp() > 0 ? r.timestamp() : timestamp;
            ByteArrayOutputStream rb = new ByteArrayOutputStream();
            rb.write(0);
            putVarlong(rb, ts - firstTs);
            putVarlong(rb, i);
            if (r.key() == null) {
                putVarlong(rb, -1);
            } else {
                putVarlong(rb, r.key().length);
                rb.write(r.key(), 0, r.key().length);
            }
            if (r.value() == null) {
                putVarlong(rb, -1);
            } else {
                putVarlong(rb, r.value().length);
                rb.write(r.value(), 0, r.value().length);
            }
            List<byte[][]> hs = r.headers() == null ? List.of() : r.headers();
            putVarlong(rb, hs.size());
            for (byte[][] h : hs) {
                putVarlong(rb, h[0].length);
                rb.write(h[0], 0, h[0].length);
                if (h[1] == null) {
                    putVarlong(rb, -1);
                } else {
                    putVarlong(rb, h[1].length);
                    rb.write(h[1], 0, h[1].length);
                }
            }
            byte[] rbb = rb.toByteArray();
            putVarlong(body, rbb.length);
            body.write(rbb, 0, rbb.length);
        }
        byte[] rec = body.toByteArray();
        KWriter w = new KWriter(false);
        w.i64(0).i32(HEADER - 12 + rec.length).i32(0).i8(2).i32(0);
        w.i16(0).i32(recs.size() - 1).i64(firstTs).i64(maxTs).i64(-1).i16(-1).i32(-1).i32(recs.size());
        w.raw(rec, 0, rec.length);
        byte[] out = w.toBytes();
        CRC32C c = new CRC32C();
        c.update(out, 21, out.length - 21);
        int crc = (int) c.getValue();
        out[17] = (byte) (crc >> 24);
        out[18] = (byte) (crc >> 16);
        out[19] = (byte) (crc >> 8);
        out[20] = (byte) crc;
        return out;
    }

    static void setBaseOffset(byte[] batch, long base) {
        for (int i = 0; i < 8; i++) {
            batch[i] = (byte) (base >> (56 - 8 * i));
        }
    }

    static void setLeaderEpoch(byte[] batch, int epoch) {
        batch[12] = (byte) (epoch >> 24);
        batch[13] = (byte) (epoch >> 16);
        batch[14] = (byte) (epoch >> 8);
        batch[15] = (byte) epoch;
    }
}
