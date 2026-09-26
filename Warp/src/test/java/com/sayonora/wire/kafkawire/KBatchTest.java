package com.sayonora.wire.kafkawire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

class KBatchTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void buildParseAndCrc() {
        byte[] raw = KBatch.build(List.of(new KBatch.Rec(0, 1000, b("k1"), b("v1"), List.<byte[][]>of(new byte[][] {b("h"), b("x")})),
                new KBatch.Rec(0, 1005, null, b("v2"), List.of())), 9999);
        List<KBatch> batches = KBatch.parse(raw);
        assertEquals(1, batches.size());
        KBatch k = batches.get(0);
        assertEquals(2, k.magic());
        assertEquals(2, k.recordCount());
        assertEquals(1, k.lastOffsetDelta());
        assertEquals(1000, k.firstTimestamp());
        assertEquals(1005, k.maxTimestamp());
        assertEquals(-1, k.producerId());
        assertTrue(k.crcOk());
        List<KBatch.Rec> recs = k.records();
        assertEquals(2, recs.size());
        assertArrayEquals(b("k1"), recs.get(0).key());
        assertArrayEquals(b("v1"), recs.get(0).value());
        assertEquals("h", new String(recs.get(0).headers().get(0)[0], StandardCharsets.UTF_8));
        assertNull(recs.get(1).key());
        assertEquals(1005, recs.get(1).timestamp());
    }

    @Test
    void baseOffsetAndLeaderEpochAreNotCoveredByTheCrc() {
        byte[] raw = KBatch.build(List.of(new KBatch.Rec(0, 1, null, b("v"), List.of())), 1);
        KBatch.setBaseOffset(raw, 123456789L);
        KBatch.setLeaderEpoch(raw, 7);
        KBatch k = KBatch.parse(raw).get(0);
        assertEquals(123456789L, k.baseOffset());
        assertTrue(k.crcOk());
        assertEquals(123456789L, k.records().get(0).offset());
    }

    @Test
    void corruptionIsDetected() {
        byte[] raw = KBatch.build(List.of(new KBatch.Rec(0, 1, null, b("value"), List.of())), 1);
        raw[raw.length - 2] ^= 0x55;
        assertFalse(KBatch.parse(raw).get(0).crcOk());
    }

    @Test
    void malformedInputIsRejected() {
        byte[] raw = KBatch.build(List.of(new KBatch.Rec(0, 1, null, b("v"), List.of())), 1);
        KafkaError trunc = assertThrows(KafkaError.class, () -> KBatch.parse(java.util.Arrays.copyOf(raw, raw.length - 3)));
        assertEquals(KafkaError.INVALID_RECORD, trunc.code);
        byte[] legacy = raw.clone();
        legacy[16] = 1;
        assertEquals(KafkaError.INVALID_RECORD, assertThrows(KafkaError.class, () -> KBatch.parse(legacy)).code);
        assertEquals(KafkaError.INVALID_RECORD, assertThrows(KafkaError.class, () -> KBatch.parse(new byte[5])).code);
    }

    @Test
    void severalBatchesInOnePayloadAreSplit() {
        byte[] one = KBatch.build(List.of(new KBatch.Rec(0, 1, null, b("a"), List.of())), 1);
        byte[] two = KBatch.build(List.of(new KBatch.Rec(0, 2, null, b("b"), List.of())), 2);
        byte[] both = new byte[one.length + two.length];
        System.arraycopy(one, 0, both, 0, one.length);
        System.arraycopy(two, 0, both, one.length, two.length);
        assertEquals(2, KBatch.parse(both).size());
    }

    @Test
    void gzipBatchesAreDecoded() throws Exception {
        byte[] plain = KBatch.build(List.of(new KBatch.Rec(0, 50, b("k"), b("compressed value"), List.of())), 50);
        byte[] body = java.util.Arrays.copyOfRange(plain, KBatch.HEADER, plain.length);
        ByteArrayOutputStream gz = new ByteArrayOutputStream();
        try (GZIPOutputStream o = new GZIPOutputStream(gz)) {
            o.write(body);
        }
        byte[] head = java.util.Arrays.copyOf(plain, KBatch.HEADER);
        byte[] out = new byte[KBatch.HEADER + gz.size()];
        System.arraycopy(head, 0, out, 0, KBatch.HEADER);
        System.arraycopy(gz.toByteArray(), 0, out, KBatch.HEADER, gz.size());
        out[22] = 1; // attributes low byte: gzip
        int len = out.length - 12;
        out[8] = (byte) (len >> 24);
        out[9] = (byte) (len >> 16);
        out[10] = (byte) (len >> 8);
        out[11] = (byte) len;
        CRC32C c = new CRC32C();
        c.update(out, 21, out.length - 21);
        int crc = (int) c.getValue();
        out[17] = (byte) (crc >> 24);
        out[18] = (byte) (crc >> 16);
        out[19] = (byte) (crc >> 8);
        out[20] = (byte) crc;
        KBatch k = KBatch.parse(out).get(0);
        assertEquals(1, k.compression());
        assertTrue(k.crcOk());
        assertArrayEquals(b("compressed value"), k.records().get(0).value());
        out[22] = 2; // snappy: kept opaque, not decoded
        assertNull(KBatch.parse(out).get(0).records());
    }

    @Test
    void murmur2MatchesKafkasPartitioner() {
        assertEquals(-973932308, KafkaEmbedded.murmur2(b("21")));
        assertEquals(-790332482, KafkaEmbedded.murmur2(b("foobar")));
        assertEquals(-985981536, KafkaEmbedded.murmur2(b("a-little-bit-long-string")));
        assertEquals(-1486304829, KafkaEmbedded.murmur2(b("a-little-bit-longer-string")));
        assertEquals(479470107, KafkaEmbedded.murmur2(new byte[] {'a', 'b', 'c'}));
    }
}
