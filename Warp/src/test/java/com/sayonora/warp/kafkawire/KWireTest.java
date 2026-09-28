package com.sayonora.warp.kafkawire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class KWireTest {

    @Test
    void classicRoundTrip() {
        KWriter w = new KWriter(false);
        UUID u = UUID.randomUUID();
        w.i8(-3).bool(true).i16(-2).i32(0x01020304).i64(Long.MIN_VALUE + 5).str("héllo").str(null).bytes(new byte[] {1, 2, 3}).bytes(null).arr(2).i32(7).i32(8).uuid(u);
        KReader r = new KReader(w.toBytes(), 0, w.n, false);
        assertEquals(-3, r.i8());
        assertTrue(r.bool());
        assertEquals(-2, r.i16());
        assertEquals(0x01020304, r.i32());
        assertEquals(Long.MIN_VALUE + 5, r.i64());
        assertEquals("héllo", r.str());
        assertNull(r.str());
        assertArrayEquals(new byte[] {1, 2, 3}, r.bytes());
        assertNull(r.bytes());
        assertEquals(2, r.arr());
        assertEquals(7, r.i32());
        assertEquals(8, r.i32());
        assertEquals(u, r.uuid());
        assertEquals(0, r.remaining());
    }

    @Test
    void flexibleRoundTripWithTaggedFields() {
        KWriter w = new KWriter(true);
        w.str("compact").str(null).bytes(new byte[300]).arr(-1).arr(0).uvarint(300).tagged();
        KReader r = new KReader(w.toBytes(), 0, w.n, true);
        assertEquals("compact", r.str());
        assertNull(r.str());
        assertEquals(300, r.bytes().length);
        assertEquals(-1, r.arr());
        assertEquals(0, r.arr());
        assertEquals(300, r.uvarint());
        r.tagged();
        assertEquals(0, r.remaining());
    }

    @Test
    void taggedFieldsAreSkipped() {
        byte[] body = {2, 9, 3, 'a', 'b', 'c', 5, 1, 'x', 42};
        KReader r = new KReader(body, 0, body.length, true);
        r.tagged();
        assertEquals(-1, r.i8() == 42 ? -1 : 0);
    }

    @Test
    void truncatedRequestsAreRejected() {
        KReader r = new KReader(new byte[] {0, 0, 0}, 0, 3, false);
        assertEquals(KafkaError.CORRUPT_MESSAGE, assertThrows(KafkaError.class, r::i32).code);
        KReader s = new KReader(new byte[] {0, 9, 'a'}, 0, 3, false);
        assertEquals(KafkaError.CORRUPT_MESSAGE, assertThrows(KafkaError.class, s::str).code);
    }

    @Test
    void versionTableIsConsistent() {
        for (int[] a : KafkaBroker.APIS) {
            assertTrue(a[1] <= a[2], "range of api " + a[0]);
            assertTrue(KafkaBroker.supported(a[0], a[1]));
            assertTrue(KafkaBroker.supported(a[0], a[2]));
            assertFalse(KafkaBroker.supported(a[0], a[2] + 1));
            assertFalse(KafkaBroker.supported(a[0], a[1] - 1) && a[1] > 0);
        }
        assertFalse(KafkaBroker.supported(999, 0));
        // flexible boundaries from the Kafka message definitions
        assertFalse(KafkaBroker.flexible(0, 8));
        assertTrue(KafkaBroker.flexible(0, 9));
        assertFalse(KafkaBroker.flexible(1, 11));
        assertTrue(KafkaBroker.flexible(1, 12));
        assertTrue(KafkaBroker.flexible(60, 0));
        assertFalse(KafkaBroker.flexible(17, 1));
        assertFalse(KafkaBroker.flexible(18, 2));
        assertTrue(KafkaBroker.flexible(18, 3));
    }

    @Test
    void topicNameValidation() {
        assertNull(KafkaBroker.validateTopicName("orders.v1_a-b"));
        assertTrue(KafkaBroker.validateTopicName("").contains("empty"));
        assertTrue(KafkaBroker.validateTopicName(".") != null);
        assertTrue(KafkaBroker.validateTopicName("..") != null);
        assertTrue(KafkaBroker.validateTopicName("a b") != null);
        assertTrue(KafkaBroker.validateTopicName("x".repeat(250)) != null);
        assertNull(KafkaBroker.validateTopicName("x".repeat(249)));
    }

    @Test
    void configValidation() {
        assertNull(KafkaConfigs.validate("retention.ms", "3600000"));
        assertNull(KafkaConfigs.validate("retention.ms", "-1"));
        assertTrue(KafkaConfigs.validate("retention.ms", "-2") != null);
        assertTrue(KafkaConfigs.validate("retention.ms", "abc").contains("Not a number of type LONG"));
        assertTrue(KafkaConfigs.validate("no.such.key", "1").contains("Unknown topic config name"));
        assertNull(KafkaConfigs.validate("cleanup.policy", "compact,delete"));
        assertTrue(KafkaConfigs.validate("cleanup.policy", "weird") != null);
        assertTrue(KafkaConfigs.validate("preallocate", "maybe") != null);
        assertNull(KafkaConfigs.validate("compression.type", "zstd"));
        assertTrue(KafkaConfigs.validate("segment.bytes", "3") != null);
        assertEquals(604800000L, KafkaConfigs.longOf(java.util.Map.of(), "retention.ms"));
        assertEquals(5L, KafkaConfigs.longOf(java.util.Map.of("retention.ms", "5"), "retention.ms"));
    }
}
