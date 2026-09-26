package com.sayonora.wire.cqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Tokens as a real Apache Cassandra 5.0.9 computed them (recorded from `SELECT token(k)`). */
class Murmur3Test {

    private static long intKey(int k) {
        return Murmur3.token(CqlType.INT.serialize(k));
    }

    private static long textKey(String s) {
        return Murmur3.token(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void intPartitionKeys() {
        assertEquals(-4069959284402364209L, intKey(1));
        assertEquals(-9157060164899361011L, intKey(23));
        assertEquals(-7509452495886106294L, intKey(5));
        assertEquals(-7492342649816923291L, intKey(28));
        assertEquals(-6715243485458697746L, intKey(10));
        assertEquals(-5477287129830487822L, intKey(16));
    }

    @Test
    void textPartitionKeysCoverTailLengths() {
        assertEquals(-5732932993672985672L, textKey("key5"));
        assertEquals(-2464294509315045284L, textKey("key6"));
        assertEquals(-1018666259631851553L, textKey("key10"));
        assertEquals(-468459073612751032L, textKey("key0"));
        assertEquals(876663562975854161L, textKey("key11"));
    }

    @Test
    void compositePartitionKeysUseCassandrasComponentFraming() {
        Schema.Table t = new Schema.Table("ks", "t", java.util.UUID.randomUUID(), java.util.List.of(
                new Schema.Column("a", CqlType.BIGINT, Schema.Kind.PARTITION_KEY, 0, false),
                new Schema.Column("b", CqlType.BLOB, Schema.Kind.PARTITION_KEY, 1, false),
                new Schema.Column("c", CqlType.UUID_T, Schema.Kind.PARTITION_KEY, 2, false)), new java.util.LinkedHashMap<>(), java.util.List.of(), null);
        byte[] pk = t.partitionBytes(new Object[] {1L, new byte[] {0, (byte) 0xff}, java.util.UUID.fromString("11111111-1111-1111-1111-111111111111")});
        assertEquals(8011033935756728145L, Murmur3.token(pk));
        pk = t.partitionBytes(new Object[] {-1L, new byte[0], java.util.UUID.fromString("22222222-2222-2222-2222-222222222222")});
        assertEquals(2113321060545962400L, Murmur3.token(pk));
        assertEquals(t.partitionValues(pk)[0], -1L);
    }

    @Test
    void minValueIsNeverReturned() {
        // MIN_VALUE is reserved for the ring: the partitioner maps it to MAX_VALUE
        for (int i = 0; i < 2000; i++) {
            assertNotEquals(Long.MIN_VALUE, intKey(i));
        }
    }
}
