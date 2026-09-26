package com.sayonora.warp.awswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Partition-key hashing, shard hash ranges and sequence numbers, the way Kinesis defines them. */
class KinesisHashTest {

    @Test
    void partitionKeyHashIsMd5AsUnsigned128BitInteger() {
        // MD5("abc") = 900150983cd24fb0d6963f7d28e17f72
        assertEquals(new BigInteger("900150983cd24fb0d6963f7d28e17f72", 16), KinesisHash.hashKey("abc"));
        assertTrue(KinesisHash.hashKey("anything").compareTo(KinesisHash.MAX) <= 0);
        assertTrue(KinesisHash.hashKey("é中").signum() > 0);
    }

    @Test
    void oneShardCoversEverything() {
        List<BigInteger[]> r = KinesisHash.evenRanges(1);
        assertEquals(BigInteger.ZERO, r.get(0)[0]);
        assertEquals(new BigInteger("340282366920938463463374607431768211455"), r.get(0)[1]);
    }

    @Test
    void threeShardsMatchKinesisLayout() {
        List<BigInteger[]> r = KinesisHash.evenRanges(3);
        assertEquals("0", r.get(0)[0].toString());
        assertEquals("113427455640312821154458202477256070484", r.get(0)[1].toString());
        assertEquals("113427455640312821154458202477256070485", r.get(1)[0].toString());
        assertEquals("226854911280625642308916404954512140969", r.get(1)[1].toString());
        assertEquals("226854911280625642308916404954512140970", r.get(2)[0].toString());
        assertEquals("340282366920938463463374607431768211455", r.get(2)[1].toString());
    }

    @Test
    void rangesAreContiguousAndDisjointForAnyShardCount() {
        for (int n : new int[] {1, 2, 3, 4, 7, 16, 100, 500}) {
            List<BigInteger[]> r = KinesisHash.evenRanges(n);
            assertEquals(n, r.size());
            assertEquals(BigInteger.ZERO, r.get(0)[0]);
            assertEquals(KinesisHash.MAX, r.get(n - 1)[1]);
            for (int i = 1; i < n; i++) {
                assertEquals(r.get(i - 1)[1].add(BigInteger.ONE), r.get(i)[0], "gap or overlap at " + i + " of " + n);
                assertTrue(r.get(i)[0].compareTo(r.get(i)[1]) <= 0);
            }
        }
    }

    @Test
    void everyKeyLandsInExactlyOneShard() {
        List<BigInteger[]> r = KinesisHash.evenRanges(5);
        for (int k = 0; k < 500; k++) {
            BigInteger h = KinesisHash.hashKey("key-" + k);
            int hits = 0;
            for (BigInteger[] range : r) {
                if (h.compareTo(range[0]) >= 0 && h.compareTo(range[1]) <= 0) {
                    hits++;
                }
            }
            assertEquals(1, hits);
        }
    }

    @Test
    void sequenceNumbersAreFixedWidthOrderedAndReversible() {
        String s1 = KinesisHash.seqNumber("stream", "shardId-000000000000", 1);
        String s2 = KinesisHash.seqNumber("stream", "shardId-000000000000", 2);
        String s10 = KinesisHash.seqNumber("stream", "shardId-000000000000", 10);
        assertEquals(56, s1.length());
        assertTrue(s1.compareTo(s2) < 0 && s2.compareTo(s10) < 0, "lexical order == counter order");
        assertTrue(new BigInteger(s1).compareTo(new BigInteger(s2)) < 0);
        assertEquals(10, KinesisHash.counterOf("stream", "shardId-000000000000", s10));
        assertEquals(-1, KinesisHash.counterOf("stream", "shardId-000000000001", s10), "another shard's number is refused");
        assertEquals(-1, KinesisHash.counterOf("stream", "shardId-000000000000", "123"));
        assertEquals("shardId-000000000007", KinesisHash.shardId(7));
    }
}
