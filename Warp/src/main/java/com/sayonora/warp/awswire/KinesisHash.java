package com.sayonora.warp.awswire;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/** Kinesis partition-key hashing and shard hash-range math: MD5 of the key read as an unsigned 128-bit integer. */
public final class KinesisHash {

    public static final BigInteger MAX = BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE);
    private static final BigInteger SPACE = BigInteger.ONE.shiftLeft(128);

    private KinesisHash() {
    }

    public static BigInteger hashKey(String partitionKey) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(partitionKey.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, d);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@code n} contiguous ranges covering 0..2^128-1 the way Kinesis lays out a new stream: start_i = floor(i * 2^128 / n). */
    public static List<BigInteger[]> evenRanges(int n) {
        List<BigInteger[]> out = new ArrayList<>();
        BigInteger bn = BigInteger.valueOf(n);
        for (int i = 0; i < n; i++) {
            BigInteger start = SPACE.multiply(BigInteger.valueOf(i)).divide(bn);
            BigInteger end = i == n - 1 ? MAX : SPACE.multiply(BigInteger.valueOf(i + 1)).divide(bn).subtract(BigInteger.ONE);
            out.add(new BigInteger[] {start, end});
        }
        return out;
    }

    /** Fixed 36-digit prefix of a shard's sequence numbers (deterministic per stream and shard). */
    public static String seqPrefix(String stream, String shardId) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest((stream + "/" + shardId).getBytes(StandardCharsets.UTF_8));
            String digits = new BigInteger(1, d).toString();
            return "49" + digits.substring(0, 34);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** A sequence number: the shard prefix followed by the 20-digit zero-padded per-shard counter (so numeric order == counter order). */
    public static String seqNumber(String stream, String shardId, long counter) {
        return seqPrefix(stream, shardId) + String.format("%020d", counter);
    }

    /** The counter of a sequence number produced by {@link #seqNumber}; -1 when it is not one of this shard's. */
    public static long counterOf(String stream, String shardId, String seq) {
        String p = seqPrefix(stream, shardId);
        if (seq == null || seq.length() != p.length() + 20 || !seq.startsWith(p)) {
            return -1;
        }
        try {
            return Long.parseLong(seq.substring(p.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public static String shardId(int index) {
        return String.format("shardId-%012d", index);
    }
}
