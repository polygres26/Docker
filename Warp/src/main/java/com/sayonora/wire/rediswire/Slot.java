package com.sayonora.wire.rediswire;

/** Redis Cluster hash slots: CRC16 (XMODEM) of the key, or of its {hash tag}, modulo 16384. */
final class Slot {

    static final int SLOTS = 16384;
    private static final int[] TABLE = new int[256];

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = (crc & 0x8000) != 0 ? ((crc << 1) ^ 0x1021) : (crc << 1);
            }
            TABLE[i] = crc & 0xffff;
        }
    }

    private Slot() {
    }

    static int crc16(byte[] b, int from, int to) {
        int crc = 0;
        for (int i = from; i < to; i++) {
            crc = ((crc << 8) & 0xffff) ^ TABLE[((crc >> 8) ^ (b[i] & 0xff)) & 0xff];
        }
        return crc;
    }

    /** keyHashSlot from the Redis Cluster specification, including the {hash tag} rule. */
    static int of(byte[] key) {
        int s = -1;
        for (int i = 0; i < key.length; i++) {
            if (key[i] == '{') {
                s = i;
                break;
            }
        }
        if (s >= 0) {
            for (int e = s + 1; e < key.length; e++) {
                if (key[e] == '}') {
                    if (e == s + 1) {
                        break; // empty tag: hash the whole key
                    }
                    return crc16(key, s + 1, e) & (SLOTS - 1);
                }
            }
        }
        return crc16(key, 0, key.length) & (SLOTS - 1);
    }

    /** Index of the shard (of {@code n}) owning {@code slot}: contiguous ranges, like a real cluster. */
    static int shardOf(int slot, int n) {
        return n <= 1 ? 0 : (int) ((long) slot * n / SLOTS);
    }
}
