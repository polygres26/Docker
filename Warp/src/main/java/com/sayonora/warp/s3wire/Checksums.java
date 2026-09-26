package com.sayonora.warp.s3wire;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 * S3 additional checksums: CRC32, CRC32C, CRC64NVME, SHA1, SHA256. Values are the Base64 of the big-endian
 * digest, exactly as they travel in {@code x-amz-checksum-*} headers.
 *
 * <p>Multipart semantics (as documented by AWS): a COMPOSITE checksum is the algorithm applied to the
 * concatenated binary part checksums, Base64, suffixed {@code -<parts>}; a FULL_OBJECT checksum (CRC family
 * only) is the checksum of the whole object, computed here by combining the part CRCs with
 * {@link #crcCombine} so no bytes are re-read.
 */
final class Checksums {

    /** Algorithm names as they appear in {@code x-amz-checksum-algorithm} / XML element suffixes. */
    static final List<String> ALL = List.of("CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256");
    private static final long CRC64_POLY = 0x9a6c9329ac4bc9b5L; // reflected CRC-64/NVME
    private static final long[][] CRC64_TABLES = crc64Tables();

    private Checksums() {
    }

    static boolean isCrc(String algo) {
        return "CRC32".equals(algo) || "CRC32C".equals(algo) || "CRC64NVME".equals(algo);
    }

    /** @return canonical upper-case name, or null when unknown */
    static String canonical(String algo) {
        if (algo == null) {
            return null;
        }
        String u = algo.trim().toUpperCase(Locale.ROOT);
        return ALL.contains(u) ? u : null;
    }

    static String headerName(String algo) {
        return "x-amz-checksum-" + algo.toLowerCase(Locale.ROOT);
    }

    static String xmlName(String algo) {
        return "Checksum" + algo;
    }

    /** Reads {@code x-amz-checksum-*} header values (claimed by the client) from a lower-cased header lookup. */
    static Map<String, String> claimedFrom(java.util.function.Function<String, String> header) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String a : ALL) {
            String v = header.apply(headerName(a));
            if (v != null && !v.isBlank()) {
                out.put(a, v.trim());
            }
        }
        return out;
    }

    /** Streaming accumulator over a set of algorithms. */
    static final class Acc {
        private final Map<String, Object> state = new LinkedHashMap<>();
        private long length;

        Acc(Set<String> algos) {
            for (String a : ALL) {
                if (algos.contains(a)) {
                    state.put(a, newState(a));
                }
            }
        }

        boolean isEmpty() {
            return state.isEmpty();
        }

        void update(byte[] b, int off, int len) {
            length += len;
            for (Map.Entry<String, Object> e : state.entrySet()) {
                Object s = e.getValue();
                if (s instanceof Checksum c) {
                    c.update(b, off, len);
                } else if (s instanceof Crc64 c) {
                    c.update(b, off, len);
                } else {
                    ((MessageDigest) s).update(b, off, len);
                }
            }
        }

        /** algo -> Base64 value */
        Map<String, String> finish() {
            Map<String, String> out = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : state.entrySet()) {
                Object s = e.getValue();
                byte[] raw;
                if (s instanceof CRC32 || s instanceof CRC32C) {
                    raw = be(((Checksum) s).getValue(), 4);
                } else if (s instanceof Crc64 c) {
                    raw = be(c.value(), 8);
                } else {
                    raw = ((MessageDigest) s).digest();
                }
                out.put(e.getKey(), Base64.getEncoder().encodeToString(raw));
            }
            return out;
        }
    }

    private static Object newState(String algo) {
        return switch (algo) {
            case "CRC32" -> new CRC32();
            case "CRC32C" -> new CRC32C();
            case "CRC64NVME" -> new Crc64();
            case "SHA1" -> digest("SHA-1");
            default -> digest("SHA-256");
        };
    }

    private static MessageDigest digest(String name) {
        try {
            return MessageDigest.getInstance(name);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] be(long v, int bytes) {
        byte[] out = new byte[bytes];
        for (int i = 0; i < bytes; i++) {
            out[i] = (byte) (v >>> (8 * (bytes - 1 - i)));
        }
        return out;
    }

    static long fromBe(byte[] b) {
        long v = 0;
        for (byte x : b) {
            v = (v << 8) | (x & 0xff);
        }
        return v;
    }

    /** Expected raw length in bytes of a Base64-decoded value of {@code algo}. */
    static int rawLength(String algo) {
        return switch (algo) {
            case "CRC32", "CRC32C" -> 4;
            case "CRC64NVME" -> 8;
            case "SHA1" -> 20;
            default -> 32;
        };
    }

    /** @return true when {@code value} is valid Base64 of the right length for {@code algo} */
    static boolean wellFormed(String algo, String value) {
        try {
            return Base64.getDecoder().decode(value).length == rawLength(algo);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // ---- multipart --------------------------------------------------------------------------------

    /** {@code base64(algo(concat(binary part checksums))) + "-" + n}. */
    static String composite(String algo, List<String> partValues) {
        java.io.ByteArrayOutputStream joined = new java.io.ByteArrayOutputStream();
        for (String v : partValues) {
            joined.writeBytes(Base64.getDecoder().decode(v));
        }
        Acc acc = new Acc(Set.of(algo));
        byte[] all = joined.toByteArray();
        acc.update(all, 0, all.length);
        return acc.finish().get(algo) + "-" + partValues.size();
    }

    /** FULL_OBJECT checksum of parts {@code (crc_i, size_i)} in order. */
    static String fullObject(String algo, List<String> partValues, List<Long> partSizes) {
        int bytes = rawLength(algo);
        long acc = fromBe(Base64.getDecoder().decode(partValues.get(0)));
        for (int i = 1; i < partValues.size(); i++) {
            long next = fromBe(Base64.getDecoder().decode(partValues.get(i)));
            acc = crcCombine(algo, acc, next, partSizes.get(i));
        }
        return Base64.getEncoder().encodeToString(be(acc, bytes));
    }

    // ---- CRC combine (zlib's gf2 method, generalised to 32/64 bit reflected polynomials) -----------

    private static long poly(String algo) {
        return switch (algo) {
            case "CRC32" -> 0xedb88320L;
            case "CRC32C" -> 0x82f63b78L;
            default -> CRC64_POLY;
        };
    }

    private static long times(long[] mat, long vec) {
        long sum = 0;
        int i = 0;
        while (vec != 0) {
            if ((vec & 1) != 0) {
                sum ^= mat[i];
            }
            vec >>>= 1;
            i++;
        }
        return sum;
    }

    private static void square(long[] sq, long[] mat) {
        for (int n = 0; n < sq.length; n++) {
            sq[n] = times(mat, mat[n]);
        }
    }

    /** CRC of (A || B) from crc(A), crc(B) and len(B). */
    static long crcCombine(String algo, long crc1, long crc2, long len2) {
        if (len2 <= 0) {
            return crc1;
        }
        int bits = "CRC64NVME".equals(algo) ? 64 : 32;
        long[] even = new long[bits];
        long[] odd = new long[bits];
        odd[0] = poly(algo);
        long row = 1;
        for (int n = 1; n < bits; n++) {
            odd[n] = row;
            row <<= 1;
        }
        square(even, odd);
        square(odd, even);
        do {
            square(even, odd);
            if ((len2 & 1) != 0) {
                crc1 = times(even, crc1);
            }
            len2 >>>= 1;
            if (len2 == 0) {
                break;
            }
            square(odd, even);
            if ((len2 & 1) != 0) {
                crc1 = times(odd, crc1);
            }
            len2 >>>= 1;
        } while (len2 != 0);
        return crc1 ^ crc2;
    }

    // ---- CRC-64/NVME (slicing-by-8) -----------------------------------------------------------------

    private static long[][] crc64Tables() {
        long[][] t = new long[8][256];
        for (int i = 0; i < 256; i++) {
            long c = i;
            for (int j = 0; j < 8; j++) {
                c = (c & 1) != 0 ? (c >>> 1) ^ CRC64_POLY : c >>> 1;
            }
            t[0][i] = c;
        }
        for (int i = 0; i < 256; i++) {
            long c = t[0][i];
            for (int k = 1; k < 8; k++) {
                c = t[0][(int) (c & 0xff)] ^ (c >>> 8);
                t[k][i] = c;
            }
        }
        return t;
    }

    static final class Crc64 {
        private long crc = -1L;

        void update(byte[] b, int off, int len) {
            long c = crc;
            long[][] t = CRC64_TABLES;
            int i = off;
            int end = off + len;
            while (end - i >= 8) {
                long v = c ^ ((b[i] & 0xffL) | (b[i + 1] & 0xffL) << 8 | (b[i + 2] & 0xffL) << 16
                        | (b[i + 3] & 0xffL) << 24 | (b[i + 4] & 0xffL) << 32 | (b[i + 5] & 0xffL) << 40
                        | (b[i + 6] & 0xffL) << 48 | (b[i + 7] & 0xffL) << 56);
                c = t[7][(int) (v & 0xff)] ^ t[6][(int) ((v >>> 8) & 0xff)] ^ t[5][(int) ((v >>> 16) & 0xff)]
                        ^ t[4][(int) ((v >>> 24) & 0xff)] ^ t[3][(int) ((v >>> 32) & 0xff)]
                        ^ t[2][(int) ((v >>> 40) & 0xff)] ^ t[1][(int) ((v >>> 48) & 0xff)] ^ t[0][(int) (v >>> 56)];
                i += 8;
            }
            while (i < end) {
                c = t[0][(int) ((c ^ b[i]) & 0xff)] ^ (c >>> 8);
                i++;
            }
            crc = c;
        }

        long value() {
            return ~crc;
        }
    }

    /** One-shot helper (tests). */
    static Map<String, String> of(byte[] data, String... algos) {
        Acc a = new Acc(Set.of(algos));
        a.update(data, 0, data.length);
        return a.finish();
    }

    static List<String> sortedAlgos(Map<String, String> m) {
        List<String> out = new ArrayList<>();
        for (String a : ALL) {
            if (m.containsKey(a)) {
                out.add(a);
            }
        }
        return out;
    }
}
