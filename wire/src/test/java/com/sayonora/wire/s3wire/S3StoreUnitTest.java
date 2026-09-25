package com.sayonora.wire.s3wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure unit tests for the Postgres object store's building blocks (no database): key order, chunk math, merge, ranges. */
class S3StoreUnitTest {

    // ---- key ordering / hashing -------------------------------------------------------------------

    @Test
    void compareIsUtf8ByteOrderNotUtf16() {
        // S3 order: 'A' < 'a-b' < 'a.b' < 'a/b' < 'b'  ('-'=0x2d '.'=0x2e '/'=0x2f)
        List<String> keys = new ArrayList<>(List.of("a/b", "a-b", "a.b", "A", "b", "a b", "a+b", "ü", "日本", "z"));
        keys.sort(S3Keys::compare);
        assertEquals(List.of("A", "a b", "a+b", "a-b", "a.b", "a/b", "b", "z", "ü", "日本"), keys);
        // supplementary characters sort AFTER U+FFEE in UTF-8 / code point order, BEFORE it in UTF-16
        String bmp = "￮";
        String supp = "𐀀"; // U+10000
        assertTrue(bmp.compareTo(supp) > 0, "String.compareTo is UTF-16 order");
        assertTrue(S3Keys.compare(bmp, supp) < 0, "S3Keys.compare is code point order");
        // and it agrees with the actual UTF-8 bytes for random strings
        Random r = new Random(1);
        for (int i = 0; i < 2000; i++) {
            String a = randomString(r);
            String b = randomString(r);
            int expected = Integer.signum(Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8),
                    b.getBytes(StandardCharsets.UTF_8)));
            assertEquals(expected, Integer.signum(S3Keys.compare(a, b)), a + " vs " + b);
        }
    }

    private static String randomString(Random r) {
        int[] pool = {'a', 'b', '/', '-', 0xe9, 0x65e5, 0xfffd, 0xffee, 0x10000, 0x1f600, 0x10ffff};
        StringBuilder sb = new StringBuilder();
        for (int i = r.nextInt(5); i > 0; i--) {
            sb.appendCodePoint(pool[r.nextInt(pool.length)]);
        }
        return sb.toString();
    }

    @Test
    void prefixUpperBoundBoundsEveryKeyWithThePrefix() {
        assertEquals("dir0", S3Keys.prefixUpperBound("dir/")); // '/' + 1 = '0'
        assertEquals("b", S3Keys.prefixUpperBound("a"));
        assertNull(S3Keys.prefixUpperBound(""));
        assertNull(S3Keys.prefixUpperBound("􏿿")); // U+10FFFF only
        assertEquals("b", S3Keys.prefixUpperBound("a􏿿")); // trailing max code points dropped
        assertEquals("", S3Keys.prefixUpperBound("퟿")); // skips the surrogate range
        Random r = new Random(2);
        for (int i = 0; i < 500; i++) {
            String p = randomString(r);
            String ub = S3Keys.prefixUpperBound(p);
            for (int j = 0; j < 30; j++) {
                String k = p + randomString(r);
                assertTrue(ub == null || S3Keys.compare(k, ub) < 0, "key with prefix must be below the bound");
                assertTrue(S3Keys.compare(k, p) >= 0);
            }
            if (ub != null && !p.isEmpty()) {
                assertTrue(S3Keys.compare(p, ub) < 0);
                assertFalse(ub.startsWith(p));
            }
        }
    }

    @Test
    void keyValidation() {
        S3Keys.validate("ok/key with spaces+plus%20");
        S3Keys.validate("x".repeat(1024));
        S3Keys.validate("ü".repeat(512));
        assertEquals("KeyTooLongError", assertThrows(S3WireException.class, () -> S3Keys.validate("x".repeat(1025))).code);
        assertEquals("KeyTooLongError", assertThrows(S3WireException.class, () -> S3Keys.validate("ü".repeat(513))).code);
        assertEquals("InvalidArgument", assertThrows(S3WireException.class, () -> S3Keys.validate("a\u0000b")).code);
        assertEquals("InvalidArgument", assertThrows(S3WireException.class, () -> S3Keys.validate("a\uD800b")).code);
        S3Keys.validate("emoji😀");
    }

    @Test
    void shardOwnerIsDeterministicAndSpreadsKeys() {
        List<String> hosts = List.of("default", "pg2");
        int[] counts = new int[2];
        for (int i = 0; i < 400; i++) {
            String a = PostgresObjectStore.ownerOf(hosts, "b", "key-" + i);
            assertEquals(a, PostgresObjectStore.ownerOf(hosts, "b", "key-" + i));
            counts[hosts.indexOf(a)]++;
        }
        assertTrue(counts[0] > 120 && counts[1] > 120, Arrays.toString(counts));
        // a single host owns everything; the shard key separates bucket from key unambiguously
        assertEquals("solo", PostgresObjectStore.ownerOf(List.of("solo"), "b", "k"));
        assertEquals("b/k", S3Keys.shardKey("b", "k"));
        // adding a host changes the owner of many keys (why rebalanceRequired exists)
        List<String> three = List.of("default", "pg2", "pg3");
        int moved = 0;
        for (int i = 0; i < 400; i++) {
            if (!PostgresObjectStore.ownerOf(hosts, "b", "key-" + i).equals(PostgresObjectStore.ownerOf(three, "b", "key-" + i))) {
                moved++;
            }
        }
        assertTrue(moved > 100);
    }

    // ---- chunk math ----------------------------------------------------------------------------------

    private static ChunkMath.Segment seg(String id, long size, int cs) {
        return new ChunkMath.Segment(id, size, cs);
    }

    @Test
    void rangeToChunksSingleSegment() {
        int cs = 4;
        List<ChunkMath.Segment> one = List.of(seg("o", 10, cs)); // chunks: 0-3 | 4-7 | 8-9
        assertEquals(List.of(new ChunkMath.Slice("o", 0, 0, 4), new ChunkMath.Slice("o", 1, 0, 4),
                new ChunkMath.Slice("o", 2, 0, 2)), ChunkMath.slices(one, 0, 9));
        assertEquals(List.of(new ChunkMath.Slice("o", 0, 3, 1), new ChunkMath.Slice("o", 1, 0, 2)),
                ChunkMath.slices(one, 3, 5));
        assertEquals(List.of(new ChunkMath.Slice("o", 1, 0, 1)), ChunkMath.slices(one, 4, 4));
        assertEquals(List.of(new ChunkMath.Slice("o", 2, 1, 1)), ChunkMath.slices(one, 9, 9));
        assertEquals(List.of(), ChunkMath.slices(List.of(seg("e", 0, cs)), 0, -1));
        assertEquals(3, ChunkMath.chunkCount(10, 4));
        assertEquals(0, ChunkMath.chunkCount(0, 4));
        assertEquals(1, ChunkMath.chunkCount(4, 4));
    }

    @Test
    void rangeToChunksAcrossMultipartSegmentsCoversExactlyTheRange() {
        // three parts of unrelated sizes and chunk sizes: boundaries do not align
        List<ChunkMath.Segment> segs = List.of(seg("p1", 11, 4), seg("p2", 6, 4), seg("p3", 3, 8));
        long total = 20;
        Random r = new Random(3);
        for (int i = 0; i < 400; i++) {
            long a = r.nextInt((int) total);
            long b = a + r.nextInt((int) (total - a));
            List<ChunkMath.Slice> slices = ChunkMath.slices(segs, a, b);
            long sum = slices.stream().mapToLong(ChunkMath.Slice::length).sum();
            assertEquals(b - a + 1, sum, a + "-" + b);
            // replay: reconstruct absolute offsets from the slices and compare
            long expectStart = a;
            for (ChunkMath.Slice s : slices) {
                long segBase = 0;
                int csz = 0;
                for (ChunkMath.Segment sg : segs) {
                    if (sg.blobId().equals(s.blobId())) {
                        csz = sg.chunkSize();
                        break;
                    }
                    segBase += sg.size();
                }
                assertEquals(expectStart, segBase + (long) s.seq() * csz + s.offset());
                assertTrue(s.offset() + s.length() <= csz);
                expectStart += s.length();
            }
        }
        // a range entirely inside the middle part touches only that part
        assertEquals(Set.of("p2"), ChunkMath.slices(segs, 12, 14).stream().map(ChunkMath.Slice::blobId)
                .collect(java.util.stream.Collectors.toSet()));
    }

    // ---- listing merge ---------------------------------------------------------------------------------

    private static ListMerge.Entry obj(String k) {
        return new ListMerge.Entry(k, new S3Xml.ObjectEntry(k, Instant.EPOCH, "\"e\"", 1));
    }

    private static ListMerge.Entry pfx(String p) {
        return new ListMerge.Entry(p, null);
    }

    @Test
    void mergeIsKWayOrderedDedupesPrefixesAndTruncates() {
        List<ListMerge.Entry> s1 = List.of(obj("a"), pfx("b/"), obj("d"), obj("f"));
        List<ListMerge.Entry> s2 = List.of(pfx("b/"), obj("c"), pfx("e/"), obj("g"));
        ListMerge.Merged all = ListMerge.merge(List.of(s1, s2), 100);
        assertEquals(List.of("a", "b/", "c", "d", "e/", "f", "g"), all.entries().stream().map(ListMerge.Entry::sortKey).toList());
        assertFalse(all.truncated());
        ListMerge.Merged page = ListMerge.merge(List.of(s1, s2), 3);
        assertEquals(List.of("a", "b/", "c"), page.entries().stream().map(ListMerge.Entry::sortKey).toList());
        assertTrue(page.truncated());
        // exactly max entries and nothing more: not truncated, even though a duplicate prefix follows
        ListMerge.Merged exact = ListMerge.merge(List.of(List.of(obj("a"), pfx("b/")), List.of(pfx("b/"))), 2);
        assertEquals(2, exact.entries().size());
        assertFalse(exact.truncated());
        // UTF-8 order across shards: U+FFEE before U+10000
        ListMerge.Merged utf = ListMerge.merge(List.of(List.of(obj("￮")), List.of(obj("𐀀"))), 10);
        assertEquals("￮", utf.entries().get(0).sortKey());
        assertEquals(0, ListMerge.merge(List.of(List.of(), List.of()), 5).entries().size());
    }

    @Test
    void mergedPagesReconstructTheFullListingForAnyPageSize() {
        Random r = new Random(4);
        List<List<ListMerge.Entry>> shards = new ArrayList<>();
        Set<String> expected = new java.util.TreeSet<>(S3Keys::compare);
        for (int s = 0; s < 3; s++) {
            List<ListMerge.Entry> shard = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                String k = "k" + r.nextInt(500);
                if (shard.stream().noneMatch(e -> e.sortKey().equals(k)) && expected.stream().noneMatch(k::equals)) {
                    shard.add(obj(k));
                    expected.add(k);
                }
            }
            shard.sort((x, y) -> S3Keys.compare(x.sortKey(), y.sortKey()));
            shards.add(shard);
        }
        for (int max : new int[] {1, 2, 7, 50, 1000}) {
            List<String> seen = new ArrayList<>();
            String last = null;
            while (true) {
                final String after = last;
                List<List<ListMerge.Entry>> pages = new ArrayList<>();
                for (List<ListMerge.Entry> shard : shards) { // what listShard does: entries after the marker, limit+1
                    pages.add(shard.stream().filter(e -> after == null || S3Keys.compare(e.sortKey(), after) > 0)
                            .limit(max + 1L).toList());
                }
                ListMerge.Merged m = ListMerge.merge(pages, max);
                m.entries().forEach(e -> seen.add(e.sortKey()));
                if (!m.truncated()) {
                    break;
                }
                last = m.entries().get(m.entries().size() - 1).sortKey();
            }
            assertEquals(new ArrayList<>(expected), seen, "max=" + max);
        }
    }

    @Test
    void continuationTokenRoundTrip() {
        ListMerge.Marker k = ListMerge.decodeToken(ListMerge.encodeToken(obj("some/key with ü 日本 😀")));
        assertEquals(new ListMerge.Marker("some/key with ü 日本 😀", false), k);
        // a common prefix resumes AFTER every key it covers
        ListMerge.Marker p = ListMerge.decodeToken(ListMerge.encodeToken(pfx("dir/")));
        assertEquals(new ListMerge.Marker("dir0", true), p);
        assertNull(ListMerge.decodeToken(ListMerge.encodeToken(pfx("􏿿"))), "nothing can follow the last prefix");
        assertEquals("InvalidArgument", assertThrows(S3WireException.class, () -> ListMerge.decodeToken("@@@")).code);
        assertEquals("InvalidArgument", assertThrows(S3WireException.class, () -> ListMerge.decodeToken("")).code);
        assertNotNull(ListMerge.encodeToken(obj("x")));
        // a v1 marker equal to a previously returned common prefix is recognised
        assertTrue(ListMerge.isRolledUpPrefix("dir/", "", "/"));
        assertTrue(ListMerge.isRolledUpPrefix("a/b/", "a/", "/"));
        assertFalse(ListMerge.isRolledUpPrefix("a/b/c/", "a/", "/"));
        assertFalse(ListMerge.isRolledUpPrefix("dir/x", "", "/"));
        assertFalse(ListMerge.isRolledUpPrefix("dir/", "dir/", "/"));
        assertFalse(ListMerge.isRolledUpPrefix("dir/", "", null));
    }

    // ---- ranges, conditionals, etags, upload ids --------------------------------------------------------

    @Test
    void rangeParsing() {
        assertEquals(new S3Http.ByteRange(0, 9), S3Http.parseRange("bytes=0-9", 100));
        assertEquals(new S3Http.ByteRange(90, 99), S3Http.parseRange("bytes=90-", 100));
        assertEquals(new S3Http.ByteRange(90, 99), S3Http.parseRange("bytes=-10", 100));
        assertEquals(new S3Http.ByteRange(0, 99), S3Http.parseRange("bytes=-500", 100));
        assertEquals(new S3Http.ByteRange(95, 99), S3Http.parseRange("bytes=95-500", 100));
        assertNull(S3Http.parseRange(null, 100));
        assertNull(S3Http.parseRange("bytes=0-1,5-6", 100));
        assertNull(S3Http.parseRange("bytes=5-2", 100));
        assertNull(S3Http.parseRange("items=0-1", 100));
        assertNull(S3Http.parseRange("bytes=abc", 100));
        assertEquals(416, assertThrows(S3WireException.class, () -> S3Http.parseRange("bytes=100-", 100)).status);
        assertEquals(416, assertThrows(S3WireException.class, () -> S3Http.parseRange("bytes=-0", 100)).status);
        assertEquals(416, assertThrows(S3WireException.class, () -> S3Http.parseRange("bytes=0-0", 0)).status);
        assertEquals(10, new S3Http.ByteRange(5, 14).length());
    }

    @Test
    void conditionalRequests() {
        Instant lm = Instant.parse("2026-05-01T10:00:00.900Z");
        String etag = "\"abc\"";
        ObjectStore.Conditions none = ObjectStore.Conditions.NONE;
        assertEquals(200, S3Http.evaluate(none, etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions("\"abc\"", null, null, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions("*", null, null, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions("\"x\", \"abc\"", null, null, null), etag, lm));
        assertEquals(412, S3Http.evaluate(new ObjectStore.Conditions("\"x\"", null, null, null), etag, lm));
        assertEquals(304, S3Http.evaluate(new ObjectStore.Conditions(null, "\"abc\"", null, null), etag, lm));
        assertEquals(304, S3Http.evaluate(new ObjectStore.Conditions(null, "W/\"abc\"", null, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions(null, "\"zzz\"", null, null), etag, lm));
        String same = "Fri, 01 May 2026 10:00:00 GMT";
        String earlier = "Fri, 01 May 2026 09:59:59 GMT";
        assertEquals(304, S3Http.evaluate(new ObjectStore.Conditions(null, null, same, null), etag, lm)); // sub-second ignored
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions(null, null, earlier, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions(null, null, null, same), etag, lm));
        assertEquals(412, S3Http.evaluate(new ObjectStore.Conditions(null, null, null, earlier), etag, lm));
        // If-Match takes precedence over If-Unmodified-Since; If-None-Match and If-Modified-Since are both evaluated
        // (either one answers 304, as S3 / MinIO do; RFC 7232 would let If-None-Match win)
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions("\"abc\"", null, null, earlier), etag, lm));
        assertEquals(304, S3Http.evaluate(new ObjectStore.Conditions(null, "\"zzz\"", same, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions(null, "\"zzz\"", earlier, null), etag, lm));
        assertEquals(200, S3Http.evaluate(new ObjectStore.Conditions(null, null, "garbage", null), etag, lm));
    }

    @Test
    void multipartEtagAndUploadIds() throws Exception {
        byte[] m1 = java.security.MessageDigest.getInstance("MD5").digest("one".getBytes(StandardCharsets.UTF_8));
        byte[] m2 = java.security.MessageDigest.getInstance("MD5").digest("two".getBytes(StandardCharsets.UTF_8));
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
        md.update(m1);
        md.update(m2);
        assertEquals("\"" + S3SigV4Verifier.hex(md.digest()) + "-2\"", S3Http.multipartEtag(List.of(m1, m2)));
        assertArrayEquals16(m1, S3Http.unhex(S3SigV4Verifier.hex(m1)));
        UUID id = UUID.randomUUID();
        String uploadId = S3Http.encodeUploadId("pg-two", id);
        S3Http.UploadRef ref = S3Http.decodeUploadId(uploadId);
        assertEquals("pg-two", ref.host());
        assertEquals(id, ref.id());
        assertEquals("host.with.dots and ü", S3Http.decodeUploadId(S3Http.encodeUploadId("host.with.dots and ü", id)).host());
        assertNull(S3Http.decodeUploadId("bogus"));
        assertNull(S3Http.decodeUploadId(null));
        assertNull(S3Http.decodeUploadId("cGcy." + "z".repeat(32)));
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            ids.add(S3Http.encodeUploadId("h", UUID.randomUUID()));
        }
        assertEquals(100, ids.size());
    }

    private static void assertArrayEquals16(byte[] a, byte[] b) {
        assertEquals(16, b.length);
        org.junit.jupiter.api.Assertions.assertArrayEquals(a, b);
    }

    @Test
    void storeOptionsDefaultsAreTheDocumentedOnes() {
        S3StoreOptions d = S3StoreOptions.defaults();
        assertEquals(4 * 1024 * 1024, d.chunkBytes());
        assertEquals(5L << 30, d.maxObjectBytes());
        assertEquals(60, d.gcIntervalSeconds());
        assertEquals(600, d.gcGraceSeconds());
        assertFalse(d.probeOtherShards());
    }
}
