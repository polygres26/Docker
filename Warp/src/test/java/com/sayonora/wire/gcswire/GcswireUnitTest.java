package com.sayonora.wire.gcswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.gcswire.GcsModel.Obj;
import com.sayonora.wire.gcswire.GcsModel.Pre;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Pure-logic tests of gcswire: hash encodings, resumable range math, generations, filters/globs, error rendering, MIME parsing. */
class GcswireUnitTest {

    // ---- crc32c / md5 encodings ------------------------------------------------------------------------------

    @Test
    void crc32cIsBase64OfTheBigEndianChecksum() {
        assertEquals("mnG7TA==", GcsHash.crc32c("hello".getBytes(StandardCharsets.UTF_8)));
        // the canonical CRC-32C test vector: 0xE3069283
        assertEquals("4waSgw==", GcsHash.crc32c("123456789".getBytes(StandardCharsets.UTF_8)));
        assertEquals("AAAAAA==", GcsHash.crc32c(new byte[0]));
        assertEquals("4waSgw==", GcsHash.crc32cB64(0xE3069283L));
    }

    @Test
    void md5IsBase64OfTheDigestAndHexForXmlEtags() {
        assertEquals("XUFAKrxLKna5cZ2REBfFkg==", GcsHash.md5B64("hello".getBytes(StandardCharsets.UTF_8)));
        assertEquals("5d41402abc4b2a76b9719d911017c592", GcsHash.md5Hex(GcsHash.b64("XUFAKrxLKna5cZ2REBfFkg==")));
    }

    @Test
    void googHashHeaderRoundTrips() {
        Map<String, String> m = GcsHash.parseGoogHash("crc32c=mnG7TA==, md5=XUFAKrxLKna5cZ2REBfFkg==");
        assertEquals("mnG7TA==", m.get("crc32c"));
        assertEquals("XUFAKrxLKna5cZ2REBfFkg==", m.get("md5"));
        assertEquals("crc32c=mnG7TA==,md5=XUFAKrxLKna5cZ2REBfFkg==", GcsHash.googHash("mnG7TA==", "XUFAKrxLKna5cZ2REBfFkg=="));
        assertEquals("crc32c=mnG7TA==", GcsHash.googHash("mnG7TA==", null));
        assertTrue(GcsHash.parseGoogHash(null).isEmpty());
    }

    @Test
    void etagChangesWithGenerationAndMetageneration() {
        assertFalse(GcsHash.etag(1, 1).equals(GcsHash.etag(1, 2)));
        assertFalse(GcsHash.etag(1, 1).equals(GcsHash.etag(2, 1)));
    }

    @Test
    void hashMismatchIsAnInvalidRequest() {
        GcsException e = assertThrows(GcsException.class, () -> GcsOps.checkHashes(Map.of("crc32c", "AAAAAA=="), "x", "mnG7TA=="));
        assertEquals(400, e.status);
        assertTrue(e.getMessage().contains("CRC32C"));
        GcsOps.checkHashes(Map.of("crc32c", "mnG7TA==", "md5", "m"), "m", "mnG7TA==");
        assertThrows(GcsException.class, () -> GcsOps.checkHashes(Map.of("md5", "a"), "b", "c"));
    }

    // ---- resumable range math --------------------------------------------------------------------------------

    @Test
    void contentRangeParsing() {
        GcsRange.ContentRange c = GcsRange.parseContentRange("bytes 0-262143/*");
        assertEquals(0, c.first());
        assertEquals(262143, c.last());
        assertEquals(-1, c.total());
        assertEquals(262144, c.length());
        assertFalse(c.query());
        c = GcsRange.parseContentRange("bytes 262144-524287/524288");
        assertEquals(524288, c.total());
        c = GcsRange.parseContentRange("bytes */*");
        assertTrue(c.query());
        assertEquals(-1, c.total());
        c = GcsRange.parseContentRange("bytes */1000");
        assertTrue(c.query());
        assertEquals(1000, c.total());
        assertNull(GcsRange.parseContentRange("bytes 5-3/10"));
        assertNull(GcsRange.parseContentRange("bytes 0-10/10"));
        assertNull(GcsRange.parseContentRange("garbage"));
        assertNull(GcsRange.parseContentRange(null));
    }

    @Test
    void onlyTheAlignedPrefixOfANonFinalChunkIsPersisted() {
        assertEquals(262144, GcsRange.persistable(262144, false));
        assertEquals(262144, GcsRange.persistable(300000, false));
        assertEquals(0, GcsRange.persistable(1000, false));
        assertEquals(1000, GcsRange.persistable(1000, true));
        assertEquals(3 * 262144, GcsRange.persistable(3 * 262144 + 5, false));
    }

    @Test
    void rangeHeaderOfA308() {
        assertNull(GcsRange.rangeHeader(0));
        assertEquals("bytes=0-262143", GcsRange.rangeHeader(262144));
        assertEquals("bytes=0-0", GcsRange.rangeHeader(1));
    }

    @Test
    void downloadRanges() {
        assertEquals(new GcsRange.Span(0, 4), GcsRange.parseRange("bytes=0-4", 11));
        assertEquals(new GcsRange.Span(6, 10), GcsRange.parseRange("bytes=6-", 11));
        assertEquals(new GcsRange.Span(6, 10), GcsRange.parseRange("bytes=-5", 11));
        assertEquals(new GcsRange.Span(0, 10), GcsRange.parseRange("bytes=-50", 11));
        assertEquals(new GcsRange.Span(2, 10), GcsRange.parseRange("bytes=2-1000", 11));
        assertEquals(-1, GcsRange.parseRange("bytes=50-60", 11).start());
        assertEquals(-1, GcsRange.parseRange("bytes=-0", 11).start());
        assertNull(GcsRange.parseRange("bytes=0-1,4-5", 11));
        assertNull(GcsRange.parseRange("items=0-1", 11));
        assertNull(GcsRange.parseRange(null, 11));
        assertEquals(-1, GcsRange.parseRange("bytes=0-", 0).start());
    }

    // ---- generations / preconditions --------------------------------------------------------------------------

    private static Obj obj(long gen, long meta) {
        Obj o = new Obj();
        o.bucket = "b";
        o.name = "n";
        o.generation = gen;
        o.metageneration = meta;
        return o;
    }

    @Test
    void ifGenerationMatchZeroMeansMustNotExist() {
        Pre p = new Pre(0L, null, null, null);
        p.check(null);
        GcsException e = assertThrows(GcsException.class, () -> p.check(obj(5, 1)));
        assertEquals(412, e.status);
        assertEquals("conditionNotMet", e.reason);
        assertEquals("ifGenerationMatch", e.location);
    }

    @Test
    void generationAndMetagenerationPreconditions() {
        new Pre(5L, null, null, null).check(obj(5, 1));
        assertThrows(GcsException.class, () -> new Pre(6L, null, null, null).check(obj(5, 1)));
        assertThrows(GcsException.class, () -> new Pre(6L, null, null, null).check(null));
        new Pre(null, 6L, null, null).check(obj(5, 1));
        assertThrows(GcsException.class, () -> new Pre(null, 5L, null, null).check(obj(5, 1)));
        // ifGenerationNotMatch=0: the object must exist
        new Pre(null, 0L, null, null).check(obj(5, 1));
        assertThrows(GcsException.class, () -> new Pre(null, 0L, null, null).check(null));
        new Pre(null, null, 2L, null).check(obj(5, 2));
        assertThrows(GcsException.class, () -> new Pre(null, null, 2L, null).check(obj(5, 3)));
        assertThrows(GcsException.class, () -> new Pre(null, null, 2L, null).check(null));
        new Pre(null, null, null, 2L).check(obj(5, 3));
        assertThrows(GcsException.class, () -> new Pre(null, null, null, 3L).check(obj(5, 3)));
        assertFalse(Pre.NONE.any());
    }

    @Test
    void sessionPreconditionsRoundTripThroughJson() {
        Pre p = new Pre(0L, 7L, 1L, 2L);
        Pre q = GcsResumable.preOf(GcsResumable.preJson(p));
        assertEquals(p, q);
        assertEquals(Pre.NONE, GcsResumable.preOf(GcsResumable.preJson(Pre.NONE)));
    }

    @Test
    void generationsAreMicrosecondTimestampsAndPrintAsStrings() {
        Obj o = obj(1790390493717328L, 3);
        o.created = java.time.Instant.parse("2026-09-26T02:41:33.717Z");
        o.updated = o.created;
        o.classUpdated = o.created;
        o.crc32c = "mnG7TA==";
        o.md5 = "XUFAKrxLKna5cZ2REBfFkg==";
        o.size = 5;
        JsonObject j = GcsJson.object(o, "http://h", null);
        assertEquals("1790390493717328", j.get("generation").getAsString());
        assertEquals("3", j.get("metageneration").getAsString());
        assertEquals("5", j.get("size").getAsString());
        assertEquals("n", j.get("name").getAsString());
        assertEquals("b/n/1790390493717328", j.get("id").getAsString());
        assertEquals("2026-09-26T02:41:33.717Z", j.get("timeCreated").getAsString());
        assertFalse(j.has("timeDeleted"));
        o.deleted = o.created;
        assertTrue(GcsJson.object(o, "http://h", null).has("timeDeleted"));
        assertFalse(o.live());
        // a composite object has no md5Hash but a componentCount
        o.md5 = null;
        o.componentCount = 3;
        JsonObject c = GcsJson.object(o, "http://h", null);
        assertFalse(c.has("md5Hash"));
        assertEquals(3, c.get("componentCount").getAsInt());
    }

    // ---- listing helpers, globs, fields --------------------------------------------------------------------------

    @Test
    void globSyntax() {
        Pattern p = GcsMatch.glob("a/*");
        assertTrue(p.matcher("a/x").matches());
        assertFalse(p.matcher("a/x/y").matches());
        assertTrue(GcsMatch.glob("a/**").matcher("a/x/y").matches());
        assertTrue(GcsMatch.glob("**/1").matcher("a/1").matches());
        assertTrue(GcsMatch.glob("**/1").matcher("a/b/1").matches());
        assertTrue(GcsMatch.glob("**/1").matcher("1").matches());
        assertTrue(GcsMatch.glob("top?").matcher("top1").matches());
        assertFalse(GcsMatch.glob("top?").matcher("top/").matches());
        assertTrue(GcsMatch.glob("{c,z}").matcher("z").matches());
        assertFalse(GcsMatch.glob("{c,z}").matcher("cz").matches());
        assertTrue(GcsMatch.glob("a/[12]").matcher("a/2").matches());
        assertFalse(GcsMatch.glob("a/[12]").matcher("a/3").matches());
        assertTrue(GcsMatch.glob("a/[!12]").matcher("a/3").matches());
        assertTrue(GcsMatch.glob("a.b").matcher("a.b").matches());
        assertFalse(GcsMatch.glob("a.b").matcher("aXb").matches());
        assertTrue(GcsMatch.glob("a\\*b").matcher("a*b").matches());
    }

    @Test
    void prefixEndAndDelimiterRollup() {
        assertEquals("a0", GcsMatch.prefixEnd("a/"));
        assertNull(GcsMatch.prefixEnd(""));
        assertEquals("a/", GcsMatch.rollup("a/x/y", "", "/"));
        assertEquals("a/x/", GcsMatch.rollup("a/x/y", "a/", "/"));
        assertNull(GcsMatch.rollup("a/x", "a/", "/"));
        assertNull(GcsMatch.rollup("a/x", "", null));
        assertEquals("dir/", GcsMatch.rollup("dir/", "", "/"));
    }

    @Test
    void listTokensRoundTripAndRejectGarbage() {
        Obj o = obj(42, 1);
        o.name = "some/name";
        String t = GcsStore.encodeToken(new GcsStore.Entry(false, o.name, o));
        Object[] pos = GcsStore.decodeToken(t);
        assertEquals("some/name", pos[0]);
        assertEquals(42L, pos[1]);
        String tp = GcsStore.encodeToken(new GcsStore.Entry(true, "dir/", null));
        assertEquals("dir0", GcsStore.decodeToken(tp)[0]);
        assertThrows(GcsException.class, () -> GcsStore.decodeToken("!!!bad"));
        assertNull(GcsStore.encodeToken(null));
    }

    @Test
    void partialResponseFields() {
        JsonObject o = JsonParser.parseString("{\"kind\":\"k\",\"name\":\"n\",\"size\":\"3\",\"metadata\":{\"a\":\"1\",\"b\":\"2\"},"
                + "\"items\":[{\"name\":\"x\",\"size\":\"1\"},{\"name\":\"y\",\"size\":\"2\"}]}").getAsJsonObject();
        assertEquals("{\"name\":\"n\",\"size\":\"3\"}", GcsFields.parse("name,size").apply(o).toString());
        assertEquals("{\"metadata\":{\"a\":\"1\"}}", GcsFields.parse("metadata/a").apply(o).toString());
        assertEquals("{\"items\":[{\"name\":\"x\"},{\"name\":\"y\"}]}", GcsFields.parse("items(name)").apply(o).toString());
        assertEquals("{\"kind\":\"k\"}", GcsFields.parse("kind").apply(o).toString());
        assertNull(GcsFields.parse(""));
        assertEquals(o.size(), ((JsonObject) GcsFields.parse("*").apply(o)).size());
    }

    // ---- names -----------------------------------------------------------------------------------------------

    @Test
    void bucketAndObjectNameValidation() {
        GcsOps.validateBucketName("my-bucket_1.data");
        for (String bad : new String[] {"AB", "a", "-bad", "bad-", "goog-x", "my-google-bucket", "192.168.1.1", "a..b", "has space", "UPPER"}) {
            GcsException e = assertThrows(GcsException.class, () -> GcsOps.validateBucketName(bad), bad);
            assertEquals(400, e.status);
        }
        GcsOps.validateObjectName("a/b c/日本.txt");
        for (String bad : new String[] {"", ".", "..", "a\nb", "x".repeat(1025)}) {
            assertThrows(GcsException.class, () -> GcsOps.validateObjectName(bad));
        }
    }

    // ---- error rendering --------------------------------------------------------------------------------------

    @Test
    void jsonErrorEnvelope() {
        JsonObject e = GcsJson.error(GcsException.noSuchObject("b", "o"));
        assertEquals(404, e.getAsJsonObject("error").get("code").getAsInt());
        assertEquals("No such object: b/o", e.getAsJsonObject("error").get("message").getAsString());
        JsonObject one = e.getAsJsonObject("error").getAsJsonArray("errors").get(0).getAsJsonObject();
        assertEquals("notFound", one.get("reason").getAsString());
        assertEquals("global", one.get("domain").getAsString());
        JsonObject pre = GcsJson.error(GcsException.precondition("parameter", "ifGenerationMatch")).getAsJsonObject("error");
        assertEquals(412, pre.get("code").getAsInt());
        JsonObject pe = pre.getAsJsonArray("errors").get(0).getAsJsonObject();
        assertEquals("conditionNotMet", pe.get("reason").getAsString());
        assertEquals("parameter", pe.get("locationType").getAsString());
        assertEquals("ifGenerationMatch", pe.get("location").getAsString());
    }

    @Test
    void xmlErrorDocument() {
        String x = GcsXml.error(GcsException.noSuchObject("b", "o<&>"));
        assertTrue(x.startsWith("<?xml"));
        assertTrue(x.contains("<Code>NoSuchKey</Code>"));
        assertTrue(x.contains("<Message>The specified key does not exist.</Message>"));
        assertTrue(x.contains("<Details>No such object: b/o&lt;&amp;&gt;</Details>"));
        assertTrue(GcsXml.error(GcsException.noSuchBucket()).contains("<Code>NoSuchBucket</Code>"));
        assertEquals("a&amp;b&lt;c&gt;&quot;&apos;", GcsXml.esc("a&b<c>\"'"));
    }

    @Test
    void authenticationErrorsCarryWwwAuthenticate() {
        GcsException e = GcsAuth.invalidCredentials();
        assertEquals(401, e.status);
        assertEquals("Invalid Credentials", e.getMessage());
        assertTrue(e.headers.get("WWW-Authenticate").startsWith("Bearer"));
    }

    // ---- multipart/related body parsing ---------------------------------------------------------------------

    @Test
    void streamingMimeParserSplitsMetadataAndMediaEvenWithBoundaryLikeContent() throws Exception {
        String media = "line1\r\n--xy not a boundary\r\n--x\r\nend";
        String body = "--xx\r\nContent-Type: application/json\r\n\r\n{\"name\":\"n\"}\r\n--xx\r\nContent-Type: text/plain\r\n\r\n" + media
                + "\r\n--xx--\r\n";
        GcsMime m = new GcsMime(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), "xx");
        GcsMime.Part p1 = m.next();
        assertEquals("application/json", p1.headers().get("content-type"));
        assertEquals("{\"name\":\"n\"}", new String(p1.body().readAllBytes(), StandardCharsets.UTF_8));
        GcsMime.Part p2 = m.next();
        assertEquals("text/plain", p2.headers().get("content-type"));
        assertEquals(media, new String(p2.body().readAllBytes(), StandardCharsets.UTF_8));
        assertNull(m.next());
    }

    @Test
    void mimeParserHandlesLargeBodiesAcrossBufferRefills() throws Exception {
        byte[] big = new byte[300_000];
        new java.util.Random(1).nextBytes(big);
        byte[] head = "--B\r\nContent-Type: application/json\r\n\r\n{}\r\n--B\r\nContent-Type: x/y\r\n\r\n".getBytes(StandardCharsets.UTF_8);
        byte[] tail = "\r\n--B--".getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[head.length + big.length + tail.length];
        System.arraycopy(head, 0, all, 0, head.length);
        System.arraycopy(big, 0, all, head.length, big.length);
        System.arraycopy(tail, 0, all, head.length + big.length, tail.length);
        GcsMime m = new GcsMime(new ByteArrayInputStream(all), "B");
        m.next();
        GcsMime.Part media = m.next();
        assertTrue(java.util.Arrays.equals(big, media.body().readAllBytes()));
        assertEquals("B", GcsMime.boundaryOf("multipart/related; boundary=B"));
        assertEquals("a b", GcsMime.boundaryOf("multipart/related; boundary=\"a b\""));
        assertNull(GcsMime.boundaryOf("text/plain"));
    }

    @Test
    void acceptedProjectionAndPredefinedAcls() {
        assertTrue(GcsJson.validPredefined("publicRead", false));
        assertFalse(GcsJson.validPredefined("publicReadWrite", false));
        assertTrue(GcsJson.validPredefined("publicReadWrite", true));
        assertFalse(GcsJson.validPredefined("nonsense", true));
        assertEquals(2, GcsJson.predefined("publicRead", "p", false).size());
        assertEquals(3, GcsJson.predefined(null, "p", false).size());
        assertEquals(List.of("allUsers"), List.of(GcsJson.predefined("publicRead", "p", false).get(1).getAsJsonObject().get("entity").getAsString()));
    }

    @Test
    void reqSegmentsAreDecodedPerSegment() {
        GcsReq r = new GcsReq("GET", "/storage/v1/b/bk/o/a%2Fb%20c.txt", "alt=media&x=a%2Bb", Map.of(), "h:1", "http", new ByteArrayInputStream(new byte[0]));
        assertEquals(List.of("storage", "v1", "b", "bk", "o", "a/b c.txt"), r.segments());
        assertEquals("media", r.q("alt"));
        assertEquals("a+b", r.q("x"));
        assertTrue(GcsService.isJsonPath(r.segments()));
        assertFalse(GcsService.isJsonPath(List.of("bucket", "object")));
        assertEquals("a%2Fb%20c.txt", GcsReq.enc("a/b c.txt"));
    }
}
