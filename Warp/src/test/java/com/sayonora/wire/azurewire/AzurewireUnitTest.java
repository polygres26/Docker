package com.sayonora.wire.azurewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.azurewire.ODataFilter.Val;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure-logic tests of azurewire: SharedKey string-to-sign, SAS validation, OData filters, tokens, XML rendering. */
class AzurewireUnitTest {

    private static final String KEY = AzureConfig.DEV_KEY;
    private static final AzureConfig CFG = new AzureConfig(Map.of(AzureConfig.DEV_ACCOUNT, KEY), null, "tok");

    private static AzReq req(String service, String method, String uri, String query, Map<String, String> headers) {
        return new AzReq(SubRequest.create(null, method, uri, query, headers, new byte[0], "http", "10.1.2.3"), service, CFG, "rid");
    }

    // ---- SharedKey ------------------------------------------------------------------------------------------

    @Test
    void sharedKeyStringToSignFollowsTheDocumentedFieldOrder() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("x-ms-date", "Sat, 01 Jan 2050 00:00:00 GMT");
        h.put("x-ms-version", "2021-08-06");
        h.put("x-ms-blob-type", "BlockBlob");
        h.put("Content-Length", "5");
        h.put("Content-Type", "text/plain");
        AzReq r = req("blob", "PUT", "/devstoreaccount1/c1/b%201", "comp=metadata&z=2&a=1", h);
        String sts = AzureAuth.stringsToSign(r, AzureConfig.DEV_ACCOUNT, false).get(0);
        String expected = "PUT\n\n\n5\n\ntext/plain\n\n\n\n\n\n\n"
                + "x-ms-blob-type:BlockBlob\nx-ms-date:Sat, 01 Jan 2050 00:00:00 GMT\nx-ms-version:2021-08-06\n"
                + "/devstoreaccount1/devstoreaccount1/c1/b%201\na:1\ncomp:metadata\nz:2";
        assertEquals(expected, sts);
    }

    @Test
    void zeroContentLengthIsEmptyInTheStringToSign() {
        AzReq r = req("blob", "GET", "/devstoreaccount1/c1", "restype=container", Map.of("x-ms-date", "d", "Content-Length", "0"));
        String sts = AzureAuth.stringsToSign(r, AzureConfig.DEV_ACCOUNT, false).get(0);
        assertTrue(sts.startsWith("GET\n\n\n\n\n\n\n"), sts);
    }

    @Test
    void sharedKeyLiteAndTableUseTheirOwnShorterForms() {
        Map<String, String> h = Map.of("x-ms-date", "D", "x-ms-version", "v", "Content-Type", "application/json");
        AzReq lite = req("blob", "PUT", "/acct/c/b", "comp=block&blockid=x", h);
        assertEquals("PUT\n\napplication/json\n\nx-ms-date:D\nx-ms-version:v\n/devstoreaccount1/acct/c/b?comp=block",
                AzureAuth.stringsToSign(lite, AzureConfig.DEV_ACCOUNT, true).get(0));
        AzReq table = req("table", "POST", "/devstoreaccount1/mytable", "", h);
        assertEquals("POST\n\napplication/json\nD\n/devstoreaccount1/devstoreaccount1/mytable",
                AzureAuth.stringsToSign(table, AzureConfig.DEV_ACCOUNT, false).get(0));
        assertEquals("D\n/devstoreaccount1/devstoreaccount1/mytable", AzureAuth.stringsToSign(table, AzureConfig.DEV_ACCOUNT, true).get(0));
    }

    @Test
    void hostStyleAddressingDropsTheAccountFromThePath() {
        AzReq r = new AzReq(SubRequest.create(null, "GET", "/c1/b", "", Map.of("Host", "devstoreaccount1.blob.localhost:10000"), new byte[0],
                "http", "1.1.1.1"), "blob", CFG, "rid");
        assertTrue(r.hostStyle);
        assertEquals("devstoreaccount1", r.account);
        assertEquals("c1", r.first());
        assertEquals("b", r.rest());
    }

    @Test
    void wrongSignatureCarriesTheExpectedStringToSign() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("x-ms-date", "D");
        h.put("Authorization", "SharedKey devstoreaccount1:AAAA");
        AzReq r = req("blob", "GET", "/devstoreaccount1/c/b", "", h);
        AzureException e = assertThrows(AzureException.class, () -> new AzureAuth(CFG).authenticate(r, id -> null, "c", "b"));
        assertEquals(403, e.status);
        assertEquals("AuthenticationFailed", e.code);
        assertTrue(e.extra.get("AuthenticationErrorDetail").contains("Server used following string to sign: 'GET\n"));
    }

    @Test
    void correctSignatureIsAcceptedInEitherPathEncoding() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("x-ms-date", "D");
        h.put("x-ms-version", "2021-08-06");
        AzReq probe = req("blob", "GET", "/devstoreaccount1/c/b%20x", "", h);
        String sts = AzureAuth.stringsToSign(probe, AzureConfig.DEV_ACCOUNT, false).get(1); // decoded variant
        h.put("Authorization", "SharedKey devstoreaccount1:" + AzureAuth.signForTest(java.util.Base64.getDecoder().decode(KEY), sts));
        AzReq r = req("blob", "GET", "/devstoreaccount1/c/b%20x", "", h);
        assertEquals(AzureAuth.Kind.KEY, new AzureAuth(CFG).authenticate(r, id -> null, "c", "b x").kind);
    }

    // ---- SAS ------------------------------------------------------------------------------------------------

    private static String iso(Instant t) {
        return t.truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static String sasQuery(String sts, String extra) {
        String sig = AzureAuth.signForTest(java.util.Base64.getDecoder().decode(KEY), sts);
        return extra + "&sig=" + java.net.URLEncoder.encode(sig, StandardCharsets.UTF_8);
    }

    @Test
    void blobServiceSasIsVerifiedForPermissionExpiryAndScope() {
        String se = iso(Instant.now().plusSeconds(3600));
        String sts = String.join("\n", "r", "", se, "/blob/devstoreaccount1/c/b", "", "", "", "2021-08-06", "b", "", "", "", "", "", "", "");
        String q = sasQuery(sts, "sv=2021-08-06&sr=b&sp=r&se=" + se);
        AzReq r = req("blob", "GET", "/devstoreaccount1/c/b", q, Map.of());
        AzureAuth.Result ok = new AzureAuth(CFG).authenticate(r, id -> null, "c", "b");
        assertEquals(AzureAuth.Kind.SAS, ok.kind);
        ok.authorize('o', "r");
        assertThrows(AzureException.class, () -> ok.authorize('o', "w"));
        // the same token on another blob fails the signature
        AzReq other = req("blob", "GET", "/devstoreaccount1/c/other", q, Map.of());
        assertThrows(AzureException.class, () -> new AzureAuth(CFG).authenticate(other, id -> null, "c", "other"));
    }

    @Test
    void expiredSasIsRejected() {
        String se = iso(Instant.now().minusSeconds(60));
        String sts = String.join("\n", "r", "", se, "/queue/devstoreaccount1/q", "", "", "", "2021-08-06");
        AzReq r = req("queue", "GET", "/devstoreaccount1/q/messages", sasQuery(sts, "sv=2021-08-06&sp=r&se=" + se), Map.of());
        AzureException e = assertThrows(AzureException.class, () -> new AzureAuth(CFG).authenticate(r, id -> null, "q", null));
        assertEquals("AuthenticationFailed", e.code);
        assertTrue(e.extra.get("AuthenticationErrorDetail").contains("Signature not valid in the specified time frame"));
    }

    @Test
    void accountSasChecksServiceResourceTypeAndPermission() {
        String se = iso(Instant.now().plusSeconds(3600));
        String sts = AzureAuth.accountSasStringToSign("devstoreaccount1", "rl", "b", "sco", null, se, null, null, "2021-08-06");
        String q = sasQuery(sts, "sv=2021-08-06&ss=b&srt=sco&sp=rl&se=" + se);
        AzureAuth.Result res = new AzureAuth(CFG).authenticate(req("blob", "GET", "/devstoreaccount1/c/b", q, Map.of()), id -> null, "c", "b");
        res.authorize('o', "r");
        assertThrows(AzureException.class, () -> res.authorize('o', "w"));
        AzReq wrongService = req("queue", "GET", "/devstoreaccount1/q/messages", q, Map.of());
        assertThrows(AzureException.class, () -> new AzureAuth(CFG).authenticate(wrongService, id -> null, "q", null));
    }

    @Test
    void ipRangesAndBearerToken() {
        assertTrue(AzureAuth.ipAllowed("10.0.0.1-10.0.0.9", "10.0.0.5"));
        assertFalse(AzureAuth.ipAllowed("10.0.0.1-10.0.0.9", "10.0.1.5"));
        assertTrue(AzureAuth.ipAllowed("1.2.3.4", "1.2.3.4"));
        AzReq r = req("blob", "GET", "/devstoreaccount1", "comp=list", Map.of("Authorization", "Bearer tok"));
        assertEquals(AzureAuth.Kind.BEARER, new AzureAuth(CFG).authenticate(r, id -> null, null, null).kind);
        AzReq bad = req("blob", "GET", "/devstoreaccount1", "comp=list", Map.of("Authorization", "Bearer nope"));
        assertEquals(401, assertThrows(AzureException.class, () -> new AzureAuth(CFG).authenticate(bad, id -> null, null, null)).status);
    }

    // ---- OData -----------------------------------------------------------------------------------------------

    private static java.util.function.Function<String, Val> props(Object... kv) {
        Map<String, Val> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], (Val) kv[i + 1]);
        }
        return m::get;
    }

    @Test
    void oDataFilterGrammar() {
        Val n = new Val("Edm.Int32", 5);
        var p = props("N", n, "S", new Val("Edm.String", "it's"), "F", new Val("Edm.Double", 2.5), "L", new Val("Edm.Int64", 4_000_000_000L),
                "B", new Val("Edm.Boolean", true), "PartitionKey", new Val("Edm.String", "pa"));
        assertTrue(ODataFilter.parse("N eq 5 and S eq 'it''s'").matches(p));
        assertTrue(ODataFilter.parse("N gt 4 or N lt 0").matches(p));
        assertFalse(ODataFilter.parse("not (N eq 5)").matches(p));
        assertTrue(ODataFilter.parse("(N eq 1 or N eq 5) and B").matches(p));
        assertTrue(ODataFilter.parse("F ge 2.5 and F lt 3").matches(p));
        assertTrue(ODataFilter.parse("L gt 3999999999L").matches(p));
        assertTrue(ODataFilter.parse("N eq 5.0").matches(p)); // numeric comparison across Int32 / Double
        assertFalse(ODataFilter.parse("N eq 'x'").matches(p)); // type mismatch never matches
        assertFalse(ODataFilter.parse("Missing eq 1").matches(p));
        assertTrue(ODataFilter.parse("B eq true").matches(p));
        assertEquals(1, ODataFilter.parse("PartitionKey eq 'pa' and N gt 1").bounds().size());
        assertEquals(0, ODataFilter.parse("PartitionKey eq 'pa' or N gt 1").bounds().size());
        assertTrue(ODataFilter.parse("guid'0F8FAD5B-D9CB-469F-A165-70867728950E' eq guid'x'".replace("guid'0F8FAD5B-D9CB-469F-A165-70867728950E' eq guid'x'", "N ge 5")).matches(p));
        assertThrows(ODataFilter.ParseError.class, () -> ODataFilter.parse("N eq"));
        assertThrows(ODataFilter.ParseError.class, () -> ODataFilter.parse("((N eq 1)"));
        assertThrows(ODataFilter.ParseError.class, () -> ODataFilter.parse("N eq 1 and"));
    }

    @Test
    void oDataDateTimeAndBinaryLiterals() {
        var p = props("T", new Val("Edm.DateTime", "2024-05-06T07:08:09.1234567Z"), "X", new Val("Edm.Binary", new byte[] {1, 2, 3}));
        assertTrue(ODataFilter.parse("T gt datetime'2024-01-01T00:00:00Z'").matches(p));
        assertTrue(ODataFilter.parse("T eq datetime'2024-05-06T07:08:09.1234567Z'").matches(p));
        assertTrue(ODataFilter.parse("X eq X'010203'").matches(p));
        assertTrue(ODataFilter.parse("X eq binary'AQID'").matches(p));
    }

    // ---- tokens, tags, xml ---------------------------------------------------------------------------------------

    @Test
    void continuationTokensRoundTripUnicodeKeys() {
        String pk = "pk é ☃/1";
        assertEquals(pk, TableService.unb64(TableService.b64(pk)));
        assertThrows(AzureException.class, () -> TableService.unb64("!!!"));
    }

    @Test
    void tableTimestampsAreSevenDigitAndIncreasing() {
        String a = TableStore.newTs();
        String b = TableStore.newTs();
        assertTrue(a.matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{7}Z"), a);
        assertTrue(b.compareTo(a) > 0);
        TableStore.Ent e = new TableStore.Ent();
        e.ts = a;
        assertTrue(e.etag().startsWith("W/\"datetime'") && e.etag().contains("%3A"));
    }

    @Test
    void blobTagFilterGrammar() {
        Map<String, String> tags = Map.of("env", "prod", "n", "5", "@container", "c1");
        assertTrue(BlobTags.matches("\"env\"='prod' AND @container='c1'", tags));
        assertFalse(BlobTags.matches("\"env\"='dev'", tags));
        assertTrue(BlobTags.matches("\"n\">='5' AND \"n\"<'6'", tags));
        assertThrows(IllegalArgumentException.class, () -> BlobTags.matches("env=prod", tags));
        assertThrows(AzureException.class, () -> BlobTags.validate(Map.of("bad$key", "v")));
    }

    @Test
    void xmlRenderingEscapesAndNestsElements() {
        String x = new AzXml().open("Error").text("Code", "X").text("Message", "a<b & \"c\"").textOrEmpty("Empty", "").close().toString();
        assertEquals("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>X</Code><Message>a&lt;b &amp; &quot;c&quot;</Message><Empty/></Error>", x);
        AzureException e = new AzureException(404, "BlobNotFound", "The specified blob does not exist.");
        String body = AzHttp.xmlError(e, req("blob", "GET", "/a/b", "", Map.of()));
        assertTrue(body.contains("<Code>BlobNotFound</Code>") && body.contains("RequestId:rid"));
    }

    @Test
    void blobPropertiesXmlUsesAzureElementNames() {
        BlobModel.Blob b = new BlobModel.Blob();
        b.etag = "\"0x1\"";
        b.size = 5;
        b.contentType = "text/plain";
        AzXml x = new AzXml().open("Properties");
        BlobService.blobProperties(x, b, Instant.now());
        String s = x.close().toString();
        assertTrue(s.contains("<Content-Length>5</Content-Length>") && s.contains("<BlobType>BlockBlob</BlobType>")
                && s.contains("<LeaseState>available</LeaseState>") && s.contains("<Etag>0x1</Etag>"), s);
    }

    @Test
    void leaseStateMachineExpiresAndBreaks() {
        BlobModel.Lease l = new BlobModel.Lease();
        l.state = "leased";
        l.id = "x";
        l.duration = 15;
        l.expiry = Instant.now().minusSeconds(1);
        assertEquals("expired", l.effective(Instant.now()));
        l.state = "breaking";
        l.breakAt = Instant.now().minusSeconds(1);
        assertEquals("broken", l.effective(Instant.now()));
        assertFalse(l.locked(Instant.now()));
    }

    @Test
    void rangeParsingFollowsAzure() {
        assertEquals(List.of(0L, 4L), toList(BlobService.parseRange("bytes=0-4", 10)));
        assertEquals(List.of(7L, 9L), toList(BlobService.parseRange("bytes=-3", 10)));
        assertEquals(List.of(2L, 9L), toList(BlobService.parseRange("bytes=2-", 10)));
        assertEquals(null, BlobService.parseRange("bytes=0-1,3-4", 10));
        assertEquals(null, BlobService.parseRange("bytes=4-2", 10));
        assertEquals(416, assertThrows(AzureException.class, () -> BlobService.parseRange("bytes=10-12", 10)).status);
    }

    private static List<Long> toList(long[] a) {
        return List.of(a[0], a[1]);
    }
}
