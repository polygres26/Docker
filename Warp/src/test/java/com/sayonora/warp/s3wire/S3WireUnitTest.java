package com.sayonora.warp.s3wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.dynamowire.auth.AwsIamCredentialStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/** Pure unit tests (no containers): SigV4 verification against the AWS SDK's own signer, XML, chunking. */
class S3WireUnitTest {

    private static final AwsBasicCredentials CREDS = AwsBasicCredentials.create("AKIDTEST", "sekrit/Value+1");
    private final S3SigV4Verifier verifier =
            new S3SigV4Verifier(AwsIamCredentialStore.parse("AKIDTEST=sekrit/Value+1"));

    /** Signs with the AWS SDK v2 S3 signer (an independent implementation) and verifies with ours. */
    private S3SigV4Verifier.Result signAndVerify(String uri, SdkHttpMethod method, String secretOverride) {
        SdkHttpFullRequest req = SdkHttpFullRequest.builder().method(method).uri(URI.create(uri)).build();
        AwsBasicCredentials c = secretOverride == null ? CREDS : AwsBasicCredentials.create("AKIDTEST", secretOverride);
        SdkHttpFullRequest signed = AwsS3V4Signer.create().sign(req, AwsS3V4SignerParams.builder()
                .awsCredentials(c).signingName("s3").signingRegion(Region.US_EAST_1).doubleUrlEncode(false).build());
        Map<String, List<String>> headers = new HashMap<>();
        signed.headers().forEach((k, v) -> headers.put(k.toLowerCase(), v));
        URI u = signed.getUri();
        return verifier.verify(method.name(), u.getRawPath(), u.getRawQuery(), headers, Instant.now());
    }

    @Test
    void acceptsSdkSignedRequestsIncludingEncodedPathsAndQueries() {
        assertTrue(signAndVerify("http://localhost:1234/bucket/key", SdkHttpMethod.GET, null).valid());
        assertTrue(signAndVerify("http://localhost:1234/bucket/dir/a%20b%C3%A9.txt", SdkHttpMethod.PUT, null).valid());
        assertTrue(signAndVerify("http://localhost:1234/bucket?list-type=2&prefix=a%2Fb%20c&delimiter=%2F&max-keys=5",
                SdkHttpMethod.GET, null).valid());
        assertTrue(signAndVerify("http://localhost:1234/bucket?uploads", SdkHttpMethod.POST, null).valid());
    }

    @Test
    void rejectsWrongSecretUnknownKeyTamperingAndSkew() {
        S3SigV4Verifier.Result bad = signAndVerify("http://localhost:1234/bucket/key", SdkHttpMethod.GET, "other");
        assertFalse(bad.valid());
        assertEquals("SignatureDoesNotMatch", bad.code());

        SdkHttpFullRequest req = SdkHttpFullRequest.builder().method(SdkHttpMethod.GET)
                .uri(URI.create("http://localhost:1234/bucket/key")).build();
        SdkHttpFullRequest signed = AwsS3V4Signer.create().sign(req, AwsS3V4SignerParams.builder().awsCredentials(CREDS)
                .signingName("s3").signingRegion(Region.US_EAST_1).doubleUrlEncode(false).build());
        Map<String, List<String>> headers = new HashMap<>();
        signed.headers().forEach((k, v) -> headers.put(k.toLowerCase(), v));
        // tampered path
        assertEquals("SignatureDoesNotMatch",
                verifier.verify("GET", "/bucket/other", null, headers, Instant.now()).code());
        // tampered method
        assertEquals("SignatureDoesNotMatch",
                verifier.verify("PUT", "/bucket/key", null, headers, Instant.now()).code());
        // clock skew
        assertEquals("RequestTimeTooSkewed",
                verifier.verify("GET", "/bucket/key", null, headers, Instant.now().plusSeconds(3600)).code());
        // no auth at all
        assertEquals("AccessDenied", verifier.verify("GET", "/bucket/key", null,
                Map.of("host", List.of("localhost")), Instant.now()).code());
        // unknown key
        S3SigV4Verifier other = new S3SigV4Verifier(AwsIamCredentialStore.parse("someoneelse=x"));
        assertEquals("InvalidAccessKeyId", other.verify("GET", "/bucket/key", null, headers, Instant.now()).code());
    }

    @Test
    void xmlRenderingEscapesAndShapesListResponses() {
        String xml = S3Xml.listObjects(new S3Xml.ListParams("b", "p/", "/", 1000, true, true, null, "tok", null,
                        null, null, false),
                List.of(new S3Xml.ObjectEntry("p/a&b<c>.txt", Instant.parse("2026-01-02T03:04:05.678Z"), "\"abc\"", 7)),
                List.of("p/sub/"));
        assertTrue(xml.contains("<Key>p/a&amp;b&lt;c&gt;.txt</Key>"));
        assertTrue(xml.contains("<LastModified>2026-01-02T03:04:05.678Z</LastModified>"));
        assertTrue(xml.contains("<KeyCount>2</KeyCount>"));
        assertTrue(xml.contains("<NextContinuationToken>tok</NextContinuationToken>"));
        assertTrue(xml.contains("<CommonPrefixes><Prefix>p/sub/</Prefix></CommonPrefixes>"));
        assertTrue(S3Xml.error("NoSuchKey", "m", "/b/k", "RID", "Key", "k")
                .contains("<Code>NoSuchKey</Code><Message>m</Message><Key>k</Key><Resource>/b/k</Resource>"));
    }

    @Test
    void parsesDeleteAndCompleteMultipartBodiesAndRejectsDoctype() {
        S3Xml.Delete d = S3Xml.parseDelete(("<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Object><Key>a&amp;b</Key></Object><Object><Key>c</Key></Object><Quiet>true</Quiet></Delete>")
                .getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of("a&b", "c"), d.keys());
        assertTrue(d.quiet());
        List<S3Xml.Part> parts = S3Xml.parseCompleteMultipart(("<CompleteMultipartUpload><Part><PartNumber>1"
                + "</PartNumber><ETag>&quot;e1&quot;</ETag></Part></CompleteMultipartUpload>").getBytes(StandardCharsets.UTF_8));
        assertEquals(new S3Xml.Part(1, "\"e1\""), parts.get(0));
        assertThrows(S3WireException.class, () -> S3Xml.parseDelete(
                "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><Delete/>".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void decodesAwsChunkedBodyWithTrailer() throws IOException {
        String framed = "5;chunk-signature=aa\r\nhello\r\n6;chunk-signature=bb\r\n world\r\n"
                + "0;chunk-signature=cc\r\nx-amz-checksum-crc32:AAAA\r\n\r\n";
        byte[] out = new AwsChunkedInputStream(new ByteArrayInputStream(framed.getBytes(StandardCharsets.UTF_8)))
                .readAllBytes();
        assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), out);
    }
}
