package com.sayonora.wire.s3wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Pure unit tests for the S3 features layered on the Postgres store: checksums, config documents, addressing, forms, select. */
class S3ConformanceUnitTest {

    private static String code(Runnable r) {
        return assertThrows(S3WireException.class, r::run).code;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- checksums -----------------------------------------------------------------------------------

    @Test
    void checksumsMatchTheStandardCheckValues() {
        Map<String, String> m = Checksums.of(utf8("123456789"), "CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256");
        assertEquals(Base64.getEncoder().encodeToString(new byte[] {(byte) 0xcb, (byte) 0xf4, 0x39, 0x26}), m.get("CRC32"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[] {(byte) 0xe3, 0x06, (byte) 0x92, (byte) 0x83}), m.get("CRC32C"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[] {(byte) 0xae, (byte) 0x8b, 0x14, (byte) 0x86, 0x0a,
                0x79, (byte) 0x98, (byte) 0x88}), m.get("CRC64NVME")); // CRC-64/NVME check value 0xae8b14860a799888
        assertEquals("nrtL5Yjl3M8SLkvXvW6aYRoZoNA=".length(), m.get("SHA1").length());
        assertTrue(Checksums.wellFormed("SHA256", m.get("SHA256")));
        assertFalse(Checksums.wellFormed("CRC32", m.get("SHA256")));
        assertFalse(Checksums.wellFormed("CRC32", "not base64!"));
    }

    @Test
    void crcCombineEqualsTheCrcOfTheConcatenation() {
        Random rnd = new Random(7);
        for (String algo : List.of("CRC32", "CRC32C", "CRC64NVME")) {
            for (int trial = 0; trial < 20; trial++) {
                byte[] a = new byte[rnd.nextInt(5000) + 1];
                byte[] b = new byte[rnd.nextInt(5000) + 1];
                byte[] c = new byte[rnd.nextInt(5000) + 1];
                rnd.nextBytes(a);
                rnd.nextBytes(b);
                rnd.nextBytes(c);
                byte[] all = new byte[a.length + b.length + c.length];
                System.arraycopy(a, 0, all, 0, a.length);
                System.arraycopy(b, 0, all, a.length, b.length);
                System.arraycopy(c, 0, all, a.length + b.length, c.length);
                String full = Checksums.fullObject(algo, List.of(Checksums.of(a, algo).get(algo), Checksums.of(b, algo).get(algo),
                        Checksums.of(c, algo).get(algo)), List.of((long) a.length, (long) b.length, (long) c.length));
                assertEquals(Checksums.of(all, algo).get(algo), full, algo);
            }
        }
    }

    @Test
    void compositeIsTheDigestOfTheConcatenatedPartDigestsWithACountSuffix() throws Exception {
        byte[] p1 = utf8("part one");
        byte[] p2 = utf8("part two");
        String c = Checksums.composite("SHA256", List.of(Checksums.of(p1, "SHA256").get("SHA256"), Checksums.of(p2, "SHA256").get("SHA256")));
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] joined = new byte[64];
        System.arraycopy(md.digest(p1), 0, joined, 0, 32);
        System.arraycopy(md.digest(p2), 0, joined, 32, 32);
        assertEquals(Base64.getEncoder().encodeToString(md.digest(joined)) + "-2", c);
    }

    @Test
    void claimedChecksumsAreComparedWithTheComputedOnes() {
        Map<String, String> computed = Checksums.of(utf8("hello"), "CRC32");
        assertNull(PostgresObjectStore.checkClaimed(computed, Map.of("CRC32", computed.get("CRC32"))));
        assertEquals("BadDigest", PostgresObjectStore.checkClaimed(computed, Map.of("CRC32", "AAAAAA==")).code);
        assertEquals("InvalidRequest", PostgresObjectStore.checkClaimed(computed, Map.of("CRC32", "###")).code);
        assertEquals(Set.of("CRC64NVME"), PostgresObjectStore.effectiveAlgos(Set.of(), null)); // S3's default
        assertEquals(Set.of("SHA256", "CRC32"), PostgresObjectStore.effectiveAlgos(Set.of("SHA256"), "CRC32"));
    }

    // ---- versions -------------------------------------------------------------------------------------

    @Test
    void versionIdsAreOpaqueAndValidated() {
        String v = PostgresObjectStore.newVersionId();
        assertEquals(32, v.length());
        assertTrue(v.matches("[A-Za-z0-9_-]{32}"));
        assertFalse(v.equals(PostgresObjectStore.newVersionId()));
        PostgresObjectStore.checkVersionId(v);
        PostgresObjectStore.checkVersionId("null");
        PostgresObjectStore.checkVersionId(null);
        assertEquals("InvalidArgument", code(() -> PostgresObjectStore.checkVersionId("not valid!")));
        assertEquals("null", PostgresObjectStore.ext(null));
        assertEquals(v, PostgresObjectStore.ext(v));
    }

    // ---- tagging ----------------------------------------------------------------------------------------

    @Test
    void tagHeaderParsingFollowsS3() {
        assertEquals(Map.of("env", "prod", "team", "core dev"), S3Cfg.parseTagHeader("env=prod&team=core%20dev"));
        assertTrue(S3Cfg.parseTagHeader("").isEmpty());
        assertEquals("InvalidArgument", code(() -> S3Cfg.parseTagHeader("novalue")));
        assertEquals("InvalidArgument", code(() -> S3Cfg.parseTagHeader("=v")));
        assertEquals("InvalidArgument", code(() -> S3Cfg.parseTagHeader("a=1&a=2")));
        StringBuilder eleven = new StringBuilder();
        for (int i = 0; i < 11; i++) {
            eleven.append(i > 0 ? "&" : "").append("k").append(i).append("=v");
        }
        assertEquals("BadRequest", code(() -> S3Cfg.parseTagHeader(eleven.toString())));
        assertEquals("InvalidArgument", code(() -> S3Cfg.parseTagHeader("k=" + "v".repeat(9000))));
        assertEquals("InvalidTag", code(() -> S3Cfg.parseTagHeader("aws:reserved=1")));
        assertEquals("InvalidTag", code(() -> S3Cfg.parseTagHeader("k=" + "v".repeat(257))));
    }

    @Test
    void taggingXmlParsingAndRendering() {
        byte[] xml = utf8("<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>b</Key><Value>&amp;</Value></Tag></TagSet></Tagging>");
        Map<String, String> tags = S3Cfg.parseTaggingXml(xml, 10);
        assertEquals(Map.of("a", "1", "b", "&"), tags);
        assertTrue(S3Cfg.taggingXml(tags).contains("<Value>&amp;</Value>"));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseTaggingXml(utf8("<Tagging>"), 10)));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseTaggingXml(utf8("<Other/>"), 10)));
        assertEquals("InvalidTag", code(() -> S3Cfg.parseTaggingXml(utf8("<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>"
                + "<Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>"), 10)));
        assertEquals("BadRequest", code(() -> S3Cfg.parseTaggingXml(xml, 1)));
    }

    // ---- versioning / ACL / PAB / ownership ---------------------------------------------------------------

    @Test
    void versioningDocuments() {
        assertEquals("Enabled", S3Cfg.parseVersioning(utf8("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")));
        assertEquals("Suspended", S3Cfg.parseVersioning(utf8("<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Status>Suspended</Status></VersioningConfiguration>")));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseVersioning(utf8("<VersioningConfiguration><Status>Nope</Status></VersioningConfiguration>"))));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseVersioning(utf8("<VersioningConfiguration/>"))));
        assertFalse(S3Cfg.versioningXml(null, false).contains("Status"));
        assertTrue(S3Cfg.versioningXml("Enabled", false).contains("<Status>Enabled</Status>"));
    }

    @Test
    void aclDocumentsFromCannedGrantHeadersAndBodies() {
        Function<String, String> canned = n -> n.equals("x-amz-acl") ? "public-read" : null;
        String acl = S3Cfg.aclFromRequest(canned, null);
        assertTrue(acl.contains("FULL_CONTROL") && acl.contains(S3Cfg.ALL_USERS) && acl.contains("<Permission>READ</Permission>"));
        assertTrue(S3Cfg.aclIsPublic(acl));
        assertFalse(S3Cfg.aclIsPublic(S3Cfg.defaultAclXml()));
        Function<String, String> grant = n -> n.equals("x-amz-grant-read") ? "uri=\"" + S3Cfg.AUTH_USERS + "\", id=\"abc\"" : null;
        String g = S3Cfg.aclFromRequest(grant, null);
        assertTrue(g.contains(S3Cfg.AUTH_USERS) && g.contains("<ID>abc</ID>"));
        assertNull(S3Cfg.aclFromRequest(n -> null, null));
        assertEquals("InvalidRequest", code(() -> S3Cfg.aclFromRequest(n -> n.equals("x-amz-acl") ? "private" : n.equals("x-amz-grant-read")
                ? "id=\"a\"" : null, null)));
        assertEquals("InvalidArgument", code(() -> S3Cfg.aclFromRequest(n -> n.equals("x-amz-acl") ? "bogus" : null, null)));
        byte[] body = utf8("<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList><Grant><Grantee "
                + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>" + S3Cfg.ALL_USERS
                + "</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>");
        assertTrue(S3Cfg.aclIsPublic(S3Cfg.aclFromRequest(n -> null, body)));
    }

    @Test
    void publicAccessBlockAndOwnership() {
        String xml = S3Cfg.validatePublicAccessBlock(utf8("<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls>"
                + "<BlockPublicPolicy>false</BlockPublicPolicy></PublicAccessBlockConfiguration>"));
        assertTrue(S3Cfg.pabFlag(xml, "BlockPublicAcls"));
        assertFalse(S3Cfg.pabFlag(xml, "BlockPublicPolicy"));
        assertFalse(S3Cfg.pabFlag(xml, "RestrictPublicBuckets"));
        assertFalse(S3Cfg.pabFlag(null, "BlockPublicAcls"));
        assertEquals("MalformedXML", code(() -> S3Cfg.validatePublicAccessBlock(utf8("<PublicAccessBlockConfiguration>"
                + "<BlockPublicAcls>maybe</BlockPublicAcls></PublicAccessBlockConfiguration>"))));
        assertEquals("BucketOwnerEnforced", S3Cfg.validateOwnership(utf8("<OwnershipControls><Rule><ObjectOwnership>"
                + "BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>")));
        assertEquals("MalformedXML", code(() -> S3Cfg.validateOwnership(utf8("<OwnershipControls/>"))));
    }

    @Test
    void policyPublicHeuristic() {
        assertTrue(S3Api.policyIsPublic("{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\"}]}"));
        assertTrue(S3Api.policyIsPublic("{\"Statement\":{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"}}}"));
        assertFalse(S3Api.policyIsPublic("{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::1:root\"}}]}"));
        assertFalse(S3Api.policyIsPublic("{\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\"}]}"));
        assertFalse(S3Api.policyIsPublic("garbage"));
    }

    @Test
    void encryptionDefaultsAndValidation() {
        assertEquals("AES256", S3Cfg.encryptionDefaults(null)[0]);
        String[] kms = S3Cfg.encryptionDefaults("<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>"
                + "<SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>k1</KMSMasterKeyID></ApplyServerSideEncryptionByDefault>"
                + "</Rule></ServerSideEncryptionConfiguration>");
        assertEquals("aws:kms", kms[0]);
        assertEquals("k1", kms[1]);
        assertEquals("MalformedXML", code(() -> S3Cfg.validateEncryption(utf8("<ServerSideEncryptionConfiguration><Rule>"
                + "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>ROT13</SSEAlgorithm></ApplyServerSideEncryptionByDefault>"
                + "</Rule></ServerSideEncryptionConfiguration>"))));
    }

    @Test
    void objectLockDocuments() {
        String[] d = S3Cfg.parseObjectLockConfig(utf8("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule>"
                + "<DefaultRetention><Mode>GOVERNANCE</Mode><Days>3</Days></DefaultRetention></Rule></ObjectLockConfiguration>"));
        assertEquals("GOVERNANCE", d[0]);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertEquals(Instant.parse("2026-01-04T00:00:00Z"), S3Cfg.defaultRetentionUntil(d, now));
        assertNull(S3Cfg.defaultRetentionUntil(new String[] {null, null, null}, now));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseObjectLockConfig(utf8("<ObjectLockConfiguration><Rule><DefaultRetention>"
                + "<Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"))));
        String[] r = S3Cfg.parseRetention(utf8("<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"));
        assertEquals("COMPLIANCE", r[0]);
        assertEquals("ON", S3Cfg.parseLegalHold(utf8("<LegalHold><Status>ON</Status></LegalHold>")));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseLegalHold(utf8("<LegalHold><Status>MAYBE</Status></LegalHold>"))));
    }

    // ---- CORS -----------------------------------------------------------------------------------------------

    @Test
    void corsRulesAndMatching() {
        List<S3Cfg.CorsRule> rules = S3Cfg.parseCors(utf8("<CORSConfiguration>"
                + "<CORSRule><AllowedOrigin>http://*.example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod>"
                + "<AllowedMethod>PUT</AllowedMethod><AllowedHeader>x-amz-*</AllowedHeader><ExposeHeader>ETag</ExposeHeader>"
                + "<MaxAgeSeconds>600</MaxAgeSeconds></CORSRule>"
                + "<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"));
        assertEquals(2, rules.size());
        assertEquals(600, rules.get(0).maxAge());
        assertEquals(rules.get(0), S3Cfg.matchCors(rules, "http://app.example.com", "PUT", "x-amz-meta-a, X-Amz-Date"));
        assertNull(S3Cfg.matchCors(rules, "http://app.example.com", "DELETE", null));
        assertNull(S3Cfg.matchCors(rules, "https://app.example.com", "PUT", null)); // wrong scheme: rule 1 no, rule 2 lacks PUT
        assertEquals(rules.get(1), S3Cfg.matchCors(rules, "http://other.io", "GET", null));
        assertNull(S3Cfg.matchCors(rules, "http://app.example.com", "PUT", "content-type")); // header not allowed by rule 1
        assertTrue(S3Cfg.wildcardMatch("http://*.example.com", "http://a.b.example.com", false));
        assertFalse(S3Cfg.wildcardMatch("http://*.example.com", "http://example.com", false));
        assertTrue(S3Cfg.wildcardMatch("*", "anything", false));
        assertTrue(S3Cfg.wildcardMatch("X-AMZ-*", "x-amz-date", true));
        assertEquals("InvalidRequest", code(() -> S3Cfg.parseCors(utf8("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin>"
                + "<AllowedMethod>PATCH</AllowedMethod></CORSRule></CORSConfiguration>"))));
        assertEquals("InvalidRequest", code(() -> S3Cfg.parseCors(utf8("<CORSConfiguration><CORSRule><AllowedOrigin>a*b*c</AllowedOrigin>"
                + "<AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"))));
        assertEquals("MalformedXML", code(() -> S3Cfg.parseCors(utf8("<CORSConfiguration/>"))));
    }

    @Test
    void lifecycleValidation() {
        S3Cfg.validateLifecycle(utf8("<LifecycleConfiguration><Rule><ID>a</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>"));
        assertEquals("MalformedXML", code(() -> S3Cfg.validateLifecycle(utf8("<LifecycleConfiguration/>"))));
        assertEquals("MalformedXML", code(() -> S3Cfg.validateLifecycle(utf8("<LifecycleConfiguration><Rule><Status>On</Status>"
                + "</Rule></LifecycleConfiguration>"))));
        assertEquals("InvalidArgument", code(() -> S3Cfg.validateLifecycle(utf8("<LifecycleConfiguration><Rule><ID>a</ID><Status>Enabled"
                + "</Status></Rule><Rule><ID>a</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>"))));
    }

    // ---- request bodies -------------------------------------------------------------------------------------

    @Test
    void completeMultipartBodiesCarryPartChecksums() {
        List<S3Model.CompletePart> parts = S3Xml.parseCompleteParts(utf8("<CompleteMultipartUpload>"
                + "<Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag><ChecksumSHA256>abc=</ChecksumSHA256></Part>"
                + "<Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part></CompleteMultipartUpload>"));
        assertEquals(2, parts.size());
        assertEquals(Map.of("SHA256", "abc="), parts.get(0).checksums());
        assertTrue(parts.get(1).checksums().isEmpty());
        assertEquals("MalformedXML", code(() -> S3Xml.parseCompleteParts(utf8("<CompleteMultipartUpload/>"))));
        assertEquals("InvalidArgument", code(() -> S3Xml.parseCompleteParts(utf8("<CompleteMultipartUpload><Part>"
                + "<PartNumber>10001</PartNumber><ETag>x</ETag></Part></CompleteMultipartUpload>"))));
    }

    @Test
    void deleteBodiesCarryVersionIds() {
        S3Xml.DeleteIds d = S3Xml.parseDeleteIds(utf8("<Delete><Quiet>true</Quiet><Object><Key>a</Key></Object>"
                + "<Object><Key>b</Key><VersionId>v1</VersionId></Object></Delete>"));
        assertTrue(d.quiet());
        assertEquals(new S3Model.ObjId("a", null), d.ids().get(0));
        assertEquals(new S3Model.ObjId("b", "v1"), d.ids().get(1));
        assertEquals("MalformedXML", code(() -> S3Xml.parseDeleteIds(utf8("<Nope/>"))));
        assertNull(S3Xml.parseLocationConstraint(new byte[0]));
        assertEquals("eu-west-1", S3Xml.parseLocationConstraint(utf8("<CreateBucketConfiguration><LocationConstraint>eu-west-1"
                + "</LocationConstraint></CreateBucketConfiguration>")));
        assertEquals("MalformedXML", code(() -> S3Xml.parseLocationConstraint(utf8("<Other/>"))));
    }

    // ---- addressing -----------------------------------------------------------------------------------------------

    @Test
    void virtualHostedAddressing() {
        List<String> domains = S3Addressing.parseDomains("s3.example.test, .custom.io");
        assertEquals(List.of("s3.example.test", "custom.io"), domains);
        assertEquals("mybucket", S3Addressing.bucketFromHost("mybucket.s3.example.test:9000", domains));
        assertEquals("my.dotted.bucket", S3Addressing.bucketFromHost("my.dotted.bucket.s3.example.test", domains));
        assertEquals("b", S3Addressing.bucketFromHost("b.custom.io", domains));
        assertNull(S3Addressing.bucketFromHost("s3.example.test", domains)); // the service endpoint itself
        assertNull(S3Addressing.bucketFromHost("s3.s3.example.test", domains));
        assertNull(S3Addressing.bucketFromHost("localhost:18020", domains));
        assertNull(S3Addressing.bucketFromHost("127.0.0.1:18020", domains));
        assertNull(S3Addressing.bucketFromHost("[::1]:18020", domains));
        assertNull(S3Addressing.bucketFromHost(null, domains));
        assertEquals("b", S3Addressing.bucketFromHost("b.localhost:18020", List.of()));
        assertEquals("b", S3Addressing.bucketFromHost("b.s3.localhost", List.of()));
        assertNull(S3Addressing.bucketFromHost("s3.localhost", List.of()));
        assertEquals("vh", S3Addressing.bucketFromHost("vh.s3.localhost.floci.io:4566", List.of()));
        assertEquals("vh", S3Addressing.bucketFromHost("vh.s3.localhost.localstack.cloud", List.of()));
        assertEquals("b", S3Addressing.bucketFromHost("b.s3.amazonaws.com", List.of()));
        assertEquals("b", S3Addressing.bucketFromHost("b.s3.eu-west-1.amazonaws.com", List.of()));
        assertEquals("b", S3Addressing.bucketFromHost("b.s3-eu-west-1.amazonaws.com", List.of()));
        assertEquals("b", S3Addressing.bucketFromHost("b.s3.dualstack.us-east-2.amazonaws.com", List.of()));
        assertNull(S3Addressing.bucketFromHost("s3.amazonaws.com", List.of()));
        assertNull(S3Addressing.bucketFromHost("example.org", List.of()));
    }

    // ---- POST policy -------------------------------------------------------------------------------------------------

    @Test
    void postFormParsingAndPolicyChecks() {
        String boundary = "----b0und";
        String form = "--" + boundary + "\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nuploads/${filename}\r\n"
                + "--" + boundary + "\r\nContent-Disposition: form-data; name=\"Content-Type\"\r\n\r\ntext/plain\r\n"
                + "--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\nFILEDATA\r\n--" + boundary + "--\r\n";
        S3PostForm.Form f = S3PostForm.parse(utf8(form), boundary);
        assertEquals("uploads/${filename}", f.fields().get("key"));
        assertEquals("text/plain", f.fields().get("content-type"));
        assertEquals("a.txt", f.filename());
        assertArrayEquals(utf8("FILEDATA"), f.file());
        assertEquals(boundary, S3PostForm.boundary("multipart/form-data; boundary=\"" + boundary + "\""));
        assertEquals("InvalidArgument", code(() -> S3PostForm.parse(utf8("--" + boundary + "\r\nContent-Disposition: form-data; "
                + "name=\"key\"\r\n\r\nk\r\n--" + boundary + "--\r\n"), boundary))); // no file part
        Instant now = Instant.parse("2026-06-01T00:00:00Z");
        String policy = "{\"expiration\":\"2026-06-01T01:00:00Z\",\"conditions\":[{\"bucket\":\"bk\"},[\"starts-with\",\"$key\",\"uploads/\"],"
                + "[\"eq\",\"$Content-Type\",\"text/plain\"],[\"content-length-range\",1,100]]}";
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("key", "uploads/a.txt");
        fields.put("content-type", "text/plain");
        fields.put("policy", "x");
        fields.put("x-amz-signature", "y");
        S3PostForm.checkPolicy(policy, fields, 10, "bk", now);
        assertEquals("AccessDenied", code(() -> S3PostForm.checkPolicy(policy, fields, 10, "other", now)));
        assertEquals("EntityTooLarge", code(() -> S3PostForm.checkPolicy(policy, fields, 1000, "bk", now)));
        assertEquals("EntityTooSmall", code(() -> S3PostForm.checkPolicy(policy, fields, 0, "bk", now)));
        Map<String, String> wrong = new LinkedHashMap<>(fields);
        wrong.put("key", "elsewhere/a.txt");
        assertEquals("AccessDenied", code(() -> S3PostForm.checkPolicy(policy, wrong, 10, "bk", now)));
        Map<String, String> extra = new LinkedHashMap<>(fields);
        extra.put("x-amz-meta-x", "1");
        assertEquals("AccessDenied", code(() -> S3PostForm.checkPolicy(policy, extra, 10, "bk", now)));
        assertEquals("AccessDenied", code(() -> S3PostForm.checkPolicy(policy, fields, 10, "bk", now.plusSeconds(7200)))); // expired
        assertEquals("InvalidPolicyDocument", code(() -> S3PostForm.checkPolicy("{}", fields, 10, "bk", now)));
        assertTrue(S3PostForm.validCredentialScope("AK/20260101/us-east-1/s3/aws4_request"));
        assertFalse(S3PostForm.validCredentialScope("AK/20260101/us-east-1/sqs/aws4_request"));
    }

    // ---- aws-chunked ---------------------------------------------------------------------------------------------------

    @Test
    void awsChunkedBodiesExposeTrailingChecksums() throws Exception {
        String wire = "5;chunk-signature=aa\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
        AwsChunkedInputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(utf8(wire)));
        assertEquals("hello world", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        assertEquals(Map.of("x-amz-checksum-crc32", "AAAAAA=="), in.trailers());
        AwsChunkedInputStream plain = new AwsChunkedInputStream(new ByteArrayInputStream(utf8("3\r\nabc\r\n0\r\n\r\n")));
        assertEquals("abc", new String(plain.readAllBytes(), StandardCharsets.UTF_8));
        assertTrue(plain.trailers().isEmpty());
        assertThrows(java.io.IOException.class, () -> new AwsChunkedInputStream(new ByteArrayInputStream(utf8("zz\r\n"))).readAllBytes());
    }

    // ---- SelectObjectContent ---------------------------------------------------------------------------------------------

    private static S3Select.Request csvRequest(String sql, String header, String outType) {
        return S3Select.parseRequest(utf8("<SelectObjectContentRequest><Expression>" + S3Xml.escape(sql) + "</Expression>"
                + "<ExpressionType>SQL</ExpressionType><InputSerialization><CSV><FileHeaderInfo>" + header
                + "</FileHeaderInfo></CSV></InputSerialization><OutputSerialization><" + outType + "/></OutputSerialization>"
                + "</SelectObjectContentRequest>"));
    }

    @Test
    void selectEvaluatesSqlOverCsvAndJson() {
        String csv = "name,age,city\nAlice,30,NYC\nBob,25,\nCharlie,41,LA\n\"Smith, John\",50,SF\n";
        assertEquals("Alice\nCharlie\n\"Smith, John\"\n", new String(S3Select.run(utf8(csv),
                csvRequest("SELECT name FROM S3Object WHERE CAST(age AS INT) > 28", "USE", "CSV")), StandardCharsets.UTF_8));
        assertEquals("4,146,50\n", new String(S3Select.run(utf8(csv),
                csvRequest("SELECT COUNT(*), SUM(age), MAX(age) FROM S3Object", "USE", "CSV")), StandardCharsets.UTF_8));
        assertEquals("Bob\n", new String(S3Select.run(utf8(csv),
                csvRequest("SELECT name FROM S3Object s WHERE s.city = ''", "USE", "CSV")), StandardCharsets.UTF_8));
        // a record with fewer fields than the header has MISSING (NULL) trailing columns; an empty field is an empty string
        assertEquals("Bob\n", new String(S3Select.run(utf8("name,age,city\nAlice,30,NY\nBob,25\n"),
                csvRequest("SELECT name FROM S3Object WHERE city IS NULL", "USE", "CSV")), StandardCharsets.UTF_8));
        assertEquals("name\nAlice\n", new String(S3Select.run(utf8(csv),
                csvRequest("SELECT _1 FROM S3Object LIMIT 2", "NONE", "CSV")), StandardCharsets.UTF_8));
        assertEquals("ALICE,60\n", new String(S3Select.run(utf8(csv),
                csvRequest("SELECT UPPER(name), age * 2 FROM S3Object WHERE name LIKE 'A%' AND age BETWEEN 25 AND 35", "USE", "CSV")),
                StandardCharsets.UTF_8));
        assertTrue(new String(S3Select.run(utf8(csv), csvRequest("SELECT name, age AS a FROM S3Object LIMIT 1", "USE", "JSON")),
                StandardCharsets.UTF_8).startsWith("{\"name\":\"Alice\",\"a\":\"30\"}"));
        assertEquals("ParseUnexpectedToken", code(() -> S3Select.run(utf8(csv), csvRequest("SELEKT *", "USE", "CSV"))));
        S3Select.Request json = S3Select.parseRequest(utf8("<SelectObjectContentRequest><Expression>SELECT s.a.b FROM S3Object s "
                + "WHERE s.n &gt; 1</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><JSON><Type>LINES</Type>"
                + "</JSON></InputSerialization><OutputSerialization><CSV/></OutputSerialization></SelectObjectContentRequest>"));
        assertEquals("x\n", new String(S3Select.run(utf8("{\"n\":1,\"a\":{\"b\":\"w\"}}\n{\"n\":2,\"a\":{\"b\":\"x\"}}\n"), json),
                StandardCharsets.UTF_8));
    }

    @Test
    void selectEventStreamFraming() throws Exception {
        byte[] es = S3Select.eventStream(utf8("a,b\n"), 10, false);
        java.io.DataInputStream in = new java.io.DataInputStream(new ByteArrayInputStream(es));
        List<String> types = new ArrayList<>();
        while (in.available() > 0) {
            int total = in.readInt();
            int hdr = in.readInt();
            in.readInt(); // prelude crc
            byte[] headers = in.readNBytes(hdr);
            in.readNBytes(total - 16 - hdr);
            in.readInt(); // message crc
            String h = new String(headers, StandardCharsets.UTF_8);
            types.add(h.contains("Records") ? "Records" : h.contains("Stats") ? "Stats" : "End");
        }
        assertEquals(List.of("Records", "Stats", "End"), types);
        ByteArrayOutputStream ignored = new ByteArrayOutputStream();
        assertNotNull(ignored);
    }
}
