package com.sayonora.wire.ab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AbAuthUnitTest {

    private HttpServer sts;
    private final List<String> requests = new ArrayList<>();
    private final AtomicInteger seq = new AtomicInteger();
    private volatile Instant now = Instant.parse("2030-01-01T00:00:00Z");
    private volatile boolean fail;

    private String startSts() throws Exception {
        sts = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        sts.createContext("/", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            synchronized (requests) {
                requests.add(body + "|" + ex.getRequestHeaders().getFirst("Authorization"));
            }
            byte[] out;
            int code = 200;
            if (fail) {
                code = 403;
                out = "<ErrorResponse><Error><Code>AccessDenied</Code></Error></ErrorResponse>".getBytes();
            } else {
                int n = seq.incrementAndGet();
                out = ("<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>ASIA" + n + "</AccessKeyId>"
                        + "<SecretAccessKey>sec" + n + "</SecretAccessKey><SessionToken>tok" + n + "</SessionToken>"
                        + "<Expiration>" + now.plusSeconds(900) + "</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>")
                        .getBytes();
            }
            ex.sendResponseHeaders(code, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        sts.start();
        return "http://127.0.0.1:" + sts.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        if (sts != null) {
            sts.stop(0);
        }
        AbAwsAuth.clock = Instant::now;
    }

    private JsonObject json(String s) {
        return JsonParser.parseString(s).getAsJsonObject();
    }

    @Test
    void assumeRoleRefreshesBeforeExpiryAndReusesUntilThen() throws Exception {
        String ep = startSts();
        AbAwsAuth.clock = () -> now;
        AbAuthProvider p = AbAwsAuth.create(json("{\"type\":\"assume-role\",\"roleArn\":\"arn:aws:iam::1:role/r\",\"stsEndpoint\":\"" + ep
                + "\",\"refreshSkewSeconds\":60,\"source\":{\"type\":\"static\",\"accessKeyId\":\"AKIASRC\",\"secretAccessKey\":\"srcsecret\"}}"), "us-east-1");
        AbAuthProvider.AwsCreds a = (AbAuthProvider.AwsCreds) p.resolve(null);
        assertEquals("ASIA1", a.accessKeyId());
        now = now.plusSeconds(600); // still fresh (expires at +900, skew 60)
        assertEquals("ASIA1", ((AbAuthProvider.AwsCreds) p.resolve(null)).accessKeyId());
        assertEquals(1, requests.size());
        now = now.plusSeconds(260); // inside the skew window -> refresh
        assertEquals("ASIA2", ((AbAuthProvider.AwsCreds) p.resolve(null)).accessKeyId());
        assertEquals(2, requests.size());
        assertTrue(requests.get(0).contains("Action=AssumeRole") && requests.get(0).contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr"));
        assertTrue(requests.get(0).contains("AKIASRC/"), "AssumeRole is SigV4-signed with the source credentials");
        assertFalse(requests.get(0).contains("srcsecret"), "the secret itself is never sent");
    }

    @Test
    void refreshFailureKeepsValidCredentialsThenFails() throws Exception {
        String ep = startSts();
        AbAwsAuth.clock = () -> now;
        AbAuthProvider p = AbAwsAuth.create(json("{\"type\":\"assume-role\",\"roleArn\":\"arn:aws:iam::1:role/r\",\"stsEndpoint\":\"" + ep
                + "\",\"refreshSkewSeconds\":300,\"source\":{\"type\":\"static\",\"accessKeyId\":\"a\",\"secretAccessKey\":\"b\"}}"), "us-east-1");
        p.resolve(null);
        fail = true;
        now = now.plusSeconds(700); // in skew window, not expired: old creds still served
        assertEquals("ASIA1", ((AbAuthProvider.AwsCreds) p.resolve(null)).accessKeyId());
        now = now.plusSeconds(400); // expired now
        assertThrows(AbAuthProvider.AuthException.class, () -> p.resolve(null));
    }

    @Test
    void roleOverrideAssumesThatRoleWithItsOwnCache() throws Exception {
        String ep = startSts();
        AbAuthProvider p = AbAwsAuth.create(json("{\"type\":\"static\",\"accessKeyId\":\"AKIA\",\"secretAccessKey\":\"s\",\"stsEndpoint\":\"" + ep + "\"}"), "us-east-1");
        assertEquals("AKIA", ((AbAuthProvider.AwsCreds) p.resolve(null)).accessKeyId());
        AbAuthProvider.AwsCreds r = (AbAuthProvider.AwsCreds) p.resolve("arn:aws:iam::1:role/narrow");
        assertEquals("ASIA1", r.accessKeyId());
        assertEquals("ASIA1", ((AbAuthProvider.AwsCreds) p.resolve("arn:aws:iam::1:role/narrow")).accessKeyId());
        assertEquals(1, requests.size());
        assertNotEquals("ASIA1", ((AbAuthProvider.AwsCreds) p.resolve("arn:aws:iam::1:role/other")).accessKeyId());
    }

    @Test
    void webIdentityUsesTokenFileUnsigned() throws Exception {
        String ep = startSts();
        java.nio.file.Path f = java.nio.file.Files.createTempFile("wi", ".jwt");
        java.nio.file.Files.writeString(f, "header.payload.sig\n");
        AbAuthProvider p = AbAwsAuth.create(json("{\"type\":\"web-identity\",\"roleArn\":\"arn:aws:iam::1:role/irsa\",\"tokenFile\":\""
                + f.toString().replace("\\", "\\\\") + "\",\"stsEndpoint\":\"" + ep + "\"}"), "us-east-1");
        assertEquals("ASIA1", ((AbAuthProvider.AwsCreds) p.resolve(null)).accessKeyId());
        assertTrue(requests.get(0).contains("Action=AssumeRoleWithWebIdentity") && requests.get(0).contains("WebIdentityToken=header.payload.sig"));
        assertTrue(requests.get(0).endsWith("|null"), "web identity is not SigV4 signed");
    }

    @Test
    void azureAndGoogleAreUnimplementedStubs() {
        for (String t : List.of("azure-service-principal", "azure-managed-identity", "google-service-account", "google-workload-identity", "google-impersonation")) {
            AbAuthProvider p = AbAwsAuth.create(json("{\"type\":\"" + t + "\"}"), "us-east-1");
            AbAuthProvider.AuthException e = assertThrows(AbAuthProvider.AuthException.class, () -> p.resolve(null));
            assertTrue(e.getMessage().contains("UNIMPLEMENTED"), e.getMessage());
            assertThrows(IllegalArgumentException.class, () -> new AbTarget("x", json("{\"auth\":{\"type\":\"" + t + "\"}}")));
        }
    }

    @Test
    void publicJsonNeverContainsSecrets() {
        AbTarget t = new AbTarget("t", json("{\"region\":\"us-east-1\",\"auth\":{\"type\":\"assume-role\",\"roleArn\":\"arn:x\","
                + "\"source\":{\"type\":\"static\",\"accessKeyId\":\"AKIAX\",\"secretAccessKey\":\"TOPSECRET\",\"sessionToken\":\"SESSTOK\"}}}"));
        String pub = t.publicJson().toString();
        assertFalse(pub.contains("TOPSECRET") || pub.contains("SESSTOK"), pub);
        assertTrue(pub.contains("\"secretAccessKeySet\":true"));
        assertFalse(t.provider().toString().contains("TOPSECRET"));
        assertFalse(new AbAuthProvider.AwsCreds("a", "TOPSECRET", "T", null).toString().contains("TOPSECRET"));
    }

    @Test
    void secretsAreOmittedThenKeptOnUpdate() {
        JsonObject stored = json("{\"auth\":{\"type\":\"static\",\"accessKeyId\":\"A\",\"secretAccessKey\":\"S1\"}}");
        JsonObject merged = AbTarget.mergeSecrets(json("{\"auth\":{\"type\":\"static\",\"accessKeyId\":\"B\"}}"), stored);
        assertEquals("S1", merged.getAsJsonObject("auth").get("secretAccessKey").getAsString());
        JsonObject changed = AbTarget.mergeSecrets(json("{\"auth\":{\"type\":\"static\",\"accessKeyId\":\"B\",\"secretAccessKey\":\"S2\"}}"), stored);
        assertEquals("S2", changed.getAsJsonObject("auth").get("secretAccessKey").getAsString());
        JsonObject otherType = AbTarget.mergeSecrets(json("{\"auth\":{\"type\":\"default-chain\"}}"), stored);
        assertFalse(otherType.getAsJsonObject("auth").has("secretAccessKey"));
    }

    @Test
    void sigV4MatchesAwsPublishedGetVanillaVector() {
        // AWS SigV4 test suite "get-vanilla": GET / host:example.amazonaws.com, service "service", us-east-1, 20150830T123600Z
        AbAuthProvider.AwsCreds c = new AbAuthProvider.AwsCreds("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", null, null);
        java.util.Map<String, String> h = new java.util.LinkedHashMap<>();
        h.put("host", "example.amazonaws.com");
        var out = AbSigV4.sign("GET", "/", "", h, AbSigV4.hex(AbSigV4.sha256(new byte[0])), c, "us-east-1", "service",
                Instant.parse("2015-08-30T12:36:00Z"), false);
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, "
                + "Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31", out.get("authorization"));
    }
}
