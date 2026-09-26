package com.sayonora.wire.ab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AbDiffUnitTest {

    private static AbDiff.Capture cap(int status, String ct, String body) {
        AbDiff.Capture c = new AbDiff.Capture();
        c.status = status;
        c.headers.put("content-type", ct);
        c.head = body.getBytes(StandardCharsets.UTF_8);
        c.total = c.head.length;
        c.sha256 = AbSigV4.hex(AbSigV4.sha256(c.head));
        return c;
    }

    private static final Set<String> IGN = Set.of("RequestId", "ConsumedCapacity");

    @Test
    void jsonKeyOrderNumbersAndIgnoredKeysAreNormalised() {
        var a = cap(200, "application/x-amz-json-1.0", "{\"Item\":{\"n\":1.0,\"s\":{\"S\":\"x\"}},\"ConsumedCapacity\":{\"a\":1}}");
        var b = cap(200, "application/x-amz-json-1.0; charset=utf-8", "{\"Item\":{\"s\":{\"S\":\"x\"},\"n\":1}}");
        assertEquals(List.of(), AbDiff.diff(a, b, IGN, false, true));
    }

    @Test
    void jsonValueAndMissingKeyDifferencesArePathBased() {
        var a = cap(200, "application/json", "{\"Item\":{\"s\":{\"S\":\"local\"}},\"only\":1}");
        var b = cap(200, "application/json", "{\"Item\":{\"s\":{\"S\":\"cloud\"}},\"extra\":2}");
        List<String> d = AbDiff.diff(a, b, IGN, false, true);
        assertTrue(d.contains("body /Item/s/S: value differs"), d.toString());
        assertTrue(d.contains("body /only: missing on cloud"));
        assertTrue(d.contains("body /extra: missing on local"));
        assertTrue(AbDiff.diff(a, b, IGN, true, true).get(0).contains("local=local cloud=cloud"));
    }

    @Test
    void statusDifferenceAndXml() {
        var a = cap(200, "application/xml", "<R xmlns=\"x\"><RequestId>1</RequestId><K>a</K><K>b</K></R>");
        var b = cap(404, "application/xml", "<R xmlns=\"x\"><RequestId>2</RequestId><K>a</K><K>c</K></R>");
        List<String> d = AbDiff.diff(a, b, IGN, false, true);
        assertEquals("status: local=200 cloud=404", d.get(0));
        assertTrue(d.stream().anyMatch(s -> s.startsWith("body R[0]/K[1]")) || d.stream().anyMatch(s -> s.contains("/K[1]")), d.toString());
        assertTrue(d.stream().noneMatch(s -> s.contains("RequestId")));
    }

    @Test
    void opaqueBodiesCompareByDigest() {
        var a = cap(200, "application/octet-stream", "abc");
        var b = cap(200, "application/octet-stream", "abd");
        assertTrue(AbDiff.diff(a, b, IGN, false, false).get(0).startsWith("body differs (local 3 bytes, cloud 3 bytes, sha256 differs)"));
        assertEquals(List.of(), AbDiff.diff(a, cap(200, "application/octet-stream", "abc"), IGN, false, false));
    }

    @Test
    void sideFailureIsReported() {
        var a = cap(200, "a/b", "");
        var b = new AbDiff.Capture();
        b.error = "ConnectException";
        assertEquals(List.of("cloud side failed: ConnectException"), AbDiff.diff(a, b, IGN, false, true));
    }
}
