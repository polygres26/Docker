package com.sayonora.wire.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The AWS MD5OfMessageAttributes algorithm -- the vectors are the checksums real AWS SDKs compute client side. */
class SqsMessageAttributesTest {

    private static JsonObject attrs(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    @Test
    void stringAttributeMatchesTheChecksumTheJavaSdkComputes() {
        // captured from AWS SDK for Java v2 validating SendMessage: myattr = String "myval"
        assertEquals("0d2b66022a021b692df4bd9c923ff80d", SqsMessageAttributes.md5OfAttributes(
                attrs("{\"myattr\":{\"DataType\":\"String\",\"StringValue\":\"myval\"}}")));
    }

    @Test
    void binaryAttributeMatchesTheChecksumTheJavaSdkComputes() {
        // payload = Binary {1,2,3,4,5} (base64 AQIDBAU=)
        assertEquals("eb949ffb0fde53eb7625b04de10758ed", SqsMessageAttributes.md5OfAttributes(
                attrs("{\"payload\":{\"DataType\":\"Binary\",\"BinaryValue\":\"AQIDBAU=\"}}")));
    }

    @Test
    void attributesAreHashedInNameOrderNotInsertionOrder() {
        String a = SqsMessageAttributes.md5OfAttributes(attrs(
                "{\"trace-id\":{\"DataType\":\"String\",\"StringValue\":\"abc-123\"},\"priority\":{\"DataType\":\"Number\",\"StringValue\":\"42\"}}"));
        String b = SqsMessageAttributes.md5OfAttributes(attrs(
                "{\"priority\":{\"DataType\":\"Number\",\"StringValue\":\"42\"},\"trace-id\":{\"DataType\":\"String\",\"StringValue\":\"abc-123\"}}"));
        assertEquals(a, b);
        // the SDK-computed value for this exact pair (SqsMd5Test.standardQueueSendMessageWithAttributes...)
        assertEquals("cfd821c7862ee66f6dbf2e411c55ff08", a);
    }

    @Test
    void customTypeSuffixIsPartOfTheHashedTypeName() {
        assertEquals("7818dd9025affc7e1aa978a97e9e38fe", SqsMessageAttributes.md5OfAttributes(
                attrs("{\"priority\":{\"DataType\":\"Number.int\",\"StringValue\":\"42\"}}")));
    }

    @Test
    void noAttributesHaveNoChecksum() {
        assertNull(SqsMessageAttributes.md5OfAttributes(new JsonObject()));
        assertNull(SqsMessageAttributes.md5OfAttributes(null));
    }

    @Test
    void bodyMd5IsLowercaseHex() {
        assertEquals("5eb63bbbe01eeed093cb22bb8f5acdc3", SqsMessageAttributes.md5Hex("hello world"));
    }

    @Test
    void receiveFiltersSupportAllExactAndPrefixWildcards() {
        JsonObject all = attrs("{\"a\":{\"DataType\":\"String\",\"StringValue\":\"1\"},\"bar.x\":{\"DataType\":\"String\",\"StringValue\":\"2\"},"
                + "\"bar.y\":{\"DataType\":\"String\",\"StringValue\":\"3\"},\"baz\":{\"DataType\":\"String\",\"StringValue\":\"4\"}}");
        assertEquals(4, SqsMessageAttributes.filter(all, List.of("All")).size());
        assertEquals(4, SqsMessageAttributes.filter(all, List.of(".*")).size());
        assertEquals(2, SqsMessageAttributes.filter(all, List.of("bar.*")).size());
        assertEquals(1, SqsMessageAttributes.filter(all, List.of("a")).size());
        assertEquals(0, SqsMessageAttributes.filter(all, List.of()).size());
        assertEquals(3, SqsMessageAttributes.filter(all, List.of("a", "bar.*")).size());
    }

    @Test
    void validationRejectsBadTypesNamesAndValues() {
        assertThrows(SqsException.class, () -> SqsMessageAttributes.validateAndNormalise(
                attrs("{\"x\":{\"DataType\":\"Blob\",\"StringValue\":\"1\"}}")));
        assertThrows(SqsException.class, () -> SqsMessageAttributes.validateAndNormalise(
                attrs("{\"aws.x\":{\"DataType\":\"String\",\"StringValue\":\"1\"}}")));
        assertThrows(SqsException.class, () -> SqsMessageAttributes.validateAndNormalise(
                attrs("{\"x\":{\"DataType\":\"Number\",\"StringValue\":\"abc\"}}")));
        assertThrows(SqsException.class, () -> SqsMessageAttributes.validateAndNormalise(
                attrs("{\"x\":{\"DataType\":\"String\"}}")));
        JsonObject ok = SqsMessageAttributes.validateAndNormalise(
                attrs("{\"x\":{\"DataType\":\"Number.float\",\"StringValue\":\"-1.5e3\"}}"));
        assertTrue(ok.has("x"));
    }

    @Test
    void systemAttributeChecksumCoversTheTraceHeader() {
        String trace = "Root=1-5759e988-bd862e3fe1be46a994272793";
        JsonObject o = attrs("{\"AWSTraceHeader\":{\"DataType\":\"String\",\"StringValue\":\"" + trace + "\"}}");
        assertEquals(SqsMessageAttributes.md5OfAttributes(o), SqsMessageAttributes.md5OfTraceHeader(trace));
        assertEquals(trace, SqsMessageAttributes.traceHeader(o));
        assertNull(SqsMessageAttributes.md5OfTraceHeader(null));
    }
}
