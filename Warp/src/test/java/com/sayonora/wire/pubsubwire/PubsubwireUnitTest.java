package com.sayonora.wire.pubsubwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Pure-logic tests of pubsubwire: the filter grammar, ack ids, retry backoff, ids and resource names. */
class PubsubwireUnitTest {

    private static boolean m(String filter, Map<String, String> attrs) {
        return PsFilter.parse(filter).matches(attrs);
    }

    // ---- filter grammar ----------------------------------------------------------------------------------------

    @Test
    void equalityPresencePrefix() {
        assertTrue(m("attributes.name = \"com\"", Map.of("name", "com")));
        assertFalse(m("attributes.name = \"com\"", Map.of("name", "org")));
        assertFalse(m("attributes.name = \"com\"", Map.of()));
        assertTrue(m("attributes:name", Map.of("name", "")));
        assertFalse(m("attributes:name", Map.of("other", "x")));
        assertTrue(m("hasPrefix(attributes.name, \"ab\")", Map.of("name", "abc")));
        assertFalse(m("hasPrefix(attributes.name, \"ab\")", Map.of("name", "ba")));
        assertFalse(m("hasPrefix(attributes.name, \"ab\")", Map.of()));
    }

    @Test
    void notEqualRequiresThePresenceOfTheAttribute() {
        assertTrue(m("attributes.k != \"a\"", Map.of("k", "b")));
        assertFalse(m("attributes.k != \"a\"", Map.of("k", "a")));
        assertFalse(m("attributes.k != \"a\"", Map.of()));
    }

    @Test
    void booleanOperatorsAndPrecedence() {
        // NOT binds tighter than AND, AND tighter than OR
        String f = "attributes:a OR attributes:b AND NOT attributes:c";
        assertTrue(m(f, Map.of("a", "1", "c", "1")));      // a OR (b AND NOT c)
        assertTrue(m(f, Map.of("b", "1")));
        assertFalse(m(f, Map.of("b", "1", "c", "1")));
        assertFalse(m(f, Map.of()));
        assertTrue(m("(attributes:a OR attributes:b) AND NOT attributes:c", Map.of("a", "1")));
        assertFalse(m("(attributes:a OR attributes:b) AND NOT attributes:c", Map.of("a", "1", "c", "1")));
        assertTrue(m("-attributes:c", Map.of("a", "1")));
        assertTrue(m("NOT NOT attributes:a", Map.of("a", "1")));
    }

    @Test
    void quotedAndDottedNames() {
        assertTrue(m("attributes:\"iana.org\"", Map.of("iana.org", "x")));
        assertTrue(m("attributes:iana.org", Map.of("iana.org", "x")));
        assertTrue(m("attributes.\"my-key\" = \"a \\\"q\\\"\"", Map.of("my-key", "a \"q\"")));
        assertTrue(m("attributes.my-key = \"1\"", Map.of("my-key", "1")));
    }

    @Test
    void emptyFilterMatchesEverything() {
        assertTrue(m("", Map.of()));
        assertTrue(m("   ", Map.of("a", "b")));
    }

    @Test
    void invalidFiltersAreRejected() {
        for (String bad : new String[] {"attributes.color =", "foo = 1", "(attributes:x", "attributes:x)", "attributes.x", "attributes.x = y",
            "attributes:x AND", "hasPrefix(attributes.x)", "hasPrefix(x, \"a\")", "attributes.x = \"unterminated", "attributes.x == \"a\"",
            "attributes:x attributes:y", "attributes.x = \"a\" and attributes:y", "@"}) {
            assertThrows(IllegalArgumentException.class, () -> PsFilter.parse(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> PsFilter.parse("attributes:" + "x".repeat(300)));
    }

    // ---- ack ids ------------------------------------------------------------------------------------------------

    @Test
    void ackIdRoundTrip() {
        String id = PsAckId.encode(123456789012L, 3, "0a1b2c3d4e5f");
        PsAckId d = PsAckId.decode(id);
        assertEquals(123456789012L, d.seq());
        assertEquals(3, d.attempt());
        assertEquals("0a1b2c3d4e5f", d.token());
    }

    @Test
    void ackIdsOfDifferentDeliveriesDiffer() {
        assertNotEquals(PsAckId.encode(5, 1, "aaaaaaaaaaaa"), PsAckId.encode(5, 2, "aaaaaaaaaaaa"));
        assertNotEquals(PsAckId.encode(5, 1, "aaaaaaaaaaaa"), PsAckId.encode(5, 1, "bbbbbbbbbbbb"));
    }

    @Test
    void malformedAckIdsAreRejected() {
        for (String bad : new String[] {null, "", "W", "bogus", "not-an-ack-id", "W!!!!", "Wabcd", "projects/p/subscriptions/s:1"}) {
            assertNull(PsAckId.decode(bad), String.valueOf(bad));
        }
        assertTrue(PsAckId.encode(1, 1, "abcdef012345").matches("[A-Za-z0-9_\\-]+"), "url-safe");
    }

    // ---- retry backoff -------------------------------------------------------------------------------------------

    @Test
    void backoffDoublesFromTheMinimumAndIsCapped() {
        assertEquals(2000, PsBackoff.delayMillis(2000, 10000, 1));
        assertEquals(4000, PsBackoff.delayMillis(2000, 10000, 2));
        assertEquals(8000, PsBackoff.delayMillis(2000, 10000, 3));
        assertEquals(10000, PsBackoff.delayMillis(2000, 10000, 4));
        assertEquals(10000, PsBackoff.delayMillis(2000, 10000, 500));
        assertEquals(0, PsBackoff.delayMillis(0, 0, 3));
        assertEquals(600_000, PsBackoff.delayMillis(PsBackoff.DEFAULT_MIN_MS, PsBackoff.DEFAULT_MAX_MS, 20));
    }

    // ---- ordering keys (the SQL is exercised end to end in test_pubsub_conformance.py; here the rule's inputs) -----

    @Test
    void messageIdsIncreaseAndAreUnique() {
        Set<String> seen = new HashSet<>();
        long last = 0;
        for (int i = 0; i < 20000; i++) {
            String id = PsIds.next();
            long v = Long.parseLong(id);
            assertTrue(v > last);
            last = v;
            assertTrue(seen.add(id));
        }
    }

    // ---- names ---------------------------------------------------------------------------------------------------

    @Test
    void resourceNames() {
        assertEquals("my-topic", PsNames.parse("topics", "projects/p1/topics/my-topic").id());
        PsException e = assertThrows(PsException.class, () -> PsNames.parse("topics", "garbage"));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.code);
        assertTrue(e.getMessage().startsWith("Invalid resource name given (name=garbage)."));
        e = assertThrows(PsException.class, () -> PsNames.parse("topics", "projects/p/topics/1abc"));
        assertEquals("Invalid [topics] name: (name=projects/p/topics/1abc)", e.getMessage());
        assertThrows(PsException.class, () -> PsNames.parse("topics", "projects/p/topics/goog-x"));
        assertThrows(PsException.class, () -> PsNames.parse("topics", "projects/p/topics/ab"));
        assertThrows(PsException.class, () -> PsNames.parse("subscriptions", "projects/p/topics/abc"));
        assertEquals("p", PsNames.project("projects/p"));
    }

    @Test
    void errorTextsAndHttpStatus() {
        assertEquals("Resource not found (resource=t).", PsException.notFound("t").getMessage());
        assertEquals("Resource already exists in the project (resource=t).", PsException.exists("t").getMessage());
        assertEquals(404, PsException.notFound("t").httpStatus());
        assertEquals(409, PsException.exists("t").httpStatus());
        assertEquals(400, PsException.invalid("x").httpStatus());
        assertEquals(501, PsException.unimplemented("x").httpStatus());
        assertEquals(400, PsException.precondition("x").httpStatus());
        PsException eo = PsException.exactlyOnce(Map.of("id1", "PERMANENT_FAILURE_INVALID_ACK_ID"));
        assertEquals("EXACTLY_ONCE_ACKID_FAILURE", eo.info.getReason());
        assertEquals("PERMANENT_FAILURE_INVALID_ACK_ID", eo.info.getMetadataMap().get("id1"));
    }

    @Test
    void schemaValidationOfAvro() {
        com.google.pubsub.v1.Schema s = com.google.pubsub.v1.Schema.newBuilder().setType(com.google.pubsub.v1.Schema.Type.AVRO)
                .setDefinition("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}").build();
        PsSchemas.validateDefinition(s);
        PsSchemas.validateMessage(s, com.google.pubsub.v1.Encoding.JSON, com.google.protobuf.ByteString.copyFromUtf8("{\"a\":\"x\"}"));
        assertThrows(PsException.class, () -> PsSchemas.validateMessage(s, com.google.pubsub.v1.Encoding.JSON,
                com.google.protobuf.ByteString.copyFromUtf8("{\"b\":1}")));
        // binary Avro: the string "x" is zigzag length 1 (0x02) followed by the byte
        PsSchemas.validateMessage(s, com.google.pubsub.v1.Encoding.BINARY, com.google.protobuf.ByteString.copyFrom(new byte[] {2, 'x'}));
        assertThrows(PsException.class, () -> PsSchemas.validateMessage(s, com.google.pubsub.v1.Encoding.BINARY,
                com.google.protobuf.ByteString.copyFrom(new byte[] {2, 'x', 'y'})));
        com.google.pubsub.v1.Schema proto = com.google.pubsub.v1.Schema.newBuilder().setType(com.google.pubsub.v1.Schema.Type.PROTOCOL_BUFFER)
                .setDefinition("syntax = \"proto3\"; message M { string a = 1; }").build();
        PsException e = assertThrows(PsException.class, () -> PsSchemas.validateMessage(proto, com.google.pubsub.v1.Encoding.BINARY,
                com.google.protobuf.ByteString.EMPTY));
        assertEquals(Status.Code.UNIMPLEMENTED, e.code);
    }
}
