package com.sayonora.wire.amqpwire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure-logic tests of amqpwire: field tables, content header properties, topic and headers routing, queue argument validation. */
class AmqpwireUnitTest {

    private static AmqpStore.BindingDef b(String key, Map<String, Object> args) {
        return new AmqpStore.BindingDef("/", "x", "q", 'q', key, args);
    }

    private static boolean topic(String pattern, String key) {
        return AmqpRouting.topic(pattern, key);
    }

    // ---- topic exchange ----------------------------------------------------------------------------------------

    @Test
    void topicStarIsExactlyOneWord() {
        assertTrue(topic("a.*.c", "a.b.c"));
        assertFalse(topic("a.*.c", "a.c"));
        assertFalse(topic("a.*.c", "a.b.b.c"));
        assertTrue(topic("*", "a"));
        assertFalse(topic("*", "a.b"));
        assertTrue(topic("*.*", "a.b"));
    }

    @Test
    void topicHashIsZeroOrMoreWords() {
        assertTrue(topic("#", ""));
        assertTrue(topic("#", "a.b.c"));
        assertTrue(topic("a.#", "a"));
        assertTrue(topic("a.#", "a.b.c"));
        assertTrue(topic("a.#.c", "a.c"));
        assertTrue(topic("a.#.c", "a.x.y.c"));
        assertFalse(topic("a.#.c", "a.x.y"));
        assertTrue(topic("#.c", "c"));
        assertTrue(topic("#.b.#", "a.b.c"));
        assertFalse(topic("#.b.#", "a.c"));
        assertTrue(topic("#.#", "a.b"));
    }

    @Test
    void topicEmptyWordsAreWords() {
        assertTrue(topic("a..c", "a..c"));
        assertTrue(topic("a.*.c", "a..c"));
        assertFalse(topic("a.b", "a.b."));
        assertFalse(topic("*", ""));
        assertTrue(topic("", ""));
    }

    // ---- headers exchange --------------------------------------------------------------------------------------

    private static Map<String, Object> m(Object... kv) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            r.put((String) kv[i], kv[i + 1]);
        }
        return r;
    }

    @Test
    void headersAllAndAny() {
        assertTrue(AmqpRouting.headers(m("x-match", "all", "a", 1, "b", "two"), m("a", 1, "b", "two", "c", 3)));
        assertFalse(AmqpRouting.headers(m("x-match", "all", "a", 1, "b", "two"), m("a", 1)));
        assertTrue(AmqpRouting.headers(m("x-match", "any", "a", 1, "b", "two"), m("b", "two")));
        assertFalse(AmqpRouting.headers(m("x-match", "any", "a", 1), m("a", 2)));
        assertTrue(AmqpRouting.headers(m("a", 1), m("a", 1)), "the default mode is all");
        assertTrue(AmqpRouting.headers(m("x-match", "all"), m()), "no arguments: all matches everything");
        assertFalse(AmqpRouting.headers(m("x-match", "any"), m("a", 1)), "no arguments: any matches nothing");
    }

    @Test
    void headersXKeysAreIgnoredUnlessWithX() {
        assertTrue(AmqpRouting.headers(m("x-match", "all", "x-k", "v", "a", 1), m("a", 1)));
        assertFalse(AmqpRouting.headers(m("x-match", "all-with-x", "x-k", "v", "a", 1), m("a", 1)));
        assertTrue(AmqpRouting.headers(m("x-match", "all-with-x", "x-k", "v", "a", 1), m("a", 1, "x-k", "v")));
        assertTrue(AmqpRouting.headers(m("x-match", "any-with-x", "x-k", "v", "zz", 9), m("x-k", "v")));
    }

    @Test
    void headersVoidValueMatchesPresence() {
        assertTrue(AmqpRouting.headers(m("x-match", "all", "a", null), m("a", "anything")));
        assertFalse(AmqpRouting.headers(m("x-match", "all", "a", null), m("b", 1)));
    }

    @Test
    void headersIntegerWidthsAreEqual() {
        assertTrue(AmqpRouting.headers(m("a", 1), m("a", 1L)));
        assertTrue(AmqpRouting.headers(m("a", 1), m("a", (short) 1)));
        assertFalse(AmqpRouting.headers(m("a", "1"), m("a", 1)));
    }

    @Test
    void routingByExchangeType() {
        assertTrue(AmqpRouting.matches("direct", b("k", m()), "k", null));
        assertFalse(AmqpRouting.matches("direct", b("k", m()), "K", null));
        assertTrue(AmqpRouting.matches("fanout", b("k", m()), "anything", null));
        assertTrue(AmqpRouting.matches("topic", b("a.#", m()), "a.b", null));
        assertTrue(AmqpRouting.matches("headers", b("", m("h", 1)), "", m("h", 1)));
    }

    // ---- codec -------------------------------------------------------------------------------------------------

    @Test
    void fieldTableRoundTripsEveryType() {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("bool", true);
        t.put("byte", (byte) -3);
        t.put("short", (short) 300);
        t.put("int", 5);
        t.put("long", 1L << 40);
        t.put("float", 1.5f);
        t.put("double", 3.25d);
        t.put("dec", new BigDecimal("123.45"));
        t.put("str", "héllo");
        t.put("bytes", new byte[] {0, (byte) 0xff});
        t.put("ts", new AmqpCodec.Ts(1700000000L));
        t.put("void", null);
        t.put("list", new ArrayList<Object>(List.of(1, "two", List.of(3))));
        t.put("nested", m("a", m("b", 1)));
        Map<String, Object> back = AmqpCodec.decodeTable(AmqpCodec.encodeTable(t));
        assertEquals(t.keySet(), back.keySet());
        assertEquals(t.get("dec"), back.get("dec"));
        assertArrayEquals((byte[]) t.get("bytes"), (byte[]) back.get("bytes"));
        assertEquals(t.get("ts"), back.get("ts"));
        assertNull(back.get("void"));
        assertEquals("[1, two, [3]]", back.get("list").toString());
        assertEquals(AmqpCodec.canonical(t.get("nested")), AmqpCodec.canonical(back.get("nested")));
    }

    @Test
    void malformedTablesAreFrameErrors() {
        byte[] bad = {0, 0, 0, 20, 1, 'a', 'S', 0, 0, 0, 99};
        AmqpException e = assertThrows(AmqpException.class, () -> AmqpCodec.decodeTable(bad));
        assertTrue(e.connection);
        assertEquals(501, e.code);
    }

    @Test
    void canonicalIgnoresOrderAndIntegerWidth() {
        assertEquals(AmqpCodec.canonical(m("a", 1, "b", "x")), AmqpCodec.canonical(m("b", "x", "a", 1L)));
        assertNotEquals(AmqpCodec.canonical(m("a", 1)), AmqpCodec.canonical(m("a", "1")));
    }

    @Test
    void propertiesRoundTripAllFourteen() {
        AmqpProps p = new AmqpProps();
        p.contentType = "text/plain";
        p.contentEncoding = "gzip";
        p.headers = m("h", 1);
        p.deliveryMode = 2;
        p.priority = 7;
        p.correlationId = "c";
        p.replyTo = "r";
        p.expiration = "1000";
        p.messageId = "m";
        p.timestamp = 1700000000L;
        p.type = "t";
        p.userId = "guest";
        p.appId = "app";
        p.clusterId = "cl";
        AmqpProps q = AmqpProps.parse(p.toBytes());
        assertEquals(p.asMap().toString(), q.asMap().toString());
        assertEquals(1000L, q.expirationMs());
        assertArrayEquals(p.toBytes(), q.toBytes());
    }

    @Test
    void emptyPropertiesAreJustTheFlagWord() {
        assertArrayEquals(new byte[] {0, 0}, new AmqpProps().toBytes());
        assertNull(AmqpProps.parse(new byte[] {0, 0}).expirationMs());
    }

    @Test
    void invalidExpirationIsFlagged() {
        AmqpProps p = new AmqpProps();
        p.expiration = "soon";
        assertEquals(-1L, p.expirationMs());
        p.expiration = "-5";
        assertEquals(-1L, p.expirationMs());
        p.expiration = "0";
        assertEquals(0L, p.expirationMs());
    }

    // ---- queue arguments ---------------------------------------------------------------------------------------

    @Test
    void queueArgumentsAreExtractedAndValidated() {
        AmqpStore.QueueDef d = AmqpBroker.buildQueue("/", "q", true, null, false,
                m("x-message-ttl", 5000, "x-max-length", 10L, "x-max-priority", 5, "x-dead-letter-exchange", "dlx", "x-overflow", "reject-publish",
                        "x-expires", 60000));
        assertEquals(5000L, d.ttlMs());
        assertEquals(10L, d.maxLen());
        assertEquals(5, d.maxPrio());
        assertEquals("dlx", d.dlx());
        assertEquals("reject-publish", d.overflow());
        assertEquals(60000L, d.expiresMs());
        for (Map<String, Object> bad : List.of(m("x-message-ttl", -1), m("x-message-ttl", "5"), m("x-expires", 0), m("x-max-priority", 300),
                m("x-overflow", "bogus"), m("x-queue-type", "bogus"), m("x-dead-letter-exchange", 5))) {
            AmqpException e = assertThrows(AmqpException.class, () -> AmqpBroker.buildQueue("/", "q", true, null, false, bad), bad.toString());
            assertEquals(406, e.code);
            assertFalse(e.connection);
        }
    }

    @Test
    void quorumQueuesMustBeDurableAndShared() {
        assertThrows(AmqpException.class, () -> AmqpBroker.buildQueue("/", "q", false, null, false, m("x-queue-type", "quorum")));
        assertThrows(AmqpException.class, () -> AmqpBroker.buildQueue("/", "q", true, "owner", false, m("x-queue-type", "quorum")));
        AmqpBroker.buildQueue("/", "q", true, null, false, m("x-queue-type", "quorum"));
    }

    @Test
    void errorTextsCarryRabbitMqNames() {
        assertEquals("NOT_FOUND - no queue 'q' in vhost '/'", AmqpException.notFound("no queue 'q' in vhost '/'").text);
        AmqpException e = AmqpException.conn(501, "FRAME_ERROR - short frame");
        assertEquals("FRAME_ERROR - short frame", e.text, "a prefixed text is not prefixed twice");
        assertTrue(e.connection);
        assertEquals("PRECONDITION_FAILED", AmqpException.name(406));
        assertEquals("RESOURCE_LOCKED", AmqpException.name(405));
    }

    @Test
    void generatedQueueNamesLookLikeRabbitMqAndAreUnique() {
        String a = AmqpBroker.generatedName();
        String b = AmqpBroker.generatedName();
        assertTrue(a.startsWith("amq.gen-"));
        assertEquals("amq.gen-".length() + 22, a.length());
        assertNotEquals(a, b);
    }
}
