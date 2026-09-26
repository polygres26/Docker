package com.sayonora.warp.amqpwire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.amqpwire.Amqp10Codec.AMap;
import com.sayonora.warp.amqpwire.Amqp10Codec.Described;
import com.sayonora.warp.amqpwire.Amqp10Codec.Sym;
import com.sayonora.warp.amqpwire.Amqp10Codec.Ts;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure-logic tests of the AMQP 1.0 side of amqpwire: the type system and the message conversions. */
class Amqp10UnitTest {

    private static Object roundTrip(Object v) {
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        w.value(v);
        return new Amqp10Codec.Reader(w.toBytes()).read();
    }

    @Test
    void primitivesRoundTrip() {
        assertNull(roundTrip(null));
        assertEquals(true, roundTrip(true));
        assertEquals(false, roundTrip(false));
        assertEquals(-5L, roundTrip(-5L));
        assertEquals(1L << 40, roundTrip(1L << 40));
        assertEquals(2.5d, roundTrip(2.5d));
        assertEquals("héllo ☃", roundTrip("héllo ☃"));
        assertEquals(new Sym("amqp:accepted:list"), roundTrip(new Sym("amqp:accepted:list")));
        assertArrayEquals(new byte[] {0, 1, (byte) 0xff}, (byte[]) roundTrip(new byte[] {0, 1, (byte) 0xff}));
        assertEquals(new Ts(1700000000000L), roundTrip(new Ts(1700000000000L)));
        UUID u = UUID.randomUUID();
        assertEquals(u, roundTrip(u));
    }

    @Test
    void longStringsAndBinariesUseThe32BitForms() {
        String big = "x".repeat(1000);
        assertEquals(big, roundTrip(big));
        byte[] bin = new byte[70000];
        java.util.Arrays.fill(bin, (byte) 7);
        assertArrayEquals(bin, (byte[]) roundTrip(bin));
    }

    @Test
    void listsMapsAndDescribedValuesRoundTrip() {
        AMap m = new AMap();
        m.put("k", 1L);
        m.put(new Sym("s"), new ArrayList<Object>(List.of("a", 2L)));
        Object back = roundTrip(m);
        assertEquals(m.toString(), back.toString());
        Described d = new Described(new Amqp10Codec.U64(0x70), new ArrayList<Object>(List.of(true, 5L)));
        Described dd = (Described) roundTrip(d);
        assertEquals(0x70, dd.code());
        assertEquals(List.of(true, 5L), dd.value());
        assertEquals(List.of(), roundTrip(new ArrayList<Object>()));
        List<Object> big = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            big.add((long) i);
        }
        assertEquals(big, roundTrip(big));
    }

    @Test
    void typedUnsignedsAndSymbolArraysEncodeWithTheirFormatCodes() {
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        w.value(new Amqp10Codec.U32(0)).value(new Amqp10Codec.U32(200)).value(new Amqp10Codec.U32(70000)).value(new Amqp10Codec.U8(2)).value(new Amqp10Codec.U16(63));
        byte[] b = w.toBytes();
        assertEquals(0x43, b[0] & 0xff);                   // uint0
        assertEquals(0x52, b[1] & 0xff);                   // smalluint
        assertEquals(0x70, b[3] & 0xff);                   // uint
        Object arr = roundTrip(new Amqp10Codec.SymArr(List.of("PLAIN", "ANONYMOUS")));
        assertEquals(List.of(new Sym("PLAIN"), new Sym("ANONYMOUS")), arr);
    }

    @Test
    void malformedDataIsAFormatError() {
        assertThrows(Amqp10Codec.AmqpFormatException.class, () -> new Amqp10Codec.Reader(new byte[] {(byte) 0xb1, 0, 0, 0, 9, 'a'}).read());
        assertThrows(Amqp10Codec.AmqpFormatException.class, () -> new Amqp10Codec.Reader(new byte[] {(byte) 0x74}).read());
        assertThrows(Amqp10Codec.AmqpFormatException.class, () -> new Amqp10Codec.Reader(new byte[] {(byte) 0xc0, 5, 3, 0x41}).read());
    }

    // ---- message conversion ------------------------------------------------------------------------------------------

    private static byte[] message(boolean withValueBody) {
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        w.described(Amqp10Message.HEADER).list(3, f -> f.bool(true).ubyte(5).uint(60000));
        AMap ann = new AMap();
        ann.put(new Sym("x-opt"), "v");
        w.described(Amqp10Message.MESSAGE_ANN).value(ann);
        w.described(Amqp10Message.PROPERTIES).list(11, f -> {
            f.str("id1").bin("guest".getBytes(StandardCharsets.UTF_8)).nul().str("subj").str("/queues/rq").str("c1").sym("text/plain").sym("gzip").nul()
                    .timestamp(1700000000000L).str("group");
        });
        AMap app = new AMap();
        app.put("k", "v");
        app.put("n", 5L);
        app.put("b", new byte[] {0, 1});
        w.described(Amqp10Message.APP_PROPS).value(app);
        if (withValueBody) {
            w.described(Amqp10Message.VALUE).str("text");
        } else {
            w.described(Amqp10Message.DATA).bin("body".getBytes(StandardCharsets.UTF_8));
        }
        return w.toBytes();
    }

    @Test
    void anAmqp10MessageMapsOntoTheZeroNineOneProperties() {
        Amqp10Message m = Amqp10Message.parse(message(false));
        AmqpProps p = m.toProps();
        assertEquals(2, p.deliveryMode);
        assertEquals(5, p.priority);
        assertEquals("60000", p.expiration);
        assertEquals("id1", p.messageId);
        assertEquals("guest", p.userId);
        assertEquals("rq", p.replyTo);
        assertEquals("c1", p.correlationId);
        assertEquals("text/plain", p.contentType);
        assertEquals("gzip", p.contentEncoding);
        assertEquals(1700000000L, p.timestamp);
        assertEquals("group", p.appId);
        assertEquals("v", p.headers.get("k"));
        assertEquals(5, p.headers.get("n"));
        assertEquals("v", p.headers.get("x-opt"));
        assertEquals("\u0000\u0001", p.headers.get("b"), "a binary application property becomes a string");
        assertNull(p.type);
        assertEquals("body", new String(m.toBody(), StandardCharsets.UTF_8));
    }

    @Test
    void aBodyThatIsNotDataTravelsAsItsEncodingAndIsMarkedAmqp10() {
        Amqp10Message m = Amqp10Message.parse(message(true));
        assertEquals("amqp-1.0", m.toProps().type);
        byte[] body = m.toBody();
        assertEquals(0x00, body[0]);
        assertEquals(0x77, body[2] & 0xff);
        assertTrue(new String(body, StandardCharsets.ISO_8859_1).endsWith("text"));
    }

    @Test
    void theRawSectionsKeepEverythingButTheHeader() {
        Amqp10Message m = Amqp10Message.parse(message(false));
        Amqp10Message rest = Amqp10Message.parse(m.rawWithoutHeader());
        assertEquals(m.sections.size() - 1, rest.sections.size());
        assertNull(rest.first(Amqp10Message.HEADER));
        assertTrue(rest.first(Amqp10Message.PROPERTIES) != null && rest.first(Amqp10Message.APP_PROPS) != null);
    }

    @Test
    void garbageIsReportedTheWayRabbitMqDoes() {
        Amqp10Codec.AmqpFormatException e = assertThrows(Amqp10Codec.AmqpFormatException.class, () -> Amqp10Message.parse("this is not a message".getBytes(StandardCharsets.UTF_8)));
        assertEquals("failed to parse message: {not_a_message_section,{position,0}}", e.getMessage());
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        w.described(Amqp10Message.DATA).bin(new byte[] {1});
        w.u8(0x41);
        Amqp10Codec.AmqpFormatException e2 = assertThrows(Amqp10Codec.AmqpFormatException.class, () -> Amqp10Message.parse(w.toBytes()));
        assertTrue(e2.getMessage().contains("{position,"));
    }

    private static AmqpStore.MsgRow row(AmqpProps p, byte[] body, byte[] raw10, boolean persistent, int dcount) {
        return new AmqpStore.MsgRow(1, "/", "q", "id", "ex", "rk", p.toBytes(), body, 0, persistent, false, 0, null, raw10, dcount);
    }

    @Test
    void aZeroNineOneMessageBecomesSectionsWithRabbitMqsHeaderAndAnnotations() {
        AmqpProps p = new AmqpProps();
        p.contentType = "text/plain";
        p.deliveryMode = 2;
        p.priority = 3;
        p.expiration = "60000";
        p.messageId = "m1";
        p.replyTo = "rt";
        p.timestamp = 1700000000L;
        p.type = "typ";
        p.appId = "app";
        AMap h = new AMap();
        h.put("plain", "v");
        h.put("x-custom", "xv");
        p.headers = new java.util.LinkedHashMap<>();
        h.forEach((k, v) -> p.headers.put((String) k, v));
        Amqp10Message m = Amqp10Message.parse(Amqp10Message.toWire(row(p, "payload".getBytes(StandardCharsets.UTF_8), null, true, 0), false));
        List<?> header = (List<?>) m.first(Amqp10Message.HEADER).value().value();
        assertEquals(true, header.get(0));
        assertEquals(60000L, ((Amqp10Codec.UInt) header.get(2)).value());
        assertEquals(true, header.get(3), "first-acquirer");
        Map<?, ?> ann = (Map<?, ?>) m.first(Amqp10Message.MESSAGE_ANN).value().value();
        assertEquals("rk", ann.get(new Sym("x-routing-key")));
        assertEquals("ex", ann.get(new Sym("x-exchange")));
        assertEquals("typ", ann.get(new Sym("x-basic-type")));
        assertEquals("xv", ann.get(new Sym("x-custom")));
        List<?> props = (List<?>) m.first(Amqp10Message.PROPERTIES).value().value();
        assertEquals("m1", props.get(0));
        assertEquals("/queues/rt", props.get(4), "reply-to is an address");
        assertEquals(new Sym("text/plain"), props.get(6));
        assertEquals(new Ts(1700000000000L), props.get(9));
        assertEquals("app", props.get(10));
        Map<?, ?> app = (Map<?, ?>) m.first(Amqp10Message.APP_PROPS).value().value();
        assertEquals("v", app.get("plain"));
        assertFalse(app.containsKey("x-custom"));
        assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), (byte[]) m.first(Amqp10Message.DATA).value().value());
    }

    @Test
    void aNativeAmqp10MessageIsDeliveredAsSentPlusRoutingAnnotationsAndTheBrokersHeader() {
        Amqp10Message in = Amqp10Message.parse(message(true));
        AmqpProps p = in.toProps();
        AmqpStore.MsgRow r = row(p, in.toBody(), in.rawWithoutHeader(), true, 2);
        Amqp10Message out = Amqp10Message.parse(Amqp10Message.toWire(r, true));
        List<?> header = (List<?>) out.first(Amqp10Message.HEADER).value().value();
        assertEquals(false, header.get(3), "a redelivered message is no longer first-acquirer");
        assertEquals(2L, ((Amqp10Codec.UInt) header.get(4)).value(), "delivery-count = failed deliveries");
        Map<?, ?> ann = (Map<?, ?>) out.first(Amqp10Message.MESSAGE_ANN).value().value();
        assertEquals("rk", ann.get(new Sym("x-routing-key")));
        assertEquals("v", ann.get(new Sym("x-opt")), "the publisher's own annotation survives");
        assertFalse(ann.containsKey(new Sym("x-basic-type")));
        assertArrayEquals(in.first(Amqp10Message.VALUE).raw(), out.first(Amqp10Message.VALUE).raw(), "the body is byte for byte what was sent");
        assertArrayEquals(in.first(Amqp10Message.PROPERTIES).raw(), out.first(Amqp10Message.PROPERTIES).raw());
    }
}
