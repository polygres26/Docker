package com.sayonora.wire.amqpwire;

import com.sayonora.wire.amqpwire.Amqp10Codec.AMap;
import com.sayonora.wire.amqpwire.Amqp10Codec.Described;
import com.sayonora.wire.amqpwire.Amqp10Codec.Sym;
import com.sayonora.wire.amqpwire.Amqp10Codec.Ts;
import com.sayonora.wire.amqpwire.Amqp10Codec.UInt;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An AMQP 1.0 message (bare + annotated sections) and its conversion to and from the broker's AMQP 0-9-1 model (properties + body). An AMQP 1.0
 * publisher's message is kept in its original encoding (everything but the header) so an AMQP 1.0 consumer receives it as sent; AMQP 0-9-1
 * consumers get the converted view. A message published through 0-9-1 is converted to sections for AMQP 1.0 consumers the way RabbitMQ does.
 */
final class Amqp10Message {

    static final long HEADER = 0x70, DELIVERY_ANN = 0x71, MESSAGE_ANN = 0x72, PROPERTIES = 0x73, APP_PROPS = 0x74, DATA = 0x75, SEQUENCE = 0x76,
            VALUE = 0x77, FOOTER = 0x78;

    /** One top-level section with its original bytes. */
    record Section(long code, Described value, byte[] raw) {
    }

    final List<Section> sections = new ArrayList<>();

    static Amqp10Message parse(byte[] payload) {
        Amqp10Message m = new Amqp10Message();
        Amqp10Codec.Reader r = new Amqp10Codec.Reader(payload);
        while (r.more()) {
            int start = r.p;
            if (payload[start] != 0x00) {
                throw new Amqp10Codec.AmqpFormatException("failed to parse message: {not_a_message_section,{position," + start + "}}");
            }
            Object v;
            try {
                v = r.read();
            } catch (Amqp10Codec.AmqpFormatException e) {
                throw new Amqp10Codec.AmqpFormatException("failed to parse message: {" + e.getMessage().replace(' ', '_') + ",{position," + start + "}}");
            }
            if (!(v instanceof Described d) || d.code() < HEADER || d.code() > FOOTER) {
                throw new Amqp10Codec.AmqpFormatException("failed to parse message: {not_a_message_section,{position," + start + "}}");
            }
            m.sections.add(new Section(d.code(), d, java.util.Arrays.copyOfRange(payload, start, r.p)));
        }
        return m;
    }

    Section first(long code) {
        for (Section s : sections) {
            if (s.code() == code) {
                return s;
            }
        }
        return null;
    }

    private List<?> fields(long code) {
        Section s = first(code);
        return s != null && s.value().value() instanceof List<?> l ? l : List.of();
    }

    private static Object at(List<?> l, int i) {
        return i < l.size() ? l.get(i) : null;
    }

    // ------------------------------------------------------------------------------------------ 1.0 -> 0-9-1

    /** The 0-9-1 properties of this message. */
    AmqpProps toProps() {
        AmqpProps p = new AmqpProps();
        List<?> h = fields(HEADER);
        Object durable = at(h, 0);
        Object prio = at(h, 1);
        Object ttl = at(h, 2);
        if (durable instanceof Boolean b && b) {
            p.deliveryMode = 2;
        }
        if (prio != null) {
            p.priority = (int) Math.max(0, Math.min(255, Amqp10Codec.asLong(prio, 0)));
        }
        if (ttl != null) {
            p.expiration = String.valueOf(Amqp10Codec.asLong(ttl, 0));
        }
        List<?> pr = fields(PROPERTIES);
        Object mid = at(pr, 0);
        p.messageId = mid == null ? null : idString(mid);
        Map<String, Object> extra = new LinkedHashMap<>();
        Object uid = at(pr, 1);
        p.userId = uid instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : Amqp10Codec.asString(uid);
        Object replyTo = at(pr, 4);
        String rt = Amqp10Codec.asString(replyTo);
        p.replyTo = rt == null ? null : rt.startsWith("/queues/") ? rt.substring("/queues/".length()) : rt;
        Object cid = at(pr, 5);
        if (cid instanceof byte[] cb) {
            extra.put("x-correlation-id", cb);          // a binary correlation id has no 0-9-1 property: it travels as a header
        } else {
            p.correlationId = cid == null ? null : idString(cid);
        }
        p.contentType = Amqp10Codec.asString(at(pr, 6));
        p.contentEncoding = Amqp10Codec.asString(at(pr, 7));
        if (at(pr, 9) instanceof Ts t) {
            p.timestamp = t.millis() / 1000;
        }
        p.appId = Amqp10Codec.asString(at(pr, 10));
        Map<String, Object> headers = new LinkedHashMap<>();
        Section ann = first(MESSAGE_ANN);
        if (ann != null && ann.value().value() instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String k = Amqp10Codec.asString(e.getKey());
                if (k == null || k.equals("x-routing-key") || k.equals("x-exchange")) {
                    continue;
                }
                if (k.equals("x-basic-type")) {
                    p.type = Amqp10Codec.asString(e.getValue());
                } else {
                    headers.put(k, to091(e.getValue()));
                }
            }
        }
        Section app = first(APP_PROPS);
        if (app != null && app.value().value() instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String k = Amqp10Codec.asString(e.getKey());
                if (k != null) {
                    headers.put(k, to091(e.getValue()));
                }
            }
        }
        headers.putAll(extra);
        if (!bodyIsData()) {
            p.type = "amqp-1.0";                          // the body travels as its original encoding
        }
        p.headers = headers.isEmpty() ? null : headers;
        return p;
    }

    private boolean bodyIsData() {
        boolean any = false;
        for (Section s : sections) {
            if (s.code() == VALUE || s.code() == SEQUENCE) {
                return false;
            }
            any |= s.code() == DATA;
        }
        return true;
    }

    private static String idString(Object o) {
        if (o instanceof byte[] b) {
            return new String(b, StandardCharsets.ISO_8859_1);
        }
        if (o instanceof UUID u) {
            return "urn:uuid:" + u;
        }
        if (o instanceof UInt u) {
            return Long.toUnsignedString(u.value());
        }
        return String.valueOf(o instanceof Sym s ? s.value() : o);
    }

    static Object to091(Object v) {
        if (v instanceof byte[] b) {
            return new String(b, StandardCharsets.ISO_8859_1);
        }
        if (v instanceof UInt u) {
            long x = u.value();
            return x >= Integer.MIN_VALUE && x <= Integer.MAX_VALUE ? (Object) (int) x : (Object) x;
        }
        if (v instanceof Long l) {
            return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) (int) (long) l : (Object) l;
        }
        if (v instanceof Sym s) {
            return s.value();
        }
        if (v instanceof Ts t) {
            return new AmqpCodec.Ts(t.millis() / 1000);
        }
        if (v instanceof UUID u) {
            return u.toString();
        }
        if (v instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            m.forEach((k, x) -> out.put(String.valueOf(k), to091(x)));
            return out;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>();
            l.forEach(x -> out.add(to091(x)));
            return out;
        }
        return v;
    }

    /** The 0-9-1 body: the data sections concatenated; a body of any other kind travels as its original encoding (RabbitMQ marks it type amqp-1.0). */
    byte[] toBody() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean data = bodyIsData();
        for (Section s : sections) {
            Object v = s.value().value();
            if (data && s.code() == DATA && v instanceof byte[] b) {
                out.write(b, 0, b.length);
            } else if (!data && (s.code() == VALUE || s.code() == SEQUENCE || s.code() == DATA)) {
                out.write(s.raw(), 0, s.raw().length);
            }
        }
        return out.toByteArray();
    }

    /** The original bytes of every section but the header (kept in the store for AMQP 1.0 consumers). */
    byte[] rawWithoutHeader() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Section s : sections) {
            if (s.code() != HEADER) {
                out.write(s.raw(), 0, s.raw().length);
            }
        }
        return out.toByteArray();
    }

    // ------------------------------------------------------------------------------------------ store -> AMQP 1.0

    /**
     * The wire form of a stored message for an AMQP 1.0 consumer.
     *
     * @param dcount the delivery count of the message (failed deliveries)
     */
    static byte[] toWire(AmqpStore.MsgRow m, boolean redelivered) {
        AmqpProps p = AmqpProps.parse(m.props());
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        // header
        Long ttl = p.expirationMs();
        w.described(HEADER).list(5, b -> {
            b.bool(m.persistent());
            if (p.priority != null) {
                b.ubyte(p.priority);
            } else {
                b.nul();
            }
            if (ttl != null && ttl >= 0) {
                b.uint(ttl);
            } else {
                b.nul();
            }
            b.bool(!redelivered && m.dcount() == 0);
            b.uint(m.dcount());
        });
        if (m.raw10() != null) {
            Amqp10Message orig = parse(m.raw10());
            for (Section s : orig.sections) {
                if (s.code() == DELIVERY_ANN) {
                    w.raw(s.raw());
                }
            }
            writeAnnotations(w, m, p, orig.first(MESSAGE_ANN));
            for (Section s : orig.sections) {
                if (s.code() != DELIVERY_ANN && s.code() != MESSAGE_ANN) {
                    w.raw(s.raw());
                }
            }
            return w.toBytes();
        }
        writeAnnotations(w, m, p, null);
        // properties
        List<Object> pr = new ArrayList<>();
        pr.add(p.messageId);
        pr.add(p.userId == null ? null : p.userId.getBytes(StandardCharsets.UTF_8));
        pr.add(null);
        pr.add(null);
        pr.add(p.replyTo == null ? null : "/queues/" + p.replyTo);
        pr.add(p.correlationId);
        pr.add(p.contentType == null ? null : new Sym(p.contentType));
        pr.add(p.contentEncoding == null ? null : new Sym(p.contentEncoding));
        pr.add(null);
        pr.add(p.timestamp == null ? null : new Ts(p.timestamp * 1000));
        pr.add(p.appId);
        int last = pr.size();
        while (last > 0 && pr.get(last - 1) == null) {
            last--;
        }
        if (last > 0) {
            int n = last;
            w.described(PROPERTIES).list(n, b -> {
                for (int i = 0; i < n; i++) {
                    Object o = pr.get(i);
                    if (o instanceof byte[] bytes) {
                        b.bin(bytes);
                    } else {
                        b.value(o);
                    }
                }
            });
        }
        // application-properties: the plain (non x-) headers
        if (p.headers != null) {
            Map<String, Object> app = new LinkedHashMap<>();
            p.headers.forEach((k, v) -> {
                if (!k.startsWith("x-")) {
                    app.put(k, from091(v));
                }
            });
            if (!app.isEmpty()) {
                w.described(APP_PROPS).map(app.size(), b -> app.forEach((k, v) -> b.str(k).value(v)));
            }
        }
        w.described(DATA).bin(m.body());
        return w.toBytes();
    }

    private static void writeAnnotations(Amqp10Codec.Writer w, AmqpStore.MsgRow m, AmqpProps p, Section orig) {
        Map<Object, Object> ann = new LinkedHashMap<>();
        ann.put(new Sym("x-routing-key"), m.rkey());
        ann.put(new Sym("x-exchange"), m.exchange());
        if (p.type != null && orig == null) {
            ann.put(new Sym("x-basic-type"), p.type);
        }
        if (orig != null && orig.value().value() instanceof Map<?, ?> om) {
            om.forEach((k, v) -> {
                String ks = Amqp10Codec.asString(k);
                if (ks == null || !(ks.equals("x-routing-key") || ks.equals("x-exchange"))) {
                    ann.put(k, v);
                }
            });
        }
        if (orig == null && p.headers != null) {
            p.headers.forEach((k, v) -> {
                if (k.startsWith("x-")) {
                    ann.put(new Sym(k), from091(v));
                }
            });
        }
        w.described(MESSAGE_ANN).map(ann.size(), b -> ann.forEach((k, v) -> b.value(k).value(v)));
    }

    static Object from091(Object v) {
        if (v instanceof AmqpCodec.Ts t) {
            return new Ts(t.seconds() * 1000);
        }
        if (v instanceof Map<?, ?> m) {
            AMap out = new AMap();
            m.forEach((k, x) -> out.put(k, from091(x)));
            return out;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>();
            l.forEach(x -> out.add(from091(x)));
            return out;
        }
        if (v instanceof java.math.BigDecimal d) {
            return d.doubleValue();
        }
        return v;
    }
}
