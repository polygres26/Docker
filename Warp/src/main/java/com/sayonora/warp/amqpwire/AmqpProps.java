package com.sayonora.warp.amqpwire;

import java.util.LinkedHashMap;
import java.util.Map;

/** The 14 basic-class content header properties, kept as their exact wire encoding (flags + values) between publish and delivery. */
final class AmqpProps {

    String contentType;
    String contentEncoding;
    Map<String, Object> headers;
    Integer deliveryMode;
    Integer priority;
    String correlationId;
    String replyTo;
    String expiration;
    String messageId;
    Long timestamp;
    String type;
    String userId;
    String appId;
    String clusterId;

    static AmqpProps parse(byte[] raw) {
        AmqpCodec.Reader r = new AmqpCodec.Reader(raw);
        int flags = r.u16();
        int all = flags;
        int f = flags;
        while ((f & 1) != 0) {
            f = r.u16();
            all |= 0; // continuation words carry no defined properties
        }
        AmqpProps p = new AmqpProps();
        if ((flags & 0x8000) != 0) {
            p.contentType = r.shortStr();
        }
        if ((flags & 0x4000) != 0) {
            p.contentEncoding = r.shortStr();
        }
        if ((flags & 0x2000) != 0) {
            p.headers = r.table();
        }
        if ((flags & 0x1000) != 0) {
            p.deliveryMode = r.u8();
        }
        if ((flags & 0x0800) != 0) {
            p.priority = r.u8();
        }
        if ((flags & 0x0400) != 0) {
            p.correlationId = r.shortStr();
        }
        if ((flags & 0x0200) != 0) {
            p.replyTo = r.shortStr();
        }
        if ((flags & 0x0100) != 0) {
            p.expiration = r.shortStr();
        }
        if ((flags & 0x0080) != 0) {
            p.messageId = r.shortStr();
        }
        if ((flags & 0x0040) != 0) {
            p.timestamp = r.i64();
        }
        if ((flags & 0x0020) != 0) {
            p.type = r.shortStr();
        }
        if ((flags & 0x0010) != 0) {
            p.userId = r.shortStr();
        }
        if ((flags & 0x0008) != 0) {
            p.appId = r.shortStr();
        }
        if ((flags & 0x0004) != 0) {
            p.clusterId = r.shortStr();
        }
        return p;
    }

    byte[] toBytes() {
        int flags = 0;
        AmqpCodec.Writer w = new AmqpCodec.Writer();
        if (contentType != null) {
            flags |= 0x8000;
            w.shortStr(contentType);
        }
        if (contentEncoding != null) {
            flags |= 0x4000;
            w.shortStr(contentEncoding);
        }
        if (headers != null) {
            flags |= 0x2000;
            w.table(headers);
        }
        if (deliveryMode != null) {
            flags |= 0x1000;
            w.u8(deliveryMode);
        }
        if (priority != null) {
            flags |= 0x0800;
            w.u8(priority);
        }
        if (correlationId != null) {
            flags |= 0x0400;
            w.shortStr(correlationId);
        }
        if (replyTo != null) {
            flags |= 0x0200;
            w.shortStr(replyTo);
        }
        if (expiration != null) {
            flags |= 0x0100;
            w.shortStr(expiration);
        }
        if (messageId != null) {
            flags |= 0x0080;
            w.shortStr(messageId);
        }
        if (timestamp != null) {
            flags |= 0x0040;
            w.i64(timestamp);
        }
        if (type != null) {
            flags |= 0x0020;
            w.shortStr(type);
        }
        if (userId != null) {
            flags |= 0x0010;
            w.shortStr(userId);
        }
        if (appId != null) {
            flags |= 0x0008;
            w.shortStr(appId);
        }
        if (clusterId != null) {
            flags |= 0x0004;
            w.shortStr(clusterId);
        }
        AmqpCodec.Writer out = new AmqpCodec.Writer();
        out.u16(flags);
        out.raw(w.buf, 0, w.n);
        return out.toBytes();
    }

    Map<String, Object> headersOrEmpty() {
        return headers == null ? new LinkedHashMap<>() : headers;
    }

    /** Per-message TTL in ms from the {@code expiration} property, or null; -1 when it is not a valid non-negative integer. */
    Long expirationMs() {
        if (expiration == null) {
            return null;
        }
        try {
            long v = Long.parseLong(expiration.trim());
            return v < 0 ? -1L : v;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    Map<String, Object> asMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "content_type", contentType);
        put(m, "content_encoding", contentEncoding);
        put(m, "headers", headers);
        put(m, "delivery_mode", deliveryMode);
        put(m, "priority", priority);
        put(m, "correlation_id", correlationId);
        put(m, "reply_to", replyTo);
        put(m, "expiration", expiration);
        put(m, "message_id", messageId);
        put(m, "timestamp", timestamp);
        put(m, "type", type);
        put(m, "user_id", userId);
        put(m, "app_id", appId);
        put(m, "cluster_id", clusterId);
        return m;
    }

    private static void put(Map<String, Object> m, String k, Object v) {
        if (v != null) {
            m.put(k, v);
        }
    }
}
