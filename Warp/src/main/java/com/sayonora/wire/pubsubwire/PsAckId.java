package com.sayonora.wire.pubsubwire;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Ack ids are opaque to clients. Ours carry the message's queue sequence number, the delivery attempt it belongs to and a
 * random token: {@code base64url(seq:8 | attempt:4 | token:8)} prefixed with a version marker. The token makes an ack id from
 * an earlier delivery distinguishable from the current one (exactly-once delivery rejects the stale one; at-least-once
 * subscriptions accept any well-formed id, like Pub/Sub does).
 */
record PsAckId(long seq, int attempt, String token) {

    private static final String PREFIX = "W";

    static String encode(long seq, int attempt, String token) {
        byte[] t = token.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer b = ByteBuffer.allocate(12 + t.length);
        b.putLong(seq).putInt(attempt).put(t);
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    /** Returns null for anything that is not an ack id we issued. */
    static PsAckId decode(String id) {
        if (id == null || id.length() < 3 || !id.startsWith(PREFIX)) {
            return null;
        }
        try {
            byte[] raw = Base64.getUrlDecoder().decode(id.substring(1));
            if (raw.length < 13 || raw.length > 12 + 64) {
                return null;
            }
            ByteBuffer b = ByteBuffer.wrap(raw);
            long seq = b.getLong();
            int attempt = b.getInt();
            byte[] t = new byte[raw.length - 12];
            b.get(t);
            String token = new String(t, StandardCharsets.US_ASCII);
            if (seq < 0 || !token.matches("[0-9a-f]+")) {
                return null;
            }
            return new PsAckId(seq, attempt, token);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
