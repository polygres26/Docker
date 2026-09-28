package com.sayonora.warp.awswire;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/** The {@code application/vnd.amazon.eventstream} binary framing AWS uses for streaming responses (SubscribeToShard). */
public final class EventStream {

    private EventStream() {
    }

    private static void header(ByteArrayOutputStream out, String name, String value) {
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        out.write(n.length);
        out.write(n, 0, n.length);
        out.write(7); // string
        out.write(v.length >> 8);
        out.write(v.length);
        out.write(v, 0, v.length);
    }

    private static long crc(byte[] b, int len) {
        CRC32 c = new CRC32();
        c.update(b, 0, len);
        return c.getValue();
    }

    /** One message: prelude (total length, headers length, CRC), headers, payload, message CRC. */
    public static byte[] message(String messageType, String eventType, String contentType, byte[] payload) {
        ByteArrayOutputStream h = new ByteArrayOutputStream();
        header(h, ":event-type", eventType);
        header(h, ":content-type", contentType);
        header(h, ":message-type", messageType);
        byte[] headers = h.toByteArray();
        int total = 12 + headers.length + payload.length + 4;
        ByteBuffer b = ByteBuffer.allocate(total);
        b.putInt(total);
        b.putInt(headers.length);
        b.putInt((int) crc(b.array(), 8));
        b.put(headers);
        b.put(payload);
        b.putInt((int) crc(b.array(), total - 4));
        return b.array();
    }

    public static byte[] event(String eventType, String jsonPayload) {
        return message("event", eventType, "application/json", jsonPayload.getBytes(StandardCharsets.UTF_8));
    }
}
