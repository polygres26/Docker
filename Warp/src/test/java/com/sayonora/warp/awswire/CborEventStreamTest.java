package com.sayonora.warp.awswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

class CborEventStreamTest {

    @Test
    void cborRoundTripsBlobsTimestampsAndNumbers() {
        JsonObject o = JsonParser.parseString("{\"Data\":\"aGVsbG8=\",\"ApproximateArrivalTimestamp\":1758801600.5,\"N\":-7,\"Big\":4294967296,"
                + "\"L\":[1,\"x\",true,null],\"S\":\"é\"}").getAsJsonObject();
        byte[] cbor = Cbor.encode(o, Set.of("Data"), Set.of("ApproximateArrivalTimestamp"));
        JsonObject back = Cbor.decode(cbor).getAsJsonObject();
        assertEquals("aGVsbG8=", back.get("Data").getAsString());
        assertEquals(1758801600.5, back.get("ApproximateArrivalTimestamp").getAsDouble());
        assertEquals(-7, back.get("N").getAsInt());
        assertEquals(4294967296L, back.get("Big").getAsLong());
        assertEquals(4, back.getAsJsonArray("L").size());
        assertEquals("é", back.get("S").getAsString());
    }

    @Test
    void blobsAreByteStringsAndTimestampsTagOne() {
        JsonObject o = new JsonObject();
        o.addProperty("Data", "aGVsbG8=");
        byte[] cbor = Cbor.encode(o, Set.of("Data"), Set.of());
        // a1 64 "Data" 45 "hello": map(1), text(4) Data, bytes(5) hello
        assertEquals((byte) 0xa1, cbor[0]);
        assertEquals((byte) 0x45, cbor[6]);
        JsonObject t = new JsonObject();
        t.addProperty("T", 1.5);
        byte[] tc = Cbor.encode(t, Set.of(), Set.of("T"));
        assertEquals((byte) 0xc1, tc[3], "tag 1 (epoch seconds)");
    }

    @Test
    void decodesIndefiniteLengthContainers() {
        // bf 61 61 9f 01 02 ff ff : {"a": [1, 2]} with indefinite map and array
        JsonObject o = Cbor.decode(new byte[] {(byte) 0xbf, 0x61, 0x61, (byte) 0x9f, 0x01, 0x02, (byte) 0xff, (byte) 0xff}).getAsJsonObject();
        assertEquals(2, o.getAsJsonArray("a").size());
    }

    @Test
    void eventStreamMessageHasValidPreludeAndCrcs() {
        byte[] m = EventStream.event("SubscribeToShardEvent", "{\"Records\":[]}");
        ByteBuffer b = ByteBuffer.wrap(m);
        int total = b.getInt();
        int headers = b.getInt();
        int preludeCrc = b.getInt();
        assertEquals(m.length, total);
        CRC32 c = new CRC32();
        c.update(m, 0, 8);
        assertEquals((int) c.getValue(), preludeCrc);
        CRC32 all = new CRC32();
        all.update(m, 0, m.length - 4);
        assertEquals((int) all.getValue(), ByteBuffer.wrap(m, m.length - 4, 4).getInt());
        String hdr = new String(m, 12, headers, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(hdr.contains(":event-type") && hdr.contains("SubscribeToShardEvent") && hdr.contains(":message-type") && hdr.contains("event"));
        assertEquals("{\"Records\":[]}", new String(m, 12 + headers, m.length - 16 - headers, java.nio.charset.StandardCharsets.UTF_8));
    }
}
