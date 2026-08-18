package com.polygres.wire.orawire.frontend.auth;

import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class AuthKv {

    public static void writeString(TtcWriter w, String value) {
        if (value == null) {
            w.writeUb4(0);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        w.writeUb4(bytes.length);
        w.writeBytesWithLength(bytes);
    }

    public static void writePair(TtcWriter w, String key, String value, long flags) {
        writeString(w, key);
        writeString(w, value);
        w.writeUb4(flags);
    }

    private static String readString(TtcReader r) {
        long outerLen = r.readUb4();
        if (outerLen == 0) {
            return null;
        }
        byte[] inner = r.readBytesWithLength();
        return inner == null ? null : new String(inner, StandardCharsets.UTF_8);
    }

    public static void skipPairs(TtcReader r, int numPairs) {
        for (int i = 0; i < numPairs; i++) {
            readString(r);
            readString(r);
            r.readUb4();
        }
    }

    public static Map<String, String> readPairs(TtcReader r, int numPairs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < numPairs; i++) {
            String key = readString(r);
            String value = readString(r);
            r.readUb4();
            map.put(key, value);
        }
        return map;
    }

    private AuthKv() {
    }
}
