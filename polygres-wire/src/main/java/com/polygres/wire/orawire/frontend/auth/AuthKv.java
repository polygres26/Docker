package com.polygres.wire.orawire.frontend.auth;

import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads/writes the O5LOGON key/value/flags triple list, per
 * reference/o5logon_auth_spec.md §1.3. Distinct from the single-prefixed
 * bytes_with_length used elsewhere in TTC: each string here is
 * double-length-prefixed (outer ub4 total length, then the normal inner
 * chunked bytes_with_length encoding).
 */
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

    /** Reads phase-one's fixed 5-pair block (all flags=0, values unused server-side). */
    public static void skipPairs(TtcReader r, int numPairs) {
        for (int i = 0; i < numPairs; i++) {
            readString(r);
            readString(r);
            r.readUb4();
        }
    }

    /** Reads an arbitrary number of key/value/flags triples into a map (phase two). */
    public static Map<String, String> readPairs(TtcReader r, int numPairs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < numPairs; i++) {
            String key = readString(r);
            String value = readString(r);
            r.readUb4(); // flags, meaning not established (spec §5 item 6)
            map.put(key, value);
        }
        return map;
    }

    private AuthKv() {
    }
}
