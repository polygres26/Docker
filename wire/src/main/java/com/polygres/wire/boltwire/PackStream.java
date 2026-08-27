package com.polygres.wire.boltwire;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PackStream -- Bolt's own binary serialization format. Every byte marker and struct shape here
 * was verified against a REAL Neo4j 5.26 server, not reconstructed from the spec alone: captured
 * via {@code wire/scratch_capture/bolt_proxy.py} between a genuine {@code neo4j} Python driver and
 * a real {@code neo4j:5-community} container, then hand-decoded and cross-checked byte-for-byte
 * (see this investigation's own commit message for the exact session -- handshake, HELLO/LOGON,
 * RUN, PULL, RECORD, three separate SUCCESS shapes, GOODBYE -- all captured from one real
 * {@code RETURN 1 AS x} query).
 *
 * <p>Markers implemented (the subset a real Bolt 4.4 session for a simple query actually uses --
 * extended as real messages need more of PackStream's own type system, not implemented speculatively
 * ahead of a real need):
 * <ul>
 *   <li>{@code null} (0xC0), {@code false}/{@code true} (0xC2/0xC3)</li>
 *   <li>Tiny int (0x00-0x7F positive, 0xF0-0xFF negative -16..-1), INT_8/16/32/64
 *       (0xC8/0xC9/0xCA/0xCB)</li>
 *   <li>Tiny string (0x80-0x8F), STRING_8/16/32 (0xD0/0xD1/0xD2)</li>
 *   <li>Tiny list (0x90-0x9F), LIST_8/16/32 (0xD4/0xD5/0xD6)</li>
 *   <li>Tiny map (0xA0-0xAF), MAP_8/16/32 (0xD8/0xD9/0xDA)</li>
 *   <li>Tiny struct (0xB0-0xBF) -- a 1-byte tag/signature followed by that many fields</li>
 *   <li>FLOAT (0xC1, 8-byte IEEE-754 double)</li>
 * </ul>
 */
final class PackStream {

    private PackStream() {
    }

    // ---- writer ----

    static final class Writer {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        byte[] toByteArray() {
            return buf.toByteArray();
        }

        void writeNull() {
            buf.write(0xC0);
        }

        void writeBoolean(boolean b) {
            buf.write(b ? 0xC3 : 0xC2);
        }

        void writeInt(long v) {
            if (v >= -16 && v <= 127) {
                buf.write((int) v);
            } else if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE) {
                buf.write(0xC8);
                buf.write((int) v);
            } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE) {
                buf.write(0xC9);
                writeBE(v, 2);
            } else if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
                buf.write(0xCA);
                writeBE(v, 4);
            } else {
                buf.write(0xCB);
                writeBE(v, 8);
            }
        }

        void writeFloat(double v) {
            buf.write(0xC1);
            writeBE(Double.doubleToLongBits(v), 8);
        }

        void writeString(String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            int n = bytes.length;
            if (n <= 15) {
                buf.write(0x80 | n);
            } else if (n <= 255) {
                buf.write(0xD0);
                buf.write(n);
            } else if (n <= 65535) {
                buf.write(0xD1);
                writeBE(n, 2);
            } else {
                buf.write(0xD2);
                writeBE(n, 4);
            }
            buf.writeBytes(bytes);
        }

        void writeListHeader(int n) {
            if (n <= 15) {
                buf.write(0x90 | n);
            } else if (n <= 255) {
                buf.write(0xD4);
                buf.write(n);
            } else {
                buf.write(0xD5);
                writeBE(n, 2);
            }
        }

        void writeList(List<?> items) {
            writeListHeader(items.size());
            for (Object item : items) {
                writeValue(item);
            }
        }

        void writeMapHeader(int n) {
            if (n <= 15) {
                buf.write(0xA0 | n);
            } else if (n <= 255) {
                buf.write(0xD8);
                buf.write(n);
            } else {
                buf.write(0xD9);
                writeBE(n, 2);
            }
        }

        void writeMap(Map<String, ?> map) {
            writeMapHeader(map.size());
            for (Map.Entry<String, ?> e : map.entrySet()) {
                writeString(e.getKey());
                writeValue(e.getValue());
            }
        }

        void writeStructHeader(int fieldCount, int tag) {
            buf.write(0xB0 | fieldCount);
            buf.write(tag);
        }

        /** Dispatches a plain Java value ({@code String}/{@code Long}/{@code Integer}/
         * {@code Double}/{@code Boolean}/{@code List}/{@code Map}/{@code null}) to the matching
         * PackStream marker -- used for message field values, whose real type varies by field
         * (e.g. a RECORD's row values, a SUCCESS metadata map's own values). */
        void writeValue(Object v) {
            if (v == null) {
                writeNull();
            } else if (v instanceof Boolean b) {
                writeBoolean(b);
            } else if (v instanceof Integer i) {
                writeInt(i);
            } else if (v instanceof Long l) {
                writeInt(l);
            } else if (v instanceof Double d) {
                writeFloat(d);
            } else if (v instanceof Float f) {
                writeFloat(f);
            } else if (v instanceof java.math.BigDecimal bd) {
                // Real bug, found live: a Postgres NUMERIC literal (e.g. the real column type
                // `SELECT 3.14 AS pi` produces) comes back from JDBC as BigDecimal, not Double --
                // crashed a real neo4j driver's session outright the first time this was tested
                // live (IllegalArgumentException here, uncaught, killed the connection mid-PULL).
                // Bolt's own FLOAT type is always a genuine 8-byte double (PackStream has no
                // arbitrary-precision decimal type), so this is a real, honest precision-narrowing
                // conversion, not a bug being papered over -- the same narrowing every other
                // protocol's own float handling already accepts.
                writeFloat(bd.doubleValue());
            } else if (v instanceof java.math.BigInteger bi) {
                writeInt(bi.longValueExact());
            } else if (v instanceof String s) {
                writeString(s);
            } else if (v instanceof List<?> list) {
                writeList(list);
            } else if (v instanceof Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                Map<String, ?> m = (Map<String, ?>) map;
                writeMap(m);
            } else if (v instanceof GraphNode node) {
                writeNode(node);
            } else {
                throw new IllegalArgumentException("boltwire: no PackStream encoding for " + v.getClass());
            }
        }

        /** Real Bolt Node struct: tag {@code 0x4E} ('N'), 3 fields -- id, labels, properties, in
         * that exact order. Real bug, found live writing this feature's own Phase 5 test suite
         * against the real {@code neo4j-java-driver} (which, unlike the Python driver, validates
         * struct field counts strictly): a 4th {@code elementId} field was being written
         * unconditionally, grounded in a real capture -- but that capture (re-verified against a
         * fresh real {@code neo4j:5-community} container while chasing this bug) turned out to be
         * of a session that had negotiated Bolt <b>5.8</b> via the newer "manifest" handshake
         * extension, where a Node struct really does carry a 4th elementId field. This server's own
         * {@code performHandshake} only ever replies with the classic, simpler Bolt <b>4.4</b>
         * handshake reply (see its own javadoc) -- and Bolt 4.4's own Node struct genuinely has
         * only 3 fields, no elementId at all (elementId was a later addition). Every message this
         * server sends has to honor the protocol version it actually claimed during the
         * handshake, not a newer one it never negotiated -- {@link GraphNode#elementId} stays as
         * a real, honest internal id (still exposed as the row's own {@code id} field, which
         * every Bolt version has), just no longer written to the wire until this server actually
         * negotiates Bolt 5.x. */
        void writeNode(GraphNode node) {
            writeStructHeader(3, 0x4E);
            writeInt(node.id());
            writeList(node.labels());
            writeMap(node.properties());
        }

        private void writeBE(long v, int bytes) {
            for (int i = bytes - 1; i >= 0; i--) {
                buf.write((int) ((v >>> (8 * i)) & 0xFF));
            }
        }
    }

    // ---- reader ----

    /** One decoded PackStream struct: {@code tag} is the real Bolt message signature byte
     * (e.g. 0x01 = HELLO, 0x10 = RUN, 0x3F = PULL, 0x02 = GOODBYE -- see BoltMessages), and
     * {@code fields} are the struct's own top-level fields, already recursively decoded. */
    record Struct(int tag, List<Object> fields) {
    }

    static final class Reader {
        private final byte[] data;
        private int pos;

        Reader(byte[] data) {
            this.data = data;
        }

        boolean hasRemaining() {
            return pos < data.length;
        }

        Object readValue() {
            int marker = data[pos++] & 0xFF;
            if (marker == 0xC0) {
                return null;
            }
            if (marker == 0xC2) {
                return Boolean.FALSE;
            }
            if (marker == 0xC3) {
                return Boolean.TRUE;
            }
            if (marker <= 0x7F) {
                return (long) marker;
            }
            if (marker >= 0xF0) {
                return (long) (marker - 256);
            }
            if (marker == 0xC8) {
                return (long) (byte) data[pos++];
            }
            if (marker == 0xC9) {
                return readBE(2, true);
            }
            if (marker == 0xCA) {
                return readBE(4, true);
            }
            if (marker == 0xCB) {
                return readBE(8, true);
            }
            if (marker == 0xC1) {
                long bits = readBE(8, false);
                return Double.longBitsToDouble(bits);
            }
            if ((marker & 0xF0) == 0x80) {
                return readString(marker & 0x0F);
            }
            if (marker == 0xD0) {
                return readString(data[pos++] & 0xFF);
            }
            if (marker == 0xD1) {
                return readString((int) readBE(2, false));
            }
            if (marker == 0xD2) {
                return readString((int) readBE(4, false));
            }
            if ((marker & 0xF0) == 0x90) {
                return readList(marker & 0x0F);
            }
            if (marker == 0xD4) {
                return readList(data[pos++] & 0xFF);
            }
            if (marker == 0xD5) {
                return readList((int) readBE(2, false));
            }
            if (marker == 0xD6) {
                return readList((int) readBE(4, false));
            }
            if ((marker & 0xF0) == 0xA0) {
                return readMap(marker & 0x0F);
            }
            if (marker == 0xD8) {
                return readMap(data[pos++] & 0xFF);
            }
            if (marker == 0xD9) {
                return readMap((int) readBE(2, false));
            }
            if (marker == 0xDA) {
                return readMap((int) readBE(4, false));
            }
            if ((marker & 0xF0) == 0xB0) {
                int n = marker & 0x0F;
                int tag = data[pos++] & 0xFF;
                List<Object> fields = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    fields.add(readValue());
                }
                return new Struct(tag, fields);
            }
            throw new IllegalStateException(
                    String.format("boltwire: unrecognized PackStream marker 0x%02x at position %d", marker, pos - 1));
        }

        /** Convenience for the one call site that always expects a top-level message struct
         * (every real Bolt client message is one). */
        Struct readMessage() {
            Object v = readValue();
            if (!(v instanceof Struct s)) {
                throw new IllegalStateException("boltwire: expected a PackStream struct (a Bolt message), got " + v);
            }
            return s;
        }

        private String readString(int n) {
            String s = new String(data, pos, n, StandardCharsets.UTF_8);
            pos += n;
            return s;
        }

        private List<Object> readList(int n) {
            List<Object> list = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                list.add(readValue());
            }
            return list;
        }

        private Map<String, Object> readMap(int n) {
            Map<String, Object> map = new LinkedHashMap<>(n);
            for (int i = 0; i < n; i++) {
                String key = (String) readValue();
                map.put(key, readValue());
            }
            return map;
        }

        private long readBE(int bytes, boolean signed) {
            long v = 0;
            for (int i = 0; i < bytes; i++) {
                v = (v << 8) | (data[pos++] & 0xFF);
            }
            if (signed && bytes < 8) {
                long signBit = 1L << (bytes * 8 - 1);
                if ((v & signBit) != 0) {
                    v -= (1L << (bytes * 8));
                }
            }
            return v;
        }
    }
}
