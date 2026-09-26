package com.sayonora.wire.boltwire;

import com.sayonora.wire.boltwire.Values.DurationV;
import com.sayonora.wire.boltwire.Values.NodeV;
import com.sayonora.wire.boltwire.Values.PathV;
import com.sayonora.wire.boltwire.Values.PointV;
import com.sayonora.wire.boltwire.Values.RelV;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PackStream -- Bolt's own binary serialization format, encoded and decoded here for every Bolt 4.4 / 5.x value type:
 * null, booleans, 64-bit integers, floats, strings, byte arrays, lists, maps and the structures (Node, Relationship,
 * UnboundRelationship, Path, Date, Time, LocalTime, DateTime (legacy and UTC forms), LocalDateTime, Duration, Point2D/3D).
 * Message-level structures are decoded by {@link Reader#readMessage()}; struct tags nested inside a message are values (the
 * message tags TELEMETRY 0x54 and ROUTE 0x66 collide with the Time / legacy DateTimeZoneId tags, which is why the two
 * levels must not share one decoder).
 *
 * <p>The struct layouts were verified against a real Neo4j 5 server through the official drivers; a Node in Bolt 5.x has an
 * extra {@code element_id} field, a Relationship three (element ids), and DateTime uses UTC-based seconds (tags 'I' and 'i').
 */
final class PackStream {

    private PackStream() {
    }

    // ---- writer ----

    static final class Writer {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private final int major;
        private final boolean utc;

        Writer() {
            this(4, false);
        }

        /** @param major Bolt major version (entity layouts differ between 4 and 5) */
        Writer(int major, boolean utc) {
            this.major = major;
            this.utc = utc || major >= 5;
        }

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

        void writeBytes(byte[] b) {
            int n = b.length;
            if (n <= 255) {
                buf.write(0xCC);
                buf.write(n);
            } else if (n <= 65535) {
                buf.write(0xCD);
                writeBE(n, 2);
            } else {
                buf.write(0xCE);
                writeBE(n, 4);
            }
            buf.writeBytes(b);
        }

        void writeListHeader(int n) {
            if (n <= 15) {
                buf.write(0x90 | n);
            } else if (n <= 255) {
                buf.write(0xD4);
                buf.write(n);
            } else if (n <= 65535) {
                buf.write(0xD5);
                writeBE(n, 2);
            } else {
                buf.write(0xD6);
                writeBE(n, 4);
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
            } else if (n <= 65535) {
                buf.write(0xD9);
                writeBE(n, 2);
            } else {
                buf.write(0xDA);
                writeBE(n, 4);
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

        /** Dispatches a runtime value to its PackStream encoding. */
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
                writeFloat(bd.doubleValue());
            } else if (v instanceof java.math.BigInteger bi) {
                writeInt(bi.longValueExact());
            } else if (v instanceof String s) {
                writeString(s);
            } else if (v instanceof byte[] b) {
                writeBytes(b);
            } else if (v instanceof List<?> list) {
                writeList(list);
            } else if (v instanceof Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                Map<String, ?> m = (Map<String, ?>) map;
                writeMap(m);
            } else if (v instanceof NodeV n) {
                writeNode(n);
            } else if (v instanceof RelV r) {
                writeRel(r);
            } else if (v instanceof PathV p) {
                writePath(p);
            } else if (v instanceof LocalDate d) {
                writeStructHeader(1, 0x44);
                writeInt(d.toEpochDay());
            } else if (v instanceof LocalTime t) {
                writeStructHeader(1, 0x74);
                writeInt(t.toNanoOfDay());
            } else if (v instanceof OffsetTime t) {
                writeStructHeader(2, 0x54);
                writeInt(t.toLocalTime().toNanoOfDay());
                writeInt(t.getOffset().getTotalSeconds());
            } else if (v instanceof LocalDateTime t) {
                writeStructHeader(2, 0x64);
                writeInt(t.toEpochSecond(ZoneOffset.UTC));
                writeInt(t.getNano());
            } else if (v instanceof ZonedDateTime t) {
                writeZoned(t);
            } else if (v instanceof DurationV d) {
                writeStructHeader(4, 0x45);
                writeInt(d.months());
                writeInt(d.days());
                writeInt(d.seconds());
                writeInt(d.nanos());
            } else if (v instanceof PointV p) {
                if (p.z() == null) {
                    writeStructHeader(3, 0x58);
                    writeInt(p.srid());
                    writeFloat(p.x());
                    writeFloat(p.y());
                } else {
                    writeStructHeader(4, 0x59);
                    writeInt(p.srid());
                    writeFloat(p.x());
                    writeFloat(p.y());
                    writeFloat(p.z());
                }
            } else {
                throw new IllegalArgumentException("boltwire: no PackStream encoding for " + v.getClass());
            }
        }

        private void writeZoned(ZonedDateTime t) {
            boolean region = !(t.getZone() instanceof ZoneOffset);
            long utcSeconds = t.toEpochSecond();
            long localSeconds = t.toLocalDateTime().toEpochSecond(ZoneOffset.UTC);
            if (utc) {
                writeStructHeader(3, region ? 0x69 : 0x49);
                writeInt(utcSeconds);
            } else {
                writeStructHeader(3, region ? 0x66 : 0x46);
                writeInt(localSeconds);
            }
            writeInt(t.getNano());
            if (region) {
                writeString(t.getZone().getId());
            } else {
                writeInt(t.getOffset().getTotalSeconds());
            }
        }

        private void writeNode(NodeV n) {
            writeStructHeader(major >= 5 ? 4 : 3, 0x4E);
            writeInt(n.id);
            if (n.deleted) { // a node deleted in this statement is returned empty, like Neo4j
                writeListHeader(0);
                writeMapHeader(0);
            } else {
                writeListHeader(n.labels.size());
                for (String l : n.labels) {
                    writeString(l);
                }
                writeProps(n.props);
            }
            if (major >= 5) {
                writeString(Funcs.elementId(4, n.id));
            }
        }

        private void writeProps(Map<String, Object> props) {
            int count = 0;
            for (Object o : props.values()) {
                if (o != null) {
                    count++;
                }
            }
            writeMapHeader(count);
            for (Map.Entry<String, Object> e : props.entrySet()) {
                if (e.getValue() != null) {
                    writeString(e.getKey());
                    writeValue(e.getValue());
                }
            }
        }

        private void writeRel(RelV r) {
            writeStructHeader(major >= 5 ? 8 : 5, 0x52);
            writeInt(r.id);
            writeInt(r.start);
            writeInt(r.end);
            writeString(r.type);
            if (r.deleted) {
                writeMapHeader(0);
            } else {
                writeProps(r.props);
            }
            if (major >= 5) {
                writeString(Funcs.elementId(5, r.id));
                writeString(Funcs.elementId(4, r.start));
                writeString(Funcs.elementId(4, r.end));
            }
        }

        private void writeUnboundRel(RelV r) {
            writeStructHeader(major >= 5 ? 4 : 3, 0x72);
            writeInt(r.id);
            writeString(r.type);
            writeProps(r.props);
            if (major >= 5) {
                writeString(Funcs.elementId(5, r.id));
            }
        }

        private void writePath(PathV p) {
            List<NodeV> nodes = new ArrayList<>();
            IdentityHashMap<NodeV, Integer> nodeIdx = new IdentityHashMap<>();
            List<RelV> rels = new ArrayList<>();
            IdentityHashMap<RelV, Integer> relIdx = new IdentityHashMap<>();
            List<Long> seq = new ArrayList<>();
            for (NodeV n : p.nodes()) {
                if (!nodeIdx.containsKey(n)) {
                    nodeIdx.put(n, nodes.size());
                    nodes.add(n);
                }
            }
            for (int i = 0; i < p.rels().size(); i++) {
                RelV r = p.rels().get(i);
                if (!relIdx.containsKey(r)) {
                    relIdx.put(r, rels.size() + 1);
                    rels.add(r);
                }
                NodeV prev = p.nodes().get(i);
                boolean forward = r.start == prev.id;
                int ri = relIdx.get(r);
                seq.add((long) (forward ? ri : -ri));
                seq.add((long) nodeIdx.get(p.nodes().get(i + 1)));
            }
            writeStructHeader(3, 0x50);
            writeListHeader(nodes.size());
            for (NodeV n : nodes) {
                writeNode(n);
            }
            writeListHeader(rels.size());
            for (RelV r : rels) {
                writeUnboundRel(r);
            }
            writeListHeader(seq.size());
            for (Long l : seq) {
                writeInt(l);
            }
        }

        private void writeBE(long v, int bytes) {
            for (int i = bytes - 1; i >= 0; i--) {
                buf.write((int) ((v >>> (8 * i)) & 0xFF));
            }
        }
    }

    // ---- reader ----

    /** One decoded top-level Bolt message: {@code tag} is the message signature byte. */
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

        /** A top-level Bolt message (fields decoded as values). */
        Struct readMessage() {
            int marker = data[pos++] & 0xFF;
            if ((marker & 0xF0) != 0xB0) {
                throw new IllegalStateException("boltwire: expected a PackStream struct (a Bolt message), got marker 0x"
                        + Integer.toHexString(marker));
            }
            int n = marker & 0x0F;
            int tag = data[pos++] & 0xFF;
            List<Object> fields = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                fields.add(readValue());
            }
            return new Struct(tag, fields);
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
                return Double.longBitsToDouble(readBE(8, false));
            }
            if (marker == 0xCC) {
                return readBytes(data[pos++] & 0xFF);
            }
            if (marker == 0xCD) {
                return readBytes((int) readBE(2, false));
            }
            if (marker == 0xCE) {
                return readBytes((int) readBE(4, false));
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
                List<Object> f = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    f.add(readValue());
                }
                return decodeStruct(tag, f);
            }
            throw new IllegalStateException(
                    String.format("boltwire: unrecognized PackStream marker 0x%02x at position %d", marker, pos - 1));
        }

        private static Object decodeStruct(int tag, List<Object> f) {
            switch (tag) {
                case 0x44 -> {
                    return LocalDate.ofEpochDay((Long) f.get(0));
                }
                case 0x74 -> {
                    return LocalTime.ofNanoOfDay((Long) f.get(0));
                }
                case 0x54 -> {
                    return OffsetTime.of(LocalTime.ofNanoOfDay((Long) f.get(0)), ZoneOffset.ofTotalSeconds(((Long) f.get(1)).intValue()));
                }
                case 0x64 -> {
                    return LocalDateTime.ofEpochSecond((Long) f.get(0), ((Long) f.get(1)).intValue(), ZoneOffset.UTC);
                }
                case 0x46 -> { // legacy DateTime with offset: local seconds
                    ZoneOffset off = ZoneOffset.ofTotalSeconds(((Long) f.get(2)).intValue());
                    LocalDateTime l = LocalDateTime.ofEpochSecond((Long) f.get(0), ((Long) f.get(1)).intValue(), ZoneOffset.UTC);
                    return ZonedDateTime.of(l, off);
                }
                case 0x66 -> { // legacy DateTime with zone id
                    LocalDateTime l = LocalDateTime.ofEpochSecond((Long) f.get(0), ((Long) f.get(1)).intValue(), ZoneOffset.UTC);
                    return ZonedDateTime.of(l, ZoneId.of((String) f.get(2)));
                }
                case 0x49 -> { // UTC DateTime with offset
                    ZoneOffset off = ZoneOffset.ofTotalSeconds(((Long) f.get(2)).intValue());
                    return ZonedDateTime.ofInstant(Instant.ofEpochSecond((Long) f.get(0), (Long) f.get(1)), off);
                }
                case 0x69 -> {
                    return ZonedDateTime.ofInstant(Instant.ofEpochSecond((Long) f.get(0), (Long) f.get(1)),
                            ZoneId.of((String) f.get(2)));
                }
                case 0x45 -> {
                    return new DurationV((Long) f.get(0), (Long) f.get(1), (Long) f.get(2), ((Long) f.get(3)).intValue());
                }
                case 0x58 -> {
                    return new PointV(((Long) f.get(0)).intValue(), (Double) f.get(1), (Double) f.get(2), null);
                }
                case 0x59 -> {
                    return new PointV(((Long) f.get(0)).intValue(), (Double) f.get(1), (Double) f.get(2), (Double) f.get(3));
                }
                default -> throw new IllegalStateException(String.format("boltwire: unsupported PackStream struct 0x%02x", tag));
            }
        }

        private byte[] readBytes(int n) {
            byte[] b = new byte[n];
            System.arraycopy(data, pos, b, 0, n);
            pos += n;
            return b;
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
