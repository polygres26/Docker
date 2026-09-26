package com.sayonora.warp.boltwire;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Runtime values and Cypher's value semantics (equality, comparability, orderability, grouping keys, type names). */
final class Values {

    private Values() {
    }

    // ---------------------------------------------------------------------------------------- entities

    /** A node as seen by one statement: a single instance per node id per {@link Exec}, updated in place by writes. */
    static final class NodeV {
        final long id;
        final Set<String> labels = new LinkedHashSet<>();
        final Map<String, Object> props = new LinkedHashMap<>();
        boolean deleted;
        boolean dirty;

        NodeV(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NodeV n && n.id == id;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(id) * 31 + 1;
        }
    }

    static final class RelV {
        final long id;
        final String type;
        final long start;
        final long end;
        final Map<String, Object> props = new LinkedHashMap<>();
        boolean deleted;
        boolean dirty;

        RelV(long id, String type, long start, long end) {
            this.id = id;
            this.type = type;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RelV r && r.id == id;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(id) * 31 + 2;
        }
    }

    /** nodes.size() == rels.size() + 1. */
    record PathV(List<NodeV> nodes, List<RelV> rels) {
        @Override
        public boolean equals(Object o) {
            return o instanceof PathV p && p.nodes.equals(nodes) && p.rels.equals(rels);
        }

        @Override
        public int hashCode() {
            return nodes.hashCode() * 31 + rels.hashCode();
        }
    }

    record DurationV(long months, long days, long seconds, int nanos) {
    }

    record PointV(int srid, double x, double y, Double z) {
        boolean geographic() {
            return srid == 4326 || srid == 4979;
        }
    }

    // ---------------------------------------------------------------------------------------- type names

    static String typeName(Object v) {
        if (v == null) {
            return "Null";
        }
        if (v instanceof Boolean) {
            return "Boolean";
        }
        if (v instanceof Long) {
            return "Integer";
        }
        if (v instanceof Double) {
            return "Float";
        }
        if (v instanceof String) {
            return "String";
        }
        if (v instanceof List) {
            return "List";
        }
        if (v instanceof Map) {
            return "Map";
        }
        if (v instanceof NodeV) {
            return "Node";
        }
        if (v instanceof RelV) {
            return "Relationship";
        }
        if (v instanceof PathV) {
            return "Path";
        }
        if (v instanceof LocalDate) {
            return "Date";
        }
        if (v instanceof LocalTime) {
            return "LocalTime";
        }
        if (v instanceof OffsetTime) {
            return "Time";
        }
        if (v instanceof LocalDateTime) {
            return "LocalDateTime";
        }
        if (v instanceof ZonedDateTime) {
            return "DateTime";
        }
        if (v instanceof DurationV) {
            return "Duration";
        }
        if (v instanceof PointV) {
            return "Point";
        }
        if (v instanceof byte[]) {
            return "ByteArray";
        }
        return v.getClass().getSimpleName();
    }

    /** Neo4j's valueType() names. */
    static String valueTypeName(Object v) {
        if (v == null) {
            return "NULL";
        }
        if (v instanceof Boolean) {
            return "BOOLEAN NOT NULL";
        }
        if (v instanceof Long) {
            return "INTEGER NOT NULL";
        }
        if (v instanceof Double) {
            return "FLOAT NOT NULL";
        }
        if (v instanceof String) {
            return "STRING NOT NULL";
        }
        if (v instanceof List<?> l) {
            String inner = null;
            for (Object o : l) {
                String t = valueTypeName(o);
                inner = inner == null ? t : (inner.equals(t) ? inner : "ANY");
            }
            return "LIST<" + (inner == null ? "NOTHING" : inner) + "> NOT NULL";
        }
        if (v instanceof Map) {
            return "MAP NOT NULL";
        }
        if (v instanceof NodeV) {
            return "NODE NOT NULL";
        }
        if (v instanceof RelV) {
            return "RELATIONSHIP NOT NULL";
        }
        if (v instanceof PathV) {
            return "PATH NOT NULL";
        }
        if (v instanceof LocalDate) {
            return "DATE NOT NULL";
        }
        if (v instanceof LocalTime) {
            return "LOCAL TIME NOT NULL";
        }
        if (v instanceof OffsetTime) {
            return "ZONED TIME NOT NULL";
        }
        if (v instanceof LocalDateTime) {
            return "LOCAL DATETIME NOT NULL";
        }
        if (v instanceof ZonedDateTime) {
            return "ZONED DATETIME NOT NULL";
        }
        if (v instanceof DurationV) {
            return "DURATION NOT NULL";
        }
        if (v instanceof PointV) {
            return "POINT NOT NULL";
        }
        return "ANY";
    }

    static boolean isNumber(Object v) {
        return v instanceof Long || v instanceof Double;
    }

    // ---------------------------------------------------------------------------------------- equality (3VL)

    /** Cypher's {@code =}: TRUE, FALSE or null (unknown). */
    static Boolean equal(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return x.equals(y);
        }
        if (a instanceof String x && b instanceof String y) {
            return x.equals(y);
        }
        if (isNumber(a) && isNumber(b)) {
            return numEq(a, b);
        }
        if (a instanceof List<?> x && b instanceof List<?> y) {
            if (x.size() != y.size()) {
                return false;
            }
            boolean unknown = false;
            for (int i = 0; i < x.size(); i++) {
                Boolean e = equal(x.get(i), y.get(i));
                if (e == null) {
                    unknown = true;
                } else if (!e) {
                    return false;
                }
            }
            return unknown ? null : Boolean.TRUE;
        }
        if (a instanceof Map<?, ?> x && b instanceof Map<?, ?> y) {
            if (!x.keySet().equals(y.keySet())) {
                return false;
            }
            boolean unknown = false;
            for (Object k : x.keySet()) {
                Boolean e = equal(x.get(k), y.get(k));
                if (e == null) {
                    unknown = true;
                } else if (!e) {
                    return false;
                }
            }
            return unknown ? null : Boolean.TRUE;
        }
        if (a instanceof NodeV x && b instanceof NodeV y) {
            return x.id == y.id;
        }
        if (a instanceof RelV x && b instanceof RelV y) {
            return x.id == y.id;
        }
        if (a instanceof PathV x && b instanceof PathV y) {
            return x.equals(y);
        }
        if (a instanceof PointV x && b instanceof PointV y) {
            if (x.srid != y.srid) {
                return false;
            }
            if (Double.isNaN(x.x) || Double.isNaN(y.x) || Double.isNaN(x.y) || Double.isNaN(y.y)) {
                return false;
            }
            return x.x == y.x && x.y == y.y && java.util.Objects.equals(x.z, y.z);
        }
        if (a instanceof DurationV x && b instanceof DurationV y) {
            return x.equals(y);
        }
        if (a instanceof LocalDate x && b instanceof LocalDate y) {
            return x.equals(y);
        }
        if (a instanceof LocalTime x && b instanceof LocalTime y) {
            return x.equals(y);
        }
        if (a instanceof LocalDateTime x && b instanceof LocalDateTime y) {
            return x.equals(y);
        }
        if (a instanceof OffsetTime x && b instanceof OffsetTime y) {
            return x.equals(y);
        }
        if (a instanceof ZonedDateTime x && b instanceof ZonedDateTime y) {
            return x.equals(y);
        }
        if (a instanceof byte[] x && b instanceof byte[] y) {
            return java.util.Arrays.equals(x, y);
        }
        return false;
    }

    private static Boolean numEq(Object a, Object b) {
        if (a instanceof Long x && b instanceof Long y) {
            return x.longValue() == y.longValue();
        }
        double dx = ((Number) a).doubleValue(), dy = ((Number) b).doubleValue();
        if (Double.isNaN(dx) || Double.isNaN(dy)) {
            return false;
        }
        return dx == dy;
    }

    /** Exact comparison of a long against a (non-NaN) double. */
    private static int cmpLongDouble(long l, double d) {
        if (Double.isInfinite(d)) {
            return d > 0 ? -1 : 1;
        }
        if (d >= 9.223372036854775807E18) {
            return -1;
        }
        if (d < -9.223372036854775808E18) {
            return 1;
        }
        long f = (long) Math.floor(d);
        if (l < f) {
            return -1;
        }
        if (l > f) {
            return 1;
        }
        return d > f ? -1 : 0;
    }

    // ---------------------------------------------------------------------------------------- comparability

    /** Cypher's {@code <, >, <=, >=} ordering for comparable operands: a negative/zero/positive int, or null when the
     * operands are not comparable (different types, NaN, null inside lists that decide the result, ...). */
    static Integer compare(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (isNumber(a) && isNumber(b)) {
            if (a instanceof Long x && b instanceof Long y) {
                return Long.compare(x, y);
            }
            double dx = ((Number) a).doubleValue(), dy = ((Number) b).doubleValue();
            if (Double.isNaN(dx) || Double.isNaN(dy)) {
                return null;
            }
            if (a instanceof Long x) {
                return cmpLongDouble(x, (Double) b);
            }
            if (b instanceof Long y) {
                return -cmpLongDouble(y, (Double) a);
            }
            return Double.compare(dx, dy) == 0 || dx == dy ? 0 : (dx < dy ? -1 : 1);
        }
        if (a instanceof String x && b instanceof String y) {
            return Integer.signum(compareCodePoints(x, y));
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        if (a instanceof List<?> x && b instanceof List<?> y) {
            int n = Math.min(x.size(), y.size());
            for (int i = 0; i < n; i++) {
                Boolean e = equal(x.get(i), y.get(i));
                if (e == null) {
                    return null;
                }
                if (!e) {
                    return compare(x.get(i), y.get(i));
                }
            }
            return Integer.compare(x.size(), y.size());
        }
        if (a instanceof LocalDate x && b instanceof LocalDate y) {
            return x.compareTo(y);
        }
        if (a instanceof LocalTime x && b instanceof LocalTime y) {
            return x.compareTo(y);
        }
        if (a instanceof LocalDateTime x && b instanceof LocalDateTime y) {
            return x.compareTo(y);
        }
        if (a instanceof OffsetTime x && b instanceof OffsetTime y) {
            return Long.compare(utcNanos(x), utcNanos(y));
        }
        if (a instanceof ZonedDateTime x && b instanceof ZonedDateTime y) {
            return x.toInstant().compareTo(y.toInstant());
        }
        if (a instanceof PointV || b instanceof PointV || a instanceof DurationV) {
            return null;
        }
        return null;
    }

    private static long utcNanos(OffsetTime t) {
        return t.toLocalTime().toNanoOfDay() - t.getOffset().getTotalSeconds() * 1_000_000_000L;
    }

    static int compareCodePoints(String x, String y) {
        int i = 0, j = 0;
        while (i < x.length() && j < y.length()) {
            int cx = x.codePointAt(i), cy = y.codePointAt(j);
            if (cx != cy) {
                return Integer.compare(cx, cy);
            }
            i += Character.charCount(cx);
            j += Character.charCount(cy);
        }
        return Integer.compare(x.length() - i, y.length() - j);
    }

    // ---------------------------------------------------------------------------------------- orderability

    private static int rank(Object v) {
        if (v == null) {
            return 100;
        }
        if (v instanceof Map) {
            return 1;
        }
        if (v instanceof NodeV) {
            return 2;
        }
        if (v instanceof RelV) {
            return 3;
        }
        if (v instanceof List) {
            return 4;
        }
        if (v instanceof PathV) {
            return 5;
        }
        if (v instanceof PointV) {
            return 6;
        }
        if (v instanceof ZonedDateTime) {
            return 7;
        }
        if (v instanceof LocalDateTime) {
            return 8;
        }
        if (v instanceof LocalDate) {
            return 9;
        }
        if (v instanceof OffsetTime) {
            return 10;
        }
        if (v instanceof LocalTime) {
            return 11;
        }
        if (v instanceof DurationV) {
            return 12;
        }
        if (v instanceof String) {
            return 13;
        }
        if (v instanceof Boolean) {
            return 14;
        }
        if (isNumber(v)) {
            return v instanceof Double d && d.isNaN() ? 16 : 15;
        }
        return 17;
    }

    /** Total order used by ORDER BY (null last, NaN after every other number). */
    static int order(Object a, Object b) {
        int ra = rank(a), rb = rank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        switch (ra) {
            case 100, 16 -> {
                return 0;
            }
            case 1 -> {
                return orderMaps((Map<?, ?>) a, (Map<?, ?>) b);
            }
            case 2 -> {
                return Long.compare(((NodeV) a).id, ((NodeV) b).id);
            }
            case 3 -> {
                return Long.compare(((RelV) a).id, ((RelV) b).id);
            }
            case 4 -> {
                List<?> x = (List<?>) a, y = (List<?>) b;
                int n = Math.min(x.size(), y.size());
                for (int i = 0; i < n; i++) {
                    int c = order(x.get(i), y.get(i));
                    if (c != 0) {
                        return c;
                    }
                }
                return Integer.compare(x.size(), y.size());
            }
            case 5 -> {
                PathV x = (PathV) a, y = (PathV) b;
                int n = Math.min(x.nodes.size(), y.nodes.size());
                for (int i = 0; i < n; i++) {
                    int c = Long.compare(x.nodes.get(i).id, y.nodes.get(i).id);
                    if (c != 0) {
                        return c;
                    }
                    if (i < x.rels.size() && i < y.rels.size()) {
                        c = Long.compare(x.rels.get(i).id, y.rels.get(i).id);
                        if (c != 0) {
                            return c;
                        }
                    }
                }
                return Integer.compare(x.nodes.size(), y.nodes.size());
            }
            case 6 -> {
                PointV x = (PointV) a, y = (PointV) b;
                int c = Integer.compare(x.srid, y.srid);
                if (c == 0) {
                    c = Double.compare(x.x, y.x);
                }
                if (c == 0) {
                    c = Double.compare(x.y, y.y);
                }
                return c;
            }
            case 12 -> {
                DurationV x = (DurationV) a, y = (DurationV) b;
                int c = Long.compare(x.months, y.months);
                if (c == 0) {
                    c = Long.compare(x.days, y.days);
                }
                if (c == 0) {
                    c = Long.compare(x.seconds, y.seconds);
                }
                if (c == 0) {
                    c = Integer.compare(x.nanos, y.nanos);
                }
                return c;
            }
            case 13 -> {
                return compareCodePoints((String) a, (String) b);
            }
            case 14 -> {
                return Boolean.compare((Boolean) a, (Boolean) b);
            }
            case 15 -> {
                Integer c = compare(a, b);
                return c == null ? 0 : c;
            }
            default -> {
                Integer c = compare(a, b);
                if (c != null) {
                    return c;
                }
                if (a instanceof ZonedDateTime x && b instanceof ZonedDateTime y) {
                    return x.toInstant().compareTo(y.toInstant());
                }
                return 0;
            }
        }
    }

    private static int orderMaps(Map<?, ?> x, Map<?, ?> y) {
        List<String> kx = new ArrayList<>(), ky = new ArrayList<>();
        for (Object k : x.keySet()) {
            kx.add((String) k);
        }
        for (Object k : y.keySet()) {
            ky.add((String) k);
        }
        java.util.Collections.sort(kx);
        java.util.Collections.sort(ky);
        int n = Math.min(kx.size(), ky.size());
        for (int i = 0; i < n; i++) {
            int c = compareCodePoints(kx.get(i), ky.get(i));
            if (c != 0) {
                return c;
            }
            c = order(x.get(kx.get(i)), y.get(ky.get(i)));
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(kx.size(), ky.size());
    }

    // ---------------------------------------------------------------------------------------- grouping keys

    /** A value usable as a HashMap key such that Cypher-equivalent values (1 and 1.0, NaN never equal) group together. */
    static Object groupKey(Object v) {
        if (v == null) {
            return NULL_KEY;
        }
        if (v instanceof Double d) {
            if (d.isNaN()) {
                return new Object();
            }
            if (d == Math.rint(d) && Math.abs(d) < 9.2e18) {
                return (long) d.doubleValue();
            }
            return d;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size() + 1);
            out.add(LIST_TAG);
            for (Object o : l) {
                out.add(groupKey(o));
            }
            return out;
        }
        if (v instanceof Map<?, ?> m) {
            Map<Object, Object> out = new java.util.TreeMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(e.getKey(), groupKey(e.getValue()));
            }
            return List.of(MAP_TAG, out);
        }
        if (v instanceof byte[] b) {
            return List.of("bytes", java.util.Base64.getEncoder().encodeToString(b));
        }
        if (v instanceof OffsetTime t) {
            return List.of("time", utcNanos(t));
        }
        if (v instanceof ZonedDateTime z) {
            return List.of("dt", z.toInstant());
        }
        return v;
    }

    private static final Object NULL_KEY = new Object() {
        @Override
        public String toString() {
            return "null";
        }
    };
    private static final Object LIST_TAG = "\u0000list";
    private static final Object MAP_TAG = "\u0000map";

    // ---------------------------------------------------------------------------------------- misc helpers

    static boolean truthy(Object v) {
        return Boolean.TRUE.equals(v);
    }

    static Map<String, Object> newMap() {
        return new LinkedHashMap<>();
    }
}
