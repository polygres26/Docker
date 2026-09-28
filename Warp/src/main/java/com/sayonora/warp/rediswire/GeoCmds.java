package com.sayonora.warp.rediswire;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Geo commands on top of sorted sets whose score is the 52-bit interleaved geohash (exactly like Redis). */
final class GeoCmds {

    static final double LAT_MIN = -85.05112878;
    static final double LAT_MAX = 85.05112878;
    static final double LON_MIN = -180;
    static final double LON_MAX = 180;
    static final double EARTH_RADIUS = 6372797.560856;
    private static final String ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz";

    private GeoCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("geoadd", -5, "write denyoom", 1, 1, 1, "@write @geo @slow", GeoCmds::geoadd);
        r.add("geopos", -2, "readonly", 1, 1, 1, "@read @geo @slow", GeoCmds::geopos);
        r.add("geodist", -4, "readonly", 1, 1, 1, "@read @geo @slow", GeoCmds::geodist);
        r.add("geohash", -2, "readonly", 1, 1, 1, "@read @geo @slow", GeoCmds::geohash);
        r.add("geosearch", -7, "readonly", 1, 1, 1, "@read @geo @slow", (x, a) -> search(x, a, "geosearch", false));
        r.add("geosearchstore", -8, "write denyoom", 1, 2, 1, "@write @geo @slow", (x, a) -> search(x, a, "geosearchstore", false));
        r.add("georadius", -6, "write denyoom movablekeys", 1, 1, 1, "@write @geo @slow", (x, a) -> search(x, a, "georadius", false))
                .keys(a -> geoStoreKeys(a));
        r.add("georadius_ro", -6, "readonly", 1, 1, 1, "@read @geo @slow", (x, a) -> search(x, a, "georadius_ro", true));
        r.add("georadiusbymember", -5, "write denyoom movablekeys", 1, 1, 1, "@write @geo @slow", (x, a) -> search(x, a, "georadiusbymember", false))
                .keys(a -> geoStoreKeys(a));
        r.add("georadiusbymember_ro", -5, "readonly", 1, 1, 1, "@read @geo @slow", (x, a) -> search(x, a, "georadiusbymember_ro", true));
    }

    private static int[] geoStoreKeys(byte[][] a) {
        List<Integer> ks = new ArrayList<>();
        ks.add(1);
        for (int i = 2; i + 1 < a.length; i++) {
            if (Num.eq(a[i], "STORE") || Num.eq(a[i], "STOREDIST")) {
                ks.add(i + 1);
            }
        }
        int[] r = new int[ks.size()];
        for (int i = 0; i < r.length; i++) {
            r[i] = ks.get(i);
        }
        return r;
    }

    // ------------------------------------------------------------------------------------------
    // geohash arithmetic (ported from Redis geohash.c / geohash_helper.c)
    // ------------------------------------------------------------------------------------------

    private static long spread(long v) {
        v &= 0xFFFFFFFFL;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFFL;
        v = (v | (v << 8)) & 0x00FF00FF00FF00FFL;
        v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0FL;
        v = (v | (v << 2)) & 0x3333333333333333L;
        v = (v | (v << 1)) & 0x5555555555555555L;
        return v;
    }

    private static long squash(long v) {
        v &= 0x5555555555555555L;
        v = (v | (v >>> 1)) & 0x3333333333333333L;
        v = (v | (v >>> 2)) & 0x0F0F0F0F0F0F0F0FL;
        v = (v | (v >>> 4)) & 0x00FF00FF00FF00FFL;
        v = (v | (v >>> 8)) & 0x0000FFFF0000FFFFL;
        v = (v | (v >>> 16)) & 0x00000000FFFFFFFFL;
        return v;
    }

    static long encode(double lon, double lat, double latMin, double latMax) {
        double latOffset = (lat - latMin) / (latMax - latMin);
        double lonOffset = (lon - LON_MIN) / (LON_MAX - LON_MIN);
        latOffset *= (1L << 26);
        lonOffset *= (1L << 26);
        long ilat = (long) latOffset;
        long ilon = (long) lonOffset;
        return spread(ilat) | (spread(ilon) << 1);
    }

    /** {lon, lat} of the cell centre. */
    static double[] decode(long bits) {
        long ilat = squash(bits);
        long ilon = squash(bits >>> 1);
        double latRange = LAT_MAX - LAT_MIN;
        double lonRange = LON_MAX - LON_MIN;
        // fused multiply-add: Redis built for arm64 contracts a + b * c (matches the reference server's last digit)
        double latMin = Math.fma((double) ilat / (1L << 26), latRange, LAT_MIN);
        double latMax = Math.fma((double) (ilat + 1) / (1L << 26), latRange, LAT_MIN);
        double lonMin = Math.fma((double) ilon / (1L << 26), lonRange, LON_MIN);
        double lonMax = Math.fma((double) (ilon + 1) / (1L << 26), lonRange, LON_MIN);
        double lon = (lonMin + lonMax) / 2;
        double lat = (latMin + latMax) / 2;
        return new double[] {Math.min(lon, LON_MAX), Math.min(lat, LAT_MAX)};
    }

    static double distance(double lon1, double lat1, double lon2, double lat2) {
        double lat1r = Math.toRadians(lat1);
        double lon1r = Math.toRadians(lon1);
        double lat2r = Math.toRadians(lat2);
        double lon2r = Math.toRadians(lon2);
        double u = Math.sin((lat2r - lat1r) / 2);
        double v = Math.sin((lon2r - lon1r) / 2);
        if (v == 0.0) {
            return EARTH_RADIUS * Math.abs(lat2r - lat1r);
        }
        double a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v;
        return 2.0 * EARTH_RADIUS * Math.asin(Math.sqrt(a));
    }

    private static double unit(byte[] u) {
        String s = Num.str(u).toLowerCase(Locale.ROOT);
        return switch (s) {
            case "m" -> 1;
            case "km" -> 1000;
            case "ft" -> 0.3048;
            case "mi" -> 1609.34;
            default -> throw RedisError.err("unsupported unit provided. please use M, KM, FT, MI");
        };
    }

    private static String f4(double d) {
        return String.format(Locale.ROOT, "%.4f", d);
    }

    private static byte[] coord(double d) {
        return Num.bytes(Num.fmtLongDouble(new BigDecimal(d)));
    }

    private static Double parseDouble(byte[] b) {
        Double d = Num.parseDouble(b);
        if (d == null) {
            throw RedisError.notFloat();
        }
        return d;
    }

    private static void checkLonLat(double lon, double lat) {
        if (lon < LON_MIN || lon > LON_MAX || lat < LAT_MIN || lat > LAT_MAX) {
            throw RedisError.err(String.format(Locale.ROOT, "invalid longitude,latitude pair %f,%f", lon, lat));
        }
    }

    // ------------------------------------------------------------------------------------------

    private static Object geoadd(Ctx x, byte[][] a) throws Exception {
        int i = 2;
        boolean nx = false;
        boolean xx = false;
        boolean ch = false;
        for (; i < a.length; i++) {
            if (Num.eq(a[i], "NX")) {
                nx = true;
            } else if (Num.eq(a[i], "XX")) {
                xx = true;
            } else if (Num.eq(a[i], "CH")) {
                ch = true;
            } else {
                break;
            }
        }
        if ((a.length - i) == 0 || (a.length - i) % 3 != 0) {
            throw RedisError.syntax();
        }
        if (nx && xx) {
            throw RedisError.syntax();
        }
        List<byte[]> za = new ArrayList<>();
        za.add(Num.bytes("ZADD"));
        za.add(a[1]);
        if (nx) {
            za.add(Num.bytes("NX"));
        }
        if (xx) {
            za.add(Num.bytes("XX"));
        }
        if (ch) {
            za.add(Num.bytes("CH"));
        }
        for (int j = i; j < a.length; j += 3) {
            double lon = parseDouble(a[j]);
            double lat = parseDouble(a[j + 1]);
            checkLonLat(lon, lat);
            za.add(Num.bytes(Long.toString(encode(lon, lat, LAT_MIN, LAT_MAX))));
            za.add(a[j + 2]);
        }
        return ZSetCmds.zadd(x, za.toArray(new byte[0][]));
    }

    private static List<Object[]> members(Ctx x, byte[] key, byte[][] ms) throws Exception {
        Ctx.TypedRead<Object[]> t = x.typedRead(key, Ctx.T_ZSET, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ANY(?)",
                rs -> new Object[] {rs.getBytes(2), rs.getDouble(3)}, x.db, key, ms);
        t.expect(Ctx.T_ZSET);
        return t.rows();
    }

    private static Double scoreOf(List<Object[]> rows, byte[] m) {
        for (Object[] r : rows) {
            if (Arrays.equals((byte[]) r[0], m)) {
                return (Double) r[1];
            }
        }
        return null;
    }

    private static Object geopos(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        List<Object[]> rows = members(x, a[1], ms);
        List<Object> out = new ArrayList<>();
        for (byte[] m : ms) {
            Double s = scoreOf(rows, m);
            if (s == null) {
                out.add(Resp.NIL_ARRAY);
            } else {
                double[] c = decode(s.longValue());
                out.add(new ArrayList<Object>(List.of(coord(c[0]), coord(c[1]))));
            }
        }
        return out;
    }

    private static Object geodist(Ctx x, byte[][] a) throws Exception {
        if (a.length > 5) {
            throw RedisError.syntax();
        }
        double u = a.length == 5 ? unit(a[4]) : 1;
        List<Object[]> rows = members(x, a[1], new byte[][] {a[2], a[3]});
        Double s1 = scoreOf(rows, a[2]);
        Double s2 = scoreOf(rows, a[3]);
        if (s1 == null || s2 == null) {
            return null;
        }
        double[] c1 = decode(s1.longValue());
        double[] c2 = decode(s2.longValue());
        return Num.bytes(f4(distance(c1[0], c1[1], c2[0], c2[1]) / u));
    }

    private static Object geohash(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        List<Object[]> rows = members(x, a[1], ms);
        List<Object> out = new ArrayList<>();
        for (byte[] m : ms) {
            Double s = scoreOf(rows, m);
            if (s == null) {
                out.add(null);
                continue;
            }
            double[] c = decode(s.longValue());
            long hash = encode(c[0], c[1], -90, 90);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 11; i++) {
                int idx = i == 10 ? 0 : (int) ((hash >>> (52 - ((i + 1) * 5))) & 0x1f);
                sb.append(ALPHABET.charAt(idx));
            }
            out.add(Num.bytes(sb.toString()));
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------

    private record Hit(byte[] m, double dist, long hash, double lon, double lat) {
    }

    private static Object search(Ctx x, byte[][] a, String cmd, boolean ro) throws Exception {
        boolean store = cmd.equals("geosearchstore");
        boolean legacy = cmd.startsWith("georadius");
        byte[] dst = store ? a[1] : null;
        byte[] key = store ? a[2] : a[1];
        int i = store ? 3 : 2;
        boolean fromMember = false;
        byte[] member = null;
        double lon = 0;
        double lat = 0;
        boolean haveFrom = false;
        boolean byRadius = false;
        boolean byBox = false;
        double radius = 0;
        double width = 0;
        double height = 0;
        double conv = 1;
        boolean withCoord = false;
        boolean withDist = false;
        boolean withHash = false;
        long count = 0;
        boolean any = false;
        int sort = 0; // 0 none, 1 asc, -1 desc
        boolean storeDist = false;
        byte[] legacyStore = null;
        boolean legacyStoreDist = false;
        if (legacy) {
            if (cmd.startsWith("georadiusbymember")) {
                member = a[i];
                fromMember = true;
                haveFrom = true;
                i++;
            } else {
                lon = parseDouble(a[i]);
                lat = parseDouble(a[i + 1]);
                haveFrom = true;
                i += 2;
            }
            if (a.length < i + 2) {
                throw RedisError.syntax();
            }
            radius = parseDouble(a[i]);
            conv = unit(a[i + 1]);
            byRadius = true;
            if (radius < 0) {
                throw RedisError.err("radius cannot be negative");
            }
            radius *= conv;
            i += 2;
        }
        for (; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "WITHCOORD") && !store) {
                withCoord = true;
            } else if (Num.eq(o, "WITHDIST") && !store) {
                withDist = true;
            } else if (Num.eq(o, "WITHHASH") && !store) {
                withHash = true;
            } else if (Num.eq(o, "ASC")) {
                sort = 1;
            } else if (Num.eq(o, "DESC")) {
                sort = -1;
            } else if (Num.eq(o, "COUNT") && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null) {
                    throw RedisError.notInt();
                }
                if (c <= 0) {
                    throw RedisError.err("COUNT must be > 0");
                }
                count = c;
                if (i + 1 < a.length && Num.eq(a[i + 1], "ANY")) {
                    any = true;
                    i++;
                }
            } else if (legacy && !ro && Num.eq(o, "STORE") && i + 1 < a.length) {
                legacyStore = a[++i];
            } else if (legacy && !ro && Num.eq(o, "STOREDIST") && i + 1 < a.length) {
                legacyStore = a[++i];
                legacyStoreDist = true;
            } else if (!legacy && Num.eq(o, "FROMMEMBER") && i + 1 < a.length && !haveFrom) {
                member = a[++i];
                fromMember = true;
                haveFrom = true;
            } else if (!legacy && Num.eq(o, "FROMLONLAT") && i + 2 < a.length && !haveFrom) {
                lon = parseDouble(a[++i]);
                lat = parseDouble(a[++i]);
                haveFrom = true;
            } else if (!legacy && Num.eq(o, "BYRADIUS") && i + 2 < a.length && !byRadius && !byBox) {
                radius = parseDouble(a[++i]);
                conv = unit(a[++i]);
                if (radius < 0) {
                    throw RedisError.err("radius cannot be negative");
                }
                radius *= conv;
                byRadius = true;
            } else if (!legacy && Num.eq(o, "BYBOX") && i + 3 < a.length && !byRadius && !byBox) {
                width = parseDouble(a[++i]);
                height = parseDouble(a[++i]);
                conv = unit(a[++i]);
                if (width < 0 || height < 0) {
                    throw RedisError.err("height or width cannot be negative");
                }
                width *= conv;
                height *= conv;
                byBox = true;
            } else if (store && Num.eq(o, "STOREDIST")) {
                storeDist = true;
            } else {
                throw RedisError.syntax();
            }
        }
        if (!legacy) {
            if (!haveFrom) {
                throw RedisError.err("exactly one of FROMMEMBER or FROMLONLAT can be specified for " + cmd.toUpperCase(Locale.ROOT));
            }
            if (!byRadius && !byBox) {
                throw RedisError.err("exactly one of BYRADIUS and BYBOX can be specified for " + cmd.toUpperCase(Locale.ROOT));
            }
        }
        if (any && count == 0) {
            throw RedisError.err("the ANY argument requires COUNT argument");
        }
        if (legacyStore != null) {
            dst = legacyStore;
            storeDist = legacyStoreDist;
            if (withCoord || withDist || withHash) {
                throw RedisError.err("STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options");
            }
        }
        if (dst != null && (withCoord || withDist || withHash)) {
            throw RedisError.syntax();
        }
        if (count > 0 && sort == 0 && !any) {
            sort = 1;
        }
        // all members of the key
        Ctx.TypedRead<Object[]> t = x.typedRead(key, Ctx.T_ZSET, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY score, m",
                rs -> new Object[] {rs.getBytes(2), rs.getDouble(3)}, x.db, key);
        t.expect(Ctx.T_ZSET);
        if (fromMember) {
            Double s = scoreOf(t.rows(), member);
            if (s == null) {
                throw RedisError.err("could not decode requested zset member");
            }
            double[] c = decode(s.longValue());
            lon = c[0];
            lat = c[1];
        } else {
            checkLonLat(lon, lat);
        }
        List<Hit> hits = new ArrayList<>();
        for (Object[] r : t.rows()) {
            long bits = ((Double) r[1]).longValue();
            double[] c = decode(bits);
            double d = distance(lon, lat, c[0], c[1]);
            boolean in;
            if (byRadius) {
                in = d <= radius;
            } else {
                double latDist = EARTH_RADIUS * Math.abs(Math.toRadians(c[1]) - Math.toRadians(lat));
                in = latDist <= height / 2 && distance(lon, c[1], c[0], c[1]) <= width / 2;
                d = distance(lon, lat, c[0], c[1]);
            }
            if (in) {
                hits.add(new Hit((byte[]) r[0], d, bits, c[0], c[1]));
                if (any && hits.size() >= count) {
                    break;
                }
            }
        }
        if (sort != 0) {
            final int fs = sort;
            hits.sort((p, q) -> fs * Double.compare(p.dist(), q.dist()));
        }
        if (count > 0 && hits.size() > count) {
            hits = new ArrayList<>(hits.subList(0, (int) count));
        }
        if (dst != null) {
            final List<Hit> res = hits;
            final boolean sd = storeDist;
            final byte[] fdst = dst;
            final double fconv = conv;
            return x.atomic(() -> {
                x.delete(fdst);
                if (res.isEmpty()) {
                    return 0L;
                }
                List<byte[]> za = new ArrayList<>();
                za.add(Num.bytes("ZADD"));
                za.add(fdst);
                for (Hit h : res) {
                    za.add(sd ? Num.bytes(Num.fmtDouble(h.dist() / fconv)) : Num.bytes(Long.toString(h.hash())));
                    za.add(h.m());
                }
                ZSetCmds.zadd(x, za.toArray(new byte[0][]));
                return (long) res.size();
            });
        }
        List<Object> out = new ArrayList<>();
        for (Hit h : hits) {
            if (!withCoord && !withDist && !withHash) {
                out.add(h.m());
                continue;
            }
            List<Object> item = new ArrayList<>();
            item.add(h.m());
            if (withDist) {
                item.add(Num.bytes(f4(h.dist() / conv)));
            }
            if (withHash) {
                item.add(h.hash());
            }
            if (withCoord) {
                item.add(new ArrayList<Object>(List.of(coord(h.lon()), coord(h.lat()))));
            }
            out.add(item);
        }
        return out;
    }
}
