package com.sayonora.wire.influxwire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** In-memory {@link InfluxBackend} for engine unit tests (no shards, no type-conflict tracking). */
final class MemoryInfluxBackend implements InfluxBackend {

    final TreeSet<String> dbs = new TreeSet<>();
    final Map<String, List<RetentionPolicy>> rps = new LinkedHashMap<>();
    /** key: db/rp/measurement */
    final Map<String, List<Pt>> data = new LinkedHashMap<>();

    private static String key(String db, String rp, String m) {
        return db + "/" + rp + "/" + m;
    }

    @Override
    public List<String> databases() {
        return new ArrayList<>(dbs);
    }

    @Override
    public boolean databaseExists(String db) {
        return dbs.contains(db);
    }

    @Override
    public void createDatabase(String db, RetentionPolicy rp) {
        if (dbs.add(db)) {
            rps.computeIfAbsent(db, d -> new ArrayList<>()).add(rp != null ? rp : new RetentionPolicy("autogen", 0, 168L * 3600_000_000_000L, 1, true));
        }
    }

    @Override
    public void dropDatabase(String db) {
        dbs.remove(db);
        rps.remove(db);
        data.keySet().removeIf(k -> k.startsWith(db + "/"));
    }

    @Override
    public List<RetentionPolicy> retentionPolicies(String db) {
        return rps.getOrDefault(db, List.of());
    }

    @Override
    public String defaultRetentionPolicy(String db) {
        return "autogen";
    }

    @Override
    public void createRetentionPolicy(String db, RetentionPolicy rp) {
        rps.get(db).add(rp);
    }

    @Override
    public void alterRetentionPolicy(String db, RetentionPolicy rp) {
    }

    @Override
    public void dropRetentionPolicy(String db, String rp) {
        rps.get(db).removeIf(r -> r.name().equals(rp));
    }

    @Override
    public List<String> measurements(String db, String rp) {
        TreeSet<String> out = new TreeSet<>();
        for (var e : data.entrySet()) {
            String[] k = e.getKey().split("/", 3);
            if (k[0].equals(db) && k[1].equals(rp) && !e.getValue().isEmpty()) {
                out.add(k[2]);
            }
        }
        return new ArrayList<>(out);
    }

    @Override
    public Schema schema(String db, String rp, String m) {
        Schema s = new Schema();
        for (Pt p : data.getOrDefault(key(db, rp, m), List.of())) {
            p.fields.forEach((k, v) -> s.fields.putIfAbsent(k, InfluxSelect.typeName(v)));
            s.tagKeys.addAll(p.tags.keySet());
        }
        return s;
    }

    @Override
    public List<Pt> fetch(String db, String rp, String m, Schema schema, Filter filter) {
        return new ArrayList<>(data.getOrDefault(key(db, rp, m), List.of()));
    }

    @Override
    public List<Map<String, String>> series(String db, String rp, String m, Filter filter) {
        List<Map<String, String>> out = new ArrayList<>();
        for (Pt p : data.getOrDefault(key(db, rp, m), List.of())) {
            if (!out.contains(p.tags)) {
                out.add(p.tags);
            }
        }
        return out;
    }

    @Override
    public void delete(String db, String rp, String m, Filter f) {
        data.getOrDefault(key(db, rp, m), new ArrayList<>()).removeIf(p -> p.time >= f.lo && p.time <= f.hi && (f.exact == null || f.exact.contains(p.tags)));
    }

    @Override
    public void dropMeasurement(String db, String rp, String m) {
        data.remove(key(db, rp, m));
    }

    @Override
    public WriteOutcome write(String db, String rp, List<InfluxPoint> points, boolean autoCreate) {
        if (!dbs.contains(db)) {
            createDatabase(db, null);
        }
        for (InfluxPoint p : points) {
            List<Pt> list = data.computeIfAbsent(key(db, rp == null ? "autogen" : rp, p.measurement()), k -> new ArrayList<>());
            Pt existing = null;
            for (Pt e : list) {
                if (e.time == p.timestampNanos() && e.tags.equals(p.tags())) {
                    existing = e;
                }
            }
            if (existing != null) {
                existing.fields.putAll(p.fields());
            } else {
                list.add(new Pt(p.timestampNanos(), new TreeMap<>(p.tags()), new LinkedHashMap<>(p.fields())));
            }
        }
        return new WriteOutcome(0, null);
    }
}
