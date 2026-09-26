package com.sayonora.warp.ab;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters and latency histograms per (store, side), compare outcome counters per store, and the bounded ring
 * buffer of recorded compare differences (per node, in memory). Everything here is free of secrets and of
 * request/response payloads unless the policy opts into {@code recordValues}.
 */
public final class AbStats {

    static final double[] BOUNDS_MS = {5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000};

    static final class Side {
        final LongAdder requests = new LongAdder();
        final LongAdder errors = new LongAdder();
        final LongAdder nanos = new LongAdder();
        final LongAdder[] buckets = new LongAdder[BOUNDS_MS.length + 1];

        Side() {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }
    }

    static final class Cmp {
        final LongAdder total = new LongAdder();
        final LongAdder equal = new LongAdder();
        final LongAdder differ = new LongAdder();
        final LongAdder secondaryFailed = new LongAdder();
        final LongAdder dualWriteOk = new LongAdder();
        final LongAdder dualWriteFailed = new LongAdder();
    }

    public record Entry(long ts, String store, String op, String client, String primary, int localStatus, int cloudStatus,
            long localMs, long cloudMs, boolean equal, java.util.List<String> diffs) {
        JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("ts", ts);
            o.addProperty("store", store);
            o.addProperty("op", op);
            o.addProperty("client", client);
            o.addProperty("primary", primary);
            o.addProperty("localStatus", localStatus);
            o.addProperty("cloudStatus", cloudStatus);
            o.addProperty("localMs", localMs);
            o.addProperty("cloudMs", cloudMs);
            o.addProperty("equal", equal);
            JsonArray d = new JsonArray();
            diffs.forEach(d::add);
            o.add("diffs", d);
            return o;
        }
    }

    private final Map<String, Side> sides = new ConcurrentHashMap<>();
    private final Map<String, Cmp> cmps = new ConcurrentHashMap<>();
    private final Map<String, Deque<Entry>> rings = new ConcurrentHashMap<>();

    public void record(String store, AbSide side, int status, long nanos) {
        Side s = sides.computeIfAbsent(store + "|" + side.id(), k -> new Side());
        s.requests.increment();
        if (status <= 0 || status >= 500) {
            s.errors.increment();
        }
        s.nanos.add(nanos);
        double ms = nanos / 1e6;
        int i = 0;
        while (i < BOUNDS_MS.length && ms > BOUNDS_MS[i]) {
            i++;
        }
        s.buckets[i].increment();
    }

    Cmp cmp(String store) {
        return cmps.computeIfAbsent(store, k -> new Cmp());
    }

    public void addCompare(Entry e, int capacity, boolean secondaryFailed) {
        Cmp c = cmp(e.store());
        c.total.increment();
        if (secondaryFailed) {
            c.secondaryFailed.increment();
        }
        (e.equal() ? c.equal : c.differ).increment();
        Deque<Entry> ring = rings.computeIfAbsent(e.store(), k -> new ArrayDeque<>());
        synchronized (ring) {
            ring.addFirst(e);
            while (ring.size() > capacity) {
                ring.removeLast();
            }
        }
    }

    public java.util.List<Entry> entries(String store, boolean onlyDiff, int limit) {
        java.util.List<Entry> out = new java.util.ArrayList<>();
        for (var en : rings.entrySet()) {
            if (store != null && !store.equals(en.getKey())) {
                continue;
            }
            synchronized (en.getValue()) {
                for (Entry e : en.getValue()) {
                    if (!onlyDiff || !e.equal()) {
                        out.add(e);
                    }
                }
            }
        }
        out.sort((a, b) -> Long.compare(b.ts(), a.ts()));
        return out.size() > limit ? out.subList(0, limit) : out;
    }

    public void clearEntries() {
        rings.clear();
    }

    public void reset() {
        sides.clear();
        cmps.clear();
        rings.clear();
    }

    public JsonObject toJson() {
        JsonObject o = new JsonObject();
        JsonObject ss = new JsonObject();
        sides.forEach((k, s) -> {
            JsonObject j = new JsonObject();
            long n = s.requests.sum();
            j.addProperty("requests", n);
            j.addProperty("errors", s.errors.sum());
            j.addProperty("errorRate", n == 0 ? 0.0 : (double) s.errors.sum() / n);
            j.addProperty("avgMs", n == 0 ? 0.0 : s.nanos.sum() / 1e6 / n);
            JsonArray b = new JsonArray();
            for (int i = 0; i < s.buckets.length; i++) {
                JsonObject bj = new JsonObject();
                bj.addProperty("leMs", i < BOUNDS_MS.length ? BOUNDS_MS[i] : -1.0); // -1 = +Inf
                bj.addProperty("count", s.buckets[i].sum());
                b.add(bj);
            }
            j.add("histogram", b);
            ss.add(k, j);
        });
        o.add("sides", ss);
        JsonObject cs = new JsonObject();
        cmps.forEach((k, c) -> {
            JsonObject j = new JsonObject();
            j.addProperty("compared", c.total.sum());
            j.addProperty("equal", c.equal.sum());
            j.addProperty("differ", c.differ.sum());
            j.addProperty("secondaryFailed", c.secondaryFailed.sum());
            j.addProperty("dualWriteOk", c.dualWriteOk.sum());
            j.addProperty("dualWriteFailed", c.dualWriteFailed.sum());
            cs.add(k, j);
        });
        o.add("compare", cs);
        return o;
    }

    /** Prometheus text lines (appended to /metrics). */
    public String prometheus() {
        StringBuilder sb = new StringBuilder();
        sb.append("# HELP warp_ab_requests_total A/B routed requests by store and side.\n# TYPE warp_ab_requests_total counter\n");
        sides.forEach((k, s) -> sb.append("warp_ab_requests_total{store=\"").append(k.split("\\|")[0]).append("\",side=\"")
                .append(k.split("\\|")[1]).append("\"} ").append(s.requests.sum()).append('\n'));
        sb.append("# HELP warp_ab_errors_total A/B routed requests that failed (5xx or transport error).\n# TYPE warp_ab_errors_total counter\n");
        sides.forEach((k, s) -> sb.append("warp_ab_errors_total{store=\"").append(k.split("\\|")[0]).append("\",side=\"")
                .append(k.split("\\|")[1]).append("\"} ").append(s.errors.sum()).append('\n'));
        sb.append("# HELP warp_ab_latency_seconds A/B request latency by store and side.\n# TYPE warp_ab_latency_seconds histogram\n");
        sides.forEach((k, s) -> {
            String lbl = "store=\"" + k.split("\\|")[0] + "\",side=\"" + k.split("\\|")[1] + "\"";
            long cum = 0;
            for (int i = 0; i < s.buckets.length; i++) {
                cum += s.buckets[i].sum();
                sb.append("warp_ab_latency_seconds_bucket{").append(lbl).append(",le=\"")
                        .append(i < BOUNDS_MS.length ? String.valueOf(BOUNDS_MS[i] / 1000.0) : "+Inf").append("\"} ").append(cum).append('\n');
            }
            sb.append("warp_ab_latency_seconds_sum{").append(lbl).append("} ").append(s.nanos.sum() / 1e9).append('\n');
            sb.append("warp_ab_latency_seconds_count{").append(lbl).append("} ").append(s.requests.sum()).append('\n');
        });
        sb.append("# HELP warp_ab_compare_total Compare-mode requests by store and outcome.\n# TYPE warp_ab_compare_total counter\n");
        cmps.forEach((k, c) -> {
            sb.append("warp_ab_compare_total{store=\"").append(k).append("\",outcome=\"equal\"} ").append(c.equal.sum()).append('\n');
            sb.append("warp_ab_compare_total{store=\"").append(k).append("\",outcome=\"differ\"} ").append(c.differ.sum()).append('\n');
        });
        return sb.toString();
    }
}
