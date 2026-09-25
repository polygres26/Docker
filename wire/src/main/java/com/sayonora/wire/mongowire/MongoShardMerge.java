package com.sayonora.wire.mongowire;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * Exact merge of {@code $group} results computed independently on several backends: groups with
 * the same {@code _id} are combined ({@code $sum} adds, {@code $min}/{@code $max} keep the extreme,
 * {@code $avg} is rebuilt from its per-shard sum and count), then the pipeline's {@code $sort} and
 * {@code $limit} are applied to the merged groups.
 */
final class MongoShardMerge {

    private MongoShardMerge() {
    }

    static List<Document> merge(MongoAggregationTranslator.ShardMerge plan, List<List<Document>> perShard) {
        Map<String, Document> groups = new LinkedHashMap<>();
        Map<String, Map<String, BigDecimal>> sums = new LinkedHashMap<>();
        Map<String, Map<String, BigDecimal>> counts = new LinkedHashMap<>();
        for (List<Document> shard : perShard) {
            for (Document row : shard) {
                Object id = row.get("_id");
                String key = new Document("v", id).toJson();
                Document acc = groups.computeIfAbsent(key, k -> {
                    Document d = new Document();
                    d.put("_id", id);
                    return d;
                });
                for (int i = 0; i < plan.fields().size(); i++) {
                    String f = plan.fields().get(i);
                    String op = plan.ops().get(i);
                    switch (op) {
                        case "$sum" -> {
                            Object v = row.get(f);
                            if (v != null) {
                                BigDecimal cur = acc.get(f) == null ? BigDecimal.ZERO : new BigDecimal(acc.get(f).toString());
                                acc.put(f, cur.add(new BigDecimal(v.toString())));
                            } else if (!acc.containsKey(f)) {
                                acc.put(f, null);
                            }
                        }
                        case "$min", "$max" -> {
                            Object v = row.get(f);
                            Object cur = acc.get(f);
                            if (v != null && (cur == null
                                    || ("$min".equals(op) ? String.valueOf(v).compareTo(String.valueOf(cur)) < 0
                                            : String.valueOf(v).compareTo(String.valueOf(cur)) > 0))) {
                                acc.put(f, v);
                            } else if (!acc.containsKey(f)) {
                                acc.put(f, null);
                            }
                        }
                        case "$avg" -> {
                            Object s = row.get(f + "__sum");
                            Object c = row.get(f + "__cnt");
                            Map<String, BigDecimal> sm = sums.computeIfAbsent(key, k -> new LinkedHashMap<>());
                            Map<String, BigDecimal> cm = counts.computeIfAbsent(key, k -> new LinkedHashMap<>());
                            if (s != null) {
                                sm.merge(f, new BigDecimal(s.toString()), BigDecimal::add);
                            }
                            cm.merge(f, c == null ? BigDecimal.ZERO : new BigDecimal(c.toString()), BigDecimal::add);
                        }
                        default -> throw new IllegalArgumentException("unsupported accumulator " + op);
                    }
                }
            }
        }
        List<Document> out = new ArrayList<>();
        for (Map.Entry<String, Document> e : groups.entrySet()) {
            Document d = e.getValue();
            Document ordered = new Document();
            ordered.put("_id", d.get("_id"));
            for (int i = 0; i < plan.fields().size(); i++) {
                String f = plan.fields().get(i);
                if ("$avg".equals(plan.ops().get(i))) {
                    BigDecimal cnt = counts.getOrDefault(e.getKey(), Map.of()).get(f);
                    BigDecimal sum = sums.getOrDefault(e.getKey(), Map.of()).get(f);
                    ordered.put(f, cnt == null || cnt.signum() == 0 || sum == null ? null
                            : (Object) sum.divide(cnt, MathContext.DECIMAL64).doubleValue());
                } else if ("$sum".equals(plan.ops().get(i)) && d.get(f) instanceof BigDecimal bd) {
                    ordered.put(f, number(bd));
                } else {
                    ordered.put(f, d.get(f));
                }
            }
            out.add(ordered);
        }
        BsonDocument sort = plan.sortSpec();
        if (sort != null && !sort.isEmpty()) {
            Comparator<Document> cmp = null;
            for (Map.Entry<String, BsonValue> s : sort.entrySet()) {
                String field = s.getKey();
                boolean desc = s.getValue().asNumber().intValue() < 0;
                Comparator<Document> c = (a, b) -> compareValues(a.get(field), b.get(field));
                c = desc ? c.reversed() : c;
                cmp = cmp == null ? c : cmp.thenComparing(c);
            }
            out.sort(cmp);
        }
        if (plan.limit() != null && plan.limit() > 0 && out.size() > plan.limit()) {
            return new ArrayList<>(out.subList(0, plan.limit()));
        }
        return out;
    }

    /** Integral results stay integers (as the single-backend path returns), others become doubles. */
    private static Object number(BigDecimal v) {
        BigDecimal s = v.stripTrailingZeros();
        if (s.scale() <= 0) {
            try {
                long l = s.longValueExact();
                return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) (int) l : (Object) l;
            } catch (ArithmeticException e) {
                return v.doubleValue();
            }
        }
        return v.doubleValue();
    }

    private static int rank(Object v) {
        return v == null ? 0 : v instanceof Boolean ? 2 : v instanceof Number ? 1 : 3;
    }

    /** jsonb-like ordering: null, numbers (numerically), booleans, strings. */
    static int compareValues(Object a, Object b) {
        int ra = rank(a);
        int rb = rank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        if (a instanceof Number x && b instanceof Number y) {
            return new BigDecimal(x.toString()).compareTo(new BigDecimal(y.toString()));
        }
        return a == null ? 0 : String.valueOf(a).compareTo(String.valueOf(b));
    }
}
