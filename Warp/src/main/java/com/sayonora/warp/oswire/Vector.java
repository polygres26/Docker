package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * k-NN ({@code knn} query of the OpenSearch k-NN plugin) and {@code hybrid} (neural-search) queries. There is no
 * pgvector: vectors are read from the document field and ranked by an exact linear scan in the JVM, across every
 * host of the index (so k-NN and hybrid work on sharded indexes too). Scores use the plugin's space-type formulas
 * (l2: 1/(1+d^2), cosinesimil: (1+cos)/2, innerproduct: dot>=0 ? 1+dot : 1/(1-dot)).
 */
final class Vector {

    private Vector() {
    }

    /** Every (index, host) partition a search covers -- what set-level queries (k-NN, hybrid) prepare against. */
    static final class Prep {
        final List<IndexCtx> partitions;

        Prep(List<IndexCtx> partitions) {
            this.partitions = partitions;
        }
    }

    static Query.Node parseKnn(JsonObject body) {
        if (body.size() == 0) {
            throw QueryParser.err("[knn] query malformed, empty fieldname");
        }
        String field = body.keySet().iterator().next();
        JsonObject spec = body.getAsJsonObject(field);
        if (!spec.has("vector")) {
            throw QueryParser.err("[knn] query requires a [vector]");
        }
        JsonArray va = spec.getAsJsonArray("vector");
        float[] vec = new float[va.size()];
        for (int i = 0; i < vec.length; i++) {
            vec[i] = va.get(i).getAsFloat();
        }
        int k = spec.has("k") ? spec.get("k").getAsInt() : 0;
        if (k == 0 && !spec.has("min_score") && !spec.has("max_distance")) {
            k = 10;
        }
        Knn n = new Knn(field, vec, k, spec.has("filter") ? QueryParser.parse(spec.get("filter")) : null,
                spec.has("min_score") ? spec.get("min_score").getAsDouble() : null,
                spec.has("max_distance") ? spec.get("max_distance").getAsDouble() : null);
        if (spec.has("boost")) {
            n.boost = spec.get("boost").getAsDouble();
        }
        return n;
    }

    static Query.Node parseHybrid(JsonObject body) {
        if (!body.has("queries") || !body.get("queries").isJsonArray() || body.getAsJsonArray("queries").isEmpty()) {
            throw QueryParser.err("hybrid query requires at least one entry in \"queries\"");
        }
        List<Query.Node> subs = new ArrayList<>();
        for (JsonElement e : body.getAsJsonArray("queries")) {
            subs.add(QueryParser.parse(e));
        }
        return new Hybrid(subs);
    }

    static double similarity(String space, float[] a, float[] b) {
        switch (space) {
            case "innerproduct" -> {
                double dot = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += (double) a[i] * b[i];
                }
                return dot >= 0 ? dot + 1.0 : 1.0 / (1.0 - dot);
            }
            case "l2" -> {
                double sum = 0;
                for (int i = 0; i < a.length; i++) {
                    double d = (double) a[i] - b[i];
                    sum += d * d;
                }
                return 1.0 / (1.0 + sum);
            }
            default -> {
                double dot = 0, na = 0, nb = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += (double) a[i] * b[i];
                    na += (double) a[i] * a[i];
                    nb += (double) b[i] * b[i];
                }
                double denom = Math.sqrt(na) * Math.sqrt(nb);
                double cos = denom == 0 ? 0 : dot / denom;
                return (1.0 + cos) / 2.0;
            }
        }
    }

    private static boolean legacyUnmapped(IndexCtx ix) {
        return ix.mappings.fields.isEmpty();
    }

    static final class Knn extends Query.Node {
        final String field;
        final float[] vector;
        final int k;
        final Query.Node filter;
        final Double minScore;
        final Double maxDistance;
        private final Map<PostgresSearchStore.Doc, Double> top = new IdentityHashMap<>();
        private boolean prepared;

        Knn(String field, float[] vector, int k, Query.Node filter, Double minScore, Double maxDistance) {
            this.field = field;
            this.vector = vector;
            this.k = k;
            this.filter = filter;
            this.minScore = minScore;
            this.maxDistance = maxDistance;
        }

        List<Query.Node> children() {
            return filter == null ? List.of() : List.of(filter);
        }

        void prepare(Prep p) {
            super.prepare(p);
            top.clear();
            List<Object[]> cands = new ArrayList<>();
            for (IndexCtx ix : p.partitions) {
                Mappings.Field f = ix.mappings.get(field);
                if (f == null || !f.type.equals("knn_vector")) {
                    if (f != null && f.isNumeric() || (f == null && legacyUnmapped(ix))) {
                        // extension: a plain numeric-array field (dynamically mapped float, or an index adopted from an
                        // older oswire) is searchable as a vector, ranked by cosine similarity
                    } else {
                        throw new OpenSearchException("query_shard_exception", "failed to create query: Field '" + field + "' is not knn_vector type.")
                                .with("index", ix.indexName).asShardLevel();
                    }
                }
                String space = "cosinesimil";
                if (f != null && f.def != null) {
                    if (f.def.has("space_type")) {
                        space = f.def.get("space_type").getAsString();
                    } else if (f.def.has("method") && f.def.getAsJsonObject("method").has("space_type")) {
                        space = f.def.getAsJsonObject("method").get("space_type").getAsString();
                    } else if (f.def.has("method")) {
                        space = "l2";
                    } else if (f.def.has("dimension")) {
                        space = "l2";
                    }
                }
                for (PostgresSearchStore.Doc d : ix.corpus) {
                    List<JsonElement> raw = Mappings.values(d.source, field);
                    if (raw.size() != vector.length) {
                        continue;
                    }
                    float[] v = new float[raw.size()];
                    try {
                        for (int i = 0; i < v.length; i++) {
                            v[i] = raw.get(i).getAsFloat();
                        }
                    } catch (RuntimeException e) {
                        continue;
                    }
                    if (filter != null && !Query.matched(filter.score(new Query.DocCtx(ix, d)))) {
                        continue;
                    }
                    double s = similarity(space, vector, v);
                    if (minScore != null && s < minScore) {
                        continue;
                    }
                    cands.add(new Object[] {d, s});
                }
            }
            cands.sort(Comparator.comparingDouble((Object[] o) -> -(Double) o[1]));
            int limit = k > 0 ? Math.min(k, cands.size()) : cands.size();
            for (int i = 0; i < limit; i++) {
                top.put((PostgresSearchStore.Doc) cands.get(i)[0], (Double) cands.get(i)[1]);
            }
            prepared = true;
        }

        double score(Query.DocCtx d) {
            if (!prepared) {
                throw new IllegalStateException("knn query used before prepare");
            }
            Double s = top.get(d.doc);
            return s == null ? Query.NO : s * boost;
        }
    }

    static final class Hybrid extends Query.Node {
        final List<Query.Node> subs;
        private final Map<PostgresSearchStore.Doc, Double> combined = new IdentityHashMap<>();

        Hybrid(List<Query.Node> subs) {
            this.subs = subs;
        }

        List<Query.Node> children() {
            return subs;
        }

        void prepare(Prep p) {
            super.prepare(p);
            combined.clear();
            for (Query.Node sub : subs) {
                Map<PostgresSearchStore.Doc, Double> raw = new IdentityHashMap<>();
                double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
                for (IndexCtx ix : p.partitions) {
                    for (PostgresSearchStore.Doc d : ix.corpus) {
                        double s = sub.score(new Query.DocCtx(ix, d));
                        if (Query.matched(s)) {
                            raw.put(d, s);
                            min = Math.min(min, s);
                            max = Math.max(max, s);
                        }
                    }
                }
                for (var e : raw.entrySet()) {
                    double norm = max == min ? 1.0 : (e.getValue() - min) / (max - min);
                    combined.merge(e.getKey(), norm / subs.size(), Double::sum);
                }
            }
        }

        double score(Query.DocCtx d) {
            Double s = combined.get(d.doc);
            return s == null ? Query.NO : s * boost;
        }
    }
}
