package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Everything the query engine needs about one (index, host) partition of an index: the mapping, the analyzers, and
 * lazily-computed per-field corpus statistics for BM25 (document count, total token count, per-term document
 * frequency). Statistics are per partition -- exactly like a real shard scoring on its own segments -- unless the
 * caller builds one context over the union of all hosts ({@code search_type=dfs_query_then_fetch}).
 */
final class IndexCtx {

    final String indexName;
    final Mappings mappings;
    final JsonObject settings;
    final long now;
    final List<PostgresSearchStore.Doc> corpus;
    private final Map<String, Analysis.Analyzer> analyzers = new HashMap<>();
    private final Map<String, FieldStats> stats = new HashMap<>();

    static final class FieldStats {
        int docCount;
        long sumTotalTermFreq;
        final Map<String, Integer> docFreq = new HashMap<>();
    }

    IndexCtx(String indexName, Mappings mappings, JsonObject settings, long now, List<PostgresSearchStore.Doc> corpus) {
        this.indexName = indexName;
        this.mappings = mappings;
        this.settings = settings;
        this.now = now;
        this.corpus = corpus;
    }

    Analysis.Analyzer analyzer(String name) {
        return analyzers.computeIfAbsent(name, n -> Analysis.resolve(n, settings));
    }

    private boolean hasCustom(String name) {
        JsonObject an = Analysis.analysisSettings(settings);
        return an != null && an.has("analyzer") && an.getAsJsonObject("analyzer").has(name);
    }

    Analysis.Analyzer indexAnalyzer(Mappings.Field f) {
        if (f.isKeyword()) {
            return text -> List.of(new Analysis.Token(Values.normalize(f, text, settings), 0, text.length(), 0));
        }
        if (f.analyzer != null) {
            return analyzer(f.analyzer);
        }
        return hasCustom("default") ? analyzer("default") : Analysis.STANDARD;
    }

    Analysis.Analyzer searchAnalyzer(Mappings.Field f) {
        if (f.isKeyword()) {
            return indexAnalyzer(f);
        }
        if (f.searchAnalyzer != null) {
            return analyzer(f.searchAnalyzer);
        }
        if (f.analyzer == null && hasCustom("default_search")) {
            return analyzer("default_search");
        }
        return indexAnalyzer(f);
    }

    static String sourcePath(Mappings.Field f) {
        if (f.subField) {
            return f.path.substring(0, f.path.lastIndexOf('.'));
        }
        return f.path;
    }

    /** Terms of one document for a text/keyword field (relative path inside {@code obj}). */
    List<Analysis.Token> tokens(Mappings.Field f, JsonObject obj, String rel) {
        if (f.type.equals("boolean")) {
            List<Analysis.Token> out = new ArrayList<>();
            int pos = 0;
            for (JsonElement e : Mappings.values(obj, rel)) {
                Object b = Values.parse(f, e, settings, zone());
                if (b != null) {
                    out.add(new Analysis.Token(b.toString(), 0, 1, pos++));
                }
            }
            return out;
        }
        List<String> vals = new ArrayList<>();
        for (JsonElement e : Mappings.values(obj, rel)) {
            if (e.isJsonPrimitive()) {
                String s = e.getAsString();
                if (f.isKeyword() && f.def != null && f.def.has("ignore_above") && s.length() > f.def.get("ignore_above").getAsInt()) {
                    continue;
                }
                vals.add(s);
            }
        }
        if (f.isKeyword()) {
            List<Analysis.Token> out = new ArrayList<>();
            int pos = 0;
            for (String s : vals) {
                out.add(new Analysis.Token(Values.normalize(f, s, settings), 0, s.length(), pos++));
            }
            return out;
        }
        return Analysis.analyzeAll(indexAnalyzer(f), vals);
    }

    static List<JsonObject> objectsAt(JsonObject root, String nestedPath) {
        List<JsonObject> out = new ArrayList<>();
        if (nestedPath == null) {
            out.add(root);
            return out;
        }
        for (JsonElement e : Mappings.values(root, nestedPath)) {
            if (e.isJsonObject()) {
                out.add(e.getAsJsonObject());
            }
        }
        return out;
    }

    FieldStats stats(Mappings.Field f) {
        return stats.computeIfAbsent(f.path, p -> {
            FieldStats st = new FieldStats();
            String rel = sourcePath(f);
            String prefix = f.nestedPath == null ? "" : f.nestedPath + ".";
            String relInObj = rel.startsWith(prefix) ? rel.substring(prefix.length()) : rel;
            for (PostgresSearchStore.Doc d : corpus) {
                for (JsonObject o : objectsAt(d.source, f.nestedPath)) {
                    List<Analysis.Token> toks = tokens(f, o, relInObj);
                    if (toks.isEmpty()) {
                        continue;
                    }
                    st.docCount++;
                    st.sumTotalTermFreq += toks.size();
                    Set<String> seen = new HashSet<>();
                    for (Analysis.Token t : toks) {
                        if (seen.add(t.term())) {
                            st.docFreq.merge(t.term(), 1, Integer::sum);
                        }
                    }
                }
            }
            return st;
        });
    }

    ZoneId zone() {
        return ZoneOffset.UTC;
    }

    // ---- Lucene BM25 (k1 = 1.2, b = 0.75, lossy 1-byte norms) ----

    private static final int MAX_INT4 = longToInt4(Integer.MAX_VALUE);
    private static final int NUM_FREE_VALUES = 255 - MAX_INT4;

    private static int longToInt4(long i) {
        int numBits = 64 - Long.numberOfLeadingZeros(i);
        if (numBits < 4) {
            return (int) i;
        }
        int shift = numBits - 4;
        int encoded = (int) (i >>> shift);
        encoded &= 0x07;
        encoded |= (shift + 1) << 3;
        return encoded;
    }

    private static long int4ToLong(int i) {
        long bits = i & 0x07;
        int shift = (i >>> 3) - 1;
        return shift == -1 ? bits : (bits | 0x08) << shift;
    }

    static int encodeLength(int len) {
        return len < NUM_FREE_VALUES ? len : NUM_FREE_VALUES + longToInt4(len - NUM_FREE_VALUES);
    }

    static int decodeLength(int b) {
        return b < NUM_FREE_VALUES ? b : (int) (NUM_FREE_VALUES + int4ToLong(b - NUM_FREE_VALUES));
    }

    /** BM25 contribution of one term (or one phrase with summed idf). {@code idf} is precomputed by the caller. */
    static double bm25(double idf, double tf, int fieldLen, boolean norms, FieldStats st, double boost) {
        double avgdl = st.docCount == 0 ? 1 : (double) st.sumTotalTermFreq / st.docCount;
        double dl = norms ? decodeLength(encodeLength(fieldLen)) : 1;
        double inv = 1.0 / (1.2 * (0.25 + 0.75 * dl / avgdl));
        return boost * 2.2 * idf * (1.0 - 1.0 / (1.0 + tf * inv));
    }

    static double idf(FieldStats st, String term) {
        return idfDf(st, st.docFreq.getOrDefault(term, 0));
    }

    static double idfDf(FieldStats st, int df) {
        int n = st.docCount;
        return Math.log(1 + (n - df + 0.5) / (df + 0.5));
    }

    /** Terms within {@code maxEdits} of {@code term} in this partition's dictionary, Lucene FuzzyQuery style: each expansion
     * is boosted by {@code 1 - dist/min(len)} and all expansions share the highest document frequency (blended stats). */
    record Fuzzed(List<String> terms, List<Double> boosts, int blendedDf) {
    }

    private final Map<String, Fuzzed> fuzzyCache = new HashMap<>();

    Fuzzed fuzzyExpand(Mappings.Field f, String term, int maxEdits, int prefixLen, int maxExpansions, boolean transpositions) {
        String key = f.path + "\u0000" + term + "\u0000" + maxEdits + "\u0000" + prefixLen + "\u0000" + maxExpansions + transpositions;
        return fuzzyCache.computeIfAbsent(key, k -> {
            FieldStats st = stats(f);
            List<Object[]> found = new ArrayList<>();
            for (String t : st.docFreq.keySet()) {
                if (prefixLen > 0 && (t.length() < prefixLen || term.length() < prefixLen || !t.startsWith(term.substring(0, prefixLen)))) {
                    continue;
                }
                int dist = Query.editDistance(term, t, maxEdits, transpositions);
                if (dist <= maxEdits) {
                    double sim = 1.0 - (double) dist / Math.max(1, Math.min(term.length(), t.length()));
                    found.add(new Object[] {t, sim});
                }
            }
            found.sort((a, b) -> {
                int c = Double.compare((Double) b[1], (Double) a[1]);
                return c != 0 ? c : ((String) a[0]).compareTo((String) b[0]);
            });
            List<String> terms = new ArrayList<>();
            List<Double> boosts = new ArrayList<>();
            int maxDf = 0;
            for (int i = 0; i < found.size() && i < maxExpansions; i++) {
                terms.add((String) found.get(i)[0]);
                boosts.add((Double) found.get(i)[1]);
                maxDf = Math.max(maxDf, st.docFreq.get((String) found.get(i)[0]));
            }
            return new Fuzzed(terms, boosts, maxDf);
        });
    }
}
