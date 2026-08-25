package com.polygres.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.ShardingStrategy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Document storage and search execution for oswire, backed by plain Postgres -- same shape as
 * mongowire's {@code PostgresDocumentStore}/dynamowire's {@code PgItemStore}: no OpenSearch (or
 * Qdrant) needs to actually be running, this table shape simulates the storage a search engine
 * would own.
 *
 * <p>One physical table per collection, {@code polywire_search_<collection>}:
 * <pre>
 *   doc_id     TEXT PRIMARY KEY,
 *   source     JSONB NOT NULL,   -- the document body, returned verbatim as _source
 *   embedding  JSONB,            -- optional float array for k-NN, e.g. [0.12, -0.4, ...]
 *   updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
 * </pre>
 *
 * <p><b>k-NN without pgvector, on purpose:</b> this deployment's Postgres has no {@code vector}
 * extension installed (checked live: {@code SELECT * FROM pg_available_extensions WHERE
 * name='vector'} returns zero rows on the stock {@code postgres:16} image PolyWire ships against),
 * and requiring one would contradict every other store in this codebase's "no extension, no
 * external service required" design (dynamowire/mongowire/sqswire all reimplement their target
 * system's semantics in plain SQL for the same reason). So vector distance is computed in Java,
 * not SQL: {@link #search} pulls every row matching the non-vector filters (or every row in the
 * collection, if there are none) with a non-null {@code embedding}, computes
 * {@link #distance(float[], float[], SearchRequest.DistanceMetric)} for each, sorts, and takes
 * {@code topK}. That is a correct, honest linear scan -- no ANN index, no sub-linear search. Fine
 * for the collection sizes this is realistically used at today; genuinely large vector collections
 * are exactly the case a real pgvector-backed index (or Qdrant, once V3 exists) would be for.
 *
 * <p><b>V2 relevance scoring:</b> a plain structured search's {@code _score} is now a real
 * Postgres {@code ts_rank} value (summed across every {@code match} clause anywhere in the filter
 * tree, {@code must}/{@code filter}/{@code should} all counted, {@code must_not} contributing
 * nothing), not V1's flat {@code 1.0} -- see {@link #compileScore}. This is an honest
 * simplification of real BM25/TF-IDF relevance (it doesn't distinguish {@code must} from
 * {@code should} weight, and a {@code bool} with no {@code match} clauses anywhere still scores a
 * flat {@code 1.0}), not a claim of matching OpenSearch's own Lucene-based scoring bit-for-bit.
 * k-NN {@code _score} is now a real similarity transform of the underlying distance (higher is
 * better, matching real OpenSearch's k-NN plugin space-type formulas) instead of V1's raw
 * distance value -- see {@link #similarityScore}.
 *
 * <p><b>Hybrid search</b> ({@link #searchHybrid}): OpenSearch's neural-search hybrid query runs
 * every sub-query independently, min-max normalizes each sub-query's scores to [0, 1], and
 * combines normalized scores per document (the default "arithmetic mean" combination technique) --
 * this reimplements exactly that algorithm, not an approximation of it. Each sub-query is executed
 * as a genuine, independent {@link #search} call (typically one text/filter sub-query and one
 * k-NN sub-query), so a hybrid query combining {@code match} and {@code knn} gets real full-text
 * relevance fused with real vector similarity.
 *
 * <p><b>Sharding</b> (V3): {@code doc_id} hashes across {@code backendRegistry.shardGroup()} for
 * {@link #indexDocument}/{@link #getDocument}/{@link #deleteDocument} -- the same
 * {@link ShardingStrategy#hash} every other sharded store (dynamowire/mongowire/sqswire) already
 * uses, applied to the one key a document lookup always has. Structured search (not vector, not
 * hybrid -- see below) fans out to every shard and merges centrally via
 * {@link SearchScatterMerge}: hits are globally re-sorted/re-paginated, {@code total} is summed,
 * and aggregations are merged per {@link Aggregation.MetricType} (including a real weighted merge
 * for {@code AVG}, not an average-of-averages -- see {@link SearchScatterMerge}'s javadoc).
 * {@code ensureCollection} creates the table on every shard backend (plus, unlike
 * dynamowire/sqswire, no separate always-default catalog table is needed here -- there's no
 * search-side metadata to keep off the shard group).
 *
 * <p><b>Not yet sharded, deliberately refused rather than silently wrong when a shard group is
 * configured:</b> vector (k-NN) search and hybrid search. Both are real, separate design problems
 * -- k-NN's linear scan would need per-shard candidate gathering with the same correctness-over-
 * pushdown care structured search's merge already takes, and hybrid search's score fusion runs
 * over an already-limited per-sub-query candidate pool that interacts with sharding in a way that
 * needs its own pass, not a quick extension of this one. Both throw a clear
 * {@link OpenSearchException} rather than quietly returning only the default backend's rows.
 */
public final class PostgresSearchStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresSearchStore.class);
    private static final String TABLE_PREFIX = "polywire_search_";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    /** Each hybrid sub-query pulls this many top candidates before score fusion -- real OpenSearch
     * hybrid search likewise fuses each sub-retriever's own top-N, not its full result set. Fixed
     * rather than derived from the outer request's requested page size, so pagination (paging
     * further into an already-fused, already-sorted result) doesn't silently shrink the candidate
     * pool a later page is drawn from. */
    private static final int HYBRID_CANDIDATE_POOL = 500;

    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensuredCollections = new ConcurrentHashMap<>();

    public PostgresSearchStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    private Connection open() throws SQLException {
        return open(defaultTarget());
    }

    private Connection open(BackendTarget target) throws SQLException {
        return target.open();
    }

    private BackendTarget defaultTarget() {
        // resolveForRouting, not get -- see BackendRegistry.resolveForRouting's javadoc.
        BackendTarget target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("oswire: no default backend configured");
        }
        return target;
    }

    private List<String> shardGroup() {
        return backendRegistry == null ? List.of() : backendRegistry.shardGroup();
    }

    /** Every shard target, resolved fresh from the registry -- live-reloadable, matching
     * dynamowire/sqswire's own "re-read on every call" convention. Falls back to
     * {@code [defaultTarget()]} when no shard group is configured, so callers can always iterate
     * "the shards this collection lives on" without a separate unsharded branch. */
    private List<BackendTarget> allShardTargets() {
        List<String> group = shardGroup();
        if (group.isEmpty()) {
            return List.of(defaultTarget());
        }
        List<BackendTarget> targets = new ArrayList<>();
        for (String name : group) {
            BackendTarget target = backendRegistry.resolveForRouting(name);
            if (target == null) {
                throw new IllegalStateException("oswire: shard group references unknown backend \"" + name + "\"");
            }
            targets.add(target);
        }
        return targets;
    }

    private BackendTarget targetForDoc(String docId) {
        List<String> group = shardGroup();
        if (group.isEmpty()) {
            return defaultTarget();
        }
        String shardName = ShardingStrategy.hash(group).resolve(docId);
        BackendTarget target = backendRegistry.resolveForRouting(shardName);
        if (target == null) {
            throw new IllegalStateException("oswire: shard group references unknown backend \"" + shardName + "\"");
        }
        return target;
    }

    static String pgTableName(String collection) {
        if (!IDENTIFIER.matcher(collection).matches()) {
            throw new IllegalArgumentException(
                    "oswire: collection/index name must match [A-Za-z_][A-Za-z0-9_]* -- got \"" + collection + "\"");
        }
        return TABLE_PREFIX + collection.toLowerCase(Locale.ROOT);
    }

    /** Idempotent; called on every write and on explicit {@code PUT /<index>} so a collection
     * never has to be pre-declared. Cached per-process so a hot write path isn't re-issuing
     * {@code CREATE TABLE IF NOT EXISTS} every call. Cached per (collection, physical backend
     * jdbcUrl) pair, not per collection alone -- a flat per-collection cache would mean a backend
     * that only starts receiving this collection's writes LATER (a switchover's fallback taking
     * over after this collection was already ensured against its primary, or a shard added to an
     * existing group) never gets the table created on it at all. */
    public void ensureCollection(String collection) throws SQLException {
        String table = pgTableName(collection);
        // Created on every shard target (matching dynamowire/sqswire's createTable) -- a document
        // can hash to any shard, so every shard needs the table before any write to it can land.
        for (BackendTarget target : allShardTargets()) {
            String key = table + "@" + target.jdbcUrl();
            if (ensuredCollections.putIfAbsent(key, Boolean.TRUE) != null) {
                continue;
            }
            try (Connection c = open(target); var st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " ("
                        + "doc_id TEXT PRIMARY KEY, "
                        + "source JSONB NOT NULL, "
                        + "embedding JSONB, "
                        + "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())");
            }
        }
    }

    public void indexDocument(String collection, String docId, JsonObject source, float[] vector) throws SQLException {
        ensureCollection(collection);
        String table = pgTableName(collection);
        try (Connection c = open(targetForDoc(docId));
                var ps = c.prepareStatement("INSERT INTO " + table + " (doc_id, source, embedding, updated_at) "
                        + "VALUES (?, ?::jsonb, ?::jsonb, now()) "
                        + "ON CONFLICT (doc_id) DO UPDATE SET source = EXCLUDED.source, "
                        + "embedding = EXCLUDED.embedding, updated_at = now()")) {
            ps.setString(1, docId);
            ps.setString(2, source.toString());
            ps.setString(3, vector == null ? null : vectorToJson(vector));
            ps.executeUpdate();
        }
    }

    public JsonObject getDocument(String collection, String docId) throws SQLException {
        ensureCollection(collection);
        String table = pgTableName(collection);
        try (Connection c = open(targetForDoc(docId));
                var ps = c.prepareStatement("SELECT source FROM " + table + " WHERE doc_id = ?")) {
            ps.setString(1, docId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? JsonParser.parseString(rs.getString(1)).getAsJsonObject() : null;
            }
        }
    }

    /** @return true if a row was actually deleted */
    public boolean deleteDocument(String collection, String docId) throws SQLException {
        ensureCollection(collection);
        String table = pgTableName(collection);
        try (Connection c = open(targetForDoc(docId));
                var ps = c.prepareStatement("DELETE FROM " + table + " WHERE doc_id = ?")) {
            ps.setString(1, docId);
            return ps.executeUpdate() > 0;
        }
    }

    public SearchResult search(SearchRequest request) throws SQLException {
        ensureCollection(request.collection());
        String table = pgTableName(request.collection());
        boolean sharded = !shardGroup().isEmpty();

        if (request.isHybrid()) {
            if (sharded) {
                throw new OpenSearchException("action_request_validation_exception",
                        "oswire: hybrid search across a sharded collection is not supported yet -- see "
                                + "PostgresSearchStore's class javadoc");
            }
            if (!request.aggregations().isEmpty()) {
                throw new OpenSearchException("action_request_validation_exception",
                        "oswire V2 doesn't support aggregations on a hybrid query -- run the aggregation as its "
                                + "own separate _search request");
            }
            return searchHybrid(request);
        }
        if (request.isVectorSearch()) {
            if (sharded) {
                throw new OpenSearchException("action_request_validation_exception",
                        "oswire: k-NN (vector) search across a sharded collection is not supported yet -- see "
                                + "PostgresSearchStore's class javadoc");
            }
            if (!request.aggregations().isEmpty()) {
                throw new OpenSearchException("action_request_validation_exception",
                        "oswire V2 doesn't support aggregations on a k-NN query -- run the aggregation as its own "
                                + "separate _search request");
            }
            SqlFragment where = compileFilter(request.filter());
            return searchByVector(table, request, where);
        }
        SqlFragment where = compileFilter(request.filter());
        if (sharded) {
            return searchStructuredSharded(table, request, where);
        }
        SearchResult result = searchStructured(table, request, where);
        if (request.aggregations().isEmpty()) {
            return result;
        }
        List<AggregationResult> aggs = runAggregations(table, where, request.aggregations());
        return new SearchResult(result.hits(), result.total(), aggs);
    }

    /** Fans out a structured (non-vector, non-hybrid) search across every shard and merges
     * centrally via {@link SearchScatterMerge} -- see this class's javadoc for the trade-offs
     * (fetch-everything-then-merge, real weighted AVG). Each shard's own hit fetch is unpaginated
     * (no LIMIT/OFFSET) since the global top-K can only be known after every shard's candidates
     * are gathered; each shard's aggregation request runs the {@link SearchScatterMerge#expandForSharding}
     * expanded shape so AVG can be merged correctly afterward. */
    private SearchResult searchStructuredSharded(String table, SearchRequest request, SqlFragment where) throws SQLException {
        List<BackendTarget> shards = allShardTargets();
        List<SearchResult> perShardHits = new ArrayList<>();
        List<List<AggregationResult>> perShardAggs = new ArrayList<>();
        List<Aggregation> expandedAggs = SearchScatterMerge.expandForSharding(request.aggregations());
        for (BackendTarget target : shards) {
            perShardHits.add(searchStructuredForShard(target, table, request, where));
            if (!expandedAggs.isEmpty()) {
                perShardAggs.add(runAggregations(target, table, where, expandedAggs));
            }
        }
        Comparator<SearchHit> comparator = SearchScatterMerge.comparatorFor(request.sort());
        SearchResult merged = SearchScatterMerge.mergeHits(perShardHits, comparator, request.offset(), request.topK());
        if (request.aggregations().isEmpty()) {
            return merged;
        }
        List<AggregationResult> mergedAggs = SearchScatterMerge.mergeAcrossShards(request.aggregations(), perShardAggs);
        return new SearchResult(merged.hits(), merged.total(), mergedAggs);
    }

    /** As {@link #searchStructured}, but against one specific shard and with no LIMIT/OFFSET --
     * every matching row on this shard, for the caller to merge across all shards. */
    private SearchResult searchStructuredForShard(BackendTarget target, String table, SearchRequest request,
            SqlFragment where) throws SQLException {
        ScoreFragment score = compileScore(request.filter());
        String selectSql = "SELECT doc_id, source, (" + score.expr() + ") AS score FROM " + table + " WHERE " + where.sql();
        try (Connection c = open(target)) {
            long total;
            try (var ps = prepare(c, "SELECT count(*) FROM " + table + " WHERE " + where.sql(), where.params());
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                total = rs.getLong(1);
            }
            List<SearchHit> hits = new ArrayList<>();
            List<Object> params = new ArrayList<>(score.params());
            params.addAll(where.params());
            try (var ps = prepare(c, selectSql, params); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    hits.add(new SearchHit(rs.getString(1), rs.getDouble(3),
                            JsonParser.parseString(rs.getString(2)).getAsJsonObject()));
                }
            }
            return new SearchResult(hits, total);
        }
    }

    private SearchResult searchStructured(String table, SearchRequest request, SqlFragment where) throws SQLException {
        ScoreFragment score = compileScore(request.filter());
        boolean hasRealScore = !"1.0".equals(score.expr());
        String orderBy = !request.sort().isEmpty()
                ? request.sort().stream()
                        .map(s -> jsonPath(s.field()) + (s.ascending() ? " ASC" : " DESC"))
                        .reduce((a, b) -> a + ", " + b).orElseThrow()
                : hasRealScore ? "score DESC" : "updated_at DESC";

        String countSql = "SELECT count(*) FROM " + table + " WHERE " + where.sql();
        String selectSql = "SELECT doc_id, source, (" + score.expr() + ") AS score FROM " + table
                + " WHERE " + where.sql() + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?";

        try (Connection c = open()) {
            long total;
            try (var ps = prepare(c, countSql, where.params())) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    total = rs.getLong(1);
                }
            }
            List<SearchHit> hits = new ArrayList<>();
            List<Object> params = new ArrayList<>(score.params());
            params.addAll(where.params());
            params.add(request.topK());
            params.add(request.offset());
            try (var ps = prepare(c, selectSql, params)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        hits.add(new SearchHit(rs.getString(1), rs.getDouble(3),
                                JsonParser.parseString(rs.getString(2)).getAsJsonObject()));
                    }
                }
            }
            return new SearchResult(hits, total);
        }
    }

    /** See the class javadoc's "k-NN without pgvector" section -- this is a real, correct linear
     * scan over every row whose non-vector filters match, not an ANN index lookup. */
    private SearchResult searchByVector(String table, SearchRequest request, SqlFragment where) throws SQLException {
        String selectSql = "SELECT doc_id, source, embedding FROM " + table
                + " WHERE embedding IS NOT NULL AND (" + where.sql() + ")";
        List<ScoredCandidate> candidates = new ArrayList<>();
        try (Connection c = open(); var ps = prepare(c, selectSql, where.params())) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    float[] stored = jsonToVector(rs.getString(3));
                    double dist = distance(request.vector(), stored, request.distanceMetric());
                    candidates.add(new ScoredCandidate(rs.getString(1),
                            JsonParser.parseString(rs.getString(2)).getAsJsonObject(), dist));
                }
            }
        }
        // Lower distance is a better match for every metric here (L2 and 1-cosine-similarity and
        // negated dot product are all "smaller is closer") -- see distance()'s javadoc. Sorting
        // uses the raw distance; the _score reported to the caller is a separate, real similarity
        // transform (see similarityScore()) -- these two are deliberately not the same number.
        candidates.sort(Comparator.comparingDouble(ScoredCandidate::distance));
        long total = candidates.size();
        List<SearchHit> hits = new ArrayList<>();
        int end = Math.min(candidates.size(), request.offset() + request.topK());
        for (int i = request.offset(); i < end; i++) {
            ScoredCandidate cand = candidates.get(i);
            hits.add(new SearchHit(cand.docId(), similarityScore(cand.distance(), request.distanceMetric()), cand.source()));
        }
        return new SearchResult(hits, total);
    }

    /**
     * Fuses N independently-executed sub-queries into one ranked result -- real OpenSearch hybrid
     * search's own algorithm (min-max normalize each sub-query's scores to [0, 1], then combine
     * per document by arithmetic mean, the default "normalization processor" configuration),
     * reimplemented exactly, not approximated. Each sub-request runs through the ordinary
     * {@link #search} path (recursively -- a sub-request is a complete, independent
     * {@link SearchRequest}), so a {@code match} sub-query gets real {@code ts_rank} relevance and
     * a {@code knn} sub-query gets a real similarity score, each already in a sub-query-appropriate
     * scale before normalization ever runs.
     */
    private SearchResult searchHybrid(SearchRequest request) throws SQLException {
        List<Map<String, Double>> normalizedPerSubQuery = new ArrayList<>();
        Map<String, JsonObject> sourceById = new LinkedHashMap<>();

        for (SearchRequest sub : request.hybridSubRequests()) {
            SearchRequest candidatePool = new SearchRequest(sub.collection(), sub.projection(), sub.filter(),
                    sub.textQuery(), sub.vector(), sub.vectorField(), sub.distanceMetric(),
                    HYBRID_CANDIDATE_POOL, 0, List.of(), List.of(), List.of());
            SearchResult subResult = search(candidatePool);

            Map<String, Double> raw = new LinkedHashMap<>();
            for (SearchHit hit : subResult.hits()) {
                raw.put(hit.id(), hit.score());
                sourceById.putIfAbsent(hit.id(), hit.source());
            }
            normalizedPerSubQuery.add(minMaxNormalize(raw));
        }

        Map<String, Double> combined = new LinkedHashMap<>();
        for (Map<String, Double> normalized : normalizedPerSubQuery) {
            for (var entry : normalized.entrySet()) {
                combined.merge(entry.getKey(), entry.getValue(), Double::sum);
            }
        }
        int subQueryCount = Math.max(1, normalizedPerSubQuery.size());
        List<Map.Entry<String, Double>> ranked = new ArrayList<>(combined.entrySet());
        ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        long total = ranked.size();
        List<SearchHit> hits = new ArrayList<>();
        int end = Math.min(ranked.size(), request.offset() + request.topK());
        for (int i = request.offset(); i < end; i++) {
            var entry = ranked.get(i);
            hits.add(new SearchHit(entry.getKey(), entry.getValue() / subQueryCount, sourceById.get(entry.getKey())));
        }
        return new SearchResult(hits, total);
    }

    /** Min-max normalizes a raw score map to [0, 1] -- real OpenSearch's default hybrid
     * normalization technique. When every score is identical (including the single-document or
     * empty-result case, where min == max trivially), everything present normalizes to 1.0 rather
     * than dividing by zero -- consistent with "this document is exactly as relevant as the most
     * relevant one" being vacuously true when there's nothing to distinguish it from. */
    private static Map<String, Double> minMaxNormalize(Map<String, Double> raw) {
        if (raw.isEmpty()) {
            return raw;
        }
        double min = raw.values().stream().mapToDouble(Double::doubleValue).min().orElseThrow();
        double max = raw.values().stream().mapToDouble(Double::doubleValue).max().orElseThrow();
        Map<String, Double> normalized = new LinkedHashMap<>();
        for (var entry : raw.entrySet()) {
            normalized.put(entry.getKey(), max == min ? 1.0 : (entry.getValue() - min) / (max - min));
        }
        return normalized;
    }

    private record ScoredCandidate(String docId, JsonObject source, double distance) {
    }

    /** Smaller return value = closer match, for every metric -- callers sort ascending regardless
     * of which metric was requested, so this deliberately returns a "distance", not a
     * "similarity" (cosine similarity and dot product are negated/inverted here for that reason).
     * {@link #similarityScore} is the separate transform that turns this into the higher-is-better
     * number actually shown to a caller as {@code _score}. */
    static double distance(float[] a, float[] b, SearchRequest.DistanceMetric metric) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "oswire: query vector has " + a.length + " dimensions, stored vector has " + b.length);
        }
        return switch (metric) {
            case L2 -> {
                double sum = 0;
                for (int i = 0; i < a.length; i++) {
                    double d = a[i] - b[i];
                    sum += d * d;
                }
                yield Math.sqrt(sum);
            }
            case DOT_PRODUCT -> {
                double dot = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += a[i] * b[i];
                }
                yield -dot;
            }
            case COSINE -> {
                double dot = 0, normA = 0, normB = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += a[i] * b[i];
                    normA += a[i] * a[i];
                    normB += b[i] * b[i];
                }
                double denom = Math.sqrt(normA) * Math.sqrt(normB);
                yield denom == 0 ? 1.0 : 1.0 - (dot / denom);
            }
        };
    }

    /**
     * Converts {@link #distance}'s internal "smaller is closer" value into the higher-is-better
     * {@code _score} a real client expects, using the exact space-type formulas real OpenSearch's
     * k-NN plugin documents: {@code l2 -> 1 / (1 + distance)}, {@code cosinesimil -> (1 +
     * similarity) / 2} (similarity recovered as {@code 1 - storedCosineDistance}, since
     * {@link #distance}'s COSINE case stores {@code 1 - cosineSimilarity}), and
     * {@code innerproduct -> rawDot >= 0 ? rawDot + 1 : 1 / (1 - rawDot)} (raw dot product
     * recovered as {@code -storedDotDistance}, since {@link #distance}'s DOT_PRODUCT case negates
     * it). Every formula maps onto (0, 1] with 1.0 being an exact match, matching real OpenSearch's
     * own convention -- V1 returned the raw, metric-dependent distance value directly, which
     * wasn't comparable across metrics and read backwards for cosine/dot (a "better" match showed
     * as a *smaller* number).
     */
    static double similarityScore(double dist, SearchRequest.DistanceMetric metric) {
        return switch (metric) {
            case L2 -> 1.0 / (1.0 + dist);
            case COSINE -> (1.0 + (1.0 - dist)) / 2.0;
            case DOT_PRODUCT -> {
                double rawDot = -dist;
                yield rawDot >= 0 ? rawDot + 1.0 : 1.0 / (1.0 - rawDot);
            }
        };
    }

    private static String vectorToJson(float[] vector) {
        JsonArray arr = new JsonArray();
        for (float v : vector) {
            arr.add(v);
        }
        return arr.toString();
    }

    private static float[] jsonToVector(String json) {
        if (json == null) {
            return new float[0];
        }
        JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
        float[] out = new float[arr.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = arr.get(i).getAsFloat();
        }
        return out;
    }

    // --- aggregation -> SQL compilation/execution ---

    /**
     * Runs each top-level {@link Aggregation} as its own SQL query against the already-filtered
     * document set ({@code where}) -- a {@link Aggregation.Terms} becomes a real
     * {@code GROUP BY ... ORDER BY count(*) DESC LIMIT size}, with each of its {@code subAggs} run
     * again per bucket (one additional query per bucket, since a bucket's own filter -- "this
     * group's key" -- has to be added to {@code where} first; real OpenSearch pays a similar
     * per-bucket cost internally, just amortized differently). A bare {@link Aggregation.Metric}
     * at the top level runs once over the whole filtered set.
     */
    private List<AggregationResult> runAggregations(String table, SqlFragment where, List<Aggregation> aggregations)
            throws SQLException {
        return runAggregations(defaultTarget(), table, where, aggregations);
    }

    private List<AggregationResult> runAggregations(BackendTarget target, String table, SqlFragment where,
            List<Aggregation> aggregations) throws SQLException {
        try (Connection c = open(target)) {
            List<AggregationResult> results = new ArrayList<>();
            for (Aggregation agg : aggregations) {
                results.add(runOneAggregation(c, table, where, agg));
            }
            return results;
        }
    }

    private AggregationResult runOneAggregation(Connection c, String table, SqlFragment where, Aggregation agg)
            throws SQLException {
        return switch (agg) {
            case Aggregation.Metric(String name, Aggregation.MetricType type, String field) -> {
                String metricSql = "SELECT " + metricExpr(type, field) + " FROM " + table + " WHERE " + where.sql();
                try (var ps = prepare(c, metricSql, where.params()); ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    double value = rs.getDouble(1);
                    yield new AggregationResult.SingleValue(name, rs.wasNull() ? null : value, openSearchMetricType(type));
                }
            }
            case Aggregation.Terms(String name, String field, int size, List<Aggregation> subAggs) -> {
                String bucketSql = "SELECT " + jsonPath(field) + " AS bucket_key, count(*) AS doc_count FROM " + table
                        + " WHERE " + where.sql() + " GROUP BY bucket_key ORDER BY doc_count DESC LIMIT ?";
                List<AggregationResult.Bucket> buckets = new ArrayList<>();
                List<Object> params = new ArrayList<>(where.params());
                params.add(size);
                try (var ps = prepare(c, bucketSql, params); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString(1);
                        long docCount = rs.getLong(2);
                        List<AggregationResult> subResults = new ArrayList<>();
                        if (!subAggs.isEmpty()) {
                            SqlFragment bucketWhere = new SqlFragment(
                                    "(" + where.sql() + ") AND " + jsonPath(field) + " = ?",
                                    append(where.params(), key));
                            for (Aggregation subAgg : subAggs) {
                                subResults.add(runOneAggregation(c, table, bucketWhere, subAgg));
                            }
                        }
                        buckets.add(new AggregationResult.Bucket(key, docCount, subResults));
                    }
                }
                long returnedDocCount = buckets.stream().mapToLong(AggregationResult.Bucket::docCount).sum();
                long totalMatching;
                try (var ps = prepare(c, "SELECT count(*) FROM " + table + " WHERE " + where.sql(), where.params());
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    totalMatching = rs.getLong(1);
                }
                yield new AggregationResult.Buckets(name, buckets, totalMatching - returnedDocCount);
            }
        };
    }

    /** Real OpenSearch's own aggregation-type tag for each {@link Aggregation.MetricType} --
     * see {@link AggregationResult}'s javadoc for why this has to be threaded through to the
     * response at all (the {@code typed_keys} response format official clients require). */
    private static String openSearchMetricType(Aggregation.MetricType type) {
        return switch (type) {
            case AVG -> "avg";
            case SUM -> "sum";
            case MIN -> "min";
            case MAX -> "max";
            case COUNT -> "value_count";
        };
    }

    private static String metricExpr(Aggregation.MetricType type, String field) {
        String numericPath = typedPath(field, 0.0);
        return switch (type) {
            case AVG -> "avg(" + numericPath + ")";
            case SUM -> "sum(" + numericPath + ")";
            case MIN -> "min(" + numericPath + ")";
            case MAX -> "max(" + numericPath + ")";
            case COUNT -> "count(" + jsonPath(field) + ")";
        };
    }

    private static List<Object> append(List<Object> params, Object extra) {
        List<Object> copy = new ArrayList<>(params);
        copy.add(extra);
        return copy;
    }

    // --- filter -> SQL compilation ---

    private record SqlFragment(String sql, List<Object> params) {
    }

    private static SqlFragment compileFilter(SearchFilter filter) {
        List<Object> params = new ArrayList<>();
        String sql = compile(filter, params);
        return new SqlFragment(sql, params);
    }

    private static String compile(SearchFilter filter, List<Object> params) {
        return switch (filter) {
            case SearchFilter.MatchAll ignored -> "TRUE";
            case SearchFilter.Term(String field, Object value) -> {
                params.add(value);
                yield typedPath(field, value) + " = ?";
            }
            case SearchFilter.Range(String field, Object gte, Object lte, Object gt, Object lt) -> {
                List<String> clauses = new ArrayList<>();
                Object sample = gte != null ? gte : lte != null ? lte : gt != null ? gt : lt;
                String path = typedPath(field, sample);
                if (gte != null) { clauses.add(path + " >= ?"); params.add(gte); }
                if (lte != null) { clauses.add(path + " <= ?"); params.add(lte); }
                if (gt != null) { clauses.add(path + " > ?"); params.add(gt); }
                if (lt != null) { clauses.add(path + " < ?"); params.add(lt); }
                yield clauses.isEmpty() ? "TRUE" : "(" + String.join(" AND ", clauses) + ")";
            }
            case SearchFilter.Match(String field, String text) -> {
                params.add(text);
                yield "to_tsvector('english', coalesce(" + jsonPath(field, true) + ", '')) "
                        + "@@ plainto_tsquery('english', ?)";
            }
            case SearchFilter.Bool(List<SearchFilter> must, List<SearchFilter> filterList,
                    List<SearchFilter> should, List<SearchFilter> mustNot) -> {
                List<String> clauses = new ArrayList<>();
                for (SearchFilter f : must) clauses.add(compile(f, params));
                for (SearchFilter f : filterList) clauses.add(compile(f, params));
                if (!should.isEmpty()) {
                    List<String> orClauses = new ArrayList<>();
                    for (SearchFilter f : should) orClauses.add(compile(f, params));
                    clauses.add("(" + String.join(" OR ", orClauses) + ")");
                }
                for (SearchFilter f : mustNot) clauses.add("NOT (" + compile(f, params) + ")");
                yield clauses.isEmpty() ? "TRUE" : "(" + String.join(" AND ", clauses) + ")";
            }
        };
    }

    private record ScoreFragment(String expr, List<Object> params) {
    }

    /**
     * Builds a real relevance-score SQL expression for a structured (non-vector, non-hybrid)
     * search -- the sum of {@code ts_rank(...)} across every {@link SearchFilter.Match} clause
     * found anywhere in the filter tree ({@code must}/{@code filter}/{@code should} clauses all
     * contribute; {@code must_not} contributes nothing, since a document that matched a negated
     * clause was already excluded by the WHERE clause). A filter tree with no {@code Match}
     * clause anywhere (pure term/range/bool-of-those) has nothing to rank by, so it falls back to
     * a flat {@code 1.0} -- V1's behavior, unchanged for the case V1 already handled correctly.
     * This is a deliberate simplification of real BM25/TF-IDF scoring (no per-clause weighting,
     * no must-vs-should distinction in the summed contribution), not an attempt to bit-for-bit
     * match OpenSearch's own Lucene-based relevance score -- see the class javadoc.
     */
    private static ScoreFragment compileScore(SearchFilter filter) {
        List<Object> params = new ArrayList<>();
        List<String> rankExprs = new ArrayList<>();
        collectMatchRankExprs(filter, rankExprs, params);
        String expr = rankExprs.isEmpty() ? "1.0" : String.join(" + ", rankExprs);
        return new ScoreFragment(expr, params);
    }

    private static void collectMatchRankExprs(SearchFilter filter, List<String> exprs, List<Object> params) {
        switch (filter) {
            case SearchFilter.Match(String field, String text) -> {
                params.add(text);
                exprs.add("ts_rank(to_tsvector('english', coalesce(" + jsonPath(field, true) + ", '')), "
                        + "plainto_tsquery('english', ?))");
            }
            case SearchFilter.Bool(List<SearchFilter> must, List<SearchFilter> filterList,
                    List<SearchFilter> should, List<SearchFilter> ignoredMustNot) -> {
                for (SearchFilter f : must) collectMatchRankExprs(f, exprs, params);
                for (SearchFilter f : filterList) collectMatchRankExprs(f, exprs, params);
                for (SearchFilter f : should) collectMatchRankExprs(f, exprs, params);
                // must_not deliberately contributes nothing -- see compileScore's javadoc.
            }
            case SearchFilter.Term ignored -> { }
            case SearchFilter.Range ignored -> { }
            case SearchFilter.MatchAll ignored -> { }
        }
    }

    /** {@code field} may be dotted ({@code "user.name"}) for a nested JSON path. {@code asText}
     * requests {@code #>>} (text extraction, for full-text match); the default {@code ->>} form
     * is used for direct equality/range comparisons -- both are the text-extraction operators
     * (there's no numeric-typed JSONB accessor), callers rely on Postgres's implicit cast from
     * the bind parameter's own type when comparing. Field names are validated against
     * {@link #IDENTIFIER} per path segment before being concatenated into SQL text -- never
     * derived from anything other than a validated identifier, so this isn't a SQL-injection
     * surface despite not being a bind parameter. */
    private static String jsonPath(String field) {
        return jsonPath(field, false);
    }

    /** {@code source ->> 'field'} is always text -- JSONB has no numeric/boolean-typed accessor
     * -- so a comparison against a number or boolean bind parameter needs an explicit cast or
     * Postgres rejects it outright ({@code operator does not exist: text >= double precision}),
     * it does not implicitly coerce. Cast is chosen from the bind value's own Java type, which
     * {@code OpenSearchAdapter} already derived from the JSON value's own type
     * ({@link OpenSearchAdapter#parseQuery}'s {@code scalar()}), so this stays correct for
     * whatever type a client's query actually sent -- string fields (the common case) are left
     * as plain text with no cast. */
    private static String typedPath(String field, Object sampleValue) {
        String path = jsonPath(field);
        if (sampleValue instanceof Double || sampleValue instanceof Number) {
            return path + "::numeric";
        }
        if (sampleValue instanceof Boolean) {
            return path + "::boolean";
        }
        return path;
    }

    private static String jsonPath(String field, boolean asText) {
        String[] parts = field.split("\\.");
        for (String part : parts) {
            if (!IDENTIFIER.matcher(part).matches()) {
                throw new IllegalArgumentException("oswire: invalid field name \"" + field + "\"");
            }
        }
        if (parts.length == 1) {
            return "(source ->> '" + parts[0] + "')";
        }
        StringBuilder path = new StringBuilder("'{");
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) path.append(',');
            path.append(parts[i]);
        }
        path.append("}'");
        return "(source #>> " + path + ")";
    }

    private static PreparedStatement prepare(Connection c, String sql, List<Object> params) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
        return ps;
    }
}
