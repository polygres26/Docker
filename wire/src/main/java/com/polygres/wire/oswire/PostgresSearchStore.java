package com.polygres.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
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
 * <p>No sharding yet, unlike dynamowire/sqswire/mongowire -- V1 always resolves
 * {@link BackendRegistry#DEFAULT_BACKEND_NAME}. Search doesn't have dynamowire's natural
 * per-item partition key to shard by (a query can span the whole collection), so sharding this
 * store is a genuinely separate design question, deliberately deferred rather than half-done.
 */
public final class PostgresSearchStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresSearchStore.class);
    private static final String TABLE_PREFIX = "polywire_search_";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensuredCollections = new ConcurrentHashMap<>();

    public PostgresSearchStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    private Connection open() throws SQLException {
        BackendTarget target = backendRegistry.get(BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("oswire: no default backend configured");
        }
        return target.open();
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
     * {@code CREATE TABLE IF NOT EXISTS} every call. */
    public void ensureCollection(String collection) throws SQLException {
        String table = pgTableName(collection);
        if (ensuredCollections.putIfAbsent(table, Boolean.TRUE) != null) {
            return;
        }
        try (Connection c = open(); var st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "doc_id TEXT PRIMARY KEY, "
                    + "source JSONB NOT NULL, "
                    + "embedding JSONB, "
                    + "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        }
    }

    public void indexDocument(String collection, String docId, JsonObject source, float[] vector) throws SQLException {
        ensureCollection(collection);
        String table = pgTableName(collection);
        try (Connection c = open();
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
        try (Connection c = open();
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
        try (Connection c = open();
                var ps = c.prepareStatement("DELETE FROM " + table + " WHERE doc_id = ?")) {
            ps.setString(1, docId);
            return ps.executeUpdate() > 0;
        }
    }

    public SearchResult search(SearchRequest request) throws SQLException {
        ensureCollection(request.collection());
        String table = pgTableName(request.collection());
        SqlFragment where = compileFilter(request.filter());

        if (request.isVectorSearch()) {
            return searchByVector(table, request, where);
        }
        return searchStructured(table, request, where);
    }

    private SearchResult searchStructured(String table, SearchRequest request, SqlFragment where) throws SQLException {
        String orderBy = request.sort().isEmpty() ? "updated_at DESC"
                : request.sort().stream()
                        .map(s -> jsonPath(s.field()) + (s.ascending() ? " ASC" : " DESC"))
                        .reduce((a, b) -> a + ", " + b).orElseThrow();

        String countSql = "SELECT count(*) FROM " + table + " WHERE " + where.sql();
        String selectSql = "SELECT doc_id, source FROM " + table + " WHERE " + where.sql()
                + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?";

        try (Connection c = open()) {
            long total;
            try (var ps = prepare(c, countSql, where.params())) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    total = rs.getLong(1);
                }
            }
            List<SearchHit> hits = new ArrayList<>();
            List<Object> params = new ArrayList<>(where.params());
            params.add(request.topK());
            params.add(request.offset());
            try (var ps = prepare(c, selectSql, params)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        hits.add(new SearchHit(rs.getString(1), 1.0,
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
        // negated dot product are all "smaller is closer") -- see distance()'s javadoc.
        candidates.sort(Comparator.comparingDouble(ScoredCandidate::distance));
        long total = candidates.size();
        List<SearchHit> hits = new ArrayList<>();
        int end = Math.min(candidates.size(), request.offset() + request.topK());
        for (int i = request.offset(); i < end; i++) {
            ScoredCandidate cand = candidates.get(i);
            hits.add(new SearchHit(cand.docId(), cand.distance(), cand.source()));
        }
        return new SearchResult(hits, total);
    }

    private record ScoredCandidate(String docId, JsonObject source, double distance) {
    }

    /** Smaller return value = closer match, for every metric -- callers sort ascending regardless
     * of which metric was requested, so this deliberately returns a "distance", not a
     * "similarity" (cosine similarity and dot product are negated/inverted here for that reason). */
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
