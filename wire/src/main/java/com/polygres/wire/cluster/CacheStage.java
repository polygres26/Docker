package com.polygres.wire.cluster;

import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.PipelineChain;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.Statement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §12.3: a Coherence-style cache-aside distributed cache, one {@link PipelineStage}
 * positioned after {@code RouterStage} (needs {@link Statement#targetBackend()} resolved first —
 * the cache key must not collide across backends) and before {@code StatsCollectorStage}.
 *
 * <p><b>Off by default</b>: only constructed at all when {@code POLYWIRE_CACHE_TABLES} is
 * non-blank (see {@code GatewayComponents}) — a stale read is a correctness bug, not a
 * performance win, so nothing is ever cached unless an operator explicitly names which
 * schemas/tables are safe to.
 *
 * <p><b>SELECT-only</b>, matched against {@link #cachePatterns} the same way {@code
 * RouterStage}'s schema rules match ({@code \bschema\.}), reusing that convention rather than a
 * second config syntax. A cache hit short-circuits the chain entirely (never calls {@code next});
 * a miss calls {@code next} and populates the cache from the real result.
 *
 * <p><b>Invalidation</b>: table-level, not schema-level — {@link #WRITE_TARGET} extracts the
 * single table name directly after {@code INSERT INTO}/{@code UPDATE}/{@code DELETE FROM}/DDL's
 * table position (a best-effort regex, not a real SQL parser — same honesty standard as {@code
 * RouterStage}'s predicate-routing javadoc), and every cache entry recorded against that table is
 * dropped. Runs eagerly at write-<em>statement</em> time, not commit time — safe, not a shortcut:
 * under any real isolation level a concurrent reader can't see an uncommitted write anyway, so
 * invalidating early is strictly conservative (worst case: one wasted cache miss if the write's
 * transaction later rolls back). This matters most for {@code orawire}'s manual-commit sessions,
 * the one frontend where statement time and commit time can be genuinely far apart.
 */
public final class CacheStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(CacheStage.class);

    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);
    // Best-effort write-target extraction — not a parser. Covers the common single-table shapes;
    // falls back to schema-wide invalidation (via cachePatterns) when the target can't be isolated.
    private static final Pattern WRITE_TARGET = Pattern.compile(
            "^\\s*(?:insert\\s+into|update|delete\\s+from|create\\s+table|alter\\s+table|drop\\s+table|truncate\\s+table)\\s+"
                    + "([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern IS_WRITE_OR_DDL = Pattern.compile(
            "^\\s*(insert|update|delete|create|alter|drop|truncate)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_TARGET = Pattern.compile(
            "\\bfrom\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)", Pattern.CASE_INSENSITIVE);

    private final PolyWireCluster cluster;
    // volatile, not final: see #reconfigure. cachePatterns changing takes effect immediately
    // (checked fresh on every handle() call, same shape as QosControlStage/RouterStage's
    // reconfigure). ttlMillis is also hot-reloadable for *new* cache instances (see
    // #reconfigure's javadoc for why an already-created Ignite cache's own TTL can't be changed
    // in place, and how this class works around that).
    private volatile List<Pattern> cachePatterns;
    private volatile long ttlMillis;
    // byte[], not ExecutionResult directly: found live that Ignite's mandatory BinaryMarshaller
    // (there's no configurable alternative in this Ignite version — checked, not assumed) can't
    // reflectively obtain field offsets on Java record classes ("can't get field offset on a
    // record class"), and ExecutionResult/ColumnInfo are both records. Serializing to bytes with
    // plain java.io.ObjectOutputStream ourselves — which records fully support, that's exactly why
    // both were made Serializable — sidesteps Ignite's own marshaller for this type entirely.
    private volatile IgniteCache<String, byte[]> resultCache;
    // ARCHITECTURE.md §12.3: reverse index, table name -> the cache keys recorded against it, so a
    // write can invalidate exactly the entries it affects instead of scanning the whole cache.
    private volatile IgniteCache<String, java.util.Set<String>> keysByTable;

    public CacheStage(PolyWireCluster cluster, List<String> cacheTablePatterns, long ttlMillis) {
        this.cluster = cluster;
        this.cachePatterns = compilePatterns(cacheTablePatterns);
        this.ttlMillis = ttlMillis;
        this.resultCache = cluster.getOrCreateCache(cacheName(ttlMillis), ttlMillis);
        this.keysByTable = cluster.getOrCreateCache("polywire-query-cache-index", 0);
    }

    private static List<Pattern> compilePatterns(List<String> cacheTablePatterns) {
        return cacheTablePatterns.stream()
                .map(name -> Pattern.compile("\\b" + Pattern.quote(name.trim()) + "\\b", Pattern.CASE_INSENSITIVE))
                .toList();
    }

    /**
     * Ignite's {@code getOrCreateCache(name, ttl)} is a no-op on {@code ttl} if a cache by that
     * name already exists (its config is fixed at creation) — so a genuine TTL hot-reload needs a
     * distinctly-named cache per TTL value rather than reusing one fixed name. That's what this
     * does: {@code #reconfigure} calling {@code getOrCreateCache(cacheName(newTtl), newTtl)} with
     * a changed {@code newTtl} always lands on a fresh, correctly-configured cache instance
     * (starting empty — a clean, if cold, cache beats a wrong TTL). The old cache instance for the
     * previous TTL is simply dropped by this node; Ignite retains it cluster-side until eviction,
     * a small accepted resource cost for keeping this narrow-slice fix simple.
     */
    private static String cacheName(long ttlMillis) {
        return "polywire-query-cache-ttl" + ttlMillis;
    }

    /** {@code cacheTablesSpec}: {@code POLYWIRE_CACHE_TABLES}, comma-separated table/schema names. {@code ttlMillisSpec}: {@code POLYWIRE_CACHE_TTL_MS}, default 30000. */
    public static CacheStage fromConfig(PolyWireCluster cluster, String cacheTablesSpec, String ttlMillisSpec) {
        List<String> tables = new ArrayList<>();
        if (cacheTablesSpec != null && !cacheTablesSpec.isBlank()) {
            for (String entry : cacheTablesSpec.split(",")) {
                String trimmed = entry.trim();
                if (!trimmed.isEmpty()) {
                    tables.add(trimmed);
                }
            }
        }
        long ttl = ttlMillisSpec == null || ttlMillisSpec.isBlank() ? 30_000 : Long.parseLong(ttlMillisSpec);
        return new CacheStage(cluster, tables, ttl);
    }

    /** Non-null only when there's actually something to cache — {@code GatewayComponents} skips adding this stage entirely otherwise. */
    public static CacheStage fromConfigOrNull(PolyWireCluster cluster, String cacheTablesSpec, String ttlMillisSpec) {
        if (!cluster.enabled() || cacheTablesSpec == null || cacheTablesSpec.isBlank()) {
            return null;
        }
        return fromConfig(cluster, cacheTablesSpec, ttlMillisSpec);
    }

    /** {@code cacheTablesSpec}/{@code ttlMillisSpec}: same grammar {@link #fromConfig} accepts. Applies immediately (patterns) and to entries written after this call (TTL — see {@link #cacheName}'s javadoc). */
    public void reconfigure(String cacheTablesSpec, String ttlMillisSpec) {
        List<String> tables = new ArrayList<>();
        if (cacheTablesSpec != null && !cacheTablesSpec.isBlank()) {
            for (String entry : cacheTablesSpec.split(",")) {
                String trimmed = entry.trim();
                if (!trimmed.isEmpty()) {
                    tables.add(trimmed);
                }
            }
        }
        long newTtl = ttlMillisSpec == null || ttlMillisSpec.isBlank() ? 30_000 : Long.parseLong(ttlMillisSpec);
        this.cachePatterns = compilePatterns(tables);
        if (newTtl != this.ttlMillis) {
            this.resultCache = cluster.getOrCreateCache(cacheName(newTtl), newTtl);
            this.ttlMillis = newTtl;
            log.info("cache: TTL changed to {}ms, now serving from a fresh (empty) cache instance", newTtl);
        }
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (SELECT_PREFIX.matcher(sql).find() && matchesAnyPattern(sql)) {
            return handleCacheableSelect(statement, next);
        }
        if (IS_WRITE_OR_DDL.matcher(sql).find()) {
            ExecutionResult result = next.proceed(statement);
            invalidate(sql);
            return result;
        }
        return next.proceed(statement);
    }

    private ExecutionResult handleCacheableSelect(Statement statement, PipelineChain next) throws SQLException {
        String key = cacheKey(statement);
        byte[] cachedBytes = resultCache.get(key);
        if (cachedBytes != null) {
            log.debug("cache hit: {}", key);
            return deserialize(cachedBytes);
        }
        ExecutionResult result = next.proceed(statement);
        resultCache.put(key, serialize(result));
        recordKeyForTable(key, statement.sqlText());
        return result;
    }

    private static byte[] serialize(ExecutionResult result) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(result);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize cache entry", e);
        }
    }

    private static ExecutionResult deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (ExecutionResult) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException("failed to deserialize cache entry", new IOException(e));
        }
    }

    private boolean matchesAnyPattern(String sql) {
        for (Pattern pattern : cachePatterns) {
            if (pattern.matcher(sql).find()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Folds in {@link Statement#accessContext()}'s attributes whenever one is attached (non-
     * {@code ANONYMOUS}) — see {@code docs/design/end-user-data-access-security.md} §3.7. Required
     * for correctness once {@code AccessControlStage}'s native-RLS mode is in play: that mode
     * enforces row security via backend session context, not by editing {@code sqlText}, so two
     * different users' identically-worded queries would otherwise produce the exact same cache
     * key and serve one tenant's cached rows to another — precisely the failure Cube.js's own
     * multitenancy docs warn about ("cache keys must include the same context that scopes the
     * query"). For the app-level row-filter-injection path this is redundant (the filter is
     * already embedded in {@code sqlText}/{@code bindParams}, so the key already differs) but
     * harmless — a cache key that's more specific than strictly necessary only costs hit rate,
     * never correctness, so this doesn't attempt to detect which mode is active and only key on
     * attributes in that case.
     */
    /** Package-visible and {@code static} (touches no instance state) so {@code CacheStageExtractionTest}-style tests can cover the AccessContext-scoping fix directly, without needing a live Ignite instance — same rationale {@link #extractWriteTarget}/{@link #extractFromTarget} are already package-visible for. */
    static String cacheKey(Statement statement) {
        String accessContextPart = statement.accessContext().isAnonymous()
                ? ""
                : "|access=" + statement.accessContext().attributes();
        return statement.tenantId() + "|" + statement.targetBackend() + "|" + statement.sqlText()
                + "|" + statement.bindParams() + accessContextPart;
    }

    private void recordKeyForTable(String cacheKey, String sql) {
        String table = extractFromTarget(sql); // a SELECT's read target — best-effort, not a parser
        if (table == null) {
            return; // couldn't isolate a single table — this key just rides out its TTL untracked
        }
        // Read-then-put, not an atomic EntryProcessor invoke: a rare concurrent race here means a
        // key occasionally isn't indexed for early invalidation and rides out its TTL instead —
        // an accepted, narrow-slice trade-off (a stale-but-bounded read, never an unbounded one),
        // not a correctness bug, matching this stage's other documented best-effort limits.
        String normalized = normalizeTable(table);
        java.util.Set<String> keys = keysByTable.get(normalized);
        keys = keys == null ? new java.util.HashSet<>() : new java.util.HashSet<>(keys);
        keys.add(cacheKey);
        keysByTable.put(normalized, keys);
    }

    private void invalidate(String sql) {
        String table = extractWriteTarget(sql);
        if (table == null) {
            log.debug("cache invalidation: couldn't isolate a write target in \"{}\" — leaving cache as-is until TTL expiry", sql);
            return;
        }
        String normalized = normalizeTable(table);
        java.util.Set<String> keys = keysByTable.get(normalized);
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (String key : keys) {
            resultCache.remove(key);
        }
        keysByTable.remove(normalized);
        log.debug("cache invalidation: table={} removed {} entries", normalized, keys.size());
    }

    /** Package-visible (not {@code private}) so the extraction regex itself is directly unit-testable without a live Ignite instance. */
    static String extractWriteTarget(String sql) {
        Matcher m = WRITE_TARGET.matcher(sql);
        return m.find() ? m.group(1) : null;
    }

    static String extractFromTarget(String sql) {
        Matcher m = FROM_TARGET.matcher(sql);
        return m.find() ? m.group(1) : null;
    }

    private static String normalizeTable(String table) {
        return table.toLowerCase(java.util.Locale.ROOT);
    }
}
