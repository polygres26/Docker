package com.nexagres.wire.cluster;

import com.nexagres.wire.core.ExecutionResult;
import com.nexagres.wire.core.PipelineChain;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.core.Statement;
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

public final class CacheStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(CacheStage.class);

    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern WRITE_TARGET = Pattern.compile(
            "^\\s*(?:insert\\s+into|update|delete\\s+from|create\\s+table|alter\\s+table|drop\\s+table|truncate\\s+table)\\s+"
                    + "([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern IS_WRITE_OR_DDL = Pattern.compile(
            "^\\s*(insert|update|delete|create|alter|drop|truncate)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_TARGET = Pattern.compile(
            "\\bfrom\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)", Pattern.CASE_INSENSITIVE);

    // Deliberately narrow, deliberately regex-based (same style as everything else in this
    // class): a single-table, primary-key-EQUALITY SELECT against a dynamowire-backed table's
    // own fixed physical shape (pk_value, sk_value, item -- see PgItemStore's own DDL). Anything
    // that isn't exactly this shape -- a join, an extra predicate, a range, a projection other
    // than the bare "item" column -- falls straight through to the existing table-pattern result
    // cache below, completely unaffected. This is the SQL-side half of the shared row cache: see
    // RowCache's own javadoc for why dynamowire's GetItem and this SELECT shape can share one
    // cache entry for the same row.
    private static final Pattern ROW_LOOKUP_NO_SK = Pattern.compile(
            "^\\s*select\\s+item\\s+from\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)\\s+where\\s+pk_value\\s*=\\s*\\?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ROW_LOOKUP_WITH_SK = Pattern.compile(
            "^\\s*select\\s+item\\s+from\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)\\s+where\\s+pk_value\\s*=\\s*\\?\\s+and\\s+sk_value\\s*=\\s*\\?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE);
    // Same shape, write side -- an UPDATE/DELETE whose WHERE clause is exactly this table's
    // primary key (nothing else) unambiguously touches at most one row, so it's always safe to
    // invalidate that one row-cache entry by its exact key rather than falling back to this
    // stage's existing whole-table invalidation below (which still runs too, for the ordinary
    // result cache). A write with any OTHER predicate shape is left alone here -- this only ever
    // narrows what gets invalidated, never widens it, so getting the match wrong just means a
    // stale row-cache entry sits until its TTL, not a correctness bug.
    private static final Pattern ROW_WRITE = Pattern.compile(
            "^\\s*(?:update\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)\\s+set\\s+.+?"
                    + "|delete\\s+from\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?))"
                    + "\\s+where\\s+pk_value\\s*=\\s*\\?(\\s+and\\s+sk_value\\s*=\\s*\\?)?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** {@code null} return means "not a known row-cacheable table" -- dynamowire is the only
     * implementation today (see Main's wiring), keyed by the PHYSICAL table name a SQL statement
     * actually contains. Non-null means "yes, and here's whether it has a sort key" -- needed to
     * tell a genuine single-row lookup (every key column pinned) from one that could still match
     * more than one row (only the partition key pinned on a table that also has a sort key). */
    @FunctionalInterface
    public interface RowTableLookup {
        Boolean hasSortKey(String physicalTable);
    }

    private final PolyWireCluster cluster;
    
    private volatile List<Pattern> cachePatterns;
    private volatile long ttlMillis;
    
    // Deliberately byte[], not a typed IgniteCache<String, ExecutionResult> -- tried that (to
    // skip the manual serialize/deserialize below and let Ignite's own marshaller handle it) in
    // pursuit of a sub-0.3ms cache-hit target, and it crashed real requests: Ignite's reflective
    // marshaller path throws "can't get field offset on a record class" for ExecutionResult
    // specifically because it's a Java record, not a plain class. Caught live, reverted. A safe
    // version of that idea would need either a non-record DTO or a custom Ignite Binary type
    // registration -- not attempted here given the correctness risk of hand-rolling encoding for
    // arbitrary JDBC row values (Object cells can be nearly any JDBC type) under time pressure.
    private volatile IgniteCache<String, byte[]> resultCache;
    
    private volatile IgniteCache<String, java.util.Set<String>> keysByTable;

    // Set post-construction by Main -- the shared com.nexagres.wire.core.SqlMetricsCollector
    // doesn't exist yet when CacheStage is built (it needs the cache cluster up first). Nullable:
    // a cache hit still works fine with no metrics collector attached, it just leaves the "cache
    // hit" timing row unrecorded, same as before this feature existed.
    private volatile com.nexagres.wire.core.SqlMetricsCollector sqlMetrics;

    // Both nullable, and both set post-construction by Main once RowCache and dynamowire's own
    // PgItemStore exist (Main builds CacheStage before either) -- exactly the same
    // set-after-the-fact pattern sqlMetrics above already uses. Either being null just means the
    // SQL-side row-cache fast path is skipped entirely and every SELECT falls through to this
    // stage's original, unaffected behavior -- there's no dynamowire-shaped-table detection
    // possible without both.
    private volatile RowCache rowCache;
    private volatile RowTableLookup rowTableLookup;

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

    private static String cacheName(long ttlMillis) {
        return "polywire-query-cache-ttl" + ttlMillis;
    }

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

    public static CacheStage fromConfigOrNull(PolyWireCluster cluster, String cacheTablesSpec, String ttlMillisSpec) {
        if (!cluster.enabled() || cacheTablesSpec == null || cacheTablesSpec.isBlank()) {
            return null;
        }
        return fromConfig(cluster, cacheTablesSpec, ttlMillisSpec);
    }

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
        if (SELECT_PREFIX.matcher(sql).find()) {
            ExecutionResult rowResult = tryRowCacheLookup(statement, next);
            if (rowResult != null) {
                return rowResult;
            }
            if (matchesAnyPattern(sql)) {
                return handleCacheableSelect(statement, next);
            }
        }
        if (IS_WRITE_OR_DDL.matcher(sql).find()) {
            ExecutionResult result = next.proceed(statement);
            invalidate(sql);
            invalidateRowCacheForPointWrite(statement);
            return result;
        }
        return next.proceed(statement);
    }

    private ExecutionResult handleCacheableSelect(Statement statement, PipelineChain next) throws SQLException {
        String key = cacheKey(statement);
        long start = System.nanoTime();
        byte[] cachedBytes = resultCache.get(key);
        if (cachedBytes != null) {
            long elapsedNanos = System.nanoTime() - start;
            log.debug("cache hit: {}", key);
            if (sqlMetrics != null) {
                sqlMetrics.recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, elapsedNanos);
            }
            return deserialize(cachedBytes);
        }
        ExecutionResult result = next.proceed(statement);
        resultCache.put(key, serialize(result));
        recordKeyForTable(key, statement.sqlText());
        return result;
    }

    /** Called once from {@code Main} right after the shared collector is constructed. */
    public void setSqlMetrics(com.nexagres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.sqlMetrics = sqlMetrics;
    }

    /** Called once from {@code Main} right after both RowCache and dynamowire's PgItemStore
     * exist, wiring up the SQL-side half of the shared cross-protocol row cache. */
    public void setRowCache(RowCache rowCache, RowTableLookup rowTableLookup) {
        this.rowCache = rowCache;
        this.rowTableLookup = rowTableLookup;
    }

    /** @return {@code null} if {@code sql} isn't the exact single-table/primary-key-equality
     *      SELECT shape this fast path recognizes, or if the target table isn't a known
     *      row-cacheable (today: dynamowire-backed) table -- either way, the caller falls through
     *      to this stage's original behavior untouched. Otherwise fully handles the statement
     *      (cache hit, or a real execution that then populates the row cache) and returns the
     *      result directly. */
    private ExecutionResult tryRowCacheLookup(Statement statement, PipelineChain next) throws SQLException {
        if (rowCache == null || rowTableLookup == null) {
            return null;
        }
        String sql = statement.sqlText();
        List<Object> binds = statement.bindParams();
        String table;
        String pk;
        String sk = null;
        Matcher withSk = ROW_LOOKUP_WITH_SK.matcher(sql);
        Matcher noSk = ROW_LOOKUP_NO_SK.matcher(sql);
        if (withSk.matches()) {
            table = normalizeTable(withSk.group(1));
            Boolean hasSk = rowTableLookup.hasSortKey(table);
            if (hasSk == null || !hasSk || binds == null || binds.size() < 2) {
                return null;
            }
            pk = String.valueOf(binds.get(0));
            sk = String.valueOf(binds.get(1));
        } else if (noSk.matches()) {
            table = normalizeTable(noSk.group(1));
            Boolean hasSk = rowTableLookup.hasSortKey(table);
            // hasSk == true here means the table's real key is (pk, sk) together -- a WHERE
            // clause pinning only pk_value could still match more than one row, so this is NOT a
            // safe point lookup even though it's syntactically the same shape as one that is.
            if (hasSk == null || hasSk || binds == null || binds.isEmpty()) {
                return null;
            }
            pk = String.valueOf(binds.get(0));
        } else {
            return null;
        }
        String key = RowCache.key(table, pk, sk);
        long start = System.nanoTime();
        String cachedJson = rowCache.get(key);
        if (cachedJson != null) {
            if (sqlMetrics != null) {
                sqlMetrics.recordRttOutcome(
                        com.nexagres.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - start);
            }
            log.debug("row cache hit: {}", key);
            com.nexagres.wire.core.ColumnInfo itemColumn =
                    new com.nexagres.wire.core.ColumnInfo("item", java.sql.Types.VARCHAR, 0, 0, 0, false);
            return ExecutionResult.ofQuery(List.of(itemColumn), List.of(List.of(cachedJson)));
        }
        ExecutionResult result = next.proceed(statement);
        if (result.isQuery() && result.rows().size() == 1) {
            List<Object> row = result.rows().get(0);
            if (!row.isEmpty() && row.get(0) != null) {
                rowCache.put(key, row.get(0).toString());
            }
        }
        return result;
    }

    /** As {@link #invalidate(String)}, but for the shared row cache: only fires when a write's
     * WHERE clause pins every one of a row-cacheable table's key columns (see {@code ROW_WRITE}'s
     * own comment above for why that's the only case it's safe to act on). */
    private void invalidateRowCacheForPointWrite(Statement statement) {
        if (rowCache == null || rowTableLookup == null) {
            return;
        }
        Matcher m = ROW_WRITE.matcher(statement.sqlText());
        if (!m.matches()) {
            return;
        }
        String table = normalizeTable(m.group(1) != null ? m.group(1) : m.group(2));
        Boolean hasSk = rowTableLookup.hasSortKey(table);
        if (hasSk == null) {
            return;
        }
        boolean suppliedSk = m.group(3) != null;
        if (hasSk != suppliedSk) {
            // Table has a sort key but this write only pinned the partition key (or vice versa,
            // which the regex shouldn't even let happen) -- can't safely name one exact row.
            return;
        }
        List<Object> binds = statement.bindParams();
        int needed = suppliedSk ? 2 : 1;
        if (binds == null || binds.size() < needed) {
            return;
        }
        // pk_value = ? [AND sk_value = ?] is pinned at the very end of the matched SQL by
        // ROW_WRITE's own anchor ($), so its bind(s) are always the LAST one or two elements of
        // bindParams -- NOT necessarily index 0/1. An UPDATE's own SET clause can (and typically
        // does) bind earlier values first, e.g. "UPDATE t SET item = ? WHERE pk_value = ?" has
        // bindParams = [newItemJson, pk], not [pk, ...]. Indexing from the end is correct for
        // both UPDATE and DELETE regardless of how many binds a SET clause contributes.
        String pk = String.valueOf(binds.get(binds.size() - needed));
        String sk = suppliedSk ? String.valueOf(binds.get(binds.size() - 1)) : null;
        rowCache.invalidate(RowCache.key(table, pk, sk));
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

    static String cacheKey(Statement statement) {
        String accessContextPart = statement.accessContext().isAnonymous()
                ? ""
                : "|access=" + statement.accessContext().attributes();
        return statement.tenantId() + "|" + statement.targetBackend() + "|" + statement.sqlText()
                + "|" + statement.bindParams() + accessContextPart;
    }

    private void recordKeyForTable(String cacheKey, String sql) {
        String table = extractFromTarget(sql);
        if (table == null) {
            return;
        }
        
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
