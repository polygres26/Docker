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

    // mongowire's own fixed physical shape is simpler than dynamowire's -- always exactly one key
    // column ("id", holding the Mongo Extended-JSON form of _id) and one value column ("doc", the
    // whole document as jsonb -- see PostgresDocumentStore's own DDL). No sort-key equivalent
    // exists for Mongo documents, so unlike the dynamowire patterns above there's only ever one
    // shape each for lookup and write.
    //
    // Unlike dynamowire's physical names (always a bare lowercase identifier), PostgresDocumentStore
    // ALWAYS double-quotes both the schema and the table ("db"."collection", case-preserving) --
    // see its own qualifiedTable()/quoteIdent() -- so each identifier part here must accept an
    // optional pair of double quotes, unlike the dynamowire patterns' bare-identifier-only groups.
    private static final String MONGO_IDENT = "\"?([A-Za-z_][\\w$]*)\"?";
    private static final Pattern MONGO_ROW_LOOKUP = Pattern.compile(
            "^\\s*select\\s+doc\\s+from\\s+" + MONGO_IDENT + "\\." + MONGO_IDENT
                    + "\\s+where\\s+id\\s*=\\s*\\?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MONGO_ROW_WRITE = Pattern.compile(
            "^\\s*(?:update\\s+" + MONGO_IDENT + "\\." + MONGO_IDENT + "\\s+set\\s+.+?"
                    + "|delete\\s+from\\s+" + MONGO_IDENT + "\\." + MONGO_IDENT + ")"
                    + "\\s+where\\s+id\\s*=\\s*\\?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** {@code null} return means "not a known dynamowire-backed table", keyed by the PHYSICAL
     * table name a SQL statement actually contains. Non-null means "yes, and here's whether it
     * has a sort key" -- needed to tell a genuine single-row lookup (every key column pinned)
     * from one that could still match more than one row (only the partition key pinned on a
     * table that also has a sort key). */
    @FunctionalInterface
    public interface RowTableLookup {
        Boolean hasSortKey(String physicalTable);
    }

    private final WarpCluster cluster;
    
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

    // All nullable, and all set post-construction by Main once RowCache and each protocol's own
    // table-lookup exist (Main builds CacheStage before either) -- exactly the same
    // set-after-the-fact pattern sqlMetrics above already uses. rowCache being null disables the
    // SQL-side row-cache fast path entirely; rowTableLookup/mongoTableLookup being null just
    // means THAT protocol's table shape is never recognized -- either, both, or neither can be
    // wired independently (e.g. dynamowire's row cache enabled, mongowire's disabled).
    private volatile RowCache rowCache;
    private volatile RowTableLookup rowTableLookup;
    private volatile java.util.function.Predicate<String> mongoTableLookup;

    public CacheStage(WarpCluster cluster, List<String> cacheTablePatterns, long ttlMillis) {
        this.cluster = cluster;
        this.cachePatterns = compilePatterns(cacheTablePatterns);
        this.ttlMillis = ttlMillis;
        this.resultCache = cluster.getOrCreateCache(cacheName(ttlMillis), ttlMillis);
        this.keysByTable = cluster.getOrCreateCache("warp-query-cache-index", 0);
    }

    private static List<Pattern> compilePatterns(List<String> cacheTablePatterns) {
        return cacheTablePatterns.stream()
                .map(name -> Pattern.compile("\\b" + Pattern.quote(name.trim()) + "\\b", Pattern.CASE_INSENSITIVE))
                .toList();
    }

    private static String cacheName(long ttlMillis) {
        return "warp-query-cache-ttl" + ttlMillis;
    }

    public static CacheStage fromConfig(WarpCluster cluster, String cacheTablesSpec, String ttlMillisSpec) {
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

    public static CacheStage fromConfigOrNull(WarpCluster cluster, String cacheTablesSpec, String ttlMillisSpec) {
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

    /** Called once from {@code Main} right after RowCache is constructed -- shared by both
     * protocols' own table-lookup setters below. */
    public void setRowCache(RowCache rowCache) {
        this.rowCache = rowCache;
    }

    /** Called once from {@code Main} right after dynamowire's PgItemStore exists, wiring up the
     * SQL-side half of the shared cross-protocol row cache for dynamowire-backed tables. */
    public void setDynamoRowTableLookup(RowTableLookup rowTableLookup) {
        this.rowTableLookup = rowTableLookup;
    }

    /** As {@link #setDynamoRowTableLookup}, for mongowire-backed tables -- a simple predicate
     * (not {@link RowTableLookup}) since a Mongo document's key is always just {@code _id}, no
     * sort-key equivalent to report. */
    public void setMongoRowTableLookup(java.util.function.Predicate<String> mongoTableLookup) {
        this.mongoTableLookup = mongoTableLookup;
    }

    /** @return {@code null} if {@code sql} isn't the exact single-table/primary-key-equality
     *      SELECT shape either protocol's fast path recognizes, or if the target table isn't a
     *      known row-cacheable table -- either way, the caller falls through to this stage's
     *      original behavior untouched. Otherwise fully handles the statement (cache hit, or a
     *      real execution that then populates the row cache) and returns the result directly. */
    private ExecutionResult tryRowCacheLookup(Statement statement, PipelineChain next) throws SQLException {
        if (rowCache == null) {
            return null;
        }
        String sql = statement.sqlText();
        List<Object> binds = statement.bindParams();

        if (rowTableLookup != null) {
            Matcher withSk = ROW_LOOKUP_WITH_SK.matcher(sql);
            Matcher noSk = ROW_LOOKUP_NO_SK.matcher(sql);
            String table = null;
            String pk = null;
            String sk = null;
            if (withSk.matches()) {
                table = normalizeTable(withSk.group(1));
                Boolean hasSk = rowTableLookup.hasSortKey(table);
                if (hasSk != null && hasSk && binds != null && binds.size() >= 2) {
                    pk = String.valueOf(binds.get(0));
                    sk = String.valueOf(binds.get(1));
                } else {
                    table = null;
                }
            } else if (noSk.matches()) {
                table = normalizeTable(noSk.group(1));
                Boolean hasSk = rowTableLookup.hasSortKey(table);
                // hasSk == true here means the table's real key is (pk, sk) together -- a WHERE
                // clause pinning only pk_value could still match more than one row, so this is
                // NOT a safe point lookup even though it's syntactically the same shape as one.
                if (hasSk != null && !hasSk && binds != null && !binds.isEmpty()) {
                    pk = String.valueOf(binds.get(0));
                } else {
                    table = null;
                }
            }
            if (table != null) {
                return lookupOrExecuteAndCache(RowCache.key(table, pk, sk), "item", statement, next);
            }
        }

        if (mongoTableLookup != null) {
            Matcher m = MONGO_ROW_LOOKUP.matcher(sql);
            if (m.matches()) {
                String table = normalizeTable(m.group(1) + "." + m.group(2));
                if (mongoTableLookup.test(table) && binds != null && !binds.isEmpty()) {
                    return lookupOrExecuteAndCache(RowCache.key(table, String.valueOf(binds.get(0)), null), "doc", statement, next);
                }
            }
        }

        return null;
    }

    /** Shared by both protocols' row-cache fast paths: a cache hit returns immediately (recorded
     * as a real {@code cache_hit} RTT outcome under the CALLING statement's own protocol -- e.g.
     * a pgwire SELECT hitting a row dynamowire populated shows up as a pgwire hit, not a
     * dynamowire one); a miss executes for real and populates the cache from the single returned
     * column's value, whatever that value's shape is for this table (dynamowire's typed item
     * JSON, mongowire's document JSON) -- this method doesn't need to know or care which. */
    private ExecutionResult lookupOrExecuteAndCache(String key, String valueColumnName, Statement statement, PipelineChain next) throws SQLException {
        long start = System.nanoTime();
        String cached = rowCache.get(key);
        if (cached != null) {
            if (sqlMetrics != null) {
                sqlMetrics.recordRttOutcome(
                        com.nexagres.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - start);
            }
            log.debug("row cache hit: {}", key);
            com.nexagres.wire.core.ColumnInfo column =
                    new com.nexagres.wire.core.ColumnInfo(valueColumnName, java.sql.Types.VARCHAR, 0, 0, 0, false);
            return ExecutionResult.ofQuery(List.of(column), List.of(List.of(cached)));
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
        if (rowCache == null) {
            return;
        }
        String sql = statement.sqlText();
        List<Object> binds = statement.bindParams();

        if (rowTableLookup != null) {
            Matcher m = ROW_WRITE.matcher(sql);
            if (m.matches()) {
                String table = normalizeTable(m.group(1) != null ? m.group(1) : m.group(2));
                Boolean hasSk = rowTableLookup.hasSortKey(table);
                boolean suppliedSk = m.group(3) != null;
                // Table has a sort key but this write only pinned the partition key (or vice
                // versa, which the regex shouldn't even let happen) -- can't safely name one
                // exact row, so hasSk must agree with what the WHERE clause actually supplied.
                if (hasSk != null && hasSk == suppliedSk) {
                    int needed = suppliedSk ? 2 : 1;
                    if (binds != null && binds.size() >= needed) {
                        // pk_value = ? [AND sk_value = ?] is pinned at the very end of the matched
                        // SQL by ROW_WRITE's own anchor ($), so its bind(s) are always the LAST
                        // one or two elements of bindParams -- NOT necessarily index 0/1. An
                        // UPDATE's own SET clause can (and typically does) bind earlier values
                        // first, e.g. "UPDATE t SET item = ? WHERE pk_value = ?" has bindParams =
                        // [newItemJson, pk], not [pk, ...].
                        String pk = String.valueOf(binds.get(binds.size() - needed));
                        String sk = suppliedSk ? String.valueOf(binds.get(binds.size() - 1)) : null;
                        rowCache.invalidate(RowCache.key(table, pk, sk));
                        return;
                    }
                }
            }
        }

        if (mongoTableLookup != null) {
            Matcher m = MONGO_ROW_WRITE.matcher(sql);
            if (m.matches()) {
                String table = normalizeTable(m.group(1) != null ? m.group(1) + "." + m.group(2) : m.group(3) + "." + m.group(4));
                if (mongoTableLookup.test(table) && binds != null && !binds.isEmpty()) {
                    rowCache.invalidate(RowCache.key(table, String.valueOf(binds.get(binds.size() - 1)), null));
                }
            }
        }
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
