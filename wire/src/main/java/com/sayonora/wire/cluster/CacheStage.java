package com.sayonora.wire.cluster;

import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.PipelineChain;
import com.sayonora.wire.core.PipelineStage;
import com.sayonora.wire.core.Statement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.Serializable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
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

    // Generic point-lookup/point-write SQL shape, used only when a table has a REAL discovered
    // primary key (PrimaryKeyCatalog) -- unlike ROW_LOOKUP_*/ROW_WRITE above (dynamowire/mongowire's
    // own fixed physical column names), this has to accept ANY WHERE clause shape and then verify
    // structurally (in Java, see matchesExactPkPredicate below) that it's a pure conjunction of
    // equality predicates covering EXACTLY the table's real PK columns -- a regex alone can't
    // express "these N columns, any order, nothing else" for an arbitrary column-name set.
    private static final Pattern GENERIC_SELECT = Pattern.compile(
            "^\\s*select\\s+.+?\\s+from\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)\\s+where\\s+(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern GENERIC_WRITE = Pattern.compile(
            "^\\s*(?:update\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)\\s+set\\s+.+?"
                    + "|delete\\s+from\\s+([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?))"
                    + "\\s+where\\s+(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    // One conjunct: "col = ?", allowing an optional table-alias/schema qualifier before the column
    // (e.g. "o.id = ?") since a real generated UPDATE/SELECT sometimes qualifies WHERE columns even
    // against a single table -- the qualifier itself is discarded, only the bare column name feeds
    // into the PK-column-set comparison.
    private static final Pattern EQUALITY_CONJUNCT = Pattern.compile(
            "(?:[A-Za-z_][\\w$]*\\.)?([A-Za-z_][\\w$]*)\\s*=\\s*\\?");

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

    // Set post-construction by Main -- the shared com.sayonora.wire.core.SqlMetricsCollector
    // doesn't exist yet when CacheStage is built (it needs the cache cluster up first). Nullable:
    // a cache hit still works fine with no metrics collector attached, it just leaves the "cache
    // hit" timing row unrecorded, same as before this feature existed.
    private volatile com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    // All nullable, and all set post-construction by Main once RowCache and each protocol's own
    // table-lookup exist (Main builds CacheStage before either) -- exactly the same
    // set-after-the-fact pattern sqlMetrics above already uses. rowCache being null disables the
    // SQL-side row-cache fast path entirely; rowTableLookup/mongoTableLookup being null just
    // means THAT protocol's table shape is never recognized -- either, both, or neither can be
    // wired independently (e.g. dynamowire's row cache enabled, mongowire's disabled).
    private volatile RowCache rowCache;
    private volatile RowTableLookup rowTableLookup;
    private volatile java.util.function.Predicate<String> mongoTableLookup;

    // The generic (any-backend, any-table, any-PK-column-name) point-lookup/point-write cache --
    // see PrimaryKeyCatalog's own javadoc for scope and the "first backend wins" simplification.
    // A byte[] ExecutionResult cache, not RowCache's single-string-value model, because a real SQL
    // row can have any number of columns of any JDBC type -- reuses this class's own
    // serialize/deserialize helpers, the exact ones resultCache already uses. Empty map (the
    // default, and what every deployment gets until PrimaryKeyCatalog.discover finds something)
    // means the generic fast path is a no-op everywhere -- existing behavior is completely
    // unaffected until an operator's tables actually have a discoverable real PK.
    private volatile Map<String, List<String>> primaryKeysByTable = Map.of();
    private volatile IgniteCache<String, byte[]> pkRowCache;

    public CacheStage(WarpCluster cluster, List<String> cacheTablePatterns, long ttlMillis) {
        this.cluster = cluster;
        this.cachePatterns = compilePatterns(cacheTablePatterns);
        this.ttlMillis = ttlMillis;
        this.resultCache = cluster.getOrCreateCache(cacheName(ttlMillis), ttlMillis);
        this.keysByTable = cluster.getOrCreateCache("warp-query-cache-index", 0);
        this.pkRowCache = cluster.getOrCreateCache("warp-generic-pk-cache-ttl" + ttlMillis, ttlMillis);
    }

    /** Called once from {@code Main} after {@link PrimaryKeyCatalog#discover} runs (needs
     * {@code BackendRegistry} to exist first), and again on a config reload that changes {@code
     * WARP_CACHE_TABLES}. An empty/absent entry for a table just means the generic PK fast path
     * doesn't apply to it -- existing table-level caching/invalidation is unaffected either way. */
    public void setPrimaryKeyCatalog(Map<String, List<String>> primaryKeysByTable) {
        this.primaryKeysByTable = primaryKeysByTable == null ? Map.of() : Map.copyOf(primaryKeysByTable);
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
            this.pkRowCache = cluster.getOrCreateCache("warp-generic-pk-cache-ttl" + newTtl, newTtl);
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
            ExecutionResult genericPkResult = tryGenericPkLookup(statement, next);
            if (genericPkResult != null) {
                return genericPkResult;
            }
            if (matchesAnyPattern(sql)) {
                return handleCacheableSelect(statement, next);
            }
        }
        if (IS_WRITE_OR_DDL.matcher(sql).find()) {
            ExecutionResult result = next.proceed(statement);
            invalidate(sql);
            invalidateRowCacheForPointWrite(statement);
            invalidateGenericPkForPointWrite(statement);
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
                sqlMetrics.recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, elapsedNanos);
            }
            return deserialize(cachedBytes);
        }
        ExecutionResult result = next.proceed(statement);
        byte[] serialized = serialize(result);
        // Synchronous, deliberately -- an earlier version of this fix made this putAsync
        // (fire-and-forget), reasoning the client already has its answer and doesn't need to wait
        // for the Ignite write. Reverted: it broke real, already-encoded correctness guarantees --
        // CacheStageGenericPkTest's own back-to-back "populate, then immediately read the same
        // key" assertions (see e.g. compositePkOrderIndependentInWhereClause) started failing
        // because the very next call could race ahead of the async put and see a miss instead of
        // the hit that call, and the codebase at large, assumes is guaranteed once this method has
        // returned. See docs/RTT_BASELINE_2026.md for the measured cost of keeping this
        // synchronous (small in the JIT-warm steady state; the multi-millisecond numbers this
        // session originally measured were dominated by one-time JVM/Ignite classloading and JIT
        // warm-up on a table's first-ever access, not a fixable per-request cost).
        resultCache.put(key, serialized);
        recordKeyForTable(key, statement.sqlText());
        return result;
    }

    /** Called once from {@code Main} right after the shared collector is constructed. */
    public void setSqlMetrics(com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
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
                        com.sayonora.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - start);
            }
            log.debug("row cache hit: {}", key);
            com.sayonora.wire.core.ColumnInfo column =
                    new com.sayonora.wire.core.ColumnInfo(valueColumnName, java.sql.Types.VARCHAR, 0, 0, 0, false);
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

    /**
     * The generic (any backend, any table, any PK column names) counterpart of {@link
     * #tryRowCacheLookup} -- reached only for a table {@link PrimaryKeyCatalog} found a real 1..
     * {@link PrimaryKeyCatalog#MAX_PK_COLUMNS}-column primary key for. Covers ordinary tables
     * written through orawire/mywire/mssqlwire's translated-mode or same-dialect-native-pinned
     * pipeline path against ANY backend (Oracle, MySQL, SQL Server, or Postgres) -- exactly the
     * two deployment shapes this was built for: {@code driver -> Warp (translated/native-pinned)
     * -> a same-dialect backend}, and {@code driver -> Warp (translated) -> a Postgres backend}.
     * Deliberately does NOT cover orawire's OWN true native mode ({@code NativeSessionRelay}),
     * which bypasses SQL parsing -- and therefore every cache layer, not just this one -- entirely;
     * see {@code Main}'s own comment on that and {@link PrimaryKeyCatalog}'s class javadoc.
     *
     * @return {@code null} if {@code sql} isn't a single-table SELECT whose WHERE clause is EXACTLY
     *      a conjunction of equality predicates covering every one of the table's real PK columns
     *      (and nothing else) -- the caller falls through to this stage's existing behavior
     *      untouched, same contract as {@link #tryRowCacheLookup}.
     */
    private ExecutionResult tryGenericPkLookup(Statement statement, PipelineChain next) throws SQLException {
        if (primaryKeysByTable.isEmpty()) {
            return null;
        }
        String sql = statement.sqlText();
        Matcher m = GENERIC_SELECT.matcher(sql);
        if (!m.matches()) {
            return null;
        }
        String table = normalizeTable(m.group(1));
        List<String> pkColumns = primaryKeysByTable.get(bareTableName(table));
        if (pkColumns == null) {
            return null;
        }
        List<Integer> localPositions = exactPkBindPositions(m.group(2), pkColumns);
        if (localPositions == null) {
            return null;
        }
        List<Object> binds = statement.bindParams();
        // A SELECT's only binds are the ones in its own WHERE clause (the column list can't carry
        // placeholders in a well-formed SELECT), so the WHERE clause's local bind numbering IS the
        // statement's global bind numbering here -- unlike the write side below, no offset needed.
        if (binds == null || binds.size() < pkColumns.size()) {
            return null;
        }
        String key = genericPkKey(table, pkColumns, localPositions, binds);
        return lookupOrExecuteAndCacheGeneric(key, statement, next);
    }

    private ExecutionResult lookupOrExecuteAndCacheGeneric(String key, Statement statement, PipelineChain next) throws SQLException {
        long start = System.nanoTime();
        byte[] cached = pkRowCache.get(key);
        if (cached != null) {
            if (sqlMetrics != null) {
                sqlMetrics.recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.protocolName(statement.sourceDialect()),
                        com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - start);
            }
            log.debug("generic pk cache hit: {}", key);
            return deserialize(cached);
        }
        ExecutionResult result = next.proceed(statement);
        // A real primary-key-equality WHERE clause covering every PK column can match at most one
        // row by definition -- if the backend somehow returned more (or the "PK" this catalog
        // found isn't actually unique, e.g. a stale/incorrect discovery), don't cache it: better a
        // cache miss forever than caching a result under a key that could collide with a different
        // row later.
        if (result.isQuery() && result.rows().size() <= 1) {
            // Synchronous -- see handleCacheableSelect's own comment on why an async populate here
            // was reverted (breaks the immediate "populate, then read the same key" guarantee
            // CacheStageGenericPkTest already encodes).
            pkRowCache.put(key, serialize(result));
        }
        return result;
    }

    /** As {@link #invalidateRowCacheForPointWrite}, for the generic PK cache above. */
    private void invalidateGenericPkForPointWrite(Statement statement) {
        if (primaryKeysByTable.isEmpty()) {
            return;
        }
        String sql = statement.sqlText();
        Matcher m = GENERIC_WRITE.matcher(sql);
        if (!m.matches()) {
            return;
        }
        String table = normalizeTable(m.group(1) != null ? m.group(1) : m.group(2));
        List<String> pkColumns = primaryKeysByTable.get(bareTableName(table));
        if (pkColumns == null) {
            return;
        }
        String whereClause = m.group(3);
        List<Integer> localPositions = exactPkBindPositions(whereClause, pkColumns);
        if (localPositions == null) {
            return;
        }
        List<Object> binds = statement.bindParams();
        if (binds == null) {
            return;
        }
        // Unlike the SELECT side, an UPDATE's SET clause can (and typically does) bind values
        // BEFORE the WHERE clause's own binds -- same reasoning ROW_WRITE's own comment above
        // already documents. The WHERE clause's own N binds are therefore always the LAST N
        // elements of bindParams, whatever N (the PK's real column count) turns out to be, not
        // necessarily indices 0..N-1.
        int whereBindCount = pkColumns.size();
        if (binds.size() < whereBindCount) {
            return;
        }
        int globalWhereStart = binds.size() - whereBindCount;
        List<Object> globalBinds = binds.subList(globalWhereStart, binds.size());
        String key = genericPkKey(table, pkColumns, localPositions, globalBinds);
        pkRowCache.remove(key);
        log.debug("generic pk cache invalidated: {}", key);
    }

    /**
     * Parses a WHERE clause and verifies it is EXACTLY a conjunction of {@code column = ?}
     * predicates (optionally alias/schema-qualified, e.g. {@code o.id = ?}) whose column set is
     * PRECISELY {@code pkColumns} -- no more (an extra predicate could narrow to a different,
     * non-representable subset), no fewer (a partial PK match isn't guaranteed unique), no other
     * shape (no {@code OR}, no {@code <}/{@code LIKE}/{@code IN}/etc.). A regex alone can't express
     * "these specific N columns, in any order, and nothing else" for an arbitrary column-name set,
     * so this splits on top-level {@code AND} and matches each piece individually in Java.
     *
     * @return for each of {@code pkColumns} (in that list's catalog order), which 0-based position
     *      in the WHERE clause's OWN local bind sequence (left to right as {@code ?} appears in the
     *      clause) holds that column's value -- or {@code null} if the clause doesn't have exactly
     *      this shape
     */
    static List<Integer> exactPkBindPositions(String whereClause, List<String> pkColumns) {
        if (whereClause == null || whereClause.toLowerCase(Locale.ROOT).contains(" or ")) {
            return null;
        }
        String[] parts = whereClause.trim().split("(?i)\\s+and\\s+");
        if (parts.length != pkColumns.size()) {
            return null;
        }
        List<String> foundColumnsInOrder = new ArrayList<>(parts.length);
        for (String part : parts) {
            Matcher cm = EQUALITY_CONJUNCT.matcher(part.trim());
            // matches() (not find()) anchors to the WHOLE trimmed piece -- "col = ? extra" or a
            // non-equality predicate simply doesn't match, correctly rejecting the whole clause.
            if (!cm.matches()) {
                return null;
            }
            foundColumnsInOrder.add(cm.group(1).toLowerCase(Locale.ROOT));
        }
        Set<String> pkLower = new HashSet<>();
        for (String c : pkColumns) {
            pkLower.add(c.toLowerCase(Locale.ROOT));
        }
        Set<String> foundSet = new HashSet<>(foundColumnsInOrder);
        if (!foundSet.equals(pkLower) || foundColumnsInOrder.size() != pkColumns.size()) {
            return null;
        }
        List<Integer> positions = new ArrayList<>(pkColumns.size());
        for (String pkCol : pkColumns) {
            positions.add(foundColumnsInOrder.indexOf(pkCol.toLowerCase(Locale.ROOT)));
        }
        return positions;
    }

    private static String genericPkKey(String table, List<String> pkColumns, List<Integer> localPositions, List<Object> whereLocalBinds) {
        StringBuilder sb = new StringBuilder("pk|").append(table);
        for (int i = 0; i < pkColumns.size(); i++) {
            sb.append('|').append(pkColumns.get(i)).append('=').append(whereLocalBinds.get(localPositions.get(i)));
        }
        return sb.toString();
    }

    /** {@code table} here may be {@code "schema.table"} (whatever the SQL's FROM/UPDATE/DELETE
     * target literally was) or a bare name -- {@link PrimaryKeyCatalog} keys its map by bare table
     * name only (see its own javadoc on that simplification), so this strips any schema prefix
     * before the map lookup. */
    private static String bareTableName(String normalizedTable) {
        int dot = normalizedTable.indexOf('.');
        return dot > 0 ? normalizedTable.substring(dot + 1) : normalizedTable;
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

    /** Off the response critical path, same reasoning as {@link #handleCacheableSelect}'s own
     * {@code putAsync} above -- this only maintains the invalidation index, it can never affect
     * what a concurrent reader sees for {@code cacheKey} itself (that's {@code resultCache}, a
     * separate cache entry). Worst case if this races with an invalidation: one extra stale read
     * survives until TTL, the exact same "conservative, not incorrect" tradeoff this class's own
     * {@link #invalidate(String)} javadoc already documents for the synchronous path this replaces. */
    /** Stays synchronous, deliberately -- {@link #invalidate(String)} reads this same index on a
     * concurrent write to the same table, so a read's cache key must be durably recorded here
     * before this call (and therefore the read's own response) returns; making this async would
     * open a real window where a write landing right after a read gets served finishes its own
     * invalidation scan BEFORE the read's key is indexed, leaving that read's now-stale cache
     * entry un-invalidated -- silently serving stale data after a write, exactly the kind of race
     * the fix for this class was told to avoid introducing. What IS a safe, real win here: this
     * used to be two separate synchronous Ignite round trips (a {@code get} then a {@code put}) --
     * collapsed into one atomic {@code invoke}, so it's the same guarantee for half the network
     * cost. */
    private void recordKeyForTable(String cacheKey, String sql) {
        String table = extractFromTarget(sql);
        if (table == null) {
            return;
        }
        String normalized = normalizeTable(table);
        keysByTable.invoke(normalized, new AddKeyEntryProcessor(cacheKey));
    }

    /**
     * A plain lambda passed to {@link IgniteCache#invoke} is NOT usable here -- {@code
     * javax.cache.processor.EntryProcessor} does not extend {@link Serializable}, but Ignite still
     * marshals the processor to execute it as a job even for a single-node/local invoke (this is
     * NOT bypassed the way a plain get()/put() pair would be). A raw lambda throws at runtime
     * (caught live: every {@code recordKeyForTable} call crashed its connection with "server closed
     * the connection unexpectedly" the moment this stage was exercised through a real deployed
     * Warp process -- a `CacheStageGenericPkTest` JUnit test using {@code
     * WarpCluster.startSingleNodeForCacheOnly()} did not catch this, for reasons not fully
     * root-caused here, but confirmed live via `tests/python/test_cache_rtt.py` against a real
     * subprocess). A named class implementing {@link Serializable} is required.
     */
    private static final class AddKeyEntryProcessor
            implements EntryProcessor<String, Set<String>, Void>, Serializable {
        private final String cacheKey;

        AddKeyEntryProcessor(String cacheKey) {
            this.cacheKey = cacheKey;
        }

        @Override
        public Void process(MutableEntry<String, Set<String>> entry, Object... args) {
            Set<String> existing = entry.getValue();
            Set<String> keys = existing == null ? new HashSet<>() : new HashSet<>(existing);
            keys.add(cacheKey);
            entry.setValue(keys);
            return null;
        }
    }

    private void invalidate(String sql) {
        String table = extractWriteTarget(sql);
        if (table == null) {
            log.debug("cache invalidation: couldn't isolate a write target in \"{}\" — leaving cache as-is until TTL expiry", sql);
            return;
        }
        // WRITE_TARGET's single capture group is either "orders" or "public.orders" -- split it
        // the same way invalidateTable's own two-spelling removal below expects, so a regex-driven
        // invalidation and a LISTEN/NOTIFY-driven one (CacheInvalidationListener) go through the
        // exact same code path and can never disagree about which index entries to drop.
        int dot = table.indexOf('.');
        if (dot > 0) {
            invalidateTable(table.substring(0, dot), table.substring(dot + 1));
        } else {
            invalidateTable(null, table);
        }
    }

    /**
     * The one and only result-cache invalidation path -- reached both from this stage's own
     * regex-driven {@link #invalidate(String)} on a Warp-executed write, and from
     * {@link CacheInvalidationListener} on a Postgres {@code NOTIFY} for a write that never went
     * through Warp at all.
     *
     * <p>{@code keysByTable} is keyed by whatever the cached SELECT literally wrote in its FROM
     * clause (see {@link #recordKeyForTable}) -- {@code orders} and {@code public.orders} are two
     * DIFFERENT index entries for the same physical table. So this removes BOTH spellings
     * (lower-cased) whenever a schema is known, and just the bare one when it isn't. Conservative
     * by design: over-removing costs one extra backend round trip on the next read; under-removing
     * serves a stale row.
     *
     * @param schema the table's schema, or {@code null} when the caller only knows the bare name
     */
    public void invalidateTable(String schema, String table) {
        if (table == null || table.isBlank()) {
            return;
        }
        String bare = normalizeTable(table);
        int removed = invalidateIndexEntry(bare);
        if (schema != null && !schema.isBlank()) {
            removed += invalidateIndexEntry(normalizeTable(schema + "." + table));
        }
        if (removed > 0) {
            log.debug("cache invalidation: table={}{} removed {} entries", schema == null ? "" : schema + ".", bare, removed);
        }
    }

    private int invalidateIndexEntry(String normalized) {
        // Read the volatile fields once per call, not once per stage lifetime -- reconfigure()
        // swaps resultCache for a fresh instance on a TTL change, and a listener thread calling
        // in here concurrently must see the instance the read path is currently using.
        IgniteCache<String, byte[]> results = this.resultCache;
        IgniteCache<String, java.util.Set<String>> index = this.keysByTable;
        java.util.Set<String> keys = index.get(normalized);
        if (keys == null || keys.isEmpty()) {
            return 0;
        }
        for (String key : keys) {
            results.remove(key);
        }
        index.remove(normalized);
        return keys.size();
    }

    /**
     * Drops EVERY result-cache entry and the whole {@code keysByTable} index -- the blunt tool
     * {@link CacheInvalidationListener} reaches for on every LISTEN (re)connect, because NOTIFY is
     * not durable: any write committed while the LISTEN connection was down produced a
     * notification nobody received, and there's no way to know which tables it touched. {@code
     * reason} is logged so an operator can tell a startup clear from a reconnect-after-outage one.
     */
    public void clearAll(String reason) {
        IgniteCache<String, byte[]> results = this.resultCache;
        IgniteCache<String, java.util.Set<String>> index = this.keysByTable;
        int before = results.size();
        results.clear();
        index.clear();
        log.info("cache invalidation: cleared the whole result cache ({} entries) -- {}", before, reason);
    }

    /**
     * The {@link RowCache} physical-table key prefix for a Postgres {@code (schema, table)} pair,
     * as decided by the two protocol lookups this stage already holds -- or {@code null} when
     * neither protocol claims the table (no row-cache entries can exist for it, so a listener has
     * nothing to invalidate there). dynamowire keys by the BARE {@code dynamo_item_*} name (see
     * {@code OperationHandlers#cacheKeyFor}); mongowire keys by {@code db.collection} as
     * {@code MongoCommandDispatcher} spells it -- case-preserved -- while this stage's own SQL-side
     * fast path lower-cases the same pair, so the mongowire case returns the case-preserved
     * spelling and the caller is expected to also try {@link #normalizeTable}'s lower-cased form.
     */
    public String rowCacheKeyFor(String schema, String table) {
        if (table == null) {
            return null;
        }
        RowTableLookup dynamo = this.rowTableLookup;
        if (dynamo != null && dynamo.hasSortKey(normalizeTable(table)) != null) {
            return normalizeTable(table);
        }
        java.util.function.Predicate<String> mongo = this.mongoTableLookup;
        if (mongo != null && schema != null && mongo.test(normalizeTable(schema + "." + table))) {
            return schema + "." + table;
        }
        return null;
    }

    /** Lower-cases an identifier the same way every index/row-cache key in this stage does --
     * public so a caller of {@link #rowCacheKeyFor} can derive the SQL-side spelling too. */
    public static String normalizeTable(String table) {
        return table.toLowerCase(java.util.Locale.ROOT);
    }

    static String extractWriteTarget(String sql) {
        Matcher m = WRITE_TARGET.matcher(sql);
        return m.find() ? m.group(1) : null;
    }

    static String extractFromTarget(String sql) {
        Matcher m = FROM_TARGET.matcher(sql);
        return m.find() ? m.group(1) : null;
    }
}
