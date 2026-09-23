package com.sayonora.wire.cluster;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real primary-key column discovery for {@link CacheStage}'s generic point-lookup/point-write row
 * cache -- the mechanism that lets an ORDINARY table (any backend, any PK column names) get the
 * same row-cache precision dynamowire/mongowire already have via their own fixed physical shapes.
 *
 * <p>Scope, deliberately: only backends whose {@code targetBackend} traffic goes through the
 * shared pipeline (translated mode against any backend, or a translated/dual-exec statement pinned
 * to a same-dialect native target -- see {@code Main}'s own comment on
 * {@code nativeBackendTargets}) ever reaches {@link CacheStage#handle}. orawire's own true native
 * mode ({@code NativeSessionRelay}, a raw byte pump) never parses SQL at all and therefore never
 * reaches this cache layer, or any cache layer -- there is nothing to fix here for that path; it is
 * a separate, disclosed architectural gap.
 *
 * <p>PK discovery runs ONCE per {@link #discover} call (startup, and again on a config reload that
 * changes {@code WARP_CACHE_TABLES}) against every registered backend, real JDBC metadata
 * ({@link DatabaseMetaData#getPrimaryKeys}), first backend to report a real PK for a given table
 * wins. This is a real, disclosed simplification: if two backends genuinely disagree about a same-
 * named table's primary key (a sharded deployment where "orders" means different things on
 * different backends), this catalog cannot tell them apart -- the SQL-shape match in
 * {@code CacheStage} is keyed by table name alone, same granularity {@code WARP_CACHE_TABLES}
 * itself already uses. A table with no discoverable PK, or more than
 * {@link #MAX_PK_COLUMNS} PK columns, is simply absent from the returned map -- {@code CacheStage}
 * treats "no entry" as "don't attempt the generic PK fast path for this table", falling through
 * to its existing (correct, just coarser) table-level caching/invalidation untouched.
 */
public final class PrimaryKeyCatalog {

    private static final Logger log = LoggerFactory.getLogger(PrimaryKeyCatalog.class);

    /** Kept small and explicit: the regex-based conjunct matcher in {@code CacheStage} has to
     * enumerate exactly this many required equality predicates, so an unbounded PK arity would
     * mean an unbounded regex. Three columns already covers the overwhelming majority of real
     * composite keys; a wider key falls back to table-level caching, never a correctness issue. */
    public static final int MAX_PK_COLUMNS = 3;

    private PrimaryKeyCatalog() {
    }

    /**
     * @param cacheTableNames the same {@code WARP_CACHE_TABLES} entries {@link CacheStage} already
     *      matches against -- each either a bare table name or {@code schema.table}
     * @return normalized (lower-cased) bare-table-name -&gt; ordered PK column list (catalog key
     *      order, per {@code KEY_SEQ}), for every requested table where a real PK with 1..{@link
     *      #MAX_PK_COLUMNS} columns was found on at least one registered backend
     */
    public static Map<String, List<String>> discover(BackendRegistry registry, List<String> cacheTableNames) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (registry == null || cacheTableNames == null || cacheTableNames.isEmpty()) {
            return result;
        }
        for (String rawName : cacheTableNames) {
            String trimmed = rawName == null ? "" : rawName.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String schema = null;
            String table = trimmed;
            int dot = trimmed.indexOf('.');
            if (dot > 0) {
                schema = trimmed.substring(0, dot);
                table = trimmed.substring(dot + 1);
            }
            List<String> pk = discoverOne(registry, schema, table);
            if (pk != null && !pk.isEmpty() && pk.size() <= MAX_PK_COLUMNS) {
                result.put(CacheStage.normalizeTable(table), pk);
            }
        }
        return result;
    }

    private static List<String> discoverOne(BackendRegistry registry, String schema, String table) {
        for (BackendTarget target : registry.all()) {
            try (Connection conn = target.open()) {
                List<String> pk = primaryKeyColumns(conn, schema, table);
                if (pk != null && !pk.isEmpty()) {
                    return pk;
                }
            } catch (SQLException e) {
                // A backend that can't open, or doesn't have this table, just doesn't contribute a
                // PK for it -- normal and expected for every backend except the one(s) actually
                // hosting the table. Logged at debug only; this runs at startup/reload, not per
                // request, so it's never on any hot path.
                log.debug("primary-key discovery: backend {} has no usable PK for {}{} ({})",
                        target.name(), schema == null ? "" : schema + ".", table, e.getMessage());
            }
        }
        return null;
    }

    private static List<String> primaryKeyColumns(Connection conn, String schema, String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        // Try the exact case first (Oracle/most engines fold unquoted identifiers to uppercase;
        // Postgres folds to lowercase), then the opposite case -- JDBC's getPrimaryKeys is exact-
        // match, not case-insensitive, and this catalog has no reliable way to know which fold
        // convention a given backend used without querying twice.
        List<String> exact = readPrimaryKeys(meta, schema, table);
        if (exact != null) {
            return exact;
        }
        List<String> upper = readPrimaryKeys(meta, schema == null ? null : schema.toUpperCase(Locale.ROOT),
                table.toUpperCase(Locale.ROOT));
        if (upper != null) {
            return upper;
        }
        return readPrimaryKeys(meta, schema == null ? null : schema.toLowerCase(Locale.ROOT),
                table.toLowerCase(Locale.ROOT));
    }

    private static List<String> readPrimaryKeys(DatabaseMetaData meta, String schema, String table) throws SQLException {
        Map<Short, String> bySeq = new TreeMap<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
        }
        if (bySeq.isEmpty()) {
            return null;
        }
        return new ArrayList<>(bySeq.values());
    }
}
