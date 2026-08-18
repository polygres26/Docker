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

    private final PolyWireCluster cluster;
    
    private volatile List<Pattern> cachePatterns;
    private volatile long ttlMillis;
    
    private volatile IgniteCache<String, byte[]> resultCache;
    
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
