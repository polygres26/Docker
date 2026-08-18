package com.polygres.wire.core;

import com.polygres.wire.config.TranslationCacheStore;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DialectTranslationStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(DialectTranslationStage.class);

    private final BackendRegistry registry;
    private final TranslationCache cache;
    private final TranslationLlmClient llmClient;
    private final TranslationCacheStore cacheStore;

    public DialectTranslationStage(BackendRegistry registry) {
        this(registry, new TranslationCache(), new TranslationLlmClient(), null);
    }

    public DialectTranslationStage(BackendRegistry registry, TranslationCacheStore cacheStore) {
        this(registry, new TranslationCache(), new TranslationLlmClient(), cacheStore);
    }

    public DialectTranslationStage(BackendRegistry registry, TranslationCache cache, TranslationLlmClient llmClient) {
        this(registry, cache, llmClient, null);
    }

    public DialectTranslationStage(BackendRegistry registry, TranslationCache cache, TranslationLlmClient llmClient,
            TranslationCacheStore cacheStore) {
        this.registry = registry;
        this.cache = cache;
        this.llmClient = llmClient;
        this.cacheStore = cacheStore;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String targetName = statement.targetBackend();
        if (targetName == null || RoutingBackendExecutor.SCATTER_ALL.equals(targetName)) {
            return next.proceed(statement);
        }
        BackendTarget target = registry.get(targetName);
        if (target == null) {
            return next.proceed(statement);
        }
        SourceDialect targetDialect = target.dialect();
        SourceDialect fromDialect = statement.sourceDialect();
        if (targetDialect == null || fromDialect == targetDialect) {
            return next.proceed(statement);
        }
        String sqlText = statement.sqlText();
        String rewritten = translateWithFallback(sqlText, fromDialect, targetDialect, cache, llmClient, cacheStore);
        return next.proceed(statement.withSqlText(rewritten));
    }

    public static String translateWithFallback(String sqlText, SourceDialect fromDialect,
            SourceDialect targetDialect, TranslationCache cache, TranslationLlmClient llmClient)
            throws UntranslatableQueryException {
        return translateWithFallback(sqlText, fromDialect, targetDialect, cache, llmClient, null);
    }

    public static String translateWithFallback(String sqlText, SourceDialect fromDialect,
            SourceDialect targetDialect, TranslationCache cache, TranslationLlmClient llmClient,
            TranslationCacheStore cacheStore)
            throws UntranslatableQueryException {
        if (fromDialect == targetDialect) {
            return sqlText;
        }

        String cached = cache.get(sqlText, fromDialect, targetDialect);
        if (cached != null) {
            log.info("translation cache HIT for {}->{}: {}", fromDialect, targetDialect, sqlText);
            if (cacheStore != null) {
                cacheStore.recordAccess(fromDialect, targetDialect, sqlText, cached);
            }
            return cached;
        }
        log.info("translation cache MISS for {}->{}, translating: {}", fromDialect, targetDialect, sqlText);

        String rewritten = DialectTranslations.translate(sqlText, fromDialect, targetDialect);
        if (rewritten != null) {
            cache.put(sqlText, fromDialect, targetDialect, rewritten);
            if (cacheStore != null) {
                cacheStore.recordAccess(fromDialect, targetDialect, sqlText, rewritten);
            }
            return rewritten;
        }

        String llmTranslated;
        try {
            llmTranslated = llmClient.translate(sqlText, fromDialect, targetDialect);
        } catch (Exception e) {
            throw new UntranslatableQueryException(sqlText, fromDialect, targetDialect,
                    "LLM fallback translator failed: " + e.getMessage(), e);
        }
        if (llmTranslated == null || llmTranslated.isBlank()) {
            throw new UntranslatableQueryException(sqlText, fromDialect, targetDialect,
                    "LLM fallback translator did not return usable SQL");
        }
        cache.put(sqlText, fromDialect, targetDialect, llmTranslated);
        if (cacheStore != null) {
            cacheStore.recordAccess(fromDialect, targetDialect, sqlText, llmTranslated);
        }
        return llmTranslated;
    }
}
