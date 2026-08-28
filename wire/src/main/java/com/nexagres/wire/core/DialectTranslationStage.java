package com.nexagres.wire.core;

import com.nexagres.wire.config.TranslationCacheStore;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DialectTranslationStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(DialectTranslationStage.class);

    private final BackendRegistry registry;
    private final TranslationCache cache;
    // volatile: reconfigureLlm() swaps this from the polywire_config LISTEN/NOTIFY callback on a
    // different thread than the pipeline threads that read it via handle()/translateWithFallback.
    // null means "no LLM fallback configured" (provider=none, or a config-store client that
    // failed to build) -- see translateWithFallback's null-llmClient handling below.
    private volatile TranslationLlmClient llmClient;
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

    /**
     * Hot-reload hook for {@code polywire_config}'s LLM settings, called from the same
     * {@code ConfigStore#listen} callback every other stage reconfigures from (see
     * {@code Main#main}) -- no restart needed to pick up a provider/apiKey/baseUrl/model change
     * made through {@code /api/llm-config}. Delegates to {@link TranslationLlmClient#fromConfig}
     * for the "config value wins, env var is the bootstrap fallback, provider=none disables the
     * LLM fallback entirely" precedence rules.
     */
    public void reconfigureLlm(String provider, String apiKey, String baseUrl, String model) {
        this.llmClient = TranslationLlmClient.fromConfig(provider, apiKey, baseUrl, model);
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

        if (llmClient == null) {
            // provider=none (or no LLM ever configured) -- the AST rewriter above is the only
            // translator, and it just returned null for this statement. Surface that as an
            // UntranslatableQueryException directly rather than silently doing nothing: "none"
            // means "no fallback", not "pretend it succeeded".
            throw new UntranslatableQueryException(sqlText, fromDialect, targetDialect,
                    "no LLM fallback translator configured (provider=none) and the deterministic "
                            + "AST rewriter could not translate this statement");
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
