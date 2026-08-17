package com.polygres.wire.core;

import java.sql.SQLException;

/**
 * PolyWire's own JDBC-native equivalent of Oracle's {@code dg4odbc}/Heterogeneous Services: rather
 * than relying on a real Oracle instance's HS layer to translate SQL over an ODBC bridge to a
 * foreign store (ARCHITECTURE.md §10a — researched, never verified end-to-end, needs an Oracle+
 * unixODBC toolchain this project doesn't have), this stage does the dialect rewriting itself,
 * inside PolyWire, purely over the JDBC connections it already holds — so a query written against
 * Oracle, Postgres, or MySQL syntax can reference a {@code table@link} pointing at a differently-
 * dialected backend and just work, the same way a real Oracle database link makes a foreign table
 * look like a local one.
 *
 * <p>Runs immediately after {@link DbLinkStage} (see {@code GatewayComponents}) — it only acts on
 * a {@code targetBackend} that stage already resolved, translating from the requesting frontend's
 * dialect ({@link Statement#sourceDialect()} — every {@code WireFrontend} stamps this as the
 * dialect its own SQL text is authored in, per {@link Statement}'s javadoc) to the target
 * {@link BackendTarget}'s dialect ({@link BackendTarget#dialect()}, inferred from its JDBC URL
 * scheme). {@link RouterStage}-assigned targets (schema/predicate/shard rules) are <b>not</b>
 * covered by this first slice — this stage runs before {@code RouterStage}, so a target it assigns
 * later never passes back through here.
 *
 * <p>The actual rule vocabulary lives in {@link DialectTranslations} — a hub-and-spoke design
 * (normalize the source dialect once into a shared canonical form, render once per target dialect)
 * chosen specifically so adding a new target backend dialect costs one renderer function, not one
 * new pairwise translator per existing source. See that class's javadoc for exactly which
 * {@code (source, target)} pairs have rules today, what each rule does, and — stated honestly —
 * which pairs are live-verified against a real backend versus unit-tested only.
 *
 * <p><b>Not attempted</b>: cross-backend joins (still {@code DbLinkStage}'s scope limit, unchanged
 * by this stage); {@code (+)} outer-join rewriting (needs real join-structure understanding, not
 * token substitution); result-value type conversion (Oracle {@code NUMBER}→Postgres
 * {@code numeric}/{@code integer}, {@code DATE} time-of-day) — this stage only rewrites the SQL
 * text going in, not the rows coming back.
 */
public final class DialectTranslationStage implements PipelineStage {

    private final BackendRegistry registry;
    private final TranslationCache cache;
    private final TranslationLlmClient llmClient;

    public DialectTranslationStage(BackendRegistry registry) {
        this(registry, new TranslationCache(), new TranslationLlmClient());
    }

    /** Overload for tests, or an operator-supplied {@link TranslationLlmClient} pointed at a
     * non-default provider. */
    public DialectTranslationStage(BackendRegistry registry, TranslationCache cache, TranslationLlmClient llmClient) {
        this.registry = registry;
        this.cache = cache;
        this.llmClient = llmClient;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String targetName = statement.targetBackend();
        if (targetName == null || RoutingBackendExecutor.SCATTER_ALL.equals(targetName)) {
            return next.proceed(statement); // no single named JDBC target resolved yet -- nothing to translate for
        }
        BackendTarget target = registry.get(targetName);
        if (target == null) {
            return next.proceed(statement); // unknown name -- let a later stage (or the executor) raise the real error
        }
        SourceDialect targetDialect = target.dialect();
        SourceDialect fromDialect = statement.sourceDialect();
        if (targetDialect == null || fromDialect == targetDialect) {
            return next.proceed(statement); // same dialect already, or target's dialect isn't one we recognize
        }
        String sqlText = statement.sqlText();

        String cached = cache.get(sqlText, fromDialect, targetDialect);
        if (cached != null) {
            return next.proceed(statement.withSqlText(cached));
        }

        String rewritten = DialectTranslations.translate(sqlText, fromDialect, targetDialect);
        if (rewritten != null) {
            cache.put(sqlText, fromDialect, targetDialect, rewritten);
            return next.proceed(statement.withSqlText(rewritten));
        }

        // No deterministic rule for this construct -- fall back to the LLM translator rather than
        // passing the untranslated SQL straight through (Omnigate's DialectTranslationStage passes
        // through unchanged here; PolyWire deliberately does not -- see UntranslatableQueryException's
        // javadoc for why "best effort"/silent pass-through was rejected for this project).
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
        return next.proceed(statement.withSqlText(llmTranslated));
    }
}
