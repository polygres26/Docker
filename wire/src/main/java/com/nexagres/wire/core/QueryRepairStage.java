package com.nexagres.wire.core;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last-resort self-healing for a real execution failure Postgres genuinely rejected -- distinct
 * from {@link DialectTranslationStage}'s LLM fallback, which only ever fires when {@code
 * sourceDialect != targetDialect} (a KNOWN cross-dialect gap). This stage instead reacts to the
 * ACTUAL SQLSTATE a real backend returned, so it also covers same-dialect statements that slipped
 * through translation untouched because they were syntactically valid SQL text in general, just
 * not something this specific backend accepts -- an Oracle-flavored builtin used from a plain
 * pgwire session with no dialect mismatch to ever trigger translation, for example.
 *
 * <p>Deliberately placed as the LAST pipeline stage (see {@code Main#main}) so it wraps only the
 * terminal {@link BackendExecutor} call directly -- every other stage (cache, stats, rollups)
 * sees a normal successful {@link ExecutionResult} if the repair works, with no special-casing
 * needed anywhere else in the pipeline.
 *
 * <p>Attempts exactly one repair per statement: the retry goes straight to {@code next} (the
 * terminal executor, since this stage is always last), never back through this stage again, so a
 * repaired statement that ALSO fails just propagates that second failure -- there is no retry
 * loop to bound separately.
 *
 * <p>Off by default ({@link #ENABLED_ENV}) -- unlike dialect translation (a known, necessary gap
 * between two SQL dialects every cross-dialect statement has to cross), rewriting a statement a
 * backend flatly rejected is a strictly more invasive thing to do silently: it can change what a
 * broken query actually executes as, not just how a valid one is phrased. An operator opts in
 * deliberately, the same way {@code db_emulation} extensions are opt-in rather than always on.
 */
public final class QueryRepairStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(QueryRepairStage.class);
    public static final String ENABLED_ENV = "WARP_QUERY_REPAIR_ENABLED";

    // SQLSTATEs worth one LLM repair attempt -- all "this SQL text itself is the problem" cases,
    // deliberately never "this table/role/permission genuinely doesn't exist" ones (42P01
    // undefined_table, 42501 insufficient_privilege, etc.): an LLM cannot conjure a missing object
    // into existence, so attempting a repair there would just add latency to an error the client
    // needs to see exactly as Postgres reported it, with nothing to show for the extra round trip.
    private static final Set<String> REPAIRABLE_SQLSTATES = Set.of(
            "42601", // syntax_error
            "42883", // undefined_function -- e.g. a dialect-specific builtin translation missed
            "42804", // datatype_mismatch -- often an implicit-cast difference between dialects
            "0A000"  // feature_not_supported
    );

    private final boolean enabled;
    // volatile: reconfigureLlm() swaps this from the same warp_config LISTEN/NOTIFY callback
    // DialectTranslationStage#reconfigureLlm reacts to, on a different thread than the pipeline
    // threads that read it via handle() -- same pattern, same one LLM config surface for both
    // LLM-backed features, not a second one to keep in sync.
    private volatile TranslationLlmClient llmClient;
    private final AtomicLong attempts = new AtomicLong();
    private final AtomicLong repaired = new AtomicLong();

    public QueryRepairStage(boolean enabled, TranslationLlmClient llmClient) {
        this.enabled = enabled;
        this.llmClient = llmClient;
    }

    public void reconfigureLlm(String provider, String apiKey, String baseUrl, String model) {
        this.llmClient = TranslationLlmClient.fromConfig(provider, apiKey, baseUrl, model);
    }

    /** Admin-console-visible counters -- see {@code MetricsServer}'s summary endpoint. Not
     * persisted: a process restart resetting these is fine, the same tradeoff every other
     * in-memory counter {@code StatsCollectorStage} exposes already makes. */
    public long attemptCount() {
        return attempts.get();
    }

    public long repairedCount() {
        return repaired.get();
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        if (!enabled || llmClient == null) {
            return next.proceed(statement);
        }
        try {
            return next.proceed(statement);
        } catch (SQLException original) {
            if (!REPAIRABLE_SQLSTATES.contains(original.getSQLState())) {
                throw original;
            }
            attempts.incrementAndGet();
            String repairedSql;
            try {
                repairedSql = llmClient.repair(statement.sqlText(), statement.sourceDialect(), original.getMessage());
            } catch (Exception llmFailure) {
                log.warn("query repair: LLM call failed ({}) -- surfacing the original Postgres error instead",
                        llmFailure.getMessage());
                throw original;
            }
            if (repairedSql == null || repairedSql.isBlank() || repairedSql.equals(statement.sqlText())) {
                throw original;
            }
            log.info("query repair: SQLSTATE {} on \"{}\" -- retrying once with LLM-repaired SQL: \"{}\"",
                    original.getSQLState(), statement.sqlText(), repairedSql);
            try {
                ExecutionResult result = next.proceed(statement.withSqlText(repairedSql));
                repaired.incrementAndGet();
                return result;
            } catch (SQLException retryFailure) {
                // The client's OWN SQL and Postgres's own error against it is what's actionable to
                // report -- the repair attempt is an internal implementation detail that didn't
                // pan out this time, not a second error to confuse the client with.
                log.warn("query repair: retry also failed ({}) -- surfacing the ORIGINAL Postgres error",
                        retryFailure.getMessage());
                throw original;
            }
        }
    }
}
