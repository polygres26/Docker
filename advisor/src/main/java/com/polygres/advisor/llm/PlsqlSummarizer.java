package com.polygres.advisor.llm;

import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.core.BackendTarget;

/**
 * Deep-reasoning use case: given one package/procedure/function's PL/SQL source (via {@link
 * OracleCatalogProfiler#fetchSource}), ask the configured PRIMARY LLM to summarize its intent and
 * flag Postgres-portability risks. This is the "explainer/rewrite-assist" layer the project plan
 * called out as explicitly downstream of {@link com.polygres.advisor.score.MigrationScorer} -- it
 * never feeds back into the deterministic score; it's read by a human reviewing the migration.
 *
 * <p>Reads PRIMARY's (and, if configured, JUDGE's) settings from {@link LlmSettingsStore} rather
 * than an env var -- both roles are configured through the LLM configuration page, covering both
 * the built-in Claude access and any external OpenAI-compatible provider. See {@link LlmJudge}'s
 * javadoc for why this is the one place Judge review is wired in.
 */
public class PlsqlSummarizer {

    private static final String SYSTEM_PROMPT = """
        You are assisting an Oracle-to-Postgres migration assessment tool. Given the source of one
        PL/SQL package, procedure, or function, produce:
        1. A one-paragraph summary of what it does.
        2. A bullet list of any constructs that are non-trivial to port to Postgres/PL_pgSQL
           (e.g. DBMS_* package calls, autonomous transactions, CONNECT BY, package-level state,
           collections, dynamic SQL). Cite the specific construct, not a vague "may need review".
        3. A rough migration-effort estimate: trivial / moderate / substantial, with a one-sentence
           reason.
        Be concise and specific. This output is read by a database engineer doing the migration,
        not an end user -- do not soften or hedge findings.
        """;

    private final LlmSettingsStore settingsStore;
    private final OracleCatalogProfiler profiler;

    public PlsqlSummarizer(LlmSettingsStore settingsStore, OracleCatalogProfiler profiler) {
        this.settingsStore = settingsStore;
        this.profiler = profiler;
    }

    public record Result(String summary, LlmJudge.Verdict judgeVerdict) {}

    public Result summarize(BackendTarget target, String objectName, String objectType) throws Exception {
        LlmSettings primarySettings = settingsStore.get(LlmRole.PRIMARY);
        var primary = LlmProviderFactory.resolve(primarySettings);

        // The Objects browser (OracleObjectExplorer#listObjects) always returns cross-schema
        // "OWNER.NAME" identifiers so they stay unambiguous in a tree that spans every schema --
        // but OracleCatalogProfiler#fetchSource queries USER_SOURCE, which has no OWNER column
        // and is implicitly scoped to the connecting session's own objects. Passing the qualified
        // name straight through never matches (NAME = 'OWNER.THING' against a column that only
        // ever holds 'THING'), so summarizing anything selected from the Objects tab always
        // silently returned "no source found" -- found live while testing this end to end. Strip
        // the owner qualifier before the lookup, same convention OracleObjectExplorer's own
        // splitOwner already uses.
        String bareName = objectName.contains(".") ? objectName.substring(objectName.lastIndexOf('.') + 1) : objectName;
        String source = profiler.fetchSource(target, bareName, objectType);
        if (source.isBlank()) {
            return new Result("No source found for " + objectType + " " + objectName + ".", null);
        }

        String userPrompt = "Object: " + objectName + " (" + objectType + ")\n\n" + source;
        String summary = primary.provider().complete(primary.model(), SYSTEM_PROMPT, userPrompt);

        LlmJudge.Verdict verdict = null;
        LlmSettings judgeSettings = settingsStore.get(LlmRole.JUDGE);
        if (judgeSettings.isUsable()) {
            var judge = LlmProviderFactory.resolve(judgeSettings);
            verdict = new LlmJudge(judge.provider(), judge.model()).review(source, summary);
        }

        return new Result(summary, verdict);
    }
}
