package com.nexagres.advisor.llm;

import com.nexagres.advisor.catalog.ObjectExplorer;
import com.nexagres.advisor.core.BackendTarget;

/**
 * Deep-reasoning use case: given one package/procedure/function's PL/SQL source (via the same
 * {@link ObjectExplorer} the Objects tab itself uses to display it), ask the configured PRIMARY
 * LLM to summarize its intent and flag Postgres-portability risks. This is the
 * "explainer/rewrite-assist" layer the project plan called out as explicitly downstream of
 * {@link com.nexagres.advisor.score.MigrationScorer} -- it never feeds back into the
 * deterministic score; it's read by a human reviewing the migration.
 *
 * <p>Deliberately takes an {@link ObjectExplorer}, not {@code OracleCatalogProfiler} (an earlier
 * version of this class did, and had a real bug for it: {@code OracleCatalogProfiler#fetchSource}
 * queries {@code USER_SOURCE}, which has no {@code OWNER} column and is implicitly scoped to the
 * connecting session's own objects, while the Objects tab always passes cross-schema
 * "OWNER.NAME" identifiers -- so summarizing anything selected from that tab silently returned
 * "no source found", every time. {@link com.nexagres.advisor.catalog.OracleObjectExplorer#fetchSource}
 * is the correct source of truth here: it already splits the owner qualifier and queries
 * {@code ALL_SOURCE}, so it resolves any object the connecting user has been granted visibility
 * into -- not just ones it owns -- exactly matching what the Objects tab itself already shows.
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
    private final ObjectExplorer explorer;

    public PlsqlSummarizer(LlmSettingsStore settingsStore, ObjectExplorer explorer) {
        this.settingsStore = settingsStore;
        this.explorer = explorer;
    }

    public record Result(String summary, LlmJudge.Verdict judgeVerdict) {}

    public Result summarize(BackendTarget target, String objectName, String objectType) throws Exception {
        LlmSettings primarySettings = settingsStore.get(LlmRole.PRIMARY);
        var primary = LlmProviderFactory.resolve(primarySettings);

        // explorer.fetchSource splits any "OWNER.NAME" qualifier itself and queries ALL_SOURCE --
        // see this class's javadoc for why that (not USER_SOURCE) is the correct source of truth,
        // and why it's a real bugfix, not a stylistic preference.
        String source = explorer.fetchSource(target, objectName, objectType);
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
