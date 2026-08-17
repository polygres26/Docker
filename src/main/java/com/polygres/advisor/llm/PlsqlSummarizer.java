package com.polygres.advisor.llm;

import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.core.BackendTarget;

/**
 * Deep-reasoning use case: given one package/procedure/function's PL/SQL source (via {@link
 * OracleCatalogProfiler#fetchSource}), ask Claude to summarize its intent and flag Postgres-
 * portability risks. This is the "explainer/rewrite-assist" layer the project plan called out as
 * explicitly downstream of {@link com.polygres.advisor.score.MigrationScorer} -- it never feeds
 * back into the deterministic score; it's read by a human reviewing the migration.
 *
 * <p>Reads its model id from {@code POLYGRES_LLM_SUMMARY_MODEL} -- no default baked in (see
 * {@link ClaudeLlmProvider}'s javadoc for why). Point it at a full Claude model, not a small one;
 * this is the side of the SLM/LLM split that benefits from deeper reasoning, per the project's
 * stated intent to use bigger models for genuine understanding and smaller ones for high-volume
 * classification (see {@link SqlWorkloadClassifier}).
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

    private final LlmProvider llm;
    private final OracleCatalogProfiler profiler;

    public PlsqlSummarizer(LlmProvider llm, OracleCatalogProfiler profiler) {
        this.llm = llm;
        this.profiler = profiler;
    }

    public String summarize(BackendTarget target, String objectName, String objectType) throws Exception {
        String model = requireEnv("POLYGRES_LLM_SUMMARY_MODEL");
        String source = profiler.fetchSource(target, objectName, objectType);
        if (source.isBlank()) {
            return "No source found for " + objectType + " " + objectName + ".";
        }
        String userPrompt = "Object: " + objectName + " (" + objectType + ")\n\n" + source;
        return llm.complete(model, SYSTEM_PROMPT, userPrompt);
    }

    private String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is not set -- required to run LLM summarization. "
                + "Set it to a Claude model id (see the claude-api reference for current ids).");
        }
        return value;
    }
}
