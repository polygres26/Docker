package com.polygres.advisor.llm;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Analyzes an uploaded performance/workload report (AWR for Oracle, a MySQL performance report, a
 * SQL Server DMV/Query Store export, ...) for customers who won't hand Advisor a live connect
 * string. This is the one place in Advisor where migration-difficulty findings come from an LLM
 * reading unstructured text rather than deterministic catalog queries ({@link
 * com.polygres.advisor.score.MigrationScorer} et al.) -- there's no live database to query
 * against a static report, so heuristic extraction is the only option. The findings this produces
 * are explicitly labeled less certain than the connection-based flow's in the UI for that reason.
 *
 * <p>Uses the PRIMARY LLM (and JUDGE, if configured -- same "second opinion on things a human will
 * act on" reasoning as {@link PlsqlSummarizer}, since a wrong reading of a report is exactly the
 * kind of mistake worth catching before a migration plan is built on it).
 */
public class ReportAnalyzer {

    private static final String SYSTEM_PROMPT = """
        You are analyzing a database performance/workload report (this may be an Oracle AWR
        report, a MySQL performance report, or a SQL Server DMV/Query Store export) for a
        Postgres-migration assessment tool. The customer has NOT given live database access --
        this report is the only signal available, so be explicit about uncertainty rather than
        inventing detail the report doesn't support.

        Extract what you can find and respond with ONLY a JSON object (no other text, no markdown
        fences) in exactly this shape:
        {
          "sourceVersion": "<database version/edition if visible in the report, else null>",
          "tier": "EASY" | "MEDIUM" | "HARD",
          "tierReason": "<one sentence justifying the tier>",
          "findings": [
            {"feature": "<short name>", "severity": "LOW" | "MEDIUM" | "HIGH", "note": "<what you found and why it matters for a Postgres migration>"}
          ],
          "topWorkload": [
            {"description": "<what this SQL/statement does, in plain language>", "detail": "<execution count / elapsed time / whatever resource stats the report shows>"}
          ],
          "caveats": ["<anything you could not determine from this report, or where you're inferring rather than reading directly>"]
        }
        """;

    private final LlmSettingsStore settingsStore;
    private final Gson gson = new Gson();

    public ReportAnalyzer(LlmSettingsStore settingsStore) {
        this.settingsStore = settingsStore;
    }

    public record Finding(String feature, String severity, String note) {}
    public record WorkloadItem(String description, String detail) {}
    public record Analysis(
        String sourceVersion, String tier, String tierReason,
        java.util.List<Finding> findings, java.util.List<WorkloadItem> topWorkload,
        java.util.List<String> caveats, LlmJudge.Verdict judgeVerdict
    ) {}

    public Analysis analyze(String dialect, String reportText) throws Exception {
        var primary = LlmProviderFactory.resolve(settingsStore.get(LlmRole.PRIMARY));

        // Reports (especially AWR HTML dumps) can be huge; keep the prompt within a sane budget
        // rather than failing outright on a multi-MB file -- the report's most information-dense
        // sections (top SQL, wait events) are conventionally near the top, so a straightforward
        // head-truncation is a reasonable first-pass tradeoff.
        String truncated = reportText.length() > 60_000 ? reportText.substring(0, 60_000) : reportText;

        String userPrompt = "Report dialect (as declared at upload time): " + dialect + "\n\n" + truncated;
        String response = primary.provider().complete(primary.model(), SYSTEM_PROMPT, userPrompt);
        RawAnalysis raw = parseJson(response);

        LlmJudge.Verdict verdict = null;
        LlmSettings judgeSettings = settingsStore.get(LlmRole.JUDGE);
        if (judgeSettings.isUsable()) {
            var judge = LlmProviderFactory.resolve(judgeSettings);
            verdict = new LlmJudge(judge.provider(), judge.model())
                .review(truncated, gson.toJson(raw));
        }

        return new Analysis(raw.sourceVersion, raw.tier, raw.tierReason,
            raw.findings == null ? java.util.List.of() : raw.findings,
            raw.topWorkload == null ? java.util.List.of() : raw.topWorkload,
            raw.caveats == null ? java.util.List.of() : raw.caveats,
            verdict);
    }

    private RawAnalysis parseJson(String response) {
        String json = response.strip();
        if (json.startsWith("```")) {
            json = json.replaceFirst("^```[a-zA-Z]*\\n", "").replaceFirst("```$", "").strip();
        }
        try {
            RawAnalysis parsed = gson.fromJson(json, RawAnalysis.class);
            if (parsed != null) return parsed;
        } catch (JsonSyntaxException ignored) {
            // fall through to the "couldn't parse" placeholder below
        }
        RawAnalysis fallback = new RawAnalysis();
        fallback.tier = "MEDIUM";
        fallback.tierReason = "Could not parse a structured response from the model -- raw output included as a single finding.";
        fallback.findings = java.util.List.of(new Finding("Unparsed model output", "MEDIUM", response));
        return fallback;
    }

    private static class RawAnalysis {
        String sourceVersion;
        String tier;
        String tierReason;
        java.util.List<Finding> findings;
        java.util.List<WorkloadItem> topWorkload;
        java.util.List<String> caveats;
    }
}
