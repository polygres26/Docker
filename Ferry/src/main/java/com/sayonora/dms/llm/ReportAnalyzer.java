package com.sayonora.dms.llm;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Analyzes an uploaded performance/workload report (AWR for Oracle, a MySQL performance report, a
 * SQL Server DMV/Query Store export, ...) for customers who won't hand Advisor a live connect
 * string. This is the one place in Advisor where migration-difficulty findings come from an LLM
 * reading unstructured text rather than deterministic catalog queries ({@link
 * com.sayonora.dms.score.MigrationScorer} et al.) -- there's no live database to query
 * against a static report, so heuristic extraction is the only option. The findings this produces
 * are explicitly labeled less certain than the connection-based flow's in the UI for that reason.
 *
 * <p>Uses the PRIMARY LLM (and JUDGE, if configured -- same "second opinion on things a human will
 * act on" reasoning as {@link PlsqlSummarizer}, since a wrong reading of a report is exactly the
 * kind of mistake worth catching before a migration plan is built on it).
 *
 * <p>Also extracts {@code sizingSignals} when the report states them directly (AWR reports
 * conventionally list CPU count and Memory(GB) in their header) -- this is what feeds
 * {@link com.sayonora.dms.sizing.SizingCalculator} for the report-based sizing flow, the same
 * role {@code CatalogSnapshot.schemaSizeBytes}/{@code WorkloadSummary} play for the connection-
 * based one.
 */
public class ReportAnalyzer {

    private static final String SYSTEM_PROMPT = """
        You are analyzing one or more database performance/workload reports (these may include
        Oracle AWR reports, MySQL performance reports, or SQL Server DMV/Query Store exports,
        each marked with its own "=== Report ===" section header) for a Postgres-migration
        assessment tool. The customer has NOT given live database access -- these reports are the
        only signal available, so be explicit about uncertainty rather than inventing detail the
        reports don't support. When multiple reports are given, synthesize one combined
        assessment rather than repeating each report's findings separately.

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
          "caveats": ["<anything you could not determine from this report, or where you're inferring rather than reading directly>"],
          "sizingSignals": {
            "cpuCores": <integer CPU core count if the report states it directly, else null>,
            "memoryGB": <integer RAM in GB if the report states it directly, else null>,
            "dataSizeGB": <numeric database/schema size in GB if the report states it directly, else null>
          }
        }
        Only fill in sizingSignals fields the report actually states -- do not estimate or infer
        them from indirect evidence; leave them null if genuinely absent.
        """;

    private final LlmSettingsStore settingsStore;
    private final Gson gson = new Gson();

    public ReportAnalyzer(LlmSettingsStore settingsStore) {
        this.settingsStore = settingsStore;
    }

    public record Finding(String feature, String severity, String note) {}
    public record WorkloadItem(String description, String detail) {}
    public record SizingSignals(Integer cpuCores, Integer memoryGB, Double dataSizeGB) {}
    public record Analysis(
        String sourceVersion, String tier, String tierReason,
        java.util.List<Finding> findings, java.util.List<WorkloadItem> topWorkload,
        java.util.List<String> caveats, SizingSignals sizingSignals, LlmJudge.Verdict judgeVerdict
    ) {}

    /** One report the caller wants analyzed -- used both for a single upload and for {@link #analyzeMultiple}. */
    public record ReportInput(String name, String dialect, String text) {}

    public Analysis analyze(String dialect, String reportText) throws Exception {
        return analyzeMultiple(java.util.List.of(new ReportInput(null, dialect, reportText)));
    }

    /**
     * Combines several uploaded reports into one analysis -- e.g. a customer who uploaded an AWR
     * snapshot alongside a separate workload export for the same system, or several AWR snapshots
     * taken at different times. Each report gets its own labeled section and its own share of the
     * total prompt budget (rather than the first report alone eating the whole budget), so the
     * model sees a representative slice of every report, not just the first.
     */
    public Analysis analyzeMultiple(java.util.List<ReportInput> reports) throws Exception {
        var primary = LlmProviderFactory.resolve(settingsStore.get(LlmRole.PRIMARY));

        // Reports (especially AWR HTML dumps) can be huge; keep the combined prompt within a sane
        // budget rather than failing outright on a multi-MB file -- a report's most
        // information-dense sections (top SQL, wait events) are conventionally near the top, so
        // per-report head-truncation is a reasonable first-pass tradeoff.
        int perReportBudget = Math.max(5_000, 60_000 / Math.max(1, reports.size()));
        StringBuilder combined = new StringBuilder();
        String dialectLabel = reports.stream().map(ReportInput::dialect).distinct().reduce((a, b) -> a + " + " + b).orElse("UNKNOWN");
        for (ReportInput r : reports) {
            combined.append("=== Report");
            if (r.name() != null) combined.append(": ").append(r.name());
            combined.append(" (declared dialect: ").append(r.dialect()).append(") ===\n");
            combined.append(r.text().length() > perReportBudget ? r.text().substring(0, perReportBudget) : r.text());
            combined.append("\n\n");
        }
        String truncated = combined.toString();

        String userPrompt = "Report dialect(s) (as declared at upload time): " + dialectLabel + "\n\n" + truncated;
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
            raw.sizingSignals,
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
        SizingSignals sizingSignals;
    }
}
