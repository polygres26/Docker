package com.nexagres.advisor.llm;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.nexagres.advisor.workload.CapturedStatement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * High-volume, cheap classification use case: bucket each {@link CapturedStatement} into a
 * migration-relevant category. This is the "SLM" side of the project's SLM/LLM split -- meant to
 * run over potentially thousands of captured statements, so it should point at a small, fast,
 * cheap model, not the same model {@link PlsqlSummarizer} uses for deep PL/SQL analysis.
 *
 * <p>Uses the PRIMARY role's configured provider/model (via {@link LlmSettingsStore}) -- the same
 * configuration {@link PlsqlSummarizer} uses. No separate "classify" role: the SLM-vs-LLM split
 * discussed in planning is about picking a small/fast model for PRIMARY when classification
 * volume matters, not about a third configurable role -- point PRIMARY at whatever model fits
 * both jobs, or accept that classification runs at PRIMARY's summarization-grade model if it's a
 * larger one.
 *
 * <p>Batches statements into one prompt (default {@link #BATCH_SIZE}) rather than one call per
 * statement -- purely a cost/latency tradeoff for a first pass; revisit if batch responses turn
 * out unreliable at scale.
 */
public class SqlWorkloadClassifier {

    private static final int BATCH_SIZE = 25;

    private static final String SYSTEM_PROMPT = """
        You classify SQL statements captured from an Oracle database for a Postgres-migration
        assessment tool. For each statement (given as a numbered list), respond with a JSON array
        of objects, one per statement, in the same order, each with:
        - "index": the statement's number
        - "category": one of "simple_crud", "analytical", "hierarchical_query", "dbms_package_call",
          "dynamic_sql", "ddl", "other"
        - "portabilityRisk": "low", "medium", or "high"
        Respond with ONLY the JSON array, no other text.
        """;

    private final LlmSettingsStore settingsStore;
    private final Gson gson = new Gson();

    public SqlWorkloadClassifier(LlmSettingsStore settingsStore) {
        this.settingsStore = settingsStore;
    }

    public record Classification(int index, String category, String portabilityRisk) {}

    public List<Classification> classify(List<CapturedStatement> statements) throws Exception {
        var primary = LlmProviderFactory.resolve(settingsStore.get(LlmRole.PRIMARY));
        List<Classification> results = new ArrayList<>();

        for (int start = 0; start < statements.size(); start += BATCH_SIZE) {
            List<CapturedStatement> batch = statements.subList(start, Math.min(start + BATCH_SIZE, statements.size()));
            String userPrompt = buildBatchPrompt(batch);
            String response = primary.provider().complete(primary.model(), SYSTEM_PROMPT, userPrompt);
            List<Classification> batchResults = parseResponse(response);
            // Re-offset indices to the caller's original list, not the batch-local numbering.
            for (Classification c : batchResults) {
                results.add(new Classification(start + c.index(), c.category(), c.portabilityRisk()));
            }
        }
        return results;
    }

    private String buildBatchPrompt(List<CapturedStatement> batch) {
        StringBuilder prompt = new StringBuilder();
        for (int i = 0; i < batch.size(); i++) {
            prompt.append(i).append(". ").append(batch.get(i).sqlText()).append("\n\n");
        }
        return prompt.toString();
    }

    private List<Classification> parseResponse(String response) {
        String json = response.strip();
        // Models sometimes wrap JSON in a fenced code block despite the "ONLY the JSON array"
        // instruction -- strip it rather than failing the whole batch over formatting.
        if (json.startsWith("```")) {
            json = json.replaceFirst("^```[a-zA-Z]*\\n", "").replaceFirst("```$", "").strip();
        }
        return gson.fromJson(json, new TypeToken<List<Classification>>() {}.getType());
    }

    /** Rollup used by the report layer -- category/risk counts, not per-statement detail. */
    public Map<String, Integer> summarizeByCategory(List<Classification> classifications) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Classification c : classifications) {
            counts.merge(c.category(), 1, Integer::sum);
        }
        return counts;
    }

}
