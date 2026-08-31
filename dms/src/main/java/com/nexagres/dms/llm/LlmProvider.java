package com.nexagres.dms.llm;

/**
 * One call = one model completion. Deliberately model-agnostic at this interface level -- {@link
 * PlsqlSummarizer} (deep reasoning: PL/SQL intent summarization, rewrite proposals) and {@link
 * SqlWorkloadClassifier} (high-volume, cheap classification of captured SQL) each pick their own
 * model via the {@code model} parameter, per the project's SLM-for-classification /
 * Claude-for-deep-analysis split discussed in planning -- this interface doesn't hardcode either.
 */
public interface LlmProvider {
    /**
     * @param model the model id to use for this call (caller decides -- see class javadoc)
     * @param systemPrompt system-level instructions
     * @param userPrompt the actual content to analyze
     * @return the model's text response
     */
    String complete(String model, String systemPrompt, String userPrompt) throws Exception;
}
