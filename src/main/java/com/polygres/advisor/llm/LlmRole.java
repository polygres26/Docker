package com.polygres.advisor.llm;

/**
 * PRIMARY does the actual work (summarization, classification). JUDGE is an optional second pass
 * that reviews PRIMARY's output for correctness -- deliberately a separate, independently
 * configurable role (can point at a different provider/model than PRIMARY) rather than a fixed
 * "ask the same model to double-check itself," since a genuinely different model or prompt catches
 * more real errors than self-review does. See {@link LlmJudge}'s javadoc for where it's actually
 * wired in and why it's scoped to the summarizer, not every LLM call.
 */
public enum LlmRole {
    PRIMARY, JUDGE
}
