package com.sayonora.dms.llm;

/**
 * Optional second-opinion pass over a PRIMARY LLM's output -- scoped to {@link PlsqlSummarizer}
 * today, per the project decision to make Judge review narrow and opt-in rather than a blanket
 * "ask a second model to double-check everything." The summarizer's output feeds a human decision
 * about migration effort, where a wrong "this is trivial" call is expensive to discover late;
 * {@link SqlWorkloadClassifier}'s high-volume, low-stakes-per-item classification doesn't get the
 * same treatment (judging hundreds of statements would double cost for marginal benefit).
 *
 * <p>Deliberately does NOT reuse PRIMARY's model/provider for the check -- {@link LlmSettings} for
 * {@link LlmRole#JUDGE} is configured independently (can be a different provider entirely), since
 * a genuinely different model or prompt catches more real errors than a model reviewing its own
 * output does.
 */
public class LlmJudge {

    private static final String SYSTEM_PROMPT = """
        You are reviewing another AI model's analysis of a PL/SQL object for a Postgres-migration
        assessment tool. You will be given the original PL/SQL source and the other model's summary/
        effort-estimate. Check specifically for:
        1. Any construct in the source the summary failed to mention (DBMS_* calls, autonomous
           transactions, CONNECT BY, dynamic SQL, collections, package-level state).
        2. Whether the stated effort estimate (trivial/moderate/substantial) is actually consistent
           with what's in the source -- flag if it looks too optimistic or too pessimistic.
        Respond with exactly one line starting with "VERDICT: APPROVED" or "VERDICT: FLAGGED",
        followed by a short explanation (1-3 sentences) only if flagged.
        """;

    private final LlmProvider judgeProvider;
    private final String judgeModel;

    public LlmJudge(LlmProvider judgeProvider, String judgeModel) {
        this.judgeProvider = judgeProvider;
        this.judgeModel = judgeModel;
    }

    public record Verdict(boolean approved, String explanation) {}

    public Verdict review(String originalSource, String primarySummary) throws Exception {
        String userPrompt = "=== Original PL/SQL source ===\n" + originalSource
            + "\n\n=== Other model's summary ===\n" + primarySummary;
        String response = judgeProvider.complete(judgeModel, SYSTEM_PROMPT, userPrompt);
        boolean approved = response.strip().toUpperCase().startsWith("VERDICT: APPROVED");
        String explanation = response.strip();
        return new Verdict(approved, explanation);
    }
}
