package com.polygres.advisor.llm;

/**
 * Turns a stored {@link LlmSettings} row into an actual callable {@link LlmProvider} + the model
 * id to pass on each call. The one place BUILTIN vs. EXTERNAL gets resolved into a concrete client
 * -- callers (PlsqlSummarizer, SqlWorkloadClassifier, LlmJudge) never construct a provider
 * themselves.
 */
public final class LlmProviderFactory {

    private LlmProviderFactory() {}

    public record Resolved(LlmProvider provider, String model) {}

    public static Resolved resolve(LlmSettings settings) {
        if (!settings.isUsable()) {
            throw new IllegalStateException((settings.role == null ? "This role" : settings.role.name())
                + " LLM is not configured -- set it up on the LLM configuration page.");
        }
        LlmProvider provider = settings.providerType == LlmProviderType.BUILTIN
            ? new ClaudeLlmProvider()
            : new OpenAiCompatibleLlmProvider(settings.apiKey, settings.baseUrl);
        return new Resolved(provider, settings.model);
    }
}
