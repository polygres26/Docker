package com.nexagres.advisor.llm;

/**
 * Turns a stored {@link LlmSettings} row into an actual callable {@link LlmProvider} + the model
 * id to pass on each call. The one place BUILTIN vs. LOCAL vs. EXTERNAL gets resolved into a
 * concrete client -- callers (PlsqlSummarizer, SqlWorkloadClassifier, LlmJudge) never construct a
 * provider themselves.
 */
public final class LlmProviderFactory {

    private LlmProviderFactory() {}

    public record Resolved(LlmProvider provider, String model) {}

    public static Resolved resolve(LlmSettings settings) {
        if (!settings.isUsable()) {
            throw new IllegalStateException((settings.role == null ? "This role" : settings.role.name())
                + " LLM is not configured -- set it up on the LLM configuration page.");
        }
        return switch (settings.providerType) {
            case BUILTIN -> new Resolved(new ClaudeLlmProvider(), settings.model);
            case EXTERNAL -> new Resolved(new OpenAiCompatibleLlmProvider(settings.apiKey, settings.baseUrl), settings.model);
            case LOCAL -> resolveLocal(settings);
        };
    }

    private static Resolved resolveLocal(LlmSettings settings) {
        String serverPath = System.getenv().getOrDefault("NEXAGRES_LLM_LOCAL_SERVER_PATH", "llama-server");
        try {
            int port = LocalLlamaManager.ensureRunning(serverPath, settings.modelPath);
            // llama-server's single-model chat-completions endpoint doesn't care what "model" string
            // is sent -- it always serves the one model it was started with -- but the chat-completions
            // JSON payload requires the field present, so fall back to a placeholder if none was set.
            String model = (settings.model == null || settings.model.isBlank()) ? "local-model" : settings.model;
            return new Resolved(new OpenAiCompatibleLlmProvider("not-needed", "http://127.0.0.1:" + port + "/v1"), model);
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local llama-server: " + e.getMessage(), e);
        }
    }
}
