package com.polygres.advisor.llm;

/**
 * One role's (PRIMARY or JUDGE) LLM configuration -- persisted by {@link LlmSettingsStore}.
 *
 * <p>Same plaintext-at-rest caveat as {@link com.polygres.advisor.core.ConnectionRecord}: {@code
 * apiKey} is stored as-is in the embedded HSQLDB store, not encrypted. {@link #redacted()} is what
 * keeps it from round-tripping back to the browser, same pattern as connections.
 */
public class LlmSettings {
    public LlmRole role;
    public LlmProviderType providerType = LlmProviderType.BUILTIN;
    public String apiKey;
    public String baseUrl;   // EXTERNAL only; e.g. https://api.openai.com/v1
    public String model;
    public boolean enabled = true; // JUDGE defaults to disabled at the store level when first created; see LlmSettingsStore
    public String updatedAt;

    public LlmSettings() {}

    public LlmSettings(LlmRole role) {
        this.role = role;
        this.enabled = role == LlmRole.PRIMARY; // PRIMARY on by default, JUDGE opt-in
    }

    public LlmSettings redacted() {
        LlmSettings copy = new LlmSettings();
        copy.role = role;
        copy.providerType = providerType;
        copy.apiKey = null;
        copy.baseUrl = baseUrl;
        copy.model = model;
        copy.enabled = enabled;
        copy.updatedAt = updatedAt;
        return copy;
    }

    /** Whether this role has enough configured to actually make a call. BUILTIN still needs a model id set explicitly (no default baked in project-wide -- see ClaudeLlmProvider's javadoc) even though it reuses the server's ANTHROPIC_API_KEY; EXTERNAL additionally needs its own key and base URL. */
    public boolean isUsable() {
        if (!enabled || model == null || model.isBlank()) return false;
        if (providerType == LlmProviderType.BUILTIN) return true;
        return apiKey != null && !apiKey.isBlank() && baseUrl != null && !baseUrl.isBlank();
    }
}
