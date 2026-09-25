package com.sayonora.dms.llm;

/**
 * One role's (PRIMARY or JUDGE) LLM configuration -- persisted by {@link LlmSettingsStore}.
 *
 * <p>Same plaintext-at-rest caveat as {@link com.sayonora.dms.core.ConnectionRecord}: {@code
 * apiKey} is stored as-is in the embedded HSQLDB store, not encrypted. {@link #redacted()} is what
 * keeps it from round-tripping back to the browser, same pattern as connections.
 *
 * <p>PRIMARY defaults to {@link LlmProviderType#LOCAL} on a fresh install -- no API key, no
 * outbound network call, works wherever a llama-server binary + model file are available locally,
 * same self-hosted-first posture Omnigate takes with its own local Qwen/Gemma sidecars.
 */
public class LlmSettings {
    public LlmRole role;
    public LlmProviderType providerType = LlmProviderType.LOCAL;
    public String apiKey;
    public String baseUrl;    // EXTERNAL only; e.g. https://api.openai.com/v1
    public String modelPath;  // LOCAL only; path to a .gguf model file
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
        copy.modelPath = modelPath;
        copy.model = model;
        copy.enabled = enabled;
        copy.updatedAt = updatedAt;
        return copy;
    }

    /**
     * Whether this role has enough configured to actually make a call.
     * LOCAL needs a model file path (the llama-server binary itself can come from PATH/
     * SAYONORA_LLM_LOCAL_SERVER_PATH, so it isn't required here -- see LocalLlamaManager).
     * BUILTIN needs a model id set explicitly (no default baked in project-wide -- see
     * ClaudeLlmProvider's javadoc) even though it reuses the server's ANTHROPIC_API_KEY.
     * EXTERNAL additionally needs its own key, base URL, and model id.
     */
    public boolean isUsable() {
        if (!enabled) return false;
        if (providerType == LlmProviderType.LOCAL) return modelPath != null && !modelPath.isBlank();
        if (model == null || model.isBlank()) return false;
        if (providerType == LlmProviderType.BUILTIN) return true;
        return apiKey != null && !apiKey.isBlank() && baseUrl != null && !baseUrl.isBlank();
    }
}
