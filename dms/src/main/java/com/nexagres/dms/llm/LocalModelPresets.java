package com.nexagres.dms.llm;

/**
 * The two local models Advisor expects an operator to have on disk -- Qwen 2.5 1.5B Instruct and
 * Gemma 2 2B IT, same pair Omnigate uses for its own local sidecars (see {@link
 * LocalLlamaManager}'s javadoc). Customers pick between them per-role (Primary/Judge) from a
 * simple two-way switch on the LLM configuration page instead of typing a raw file path -- this is
 * just where those two paths come from.
 *
 * <p>Paths are still operator-provided, not bundled or auto-downloaded (same posture as before):
 * {@code NEXAGRES_LLM_QWEN_MODEL_PATH}/{@code NEXAGRES_LLM_GEMMA_MODEL_PATH} override the default,
 * which assumes the same {@code ~/.cache/omnigate-models/} convention this project's own dev/test
 * setup uses. A customer install with the files somewhere else just sets the env vars.
 */
public final class LocalModelPresets {

    public enum Model { QWEN, GEMMA }

    public record Preset(String label, String modelPath) {}

    private LocalModelPresets() {}

    public static Preset qwen() {
        String home = System.getProperty("user.home");
        String path = System.getenv().getOrDefault("NEXAGRES_LLM_QWEN_MODEL_PATH",
            home + "/.cache/omnigate-models/qwen2.5-1.5b-instruct-q4_k_m.gguf");
        return new Preset("Qwen 2.5 1.5B Instruct", path);
    }

    public static Preset gemma() {
        String home = System.getProperty("user.home");
        String path = System.getenv().getOrDefault("NEXAGRES_LLM_GEMMA_MODEL_PATH",
            home + "/.cache/omnigate-models/gemma-2-2b-it-q4_k_m.gguf");
        return new Preset("Gemma 2 2B IT", path);
    }

    /** Which preset a stored {@code modelPath} matches, if either -- lets the UI highlight the right switch position for an already-saved setting. */
    public static Model matchByPath(String modelPath) {
        if (modelPath != null && modelPath.equals(gemma().modelPath())) return Model.GEMMA;
        return Model.QWEN; // default assumption when unset/unrecognized -- Qwen is PRIMARY's out-of-the-box default (see LlmSettings)
    }
}
