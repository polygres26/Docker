package com.polygres.wire.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Self-contained OpenAI-compatible chat-completions client used as {@link DialectTranslationStage}'s
 * last-resort fallback when {@link DialectTranslations} has no deterministic rule for a
 * (sourceDialect, targetDialect) construct — mirrors the pattern in
 * {@code com.polygres.advisor.llm.OpenAiCompatibleLlmProvider} (see that file), reimplemented here
 * rather than pulled in as a cross-module dependency on {@code polygres-advisor}, per this project's
 * module-independence rule.
 *
 * <p>Configured entirely via env vars, "local-first, no required API key" — same posture as the rest
 * of this project:
 * <ul>
 *   <li>{@code POLYWIRE_LLM_BASE_URL} — default {@code http://127.0.0.1:8080/v1}, a local
 *   llama-server sidecar's OpenAI-compatible endpoint.</li>
 *   <li>{@code POLYWIRE_LLM_MODEL} — default {@code qwen2.5-1.5b-instruct}, the same small model
 *   {@code com.polygres.advisor.llm.LocalModelPresets} defaults PRIMARY to, for the same reason
 *   (small enough to run locally, no bundled/auto-downloaded weights — operator-provided).</li>
 *   <li>{@code POLYWIRE_LLM_API_KEY} — optional; a local llama-server needs none. Sent as a bearer
 *   token only when set, so a real hosted provider (OpenAI, Azure OpenAI, ...) can be swapped in
 *   just by setting all three env vars.</li>
 * </ul>
 */
public final class TranslationLlmClient {

    private final String apiKey;
    private final String baseUrl;
    private final String model;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final Gson gson = new Gson();

    public TranslationLlmClient() {
        this(System.getenv("POLYWIRE_LLM_API_KEY"),
                System.getenv().getOrDefault("POLYWIRE_LLM_BASE_URL", "http://127.0.0.1:8080/v1"),
                System.getenv().getOrDefault("POLYWIRE_LLM_MODEL", "qwen2.5-1.5b-instruct"));
    }

    public TranslationLlmClient(String apiKey, String baseUrl, String model) {
        this.apiKey = apiKey;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
    }

    /**
     * @return translated SQL text, or {@code null} if the model's response doesn't look like a
     *     single SQL statement (no attempt at "best effort" cleanup — the caller
     *     ({@link DialectTranslationStage}) treats {@code null} the same as any other translation
     *     failure and throws {@link UntranslatableQueryException} rather than passing through
     *     whatever came back).
     */
    public String translate(String sqlText, SourceDialect from, SourceDialect to) throws Exception {
        String systemPrompt = "You are a SQL dialect translator. Translate the user's " + from
                + " SQL statement into equivalent " + to + " SQL. Reply with ONLY the translated SQL "
                + "statement, no explanation, no markdown code fences, no trailing semicolon commentary.";

        JsonObject systemMessage = new JsonObject();
        systemMessage.addProperty("role", "system");
        systemMessage.addProperty("content", systemPrompt);
        JsonObject userMessage = new JsonObject();
        userMessage.addProperty("role", "user");
        userMessage.addProperty("content", sqlText);
        JsonArray messages = new JsonArray();
        messages.add(systemMessage);
        messages.add(userMessage);

        JsonObject body = new JsonObject();
        body.addProperty("model", model);
        body.addProperty("temperature", 0);
        body.add("messages", messages);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/chat/completions"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)));
        if (apiKey != null && !apiKey.isBlank()) {
            requestBuilder.header("Authorization", "Bearer " + apiKey);
        }

        HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("translation LLM error (HTTP " + response.statusCode() + "): " + response.body());
        }

        JsonObject responseBody = gson.fromJson(response.body(), JsonObject.class);
        String content = responseBody.getAsJsonArray("choices")
                .get(0).getAsJsonObject()
                .getAsJsonObject("message")
                .get("content").getAsString();
        return cleanUp(content);
    }

    /** Strips markdown code fences a chat model commonly wraps SQL in; returns {@code null} (not a
     * best-effort guess) if what's left is blank. */
    private static String cleanUp(String content) {
        if (content == null) {
            return null;
        }
        String trimmed = content.strip();
        if (trimmed.startsWith("```")) {
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline >= 0) {
                trimmed = trimmed.substring(firstNewline + 1);
            }
            int closingFence = trimmed.lastIndexOf("```");
            if (closingFence >= 0) {
                trimmed = trimmed.substring(0, closingFence);
            }
            trimmed = trimmed.strip();
        }
        return trimmed.isEmpty() ? null : trimmed;
    }
}
