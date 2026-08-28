package com.nexagres.wire.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

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
     * Builds a client from {@code polywire_config}'s dynamic LLM settings (see
     * {@link com.nexagres.wire.config.PolyWireConfig#llmProvider()} et al.), falling back to the
     * {@code POLYWIRE_LLM_*} env vars for any field the config store hasn't been given a value
     * for yet -- so a bare env-var deployment (no admin UI, no {@code polywire_config} row for
     * these fields) keeps working exactly as before, while any field actually set through
     * {@code /api/llm-config} takes precedence over its env-var equivalent.
     *
     * <p>Returns {@code null} when {@code provider} is {@code "none"} -- see
     * {@link DialectTranslationStage#translateWithFallback} for why {@code null} here means "skip
     * the LLM fallback entirely" rather than "fall back to env vars silently".
     */
    public static TranslationLlmClient fromConfig(String provider, String apiKey, String baseUrl, String model) {
        if ("none".equalsIgnoreCase(provider)) {
            return null;
        }
        String resolvedApiKey = firstNonBlank(apiKey, System.getenv("POLYWIRE_LLM_API_KEY"));
        String resolvedBaseUrl = firstNonBlank(baseUrl, System.getenv("POLYWIRE_LLM_BASE_URL"));
        String resolvedModel = firstNonBlank(model, System.getenv("POLYWIRE_LLM_MODEL"));
        if ("openai".equalsIgnoreCase(provider) && (resolvedBaseUrl == null || resolvedBaseUrl.isBlank())) {
            resolvedBaseUrl = "https://api.openai.com/v1";
        }
        if (resolvedBaseUrl == null || resolvedBaseUrl.isBlank()) {
            resolvedBaseUrl = "http://127.0.0.1:8080/v1";
        }
        if (resolvedModel == null || resolvedModel.isBlank()) {
            resolvedModel = "qwen2.5-1.5b-instruct";
        }
        return new TranslationLlmClient(resolvedApiKey, resolvedBaseUrl, resolvedModel);
    }

    private static String firstNonBlank(String primary, String fallback) {
        return (primary != null && !primary.isBlank()) ? primary : fallback;
    }

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
