package com.sayonora.dms.llm;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Real Anthropic Messages API client -- plain {@link HttpClient} + Gson rather than pulling in
 * the full Anthropic Java SDK, to keep this dependency-light while the integration shape is still
 * being proven out. Swap for the official SDK later if/when this needs streaming, tool use, or
 * other features the raw REST call doesn't bother with.
 *
 * <p>Requires {@code ANTHROPIC_API_KEY} in the environment. No default model id is baked in on
 * purpose -- callers ({@link PlsqlSummarizer}, {@link SqlWorkloadClassifier}) require their model
 * env vars to be set explicitly rather than this class guessing at a model string that might not
 * exist by the time this code runs; see each caller's javadoc for which env var it reads.
 */
public class ClaudeLlmProvider implements LlmProvider {

    private static final String API_URL = "https://api.anthropic.com/v1/messages";
    private static final String API_VERSION = "2023-06-01";

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
    private final Gson gson = new Gson();

    @Override
    public String complete(String model, String systemPrompt, String userPrompt) throws Exception {
        String apiKey = System.getenv("ANTHROPIC_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("ANTHROPIC_API_KEY is not set -- required for LLM-backed features.");
        }

        JsonObject message = new JsonObject();
        message.addProperty("role", "user");
        message.addProperty("content", userPrompt);
        JsonArray messages = new JsonArray();
        messages.add(message);

        JsonObject body = new JsonObject();
        body.addProperty("model", model);
        body.addProperty("max_tokens", 4096);
        body.addProperty("system", systemPrompt);
        body.add("messages", messages);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(API_URL))
            .header("x-api-key", apiKey)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Anthropic API error (HTTP " + response.statusCode() + "): " + response.body());
        }

        JsonObject responseBody = gson.fromJson(response.body(), JsonObject.class);
        JsonArray content = responseBody.getAsJsonArray("content");
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < content.size(); i++) {
            JsonObject block = content.get(i).getAsJsonObject();
            if ("text".equals(block.get("type").getAsString())) {
                text.append(block.get("text").getAsString());
            }
        }
        return text.toString();
    }
}
