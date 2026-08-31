package com.nexagres.dms.llm;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Generic external-LLM provider -- the chat-completions request/response shape OpenAI itself,
 * Azure OpenAI, and most self-hosted servers (Ollama, vLLM, LM Studio, ...) all speak, which is
 * what "support external LLM" means in practice: one client covers the large majority of
 * third-party options rather than writing one integration per vendor.
 *
 * <p>Constructed with the caller's own API key and base URL (from {@link LlmSettings}, not an env
 * var -- unlike {@link ClaudeLlmProvider}'s built-in path, external credentials are per-deployment
 * user configuration, entered through the LLM settings page and stored via {@link
 * LlmSettingsStore}).
 */
public class OpenAiCompatibleLlmProvider implements LlmProvider {

    private final String apiKey;
    private final String baseUrl;
    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
    private final Gson gson = new Gson();

    public OpenAiCompatibleLlmProvider(String apiKey, String baseUrl) {
        this.apiKey = apiKey;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    @Override
    public String complete(String model, String systemPrompt, String userPrompt) throws Exception {
        JsonObject systemMessage = new JsonObject();
        systemMessage.addProperty("role", "system");
        systemMessage.addProperty("content", systemPrompt);
        JsonObject userMessage = new JsonObject();
        userMessage.addProperty("role", "user");
        userMessage.addProperty("content", userPrompt);
        JsonArray messages = new JsonArray();
        messages.add(systemMessage);
        messages.add(userMessage);

        JsonObject body = new JsonObject();
        body.addProperty("model", model);
        body.add("messages", messages);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/chat/completions"))
            .header("Authorization", "Bearer " + apiKey)
            .header("content-type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("External LLM API error (HTTP " + response.statusCode() + "): " + response.body());
        }

        JsonObject responseBody = gson.fromJson(response.body(), JsonObject.class);
        return responseBody.getAsJsonArray("choices")
            .get(0).getAsJsonObject()
            .getAsJsonObject("message")
            .get("content").getAsString();
    }
}
