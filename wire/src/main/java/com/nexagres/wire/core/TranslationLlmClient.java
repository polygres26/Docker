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
        return chatComplete(systemPrompt, sqlText, "translation");
    }

    /**
     * Asks the LLM to repair a statement a real backend genuinely rejected, rather than translate
     * between two known dialects -- see {@link com.nexagres.wire.core.QueryRepairStage}'s own
     * javadoc for why these are different asks. {@code dialect} is passed only as context (what
     * SQL flavor the client speaks), not a translation target: the output is always meant to run
     * against the SAME backend that just rejected the input, so unlike {@link #translate}, there
     * is no separate "to" dialect here.
     */
    public String repair(String sqlText, SourceDialect dialect, String backendErrorMessage) throws Exception {
        String systemPrompt = "You are a SQL repair assistant. The user's " + dialect + " SQL statement "
                + "below was rejected by the database with the error shown. Return a corrected SQL "
                + "statement that preserves the original intent and is valid to execute as-is against "
                + "the same database. Reply with ONLY the corrected SQL statement, no explanation, no "
                + "markdown code fences.\n\nDatabase error: " + backendErrorMessage;
        return chatComplete(systemPrompt, sqlText, "repair");
    }

    /**
     * Asks the LLM to turn a plain-English security policy into a single, strictly-shaped JSON
     * firewall-rule draft -- the caller ({@code MetricsServer}'s {@code /api/firewall-rules/draft}
     * handler) is responsible for parsing and validating the result before ever showing it to an
     * admin; this method only makes the request and returns whatever text came back (fence-
     * stripped, same as {@link #translate}/{@link #repair}). Deliberately returns raw text rather
     * than a parsed object -- a hallucinating LLM can return malformed JSON, and the caller needs
     * to be able to surface that as a clear "the LLM didn't return valid JSON" error rather than
     * this method throwing an opaque parse exception from inside a generically-named HTTP client.
     */
    public String draftFirewallRule(String prompt) throws Exception {
        String systemPrompt = "You are a database firewall rule assistant. Convert the user's plain-English "
                + "security policy into a single JSON object with EXACTLY these fields and no others:\n"
                + "{\n"
                + "  \"action\": \"allow\" or \"deny\",\n"
                + "  \"priority\": integer, lower runs first -- use 100 unless the request implies otherwise,\n"
                + "  \"statementType\": the single leading SQL keyword this rule applies to (e.g. \"SELECT\", "
                + "\"INSERT\", \"UPDATE\", \"DELETE\"), or null to match every statement type,\n"
                + "  \"tablePattern\": a glob (\"*\" wildcard only, e.g. \"orders\" or \"dynamo_item_*\") "
                + "matching the table name(s) this rule applies to, or null to match every table,\n"
                + "  \"sqlPattern\": a Java regular expression the raw SQL text must contain to match, or "
                + "null,\n"
                + "  \"enabled\": true,\n"
                + "  \"description\": a short human-readable summary of what this rule does\n"
                + "}\n"
                + "Reply with ONLY that JSON object -- no explanation, no markdown code fences.";
        return chatComplete(systemPrompt, prompt, "firewall-rule-draft");
    }

    /**
     * Asks the LLM for ONE short plain-English sentence explaining a traffic anomaly {@link
     * AnomalyDetectionScheduler} already detected deterministically -- this method is never asked
     * to decide whether something is anomalous, only to phrase a numeric fact a human would
     * otherwise have to interpret unassisted. {@code topSql} is passed as context only (what was
     * actually running around the time of the spike), capped to a handful of entries so the
     * prompt stays small.
     */
    public String summarizeAnomaly(String protocol, double baselinePerSec, double currentPerSec, double ratio,
            java.util.List<SqlMetricsCollector.SqlStat> topSql) throws Exception {
        StringBuilder context = new StringBuilder();
        context.append("Protocol: ").append(protocol).append('\n');
        context.append("Baseline rate: ").append(String.format(java.util.Locale.ROOT, "%.2f", baselinePerSec)).append("/s\n");
        context.append("Current rate: ").append(String.format(java.util.Locale.ROOT, "%.2f", currentPerSec)).append("/s\n");
        context.append("Ratio: ").append(String.format(java.util.Locale.ROOT, "%.1f", ratio)).append("x\n");
        context.append("Top SQL by total cost right now (normalized, may be unrelated to the spike):\n");
        int shown = 0;
        for (SqlMetricsCollector.SqlStat s : topSql) {
            if (shown++ >= 5) {
                break;
            }
            context.append("- ").append(s.normalizedSql()).append(" (").append(s.calls()).append(" calls)\n");
        }
        String systemPrompt = "You are a database traffic monitoring assistant. A monitoring system has "
                + "already deterministically detected a traffic-rate anomaly with the numbers below -- your "
                + "only job is to explain it in ONE short, plain-English sentence a non-expert on-call "
                + "engineer would understand at a glance. Do not restate all the numbers verbatim; interpret "
                + "them. Do not speculate about a root cause you can't actually see in the data provided. "
                + "Reply with ONLY that one sentence, no explanation, no markdown.";
        return chatComplete(systemPrompt, context.toString(), "anomaly-summary");
    }

    /**
     * Asks the LLM to propose ONE targeted QoS rate-limit change (the "default" limit or a single
     * workload class), based only on the current config and recent backend load given in {@code
     * context}. Deliberately scoped to one class per call, the same "small, reviewable blast
     * radius" reasoning {@link #draftFirewallRule} uses -- the caller (MetricsServer's
     * {@code /api/qos-suggestions/draft} handler) merges the single proposed change into the
     * REST of the current {@code qosClassLimits} config unchanged and returns that as the draft,
     * never applying anything itself.
     */
    public String draftQosTuning(String context) throws Exception {
        String systemPrompt = "You are a database QoS tuning assistant. Given the current QoS "
                + "configuration and recent backend load below, propose ONE targeted rate-limit change "
                + "-- either the \"default\" limit or a single existing (or, if clearly warranted, new) "
                + "workload class -- that would plausibly improve throughput or reduce contention, based "
                + "ONLY on the data given. Reply with a single JSON object with EXACTLY these fields and "
                + "no others:\n"
                + "{\n"
                + "  \"target\": \"default\" or a workload class name,\n"
                + "  \"ratePerSecond\": number,\n"
                + "  \"burstCapacity\": number,\n"
                + "  \"maxWaitMillis\": integer, 0 or greater,\n"
                + "  \"rationale\": a short plain-English sentence explaining why\n"
                + "}\n"
                + "Reply with ONLY that JSON object -- no explanation, no markdown code fences.";
        return chatComplete(systemPrompt, context, "qos-tuning-draft");
    }

    private String chatComplete(String systemPrompt, String userContent, String purpose) throws Exception {
        JsonObject systemMessage = new JsonObject();
        systemMessage.addProperty("role", "system");
        systemMessage.addProperty("content", systemPrompt);
        JsonObject userMessage = new JsonObject();
        userMessage.addProperty("role", "user");
        userMessage.addProperty("content", userContent);
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
            throw new RuntimeException(purpose + " LLM error (HTTP " + response.statusCode() + "): " + response.body());
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
