package com.sayonora.wire.core;

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
        this(System.getenv("WARP_LLM_API_KEY"),
                System.getenv().getOrDefault("WARP_LLM_BASE_URL", "http://127.0.0.1:8080/v1"),
                System.getenv().getOrDefault("WARP_LLM_MODEL", "qwen2.5-1.5b-instruct"));
    }

    public TranslationLlmClient(String apiKey, String baseUrl, String model) {
        this.apiKey = apiKey;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
    }

    /**
     * Builds a client from {@code warp_config}'s dynamic LLM settings (see
     * {@link com.sayonora.wire.config.WarpConfig#llmProvider()} et al.), falling back to the
     * {@code WARP_LLM_*} env vars for any field the config store hasn't been given a value
     * for yet -- so a bare env-var deployment (no admin UI, no {@code warp_config} row for
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
        String resolvedApiKey = firstNonBlank(apiKey, System.getenv("WARP_LLM_API_KEY"));
        String resolvedBaseUrl = firstNonBlank(baseUrl, System.getenv("WARP_LLM_BASE_URL"));
        String resolvedModel = firstNonBlank(model, System.getenv("WARP_LLM_MODEL"));
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
     * between two known dialects -- see {@link com.sayonora.wire.core.QueryRepairStage}'s own
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
     * Drafts a read-only Postgres SELECT from a plain-English question and a schema summary --
     * the first of the two LLM calls {@code query_natural_language} (see {@code
     * WarpMcpServer#runNaturalLanguageQuery}) makes; the second, independent one is {@link
     * #judgeSql}. Deliberately two separate calls rather than one -- the same reason a code
     * review is a different pass than writing the code: a model judging a draft against the
     * original question afresh catches mistakes a single pass asked to "get it right the first
     * time" tends not to notice in its own output.
     */
    public String draftSqlFromNaturalLanguage(String schemaAndQuestion) throws Exception {
        String systemPrompt = "You are a natural-language-to-SQL assistant for a Postgres database. Given "
                + "the schema and question below, draft ONE Postgres SELECT statement that answers the "
                + "question. Read-only ONLY -- never INSERT/UPDATE/DELETE/DDL, even if the question seems "
                + "to ask for a change; if you can't answer with a SELECT alone, draft the closest read "
                + "that's still useful. Reply with ONLY the SQL statement, no explanation, no markdown code "
                + "fences.";
        return chatComplete(systemPrompt, schemaAndQuestion, "nl2sql-draft");
    }

    /**
     * Judges (and, if needed, corrects) a drafted SELECT against the schema and the original
     * question -- see {@link #draftSqlFromNaturalLanguage}'s own javadoc for why this is a
     * separate call rather than folded into drafting. This method only phrases/decides what the
     * caller asked it to decide for THIS one draft; {@code WarpMcpServer} still runs its own
     * deterministic read-only check on whatever SQL comes back before ever executing it -- this
     * judge is a quality/safety improvement layered on top of that check, not a replacement for
     * it.
     */
    public String judgeSql(String context) throws Exception {
        String systemPrompt = "You are a SQL judge reviewing a natural-language-to-SQL draft before it "
                + "runs. Given the schema, the user's original question, and the drafted SQL below, check "
                + "whether the SQL actually answers the question, references only tables/columns that "
                + "exist in the schema, and is a genuine read (SELECT only, nothing else). Reply with a "
                + "single JSON object with EXACTLY these fields and no others:\n"
                + "{\n"
                + "  \"corrected\": true or false -- whether you changed the drafted SQL,\n"
                + "  \"sql\": the SQL to actually run -- the draft unchanged if it's already correct, or "
                + "your fixed version if not,\n"
                + "  \"reasoning\": one short plain-English sentence explaining your verdict\n"
                + "}\n"
                + "Reply with ONLY that JSON object -- no explanation, no markdown code fences.";
        return chatComplete(systemPrompt, context, "nl2sql-judge");
    }

    /**
     * Asks the LLM to propose ONE new {@code RollupStage} pre-aggregation definition, based on
     * the existing definitions and recent expensive/frequent SQL given in {@code context}. The
     * caller (MetricsServer's {@code /api/rollup-suggestions/draft} handler) validates the result
     * by literally running it through {@code RollupConfig.parse} -- the real parser the runtime
     * itself uses, not a second copy of its grammar -- and merges it into the rest of the current
     * definitions unchanged before ever showing it to an admin, the same "small, reviewable, LLM
     * proposes / human applies through the endpoint that already existed" shape every drafting
     * feature in this series uses.
     */
    public String draftRollupSuggestion(String context) throws Exception {
        String systemPrompt = "You are a database performance assistant. Given the existing rollup "
                + "(pre-aggregation) definitions and recent expensive/frequent SQL below, look for a "
                + "GROUP BY query shape that runs often enough or costs enough to be worth pre-aggregating, "
                + "and propose ONE new rollup definition for it. Reply with a single JSON object with "
                + "EXACTLY these fields and no others:\n"
                + "{\n"
                + "  \"name\": a plain identifier (letters/digits/underscore, not starting with a digit), "
                + "not already used by an existing definition,\n"
                + "  \"backend\": the backend name to run it against (use \"default\" unless the context "
                + "names a specific one),\n"
                + "  \"sourceTable\": the table the rollup aggregates,\n"
                + "  \"groupBy\": a list of the GROUP BY expressions/columns,\n"
                + "  \"aggregations\": a list of aggregation expressions, each EXACTLY in the form "
                + "\"SUM|COUNT|AVG|MIN|MAX(expr) AS alias\",\n"
                + "  \"refreshIntervalMinutes\": positive integer,\n"
                + "  \"maxStalenessMinutes\": positive integer, at least refreshIntervalMinutes,\n"
                + "  \"rationale\": a short plain-English sentence explaining why this rollup is worth it\n"
                + "}\n"
                + "If nothing in the given SQL is actually worth pre-aggregating, reply with exactly "
                + "{\"name\": null} instead. Reply with ONLY that JSON object -- no explanation, no markdown "
                + "code fences.";
        return chatComplete(systemPrompt, context, "rollup-suggestion-draft");
    }

    /**
     * Asks the LLM to propose ONE new per-table hash-sharding rule ({@code
     * RouterStage.TableShardRule}), based on recent per-backend load and the list of currently
     * configured backends given in {@code context}. Deliberately restricted to the {@code hash}
     * sharding strategy only, the simplest and most common case -- the caller (MetricsServer's
     * {@code /api/router-suggestions/draft} handler) validates the proposed backends are real,
     * currently-configured names (an LLM cannot know a backend exists that isn't in the context
     * it was given, but it CAN hallucinate a plausible-sounding one anyway) before ever showing
     * the draft to an admin.
     */
    public String draftTableShardSuggestion(String context) throws Exception {
        String systemPrompt = "You are a database sharding assistant. Given the list of currently "
                + "configured backends and recent per-backend load below, look for a table that would "
                + "benefit from being horizontally sharded (hashed) across multiple backends -- for "
                + "example because one backend is carrying disproportionate load. Propose ONE such "
                + "table, restricted to hash sharding only. Reply with a single JSON object with EXACTLY "
                + "these fields and no others:\n"
                + "{\n"
                + "  \"table\": the table name to shard,\n"
                + "  \"shardColumn\": the column to hash on (e.g. a tenant or customer id),\n"
                + "  \"backends\": a list of 2 or more backend names, from EXACTLY the configured list "
                + "given below, to spread this table's rows across,\n"
                + "  \"rationale\": a short plain-English sentence explaining why\n"
                + "}\n"
                + "If nothing in the given load data actually justifies sharding a table right now, reply "
                + "with exactly {\"table\": null} instead. Reply with ONLY that JSON object -- no "
                + "explanation, no markdown code fences.";
        return chatComplete(systemPrompt, context, "router-suggestion-draft");
    }

    /**
     * Turns a real Postgres {@code EXPLAIN} plan into a short plain-English explanation -- the
     * safest category of LLM-backed feature in this series: pure narration of a fact Postgres
     * itself already computed, no decision of any kind, nothing to validate or gate afterward
     * (see {@code WarpMcpServer}'s {@code explain_query} tool, which always returns the real
     * plan JSON alongside this narrative, never the narrative alone).
     */
    public String narrateExplainPlan(String sql, String planJson) throws Exception {
        String systemPrompt = "You are a Postgres query performance assistant. Below is the SQL that was "
                + "run and its real EXPLAIN plan (JSON format). Explain in 2-4 short plain-English "
                + "sentences what the plan actually does and call out anything a non-expert should know -- "
                + "a sequential scan on a large table, a missing index, an expensive sort or nested loop, "
                + "etc. Only describe what's actually in the plan; don't speculate about the schema beyond "
                + "it. Reply with ONLY the explanation, no markdown, no restating the raw JSON.";
        return chatComplete(systemPrompt, "SQL:\n" + sql + "\n\nEXPLAIN plan (JSON):\n" + planJson, "explain-narration");
    }

    /**
     * Turns a real table/column/foreign-key listing into a short plain-English data dictionary --
     * pure narration of the actual schema, like {@link #narrateExplainPlan}: the caller ({@code
     * WarpMcpServer}'s {@code document_schema} tool) always returns the real structured
     * listing alongside this, and this method is told explicitly not to invent business meaning
     * beyond what the names/types/foreign keys actually show.
     */
    public String documentSchema(String schemaContext) throws Exception {
        String systemPrompt = "You are a database documentation assistant. Given the table/column list "
                + "and foreign-key relationships below, write a short plain-English data dictionary -- "
                + "1-2 sentences per table describing what it most likely represents and how it relates "
                + "to other tables, based ONLY on what's actually in the schema (names, types, foreign "
                + "keys). Don't invent business meaning you can't reasonably infer from that. Reply with "
                + "ONLY the documentation, plain paragraphs, no markdown headers or code fences.";
        return chatComplete(systemPrompt, schemaContext, "schema-documentation");
    }

    /**
     * Asks the LLM to turn a list of real {@code MCP_TOOL_CALLED} audit events into a short
     * plain-English narrative of what an MCP client actually did against the database -- for an
     * admin who wants to know "what did this agent do in its last session" without reading raw
     * audit JSON line by line. {@code context} is caller-built (see {@code MetricsServer}'s
     * {@code /api/mcp-audit/summarize} handler) from real {@link
     * com.sayonora.wire.audit.AuditEvent} rows, not invented -- this method only phrases what's
     * already there, the same "narrate, don't decide" split every other LLM-backed feature here
     * uses ({@link #summarizeAnomaly}, {@link QueryRepairStage}, etc.).
     */
    public String summarizeMcpActivity(String context) throws Exception {
        String systemPrompt = "You are a database audit assistant. Below is a chronological list of real "
                + "tool calls an MCP (Model Context Protocol) client made against a database, most recent "
                + "first. Summarize what the client actually did in 2-4 short plain-English sentences -- "
                + "call out any failed calls and anything that looks like a write (INSERT/UPDATE/DELETE/DDL), "
                + "since those matter most to an admin reviewing this after the fact. Do not invent actions "
                + "that aren't in the list. Reply with ONLY the summary, no markdown, no restating the raw list.";
        return chatComplete(systemPrompt, context, "mcp-activity-summary");
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
