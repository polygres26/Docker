package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.llm.LlmSettingsStore;
import com.polygres.advisor.llm.PlsqlSummarizer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code POST /api/summarize} -- PL/SQL intent summary + portability-risk flags for one named
 * package/procedure/function, via {@link PlsqlSummarizer}, using whatever PRIMARY (and, if
 * configured, JUDGE) LLM is set up on the LLM configuration page. Unlike {@link WorkloadRoute}'s
 * classification, this is not best-effort -- an unconfigured PRIMARY fails the request outright,
 * since summarization is this route's whole purpose rather than an enrichment on top of something
 * else useful.
 */
public class SummarizeRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final LlmSettingsStore settingsStore = new LlmSettingsStore();

    public static class SummarizeRequest extends ScanRequest {
        public String objectName;
        public String objectType; // PACKAGE | PACKAGE BODY | PROCEDURE | FUNCTION
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(405);
            return;
        }

        SummarizeRequest req = GSON.fromJson(request.getReader(), SummarizeRequest.class);
        if (req == null || req.jdbcUrl == null || req.objectName == null || req.objectType == null) {
            writeError(response, 400, "jdbcUrl, objectName, and objectType are required.");
            return;
        }

        BackendTarget target = new BackendTarget(
            req.name != null ? req.name : "summarize-" + System.currentTimeMillis(),
            req.jdbcUrl, req.user, req.password);

        if (target.dialect() != SourceDialect.ORACLE) {
            writeError(response, 501, "Only Oracle PL/SQL summarization is supported today.");
            return;
        }

        try {
            PlsqlSummarizer summarizer = new PlsqlSummarizer(settingsStore, new OracleCatalogProfiler());
            PlsqlSummarizer.Result result = summarizer.summarize(target, req.objectName, req.objectType);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("summary", result.summary());
            if (result.judgeVerdict() != null) {
                body.put("judge", Map.of(
                    "approved", result.judgeVerdict().approved(),
                    "explanation", result.judgeVerdict().explanation()));
            }
            response.setContentType("application/json");
            response.setStatus(200);
            response.getWriter().write(GSON.toJson(body));
        } catch (IllegalStateException e) {
            writeError(response, 500, e.getMessage());
        } catch (Exception e) {
            writeError(response, 502, "Summarization failed: " + e.getMessage());
        }
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(Map.of("error", message)));
    }
}
