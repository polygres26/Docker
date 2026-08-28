package com.nexagres.advisor.http;

import com.google.gson.Gson;
import com.nexagres.advisor.core.BackendTarget;
import com.nexagres.advisor.core.DialectSupport;
import com.nexagres.advisor.core.SourceDialect;
import com.nexagres.advisor.llm.LlmRole;
import com.nexagres.advisor.llm.LlmSettingsStore;
import com.nexagres.advisor.llm.SqlWorkloadClassifier;
import com.nexagres.advisor.workload.CapturedStatement;
import com.nexagres.advisor.workload.WorkloadCapture;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/workload} -- capture the currently-cached SQL for a source database (see
 * {@link WorkloadCapture}) and, if the PRIMARY LLM is configured (LLM configuration page),
 * classify each captured statement via {@link SqlWorkloadClassifier}. Classification is best-
 * effort: an unconfigured PRIMARY or an LLM-call failure degrades to "capture succeeded,
 * classification skipped" rather than failing the whole request -- capture is the deterministic,
 * always-useful part; classification is the enrichment layer on top.
 */
public class WorkloadRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final LlmSettingsStore settingsStore = new LlmSettingsStore();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(405);
            return;
        }

        ScanRequest scanRequest = GSON.fromJson(request.getReader(), ScanRequest.class);
        if (scanRequest == null || scanRequest.jdbcUrl == null || scanRequest.jdbcUrl.isBlank()) {
            writeError(response, 400, "jdbcUrl is required.");
            return;
        }

        BackendTarget target = new BackendTarget(
            scanRequest.name != null ? scanRequest.name : "workload-" + System.currentTimeMillis(),
            scanRequest.jdbcUrl, scanRequest.user, scanRequest.password);

        SourceDialect dialect = target.dialect();
        if (dialect == null || dialect == SourceDialect.POSTGRES) {
            writeError(response, 501, "Unrecognized or unsupported source dialect for jdbcUrl: " + scanRequest.jdbcUrl);
            return;
        }

        List<CapturedStatement> statements;
        try {
            statements = DialectSupport.workloadCaptureFor(dialect).capture(target, 200);
        } catch (Exception e) {
            writeError(response, 502, "Could not capture workload: " + e.getMessage());
            return;
        }

        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("statements", statements);

        if (settingsStore.get(LlmRole.PRIMARY).isUsable() && !statements.isEmpty()) {
            try {
                SqlWorkloadClassifier classifier = new SqlWorkloadClassifier(settingsStore);
                List<SqlWorkloadClassifier.Classification> classifications = classifier.classify(statements);
                body.put("classifications", classifications);
                body.put("categorySummary", classifier.summarizeByCategory(classifications));
            } catch (Exception e) {
                body.put("classificationError", e.getMessage());
            }
        } else {
            body.put("classificationSkipped", "PRIMARY LLM is not configured -- capture-only result. Set it up on the LLM configuration page.");
        }

        response.setContentType("application/json");
        response.setStatus(200);
        response.getWriter().write(GSON.toJson(body));
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(Map.of("error", message)));
    }
}
