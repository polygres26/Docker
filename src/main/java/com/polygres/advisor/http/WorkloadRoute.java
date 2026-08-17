package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.DialectSupport;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.llm.ClaudeLlmProvider;
import com.polygres.advisor.llm.SqlWorkloadClassifier;
import com.polygres.advisor.workload.CapturedStatement;
import com.polygres.advisor.workload.WorkloadCapture;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/workload} -- capture the currently-cached SQL for a source database (Oracle
 * only today, see {@link WorkloadCapture}) and, if {@code POLYGRES_LLM_CLASSIFY_MODEL} is set,
 * classify each captured statement via {@link SqlWorkloadClassifier}. Classification is best-
 * effort: a missing model env var or an LLM-call failure degrades to "capture succeeded,
 * classification skipped" rather than failing the whole request -- capture is the deterministic,
 * always-useful part; classification is the enrichment layer on top.
 */
public class WorkloadRoute implements RouteHandler {

    private static final Gson GSON = new Gson();

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

        String classifyModel = System.getenv("POLYGRES_LLM_CLASSIFY_MODEL");
        if (classifyModel != null && !classifyModel.isBlank() && !statements.isEmpty()) {
            try {
                SqlWorkloadClassifier classifier = new SqlWorkloadClassifier(new ClaudeLlmProvider());
                List<SqlWorkloadClassifier.Classification> classifications = classifier.classify(statements);
                body.put("classifications", classifications);
                body.put("categorySummary", classifier.summarizeByCategory(classifications));
            } catch (Exception e) {
                body.put("classificationError", e.getMessage());
            }
        } else {
            body.put("classificationSkipped", "POLYGRES_LLM_CLASSIFY_MODEL not set -- capture-only result.");
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
