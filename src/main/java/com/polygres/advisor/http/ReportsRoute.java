package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.llm.LlmSettingsStore;
import com.polygres.advisor.llm.ReportAnalyzer;
import com.polygres.advisor.uploads.HtmlTextExtractor;
import com.polygres.advisor.uploads.ReportStore;
import com.polygres.advisor.uploads.UploadedReport;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * {@code /api/reports} -- the upload-a-report on-ramp for customers who won't hand Advisor a live
 * connect string but will share a performance report their DBA already pulled (AWR for Oracle, a
 * MySQL performance report, a SQL Server DMV/Query Store export). Separate from {@code
 * /api/connections} entirely: no {@link com.polygres.advisor.core.BackendTarget}, no live
 * database, no {@link com.polygres.advisor.score.MigrationScorer} -- findings here come from
 * {@link ReportAnalyzer} reading the uploaded text, not deterministic catalog queries.
 *
 * <pre>
 * GET    /api/reports                        list (metadata only, no report text)
 * POST   /api/reports?name=X&amp;dialect=Y&amp;filename=Z   upload -- raw file bytes as the request body
 * GET    /api/reports/{id}                   get one (includes cached analysis, if any)
 * DELETE /api/reports/{id}                   delete (removes the on-disk text too)
 * POST   /api/reports/{id}/analyze           run/re-run ReportAnalyzer, cache + return the result
 * POST   /api/reports/analyze-batch           body {"ids": [...]} -> one combined analysis across
 *                                             several uploaded reports (see ReportAnalyzer#analyzeMultiple);
 *                                             not cached against any single report row, since it
 *                                             spans several
 * </pre>
 *
 * Upload is a raw-body POST (not multipart) deliberately -- the browser reads the file as an
 * ArrayBuffer and POSTs it directly, which sidesteps needing Jetty's multipart config on a route
 * table that otherwise has no reason to parse multipart bodies.
 */
public class ReportsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final ReportStore store = new ReportStore();
    private final LlmSettingsStore llmSettingsStore = new LlmSettingsStore();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String[] parts = request.getRequestURI().split("\\?")[0].split("/");
        String method = request.getMethod();

        try {
            if (parts.length == 3) {
                if ("GET".equalsIgnoreCase(method)) { list(response); return; }
                if ("POST".equalsIgnoreCase(method)) { upload(request, response); return; }
            } else if (parts.length == 4 && "analyze-batch".equals(parts[3]) && "POST".equalsIgnoreCase(method)) {
                analyzeBatch(request, response); return;
            } else if (parts.length == 4) {
                String id = parts[3];
                if ("GET".equalsIgnoreCase(method)) { getOne(id, response); return; }
                if ("DELETE".equalsIgnoreCase(method)) { delete(id, response); return; }
            } else if (parts.length == 5 && "analyze".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                analyze(parts[3], request, response); return;
            }
            response.setStatus(404);
        } catch (Exception e) {
            writeError(response, 502, e.getMessage());
        }
    }

    private void list(HttpServletResponse response) throws IOException {
        writeJson(response, 200, store.list());
    }

    private void upload(HttpServletRequest request, HttpServletResponse response) throws Exception {
        String name = queryParam(request, "name");
        String dialect = queryParam(request, "dialect");
        String filename = queryParam(request, "filename");
        if (dialect == null || dialect.isBlank()) { writeError(response, 400, "dialect is required."); return; }
        if (filename == null || filename.isBlank()) filename = "report.txt";

        byte[] bytes = readAll(request.getInputStream());
        if (bytes.length == 0) { writeError(response, 400, "Upload was empty."); return; }
        if (bytes.length > 20_000_000) { writeError(response, 400, "Report is too large (20 MB limit)."); return; }

        String raw = new String(bytes, StandardCharsets.UTF_8);
        String extracted = HtmlTextExtractor.maybeStrip(raw);

        UploadedReport report = store.create(name, dialect.toUpperCase(), filename, extracted);
        writeJson(response, 201, report);
    }

    private void getOne(String id, HttpServletResponse response) throws IOException {
        Optional<UploadedReport> report = store.get(id);
        if (report.isEmpty()) { writeError(response, 404, "Report not found."); return; }
        writeJson(response, 200, report.get());
    }

    private void delete(String id, HttpServletResponse response) throws IOException {
        response.setStatus(store.delete(id) ? 204 : 404);
    }

    private void analyze(String id, HttpServletRequest request, HttpServletResponse response) throws Exception {
        Optional<UploadedReport> reportOpt = store.get(id);
        if (reportOpt.isEmpty()) { writeError(response, 404, "Report not found."); return; }
        UploadedReport report = reportOpt.get();

        String text = store.getText(id);
        ReportAnalyzer.Analysis analysis = new ReportAnalyzer(llmSettingsStore).analyze(report.dialect, text);
        String analysisJson = GSON.toJson(analysis);
        store.saveAnalysis(id, analysisJson);

        writeJson(response, 200, analysis);
    }

    private void analyzeBatch(HttpServletRequest request, HttpServletResponse response) throws Exception {
        BatchRequest form = GSON.fromJson(request.getReader(), BatchRequest.class);
        if (form == null || form.ids == null || form.ids.length == 0) {
            writeError(response, 400, "ids (a non-empty array) is required.");
            return;
        }

        java.util.List<ReportAnalyzer.ReportInput> inputs = new java.util.ArrayList<>();
        for (String id : form.ids) {
            Optional<UploadedReport> reportOpt = store.get(id);
            if (reportOpt.isEmpty()) { writeError(response, 404, "Report not found: " + id); return; }
            UploadedReport report = reportOpt.get();
            inputs.add(new ReportAnalyzer.ReportInput(report.name, report.dialect, store.getText(id)));
        }

        ReportAnalyzer.Analysis analysis = new ReportAnalyzer(llmSettingsStore).analyzeMultiple(inputs);
        writeJson(response, 200, analysis);
    }

    private static class BatchRequest {
        String[] ids;
    }

    private byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
        return out.toByteArray();
    }

    private String queryParam(HttpServletRequest request, String name) {
        String value = request.getParameter(name);
        if (value == null) return null;
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    private void writeJson(HttpServletResponse response, int status, Object body) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(body));
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        writeJson(response, status, Map.of("error", message == null ? "Unknown error." : message));
    }
}
