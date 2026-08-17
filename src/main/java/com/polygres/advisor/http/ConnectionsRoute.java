package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.ConnectionRecord;
import com.polygres.advisor.core.ConnectionStore;
import com.polygres.advisor.core.DialectSupport;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.llm.LlmSettingsStore;
import com.polygres.advisor.llm.PlsqlSummarizer;
import com.polygres.advisor.report.FindingsReportGenerator;
import com.polygres.advisor.score.MigrationScorer;
import com.polygres.advisor.sizing.SizingCalculator;
import com.polygres.advisor.sizing.SizingInput;
import com.polygres.advisor.workload.WorkloadSummary;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Everything under {@code /api/connections} -- CRUD on the saved-connection registry, plus the
 * per-connection "explore" and "assess" actions, all keyed off a stored {@link ConnectionRecord}'s
 * id rather than a fresh jdbcUrl/user/password submitted from the browser each time (see
 * {@link ConnectionRecord}'s javadoc: the browser never gets the real password back). Dialect
 * dispatch (Oracle vs. MySQL/MariaDB vs. SQL Server) goes through {@link DialectSupport} -- this
 * class never hardcodes a vendor-specific implementation. Path-parsed by hand
 * ({@link #splitPath}) rather than a full router, since the route shape is small and fixed:
 *
 * <pre>
 * GET    /api/connections                          list (redacted)
 * POST   /api/connections                           create
 * GET    /api/connections/{id}                       get one (redacted)
 * PUT    /api/connections/{id}                        update
 * DELETE /api/connections/{id}                        delete
 * GET    /api/connections/{id}/objects                object tree (grouped by type)
 * GET    /api/connections/{id}/objects/detail          ?type=TABLE&amp;name=FOO -> columns, or source for routine-shaped objects
 * GET    /api/connections/{id}/parameters              database parameter/config rows
 * POST   /api/connections/{id}/scan                    run CatalogProfiler + MigrationScorer
 * POST   /api/connections/{id}/workload                run WorkloadCapture
 * POST   /api/connections/{id}/findings                 scan + workload in one round trip --
 *                                                        backs the "Findings" dashboard tab
 * POST   /api/connections/{id}/summarize                ?type=PACKAGE&amp;name=FOO -> PlsqlSummarizer
 *                                                        (PRIMARY + optional JUDGE review),
 *                                                        Oracle only today
 * GET    /api/connections/{id}/report                   live-generated Findings PDF ("Download
 *                                                        report") -- re-scans on every request,
 *                                                        never a cached copy from an earlier visit
 * POST   /api/connections/{id}/sizing                    live schema-size + workload scan ->
 *                                                        SizingCalculator recommendation
 * </pre>
 */
public class ConnectionsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final ConnectionStore store = new ConnectionStore();
    private final LlmSettingsStore llmSettingsStore = new LlmSettingsStore();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String[] parts = splitPath(request.getRequestURI());
        String method = request.getMethod();

        try {
            if (parts.length == 3) {
                if ("GET".equalsIgnoreCase(method)) { listConnections(response); return; }
                if ("POST".equalsIgnoreCase(method)) { createConnection(request, response); return; }
            } else if (parts.length == 4) {
                String id = parts[3];
                if ("GET".equalsIgnoreCase(method)) { getConnection(id, response); return; }
                if ("PUT".equalsIgnoreCase(method)) { updateConnection(id, request, response); return; }
                if ("DELETE".equalsIgnoreCase(method)) { deleteConnection(id, response); return; }
            } else if (parts.length == 5 && "objects".equals(parts[4]) && "GET".equalsIgnoreCase(method)) {
                listObjects(parts[3], response); return;
            } else if (parts.length == 5 && "parameters".equals(parts[4]) && "GET".equalsIgnoreCase(method)) {
                listParameters(parts[3], response); return;
            } else if (parts.length == 5 && "scan".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                runScan(parts[3], response); return;
            } else if (parts.length == 5 && "workload".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                runWorkload(parts[3], response); return;
            } else if (parts.length == 5 && "findings".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                runFindings(parts[3], response); return;
            } else if (parts.length == 5 && "summarize".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                runSummarize(parts[3], request, response); return;
            } else if (parts.length == 5 && "report".equals(parts[4]) && "GET".equalsIgnoreCase(method)) {
                downloadReport(parts[3], response); return;
            } else if (parts.length == 5 && "sizing".equals(parts[4]) && "POST".equalsIgnoreCase(method)) {
                runSizing(parts[3], response); return;
            } else if (parts.length == 6 && "objects".equals(parts[4]) && "detail".equals(parts[5]) && "GET".equalsIgnoreCase(method)) {
                objectDetail(parts[3], request, response); return;
            }
            response.setStatus(404);
        } catch (UnsupportedOperationException e) {
            writeError(response, 501, e.getMessage());
        } catch (Exception e) {
            writeError(response, 502, e.getMessage());
        }
    }

    private void listConnections(HttpServletResponse response) throws IOException {
        List<ConnectionRecord> redacted = store.list().stream().map(ConnectionRecord::redacted).toList();
        writeJson(response, 200, redacted);
    }

    private void createConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ConnectionForm form = GSON.fromJson(request.getReader(), ConnectionForm.class);
        if (form == null || form.jdbcUrl == null || form.jdbcUrl.isBlank()) {
            writeError(response, 400, "jdbcUrl is required.");
            return;
        }
        ConnectionRecord record = store.create(form.name, form.jdbcUrl, form.user, form.password);
        writeJson(response, 201, record.redacted());
    }

    private void getConnection(String id, HttpServletResponse response) throws IOException {
        Optional<ConnectionRecord> record = store.get(id);
        if (record.isEmpty()) { writeError(response, 404, "Connection not found."); return; }
        writeJson(response, 200, record.get().redacted());
    }

    private void updateConnection(String id, HttpServletRequest request, HttpServletResponse response) throws IOException {
        ConnectionForm form = GSON.fromJson(request.getReader(), ConnectionForm.class);
        Optional<ConnectionRecord> updated = store.update(id, form.name, form.jdbcUrl, form.user, form.password);
        if (updated.isEmpty()) { writeError(response, 404, "Connection not found."); return; }
        writeJson(response, 200, updated.get().redacted());
    }

    private void deleteConnection(String id, HttpServletResponse response) throws IOException {
        boolean removed = store.delete(id);
        response.setStatus(removed ? 204 : 404);
    }

    private void listObjects(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        writeJson(response, 200, DialectSupport.explorerFor(target.dialect()).listObjects(target));
    }

    private void objectDetail(String id, HttpServletRequest request, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        String type = request.getParameter("type");
        String name = request.getParameter("name");
        if (type == null || name == null) { writeError(response, 400, "type and name query params are required."); return; }

        var explorer = DialectSupport.explorerFor(target.dialect());
        if ("TABLE".equalsIgnoreCase(type) || "VIEW".equalsIgnoreCase(type)) {
            writeJson(response, 200, Map.of("columns", explorer.describeTable(target, name)));
        } else {
            writeJson(response, 200, Map.of("source", explorer.fetchSource(target, name, type)));
        }
    }

    private void listParameters(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        writeJson(response, 200, DialectSupport.parameterReaderFor(target.dialect()).listParameters(target));
    }

    private void runScan(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        var snapshot = DialectSupport.profilerFor(target.dialect()).profile(target);
        var score = new MigrationScorer().score(snapshot);
        writeJson(response, 200, Map.of("snapshot", snapshot, "score", score));
    }

    private void runWorkload(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        var statements = DialectSupport.workloadCaptureFor(target.dialect()).capture(target, 300);
        writeJson(response, 200, Map.of("statements", statements, "summary", WorkloadSummary.summarize(statements)));
    }

    /**
     * Scan + workload capture in one round trip, for the Findings dashboard. Workload capture is
     * best-effort here (same reasoning as {@link WorkloadRoute}): it needs a higher privilege tier
     * than the catalog scan does on every dialect (V$SQL / performance_schema / DMVs), so a
     * locked-down read-only account can still get a full findings dashboard minus the "what's
     * actually running" section, rather than the whole request failing over one missing grant.
     */
    private void runFindings(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;

        var snapshot = DialectSupport.profilerFor(target.dialect()).profile(target);
        var score = new MigrationScorer().score(snapshot);

        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("snapshot", snapshot);
        body.put("score", score);
        try {
            body.put("workload", DialectSupport.workloadCaptureFor(target.dialect()).capture(target, 100));
        } catch (Exception e) {
            body.put("workloadError", "Workload capture unavailable: " + e.getMessage());
        }
        writeJson(response, 200, body);
    }

    /**
     * PL/SQL summarization for one named object, using this connection's stored credentials and
     * whatever PRIMARY/JUDGE LLM is configured on the LLM configuration page. Oracle-only today
     * (same as {@link SummarizeRoute}) -- {@link PlsqlSummarizer} is PL/SQL-specific; a MySQL/SQL
     * Server routine-source summarizer is a natural follow-up once one exists.
     */
    private void runSummarize(String id, HttpServletRequest request, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        if (target.dialect() != SourceDialect.ORACLE) {
            writeError(response, 501, "PL/SQL summarization is only supported for Oracle today.");
            return;
        }
        String type = request.getParameter("type");
        String name = request.getParameter("name");
        if (type == null || name == null) { writeError(response, 400, "type and name query params are required."); return; }

        PlsqlSummarizer summarizer = new PlsqlSummarizer(llmSettingsStore, new OracleCatalogProfiler());
        PlsqlSummarizer.Result result = summarizer.summarize(target, name, type);

        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("summary", result.summary());
        if (result.judgeVerdict() != null) {
            body.put("judge", Map.of(
                "approved", result.judgeVerdict().approved(),
                "explanation", result.judgeVerdict().explanation()));
        }
        writeJson(response, 200, body);
    }

    /**
     * "Download report" -- generates the Findings PDF live from a fresh scan (not a cached copy),
     * per the project decision. GET, not POST: a plain browser navigation/{@code <a download>}
     * can trigger a file download with the session cookie attached; a JSON POST route couldn't
     * without extra blob-fetching plumbing on the client for no real benefit here.
     */
    private void downloadReport(String id, HttpServletResponse response) throws Exception {
        Optional<ConnectionRecord> recordOpt = store.get(id);
        if (recordOpt.isEmpty()) { writeError(response, 404, "Connection not found."); return; }
        ConnectionRecord record = recordOpt.get();
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;

        var snapshot = DialectSupport.profilerFor(target.dialect()).profile(target);
        var score = new MigrationScorer().score(snapshot);
        byte[] pdf = new FindingsReportGenerator().generate(record, snapshot, score);

        String filename = record.name.replaceAll("[^a-zA-Z0-9._-]", "_") + "-migration-assessment.pdf";
        response.setContentType("application/pdf");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        response.setContentLength(pdf.length);
        response.getOutputStream().write(pdf);
        response.getOutputStream().flush();
    }

    /**
     * Postgres instance sizing for this connection: a fresh catalog scan for schema size (storage
     * sizing) plus a fresh workload capture for CPU/logical-IO/physical-IO magnitude (vCPU/memory/
     * IOPS sizing) feed {@link SizingCalculator}. Workload capture is best-effort here, same
     * reasoning as {@link #runFindings} -- a locked-down account still gets a storage-only
     * recommendation rather than the whole request failing.
     */
    private void runSizing(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireTarget(id, response);
        if (target == null) return;
        Optional<ConnectionRecord> recordOpt = store.get(id);
        String sourceLabel = recordOpt.map(r -> r.name).orElse(id);

        var snapshot = DialectSupport.profilerFor(target.dialect()).profile(target);

        long totalExec = 0, totalElapsed = 0, totalCpu = 0, totalBufferGets = 0, totalDiskReads = 0;
        try {
            var statements = DialectSupport.workloadCaptureFor(target.dialect()).capture(target, 300);
            for (var s : statements) {
                totalExec += s.executions();
                totalElapsed += s.elapsedTimeMicros();
                totalCpu += s.cpuTimeMicros();
                totalBufferGets += s.bufferGets();
                totalDiskReads += s.diskReads();
            }
        } catch (Exception ignored) {
            // Workload capture needs a higher privilege tier than the catalog scan on every
            // dialect -- degrade to storage-only sizing rather than failing the whole request.
        }

        SizingInput input = new SizingInput(sourceLabel, snapshot.schemaSizeBytes,
            totalExec, totalElapsed, totalCpu, totalBufferGets, totalDiskReads, null, null, null);
        writeJson(response, 200, SizingCalculator.calculate(input));
    }

    /** {@code null} on 404 (already written); throws {@link UnsupportedOperationException} for a dialect Advisor can't recognize at all -- {@link #handle} turns that into a 501. */
    private BackendTarget requireTarget(String id, HttpServletResponse response) throws IOException {
        Optional<ConnectionRecord> record = store.get(id);
        if (record.isEmpty()) { writeError(response, 404, "Connection not found."); return null; }
        BackendTarget target = record.get().toTarget();
        if (target.dialect() == null || target.dialect() == SourceDialect.POSTGRES) {
            writeError(response, 501, "Unrecognized or unsupported source dialect for jdbcUrl: " + target.jdbcUrl());
            return null;
        }
        return target;
    }

    private String[] splitPath(String uri) {
        String path = uri.split("\\?")[0];
        return path.split("/");
    }

    private void writeJson(HttpServletResponse response, int status, Object body) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(body));
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        writeJson(response, status, Map.of("error", message == null ? "Unknown error." : message));
    }

    private static class ConnectionForm {
        String name;
        String jdbcUrl;
        String user;
        String password;
    }
}
