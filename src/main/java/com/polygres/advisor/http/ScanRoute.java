package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.catalog.CatalogProfiler;
import com.polygres.advisor.catalog.CatalogSnapshot;
import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.score.MigrationScorer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code POST /api/scan} -- connect to a source database, run its {@link CatalogProfiler}, score
 * the result, return both as JSON. Synchronous end-to-end for now (fine for the schema sizes an
 * MVP scan hits); a long-running/async job model is a natural next step once real customer
 * schemas make single-request scans too slow for an HTTP round trip.
 *
 * <p>Only {@link SourceDialect#ORACLE} is wired up today, per the project sequencing decision
 * (Oracle first, then MariaDB/MySQL) -- other dialects get a 501 with an explicit message rather
 * than silently failing.
 */
public class ScanRoute implements RouteHandler {

    private static final Gson GSON = new Gson();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
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
            scanRequest.name != null ? scanRequest.name : "scan-" + System.currentTimeMillis(),
            scanRequest.jdbcUrl, scanRequest.user, scanRequest.password);

        SourceDialect dialect = target.dialect();
        if (dialect != SourceDialect.ORACLE) {
            writeError(response, 501, "Only Oracle scans are supported today. "
                + "MariaDB/MySQL support is next on the roadmap -- see README.md.");
            return;
        }

        CatalogProfiler profiler = new OracleCatalogProfiler();
        CatalogSnapshot snapshot;
        try {
            snapshot = profiler.profile(target);
        } catch (Exception e) {
            writeError(response, 502, "Could not connect or profile the source database: " + e.getMessage());
            return;
        }

        MigrationScorer.MigrationScoreReport scoreReport = new MigrationScorer().score(snapshot);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("snapshot", snapshot);
        body.put("score", scoreReport);

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
