package com.nexagres.advisor.http;

import com.google.gson.Gson;
import com.nexagres.advisor.catalog.CatalogProfiler;
import com.nexagres.advisor.catalog.CatalogSnapshot;
import com.nexagres.advisor.core.BackendTarget;
import com.nexagres.advisor.core.DialectSupport;
import com.nexagres.advisor.core.SourceDialect;
import com.nexagres.advisor.score.MigrationScorer;
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
 * <p>Dispatches to the right {@link CatalogProfiler} via {@link DialectSupport} -- Oracle,
 * MySQL/MariaDB, and SQL Server are all wired up; an unrecognized dialect (or Postgres, which is
 * a migration *target*, never a source) gets a 501 with an explicit message rather than silently
 * falling back to a default implementation.
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
        if (dialect == null || dialect == SourceDialect.POSTGRES) {
            writeError(response, 501, "Unrecognized or unsupported source dialect for jdbcUrl: " + scanRequest.jdbcUrl);
            return;
        }

        CatalogProfiler profiler;
        CatalogSnapshot snapshot;
        try {
            profiler = DialectSupport.profilerFor(dialect);
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
