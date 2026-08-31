package com.nexagres.dms.http;

import com.google.gson.Gson;
import com.nexagres.dms.migration.MigrationJobRequest;
import com.nexagres.dms.migration.MigrationJobRunner;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * {@code POST /api/migration/jobs} starts a real {@code nexagres-migration} run against the
 * connector + config in the request body (see {@link MigrationJobRequest}'s own javadoc for the
 * shape); {@code GET /api/migration/jobs} lists every job this Advisor process has launched since
 * it started (see {@link MigrationJobRunner}'s own javadoc for why that list is in-memory, not
 * persisted); {@code DELETE /api/migration/jobs/<jobId>} asks a running job to stop at its next
 * checkpoint. This is the "launch" counterpart to {@link MigrationStatusRoute}'s "report on
 * progress" -- together they're the whole Data Sync section's backend.
 */
public class MigrationJobsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final MigrationJobRunner jobRunner = new MigrationJobRunner();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ("POST".equalsIgnoreCase(request.getMethod())) {
            handleStart(request, response);
        } else if ("GET".equalsIgnoreCase(request.getMethod())) {
            writeJson(response, 200, jobRunner.list());
        } else if ("DELETE".equalsIgnoreCase(request.getMethod())) {
            handleStop(request, response);
        } else {
            response.setStatus(404);
        }
    }

    private void handleStop(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getRequestURI();
        String jobId = path.substring(path.lastIndexOf('/') + 1);
        if (jobId.isBlank()) {
            writeError(response, 400, "jobId path segment is required, e.g. DELETE /api/migration/jobs/<jobId>.");
            return;
        }
        boolean stopped = jobRunner.stop(jobId);
        if (!stopped) {
            writeError(response, 404, "No running job with id " + jobId + ".");
            return;
        }
        writeJson(response, 200, Map.of("stopped", true));
    }

    private void handleStart(HttpServletRequest request, HttpServletResponse response) throws IOException {
        MigrationJobRequest jobRequest;
        try {
            jobRequest = GSON.fromJson(request.getReader(), MigrationJobRequest.class);
        } catch (Exception e) {
            writeError(response, 400, "Malformed request body: " + e.getMessage());
            return;
        }
        if (jobRequest == null) {
            writeError(response, 400, "Request body is required.");
            return;
        }
        try {
            MigrationJobRunner.JobState state = jobRunner.start(jobRequest);
            writeJson(response, 200, state);
        } catch (IllegalArgumentException e) {
            // A malformed connectorType/sourceConfig/target -- a real client error (400), not a
            // server failure. Real construction failures (a source that can't actually be
            // reached, a Polywire gRPC endpoint that's down) surface later as the job's own FAILED
            // status via GET, not synchronously here -- see MigrationJobRunner's own javadoc for
            // why sourceConfig validation happens eagerly on the calling thread while everything
            // else runs in the background.
            writeError(response, 400, e.getMessage());
        } catch (Exception e) {
            writeError(response, 502, "Could not start migration job: " + e.getMessage());
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
