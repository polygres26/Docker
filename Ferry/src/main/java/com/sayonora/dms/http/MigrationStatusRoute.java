package com.sayonora.dms.http;

import com.google.gson.Gson;
import com.sayonora.dms.core.ConnectionRecord;
import com.sayonora.dms.core.ConnectionStore;
import com.sayonora.dms.core.MigrationStatusStore;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@code GET /api/migration/status?connectionId=<id>} -- the "Web Progress Report" for a running
 * or finished {@code sayonora-migration} run, reusing Advisor's existing saved-connection registry
 * ({@link ConnectionStore}) rather than inventing a second place to store a jdbcUrl/user/password.
 * The connection here is expected to point at the migration's TARGET Postgres (where Warp
 * itself writes) -- deliberately does NOT go through {@link ConnectionsRoute#requireTarget}, which
 * exists specifically to reject a Postgres-dialect connection as an invalid Advisor *assessment*
 * source; that restriction doesn't apply here since this route's whole job is reading Postgres.
 */
public class MigrationStatusRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final ConnectionStore connectionStore = new ConnectionStore();
    private final MigrationStatusStore migrationStatusStore = new MigrationStatusStore();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!"GET".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(404);
            return;
        }
        String connectionId = request.getParameter("connectionId");
        if (connectionId == null || connectionId.isBlank()) {
            writeError(response, 400, "connectionId query param is required.");
            return;
        }
        Optional<ConnectionRecord> record = connectionStore.get(connectionId);
        if (record.isEmpty()) {
            writeError(response, 404, "Connection not found.");
            return;
        }
        ConnectionRecord target = record.get();
        try {
            List<MigrationStatusStore.SourceStatus> statuses = migrationStatusStore.listStatuses(
                    target.jdbcUrl, target.user, target.password, "migration-status-" + target.id);
            writeJson(response, 200, statuses);
        } catch (Exception e) {
            writeError(response, 502, "Could not read migration status: " + e.getMessage());
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
