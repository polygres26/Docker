package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.catalog.OracleObjectExplorer;
import com.polygres.advisor.catalog.OracleParameterReader;
import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.ConnectionRecord;
import com.polygres.advisor.core.ConnectionStore;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.score.MigrationScorer;
import com.polygres.advisor.workload.OracleWorkloadCapture;
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
 * {@link ConnectionRecord}'s javadoc: the browser never gets the real password back). Path-parsed
 * by hand ({@link #splitPath}) rather than a full router, since the route shape is small and fixed:
 *
 * <pre>
 * GET    /api/connections                          list (redacted)
 * POST   /api/connections                           create
 * GET    /api/connections/{id}                       get one (redacted)
 * PUT    /api/connections/{id}                        update
 * DELETE /api/connections/{id}                        delete
 * GET    /api/connections/{id}/objects                object tree (grouped by type)
 * GET    /api/connections/{id}/objects/detail          ?type=TABLE&amp;name=FOO -> columns, or source for PL/SQL objects
 * GET    /api/connections/{id}/parameters              V$PARAMETER (Oracle) rows
 * POST   /api/connections/{id}/scan                    run CatalogProfiler + MigrationScorer
 * POST   /api/connections/{id}/workload                run WorkloadCapture
 * </pre>
 *
 * Only Oracle is wired up for the explore/assess actions today (MariaDB/MySQL is next -- same
 * 501-not-silent-failure convention as {@link ScanRoute}).
 */
public class ConnectionsRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private final ConnectionStore store = new ConnectionStore();

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
            } else if (parts.length == 6 && "objects".equals(parts[4]) && "detail".equals(parts[5]) && "GET".equalsIgnoreCase(method)) {
                objectDetail(parts[3], request, response); return;
            }
            response.setStatus(404);
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
        BackendTarget target = requireOracleTarget(id, response);
        if (target == null) return;
        writeJson(response, 200, new OracleObjectExplorer().listObjects(target));
    }

    private void objectDetail(String id, HttpServletRequest request, HttpServletResponse response) throws Exception {
        BackendTarget target = requireOracleTarget(id, response);
        if (target == null) return;
        String type = request.getParameter("type");
        String name = request.getParameter("name");
        if (type == null || name == null) { writeError(response, 400, "type and name query params are required."); return; }

        OracleObjectExplorer explorer = new OracleObjectExplorer();
        if ("TABLE".equalsIgnoreCase(type) || "VIEW".equalsIgnoreCase(type)) {
            writeJson(response, 200, Map.of("columns", explorer.describeTable(target, name)));
        } else {
            writeJson(response, 200, Map.of("source", explorer.fetchSource(target, name, type)));
        }
    }

    private void listParameters(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireOracleTarget(id, response);
        if (target == null) return;
        writeJson(response, 200, new OracleParameterReader().listParameters(target));
    }

    private void runScan(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireOracleTarget(id, response);
        if (target == null) return;
        var snapshot = new OracleCatalogProfiler().profile(target);
        var score = new MigrationScorer().score(snapshot);
        writeJson(response, 200, Map.of("snapshot", snapshot, "score", score));
    }

    private void runWorkload(String id, HttpServletResponse response) throws Exception {
        BackendTarget target = requireOracleTarget(id, response);
        if (target == null) return;
        writeJson(response, 200, Map.of("statements", new OracleWorkloadCapture().capture(target, 200)));
    }

    private BackendTarget requireOracleTarget(String id, HttpServletResponse response) throws IOException {
        Optional<ConnectionRecord> record = store.get(id);
        if (record.isEmpty()) { writeError(response, 404, "Connection not found."); return null; }
        BackendTarget target = record.get().toTarget();
        if (target.dialect() != SourceDialect.ORACLE) {
            writeError(response, 501, "Only Oracle is supported today. MariaDB/MySQL is next on the roadmap.");
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
