package com.nexagres.wire.capture;

import com.nexagres.wire.config.NodeRegistry;
import com.nexagres.wire.server.ServerOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plays a captured workload back against a Postgres target in the order it was originally
 * captured across the whole fleet -- not just one instance's own order.
 *
 * <p>Every live PolyWire instance keeps its own in-memory {@link WorkloadCaptureBuffer}, stamped
 * with the wall-clock instant each statement was captured. This tool discovers every live
 * instance via {@link NodeRegistry} (the same {@code polywire_nodes} heartbeat table the admin
 * UI's topology view reads), pulls each one's captured entries over its {@code GET /api/capture}
 * admin route, and merges all of them into one list sorted by {@code wallClock} -- reconstructing
 * a single global arrival order across every instance, not each instance's own local order.
 *
 * <p><b>The honest limit of this approach:</b> ordering is only as good as clock sync across the
 * instances it pulled from. Two statements captured on different hosts within the same
 * few-millisecond window can end up ordered by clock skew rather than true arrival order --
 * appropriate for an NTP-synced fleet (the normal case for anything running in a real cloud
 * environment), not a guarantee under adversarial or badly-skewed clocks. This replaces this
 * feature's previous design (a shared Postgres table with a {@code bigserial} sequence, which
 * gave an exact single order at the cost of a DB round trip per captured statement and no
 * wall-clock fidelity to the client's true arrival time); this version trades that DB-backed exact
 * order for zero-I/O in-memory capture and ordering that reflects when each instance actually saw
 * the statement.
 *
 * <p>Because captured buffers are in-memory and bounded ({@code POLYWIRE_CAPTURE_BUFFER_SIZE}),
 * this tool takes one pull per node, per run -- it is not a durable log; run it before an
 * instance's buffer wraps if you need everything currently held.
 *
 * <p>Run via {@code java -cp nexagres-wire.jar com.nexagres.wire.capture.WorkloadReplayer}, using
 * the same {@code POLYWIRE_PG_*}/{@code POLYWIRE_ADMIN_TOKEN} options the server itself uses.
 */
public final class WorkloadReplayer {

    private static final Logger log = LoggerFactory.getLogger(WorkloadReplayer.class);
    private static final int PAGE_SIZE = 1000;

    private WorkloadReplayer() {
    }

    record CapturedEntry(Instant wallClock, String nodeId, long localSeq, String sqlText, List<Object> bindParams) {
    }

    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.parse(args);
        String adminToken = System.getenv("POLYWIRE_ADMIN_TOKEN");

        List<NodeRegistry.NodeRow> nodes = NodeRegistry.listAll(options).stream()
                .filter(n -> "up".equals(n.status()))
                .toList();
        log.info("workload replay: {} live node(s) discovered via polywire_nodes", nodes.size());

        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        List<CapturedEntry> all = new ArrayList<>();
        for (NodeRegistry.NodeRow node : nodes) {
            int fromNode = fetchAll(http, node, adminToken, all);
            log.info("workload replay: pulled {} entries from node {} ({}:{})", fromNode, node.nodeId(),
                    node.host(), node.adminPort());
        }

        // The whole point: merge every instance's own-order stream into one global order by the
        // wall-clock instant each entry was captured, not by which node it came from or the order
        // nodes happened to be pulled in. nodeId/localSeq only break ties within the same instant.
        all.sort(Comparator.comparing(CapturedEntry::wallClock)
                .thenComparing(CapturedEntry::nodeId)
                .thenComparingLong(CapturedEntry::localSeq));

        log.info("workload replay: {} total captured statement(s) merged into one global order, replaying now",
                all.size());

        long replayed = 0;
        long failed = 0;
        try (Connection conn = com.nexagres.wire.pgwire.PgConnections.open(options)) {
            conn.setAutoCommit(true);
            for (CapturedEntry entry : all) {
                try {
                    replayOne(conn, entry);
                    replayed++;
                } catch (SQLException e) {
                    failed++;
                    log.warn("workload replay: node={} localSeq={} failed ({}): {}", entry.nodeId(),
                            entry.localSeq(), e.getMessage(), entry.sqlText());
                }
            }
        }
        log.info("workload replay: done -- {} replayed, {} failed, out of {} merged", replayed, failed, all.size());
    }

    /** Pages through one node's {@code /api/capture} until exhausted, appending into {@code out}. */
    private static int fetchAll(HttpClient http, NodeRegistry.NodeRow node, String adminToken,
            List<CapturedEntry> out) throws Exception {
        int pulled = 0;
        long since = 0;
        while (true) {
            URI uri = URI.create("http://" + node.host() + ":" + node.adminPort()
                    + "/api/capture?since=" + since + "&limit=" + PAGE_SIZE);
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(10));
            if (adminToken != null && !adminToken.isBlank()) {
                reqBuilder.header("Authorization", "Bearer " + adminToken);
            }
            HttpResponse<String> response = http.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.warn("workload replay: GET {} returned {} -- skipping the rest of this node", uri,
                        response.statusCode());
                break;
            }
            JsonArray page = JsonParser.parseString(response.body()).getAsJsonArray();
            if (page.isEmpty()) {
                break;
            }
            for (JsonElement el : page) {
                JsonObject obj = el.getAsJsonObject();
                long localSeq = obj.get("localSeq").getAsLong();
                Instant wallClock = Instant.parse(obj.get("wallClock").getAsString());
                String nodeId = obj.get("nodeId").getAsString();
                String sqlText = obj.get("sqlText").getAsString();
                List<Object> bindParams = new ArrayList<>();
                for (JsonElement p : obj.getAsJsonArray("bindParams")) {
                    bindParams.add(p.isJsonNull() ? null : p.getAsString());
                }
                out.add(new CapturedEntry(wallClock, nodeId, localSeq, sqlText, bindParams));
                since = Math.max(since, localSeq);
                pulled++;
            }
            if (page.size() < PAGE_SIZE) {
                break;
            }
        }
        return pulled;
    }

    private static void replayOne(Connection conn, CapturedEntry entry) throws SQLException {
        List<Object> bindParams = entry.bindParams();
        if (bindParams.isEmpty()) {
            try (java.sql.Statement st = conn.createStatement()) {
                st.execute(entry.sqlText());
            }
            return;
        }
        try (java.sql.PreparedStatement ps = conn.prepareStatement(entry.sqlText())) {
            for (int i = 0; i < bindParams.size(); i++) {
                Object p = bindParams.get(i);
                if (p == null) {
                    ps.setNull(i + 1, java.sql.Types.VARCHAR);
                } else {
                    ps.setString(i + 1, String.valueOf(p));
                }
            }
            ps.execute();
        }
    }
}
