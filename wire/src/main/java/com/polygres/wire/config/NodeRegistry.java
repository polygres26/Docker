package com.polygres.wire.config;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight deployment-topology visibility: every polywire instance writes a heartbeat row to
 * {@code polywire_nodes} on the config-primary Postgres it already connects to (the same one
 * {@link ConfigStore} uses), every ~10s. This is intentionally simpler than Ignite cluster
 * membership ({@code CacheStage}/{@code PolyWireCluster}) -- it doesn't cross-reference actual
 * cache-cluster state, it's just "which processes are alive and where", for a multi-AZ /
 * behind-a-load-balancer deployment to have basic topology visibility in the admin UI. See
 * {@code MetricsServer#handleNodes} for the read side ({@code GET /api/nodes}).
 */
public final class NodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);
    private static final long HEARTBEAT_PERIOD_SECONDS = 10;
    private static final long STALE_ROW_MAX_AGE_SECONDS = 24 * 60 * 60;

    public record NodeRow(UUID nodeId, String host, int adminPort, String zone, String version,
            Instant startedAt, Instant lastHeartbeat, String status) {
    }

    private final com.polygres.wire.server.ServerOptions options;
    private final UUID nodeId = UUID.randomUUID();
    private final String host;
    private final int adminPort;
    private final String zone;
    private final String version;
    private final Instant startedAt = Instant.now();
    private ScheduledExecutorService scheduler;

    public NodeRegistry(com.polygres.wire.server.ServerOptions options, int adminPort, String version) {
        this.options = options;
        this.adminPort = adminPort;
        this.host = resolveHost();
        this.zone = resolveZone(this.host);
        this.version = version;
    }

    // POLYWIRE_ZONE is how an operator names a real availability zone/region ("us-east-1a",
    // "zone-b", whatever their cloud or scheme calls it) -- there's no portable way to detect
    // that automatically across AWS/GCP/Azure/on-prem/a laptop, so it's opt-in. When it's not
    // set (the common case for local dev and single-machine runs), fall back to this node's own
    // hostname as the group label instead of a generic "unknown" bucket or, worse, a fabricated
    // cloud-sounding placeholder like "us-east-1" that would be actively misleading on a laptop.
    // One real machine not in a zoned deployment is its own honest group of one.
    private static String resolveZone(String host) {
        String zone = System.getenv("POLYWIRE_ZONE");
        return (zone != null && !zone.isBlank()) ? zone : host;
    }

    /** Same host-resolution {@link #host} uses, exposed so a caller outside this class (the
     * switchover drain fan-out in {@code MetricsServer}) can recognize its OWN row in {@link
     * #listAll} and skip forwarding a drain call to itself -- it already applied that call
     * locally. */
    public static String resolveHost() {
        String advertised = System.getenv("POLYWIRE_ADVERTISED_HOST");
        if (advertised != null && !advertised.isBlank()) {
            return advertised;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.warn("nodes: could not resolve local hostname, falling back to \"unknown\"", e);
            return "unknown";
        }
    }

    public static void ensureSchema(com.polygres.wire.server.ServerOptions options) throws SQLException {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_nodes ("
                    + "node_id uuid PRIMARY KEY, "
                    + "host text NOT NULL, "
                    + "admin_port int NOT NULL, "
                    + "zone text, "
                    + "version text, "
                    + "started_at timestamptz NOT NULL, "
                    + "last_heartbeat timestamptz NOT NULL)");
        }
    }

    /** Starts the ~10s heartbeat loop. Call once the config-primary connection is known good
     * (i.e. after {@link ConfigStore#ensureSchema()}). Daemon thread, no graceful shutdown needed
     * beyond the JVM exiting -- same pattern as {@link ConfigStore}'s listen executor. */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-node-heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::heartbeatSafely, 0, HEARTBEAT_PERIOD_SECONDS, TimeUnit.SECONDS);
        log.info("nodes: heartbeat started, node_id={} host={} adminPort={} zone={} version={}",
                nodeId, host, adminPort, zone == null ? "(none)" : zone, version);
    }

    private void heartbeatSafely() {
        try {
            heartbeatOnce();
        } catch (Exception e) {
            log.warn("nodes: heartbeat failed, will retry in {}s", HEARTBEAT_PERIOD_SECONDS, e);
        }
    }

    private void heartbeatOnce() throws SQLException {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO polywire_nodes (node_id, host, admin_port, zone, version, started_at, last_heartbeat) "
                            + "VALUES (?, ?, ?, ?, ?, ?, now()) "
                            + "ON CONFLICT (node_id) DO UPDATE SET "
                            + "host = EXCLUDED.host, admin_port = EXCLUDED.admin_port, zone = EXCLUDED.zone, "
                            + "version = EXCLUDED.version, last_heartbeat = now()")) {
                ps.setObject(1, nodeId);
                ps.setString(2, host);
                ps.setInt(3, adminPort);
                ps.setString(4, zone);
                ps.setString(5, version);
                ps.setTimestamp(6, Timestamp.from(startedAt));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM polywire_nodes WHERE last_heartbeat < now() - (? || ' seconds')::interval")) {
                ps.setLong(1, STALE_ROW_MAX_AGE_SECONDS);
                ps.executeUpdate();
            }
        }
    }

    /** Read side for {@code GET /api/nodes} -- all rows, sorted by zone then host. */
    public static List<NodeRow> listAll(com.polygres.wire.server.ServerOptions options) throws SQLException {
        List<NodeRow> rows = new ArrayList<>();
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT node_id, host, admin_port, zone, version, started_at, last_heartbeat "
                                + "FROM polywire_nodes ORDER BY zone NULLS LAST, host")) {
            while (rs.next()) {
                UUID id = (UUID) rs.getObject(1);
                String host = rs.getString(2);
                int adminPort = rs.getInt(3);
                String zone = rs.getString(4);
                String version = rs.getString(5);
                Instant startedAt = rs.getTimestamp(6).toInstant();
                Instant lastHeartbeat = rs.getTimestamp(7).toInstant();
                String status = lastHeartbeat.isAfter(Instant.now().minusSeconds(30)) ? "up" : "stale";
                rows.add(new NodeRow(id, host, adminPort, zone, version, startedAt, lastHeartbeat, status));
            }
        }
        return rows;
    }
}
