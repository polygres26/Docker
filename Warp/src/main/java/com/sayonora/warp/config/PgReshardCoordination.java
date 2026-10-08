package com.sayonora.warp.config;

import com.sayonora.warp.core.ReshardCoordinator;
import com.sayonora.warp.core.ReshardGate;
import com.sayonora.warp.pgwire.PgConnections;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ReshardCoordinator} on the config Postgres, plus the loop every instance runs to follow it. The mover writes one gate row per table
 * (phase, moving slots, lease); each instance polls the rows, applies the phase to its own {@link ReshardGate}, and writes an acknowledgement row
 * once the phase is in force locally (writes admitted before a freeze have finished; the new config version has been applied before writes are
 * released). A gate whose lease has run out is treated as open, so a mover that dies cannot hold writes for longer than the lease.
 */
public final class PgReshardCoordination implements ReshardCoordinator {

    private static final Logger log = LoggerFactory.getLogger(PgReshardCoordination.class);

    private final ServerOptions options;
    private final Supplier<String> instanceId;
    private final long leaseSeconds = envLong("WARP_RESHARD_LEASE_SECONDS", 30);
    private final long syncMillis = envLong("WARP_RESHARD_SYNC_MILLIS", 200);
    private final Map<String, String> applied = new HashMap<>();
    private ScheduledExecutorService scheduler;
    private volatile Runnable syncNow;

    public PgReshardCoordination(ServerOptions options, Supplier<String> instanceId) {
        this.options = options;
        this.instanceId = instanceId;
    }

    private static long envLong(String name, long fallback) {
        try {
            String v = System.getenv(name);
            return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public void ensureSchema() throws SQLException {
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement()) {
            com.sayonora.warp.core.DdlTemplates.run(st, "postgres", "warp_reshard", Map.of());
        }
    }

    // ---- the mover's side ----------------------------------------------------------------------------------------------------------------

    @Override
    public long publish(String table, Phase phase, Collection<Integer> slots, boolean blockScatter, long flipVersion) throws SQLException {
        String slotText = slots == null ? "" : slots.stream().map(String::valueOf).collect(Collectors.joining(","));
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO warp_reshard_gate (table_name, epoch, phase, slots, block_scatter, flip_version, holder, lease_until) "
                                + "VALUES (?, 1, ?, ?, ?, ?, ?, now() + (? || ' seconds')::interval) "
                                + "ON CONFLICT (table_name) DO UPDATE SET epoch = warp_reshard_gate.epoch + 1, phase = EXCLUDED.phase, "
                                + "slots = EXCLUDED.slots, block_scatter = EXCLUDED.block_scatter, flip_version = EXCLUDED.flip_version, "
                                + "holder = EXCLUDED.holder, lease_until = EXCLUDED.lease_until "
                                + "WHERE warp_reshard_gate.holder = EXCLUDED.holder OR warp_reshard_gate.phase = 'OPEN' "
                                + "OR warp_reshard_gate.lease_until < now() RETURNING epoch")) {
            ps.setString(1, table.toLowerCase());
            ps.setString(2, phase.name());
            ps.setString(3, slotText);
            ps.setBoolean(4, blockScatter);
            ps.setLong(5, flipVersion);
            ps.setString(6, instanceId.get());
            ps.setString(7, String.valueOf(leaseSeconds));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("another instance is moving slots of '" + table + "' (its hold has not been released or expired)");
                }
                long epoch = rs.getLong(1);
                Runnable now = syncNow;
                if (now != null) {
                    now.run(); // this instance follows its own gate at once instead of at the next poll
                }
                return epoch;
            }
        }
    }

    @Override
    public void renew(String table) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement("UPDATE warp_reshard_gate SET lease_until = now() + (? || ' seconds')::interval "
                        + "WHERE table_name = ? AND holder = ? AND phase <> 'OPEN'")) {
            ps.setString(1, String.valueOf(leaseSeconds));
            ps.setString(2, table.toLowerCase());
            ps.setString(3, instanceId.get());
            ps.executeUpdate();
        }
    }

    @Override
    public void awaitAcks(String table, long epoch, long timeoutMillis) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<String> missing;
        do {
            Set<String> live = new HashSet<>();
            for (java.util.UUID id : NodeRegistry.liveNodeIds(options)) {
                live.add(id.toString());
            }
            live.add(instanceId.get()); // this instance acknowledges even if its first heartbeat is not in yet
            Map<String, Long> acks = new HashMap<>();
            try (Connection c = PgConnections.open(options); PreparedStatement ps = c.prepareStatement(
                    "SELECT instance, epoch FROM warp_reshard_ack WHERE table_name = ?")) {
                ps.setString(1, table.toLowerCase());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        acks.put(rs.getString(1), rs.getLong(2));
                    }
                }
            }
            missing = new ArrayList<>();
            for (String id : live) {
                if (acks.getOrDefault(id, 0L) < epoch) {
                    missing.add(id);
                }
            }
            if (missing.isEmpty()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for the other Warp instances");
            }
        } while (System.currentTimeMillis() < deadline);
        throw new IllegalStateException("Warp instance(s) " + missing + " did not acknowledge the hold on '" + table + "' in time; nothing was moved");
    }

    // ---- every instance's side -----------------------------------------------------------------------------------------------------------

    /** Starts following the gate rows; the first pass runs before this returns, so a hold already in force is applied before traffic is served. */
    public synchronized void start(ReshardGate gate, LongSupplier appliedConfigVersion, Runnable catchUpConfig) {
        if (scheduler != null) {
            return;
        }
        syncOnce(gate, appliedConfigVersion, catchUpConfig);
        syncNow = () -> syncOnce(gate, appliedConfigVersion, catchUpConfig);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-reshard-sync");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> syncOnce(gate, appliedConfigVersion, catchUpConfig), syncMillis, syncMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private record State(String table, long epoch, String phase, String slots, boolean blockScatter, long flipVersion, boolean live) {
    }

    /** One pass: apply every gate row that changed since the last pass to the local gate and acknowledge it. Package-visible for tests. */
    public synchronized void syncOnce(ReshardGate gate, LongSupplier appliedConfigVersion, Runnable catchUpConfig) {
        List<State> states = new ArrayList<>();
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT table_name, epoch, phase, slots, block_scatter, flip_version, lease_until > now() "
                        + "FROM warp_reshard_gate")) {
            while (rs.next()) {
                states.add(new State(rs.getString(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBoolean(5), rs.getLong(6), rs.getBoolean(7)));
            }
        } catch (SQLException e) {
            log.debug("reshard sync: cannot read the gate: {}", e.toString());
            return;
        }
        for (State s : states) {
            String phase = s.live() ? s.phase() : "OPEN"; // a mover that stopped renewing no longer holds anything
            String marker = s.epoch() + ":" + phase;
            if (marker.equals(applied.get(s.table()))) {
                continue;
            }
            try {
                if (apply(gate, s, phase, appliedConfigVersion, catchUpConfig)) {
                    applied.put(s.table(), marker);
                    if (s.live()) {
                        ack(s.table(), s.epoch());
                    }
                }
            } catch (SQLException | RuntimeException e) {
                log.warn("reshard sync: applying {} for {} failed: {}", phase, s.table(), e.toString());
            }
        }
    }

    private boolean apply(ReshardGate gate, State s, String phase, LongSupplier appliedConfigVersion, Runnable catchUpConfig) {
        switch (phase) {
            case "FREEZE" -> {
                List<Integer> slots = new ArrayList<>();
                for (String t : s.slots().split(",")) {
                    if (!t.isBlank()) {
                        slots.add(Integer.parseInt(t.trim()));
                    }
                }
                gate.freeze(s.table(), slots);
                if (s.blockScatter()) {
                    gate.blockScatter(s.table(), true);
                }
                return gate.awaitDrained(s.table(), 60_000) && (!s.blockScatter() || gate.awaitScatterDrained(s.table(), 60_000));
            }
            case "SCATTER" -> {
                gate.blockScatter(s.table(), true);
                return gate.awaitScatterDrained(s.table(), 60_000);
            }
            case "FLIP" -> {
                long deadline = System.currentTimeMillis() + 30_000;
                while (appliedConfigVersion.getAsLong() < s.flipVersion() && System.currentTimeMillis() < deadline) {
                    catchUpConfig.run();
                    if (appliedConfigVersion.getAsLong() < s.flipVersion()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                }
                if (appliedConfigVersion.getAsLong() < s.flipVersion()) {
                    return false; // not acknowledged: the mover will not purge while an instance still routes by the old map
                }
                gate.blockScatter(s.table(), true);
                gate.thaw(s.table());
                return true;
            }
            default -> { // OPEN
                gate.thaw(s.table());
                gate.blockScatter(s.table(), false);
                return true;
            }
        }
    }

    private void ack(String table, long epoch) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement("INSERT INTO warp_reshard_ack (table_name, instance, epoch, acked_at) VALUES (?, ?, ?, now()) "
                        + "ON CONFLICT (table_name, instance) DO UPDATE SET epoch = GREATEST(warp_reshard_ack.epoch, EXCLUDED.epoch), acked_at = now()")) {
            ps.setString(1, table.toLowerCase());
            ps.setString(2, instanceId.get());
            ps.setLong(3, epoch);
            ps.executeUpdate();
        }
    }
}
