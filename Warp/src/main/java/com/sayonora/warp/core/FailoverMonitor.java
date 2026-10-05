package com.sayonora.warp.core;

import com.sayonora.warp.audit.AuditEvent;
import com.sayonora.warp.audit.AuditLog;
import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Follow" failover for a backend that has read replicas: when the configured primary stops being
 * the writable node, Warp repoints that backend at the node that now IS writable -- the old primary
 * becomes a replica entry and the promoted node becomes the primary -- and records the change in
 * {@code warp_config} so every Warp instance and every restart agrees.
 *
 * <p><b>Warp does not promote anything here.</b> A promotion is done by the database's own HA
 * tooling (Patroni, repmgr, {@code pg_ctl promote}, a managed service's failover...). This monitor
 * only observes which node reports {@code pg_is_in_recovery() = false} and follows it. Promoting a
 * replica itself is a separate, riskier mode that does not exist yet.
 *
 * <p>Safety rules, all deliberate:
 * <ul>
 *   <li><b>Confirmation:</b> the primary must be non-writable (unreachable, or reachable but in
 *       recovery) for {@code WARP_FAILOVER_CONFIRM_PROBES} consecutive probes, and the node being
 *       switched to must have been writable for as many, before anything changes. One failed
 *       connect never moves traffic (unlike {@link BackendHealthChecker}'s single-failure DOWN).</li>
 *   <li><b>Exactly one writable candidate.</b> Two writable nodes while the primary is down, or a
 *       writable replica while the primary is still writable, is a suspected split brain: Warp
 *       raises it and changes nothing.</li>
 *   <li><b>Cooldown:</b> at most one switch per backend per {@code WARP_FAILOVER_COOLDOWN_SECONDS}
 *       so a flapping cluster cannot ping-pong traffic.</li>
 *   <li><b>No switch while the primary is healthy,</b> however a replica looks.</li>
 * </ul>
 *
 * <p>Every instance runs its own monitor and reaches the same decision from the same cluster
 * state. The config write is compare-and-set on the old primary URL, so concurrent identical
 * decisions collapse into one change; if the shared config cannot be written (e.g. the config
 * database is the node that died) the change is applied in memory only and re-applied across
 * reloads until the config catches up. A single decider across instances (a lease) is added with
 * promote mode, where two instances disagreeing would be dangerous rather than redundant.
 */
public final class FailoverMonitor {

    private static final Logger log = LoggerFactory.getLogger(FailoverMonitor.class);

    public enum NodeRole { WRITABLE, READ_ONLY, UNREACHABLE }

    public enum Action { NONE, SWITCH, SPLIT_BRAIN, NO_WRITABLE_NODE }

    /** What the last probes say about one node. Streaks count consecutive probes. */
    public record NodeState(String url, NodeRole role, int badStreak, int writableStreak) {
    }

    public record Decision(Action action, int targetReplicaIndex, String reason) {
        static Decision none(String reason) {
            return new Decision(Action.NONE, -1, reason);
        }
    }

    @FunctionalInterface
    public interface RoleProbe {
        NodeRole probe(BackendTarget node);
    }

    /** Records a primary change in the shared config (compare-and-set on {@code expectedOldUrl}).
     * Returns false if the config no longer names {@code expectedOldUrl} (already changed). */
    @FunctionalInterface
    public interface Persister {
        boolean persist(String backend, String expectedOldUrl, String newPrimaryUrl, List<ReplicaSpec> newReplicas)
                throws Exception;
    }

    public record Event(Instant at, String backend, String kind, String detail) {
    }

    private static final class Streak {
        int bad;
        int writable;
        NodeRole last = NodeRole.UNREACHABLE;
    }

    private final BackendRegistry registry;
    private final RoleProbe probe;
    private final Persister persister;
    private final AuditLog auditLog;
    private final LongSupplier clock;
    private final int confirmProbes;
    private final long cooldownMillis;
    private final long intervalSeconds;

    private final Map<String, Streak> streaks = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSwitchMillis = new ConcurrentHashMap<>();
    private final Map<String, String> lastDecision = new ConcurrentHashMap<>();
    private final Deque<Event> events = new ArrayDeque<>();
    private volatile ScheduledExecutorService scheduler;

    public FailoverMonitor(BackendRegistry registry, Persister persister, AuditLog auditLog) {
        this(registry, FailoverMonitor::probeRole, persister, auditLog, System::currentTimeMillis,
                (int) longEnv("WARP_FAILOVER_CONFIRM_PROBES", 3), longEnv("WARP_FAILOVER_COOLDOWN_SECONDS", 60),
                longEnv("WARP_FAILOVER_PROBE_SECONDS", 5));
    }

    FailoverMonitor(BackendRegistry registry, RoleProbe probe, Persister persister, AuditLog auditLog,
            LongSupplier clock, int confirmProbes, long cooldownSeconds, long intervalSeconds) {
        this.registry = registry;
        this.probe = probe;
        this.persister = persister;
        this.auditLog = auditLog;
        this.clock = clock;
        this.confirmProbes = Math.max(1, confirmProbes);
        this.cooldownMillis = cooldownSeconds * 1000L;
        this.intervalSeconds = intervalSeconds;
    }

    private static long longEnv(String name, long fallback) {
        try {
            String v = System.getenv(name);
            return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** The decision rule, as a pure function of what the probes saw. */
    public static Decision decide(NodeState primary, List<NodeState> replicas, int confirmProbes) {
        List<Integer> writable = new ArrayList<>();
        for (int i = 0; i < replicas.size(); i++) {
            if (replicas.get(i).role() == NodeRole.WRITABLE) {
                writable.add(i);
            }
        }
        if (primary.role() == NodeRole.WRITABLE) {
            return writable.isEmpty()
                    ? Decision.none("primary is writable")
                    : new Decision(Action.SPLIT_BRAIN, -1, "primary is writable but replica "
                            + replicas.get(writable.get(0)).url() + " is also writable -- suspected split brain");
        }
        if (primary.badStreak() < confirmProbes) {
            return Decision.none("primary not writable (" + primary.role() + ") for " + primary.badStreak() + "/"
                    + confirmProbes + " probes -- waiting to confirm");
        }
        if (writable.isEmpty()) {
            return new Decision(Action.NO_WRITABLE_NODE, -1,
                    "primary is " + primary.role() + " and no replica is writable -- nothing to follow");
        }
        if (writable.size() > 1) {
            return new Decision(Action.SPLIT_BRAIN, -1, writable.size()
                    + " replicas are writable while the primary is down -- ambiguous, not switching");
        }
        int idx = writable.get(0);
        NodeState candidate = replicas.get(idx);
        if (candidate.writableStreak() < confirmProbes) {
            return Decision.none("candidate " + candidate.url() + " writable for " + candidate.writableStreak() + "/"
                    + confirmProbes + " probes -- waiting to confirm");
        }
        return new Decision(Action.SWITCH, idx, "primary " + primary.role() + " for " + primary.badStreak()
                + " probes; " + candidate.url() + " is the only writable node");
    }

    /** Probes every follow-mode group once and acts. {@code forceNow} (manual evaluate) needs only
     * one observation instead of {@code confirmProbes}, but every other rule still applies. */
    public void evaluateOnce(boolean forceNow) {
        for (String backend : new ArrayList<>(registry.allReplicaSpecs().keySet())) {
            if (!"follow".equals(registry.failoverModeOf(backend))) {
                continue;
            }
            try {
                evaluateGroup(backend, forceNow);
            } catch (RuntimeException e) {
                log.warn("failover: evaluating '{}' failed: {}", backend, e.toString());
            }
        }
    }

    /** Evaluates a single backend now (manual trigger). Returns the decision taken. */
    public Decision evaluateNow(String backend) {
        if (!"follow".equals(registry.failoverModeOf(backend))) {
            return Decision.none("failover mode for '" + backend + "' is " + registry.failoverModeOf(backend));
        }
        return evaluateGroup(backend, true);
    }

    private Decision evaluateGroup(String backend, boolean forceNow) {
        BackendTarget primary = registry.get(backend);
        if (primary == null) {
            return Decision.none("unknown backend");
        }
        if (primary.dialect() != SourceDialect.POSTGRES) {
            note(backend, "unsupported", primary.dialect() + " backends are not monitored for failover yet");
            return Decision.none("unsupported engine");
        }
        List<ReplicaRouter.Replica> replicas = registry.replicaRouter().replicasOf(backend);
        List<BackendTarget> nodes = new ArrayList<>();
        nodes.add(primary);
        replicas.forEach(r -> nodes.add(r.target()));
        Map<String, NodeRole> roles = probeAll(nodes);

        List<NodeState> states = new ArrayList<>();
        for (BackendTarget n : nodes) {
            Streak s = streaks.computeIfAbsent(backend + "|" + n.jdbcUrl(), k -> new Streak());
            NodeRole role = roles.getOrDefault(n.jdbcUrl(), NodeRole.UNREACHABLE);
            s.last = role;
            if (role == NodeRole.WRITABLE) {
                s.writable++;
                s.bad = 0;
            } else {
                s.bad++;
                s.writable = 0;
            }
            states.add(new NodeState(n.jdbcUrl(), role, s.bad, s.writable));
        }
        int need = forceNow ? 1 : confirmProbes;
        Decision d = decide(states.get(0), states.subList(1, states.size()), need);
        String previous = lastDecision.put(backend, d.action() + ":" + d.reason());
        boolean changed = previous == null || !previous.equals(d.action() + ":" + d.reason());

        switch (d.action()) {
            case SPLIT_BRAIN -> {
                if (changed) {
                    record(backend, "split-brain-suspected", d.reason(), AuditEvent.Type.BACKEND_SPLIT_BRAIN_SUSPECTED);
                    log.error("failover: '{}': {}", backend, d.reason());
                }
            }
            case NO_WRITABLE_NODE -> {
                if (changed) {
                    record(backend, "no-writable-node", d.reason(), null);
                    log.error("failover: '{}': {}", backend, d.reason());
                }
            }
            case SWITCH -> executeSwitch(backend, primary, replicas, d);
            default -> {
            }
        }
        return d;
    }

    private void executeSwitch(String backend, BackendTarget oldPrimary, List<ReplicaRouter.Replica> replicas,
            Decision d) {
        long now = clock.getAsLong();
        Long last = lastSwitchMillis.get(backend);
        if (last != null && now - last < cooldownMillis) {
            if (!d.reason().equals(lastDecision.get(backend + "#cooldown"))) {
                lastDecision.put(backend + "#cooldown", d.reason());
                record(backend, "switch-suppressed", "cooldown (" + cooldownMillis / 1000 + "s) after the last switch; "
                        + d.reason(), null);
            }
            return;
        }
        ReplicaRouter.Replica promoted = replicas.get(d.targetReplicaIndex());
        List<ReplicaSpec> old = registry.replicaSpecsOf(backend);
        List<ReplicaSpec> next = new ArrayList<>(old);
        // The old primary takes the promoted node's slot in the replica list (same lag allowance).
        next.set(d.targetReplicaIndex(), new ReplicaSpec(oldPrimary.jdbcUrl(), old.get(d.targetReplicaIndex()).maxLagSeconds()));
        String oldUrl = oldPrimary.jdbcUrl();
        String newUrl = promoted.target().jdbcUrl();

        String how;
        try {
            boolean wrote = persister != null && persister.persist(backend, oldUrl, newUrl, next);
            how = wrote ? "recorded in warp_config" : "config already changed by another instance";
            if (!wrote && persister != null) {
                // CAS lost: the config already names a different primary. The LISTEN/NOTIFY reload
                // (or this instance's own pending reload) brings the registry in line; do not
                // touch it here.
                log.info("failover: '{}' config no longer names {} as primary -- not writing", backend, oldUrl);
            }
            if (persister == null) {
                registry.applyFailoverLocally(backend, oldUrl, newUrl, next);
                how = "applied in memory only (no config store)";
            }
        } catch (Exception e) {
            boolean applied = registry.applyFailoverLocally(backend, oldUrl, newUrl, next);
            how = "applied IN MEMORY ONLY (" + (applied ? "" : "already ") + "config write failed: " + e.getMessage()
                    + ") -- other instances decide for themselves; the config is stale until it can be written";
            log.error("failover: '{}' config write failed, switched in memory only: {}", backend, e.toString());
        }
        registry.setState(backend, BackendRegistry.BackendState.ACTIVE);
        lastSwitchMillis.put(backend, now);
        record(backend, "switched", "primary " + BackendSetModel.maskUrl(oldUrl) + " -> "
                + BackendSetModel.maskUrl(newUrl) + " (" + how + "); " + d.reason(), AuditEvent.Type.BACKEND_FAILOVER);
        log.warn("failover: '{}' now writes to {} (was {}); {}", backend, BackendSetModel.maskUrl(newUrl),
                BackendSetModel.maskUrl(oldUrl), how);
        // Close the dead primary's pool so its connections are not kept around; a no-op if absent.
        try {
            BackendConnectionPools.drain(BackendConnectionPools.poolKeyFor(oldUrl, oldPrimary.user()), 1000);
        } catch (RuntimeException e) {
            log.debug("failover: draining old primary pool failed: {}", e.toString());
        }
    }

    private Map<String, NodeRole> probeAll(List<BackendTarget> nodes) {
        Map<String, NodeRole> out = new LinkedHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(4, nodes.size()), r -> {
            Thread t = new Thread(r, "warp-failover-probe");
            t.setDaemon(true);
            return t;
        });
        try {
            Map<String, Future<NodeRole>> futures = new LinkedHashMap<>();
            for (BackendTarget n : nodes) {
                futures.put(n.jdbcUrl(), pool.submit(() -> probe.probe(n)));
            }
            for (var e : futures.entrySet()) {
                NodeRole role;
                try {
                    role = e.getValue().get(20, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    e.getValue().cancel(true);
                    role = NodeRole.UNREACHABLE;
                }
                out.put(e.getKey(), role);
            }
        } finally {
            pool.shutdownNow();
        }
        return out;
    }

    /** Postgres role probe: reachable and {@code pg_is_in_recovery() = false} means writable. */
    static NodeRole probeRole(BackendTarget node) {
        Properties props = new Properties();
        try {
            if (node.user() != null) {
                props.setProperty("user", node.user());
                String pw = SecretResolver.resolve(node.password());
                props.setProperty("password", pw == null ? "" : pw);
            }
        } catch (RuntimeException e) {
            return NodeRole.UNREACHABLE;
        }
        props.setProperty("loginTimeout", "3");
        props.setProperty("socketTimeout", "5");
        props.setProperty("connectTimeout", "3");
        try (Connection c = DriverManager.getConnection(node.jdbcUrl(), props);
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_is_in_recovery()")) {
            if (!rs.next()) {
                return NodeRole.UNREACHABLE;
            }
            return rs.getBoolean(1) ? NodeRole.READ_ONLY : NodeRole.WRITABLE;
        } catch (java.sql.SQLException e) {
            return NodeRole.UNREACHABLE;
        }
    }

    private void note(String backend, String kind, String detail) {
        String key = backend + "#" + kind;
        if (!detail.equals(lastDecision.put(key, detail))) {
            record(backend, kind, detail, null);
        }
    }

    private void record(String backend, String kind, String detail, AuditEvent.Type auditType) {
        synchronized (events) {
            events.addFirst(new Event(Instant.now(), backend, kind, detail));
            while (events.size() > 50) {
                events.removeLast();
            }
        }
        if (auditType != null && auditLog != null) {
            auditLog.record(AuditEvent.of(auditType, "warp-failover", backend + ": " + detail,
                    Map.of("backend", backend, "kind", kind)));
        }
    }

    public synchronized void start() {
        if (scheduler != null || intervalSeconds <= 0) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-failover-monitor");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                evaluateOnce(false);
            } catch (RuntimeException e) {
                log.warn("failover: monitor pass failed: {}", e.toString());
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        log.info("failover: follow-mode monitor started (probe every {}s, confirm {} probes, cooldown {}s)",
                intervalSeconds, confirmProbes, cooldownMillis / 1000);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    public List<Event> recentEvents() {
        synchronized (events) {
            return List.copyOf(events);
        }
    }

    /** Status for {@code GET /api/failover}. */
    public com.google.gson.JsonObject toJson() {
        com.google.gson.JsonObject root = new com.google.gson.JsonObject();
        root.addProperty("probeSeconds", intervalSeconds);
        root.addProperty("confirmProbes", confirmProbes);
        root.addProperty("cooldownSeconds", cooldownMillis / 1000);
        com.google.gson.JsonArray groups = new com.google.gson.JsonArray();
        for (String backend : registry.allReplicaSpecs().keySet()) {
            com.google.gson.JsonObject g = new com.google.gson.JsonObject();
            g.addProperty("backend", backend);
            g.addProperty("mode", registry.failoverModeOf(backend));
            com.google.gson.JsonArray nodes = new com.google.gson.JsonArray();
            BackendTarget primary = registry.get(backend);
            if (primary != null) {
                nodes.add(nodeJson("primary", primary.jdbcUrl(), backend));
            }
            for (ReplicaRouter.Replica r : registry.replicaRouter().replicasOf(backend)) {
                nodes.add(nodeJson("replica", r.target().jdbcUrl(), backend));
            }
            g.add("nodes", nodes);
            Long last = lastSwitchMillis.get(backend);
            g.addProperty("lastSwitchAt", last == null ? null : Instant.ofEpochMilli(last).toString());
            String d = lastDecision.get(backend);
            g.addProperty("lastDecision", d);
            groups.add(g);
        }
        root.add("groups", groups);
        com.google.gson.JsonArray ev = new com.google.gson.JsonArray();
        for (Event e : recentEvents()) {
            com.google.gson.JsonObject o = new com.google.gson.JsonObject();
            o.addProperty("at", e.at().toString());
            o.addProperty("backend", e.backend());
            o.addProperty("kind", e.kind());
            o.addProperty("detail", e.detail());
            ev.add(o);
        }
        root.add("events", ev);
        return root;
    }

    private com.google.gson.JsonObject nodeJson(String configuredRole, String url, String backend) {
        com.google.gson.JsonObject n = new com.google.gson.JsonObject();
        n.addProperty("configuredRole", configuredRole);
        n.addProperty("url", BackendSetModel.maskUrl(url));
        Streak s = streaks.get(backend + "|" + url);
        n.addProperty("observedRole", s == null ? null : s.last.name());
        n.addProperty("badStreak", s == null ? 0 : s.bad);
        n.addProperty("writableStreak", s == null ? 0 : s.writable);
        return n;
    }
}
