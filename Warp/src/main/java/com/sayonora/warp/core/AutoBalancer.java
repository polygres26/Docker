package com.sayonora.warp.core;

import com.google.gson.JsonObject;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the slot-sharded tables evenly filled without an operator. Every interval, on one instance only (a lease in the control plane), it counts
 * the rows of each slot table on each shard of its group; when the gap between the fullest and the emptiest shard exceeds {@code threshold} times
 * the mean, it moves slots from the fullest to the emptiest through {@link SlotRebalancer} (so the same hold, verification and cleanup apply), at
 * most {@code maxSlotsPerRun} slots per pass, only inside the allowed time window, with a cooldown after each move. A move is made only when it
 * strictly narrows the gap, so one very large key (a slot cannot be split) does not make it swing back and forth.
 *
 * <p>It balances ROW COUNTS (storage), not query load. Off unless {@code WARP_AUTO_REBALANCE=true}.
 */
public final class AutoBalancer {

    private static final Logger log = LoggerFactory.getLogger(AutoBalancer.class);

    /** Who may run the balancer: true when this instance holds the lease. */
    public interface Leader {
        boolean holds(long ttlSeconds) throws Exception;
    }

    public record Settings(long intervalSeconds, double threshold, int maxSlotsPerRun, Set<String> tables, LocalTime windowStart, LocalTime windowEnd,
            long cooldownSeconds) {
        public static Settings fromEnv(Map<String, String> env) {
            Set<String> tables = new java.util.LinkedHashSet<>();
            for (String t : env.getOrDefault("WARP_AUTO_REBALANCE_TABLES", "").split(",")) {
                if (!t.isBlank()) {
                    tables.add(t.trim().toLowerCase());
                }
            }
            LocalTime start = null;
            LocalTime end = null;
            String window = env.get("WARP_AUTO_REBALANCE_WINDOW");
            if (window != null && window.contains("-")) {
                start = LocalTime.parse(window.split("-")[0].trim());
                end = LocalTime.parse(window.split("-")[1].trim());
            }
            return new Settings(Long.parseLong(env.getOrDefault("WARP_AUTO_REBALANCE_INTERVAL_SECONDS", "300")),
                    Double.parseDouble(env.getOrDefault("WARP_AUTO_REBALANCE_THRESHOLD", "0.2")),
                    Integer.parseInt(env.getOrDefault("WARP_AUTO_REBALANCE_MAX_SLOTS_PER_RUN", "32")), tables, start, end,
                    Long.parseLong(env.getOrDefault("WARP_AUTO_REBALANCE_COOLDOWN_SECONDS", "60")));
        }

        boolean inWindow(LocalTime now) {
            if (windowStart == null || windowEnd == null) {
                return true;
            }
            return windowStart.isBefore(windowEnd)
                    ? !now.isBefore(windowStart) && now.isBefore(windowEnd)
                    : !now.isBefore(windowStart) || now.isBefore(windowEnd); // a window across midnight
        }
    }

    /** A move the balancer decided on. */
    public record Decision(String from, String to, List<Integer> slots, long rowsToMove) {
    }

    private final SlotRebalancer rebalancer;
    private final Leader leader;
    private final Settings settings;
    private final Clock clock;
    private volatile boolean enabled = true;
    private volatile String lastResult = "not run yet";
    private volatile Instant lastRun;
    private volatile Instant cooldownUntil = Instant.EPOCH;
    private ScheduledExecutorService scheduler;

    public AutoBalancer(SlotRebalancer rebalancer, Leader leader, Settings settings) {
        this(rebalancer, leader, settings, Clock.systemDefaultZone());
    }

    AutoBalancer(SlotRebalancer rebalancer, Leader leader, Settings settings, Clock clock) {
        this.rebalancer = rebalancer;
        this.leader = leader;
        this.settings = settings;
        this.clock = clock;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-auto-balancer");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                runOnce();
            } catch (Exception e) {
                log.warn("auto-rebalance: pass failed: {}", e.toString());
            }
        }, settings.intervalSeconds(), settings.intervalSeconds(), TimeUnit.SECONDS);
        log.info("auto-rebalance on: every {} s, threshold {}, up to {} slots per pass{}", settings.intervalSeconds(), settings.threshold(),
                settings.maxSlotsPerRun(), settings.windowStart() == null ? "" : ", window " + settings.windowStart() + "-" + settings.windowEnd());
    }

    public void setEnabled(boolean on) {
        this.enabled = on;
    }

    public boolean enabled() {
        return enabled;
    }

    public JsonObject status() {
        JsonObject o = new JsonObject();
        o.addProperty("enabled", enabled);
        o.addProperty("intervalSeconds", settings.intervalSeconds());
        o.addProperty("threshold", settings.threshold());
        o.addProperty("maxSlotsPerRun", settings.maxSlotsPerRun());
        o.addProperty("lastRun", lastRun == null ? null : lastRun.toString());
        o.addProperty("lastResult", lastResult);
        return o;
    }

    /** One pass over the slot tables; returns what it did (also kept as the last result). */
    public synchronized String runOnce() throws Exception {
        if (!enabled) {
            return remember("paused");
        }
        Instant now = clock.instant();
        if (now.isBefore(cooldownUntil)) {
            return remember("cooling down after the last move");
        }
        if (!settings.inWindow(LocalTime.now(clock))) {
            return remember("outside the allowed window");
        }
        if (!leader.holds(Math.max(120, settings.intervalSeconds() * 2))) {
            return remember("another instance runs the balancer");
        }
        lastRun = now;
        List<String> report = new ArrayList<>();
        for (String table : rebalancer.slotTables()) {
            if (!settings.tables().isEmpty() && !settings.tables().contains(table.toLowerCase())) {
                continue;
            }
            Map<String, Long> rows = rebalancer.shardRowCounts(table);
            Decision d = decide(rows, shard -> {
                try {
                    return rebalancer.slotRowCounts(table, shard);
                } catch (java.sql.SQLException e) {
                    throw new IllegalStateException(e);
                }
            }, settings.threshold(), settings.maxSlotsPerRun());
            if (d == null) {
                report.add(table + ": balanced " + rows);
                continue;
            }
            log.warn("auto-rebalance: {} is uneven {}; moving {} slots (~{} rows) from {} to {}", table, rows, d.slots().size(), d.rowsToMove(), d.from(), d.to());
            var plan = rebalancer.plan(table, d.to(), d.slots(), null);
            var result = rebalancer.rebalance(plan, false);
            cooldownUntil = clock.instant().plusSeconds(settings.cooldownSeconds());
            report.add(table + ": moved " + result.slotsMoved() + " slots to " + d.to() + " (" + result.rowsCopied() + " rows)");
            break; // one move per pass; the next pass measures again
        }
        return remember(String.join("; ", report));
    }

    private String remember(String text) {
        lastResult = text;
        return text;
    }

    /**
     * Pure decision: from the rows per shard, whether to move slots and which. Null when the gap between the fullest and the emptiest shard is
     * within {@code threshold} times the mean, or when no move would narrow it.
     */
    static Decision decide(Map<String, Long> rows, Function<String, Map<Integer, Long>> slotRowsOn, double threshold, int maxSlots) {
        if (rows.size() < 2) {
            return null;
        }
        long total = rows.values().stream().mapToLong(Long::longValue).sum();
        if (total == 0) {
            return null;
        }
        double mean = (double) total / rows.size();
        Map.Entry<String, Long> fullest = rows.entrySet().stream().max(Map.Entry.comparingByValue()).get();
        Map.Entry<String, Long> emptiest = rows.entrySet().stream().min(Map.Entry.comparingByValue()).get();
        long gap = fullest.getValue() - emptiest.getValue();
        if (gap <= threshold * mean) {
            return null;
        }
        long want = gap / 2;
        List<Map.Entry<Integer, Long>> slots = new ArrayList<>(slotRowsOn.apply(fullest.getKey()).entrySet());
        slots.removeIf(e -> e.getValue() <= 0);
        slots.sort(Map.Entry.<Integer, Long>comparingByValue().reversed());
        List<Integer> chosen = new ArrayList<>();
        long moved = 0;
        for (Map.Entry<Integer, Long> e : slots) {
            if (chosen.size() >= maxSlots) {
                break;
            }
            if (moved + e.getValue() <= want) {
                chosen.add(e.getKey());
                moved += e.getValue();
            }
        }
        if (chosen.isEmpty() && !slots.isEmpty()) {
            // every slot is bigger than half the gap: the smallest one, if moving it still narrows the gap
            Map.Entry<Integer, Long> smallest = slots.stream().min(Map.Entry.comparingByValue()).get();
            if (Math.abs(gap - 2 * smallest.getValue()) < gap) {
                chosen.add(smallest.getKey());
                moved = smallest.getValue();
            }
        }
        if (chosen.isEmpty() || Math.abs(gap - 2 * moved) >= gap) {
            return null;
        }
        chosen.sort(Comparator.naturalOrder());
        return new Decision(fullest.getKey(), emptiest.getKey(), chosen, moved);
    }
}
