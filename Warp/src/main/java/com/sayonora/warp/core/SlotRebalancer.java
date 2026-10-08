package com.sayonora.warp.core;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves slots of a slot-sharded table ({@code table:slots:column:N/...}, see {@link ShardingStrategy.SlotStrategy}) from the shards that own
 * them to another shard, without re-hashing the table and without pausing writes to the slots that stay.
 *
 * <p>Sequence: (1) freeze writes to the moving slots in {@link ReshardGate} and wait for statements already admitted; (2) copy the moving rows
 * from each source shard to the target (one scan per source, batches committed on the target); (3) check the target holds exactly what was
 * copied; (4) scan each source again and compare row count and a checksum with the first scan, which catches a write that slipped in; (5) switch
 * the rule to the new slot map (a new config version, applied in this process at once); (6) with scatter reads of the table held, delete the
 * moved rows from the sources; (7) release. A failure before (5) deletes what was copied to the target and changes nothing else; a failure in
 * (6) leaves duplicate rows on the sources, reported with how to finish ({@link #purge}).
 *
 * <p>Limits, by design: it needs a single live Warp instance (the hold is in this process); it copies with plain JDBC {@code getObject} /
 * {@code setObject} (LOBs as bytes/text), so identity or generated columns that refuse explicit values, and exotic types, are not supported; the
 * shard key must render to the same text the router sees (integers, strings); it holds the keys of the moving rows in memory; and writes to the
 * moving slots wait for the length of the copy and the verification scan.
 */
public final class SlotRebalancer {

    private static final Logger log = LoggerFactory.getLogger(SlotRebalancer.class);
    private static final int BATCH = 500;
    private static final AtomicBoolean RUNNING = new AtomicBoolean();

    /** Reads and replaces the {@code WARP_TABLE_SHARDS} text (the config store in production). */
    public interface SpecStore {
        String tableShards();

        void apply(String newTableShards) throws Exception;
    }

    public record Plan(String table, String to, Map<String, List<Integer>> slotsBySource, String currentParams, String newParams) {
        public int slotCount() {
            return slotsBySource.values().stream().mapToInt(List::size).sum();
        }
    }

    public record Result(String table, String to, int slotsMoved, long rowsCopied, long rowsRemovedFromSources, long writesHeldMillis,
            String newParams, String warning) {
    }

    private final BackendRegistry registry;
    private final Supplier<List<RouterStage.TableShardRule>> rules;
    private final SpecStore store;
    private final IntSupplier liveInstances;

    public SlotRebalancer(BackendRegistry registry, Supplier<List<RouterStage.TableShardRule>> rules, SpecStore store, IntSupplier liveInstances) {
        this.registry = registry;
        this.rules = rules;
        this.store = store;
        this.liveInstances = liveInstances;
    }

    private RouterStage.TableShardRule rule(String table) {
        for (RouterStage.TableShardRule r : rules.get()) {
            if (r.tableName().equalsIgnoreCase(table)) {
                return r;
            }
        }
        throw new IllegalArgumentException("no sharded table named '" + table + "'");
    }

    private ShardingStrategy.SlotStrategy slotted(RouterStage.TableShardRule r) {
        if (r.strategy() instanceof ShardingStrategy.SlotStrategy s) {
            return s;
        }
        throw new IllegalArgumentException("table '" + r.tableName() + "' uses a " + r.strategy().getClass().getSimpleName().replace("Strategy", "").toLowerCase(Locale.ROOT)
                + " strategy; only tables declared with the slots strategy can be rebalanced online");
    }

    /**
     * Chooses the slots to move to {@code to}: the given ones, or {@code count} slots taken from the shards that own the most, highest slot first.
     */
    public Plan plan(String table, String to, List<Integer> explicitSlots, Integer count) {
        RouterStage.TableShardRule rule = rule(table);
        ShardingStrategy.SlotStrategy s = slotted(rule);
        if (registry.resolveForRouting(to) == null) {
            throw new IllegalArgumentException("'" + to + "' is not a configured backend");
        }
        List<Integer> chosen = new ArrayList<>();
        if (explicitSlots != null && !explicitSlots.isEmpty()) {
            for (int slot : explicitSlots) {
                if (slot < 0 || slot >= s.slots()) {
                    throw new IllegalArgumentException("slot " + slot + " is outside 0.." + (s.slots() - 1));
                }
                if (!s.owners().get(slot).equals(to)) {
                    chosen.add(slot);
                }
            }
        } else if (count != null && count > 0) {
            Map<String, java.util.ArrayDeque<Integer>> bySource = new LinkedHashMap<>();
            for (int slot = 0; slot < s.slots(); slot++) {
                String owner = s.owners().get(slot);
                if (!owner.equals(to)) {
                    bySource.computeIfAbsent(owner, k -> new java.util.ArrayDeque<>()).addFirst(slot); // highest first
                }
            }
            while (chosen.size() < count && bySource.values().stream().anyMatch(q -> !q.isEmpty())) {
                var richest = bySource.values().stream().max(java.util.Comparator.comparingInt(java.util.ArrayDeque::size)).get();
                chosen.add(richest.pollFirst());
            }
        }
        if (chosen.isEmpty()) {
            throw new IllegalArgumentException("nothing to move: give slots, or a count, that are not already on '" + to + "'");
        }
        Map<String, List<Integer>> bySource = new LinkedHashMap<>();
        for (int slot : new TreeSet<>(chosen)) {
            bySource.computeIfAbsent(s.owners().get(slot), k -> new ArrayList<>()).add(slot);
        }
        return new Plan(rule.tableName(), to, bySource, s.toParams(), s.withOwner(chosen, to).toParams());
    }

    public Result rebalance(Plan plan, boolean allowOtherInstances) throws Exception {
        if (!allowOtherInstances) {
            int live = liveInstances.getAsInt();
            if (live > 1) {
                throw new IllegalStateException(live + " Warp instances are live: the hold on the moving slots exists only in this process, so another "
                        + "instance would keep writing to the old shard. Run with a single instance, or pass allowOtherInstances=true if the others "
                        + "do not write to this table");
            }
        }
        RouterStage.TableShardRule rule = rule(plan.table());
        ShardingStrategy.SlotStrategy before = slotted(rule);
        String originalParams = before.toParams();
        if (!RUNNING.compareAndSet(false, true)) {
            throw new IllegalStateException("another rebalance is already running in this process");
        }
        if (!originalParams.equals(plan.currentParams())) {
            RUNNING.set(false);
            throw new IllegalStateException("the slot map changed since this plan was made; plan again");
        }
        ReshardGate gate = ReshardGate.INSTANCE;
        String table = rule.tableName();
        String keyColumn = rule.column();
        BackendTarget target = registry.resolveForRouting(plan.to());
        Set<Integer> moving = new HashSet<>();
        plan.slotsBySource().values().forEach(moving::addAll);
        ShardingStrategy.SlotStrategy after = ShardingStrategy.SlotStrategy.parse(plan.newParams());
        long frozenAt = 0;
        boolean flipped = false;
        long copied = 0;
        try {
            probeTable(target, table);
            if (!before.owners().contains(plan.to()) && !before.spareMembers().contains(plan.to())) {
                // join the group first, so the table existing on the new backend is not an ambiguity for queries while the rows are copied
                store.apply(replaceEntry(store.tableShards(), table, keyColumn, before.withSpare(plan.to()).toParams()));
                before = before.withSpare(plan.to());
            }
            gate.freeze(table, moving);
            frozenAt = System.currentTimeMillis();
            if (!gate.awaitDrained(table, 60_000)) {
                throw new IllegalStateException("writes admitted before the freeze did not finish within 60 s");
            }
            if (before.owners().contains(plan.to())) {
                // The target already owns slots, so it is read by scatter queries and its copies of the moving rows would be counted twice
                // next to the originals: scatter reads of this table wait from here until the old copies are gone.
                gate.blockScatter(table, true);
                if (!gate.awaitScatterDrained(table, 60_000)) {
                    throw new IllegalStateException("scatter reads admitted before the copy did not finish within 60 s");
                }
            }
            purgeUnowned(target, plan.to(), table, keyColumn, before); // leftovers of an earlier failed attempt
            Map<String, long[]> firstScan = new LinkedHashMap<>();
            for (Map.Entry<String, List<Integer>> e : plan.slotsBySource().entrySet()) {
                BackendTarget source = registry.resolveForRouting(e.getKey());
                long[] r = copy(source, target, table, keyColumn, before, new HashSet<>(e.getValue()));
                firstScan.put(e.getKey(), r);
                copied += r[0];
            }
            long held = countOnTarget(target, table, keyColumn, before, moving);
            if (held != copied) {
                throw new IllegalStateException("verification failed: copied " + copied + " rows but the target holds " + held + " rows of the moved slots");
            }
            for (Map.Entry<String, List<Integer>> e : plan.slotsBySource().entrySet()) {
                long[] again = checksum(registry.resolveForRouting(e.getKey()), table, keyColumn, before, new HashSet<>(e.getValue()));
                long[] first = firstScan.get(e.getKey());
                if (again[0] != first[0] || again[1] != first[1]) {
                    throw new IllegalStateException("verification failed: " + e.getKey() + " changed while it was being copied (" + first[0] + " rows then "
                            + again[0] + " now); nothing was switched, retry when writes to these slots are quiet");
                }
            }
            gate.blockScatter(table, true);
            if (!gate.awaitScatterDrained(table, 60_000)) {
                throw new IllegalStateException("scatter reads admitted before the switch did not finish within 60 s");
            }
            store.apply(replaceEntry(store.tableShards(), table, keyColumn, after.toParams()));
            flipped = true;
            gate.thaw(table);
            long held1 = System.currentTimeMillis() - frozenAt;
            long removed = 0;
            String warning = null;
            try {
                for (String source : plan.slotsBySource().keySet()) {
                    removed += purgeUnowned(registry.resolveForRouting(source), source, table, keyColumn, after);
                }
            } catch (Exception e) {
                warning = "the slots moved, but removing the old copies failed (" + e.getMessage() + "); rows of the moved slots are duplicated on "
                        + plan.slotsBySource().keySet() + " until POST /api/sharding/purge {table, shard} finishes the job";
                log.error("rebalance of {}: {}", table, warning);
            } finally {
                gate.blockScatter(table, false);
            }
            log.warn("rebalance of {}: moved {} slots to {} ({} rows copied, {} removed from the sources, writes to the moving slots held {} ms)", table,
                    moving.size(), plan.to(), copied, removed, held1);
            return new Result(table, plan.to(), moving.size(), copied, removed, held1, plan.newParams(), warning);
        } catch (Exception e) {
            if (!flipped) {
                try {
                    purgeUnowned(target, plan.to(), table, keyColumn, before);
                } catch (Exception cleanup) {
                    log.error("rebalance of {}: cleaning up the target after a failure also failed: {}", table, cleanup.toString());
                }
            }
            throw e;
        } finally {
            gate.thaw(table);
            gate.blockScatter(table, false);
            RUNNING.set(false);
        }
    }

    /**
     * Makes {@code shard} a member of the table's group while it owns no slot, so schema discovery and scatter reads know about it. Routing does not
     * change. Needed once a table has been created on a new backend: until the backend belongs to the group, a query on the table is ambiguous.
     */
    public String addShard(String table, String shard) throws Exception {
        RouterStage.TableShardRule rule = rule(table);
        ShardingStrategy.SlotStrategy s = slotted(rule);
        if (registry.resolveForRouting(shard) == null) {
            throw new IllegalArgumentException("'" + shard + "' is not a configured backend");
        }
        ShardingStrategy.SlotStrategy next = s.withSpare(shard);
        if (!next.equals(s)) {
            store.apply(replaceEntry(store.tableShards(), rule.tableName(), rule.column(), next.toParams()));
        }
        return next.toParams();
    }

    /**
     * Turns a {@code hash} table into a {@code slots} table without moving a row. A hash strategy sends a key to {@code h mod N}; with a slot
     * count that is a multiple of N and slot {@code s} owned by shard {@code s mod N}, {@code (h mod slots) mod N} is the same number, so every key
     * resolves to the shard it already lives on and only the way the rule is written changes. Because routing is identical before and after, no
     * hold is needed and other Warp instances may pick the new rule up at their own pace. After this the table can be rebalanced slot by slot.
     *
     * @param slots 0 for the default (the smallest multiple of the shard count that is at least 1024); otherwise a multiple of the shard count
     */
    public String convertHashToSlots(String table, int slots) throws Exception {
        RouterStage.TableShardRule rule = rule(table);
        if (!(rule.strategy() instanceof ShardingStrategy.HashStrategy hash)) {
            throw new IllegalArgumentException("table '" + rule.tableName() + "' uses a " + rule.strategy().getClass().getSimpleName().replace("Strategy", "").toLowerCase(Locale.ROOT)
                    + " strategy; only a hash table converts without moving rows (a consistent-hash, list, range or date table would have to be reloaded)");
        }
        int n = hash.backends().size();
        int count = slots > 0 ? slots : ((1024 + n - 1) / n) * n;
        if (count % n != 0) {
            throw new IllegalArgumentException("the slot count must be a multiple of the shard count (" + n + ") so that no key changes shard; " + count + " is not");
        }
        List<String> owners = new ArrayList<>();
        for (int s = 0; s < count; s++) {
            owners.add(hash.backends().get(s % n));
        }
        ShardingStrategy.SlotStrategy converted = new ShardingStrategy.SlotStrategy(count, owners);
        for (int i = 0; i < 5000; i++) { // belt and braces: the two strategies must agree on every key
            String key = Integer.toString(i * 7919) + (i % 3 == 0 ? "x" : "");
            if (!hash.resolve(key).equals(converted.resolve(key))) {
                throw new IllegalStateException("internal check failed: key " + key + " would change shard; nothing was changed");
            }
        }
        if (!RUNNING.compareAndSet(false, true)) {
            throw new IllegalStateException("a rebalance is running in this process");
        }
        try {
            store.apply(replaceEntry(store.tableShards(), rule.tableName(), rule.column(), converted.toParams()));
        } finally {
            RUNNING.set(false);
        }
        log.warn("sharded table {} converted from hash over {} to {} slots; no rows moved", rule.tableName(), hash.backends(), count);
        return converted.toParams();
    }

    /** Deletes, from {@code shard}, every row whose key the current slot map gives to another shard. */
    public long purge(String table, String shard) throws Exception {
        RouterStage.TableShardRule rule = rule(table);
        ShardingStrategy.SlotStrategy s = slotted(rule);
        BackendTarget t = registry.resolveForRouting(shard);
        if (t == null) {
            throw new IllegalArgumentException("'" + shard + "' is not a configured backend");
        }
        if (!RUNNING.compareAndSet(false, true)) {
            throw new IllegalStateException("another rebalance is already running in this process");
        }
        try {
            ReshardGate.INSTANCE.blockScatter(rule.tableName(), true);
            return purgeUnowned(t, shard, rule.tableName(), rule.column(), s);
        } finally {
            ReshardGate.INSTANCE.blockScatter(rule.tableName(), false);
            RUNNING.set(false);
        }
    }

    // ---- the table spec ------------------------------------------------------------------------------------------------------------------

    static String replaceEntry(String spec, String table, String column, String slotParams) {
        List<String> entries = new ArrayList<>();
        boolean replaced = false;
        for (String entry : spec == null ? new String[0] : spec.split("\\|")) {
            String[] parts = entry.split(":", 4);
            if (parts.length == 4 && parts[0].trim().equalsIgnoreCase(table)) {
                entries.add(parts[0].trim() + ":slots:" + column + ":" + slotParams);
                replaced = true;
            } else if (!entry.isBlank()) {
                entries.add(entry);
            }
        }
        if (!replaced) {
            throw new IllegalStateException("table '" + table + "' is not in the table-shard rules");
        }
        return String.join("|", entries);
    }

    // ---- JDBC work -----------------------------------------------------------------------------------------------------------------------

    private static void probeTable(BackendTarget t, String table) throws SQLException {
        try (Connection c = t.open(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT * FROM " + table + " WHERE 1 = 0")) {
            rs.next();
        } catch (SQLException e) {
            throw new IllegalArgumentException("table '" + table + "' does not exist on '" + t.name() + "' (create it there first): " + e.getMessage());
        }
    }

    private static Statement streaming(Connection c, BackendTarget t) throws SQLException {
        Statement st = c.createStatement();
        st.setFetchSize(t.jdbcUrl().toLowerCase(Locale.ROOT).startsWith("jdbc:mysql") ? Integer.MIN_VALUE : 2000);
        return st;
    }

    private static Object value(ResultSet rs, int i, int type) throws SQLException {
        return switch (type) {
            case Types.BLOB, Types.LONGVARBINARY, Types.VARBINARY, Types.BINARY -> rs.getBytes(i);
            case Types.CLOB, Types.NCLOB -> rs.getString(i);
            default -> rs.getObject(i);
        };
    }

    private static int keyIndex(ResultSetMetaData md, String keyColumn) throws SQLException {
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if (md.getColumnLabel(i).equalsIgnoreCase(keyColumn)) {
                return i;
            }
        }
        throw new IllegalArgumentException("the table has no column '" + keyColumn + "'");
    }

    private static long fnv(long h, String s) {
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xFFL);
            h *= 0x100000001b3L;
        }
        return h;
    }

    private static long rowHash(Object[] values) {
        long h = 0xcbf29ce484222325L;
        for (Object v : values) {
            h = fnv(h, v instanceof byte[] b ? java.util.Arrays.toString(b) : String.valueOf(v));
            h = fnv(h, "\u0001");
        }
        return h;
    }

    /** Copies the rows of {@code slots} from {@code source} to {@code target}; returns {rows, checksum}. */
    private long[] copy(BackendTarget source, BackendTarget target, String table, String keyColumn, ShardingStrategy.SlotStrategy map, Set<Integer> slots)
            throws SQLException {
        long rows = 0;
        long sum = 0;
        try (Connection in = source.openManualCommit(); Statement st = streaming(in, source); ResultSet rs = st.executeQuery("SELECT * FROM " + table);
                Connection out = target.openManualCommit()) {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            int key = keyIndex(md, keyColumn);
            StringBuilder cols = new StringBuilder();
            StringBuilder marks = new StringBuilder();
            for (int i = 1; i <= n; i++) {
                cols.append(i == 1 ? "" : ", ").append(md.getColumnLabel(i));
                marks.append(i == 1 ? "?" : ", ?");
            }
            try (PreparedStatement ps = out.prepareStatement("INSERT INTO " + table + " (" + cols + ") VALUES (" + marks + ")")) {
                int pending = 0;
                while (rs.next()) {
                    Object k = rs.getObject(key);
                    if (k == null || !slots.contains(map.slotOf(String.valueOf(k)))) {
                        continue;
                    }
                    Object[] row = new Object[n];
                    for (int i = 1; i <= n; i++) {
                        row[i - 1] = value(rs, i, md.getColumnType(i));
                        ps.setObject(i, row[i - 1]);
                    }
                    ps.addBatch();
                    sum += rowHash(row);
                    rows++;
                    if (++pending == BATCH) {
                        ps.executeBatch();
                        out.commit();
                        pending = 0;
                    }
                }
                if (pending > 0) {
                    ps.executeBatch();
                }
                out.commit();
            } catch (SQLException e) {
                out.rollback();
                throw e;
            }
            in.rollback();
        }
        return new long[] {rows, sum};
    }

    /** {rows, checksum} of the rows of {@code slots} currently on {@code source}. */
    private long[] checksum(BackendTarget source, String table, String keyColumn, ShardingStrategy.SlotStrategy map, Set<Integer> slots) throws SQLException {
        long rows = 0;
        long sum = 0;
        try (Connection in = source.openManualCommit(); Statement st = streaming(in, source); ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            int key = keyIndex(md, keyColumn);
            while (rs.next()) {
                Object k = rs.getObject(key);
                if (k == null || !slots.contains(map.slotOf(String.valueOf(k)))) {
                    continue;
                }
                Object[] row = new Object[n];
                for (int i = 1; i <= n; i++) {
                    row[i - 1] = value(rs, i, md.getColumnType(i));
                }
                sum += rowHash(row);
                rows++;
            }
            in.rollback();
        }
        return new long[] {rows, sum};
    }

    private long countOnTarget(BackendTarget target, String table, String keyColumn, ShardingStrategy.SlotStrategy map, Set<Integer> slots) throws SQLException {
        long total = 0;
        try (Connection c = target.open(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT " + keyColumn + ", COUNT(*) FROM " + table + " GROUP BY " + keyColumn)) {
            while (rs.next()) {
                Object k = rs.getObject(1);
                if (k != null && slots.contains(map.slotOf(String.valueOf(k)))) {
                    total += rs.getLong(2);
                }
            }
        }
        return total;
    }

    /** Deletes the rows on {@code shard} whose key {@code map} gives to another shard; returns how many rows went. */
    private long purgeUnowned(BackendTarget t, String shard, String table, String keyColumn, ShardingStrategy.SlotStrategy map) throws SQLException {
        List<Object> foreign = new ArrayList<>();
        try (Connection c = t.open(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT DISTINCT " + keyColumn + " FROM " + table)) {
            while (rs.next()) {
                Object k = rs.getObject(1);
                if (k != null && !shard.equals(map.resolve(String.valueOf(k)))) {
                    foreign.add(k);
                }
            }
        }
        long deleted = 0;
        try (Connection c = t.openManualCommit()) {
            for (int from = 0; from < foreign.size(); from += BATCH) {
                List<Object> batch = foreign.subList(from, Math.min(foreign.size(), from + BATCH));
                StringBuilder marks = new StringBuilder();
                for (int i = 0; i < batch.size(); i++) {
                    marks.append(i == 0 ? "?" : ", ?");
                }
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + table + " WHERE " + keyColumn + " IN (" + marks + ")")) {
                    for (int i = 0; i < batch.size(); i++) {
                        ps.setObject(i + 1, batch.get(i));
                    }
                    deleted += ps.executeUpdate();
                }
                c.commit();
            }
        }
        return deleted;
    }

    /** Collection view used by the admin API for a table's slot ownership. */
    public Map<String, Integer> slotCounts(String table) {
        return slotted(rule(table)).slotCounts();
    }

    /** Every slot-sharded table with how many slots each shard owns. */
    public com.google.gson.JsonObject describe() {
        com.google.gson.JsonArray tables = new com.google.gson.JsonArray();
        for (RouterStage.TableShardRule r : rules.get()) {
            if (r.strategy() instanceof ShardingStrategy.SlotStrategy s) {
                com.google.gson.JsonObject t = new com.google.gson.JsonObject();
                t.addProperty("table", r.tableName());
                t.addProperty("column", r.column());
                t.addProperty("slots", s.slots());
                com.google.gson.JsonObject counts = new com.google.gson.JsonObject();
                s.slotCounts().forEach(counts::addProperty);
                t.add("slotsPerShard", counts);
                t.addProperty("map", s.toParams());
                tables.add(t);
            }
        }
        com.google.gson.JsonObject out = new com.google.gson.JsonObject();
        out.add("tables", tables);
        return out;
    }
}
