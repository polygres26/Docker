package com.sayonora.wire.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase 1a of the "Warp-native parallel execution engine" design: the actual hash-join-a-partition
 * matching algorithm, factored out of {@link ParallelJoinExecutor}'s own per-partition worker logic
 * so BOTH the local-thread path (Phase 0, unchanged) and the new remote-peer path (Phase 1a, {@code
 * grpc/WarpPeerServiceImpl.java}) call the IDENTICAL matching code -- there is exactly one
 * implementation of "what does joining this partition's rows mean," so a partition run locally and
 * one run on a peer can never silently disagree on semantics.
 *
 * <p>Deliberately operates on plain {@code List<List<Object>>} rows and integer ordinals -- no
 * {@link Statement}, no JDBC types, no Calcite types -- since this is exactly what both a local
 * in-memory partition and a deserialized gRPC {@code JoinPartitionRequest} naturally reduce to.
 */
public final class RemotePartitionJoin {

    private RemotePartitionJoin() {
    }

    /** Builds a partition's build-side hash table: key -&gt; every build row sharing that key (a
     * real hash join must preserve duplicates, not just the last one). A {@code null} key is
     * skipped entirely -- SQL join semantics: {@code NULL} never equals {@code NULL}, so a
     * null-keyed row can never participate in a match either way. */
    public static Map<Object, List<List<Object>>> buildTable(List<List<Object>> buildRows, int buildKeyOrdinal) {
        Map<Object, List<List<Object>>> table = new HashMap<>();
        for (List<Object> row : buildRows) {
            Object key = keyOf(row, buildKeyOrdinal);
            if (key == null) {
                continue;
            }
            table.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return table;
    }

    /** Probes {@code table} with every row in {@code probeRows}, batch form -- used by the remote
     * (Phase 1a) path, which receives a whole partition's probe rows in one gRPC request rather
     * than a live stream. */
    public static List<List<Object>> probeAll(Map<Object, List<List<Object>>> table, List<List<Object>> probeRows,
            int probeKeyOrdinal, boolean leftIsBuild) {
        List<List<Object>> output = new ArrayList<>();
        for (List<Object> probeRow : probeRows) {
            probeOne(table, probeRow, probeKeyOrdinal, leftIsBuild, output);
        }
        return output;
    }

    /** Probes {@code table} with a SINGLE probe row, appending any matches to {@code output} --
     * the per-row form Phase 0's local worker thread calls as rows stream in (see {@code
     * ParallelJoinExecutor#runPartitionWorker}), so the local, pipelined path and {@link
     * #probeAll}'s batch remote path both funnel through this one real matching implementation.
     * Column order is left-then-right per the original join's own structural shape: build-then-probe
     * when the build side was structurally the join's LEFT input, probe-then-build otherwise --
     * {@code leftIsBuild} carries exactly that fact (see {@code ParallelJoinPlanner.Plan}). */
    public static void probeOne(Map<Object, List<List<Object>>> table, List<Object> probeRow, int probeKeyOrdinal,
            boolean leftIsBuild, List<List<Object>> output) {
        Object key = keyOf(probeRow, probeKeyOrdinal);
        if (key == null) {
            return;
        }
        List<List<Object>> matches = table.get(key);
        if (matches == null) {
            return;
        }
        for (List<Object> buildRow : matches) {
            List<Object> joined = new ArrayList<>(buildRow.size() + probeRow.size());
            if (leftIsBuild) {
                joined.addAll(buildRow);
                joined.addAll(probeRow);
            } else {
                joined.addAll(probeRow);
                joined.addAll(buildRow);
            }
            output.add(joined);
        }
    }

    public static Object keyOf(List<Object> row, int ordinal) {
        return ordinal >= 0 && ordinal < row.size() ? row.get(ordinal) : null;
    }
}
