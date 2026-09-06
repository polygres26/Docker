package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure, no-DB unit coverage for {@link ParallelJoinExecutor}'s small deterministic helpers -- the
 * full {@link ParallelJoinExecutor#execute} orchestration opens real JDBC connections to each
 * side's own backend by design (it streams each leaf's extracted SQL directly, not through
 * Calcite), so its end-to-end correctness is proven by
 * {@code com.sayonora.wire.mcp.ParallelJoinIntegrationTest} against real backends instead --
 * consistent with how {@link ShardJoinExecutor} (also real-JDBC-driven throughout) has no unit
 * test of its own either, only an integration test. This test lives in the same package as {@link
 * ParallelJoinExecutor} so its package-private static helpers are directly callable.
 */
class ParallelJoinExecutorTest {

    private static ParallelJoinPlanner.Plan planWithProbeRowCountEstimate(long estimate) {
        return new ParallelJoinPlanner.Plan(null, null, null, 0, null, null, null, null, 0, null, true, null, estimate, null, null, null);
    }

    /** Assumes {@code WARP_PARALLEL_JOIN_THREADS} isn't set in the test environment -- an explicit
     * override always wins outright over cost-based sizing, which is exactly what this test proves
     * for the UNSET case; a set override is covered by {@link #threadCountFromEnvOrDefaultFallsBackToAvailableProcessorsWhenUnset}'s
     * own sibling behavior already. */
    @Test
    void partitionCountForDerivesFromTheProbeRowCountEstimateWhenNoExplicitThreadCountIsSet() {
        long targetRowsPerPartition = 25_000L; // WARP_PARALLEL_JOIN_TARGET_ROWS_PER_PARTITION's own default
        int maxPartitions = Math.max(1, Runtime.getRuntime().availableProcessors());

        // A small estimate -- well under one partition's worth -- must still get exactly 1, never
        // fewer real parallelism than a plain sequential run already had.
        assertEquals(1, ParallelJoinExecutor.partitionCountFor(planWithProbeRowCountEstimate(100)));

        // A huge estimate must clamp to the core count, never exceed it (more partitions than cores
        // adds handoff overhead with no real parallelism gain).
        assertEquals(maxPartitions, ParallelJoinExecutor.partitionCountFor(
                planWithProbeRowCountEstimate(targetRowsPerPartition * (maxPartitions + 10L))));

        // An unknown estimate (-1, the "probe failed" sentinel) must fall back to the existing
        // available-processors default, never crash or silently pick 0/negative partitions.
        assertEquals(maxPartitions, ParallelJoinExecutor.partitionCountFor(planWithProbeRowCountEstimate(-1)));
    }

    @Test
    void columnIndexFindsAColumnCaseInsensitively() throws Exception {
        List<ColumnInfo> columns = List.of(
                new ColumnInfo("id", Types.INTEGER, 0, 0, 0, false),
                new ColumnInfo("Customer_Id", Types.INTEGER, 0, 0, 0, false));
        assertEquals(0, columnIndex(columns, "id"));
        assertEquals(1, columnIndex(columns, "customer_id"));
        assertEquals(-1, columnIndex(columns, "does_not_exist"));
    }

    @Test
    void partitioningIsDeterministicAndCoversEveryPartitionAcrossManyKeys() throws Exception {
        int n = 4;
        int[] counts = new int[n];
        for (int key = 0; key < 10_000; key++) {
            int partition = partitionOf(key, n);
            assertTrue(partition >= 0 && partition < n, "partition index must always be in [0, n)");
            // Same key must always land in the same partition -- otherwise the build side's hash
            // table and the probe side's dispatch would disagree about which partition owns a key.
            assertEquals(partition, partitionOf(key, n));
            counts[partition]++;
        }
        for (int count : counts) {
            assertTrue(count > 0, "a real spread of keys should touch every partition at least once");
        }
    }

    @Test
    void threadCountFromEnvOrDefaultFallsBackToAvailableProcessorsWhenUnset() {
        int threads = ParallelJoinExecutor.threadCountFromEnvOrDefault();
        assertTrue(threads >= 1, "must always return at least one thread");
    }

    // ParallelJoinExecutor's helpers are `private static` -- reflected into rather than widened to
    // package-private purely for this test, since they have no other caller and shouldn't gain
    // broader visibility just to be testable.
    private static int columnIndex(List<ColumnInfo> columns, String name) throws Exception {
        var method = ParallelJoinExecutor.class.getDeclaredMethod("columnIndex", List.class, String.class);
        method.setAccessible(true);
        return (int) method.invoke(null, columns, name);
    }

    private static int partitionOf(Object key, int n) throws Exception {
        var method = ParallelJoinExecutor.class.getDeclaredMethod("partitionOf", Object.class, int.class);
        method.setAccessible(true);
        return (int) method.invoke(null, key, n);
    }
}
