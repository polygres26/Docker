package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Pure, no-DB, no-network unit coverage for {@link RemotePartitionJoin} -- the exact matching
 * algorithm shared by Phase 0's local worker threads ({@link ParallelJoinExecutor}) and Phase 1a's
 * remote peer path ({@code grpc/WarpPeerServiceImpl.java}). Proving it here once means both callers
 * inherit the same correctness guarantee without re-testing the algorithm twice.
 */
class RemotePartitionJoinTest {

    @Test
    void probeAllEmitsOneRowPerMatchInLeftIsBuildColumnOrder() {
        List<List<Object>> buildRows = List.of(List.of(1, "alice"), List.of(2, "bob"));
        List<List<Object>> probeRows = List.of(List.of(100, 1, 50.0), List.of(101, 2, 75.0));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);

        List<List<Object>> joined = RemotePartitionJoin.probeAll(table, probeRows, 1, true);

        assertEquals(2, joined.size());
        assertTrue(joined.contains(List.of(1, "alice", 100, 1, 50.0)));
        assertTrue(joined.contains(List.of(2, "bob", 101, 2, 75.0)));
    }

    @Test
    void probeAllPutsProbeColumnsFirstWhenLeftIsBuildIsFalse() {
        List<List<Object>> buildRows = List.of(List.of(1, "alice"));
        List<List<Object>> probeRows = List.of(List.of(100, 1, 50.0));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);

        List<List<Object>> joined = RemotePartitionJoin.probeAll(table, probeRows, 1, false);

        assertEquals(1, joined.size());
        assertEquals(List.of(100, 1, 50.0, 1, "alice"), joined.get(0));
    }

    @Test
    void aBuildKeyWithMultipleRowsProducesOneJoinedRowPerDuplicate() {
        // A real hash join must preserve every build-side row sharing a key, not just the last one.
        List<List<Object>> buildRows = List.of(List.of(1, "order-a"), List.of(1, "order-b"));
        List<List<Object>> probeRows = List.of(List.of(1, "alice"));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);

        List<List<Object>> joined = RemotePartitionJoin.probeAll(table, probeRows, 0, true);

        assertEquals(2, joined.size());
    }

    @Test
    void aNullKeyOnEitherSideNeverMatchesAnything() {
        List<List<Object>> buildRows = new java.util.ArrayList<>();
        buildRows.add(java.util.Arrays.asList((Object) null, "orphan-build"));
        List<List<Object>> probeRows = new java.util.ArrayList<>();
        probeRows.add(java.util.Arrays.asList((Object) null, "orphan-probe"));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);

        assertEquals(0, table.size(), "a null-keyed build row must never even enter the hash table");
        List<List<Object>> joined = RemotePartitionJoin.probeAll(table, probeRows, 0, true);
        assertEquals(0, joined.size(), "SQL join semantics: NULL never equals NULL");
    }

    @Test
    void probeOneAppendsMatchesToAnExistingOutputListMatchingTheStreamingCallerShape() {
        List<List<Object>> buildRows = List.of(List.of(1, "alice"));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);
        List<List<Object>> output = new java.util.ArrayList<>();

        RemotePartitionJoin.probeOne(table, List.of(1, 50.0), 0, true, output);
        RemotePartitionJoin.probeOne(table, List.of(2, 75.0), 0, true, output);

        assertEquals(1, output.size(), "only the matching probe row should have produced output");
        assertEquals(List.of(1, "alice", 1, 50.0), output.get(0));
    }

    @Test
    void aNonMatchingProbeKeyProducesNoOutputRows() {
        List<List<Object>> buildRows = List.of(List.of(1, "alice"));
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, 0);
        List<List<Object>> joined = RemotePartitionJoin.probeAll(table, List.of(List.of(999, "nobody")), 0, true);
        assertEquals(0, joined.size());
    }
}
