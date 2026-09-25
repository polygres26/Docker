package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure, no-DB unit coverage for {@link PartitionSpill}'s own disk round-trip -- the real
 * end-to-end proof that spilling actually engages during a real skewed join lives in {@code
 * com.sayonora.wire.mcp.ParallelJoinSpillIntegrationTest}, consistent with how the rest of this
 * package tests its own small deterministic pieces directly and leaves real-backend behavior to an
 * integration test. */
class PartitionSpillTest {

    @Test
    void roundTripsMultipleRowsUnderTheSameKeyAndPreservesMixedValueTypes() throws Exception {
        try (PartitionSpill spill = new PartitionSpill(0)) {
            spill.append(42, Arrays.asList(42, "alice", new BigDecimal("12.50"), null));
            spill.append(42, Arrays.asList(42, "alice-again", 7L, "x"));
            spill.append(7, Arrays.asList(7, "bob"));

            List<List<Object>> matches = spill.readMatches(42);
            assertEquals(2, matches.size());
            assertEquals(List.of(42, "alice", new BigDecimal("12.50")), matches.get(0).subList(0, 3));
            assertEquals(null, matches.get(0).get(3));
            assertEquals(List.of(42, "alice-again", 7L, "x"), matches.get(1));

            assertEquals(1, spill.readMatches(7).size());
            assertTrue(spill.readMatches("does-not-exist").isEmpty());
        }
    }

    @Test
    void anUnspilledKeyReturnsAnEmptyListNeverNull() throws Exception {
        try (PartitionSpill spill = new PartitionSpill(1)) {
            assertEquals(List.of(), spill.readMatches("never-appended"));
        }
    }
}
