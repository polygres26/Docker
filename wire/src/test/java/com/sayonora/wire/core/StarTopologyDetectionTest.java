package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure, no-DB, no-Calcite unit coverage for {@link ParallelJoinPlanner#findStarHub} -- the graph
 * detection at the heart of the star-topology join port (see {@link ParallelJoinPlanner}'s own
 * star-topology section header). {@code ParallelJoinChainIntegrationTest} in {@code
 * com.sayonora.wire.mcp} already proves the real, end-to-end 3-backend case (which IS a star, since
 * every 3-leaf spanning tree necessarily is one); this test instead proves the structural detection
 * itself in isolation, including the genuinely NON-star 4-leaf shape a real integration test can't
 * exercise in this environment (the Developer-edition license caps federation at 3 backends).
 */
class StarTopologyDetectionTest {

    @Test
    void aThreeLeafSpanningTreeIsAlwaysAStarCenteredOnTheMiddleLeaf() {
        // leaf0 -- leaf1 (hub) -- leaf2: leaf1 connects to both others.
        List<ParallelJoinPlanner.LeafEdge> edges = List.of(
                new ParallelJoinPlanner.LeafEdge(1, 0, 0, 0),
                new ParallelJoinPlanner.LeafEdge(1, 1, 2, 0));
        assertEquals(1, ParallelJoinPlanner.findStarHub(3, edges));
    }

    @Test
    void aHubJoinedToTwoIndependentSpokesIsDetectedAsAStar() {
        // hub (leaf0) -- spoke1 (leaf1), hub (leaf0) -- spoke2 (leaf2): leaf0 has degree 2.
        List<ParallelJoinPlanner.LeafEdge> edges = List.of(
                new ParallelJoinPlanner.LeafEdge(0, 0, 1, 0),
                new ParallelJoinPlanner.LeafEdge(0, 1, 2, 0));
        assertEquals(0, ParallelJoinPlanner.findStarHub(3, edges));
    }

    @Test
    void aGenuineFourLeafPathIsNotAStar() {
        // A -- B -- C -- D: no single leaf connects to all 3 others (max degree is 2, not 3).
        List<ParallelJoinPlanner.LeafEdge> edges = List.of(
                new ParallelJoinPlanner.LeafEdge(0, 0, 1, 0),
                new ParallelJoinPlanner.LeafEdge(1, 1, 2, 0),
                new ParallelJoinPlanner.LeafEdge(2, 1, 3, 0));
        assertNull(ParallelJoinPlanner.findStarHub(4, edges));
    }

    @Test
    void aFourLeafStarWithThreeSpokesIsDetectedCorrectly() {
        // hub (leaf0) connects independently to leaf1, leaf2, and leaf3 -- degree 3 == leafCount-1.
        List<ParallelJoinPlanner.LeafEdge> edges = List.of(
                new ParallelJoinPlanner.LeafEdge(0, 0, 1, 0),
                new ParallelJoinPlanner.LeafEdge(0, 1, 2, 0),
                new ParallelJoinPlanner.LeafEdge(0, 2, 3, 0));
        assertEquals(0, ParallelJoinPlanner.findStarHub(4, edges));
    }
}
