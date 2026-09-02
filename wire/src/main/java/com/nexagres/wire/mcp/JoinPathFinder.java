package com.nexagres.wire.mcp;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Real breadth-first search over the schema's own foreign-key graph -- the {@code find_join_path}
 * MCP tool's whole point is answering "how do I actually JOIN these two tables" for a caller (an
 * SLM being trained per the data-investigation toolset this class supports, or any other MCP
 * client) that doesn't already know the schema, without it having to read every foreign key
 * itself and work out a path by hand. Edges come from {@link DataInvestigationTools#foreignKeyEdgesSql}
 * -- this class only does the graph part, dialect-agnostic once the edges are in hand. */
final class JoinPathFinder {

    record Edge(String fromTable, String fromColumn, String toTable, String toColumn) {
    }

    record Hop(String fromTable, String fromColumn, String toTable, String toColumn) {
    }

    /** BFS gives the fewest-hops path, which for a join graph is also the simplest real SQL JOIN
     * chain a caller would actually want -- more hops means more joined tables for no benefit
     * when a shorter path exists. Treats every FK edge as traversable in both directions (a
     * caller asking "orders to customers" and "customers to orders" should get the same real join
     * condition either way -- the FK's own direction doesn't change what SQL joins the two
     * tables), with the edge's OWN direction preserved in the returned hop so the caller still
     * knows which side actually holds the foreign key. */
    static List<Hop> findPath(List<Edge> edges, String fromTable, String toTable) {
        if (fromTable.equalsIgnoreCase(toTable)) {
            return List.of();
        }
        Map<String, List<Edge>> adjacency = new HashMap<>();
        for (Edge e : edges) {
            adjacency.computeIfAbsent(e.fromTable().toUpperCase(java.util.Locale.ROOT), k -> new ArrayList<>()).add(e);
            adjacency.computeIfAbsent(e.toTable().toUpperCase(java.util.Locale.ROOT), k -> new ArrayList<>()).add(e);
        }
        String start = fromTable.toUpperCase(java.util.Locale.ROOT);
        String goal = toTable.toUpperCase(java.util.Locale.ROOT);

        Map<String, Edge> cameFromEdge = new HashMap<>();
        Map<String, String> cameFromNode = new HashMap<>();
        Set<String> visited = new HashSet<>();
        ArrayDeque<String> queue = new ArrayDeque<>();
        visited.add(start);
        queue.add(start);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (current.equals(goal)) {
                return reconstruct(cameFromEdge, cameFromNode, start, goal);
            }
            for (Edge e : adjacency.getOrDefault(current, List.of())) {
                String other = e.fromTable().toUpperCase(java.util.Locale.ROOT).equals(current)
                        ? e.toTable().toUpperCase(java.util.Locale.ROOT) : e.fromTable().toUpperCase(java.util.Locale.ROOT);
                if (visited.add(other)) {
                    cameFromEdge.put(other, e);
                    cameFromNode.put(other, current);
                    queue.add(other);
                }
            }
        }
        return null; // no path found within the reachable component
    }

    private static List<Hop> reconstruct(Map<String, Edge> cameFromEdge, Map<String, String> cameFromNode,
            String start, String goal) {
        List<Hop> hops = new ArrayList<>();
        String node = goal;
        while (!node.equals(start)) {
            Edge e = cameFromEdge.get(node);
            hops.add(0, new Hop(e.fromTable(), e.fromColumn(), e.toTable(), e.toColumn()));
            node = cameFromNode.get(node);
        }
        return hops;
    }

    private JoinPathFinder() {
    }
}
