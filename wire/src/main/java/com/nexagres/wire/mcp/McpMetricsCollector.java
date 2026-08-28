package com.nexagres.wire.mcp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-tool call counts, cumulative server-side time, and error counts for every MCP tool
 * PolyWire exposes -- {@code execute_sql}, {@code list_tables}, {@code describe_table}, and any
 * {@code POLYWIRE_MCP_TOOLS}-registered Postgres function/procedure. One shared instance per
 * process, fed from {@link PolyWireMcpServer}'s single {@code tools/call} dispatch point
 * ({@code handleToolsCall}) -- every tool invocation goes through there, so that's the only place
 * that needs to call {@link #record}.
 *
 * <p><b>What "server-side time" means here</b>: wall-clock time from the moment
 * {@code handleToolsCall} starts dispatching the call to the moment it finishes writing the
 * JSON-RPC response -- includes opening the backend connection, running the (possibly
 * dialect-translated) SQL, and serializing the result. It's the same "how long did PolyWire
 * itself take" framing {@link com.nexagres.wire.core.SqlMetricsCollector}'s RTT numbers use for
 * the SQL wire protocols, not network time to the MCP client.
 *
 * <p>Deliberately simpler than {@code SqlMetricsCollector}: no per-argument breakdown, no
 * normalized-SQL bucketing -- a tool name is already a small, known, bounded label (unlike raw
 * SQL text), so there's no cardinality concern requiring normalization.
 */
public final class McpMetricsCollector {

    public record ToolStat(String toolName, long calls, long errors, long totalMillis, long avgMillis) {
    }

    private static final class ToolEntry {
        final LongAdder calls = new LongAdder();
        final LongAdder errors = new LongAdder();
        final LongAdder totalNanos = new LongAdder();
    }

    private final Map<String, ToolEntry> byTool = new ConcurrentHashMap<>();

    /** Call once per {@code tools/call} dispatch, whether it succeeded or not. */
    public void record(String toolName, long elapsedNanos, boolean isError) {
        ToolEntry entry = byTool.computeIfAbsent(toolName, k -> new ToolEntry());
        entry.calls.increment();
        entry.totalNanos.add(elapsedNanos);
        if (isError) {
            entry.errors.increment();
        }
    }

    /** Snapshot sorted by call count descending -- the tools actually getting used lead the list,
     * same "most active first" convention {@code SqlMetricsCollector}'s top-SQL list uses. */
    public List<ToolStat> snapshot() {
        List<ToolStat> stats = new ArrayList<>();
        byTool.forEach((toolName, entry) -> {
            long calls = entry.calls.sum();
            long totalNanos = entry.totalNanos.sum();
            long totalMillis = totalNanos / 1_000_000;
            long avgMillis = calls == 0 ? 0 : totalMillis / calls;
            stats.add(new ToolStat(toolName, calls, entry.errors.sum(), totalMillis, avgMillis));
        });
        stats.sort(Comparator.comparingLong(ToolStat::calls).reversed());
        return stats;
    }
}
