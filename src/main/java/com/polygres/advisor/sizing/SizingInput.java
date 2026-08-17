package com.polygres.advisor.sizing;

/**
 * Whatever sizing-relevant signal is actually available for a source database -- from a live
 * connection (schema size via {@code CatalogSnapshot.schemaSizeBytes}, CPU/IO/throughput proxies
 * via {@code WorkloadSummary}) or from an uploaded report (whatever {@code ReportAnalyzer} could
 * read off an AWR/performance-report's CPU/memory/IO section). Every field is a nullable/zero-able
 * hint, not a guaranteed measurement -- {@link SizingCalculator} is explicit in its rationale
 * about which inputs it actually had versus which it fell back to a default for.
 */
public record SizingInput(
    String sourceLabel,
    long schemaSizeBytes,
    long totalExecutions,
    long totalElapsedTimeMicros,
    long totalCpuTimeMicros,
    long totalBufferGets,
    long totalDiskReads,
    Integer cpuCoresHint,       // from a report's "CPUs" line, if present
    Integer memoryGBHint,       // from a report's "Memory(GB)" line, if present
    Double dataSizeGBHint       // from a report, when there's no live catalog to measure schema size directly
) {
    public static SizingInput empty(String sourceLabel) {
        return new SizingInput(sourceLabel, 0, 0, 0, 0, 0, 0, null, null, null);
    }
}
