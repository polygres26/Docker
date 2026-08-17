package com.polygres.advisor.workload;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Roll-up over a {@link CapturedStatement} snapshot -- the "summary in the lingo of the data
 * source" the UI's Workload tab leads with, before the full per-statement table. Terminology
 * mirrors what an Oracle DBA would see in an AWR/Statspack report (Elapsed Time, CPU Time, Buffer
 * Gets, Disk Reads, Executions) rather than translating into generic APM vocabulary -- the
 * audience for this page already knows what these mean.
 */
public record WorkloadSummary(
    int distinctStatements,
    long totalExecutions,
    long totalElapsedTimeMicros,
    long totalCpuTimeMicros,
    long totalBufferGets,
    long totalDiskReads,
    /** Module name -> statement count, ranked, capped at 8 -- "where is this workload coming from." */
    Map<String, Long> topModules,
    /** The single most expensive statement by total elapsed time -- what an AWR "Top SQL" section leads with. */
    CapturedStatement topByElapsedTime
) {
    public static WorkloadSummary summarize(List<CapturedStatement> statements) {
        long totalExec = 0, totalElapsed = 0, totalCpu = 0, totalBufferGets = 0, totalDiskReads = 0;
        Map<String, Long> moduleCounts = new LinkedHashMap<>();
        CapturedStatement top = null;

        for (CapturedStatement s : statements) {
            totalExec += s.executions();
            totalElapsed += s.elapsedTimeMicros();
            totalCpu += s.cpuTimeMicros();
            totalBufferGets += s.bufferGets();
            totalDiskReads += s.diskReads();
            String module = (s.module() == null || s.module().isBlank()) ? "(none)" : s.module();
            moduleCounts.merge(module, 1L, Long::sum);
            if (top == null || s.elapsedTimeMicros() > top.elapsedTimeMicros()) {
                top = s;
            }
        }

        Map<String, Long> topModules = moduleCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(8)
            .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        return new WorkloadSummary(statements.size(), totalExec, totalElapsed, totalCpu,
            totalBufferGets, totalDiskReads, topModules, top);
    }
}
