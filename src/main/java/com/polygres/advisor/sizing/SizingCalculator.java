package com.polygres.advisor.sizing;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns whatever sizing signal is actually available ({@link SizingInput} -- schema size, and
 * workload CPU/logical-IO/physical-IO magnitudes from a live connection, or CPU/memory/data-size
 * hints an LLM pulled out of an uploaded report) into a starting-point Postgres instance shape.
 *
 * <p><b>This is deliberately a rules-of-thumb calculator, not a capacity-planning model.</b> Real
 * sizing needs a proper load test against the migrated system; what this produces is a reasonable
 * first guess to start that conversation from, with every rule's reasoning spelled out in the
 * returned {@code rationale} so it's auditable (same "don't produce a number nobody can explain"
 * posture as {@link com.polygres.advisor.score.MigrationScorer}). A missing signal degrades to a
 * documented default rather than silently guessing -- see {@code caveats}.
 *
 * <p>Ratios used below (RAM-per-vCPU, storage headroom, etc.) are common cloud-instance-shape and
 * Postgres-tuning conventions, not vendor-specific benchmarks -- revisit once real migrated
 * workloads are available to calibrate against, same caveat {@link
 * com.polygres.advisor.score.MigrationScorer}'s javadoc gives for its own weights.
 */
public final class SizingCalculator {

    private SizingCalculator() {}

    public static SizingRecommendation calculate(SizingInput in) {
        List<String> rationale = new ArrayList<>();
        List<String> caveats = new ArrayList<>();

        // --- Data volume -> storage -----------------------------------------------------------
        double dataSizeGB = in.schemaSizeBytes() > 0 ? in.schemaSizeBytes() / 1_000_000_000.0
            : (in.dataSizeGBHint() != null ? in.dataSizeGBHint() : 0);
        if (dataSizeGB <= 0) {
            dataSizeGB = 20; // floor -- enough to hold a small schema without erroring out
            caveats.add("No schema size was available (neither a live catalog measurement nor a report hint) -- "
                + "storage sizing defaulted to a 20 GB floor. Re-run once a live connection or a report with "
                + "data-volume detail is available.");
        }
        // 60% headroom: index rebuilds during migration, WAL, autovacuum bloat, and near-term growth
        // all add up fast -- sizing storage at exactly today's data size is a common under-provisioning mistake.
        int storageGB = (int) Math.ceil(Math.max(20, dataSizeGB * 1.6));
        rationale.add(String.format("Storage: %.1f GB of source data x1.6 headroom (index rebuilds, WAL, "
            + "autovacuum bloat, near-term growth) = %d GB.", dataSizeGB, storageGB));

        // --- Workload volume -> baseline vCPU tier ---------------------------------------------
        int vCpus;
        String tier;
        if (in.totalExecutions() >= 5_000_000) { vCpus = 16; tier = "XLARGE"; }
        else if (in.totalExecutions() >= 500_000) { vCpus = 8; tier = "LARGE"; }
        else if (in.totalExecutions() >= 50_000) { vCpus = 4; tier = "MEDIUM"; }
        else if (in.totalExecutions() > 0) { vCpus = 2; tier = "SMALL"; }
        else { vCpus = 2; tier = "SMALL"; caveats.add("No workload capture was available -- vCPU/memory sizing "
            + "defaulted to a SMALL baseline rather than inferring load from statement volume."); }
        rationale.add("Baseline vCPUs from captured statement volume (" + in.totalExecutions() + " executions): " + vCpus + " (" + tier + " tier).");

        if (in.cpuCoresHint() != null && in.cpuCoresHint() > vCpus) {
            rationale.add("Source system reports " + in.cpuCoresHint() + " CPU cores -- raised vCPU floor to match "
                + "rather than under-provisioning relative to the source.");
            vCpus = in.cpuCoresHint();
        }

        Double cpuBoundRatio = in.totalElapsedTimeMicros() > 0
            ? (double) in.totalCpuTimeMicros() / in.totalElapsedTimeMicros() : null;
        if (cpuBoundRatio != null && cpuBoundRatio > 0.6) {
            int bumped = (int) Math.ceil(vCpus * 1.5);
            rationale.add(String.format("Workload is CPU-bound (CPU time is %.0f%% of elapsed time) -- vCPUs raised "
                + "from %d to %d.", cpuBoundRatio * 100, vCpus, bumped));
            vCpus = bumped;
        }

        // --- Memory: cloud general-purpose shapes commonly run ~4 GB/vCPU as a starting ratio --
        int memoryGB = vCpus * 4;
        rationale.add("Baseline memory: " + vCpus + " vCPUs x 4 GB/vCPU (general-purpose cloud instance ratio) = " + memoryGB + " GB.");
        if (in.memoryGBHint() != null && in.memoryGBHint() > memoryGB) {
            rationale.add("Source system reports " + in.memoryGBHint() + " GB RAM -- raised memory floor to match.");
            memoryGB = in.memoryGBHint();
        }
        if (in.totalDiskReads() > 0 && in.totalBufferGets() > in.totalDiskReads() * 50L) {
            int bumped = (int) Math.ceil(memoryGB * 1.5);
            rationale.add("Logical reads (buffer gets) heavily outnumber physical reads (disk reads) -- this "
                + "workload is cache-friendly and benefits disproportionately from more RAM; raised from "
                + memoryGB + " to " + bumped + " GB.");
            memoryGB = bumped;
        }
        rationale.add("Postgres tuning starting point at this memory size: shared_buffers ~= 25% (" + (memoryGB / 4)
            + " GB), effective_cache_size ~= 65% (" + (int) (memoryGB * 0.65) + " GB).");

        // --- Storage IOPS ------------------------------------------------------------------------
        int storageIops;
        if (in.totalDiskReads() > 0) {
            // No fixed observation window on a V$SQL-style cursor-cache snapshot, so this is a
            // magnitude signal, not a true rate -- scaled conservatively and floored, not treated
            // as a precise IOPS measurement.
            storageIops = (int) Math.max(3000, in.totalDiskReads() / 50);
            rationale.add("Storage IOPS: scaled from " + in.totalDiskReads() + " observed physical reads (a "
                + "magnitude signal, not a measured rate -- see caveats) -> " + storageIops + " provisioned IOPS.");
            caveats.add("The captured physical-read count has no fixed time window attached (it's a point-in-time "
                + "cursor-cache/DMV snapshot, not a rate over a known interval) -- treat the IOPS figure as a "
                + "relative signal, not a measured peak. Confirm with real load testing before provisioning.");
        } else {
            storageIops = 3000;
            caveats.add("No physical-read/disk-IO signal was available -- storage IOPS defaulted to a 3,000 "
                + "baseline (a common cloud gp3-class default), not inferred from observed I/O.");
        }

        // --- Connections -------------------------------------------------------------------------
        int maxConnections = switch (tier) {
            case "XLARGE" -> 600;
            case "LARGE" -> 400;
            case "MEDIUM" -> 200;
            default -> 100;
        };
        rationale.add("max_connections starting point for " + tier + " tier: " + maxConnections
            + ". This is not derived from observed concurrent-session count (not available from either source) "
            + "-- tune against real concurrency once known; consider a connection pooler (pgbouncer) regardless "
            + "of this figure.");

        return new SizingRecommendation(tier, vCpus, memoryGB, storageGB, storageIops, maxConnections, rationale, caveats);
    }
}
