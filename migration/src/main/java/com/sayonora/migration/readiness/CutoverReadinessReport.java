package com.sayonora.migration.readiness;

import java.util.List;

/**
 * Phase 5 of this session's migration plan: "cutover-readiness tooling -- a 'readiness' check
 * surfaced as a single status." One {@link ReadinessCheck} per gate, plus the single boolean an
 * operator (or an automated cutover script) actually acts on: {@link #ready()} is {@code true}
 * only when every gate passed -- deliberately fail-closed, since the cost of cutting over a
 * connection to point at Postgres while migration is still silently behind is a real customer-
 * facing incident, and the cost of a false "not ready yet" is just re-running this check a minute
 * later.
 */
public record CutoverReadinessReport(String sourceKey, List<ReadinessCheck> checks) {

    public boolean ready() {
        return checks.stream().allMatch(ReadinessCheck::passed);
    }

    /** Plain-text rendering for a CLI or a log line -- one line per check plus the overall verdict,
     * in the order checks were evaluated. */
    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append("Cutover readiness for ").append(sourceKey).append(": ")
                .append(ready() ? "READY" : "NOT READY").append('\n');
        for (ReadinessCheck check : checks) {
            sb.append("  [").append(check.passed() ? "PASS" : "FAIL").append("] ")
                    .append(check.name()).append(" -- ").append(check.detail()).append('\n');
        }
        return sb.toString();
    }
}
