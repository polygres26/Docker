package com.sayonora.migration.readiness;

/**
 * One named gate inside a {@link CutoverReadinessReport} -- e.g. "snapshot complete", "change-feed
 * lag", "dead letters". {@code detail} is always a human-readable sentence explaining WHY it
 * passed or failed (the actual counts/lag/etc.), never just a bare boolean -- the whole point of
 * this tooling is that a human (or a paged-in engineer) trusts the report enough to actually cut
 * over, and that trust needs the numbers behind each verdict, not just a checkmark.
 */
public record ReadinessCheck(String name, boolean passed, String detail) {
}
