package com.sayonora.migration.verify;

/**
 * Result of comparing a source and its target: counts (a cheap, fast first check) and an
 * order-independent checksum (see {@code RowChecksum}) over every row's own id+content, which
 * catches drift a count match alone would miss (same number of rows, different content --
 * e.g. an update that never replicated, replaced by an unrelated insert count-wise).
 *
 * <p>{@link #matches()} tells you SOMETHING drifted, not WHICH row -- pinpointing the actual
 * differing row(s) needs a real per-row diff pass (fetch both sides, compare), a genuinely
 * separate, more expensive follow-up once this cheap check actually flags a real mismatch, not
 * built here. That's a deliberate scope line, not an oversight: most verification runs should
 * find zero drift, so paying the cost of a full diff pass on every run for the common case would
 * be wasted work.
 */
public record VerificationResult(long sourceCount, long targetCount, long sourceChecksum, long targetChecksum) {

    public boolean matches() {
        return sourceCount == targetCount && sourceChecksum == targetChecksum;
    }
}
