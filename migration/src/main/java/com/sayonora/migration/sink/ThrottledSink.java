package com.sayonora.migration.sink;

import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Sink;
import java.util.List;

/**
 * "Bandwidth / workload throttling" and "Source production protection" -- both packaging-table
 * rows are backed by this ONE mechanism: a token-bucket rate limiter on how many {@link
 * ChangeEvent}s per second get applied. Because every connector's own read loop calls {@code
 * sink.apply}/{@code applyBatch} SYNCHRONOUSLY from the same thread that just read that data off
 * the source (see {@code Coordinator#run} -- {@code source.readPartition(partition, sink, ...)}
 * blocks the calling worker thread until the sink accepts each row), throttling the sink also
 * throttles how fast the source itself gets read from -- there's no separate queue or buffer to
 * throttle independently. One mechanism, two honest names for what it does depending on which
 * side you're protecting.
 *
 * <p>Always wrapped in by {@code MigrationJobRunner} for every job, at a rate that's either the
 * fixed free-tier default ({@code MigrationLicensing#DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND})
 * or a caller-supplied override, which requires Enterprise (see {@code
 * MigrationLicensing#requireEnterpriseForCustomThrottle}) -- so a free-tier migration is never
 * literally unthrottled, it's throttled at a generous default nobody would notice for a normal
 * migration size, while an Enterprise customer can raise, lower, or (by passing a very large
 * value) functionally disable the cap for a workload that genuinely needs it.
 *
 * <p>Classic token-bucket via a monotonic "next free instant," not a fixed-window counter (which
 * would allow a burst right at every window boundary) -- the same style rate limiter as {@code
 * com.sayonora.wire.qos}'s own admission controller, scaled down to one dimension (events/sec, no
 * separate burst capacity) since a migration's own read loop is already naturally bursty per
 * partition/batch and doesn't need a second burst allowance layered on top.
 */
public final class ThrottledSink implements Sink {

    private final Sink delegate;
    private final double eventsPerSecond;
    private final Object lock = new Object();
    private long nextFreeNanos = Long.MIN_VALUE;

    public ThrottledSink(Sink delegate, double eventsPerSecond) {
        if (eventsPerSecond <= 0) {
            throw new IllegalArgumentException("eventsPerSecond must be positive, got " + eventsPerSecond);
        }
        this.delegate = delegate;
        this.eventsPerSecond = eventsPerSecond;
    }

    @Override
    public void apply(ChangeEvent event) throws Exception {
        awaitPermits(1);
        delegate.apply(event);
    }

    @Override
    public void applyBatch(List<ChangeEvent> events) throws Exception {
        awaitPermits(Math.max(1, events.size()));
        delegate.applyBatch(events);
    }

    private void awaitPermits(int permits) throws InterruptedException {
        long nanosPerPermit = (long) (1_000_000_000L / eventsPerSecond);
        long waitUntilNanos;
        synchronized (lock) {
            long now = System.nanoTime();
            long start = nextFreeNanos == Long.MIN_VALUE ? now : Math.max(nextFreeNanos, now);
            waitUntilNanos = start;
            nextFreeNanos = start + permits * nanosPerPermit;
        }
        long delayNanos = waitUntilNanos - System.nanoTime();
        if (delayNanos > 0) {
            java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delayNanos);
        }
    }
}
