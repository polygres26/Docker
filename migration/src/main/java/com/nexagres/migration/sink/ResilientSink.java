package com.nexagres.migration.sink;

import com.nexagres.migration.checkpoint.DeadLetterStore;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Sink;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates any {@link Sink} with retry-then-dead-letter semantics -- Phase 3 of this session's
 * migration plan. Without this, a single bad {@link ChangeEvent} (a value that doesn't fit the
 * target column type, a transient network blip, a momentary firewall/QoS rejection) throws all the
 * way up through {@code Source#readPartition}/{@code streamChanges} and kills that partition's
 * worker thread or the entire live change feed -- one bad row taking down an otherwise-healthy
 * migration. This wraps the real sink (always {@link WarpGrpcSink} in production) so a
 * connector never has to implement its own retry logic.
 *
 * <p>{@link #apply} retries the single event up to {@code maxAttempts} times with a fixed short
 * backoff, then records it to {@link DeadLetterStore} and returns normally -- the event is
 * considered "handled" from the caller's point of view (logged, recorded, not silently dropped),
 * not re-thrown. {@link #applyBatch} retries the WHOLE batch first (the common case: a transient
 * failure affecting every event in flight, e.g. the target briefly unreachable, recovers on retry
 * without needing per-row isolation); only once the whole-batch retries are exhausted does it fall
 * back to replaying the batch one event at a time through {@link #apply} -- which isolates
 * whichever single event is genuininely bad from the rest of a large batch, at the cost of losing
 * the batch's own pipelining for just that one batch. Every write in this project is idempotent by
 * id, so re-applying an event that actually already succeeded during a "failed" batch attempt is a
 * harmless no-op, not a correctness risk.
 */
public final class ResilientSink implements Sink {

    private static final Logger log = LoggerFactory.getLogger(ResilientSink.class);

    private final Sink delegate;
    private final DeadLetterStore deadLetters;
    private final int maxAttempts;
    private final long retryBackoffMillis;

    public ResilientSink(Sink delegate, DeadLetterStore deadLetters, int maxAttempts, long retryBackoffMillis) {
        this.delegate = delegate;
        this.deadLetters = deadLetters;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.retryBackoffMillis = retryBackoffMillis;
    }

    @Override
    public void apply(ChangeEvent event) throws Exception {
        Exception lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                delegate.apply(event);
                return;
            } catch (Exception e) {
                lastError = e;
                log.warn("ResilientSink: attempt {}/{} failed for one event ({}) -- {}",
                        attempt, maxAttempts, event.sql(), e.getMessage());
                if (attempt < maxAttempts) {
                    sleep();
                }
            }
        }
        deadLetter(event, lastError, maxAttempts);
    }

    @Override
    public void applyBatch(List<ChangeEvent> events) throws Exception {
        if (events.isEmpty()) {
            return;
        }
        Exception lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                delegate.applyBatch(events);
                return;
            } catch (Exception e) {
                lastError = e;
                log.warn("ResilientSink: batch attempt {}/{} failed for {} event(s) -- {}",
                        attempt, maxAttempts, events.size(), e.getMessage());
                if (attempt < maxAttempts) {
                    sleep();
                }
            }
        }
        log.warn("ResilientSink: batch of {} event(s) failed all {} whole-batch attempts -- "
                + "falling back to one-at-a-time replay to isolate the actual bad event(s) "
                + "(cause: {})", events.size(), maxAttempts, lastError == null ? "unknown" : lastError.getMessage());
        List<Exception> perEventErrors = new ArrayList<>();
        for (ChangeEvent event : events) {
            try {
                apply(event); // apply() itself retries and dead-letters -- never throws on its own failure
            } catch (Exception e) {
                // apply() only throws if even RECORDING the dead letter failed (see its own
                // javadoc's "never throws... would defeat the point" reasoning) -- a real
                // infrastructure problem worth surfacing, not swallowing per-event.
                perEventErrors.add(e);
            }
        }
        if (!perEventErrors.isEmpty()) {
            throw perEventErrors.get(0);
        }
    }

    private void deadLetter(ChangeEvent event, Exception cause, int attempts) throws Exception {
        log.error("ResilientSink: exhausted all {} attempt(s) for one event ({}) -- dead-lettering, "
                + "not failing the worker", attempts, event.sql(), cause);
        deadLetters.record(event, cause == null ? null : cause.getMessage(), attempts);
    }

    private void sleep() {
        try {
            Thread.sleep(retryBackoffMillis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
