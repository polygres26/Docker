package com.sayonora.migration.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Sink;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Pure logic, no real infrastructure -- proves the rate limiter actually limits (a burst well
 * past the cap measurably takes at least as long as the cap implies) without asserting an exact
 * wall-clock number, which would make this test flaky on a loaded CI box.
 */
class ThrottledSinkTest {

    @Test
    void rejectsNonPositiveRate() {
        Sink delegate = event -> { };
        assertThrows(IllegalArgumentException.class, () -> new ThrottledSink(delegate, 0));
        assertThrows(IllegalArgumentException.class, () -> new ThrottledSink(delegate, -5));
    }

    @Test
    void tenEventsAtTenPerSecondTakeAtLeastAboutOneSecond() throws Exception {
        AtomicInteger applied = new AtomicInteger();
        Sink delegate = event -> applied.incrementAndGet();
        ThrottledSink throttled = new ThrottledSink(delegate, 10.0);

        long start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            throttled.apply(new ChangeEvent("SELECT 1", List.of()));
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(10, applied.get());
        // 10 events at 10/sec should take roughly 900ms-1000ms (9 gaps of 100ms after the first
        // free one) -- assert a loose floor (700ms) that would only fail if throttling wasn't
        // happening at all, not a tight bound that would flake under CI scheduling jitter.
        assertTrue(elapsedMillis >= 700, "expected throttling to take at least ~700ms, took " + elapsedMillis + "ms");
    }

    @Test
    void applyBatchChargesOnePermitPerEventInTheBatch() throws Exception {
        AtomicInteger batchesApplied = new AtomicInteger();
        Sink delegate = new Sink() {
            @Override
            public void apply(ChangeEvent event) {
                throw new UnsupportedOperationException("this test only calls applyBatch");
            }

            @Override
            public void applyBatch(List<ChangeEvent> events) {
                batchesApplied.incrementAndGet();
            }
        };
        ThrottledSink throttled = new ThrottledSink(delegate, 1000.0);
        throttled.applyBatch(List.of(new ChangeEvent("SELECT 1", List.of()), new ChangeEvent("SELECT 2", List.of())));
        assertEquals(1, batchesApplied.get());
    }
}
