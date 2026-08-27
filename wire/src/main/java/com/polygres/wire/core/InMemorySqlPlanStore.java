package com.polygres.wire.core;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The default {@link SqlPlanStore}: a single-process, in-memory ring buffer (oldest entry evicted
 * once full) -- matches {@code V$SQL_PLAN}'s own nature (a live diagnostic view into the shared
 * pool, not a durable audit log; use real observability/logging for that). Lost on restart, same as
 * Oracle's cursor cache. Ported verbatim (module rename aside) from the sibling Omnigate project's
 * own class -- real, tested, production code there.
 *
 * <p><b>Thread-safe via a single lock</b>, not a lock-free structure -- plan capture happens once
 * per federated statement (already the expensive, multi-backend-round-trip path), so contention
 * here is never the bottleneck.
 */
final class InMemorySqlPlanStore implements SqlPlanStore {

    private final int capacity;
    private final Deque<PlanEntry> entries = new ArrayDeque<>();
    private final AtomicLong nextId = new AtomicLong(1);

    InMemorySqlPlanStore(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("InMemorySqlPlanStore capacity must be positive, got " + capacity);
        }
        this.capacity = capacity;
    }

    @Override
    public long record(String backends, String sqlText, String planText, long elapsedMillis, long rowCount,
            boolean success, String errorMessage) {
        long id = nextId.getAndIncrement();
        PlanEntry entry = new PlanEntry(id, Instant.now(), backends, sqlText, planText, elapsedMillis, rowCount,
                success, errorMessage);
        synchronized (entries) {
            if (entries.size() >= capacity) {
                entries.removeFirst();
            }
            entries.addLast(entry);
        }
        return id;
    }

    @Override
    public List<PlanEntry> snapshot() {
        synchronized (entries) {
            List<PlanEntry> list = new ArrayList<>(entries);
            Collections.reverse(list);
            return list;
        }
    }
}
