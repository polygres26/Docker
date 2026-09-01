package com.nexagres.wire.capture;

import com.nexagres.wire.core.Statement;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory, per-process ring buffer of captured statements -- no database, no disk. Each entry
 * is stamped with the wall-clock instant it was captured ({@link Instant#now()}), which is what
 * lets a multi-instance deployment's captured workload be merged back into one global arrival
 * order later (see {@code WorkloadReplayer}): every Warp instance keeps its own buffer, and
 * a replayer that pulls from all of them and sorts by that timestamp reconstructs the order
 * statements actually arrived across the whole fleet, not just within one process.
 *
 * <p>This trades the previous Postgres-table design's single authoritative {@code bigserial}
 * ordering (exact, but reflects commit order to a shared table, and costs a real DB round trip
 * per statement) for wall-clock ordering (approximate to clock sync/skew across hosts, but zero
 * I/O on the hot path, and orders by when the statement actually reached each instance rather than
 * when its capture row happened to commit). See the class javadoc on {@code WorkloadReplayer} for
 * the skew caveat in more detail.
 *
 * <p>{@code localSeq} exists purely as a tie-breaker for entries whose {@code wallClock} instant
 * is identical (same millisecond, same host) -- {@link Instant#now()}'s resolution is often
 * coarser than how fast statements can actually arrive.
 */
public final class WorkloadCaptureBuffer {

    private final String nodeId;
    private final int capacity;
    private final AtomicLong nextLocalSeq = new AtomicLong(1);
    private final Deque<Entry> ring = new ArrayDeque<>();

    public record Entry(long localSeq, Instant wallClock, String protocol, String tenantId, String sqlText,
            List<Object> bindParams, String targetBackend) {
    }

    public WorkloadCaptureBuffer(String nodeId, int capacity) {
        this.nodeId = nodeId;
        this.capacity = Math.max(1, capacity);
    }

    public String nodeId() {
        return nodeId;
    }

    public synchronized void append(Statement statement) {
        Entry entry = new Entry(nextLocalSeq.getAndIncrement(), Instant.now(), statement.sourceDialect().name(),
                statement.tenantId(), statement.sqlText(), statement.bindParams(), statement.targetBackend());
        ring.addLast(entry);
        while (ring.size() > capacity) {
            ring.removeFirst();
        }
    }

    /** Entries with {@code localSeq > sinceLocalSeq}, oldest first, capped at {@code limit}. */
    public synchronized List<Entry> since(long sinceLocalSeq, int limit) {
        List<Entry> out = new ArrayList<>();
        for (Entry e : ring) {
            if (e.localSeq() > sinceLocalSeq) {
                out.add(e);
                if (out.size() >= limit) {
                    break;
                }
            }
        }
        return out;
    }

    public synchronized int size() {
        return ring.size();
    }
}
