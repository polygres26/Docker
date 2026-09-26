package com.sayonora.warp.core;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bounded in-memory ring of policy <em>denials</em> that would otherwise only reach the process log:
 * SQL firewall blocks and connection ACL rejections. Node-local, lost on restart. Allow decisions are
 * deliberately not recorded (that would be one entry per statement); allow/deny decisions that
 * already have an audit event (access control, MCP tool calls, logins) stay in the audit log and are
 * merged with this feed by {@code GET /api/policy-decisions}.
 */
public final class PolicyDecisionLog {

    public static final int CAPACITY = 200;

    public record Decision(Instant at, String policy, String identity, String target, String decision, String reason) {
    }

    private static final PolicyDecisionLog INSTANCE = new PolicyDecisionLog();

    private final Deque<Decision> ring = new ArrayDeque<>();
    private final LongAdder total = new LongAdder();

    private PolicyDecisionLog() {
    }

    public static PolicyDecisionLog get() {
        return INSTANCE;
    }

    public void record(String policy, String identity, String target, String decision, String reason) {
        total.increment();
        Decision d = new Decision(Instant.now(), policy, identity, truncate(target), decision, reason);
        synchronized (this) {
            ring.addFirst(d);
            while (ring.size() > CAPACITY) {
                ring.removeLast();
            }
        }
    }

    /** Newest first. */
    public synchronized List<Decision> recent(int limit) {
        List<Decision> out = new ArrayList<>(Math.min(limit, ring.size()));
        for (Decision d : ring) {
            if (out.size() >= limit) {
                break;
            }
            out.add(d);
        }
        return out;
    }

    /** Denials recorded since process start (may exceed what the ring still holds). */
    public long totalRecorded() {
        return total.sum();
    }

    public synchronized void clearForTest() {
        ring.clear();
        total.reset();
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        String t = s.replaceAll("\\s+", " ").strip();
        return t.length() > 200 ? t.substring(0, 200) + "…" : t;
    }
}
