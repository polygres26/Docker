package com.polygres.wire.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide cap on concurrent client connections, shared across all four
 * frontends (orawire, pgwire, mywire, the gRPC native-driver service) so a
 * client mixing protocols still counts against one limit — {@link
 * Edition#maxConnections()}. A no-op (always succeeds) under {@link
 * Edition#COMMERCIAL}.
 *
 * <p>Each TCP-based frontend calls {@link #tryAcquire()} right after {@code
 * accept()}, before handing the socket to a session handler, and must call
 * {@link #release()} exactly once when that session ends (see {@code
 * ProxyServer}'s accept loops). The gRPC frontend does the same per-call via
 * a {@code ServerInterceptor} ({@code ConnectionLimitInterceptor}), since a
 * unary RPC has no long-lived "connection" of its own to hook the release
 * to.
 */
public final class ConnectionLimiter {

    private static final AtomicInteger active = new AtomicInteger(0);

    public static boolean tryAcquire() {
        int max = Edition.current().maxConnections();
        while (true) {
            int current = active.get();
            if (current >= max) {
                return false;
            }
            if (active.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    public static void release() {
        active.updateAndGet(v -> Math.max(0, v - 1));
    }

    /** For {@code /api/config} and admin visibility. */
    public static int activeCount() {
        return active.get();
    }

    private ConnectionLimiter() {
    }
}
