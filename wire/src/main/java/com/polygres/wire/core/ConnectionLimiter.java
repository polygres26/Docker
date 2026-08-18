package com.polygres.wire.core;

import java.util.concurrent.atomic.AtomicInteger;

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

    public static int activeCount() {
        return active.get();
    }

    private ConnectionLimiter() {
    }
}
