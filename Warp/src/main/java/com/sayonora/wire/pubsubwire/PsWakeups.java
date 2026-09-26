package com.sayonora.wire.pubsubwire;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.Map;

/** In-process "messages arrived" signals so long polls and streams do not have to wait for their next poll tick. */
final class PsWakeups {

    private final Map<String, Object> monitors = new ConcurrentHashMap<>();
    private final Map<String, List<Runnable>> listeners = new ConcurrentHashMap<>();

    private Object monitor(String sub) {
        return monitors.computeIfAbsent(sub, k -> new Object());
    }

    void signal(String sub) {
        Object m = monitors.get(sub);
        if (m != null) {
            synchronized (m) {
                m.notifyAll();
            }
        }
        List<Runnable> l = listeners.get(sub);
        if (l != null) {
            for (Runnable r : l) {
                r.run();
            }
        }
    }

    void await(String sub, long ms) {
        Object m = monitor(sub);
        synchronized (m) {
            try {
                m.wait(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void listen(String sub, Runnable r) {
        listeners.computeIfAbsent(sub, k -> new CopyOnWriteArrayList<>()).add(r);
    }

    void unlisten(String sub, Runnable r) {
        List<Runnable> l = listeners.get(sub);
        if (l != null) {
            l.remove(r);
        }
    }
}
