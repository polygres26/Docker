package com.sayonora.wire.cqlwire;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cached schema. The catalog rows live on the first ("home") host; every node re-reads them after {@code ttlMs} (immediately after
 * a DDL executed through this node), so a schema change made through another Warp node becomes visible within the TTL.
 */
final class Catalog {

    private final CqlStore store;
    private final long ttlMs;
    private final AtomicLong versions = new AtomicLong();
    private volatile Schema combined;
    private volatile long loadedAt;

    Catalog(CqlStore store, long ttlMs) {
        this.store = store;
        this.ttlMs = ttlMs;
    }

    CqlStore store() {
        return store;
    }

    /** The real schema plus the virtual system keyspaces. */
    Schema schema() {
        Schema s = combined;
        if (s == null || System.currentTimeMillis() - loadedAt > ttlMs) {
            synchronized (this) {
                s = combined;
                if (s == null || System.currentTimeMillis() - loadedAt > ttlMs) {
                    List<String[]> rows = store.loadSchema(store.shards().home());
                    s = SysTables.withSystem(Schema.load(versions.incrementAndGet(), rows));
                    combined = s;
                    loadedAt = System.currentTimeMillis();
                }
            }
        }
        return s;
    }

    void invalidate() {
        combined = null;
    }

    boolean isSystem(String ks) {
        for (String k : SysTables.SYSTEM_KEYSPACES) {
            if (k.equals(ks)) {
                return true;
            }
        }
        return false;
    }
}
