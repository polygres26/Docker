package com.sayonora.warp.mongowire;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

/** Server-side cursors: find/aggregate/listCollections/listIndexes results paged through getMore. */
final class MongoCursors {

    private static final long IDLE_MS = 10 * 60 * 1000L;
    static final int DEFAULT_FIRST_BATCH = 101;
    private static final int MAX_BATCH_BYTES = 16 * 1024 * 1024;
    private static final Map<Long, Cursor> CURSORS = new ConcurrentHashMap<>();

    static {
        Thread reaper = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    return;
                }
                long now = System.currentTimeMillis();
                CURSORS.values().removeIf(c -> {
                    if (now - c.lastUsed > IDLE_MS) {
                        c.close();
                        return true;
                    }
                    return false;
                });
            }
        }, "mongowire-cursor-reaper");
        reaper.setDaemon(true);
        reaper.start();
    }

    private MongoCursors() {
    }

    static final class Cursor {
        final long id;
        final String ns;
        final Iterator<BsonDocument> it;
        final AutoCloseable closer;
        volatile long lastUsed = System.currentTimeMillis();
        /** Remaining documents allowed by the query's limit, or -1. */
        long remaining;

        Cursor(long id, String ns, Iterator<BsonDocument> it, AutoCloseable closer, long remaining) {
            this.id = id;
            this.ns = ns;
            this.it = it;
            this.closer = closer;
            this.remaining = remaining;
        }

        void close() {
            try {
                if (closer != null) {
                    closer.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    static int openCount() {
        return CURSORS.size();
    }

    /**
     * Builds the cursor reply for a result iterator: takes up to {@code batchSize} documents (default 101 for the
     * first batch), registers the rest of the iterator as a server cursor when more documents remain.
     */
    static BsonDocument reply(String ns, Iterator<BsonDocument> it, AutoCloseable closer, Long batchSize, long limit,
            boolean singleBatch, boolean first) {
        return reply(ns, it, closer, batchSize, limit, singleBatch, first, false);
    }

    static BsonDocument reply(String ns, Iterator<BsonDocument> it, AutoCloseable closer, Long batchSize, long limit,
            boolean singleBatch, boolean first, boolean idFirst) {
        BsonArray batch = new BsonArray();
        long max = batchSize != null ? batchSize : (first ? DEFAULT_FIRST_BATCH : Long.MAX_VALUE);
        if (limit >= 0) {
            max = Math.min(max, limit);
        }
        int bytes = 0;
        while (batch.size() < max && it.hasNext()) {
            BsonDocument d = it.next();
            batch.add(d);
            if (!first || batchSize != null && batchSize > 1000) {
                bytes += MongoBson.encode(d).length;
                if (bytes >= MAX_BATCH_BYTES - 1024) {
                    break;
                }
            }
        }
        long remainingLimit = limit >= 0 ? limit - batch.size() : -1;
        boolean full = batch.size() >= max && max != Long.MAX_VALUE;
        boolean limitReached = limit >= 0 && remainingLimit <= 0;
        boolean exhausted = limitReached || singleBatch || !full && !it.hasNext();
        long id = 0;
        if (exhausted) {
            try {
                if (closer != null) {
                    closer.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
        } else {
            id = newId();
            CURSORS.put(id, new Cursor(id, ns, it, closer, remainingLimit));
        }
        return cursorDoc(ns, id, batch, first, idFirst);
    }

    static BsonDocument cursorDoc(String ns, long id, BsonArray batch, boolean first) {
        return cursorDoc(ns, id, batch, first, false);
    }

    static BsonDocument cursorDoc(String ns, long id, BsonArray batch, boolean first, boolean idFirst) {
        BsonDocument cursor = new BsonDocument();
        if (idFirst) {
            cursor.put("id", new BsonInt64(id));
            cursor.put("ns", new BsonString(ns));
            cursor.put(first ? "firstBatch" : "nextBatch", batch);
        } else {
            cursor.put(first ? "firstBatch" : "nextBatch", batch);
            cursor.put("id", new BsonInt64(id));
            cursor.put("ns", new BsonString(ns));
        }
        BsonDocument reply = new BsonDocument();
        reply.put("cursor", cursor);
        reply.put("ok", new org.bson.BsonDouble(1.0));
        return reply;
    }

    private static long newId() {
        long id;
        do {
            id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        } while (CURSORS.containsKey(id));
        return id;
    }

    static BsonDocument getMore(long id, String coll, String db, Long batchSize) {
        Cursor c = CURSORS.get(id);
        if (c == null) {
            throw new MongoCmdException(43, "cursor id " + id + " not found");
        }
        String ns = db + "." + coll;
        if (!c.ns.equals(ns)) {
            throw new MongoCmdException(13, "Requested getMore on namespace '" + ns + "', but cursor " + id
                    + " belongs to a different namespace " + c.ns);
        }
        synchronized (c) {
            c.lastUsed = System.currentTimeMillis();
            BsonArray batch = new BsonArray();
            long max = batchSize != null ? batchSize : Long.MAX_VALUE;
            if (c.remaining >= 0) {
                max = Math.min(max, c.remaining);
            }
            int bytes = 0;
            while (batch.size() < max && c.it.hasNext()) {
                BsonDocument d = c.it.next();
                batch.add(d);
                bytes += MongoBson.encode(d).length;
                if (bytes >= MAX_BATCH_BYTES - 1024) {
                    break;
                }
            }
            if (c.remaining >= 0) {
                c.remaining -= batch.size();
            }
            boolean full = batch.size() >= max && max != Long.MAX_VALUE;
            boolean exhausted = c.remaining == 0 || !full && !c.it.hasNext();
            if (exhausted) {
                CURSORS.remove(id);
                c.close();
                return cursorDoc(ns, 0, batch, false);
            }
            return cursorDoc(ns, id, batch, false);
        }
    }

    /** @return true when the cursor existed and was killed. */
    static boolean kill(long id, String ns) {
        Cursor c = CURSORS.get(id);
        if (c == null) {
            return false;
        }
        if (ns != null && !c.ns.equals(ns)) {
            return false;
        }
        CURSORS.remove(id);
        c.close();
        return true;
    }
}
