package com.nexagres.wire.mongowire;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.nexagres.wire.migration.CdcCheckpointStore;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-time forward sync: a real MongoDB change stream on the SOURCE (legacy) deployment, applied
 * to Polywire's own {@link PostgresDocumentStore}-backed table on the TARGET Postgres -- the exact
 * physical schema (<code>"db"."collection"</code>, {@code id text PRIMARY KEY}/{@code doc jsonb})
 * mongowire itself creates and serves live traffic against, so a client already cut over to
 * mongowire and a still-running CDC worker backfilling the same collection write to the identical
 * table with no schema drift between them.
 *
 * <p><b>Snapshot-then-stream ordering</b> (the standard low-downtime-migration shape -- see the
 * session's own migration-plan discussion): the change stream is opened BEFORE the initial
 * snapshot runs, not after. A change stream's cursor establishes its own resume point at open
 * time; anything written to the source between that point and when the snapshot finishes reading
 * is guaranteed to show up again as a change event afterward, not silently missed. Every apply
 * path in this class ({@link PostgresDocumentStore#upsertOne}/{@link
 * PostgresDocumentStore#deleteById}) is idempotent by id, so re-applying an event for a document
 * the snapshot already copied is a harmless no-op, not a correctness problem or a duplicate.
 *
 * <p><b>Crash recovery</b>: the change stream's own {@code resumeToken} is persisted to a {@link
 * CdcCheckpointStore} after every applied event. Restarting with a saved checkpoint resumes the
 * stream from exactly that token instead of re-running the initial snapshot -- MongoDB's own
 * {@code resumeAfter} guarantees no gap between where the old process stopped and the new one
 * starts.
 */
public final class MongoChangeStreamCdcWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamCdcWorker.class);

    private final MongoClient sourceClient;
    private final String sourceDb;
    private final String sourceCollection;
    private final PostgresDocumentStore targetStore;
    private final String targetDb;
    private final String targetCollection;
    private final CdcCheckpointStore checkpoints;
    private final String checkpointKey;

    private final ExecutorService executor;
    private volatile MongoCursor<ChangeStreamDocument<Document>> cursor;
    private volatile boolean running;
    private volatile Throwable failure;
    private final AtomicLong eventsApplied = new AtomicLong();
    private final AtomicLong snapshotCopied = new AtomicLong();

    public MongoChangeStreamCdcWorker(MongoClient sourceClient, String sourceDb, String sourceCollection,
            PostgresDocumentStore targetStore, String targetDb, String targetCollection,
            CdcCheckpointStore checkpoints) {
        this.sourceClient = sourceClient;
        this.sourceDb = sourceDb;
        this.sourceCollection = sourceCollection;
        this.targetStore = targetStore;
        this.targetDb = targetDb;
        this.targetCollection = targetCollection;
        this.checkpoints = checkpoints;
        this.checkpointKey = "mongo:" + sourceDb + "." + sourceCollection;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "mongo-cdc-" + checkpointKey);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        running = true;
        executor.submit(this::runLoop);
    }

    private void runLoop() {
        try {
            MongoCollection<Document> src = sourceClient.getDatabase(sourceDb).getCollection(sourceCollection);
            String savedToken = checkpoints.load(checkpointKey);
            ChangeStreamIterable<Document> streamSpec = src.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
            boolean resuming = savedToken != null;
            if (resuming) {
                streamSpec = streamSpec.resumeAfter(org.bson.BsonDocument.parse(savedToken));
                log.info("mongo cdc[{}]: resuming from saved checkpoint", checkpointKey);
            } else {
                log.info("mongo cdc[{}]: no checkpoint found -- opening the change stream before the initial "
                        + "snapshot so nothing written during the snapshot is missed", checkpointKey);
            }
            try (MongoCursor<ChangeStreamDocument<Document>> c = streamSpec.iterator()) {
                cursor = c;
                if (!resuming) {
                    runInitialSnapshot(src);
                }
                while (running) {
                    ChangeStreamDocument<Document> event;
                    try {
                        event = c.tryNext();
                    } catch (MongoInterruptedException | IllegalStateException stopped) {
                        break; // cursor closed by stop() -- expected, not a real failure
                    }
                    if (event == null) {
                        Thread.sleep(50);
                        continue;
                    }
                    applyEvent(event);
                    checkpoints.save(checkpointKey, event.getResumeToken().toJson());
                    eventsApplied.incrementAndGet();
                }
            }
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (running) {
                failure = e;
                log.error("mongo cdc[{}]: worker died unexpectedly", checkpointKey, e);
            }
        }
    }

    private void runInitialSnapshot(MongoCollection<Document> src) throws SQLException {
        long copied = 0;
        try (MongoCursor<Document> c = src.find().iterator()) {
            while (c.hasNext()) {
                targetStore.upsertOne(targetDb, targetCollection, c.next());
                copied++;
            }
        }
        snapshotCopied.set(copied);
        log.info("mongo cdc[{}]: initial snapshot copied {} document(s)", checkpointKey, copied);
    }

    private void applyEvent(ChangeStreamDocument<Document> event) throws SQLException {
        switch (event.getOperationType()) {
            case INSERT, REPLACE, UPDATE -> {
                Document full = event.getFullDocument();
                if (full != null) {
                    targetStore.upsertOne(targetDb, targetCollection, full);
                } else {
                    // Only reachable if the source document was deleted before this update's
                    // lookup ran (a genuine race, not a bug) -- FullDocument.UPDATE_LOOKUP is
                    // requested above specifically so this is rare, not the normal case.
                    log.warn("mongo cdc[{}]: update event with no fullDocument available (likely deleted "
                            + "immediately after) -- skipped", checkpointKey);
                }
            }
            case DELETE -> {
                org.bson.BsonValue rawId = event.getDocumentKey().get("_id");
                targetStore.deleteById(targetDb, targetCollection, BsonJson.valueToJson(rawId));
            }
            default -> log.debug("mongo cdc[{}]: ignoring {} event (not a document write)", checkpointKey,
                    event.getOperationType());
        }
    }

    /** Total events applied via the live stream, NOT counting the initial snapshot -- see {@link
     * #snapshotCopied()} for that separately, since they're different phases with different
     * meanings (a one-time catch-up count vs. an ongoing rate). */
    public long eventsApplied() {
        return eventsApplied.get();
    }

    public long snapshotCopied() {
        return snapshotCopied.get();
    }

    /** Non-null only if the worker's background thread died from a real, unexpected exception --
     * distinct from the normal {@link #stop()} shutdown path, which never sets this. */
    public Throwable failure() {
        return failure;
    }

    public void stop() {
        running = false;
        MongoCursor<?> c = cursor;
        if (c != null) {
            c.close();
        }
        executor.shutdown();
    }

    @Override
    public void close() {
        stop();
    }
}
