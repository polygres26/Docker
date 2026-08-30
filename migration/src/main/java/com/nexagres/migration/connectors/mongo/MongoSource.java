package com.nexagres.migration.connectors.mongo;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.util.List;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB connector: real change streams for live sync, a real {@code find()} cursor for the
 * initial bulk read, writing through the target's real mongowire physical schema
 * (<code>"db"."collection"</code>, {@code id text}/{@code doc jsonb}) via whatever {@link Sink} is
 * given -- always {@code PolywireGrpcSink} in production, so every write actually lands through
 * Polywire's own pipeline, not a direct backdoor to the target Postgres.
 *
 * <p>No partitioning yet -- {@link #listPartitions} always returns a single, whole-collection
 * {@link Partition}. Shard-key-range partitioning (the real parallel-read win for a large
 * collection) is a real, scoped follow-up once a second connector exists to validate the
 * Partition/Source contract against something other than Mongo, per this session's own
 * migration-plan discussion.
 *
 * <p><b>Snapshot-then-stream ordering</b>, done correctly: {@link #readPartition} opens the
 * change stream cursor itself (via {@code cursor()}, not {@code iterator()} -- the driver exposes
 * a real {@code getResumeToken()} on that cursor type BEFORE a single event is consumed from it,
 * exactly for this pattern) and persists that starting token to {@code checkpoints} BEFORE running
 * the {@code find()} snapshot, then stashes the still-open, not-yet-drained cursor for {@link
 * #streamChanges} to continue from. Anything written to the source between that captured point
 * and when the snapshot finishes is guaranteed to still be sitting in the stream afterward, never
 * silently missed -- every upsert/delete this connector emits is idempotent by id, so replaying
 * one for a document the snapshot already copied is a harmless no-op. A restart with an existing
 * checkpoint skips this entirely and just resumes a fresh cursor via {@code resumeAfter}.
 */
public final class MongoSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(MongoSource.class);

    private final MongoClient sourceClient;
    private final String sourceDb;
    private final String sourceCollection;
    private final String targetDb;
    private final String targetCollection;
    private final String checkpointKey;

    private volatile MongoCursor<?> activeCursor;
    private volatile MongoChangeStreamCursor<ChangeStreamDocument<Document>> preOpenedCursor;
    private volatile boolean running = true;

    public MongoSource(MongoClient sourceClient, String sourceDb, String sourceCollection,
            String targetDb, String targetCollection) {
        this.sourceClient = sourceClient;
        this.sourceDb = sourceDb;
        this.sourceCollection = sourceCollection;
        this.targetDb = targetDb;
        this.targetCollection = targetCollection;
        this.checkpointKey = "mongo:" + sourceDb + "." + sourceCollection;
    }

    /** Matches {@code PostgresDocumentStore.ensureTable}'s own DDL exactly -- the target table
     * has to exist before the first INSERT this connector sends, and unlike mongowire's own
     * live-traffic path (which calls {@code ensureTable} on demand before every write), gRPC's
     * generic {@code QueryService.Execute} has no such hook, so this connector has to create it
     * itself, once, up front. {@code IF NOT EXISTS} makes this safe even when mongowire has
     * already created the table from prior live traffic. */
    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        sink.apply(new ChangeEvent("CREATE SCHEMA IF NOT EXISTS \"" + targetDb + "\"", List.of()));
        sink.apply(new ChangeEvent("CREATE TABLE IF NOT EXISTS " + qualifiedTargetTable()
                + " (id text PRIMARY KEY, doc jsonb NOT NULL)", List.of()));
    }

    @Override
    public List<Partition> listPartitions() {
        return List.of(new Partition(checkpointKey, null));
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        MongoCollection<Document> src = sourceClient.getDatabase(sourceDb).getCollection(sourceCollection);

        if (checkpoints.load(checkpointKey) == null) {
            // First run for this source: open the change stream and capture its starting resume
            // token BEFORE the snapshot below reads a single document -- see this class's own
            // javadoc for why the ordering matters. cursor() (not iterator()) is what exposes
            // getResumeToken() at all, but the driver only actually populates it once the
            // server's own initial batch has been fetched -- which happens lazily, on the FIRST
            // tryNext() call, not at cursor() construction time (confirmed live: getResumeToken()
            // returns null immediately after cursor() otherwise). That first tryNext() can itself
            // return a real event (a genuine concurrent write that landed in the gap) -- applied
            // here rather than discarded, so it's never lost even if this process crashed right
            // after saving the checkpoint below and a restart skipped straight to streamChanges.
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> preOpened =
                    src.watch().fullDocument(FullDocument.UPDATE_LOOKUP).cursor();
            ChangeStreamDocument<Document> immediateEvent = preOpened.tryNext();
            if (immediateEvent != null) {
                applyChangeEvent(immediateEvent, sink);
            }
            checkpoints.save(checkpointKey, preOpened.getResumeToken().toJson());
            activeCursor = preOpened;
            this.preOpenedCursor = preOpened;
            log.info("mongo source[{}]: change stream resume point captured before the snapshot starts", checkpointKey);
        } else {
            log.info("mongo source[{}]: checkpoint already exists -- skipping the snapshot, "
                    + "streamChanges will resume the stream directly", checkpointKey);
            return;
        }

        long copied = 0;
        try (MongoCursor<Document> c = src.find().iterator()) {
            while (c.hasNext()) {
                sink.apply(upsertEvent(c.next()));
                copied++;
            }
        }
        log.info("mongo source[{}]: initial snapshot copied {} document(s)", checkpointKey, copied);
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = preOpenedCursor;
        if (cursor == null) {
            // No fresh snapshot ran this process (either a restart with an existing checkpoint,
            // or readPartition was never called) -- resume a brand-new cursor from the saved
            // token instead.
            String savedToken = checkpoints.load(checkpointKey);
            ChangeStreamIterable<Document> spec = sourceClient.getDatabase(sourceDb).getCollection(sourceCollection)
                    .watch().fullDocument(FullDocument.UPDATE_LOOKUP);
            if (savedToken != null) {
                spec = spec.resumeAfter(org.bson.BsonDocument.parse(savedToken));
            }
            cursor = spec.cursor();
            activeCursor = cursor;
            log.info("mongo source[{}]: resuming change stream from saved checkpoint", checkpointKey);
        }
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> c = cursor) {
            while (running) {
                ChangeStreamDocument<Document> event;
                try {
                    event = c.tryNext();
                } catch (MongoInterruptedException | IllegalStateException stopped) {
                    break; // cursor closed by close() -- expected, not a real failure
                }
                if (event == null) {
                    Thread.sleep(50);
                    continue;
                }
                applyChangeEvent(event, sink);
                checkpoints.save(checkpointKey, event.getResumeToken().toJson());
            }
        }
    }

    private void applyChangeEvent(ChangeStreamDocument<Document> event, Sink sink) throws Exception {
        switch (event.getOperationType()) {
            case INSERT, REPLACE, UPDATE -> {
                Document full = event.getFullDocument();
                if (full != null) {
                    sink.apply(upsertEvent(full));
                } else {
                    // Only reachable if the source document was deleted before this update's
                    // lookup ran (a genuine race, not a bug) -- FullDocument.UPDATE_LOOKUP is
                    // requested above specifically so this is rare, not the normal case.
                    log.warn("mongo source[{}]: update event with no fullDocument available (likely "
                            + "deleted immediately after) -- skipped", checkpointKey);
                }
            }
            case DELETE -> {
                BsonValue rawId = event.getDocumentKey().get("_id");
                sink.apply(deleteEvent(rawId));
            }
            default -> log.debug("mongo source[{}]: ignoring {} event (not a document write)",
                    checkpointKey, event.getOperationType());
        }
    }

    private ChangeEvent upsertEvent(Document document) {
        String idJson = MongoBsonJson.valueToJson(document.get("_id"));
        String docJson = MongoBsonJson.toJson(document);
        String sql = "INSERT INTO " + qualifiedTargetTable() + " (id, doc) VALUES (?, ?::jsonb) "
                + "ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc";
        return new ChangeEvent(sql, List.of(idJson, docJson));
    }

    private ChangeEvent deleteEvent(BsonValue rawId) {
        String idJson = MongoBsonJson.valueToJson(rawId);
        return new ChangeEvent("DELETE FROM " + qualifiedTargetTable() + " WHERE id = ?", List.of(idJson));
    }

    /** Matches {@code PostgresDocumentStore.qualifiedTable}'s own convention exactly -- always
     * double-quoted, case-preserving. */
    private String qualifiedTargetTable() {
        return "\"" + targetDb + "\".\"" + targetCollection + "\"";
    }

    @Override
    public void close() {
        running = false;
        MongoCursor<?> c = activeCursor;
        if (c != null) {
            c.close();
        }
    }
}
