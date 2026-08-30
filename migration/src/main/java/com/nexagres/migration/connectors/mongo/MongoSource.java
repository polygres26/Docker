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
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB connector: real change streams for live sync, a real {@code find()} cursor for the
 * initial bulk read, writing through the target's real mongowire physical schema
 * (<code>"db"."collection"</code>, {@code id text}/{@code doc jsonb}) via whatever {@link Sink} is
 * given -- always {@code PolywireGrpcSink} in production, so every write actually lands through
 * Polywire's own pipeline, not a direct backdoor to the target Postgres.
 *
 * <p><b>Partitioning</b>: {@link #listPartitions} splits the collection into {@code
 * partitionCount} hash buckets of a configurable {@code shardKeyField} (server-side, via {@code
 * $toHashedIndexKey} in a {@code $expr} match -- real filtering the database does, not a
 * client-side post-filter), so the initial snapshot's {@code partitionCount} parallel workers each
 * read a genuinely disjoint slice. Defaults to {@code partitionCount = 1} / {@code shardKeyField =
 * "_id"} (a single whole-collection partition), matching v1's original behavior exactly when a
 * caller doesn't ask for more. Deliberately keyed off the SAME field Polywire's own
 * {@code TableShardRule} would shard the target table on when one is configured (see this
 * session's own migration-plan discussion): partitioning the source read by the target's shard key
 * means each parallel worker's writes concentrate on the shard {@link
 * com.nexagres.migration.sink.PolywireGrpcSink}'s underlying {@code RouterStage} would route them
 * to anyway, instead of every worker round-robining writes across every shard's connections.
 *
 * <p>Each partition's own progress is checkpointed independently (key {@code
 * "<source>#p<bucket>"}, value {@code PARTITION_DONE} once fully copied) so a restart mid-migration skips
 * partitions already finished rather than re-reading the whole collection -- distinct from the
 * SINGLE collection-wide change-feed checkpoint (key {@code "<source>"}) {@link
 * #prepareChangeFeed}/{@link #streamChanges} manage, since there is only ever one change stream
 * regardless of how many snapshot partitions there are.
 *
 * <p><b>Snapshot-then-stream ordering</b>, done correctly: {@link #prepareChangeFeed} -- called by
 * {@link com.nexagres.migration.coordinator.Coordinator} once, before any partition is read, not
 * per partition -- opens the change stream cursor itself (via {@code cursor()}, not {@code
 * iterator()} -- the driver exposes a real {@code getResumeToken()} on that cursor type BEFORE a
 * single event is consumed from it, exactly for this pattern) and persists that starting token to
 * {@code checkpoints} BEFORE any partition's {@code find()} snapshot runs, then stashes the still-
 * open, not-yet-drained cursor for {@link #streamChanges} to continue from. Anything written to
 * the source between that captured point and when every partition's snapshot finishes is
 * guaranteed to still be sitting in the stream afterward, never silently missed -- every
 * upsert/delete this connector emits is idempotent by id, so replaying one for a document a
 * snapshot already copied is a harmless no-op. A restart with an existing checkpoint skips this
 * entirely and just resumes a fresh cursor via {@code resumeAfter}.
 */
public final class MongoSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(MongoSource.class);

    /** Snapshot rows batched per {@code Sink#applyBatch} call -- large enough to actually pipeline
     * meaningfully many gRPC round trips at once, small enough that one partition's progress
     * checkpoint (see {@link #readPartition}) never falls too far behind what's truly landed. */
    private static final int SNAPSHOT_BATCH_SIZE = 500;

    /** {@link com.nexagres.migration.checkpoint.CdcCheckpointStore#save} casts its value with
     * {@code ::jsonb} -- a bare {@code DONE} isn't valid JSON, so the "fully copied" sentinel has
     * to be a real JSON string literal (quotes included), not the raw token text. Caught live:
     * the original bare-{@code DONE} version failed every partition-checkpoint save with a real
     * Postgres {@code invalid input syntax for type json} error. */
    private static final String PARTITION_DONE = "\"DONE\"";

    private final MongoClient sourceClient;
    private final String sourceDb;
    private final String sourceCollection;
    private final String targetDb;
    private final String targetCollection;
    private final String checkpointKey;
    private final int partitionCount;
    private final String shardKeyField;

    private volatile MongoCursor<?> activeCursor;
    private volatile MongoChangeStreamCursor<ChangeStreamDocument<Document>> preOpenedCursor;
    private volatile boolean running = true;

    public MongoSource(MongoClient sourceClient, String sourceDb, String sourceCollection,
            String targetDb, String targetCollection) {
        this(sourceClient, sourceDb, sourceCollection, targetDb, targetCollection, 1, "_id");
    }

    /** @param partitionCount how many hash buckets to split the initial snapshot into for
     *     parallel reads -- 1 keeps the original single-partition behavior.
     * @param shardKeyField the field hashed to assign a document to a bucket -- ideally the same
     *     column the TARGET table is (or will be) sharded on in Polywire's own {@code
     *     TableShardRule} config, so each partition's writes land on one shard rather than
     *     scattering across all of them; {@code "_id"} is a safe, always-present default when the
     *     target isn't sharded at all. */
    public MongoSource(MongoClient sourceClient, String sourceDb, String sourceCollection,
            String targetDb, String targetCollection, int partitionCount, String shardKeyField) {
        this.sourceClient = sourceClient;
        this.sourceDb = sourceDb;
        this.sourceCollection = sourceCollection;
        this.targetDb = targetDb;
        this.targetCollection = targetCollection;
        this.checkpointKey = "mongo:" + sourceDb + "." + sourceCollection;
        this.partitionCount = Math.max(1, partitionCount);
        this.shardKeyField = shardKeyField;
    }

    /** Matches {@code PostgresDocumentStore.ensureTable}'s own DDL exactly -- the target table
     * has to exist before the first INSERT this connector sends, and unlike mongowire's own
     * live-traffic path (which calls {@code ensureTable} on demand before every write), gRPC's
     * generic {@code QueryService.Execute} has no such hook, so this connector has to create it
     * itself, once, up front. {@code IF NOT EXISTS} makes this safe even when mongowire has
     * already created the table from prior live traffic.
     *
     * <p>{@code IF NOT EXISTS} is NOT, on its own, safe against two callers racing to create the
     * SAME object concurrently -- confirmed live under {@code DistributedCoordinator} (every
     * worker process calls this): Postgres's existence check and the actual create aren't atomic
     * against each other, so two concurrent {@code CREATE TABLE IF NOT EXISTS}/{@code CREATE
     * SCHEMA IF NOT EXISTS} calls for an object neither has created yet can both pass the check
     * and then both attempt the create, and the loser gets a real {@code 23505 unique_violation}
     * (on {@code pg_type}/{@code pg_namespace}), not a graceful no-op. Since the intent here is
     * purely "make sure it exists," swallowing that specific race is correct, not papering over a
     * real bug: by the time this call returns with that error, the object DOES exist -- another
     * worker just won the race to create it. */
    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        applyTolerantOfConcurrentCreateRace(sink, "CREATE SCHEMA IF NOT EXISTS \"" + targetDb + "\"");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS " + qualifiedTargetTable()
                + " (id text PRIMARY KEY, doc jsonb NOT NULL)");
    }

    private static void applyTolerantOfConcurrentCreateRace(Sink sink, String ddl) throws Exception {
        try {
            sink.apply(new ChangeEvent(ddl, List.of()));
        } catch (java.sql.SQLException e) {
            if (!"23505".equals(e.getSQLState())) {
                throw e;
            }
            log.info("ensureTargetSchema: lost a benign concurrent CREATE race to another worker "
                    + "(23505 on the object catalog) -- the object exists either way, continuing");
        }
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) != null) {
            log.info("mongo source[{}]: change-feed checkpoint already exists -- streamChanges "
                    + "will resume the stream directly, no new resume point needed", checkpointKey);
            return;
        }
        MongoCollection<Document> src = sourceClient.getDatabase(sourceDb).getCollection(sourceCollection);
        // Open the change stream and capture its starting resume token BEFORE any partition's
        // snapshot below reads a single document -- see this class's own javadoc for why the
        // ordering matters. cursor() (not iterator()) is what exposes getResumeToken() at all, but
        // the driver only actually populates it once the server's own initial batch has been
        // fetched -- which happens lazily, on the FIRST tryNext() call, not at cursor()
        // construction time (confirmed live: getResumeToken() returns null immediately after
        // cursor() otherwise). That first tryNext() can itself return a real event (a genuine
        // concurrent write that landed in the gap) -- applied here rather than discarded, so it's
        // never lost even if this process crashed right after saving the checkpoint below and a
        // restart skipped straight to streamChanges.
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> preOpened =
                src.watch().fullDocument(FullDocument.UPDATE_LOOKUP).cursor();
        ChangeStreamDocument<Document> immediateEvent = preOpened.tryNext();
        if (immediateEvent != null) {
            applyChangeEvent(immediateEvent, sink);
            checkpoints.save(checkpointKey, preOpened.getResumeToken().toJson(), eventTimestamp(immediateEvent));
        } else {
            checkpoints.save(checkpointKey, preOpened.getResumeToken().toJson());
        }
        activeCursor = preOpened;
        this.preOpenedCursor = preOpened;
        log.info("mongo source[{}]: change stream resume point captured before any partition's snapshot starts", checkpointKey);
    }

    @Override
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<>(partitionCount);
        for (int bucket = 0; bucket < partitionCount; bucket++) {
            partitions.add(new Partition(checkpointKey + "#p" + bucket, bucket));
        }
        return partitions;
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        String partitionCheckpointKey = partition.id();
        if (PARTITION_DONE.equals(checkpoints.load(partitionCheckpointKey))) {
            log.info("mongo source[{}]: partition already fully copied -- skipping", partitionCheckpointKey);
            return;
        }

        MongoCollection<Document> src = sourceClient.getDatabase(sourceDb).getCollection(sourceCollection);
        int bucket = (Integer) partition.descriptor();
        Bson filter = bucketFilter(bucket);

        long copied = 0;
        List<ChangeEvent> batch = new ArrayList<>(SNAPSHOT_BATCH_SIZE);
        try (MongoCursor<Document> c = filter == null ? src.find().iterator() : src.find(filter).iterator()) {
            while (c.hasNext()) {
                batch.add(upsertEvent(c.next()));
                if (batch.size() >= SNAPSHOT_BATCH_SIZE) {
                    sink.applyBatch(batch);
                    copied += batch.size();
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
            copied += batch.size();
        }
        checkpoints.save(partitionCheckpointKey, PARTITION_DONE);
        log.info("mongo source[{}]: partition snapshot copied {} document(s)", partitionCheckpointKey, copied);
    }

    /** {@code null} for the (default, backward-compatible) single-partition case -- no filter at
     * all beats a trivially-true one. For {@code partitionCount > 1}, a real server-side filter:
     * hash {@code shardKeyField} via the same {@code $toHashedIndexKey} operator Polywire's own
     * hash {@code ShardingStrategy} is conceptually equivalent to, then bucket it mod {@code
     * partitionCount} -- {@code $abs} first since {@code $mod} preserves the dividend's sign and a
     * hash can be negative. */
    private Bson bucketFilter(int bucket) {
        if (partitionCount == 1) {
            return null;
        }
        Document hashed = new Document("$toHashedIndexKey", "$" + shardKeyField);
        Document abs = new Document("$abs", hashed);
        Document mod = new Document("$mod", List.of(abs, partitionCount));
        Document eq = new Document("$eq", List.of(mod, bucket));
        return new Document("$expr", eq);
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
                checkpoints.save(checkpointKey, event.getResumeToken().toJson(), eventTimestamp(event));
            }
        }
    }

    /** A change event's own {@code clusterTime} -- the server's wall-clock time when the write
     * actually happened on the SOURCE, not when this worker got around to applying it. This is
     * what makes a real lag metric ({@code now() - eventTimestamp}) meaningful: {@code
     * updated_at} on the checkpoint row only proves the worker is alive and saving, not that it's
     * caught up. Falls back to "now" on the rare event that genuinely has no clusterTime (seen in
     * practice on some synthetic/test event shapes, never on a real server-generated one) rather
     * than making the caller handle a null. */
    private static java.time.Instant eventTimestamp(ChangeStreamDocument<Document> event) {
        BsonTimestamp clusterTime = event.getClusterTime();
        return clusterTime == null ? java.time.Instant.now() : java.time.Instant.ofEpochSecond(clusterTime.getTime());
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
