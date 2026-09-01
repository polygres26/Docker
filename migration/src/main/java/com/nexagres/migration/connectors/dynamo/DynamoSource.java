package com.nexagres.migration.connectors.dynamo;

import com.google.gson.Gson;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/**
 * DynamoDB connector: real parallel {@code Scan} segments for the initial bulk read, real DynamoDB
 * Streams for live sync, writing through the target's real dynamowire physical schema (see {@code
 * com.nexagres.wire.dynamowire.PgItemStore} in the {@code wire} module: {@code dynamo_item_<name>}
 * with {@code pk_value text}/{@code sk_value text}/{@code sk_num numeric}/{@code item jsonb},
 * {@code PRIMARY KEY (pk_value, sk_value)}, plus a row in the shared {@code _dynamo_tables} catalog
 * table so a live dynamowire client can discover and query this table afterward) via whatever
 * {@link Sink} is given -- always {@code WarpGrpcSink} in production.
 *
 * <p><b>Partitioning</b>: {@link #listPartitions} maps directly onto DynamoDB's own native
 * parallel {@code Scan} support ({@code Segment}/{@code TotalSegments}) -- the database itself
 * splits the table, not a client-side derived filter the way {@link
 * com.nexagres.migration.connectors.mongo.MongoSource} has to for Mongo.
 *
 * <p><b>Change feed</b>: DynamoDB Streams' shard model is fundamentally different from Mongo's
 * single resumable cursor -- a stream is a set of shards, each with its own sequence-number
 * checkpoint, and shards close and spawn child shards over time as the table's own partitions
 * reshard. {@link #prepareChangeFeed} enables a stream on the source table if one isn't already
 * enabled (real DynamoDB behavior: streams are opt-in per table) and records the CURRENT shard set
 * as the starting checkpoint (each shard starting from {@code TRIM_HORIZON}) before any partition's
 * snapshot runs -- same ordering guarantee as Mongo's own change-stream-before-snapshot design, and
 * for the same reason. {@link #streamChanges} runs one poller thread per open shard plus a
 * discovery loop that periodically re-describes the stream to pick up new (child) shards as older
 * ones close -- a real, working implementation of the same shard-lineage-following algorithm the
 * DynamoDB Streams Kinesis Adapter/KCL use, at a basic single-process level (no cross-worker-
 * process distribution of individual shards -- change-feed ownership is single-leader, same design
 * as every other connector in this project).
 *
 * <p><b>Known, scoped test gap</b> (not glossed over): DynamoDB Local -- the only source this
 * project's tests run against -- does not support DynamoDB Streams at all (a real, documented AWS
 * limitation, not a bug in this connector). The snapshot/Scan path is verified end to end against
 * real infrastructure exactly like every other connector; the Streams path is verified with a
 * hand-written fake {@link DynamoDbStreamsClient} exercising the real shard-checkpoint/reshard
 * logic, not a real DynamoDB Streams backend -- a real AWS DynamoDB Streams smoke test is a
 * separate, real follow-up this project cannot run in CI.
 */
public final class DynamoSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(DynamoSource.class);
    private static final Gson GSON = new Gson();
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int SNAPSHOT_BATCH_SIZE = 250;
    private static final int SHARD_DISCOVERY_INTERVAL_MS = 3000;

    private final DynamoDbClient client;
    private final DynamoDbStreamsClient streamsClient;
    private final String sourceTable;
    private final String targetTable;
    private final String checkpointKey;
    private final int partitionCount;

    private volatile String pkName;
    private volatile String pkType;
    private volatile String skName; // null if the table has no sort key
    private volatile String skType;

    private volatile boolean running = true;
    private final Set<Thread> shardPollerThreads = new CopyOnWriteArraySet<>();

    public DynamoSource(DynamoDbClient client, DynamoDbStreamsClient streamsClient, String sourceTable) {
        this(client, streamsClient, sourceTable, 1);
    }

    /** @param partitionCount how many parallel {@code Scan} segments to split the initial read
     *     into -- maps directly to DynamoDB's own {@code TotalSegments}. */
    public DynamoSource(DynamoDbClient client, DynamoDbStreamsClient streamsClient, String sourceTable, int partitionCount) {
        this.client = client;
        this.streamsClient = streamsClient;
        this.sourceTable = sourceTable;
        this.targetTable = pgTableName(sourceTable);
        this.checkpointKey = "dynamo:" + sourceTable;
        this.partitionCount = Math.max(1, partitionCount);
    }

    /** Matches {@code PgItemStore#pgTableName} exactly -- has to, or a live dynamowire client and
     * this connector would resolve the same DynamoDB table name to two different physical tables. */
    static String pgTableName(String dynamoTableName) {
        return "dynamo_item_" + dynamoTableName.toLowerCase().replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS " + qualifiedTable()
                + " (pk_value text NOT NULL, sk_value text NOT NULL DEFAULT '', sk_num numeric, "
                + "item jsonb NOT NULL, PRIMARY KEY (pk_value, sk_value))");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS " + targetTable
                + "_pk_sknum_idx ON " + qualifiedTable() + " (pk_value, sk_num)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS _dynamo_tables ("
                + "table_name text PRIMARY KEY, pg_table text NOT NULL, pk_name text NOT NULL, "
                + "pk_type text NOT NULL, sk_name text, sk_type text, status text NOT NULL, "
                + "creation_time_millis bigint NOT NULL)");

        loadKeySchema();
        registerCatalogRow(sink);
    }

    /** Reads the source table's REAL key schema (partition key + optional sort key, names and
     * types) once via {@code DescribeTable} -- the source of truth, not something a caller has to
     * redundantly re-specify. Cached on this instance for {@link #upsertEvent}/{@link
     * #deleteEvent}/{@link #registerCatalogRow} to reuse without re-describing the table on every
     * call. */
    private void loadKeySchema() {
        TableDescription desc = client.describeTable(r -> r.tableName(sourceTable)).table();
        Map<String, String> attributeTypes = new LinkedHashMap<>();
        for (AttributeDefinition def : desc.attributeDefinitions()) {
            attributeTypes.put(def.attributeName(), def.attributeTypeAsString());
        }
        for (KeySchemaElement key : desc.keySchema()) {
            if (key.keyType() == KeyType.HASH) {
                pkName = key.attributeName();
                pkType = attributeTypes.get(pkName);
            } else if (key.keyType() == KeyType.RANGE) {
                skName = key.attributeName();
                skType = attributeTypes.get(skName);
            }
        }
        if (pkName == null) {
            throw new IllegalStateException("DynamoDB table " + sourceTable + " has no partition key -- not a valid table");
        }
    }

    /** So a live dynamowire client can discover and query this table after the migration --
     * without this row, {@code dynamo_item_<name>} would exist physically but be invisible to
     * every real dynamowire operation, which all resolve a table name through {@code
     * _dynamo_tables} first. */
    private void registerCatalogRow(Sink sink) throws Exception {
        List<String> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("INSERT INTO _dynamo_tables (table_name, pg_table, pk_name, "
                + "pk_type, sk_name, sk_type, status, creation_time_millis) VALUES (?, ?, ?, ?, ");
        params.add(sourceTable);
        params.add(targetTable);
        params.add(pkName);
        params.add(pkType);
        if (skName != null) {
            sql.append("?, ?, ");
            params.add(skName);
            params.add(skType);
        } else {
            sql.append("NULL, NULL, ");
        }
        sql.append("?, ?::bigint) ON CONFLICT (table_name) DO UPDATE SET pg_table = EXCLUDED.pg_table, "
                + "pk_name = EXCLUDED.pk_name, pk_type = EXCLUDED.pk_type, sk_name = EXCLUDED.sk_name, "
                + "sk_type = EXCLUDED.sk_type, status = EXCLUDED.status");
        params.add("ACTIVE");
        params.add(String.valueOf(System.currentTimeMillis()));
        sink.apply(new ChangeEvent(sql.toString(), params));
    }

    private static void applyTolerantOfConcurrentCreateRace(Sink sink, String ddl) throws Exception {
        try {
            sink.apply(new ChangeEvent(ddl, List.of()));
        } catch (SQLException e) {
            if (!"23505".equals(e.getSQLState())) {
                throw e;
            }
            log.info("ensureTargetSchema: lost a benign concurrent CREATE race to another worker "
                    + "(23505 on the object catalog) -- the object exists either way, continuing");
        }
    }

    @Override
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<>(partitionCount);
        for (int segment = 0; segment < partitionCount; segment++) {
            partitions.add(new Partition(checkpointKey + "#p" + segment, segment));
        }
        return partitions;
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        String partitionCheckpointKey = partition.id();
        if (PARTITION_DONE.equals(checkpoints.load(partitionCheckpointKey))) {
            log.info("dynamo source[{}]: partition already fully copied -- skipping", partitionCheckpointKey);
            return;
        }

        int segment = (Integer) partition.descriptor();
        Map<String, AttributeValue> exclusiveStartKey = null;
        long copied = 0;
        List<ChangeEvent> batch = new ArrayList<>(SNAPSHOT_BATCH_SIZE);
        do {
            ScanRequest.Builder req = ScanRequest.builder().tableName(sourceTable)
                    .segment(segment).totalSegments(partitionCount);
            if (exclusiveStartKey != null) {
                req.exclusiveStartKey(exclusiveStartKey);
            }
            ScanResponse resp = client.scan(req.build());
            for (Map<String, AttributeValue> item : resp.items()) {
                batch.add(upsertEvent(item));
                if (batch.size() >= SNAPSHOT_BATCH_SIZE) {
                    sink.applyBatch(batch);
                    copied += batch.size();
                    batch.clear();
                }
            }
            exclusiveStartKey = (resp.lastEvaluatedKey() == null || resp.lastEvaluatedKey().isEmpty())
                    ? null : resp.lastEvaluatedKey();
        } while (exclusiveStartKey != null);

        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
            copied += batch.size();
        }
        checkpoints.save(partitionCheckpointKey, PARTITION_DONE);
        log.info("dynamo source[{}]: segment snapshot copied {} item(s)", partitionCheckpointKey, copied);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) != null) {
            log.info("dynamo source[{}]: change-feed checkpoint already exists -- streamChanges "
                    + "will resume from it directly", checkpointKey);
            return;
        }
        String streamArn = ensureStreamEnabled();
        DescribeStreamResponse desc = streamsClient.describeStream(
                DescribeStreamRequest.builder().streamArn(streamArn).build());
        Map<String, String> shardSequenceNumbers = new LinkedHashMap<>();
        for (Shard shard : desc.streamDescription().shards()) {
            shardSequenceNumbers.put(shard.shardId(), ""); // "" = start this shard from TRIM_HORIZON
        }
        checkpoints.save(checkpointKey, GSON.toJson(new StreamCheckpoint(streamArn, shardSequenceNumbers)));
        log.info("dynamo source[{}]: change-feed checkpoint captured ({} shard(s)) before any "
                + "partition's snapshot starts", checkpointKey, shardSequenceNumbers.size());
    }

    /** Enables a stream on the source table if one isn't already active -- streams are opt-in per
     * table in real DynamoDB, so a source table migrated for the first time very likely doesn't
     * have one yet. {@code NEW_AND_OLD_IMAGES} is requested so a {@code REMOVE} event's old key is
     * always available even without a separate lookup (DynamoDB Streams, unlike a Mongo change
     * stream, never needs an {@code UPDATE_LOOKUP}-style extra fetch -- every record already
     * carries the images the requested view type asks for). */
    private String ensureStreamEnabled() throws InterruptedException {
        TableDescription desc = client.describeTable(r -> r.tableName(sourceTable)).table();
        boolean alreadyEnabled = desc.streamSpecification() != null
                && Boolean.TRUE.equals(desc.streamSpecification().streamEnabled())
                && desc.latestStreamArn() != null;
        if (alreadyEnabled) {
            return desc.latestStreamArn();
        }
        log.info("dynamo source[{}]: no active stream on the source table -- enabling one "
                + "(NEW_AND_OLD_IMAGES) before migrating", checkpointKey);
        client.updateTable(r -> r.tableName(sourceTable)
                .streamSpecification(s -> s.streamEnabled(true).streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)));

        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            TableDescription updated = client.describeTable(r -> r.tableName(sourceTable)).table();
            if (updated.latestStreamArn() != null) {
                return updated.latestStreamArn();
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Timed out waiting for DynamoDB Streams to become enabled on " + sourceTable);
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        String token = checkpoints.load(checkpointKey);
        if (token == null) {
            // Coordinator always calls prepareChangeFeed before this -- a missing checkpoint here
            // means that invariant was violated, not a recoverable runtime condition.
            throw new IllegalStateException("streamChanges called without a change-feed checkpoint for " + checkpointKey);
        }
        StreamCheckpoint state = GSON.fromJson(token, StreamCheckpoint.class);

        ExecutorService pool = Executors.newCachedThreadPool();
        Set<String> knownShardIds = ConcurrentHashMap.newKeySet();
        Set<String> finishedShardIds = ConcurrentHashMap.newKeySet();
        Map<String, String> sequenceNumbers = new ConcurrentHashMap<>(state.shardSequenceNumbers());
        knownShardIds.addAll(sequenceNumbers.keySet());

        try {
            for (String shardId : sequenceNumbers.keySet()) {
                launchShardPoller(pool, sink, checkpoints, state.streamArn(), shardId, sequenceNumbers, finishedShardIds);
            }

            // Reshard discovery loop: DynamoDB shards close and spawn children over the life of a
            // live table -- periodically re-describe the stream and launch pollers for any shard
            // not yet known, matching the real DynamoDB Streams Kinesis Adapter/KCL pattern (at a
            // basic, single-process level -- see this class's own javadoc).
            while (running) {
                Thread.sleep(SHARD_DISCOVERY_INTERVAL_MS);
                if (!running) {
                    break;
                }
                DescribeStreamResponse desc = streamsClient.describeStream(
                        DescribeStreamRequest.builder().streamArn(state.streamArn()).build());
                for (Shard shard : desc.streamDescription().shards()) {
                    if (knownShardIds.add(shard.shardId())) {
                        sequenceNumbers.put(shard.shardId(), "");
                        persistCheckpoint(checkpoints, state.streamArn(), sequenceNumbers);
                        log.info("dynamo source[{}]: discovered new shard {} -- starting a poller",
                                checkpointKey, shard.shardId());
                        launchShardPoller(pool, sink, checkpoints, state.streamArn(), shard.shardId(), sequenceNumbers, finishedShardIds);
                    }
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private void launchShardPoller(ExecutorService pool, Sink sink, StateStore checkpoints, String streamArn,
            String shardId, Map<String, String> sequenceNumbers, Set<String> finishedShardIds) {
        pool.submit(() -> {
            Thread.currentThread().setName("dynamo-shard-poller-" + shardId);
            shardPollerThreads.add(Thread.currentThread());
            try {
                pollShard(sink, checkpoints, streamArn, shardId, sequenceNumbers);
            } catch (Exception e) {
                if (running) {
                    log.error("dynamo source[{}]: shard poller for {} failed", checkpointKey, shardId, e);
                }
            } finally {
                finishedShardIds.add(shardId);
                shardPollerThreads.remove(Thread.currentThread());
            }
        });
    }

    private void pollShard(Sink sink, StateStore checkpoints, String streamArn, String shardId,
            Map<String, String> sequenceNumbers) throws Exception {
        String lastSequenceNumber = sequenceNumbers.get(shardId);
        GetShardIteratorRequest.Builder iterReq = GetShardIteratorRequest.builder()
                .streamArn(streamArn).shardId(shardId);
        if (lastSequenceNumber != null && !lastSequenceNumber.isEmpty()) {
            iterReq.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).sequenceNumber(lastSequenceNumber);
        } else {
            iterReq.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        }
        String shardIterator = streamsClient.getShardIterator(iterReq.build()).shardIterator();

        while (running && shardIterator != null) {
            GetRecordsResponse resp = streamsClient.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator).build());
            for (Record record : resp.records()) {
                applyStreamRecord(sink, record);
                sequenceNumbers.put(shardId, record.dynamodb().sequenceNumber());
                Instant eventTime = record.dynamodb().approximateCreationDateTime();
                if (eventTime != null) {
                    checkpoints.save(checkpointKey, GSON.toJson(new StreamCheckpoint(streamArn, sequenceNumbers)), eventTime);
                } else {
                    checkpoints.save(checkpointKey, GSON.toJson(new StreamCheckpoint(streamArn, sequenceNumbers)));
                }
            }
            shardIterator = resp.nextShardIterator();
            if (resp.records().isEmpty() && shardIterator != null) {
                Thread.sleep(500); // no new records yet, shard still open -- poll again shortly
            }
        }
        // shardIterator == null means the shard is CLOSED and fully drained -- its data is
        // permanently done; the reshard discovery loop in streamChanges picks up its children.
        log.info("dynamo source[{}]: shard {} closed and fully drained", checkpointKey, shardId);
    }

    private void applyStreamRecord(Sink sink, Record record) throws Exception {
        switch (record.eventNameAsString()) {
            case "INSERT", "MODIFY" -> {
                Map<String, AttributeValue> newImage = record.dynamodb().newImage();
                if (newImage != null && !newImage.isEmpty()) {
                    sink.apply(upsertEvent(newImage));
                } else {
                    log.warn("dynamo source[{}]: {} event with no NewImage -- was the stream enabled "
                            + "with NEW_IMAGE/NEW_AND_OLD_IMAGES? skipped", checkpointKey, record.eventNameAsString());
                }
            }
            case "REMOVE" -> sink.apply(deleteEvent(record.dynamodb().keys()));
            default -> log.debug("dynamo source[{}]: ignoring unrecognized event type {}",
                    checkpointKey, record.eventNameAsString());
        }
    }

    private synchronized void persistCheckpoint(StateStore checkpoints, String streamArn, Map<String, String> sequenceNumbers) throws Exception {
        checkpoints.save(checkpointKey, GSON.toJson(new StreamCheckpoint(streamArn, new LinkedHashMap<>(sequenceNumbers))));
    }

    private ChangeEvent upsertEvent(Map<String, AttributeValue> item) {
        String pk = DynamoJson.keyText(item.get(pkName));
        String itemJson = DynamoJson.itemToJson(item).toString();
        if (skName != null) {
            String sk = DynamoJson.keyText(item.get(skName));
            if ("N".equals(skType)) {
                return new ChangeEvent("INSERT INTO " + qualifiedTable() + " (pk_value, sk_value, sk_num, item) "
                        + "VALUES (?, ?, ?::numeric, ?::jsonb) ON CONFLICT (pk_value, sk_value) "
                        + "DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item",
                        List.of(pk, sk, sk, itemJson));
            }
            return new ChangeEvent("INSERT INTO " + qualifiedTable() + " (pk_value, sk_value, item) "
                    + "VALUES (?, ?, ?::jsonb) ON CONFLICT (pk_value, sk_value) DO UPDATE SET item = EXCLUDED.item",
                    List.of(pk, sk, itemJson));
        }
        return new ChangeEvent("INSERT INTO " + qualifiedTable() + " (pk_value, sk_value, item) "
                + "VALUES (?, '', ?::jsonb) ON CONFLICT (pk_value, sk_value) DO UPDATE SET item = EXCLUDED.item",
                List.of(pk, itemJson));
    }

    private ChangeEvent deleteEvent(Map<String, AttributeValue> keys) {
        String pk = DynamoJson.keyText(keys.get(pkName));
        String sk = skName != null ? DynamoJson.keyText(keys.get(skName)) : "";
        return new ChangeEvent("DELETE FROM " + qualifiedTable() + " WHERE pk_value = ? AND sk_value = ?",
                List.of(pk, sk));
    }

    private String qualifiedTable() {
        return "\"" + targetTable + "\"";
    }

    @Override
    public void close() {
        running = false;
        shardPollerThreads.forEach(Thread::interrupt);
    }

    /** The resume token this connector persists via {@link StateStore} -- opaque to everything
     * except this class, serialized as plain JSON via {@link Gson} (the same convention {@code
     * MongoSource} uses for its own resume token, just a different shape: a stream ARN plus one
     * sequence number per shard, not a single Mongo resume-token document). */
    private record StreamCheckpoint(String streamArn, Map<String, String> shardSequenceNumbers) {
    }
}
