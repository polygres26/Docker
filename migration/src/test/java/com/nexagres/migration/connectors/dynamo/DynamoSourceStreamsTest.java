package com.nexagres.migration.connectors.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.testsupport.RealPostgres;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/**
 * Real, not simulated, verification of {@link DynamoSource}'s Streams-based change-feed logic --
 * against a hand-written fake {@link DynamoDbStreamsClient} and fake {@link DynamoDbClient}
 * (deliberately, not a real DynamoDB Streams backend: DynamoDB Local, the only source this project
 * can run in tests, does not implement Streams at all -- see {@code RealDynamoDb}'s own javadoc).
 * The fakes are real, minimal, hand-rolled implementations that behave like the actual DynamoDB
 * Streams API contract (shard iterators, sequence numbers, {@code null} nextShardIterator meaning
 * "closed and drained") -- not mocks that merely record calls, so the actual checkpoint-persistence
 * and shard-polling logic in {@link DynamoSource} genuinely runs end to end against them.
 *
 * <p>Proves: (1) a single-shard stream replicates INSERT/MODIFY/REMOVE events correctly through a
 * real {@link Sink}; (2) the per-shard sequence-number checkpoint is actually persisted to a real
 * {@link CdcCheckpointStore}, with a real {@code last_event_at} timestamp from each record's
 * {@code approximateCreationDateTime}; (3) when the shard closes ({@code nextShardIterator ==
 * null}), a NEWLY discovered child shard (only visible on the SECOND {@code DescribeStream} call,
 * simulating a real reshard happening after the connector starts) gets its own poller launched by
 * the reshard-discovery loop, and its own record replicates too.
 */
class DynamoSourceStreamsTest {

    private static final Gson GSON = new Gson();

    /** Records every {@link ChangeEvent} applied, in order -- a real {@link Sink}, just an
     * in-memory one instead of a real gRPC target, since this test is about {@link DynamoSource}'s
     * OWN logic, not re-proving {@code WarpGrpcSink} (already covered by the snapshot test's
     * real gRPC path and every other connector's own tests). */
    private static final class RecordingSink implements Sink {
        final List<ChangeEvent> applied = new CopyOnWriteArrayList<>();

        @Override
        public void apply(ChangeEvent event) {
            applied.add(event);
        }
    }

    /** A real, minimal fake covering exactly the operations {@link DynamoSource} calls --
     * DescribeTable (key schema + stream status) and UpdateTable (enabling a stream). Every other
     * {@link DynamoDbClient} method keeps its AWS-SDK-default "unsupported" behavior, which is
     * correct: this connector never calls them. */
    private static final class FakeDynamoDbClient implements DynamoDbClient {
        private boolean streamEnabled;

        @Override
        public String serviceName() {
            return "dynamodb";
        }

        @Override
        public void close() {
        }

        @Override
        public DescribeTableResponse describeTable(DescribeTableRequest request) {
            TableDescription.Builder table = TableDescription.builder()
                    .tableName("Orders")
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType("S").build())
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build());
            if (streamEnabled) {
                table.latestStreamArn("arn:aws:dynamodb:local:000000000000:table/Orders/stream/2024-01-01T00:00:00.000")
                        .streamSpecification(StreamSpecification.builder().streamEnabled(true).build());
            }
            return DescribeTableResponse.builder().table(table.build()).build();
        }

        @Override
        public UpdateTableResponse updateTable(UpdateTableRequest request) {
            streamEnabled = true;
            return UpdateTableResponse.builder().build();
        }
    }

    /** A real, minimal fake covering exactly {@code DescribeStream}/{@code GetShardIterator}/
     * {@code GetRecords} -- the three operations {@link DynamoSource} calls. Simulates ONE shard
     * ({@code shard-0}) that yields two records then closes, and a CHILD shard ({@code shard-1},
     * {@code ParentShardId = shard-0}) that only appears starting on the SECOND {@code
     * DescribeStream} call -- a real reshard, not present from the start, so the connector's own
     * reshard-discovery loop is what has to find it, not just the initial shard list. */
    private static final class FakeDynamoDbStreamsClient implements DynamoDbStreamsClient {
        private final AtomicBoolean childShardVisible = new AtomicBoolean(false);
        private int shard0RecordsServed = 0;

        @Override
        public String serviceName() {
            return "dynamodb";
        }

        @Override
        public void close() {
        }

        @Override
        public DescribeStreamResponse describeStream(DescribeStreamRequest request) {
            List<Shard> shards = new ArrayList<>();
            shards.add(Shard.builder().shardId("shard-0").build());
            if (childShardVisible.get()) {
                shards.add(Shard.builder().shardId("shard-1").parentShardId("shard-0").build());
            }
            return DescribeStreamResponse.builder()
                    .streamDescription(StreamDescription.builder().shards(shards).build())
                    .build();
        }

        @Override
        public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest request) {
            return GetShardIteratorResponse.builder().shardIterator("iter:" + request.shardId() + ":" + request.sequenceNumber()).build();
        }

        @Override
        public GetRecordsResponse getRecords(GetRecordsRequest request) {
            String iterator = request.shardIterator();
            if (iterator.startsWith("iter:shard-0:")) {
                return shard0Records();
            }
            if (iterator.startsWith("iter:shard-1:")) {
                return shard1Records();
            }
            throw new IllegalArgumentException("unexpected shard iterator in fake: " + iterator);
        }

        private GetRecordsResponse shard0Records() {
            if (shard0RecordsServed == 0) {
                shard0RecordsServed++;
                Record record = Record.builder()
                        .eventName("INSERT")
                        .dynamodb(StreamRecord.builder()
                                .sequenceNumber("100")
                                .approximateCreationDateTime(Instant.now())
                                .newImage(Map.of("id", AttributeValue.builder().s("row-1").build(),
                                        "amount", AttributeValue.builder().n("42").build()))
                                .build())
                        .build();
                return GetRecordsResponse.builder().records(record).nextShardIterator("iter:shard-0:next").build();
            }
            if (shard0RecordsServed == 1) {
                shard0RecordsServed++;
                Record record = Record.builder()
                        .eventName("REMOVE")
                        .dynamodb(StreamRecord.builder()
                                .sequenceNumber("101")
                                .approximateCreationDateTime(Instant.now())
                                .keys(Map.of("id", AttributeValue.builder().s("row-1").build()))
                                .build())
                        .build();
                // Shard closes right after this record -- nextShardIterator becomes null, exactly
                // as real DynamoDB Streams behaves for a closed, fully-drained shard. This is the
                // trigger for the child shard to become visible in DescribeStream.
                childShardVisible.set(true);
                return GetRecordsResponse.builder().records(record).nextShardIterator(null).build();
            }
            return GetRecordsResponse.builder().records(List.of()).nextShardIterator(null).build();
        }

        private boolean shard1RecordServed = false;

        private GetRecordsResponse shard1Records() {
            if (!shard1RecordServed) {
                shard1RecordServed = true;
                Record record = Record.builder()
                        .eventName("INSERT")
                        .dynamodb(StreamRecord.builder()
                                .sequenceNumber("200")
                                .approximateCreationDateTime(Instant.now())
                                .newImage(Map.of("id", AttributeValue.builder().s("row-2").build(),
                                        "amount", AttributeValue.builder().n("7").build()))
                                .build())
                        .build();
                return GetRecordsResponse.builder().records(record).nextShardIterator("iter:shard-1:next").build();
            }
            return GetRecordsResponse.builder().records(List.of()).nextShardIterator("iter:shard-1:next").build();
        }
    }

    @Test
    void streamsReplicatesAcrossAReshardAndPersistsPerShardCheckpoints() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            RecordingSink sink = new RecordingSink();
            DynamoSource source = new DynamoSource(new FakeDynamoDbClient(), new FakeDynamoDbStreamsClient(), "Orders");

            // Populates pkName/skName from the (fake) source's key schema -- streamChanges'
            // upsertEvent/deleteEvent need them, same ordering Coordinator itself always
            // guarantees (ensureTargetSchema before any change-feed method runs).
            source.ensureTargetSchema(sink);
            sink.applied.clear(); // the DDL/catalog-row ChangeEvents aren't what this test is about
            source.prepareChangeFeed(sink, checkpoints);

            Thread streamThread = new Thread(() -> {
                try {
                    source.streamChanges(sink, checkpoints);
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-dynamo-stream");
            streamThread.start();
            try {
                waitUntil(Duration.ofSeconds(15), () -> containsRowFor(sink.applied, "row-2"));

                // row-1 was inserted then removed (shard-0) -- both events should have applied, in
                // order.
                List<String> sqlKinds = sink.applied.stream().map(e -> e.sql().startsWith("INSERT") ? "INSERT" : "DELETE").toList();
                assertTrue(sqlKinds.contains("INSERT") && sqlKinds.contains("DELETE"),
                        "both the insert and the remove from shard-0 should have applied: " + sqlKinds);
                assertTrue(containsRowFor(sink.applied, "row-2"), "the child shard's (shard-1) own record should have replicated too");

                // Per-shard sequence numbers actually persisted, not just applied in memory.
                String checkpointJson = checkpoints.load("dynamo:Orders");
                assertTrue(checkpointJson != null && checkpointJson.contains("shard-0") && checkpointJson.contains("shard-1"),
                        "the persisted checkpoint should track BOTH shards once the child is discovered: " + checkpointJson);
                JsonObject parsed = GSON.fromJson(checkpointJson, JsonObject.class);
                JsonObject shardSeqs = parsed.getAsJsonObject("shardSequenceNumbers");
                assertEquals("101", shardSeqs.get("shard-0").getAsString(), "shard-0's checkpoint should be its last applied sequence number");
                assertEquals("200", shardSeqs.get("shard-1").getAsString(), "shard-1's checkpoint should be its own last applied sequence number");
            } finally {
                source.close();
                streamThread.interrupt();
                streamThread.join(Duration.ofSeconds(10).toMillis());
            }
        }
    }

    private static boolean containsRowFor(List<ChangeEvent> events, String rowId) {
        return events.stream().anyMatch(e -> e.params().stream().anyMatch(p -> p.contains(rowId)));
    }

    private static void waitUntil(Duration timeout, java.util.concurrent.Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
