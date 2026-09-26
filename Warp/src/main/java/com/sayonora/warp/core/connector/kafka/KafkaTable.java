package com.sayonora.warp.core.connector.kafka;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * One Kafka topic exposed as a queryable SQL table over a real, bounded snapshot -- ported from the
 * sibling ThinkingSense project's real, tested {@code com.omnigate.calcite.kafka.KafkaTable}. See
 * {@link KafkaSchemaFactory}'s own javadoc for why this is a real bounded scan (each partition's
 * high-watermark captured once, before reading anything, via one probe consumer), not
 * continuous-streaming SQL -- this is ALREADY the exact upstream solution to the "unbounded stream"
 * risk, not something this port needed to invent.
 *
 * <p>{@code format: "json"} (the common case): each message's value parsed as a JSON object,
 * declared {@code fields} extracted and stringified -- same {@code VARCHAR}-everywhere,
 * {@code String.valueOf} posture {@code com.sayonora.warp.core.connector.mongo.MongoTable}/{@code
 * dynamodb.DynamoTable} already establish for schemaless sources. {@code format: "string"}: the raw
 * UTF-8 message value as a single unnamed {@code value} column.
 *
 * <p>No predicate pushdown, same correctness-first call as every other schemaless-source connector
 * in this codebase.
 *
 * <p><b>Sequential, not partition-parallel</b>: ThinkingSense's own version dispatches one consumer
 * per partition concurrently through its {@code LakehouseFileScanExecutor}, an executor Warp has no
 * equivalent of. This port reads each partition sequentially instead (still correct, still the same
 * bounded-snapshot contract -- only slower on a many-partition topic); porting/introducing a
 * parallel executor is explicit, approved scope-narrowing for this connector, not attempted here.
 */
final class KafkaTable extends AbstractTable implements ScannableTable {

    private final String bootstrapServers;
    private final String topic;
    private final String format;
    private final List<String> fields;

    KafkaTable(String bootstrapServers, String topic, String format, List<String> fields) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.format = format;
        this.fields = List.copyOf(fields);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        if ("string".equals(format)) {
            builder.add("value", typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);
        } else {
            for (String field : fields) {
                builder.add(field, typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);
            }
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<TopicPartition> partitions;
        Map<TopicPartition, Long> endOffsets;
        try (Consumer<byte[], byte[]> probe = KafkaSchemaFactory.newConsumer(bootstrapServers)) {
            List<PartitionInfo> partitionInfos = probe.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return Linq4j.asEnumerable(List.of()); // topic doesn't exist / has no partitions -- an empty table, not an error
            }
            partitions = partitionInfos.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            probe.assign(partitions);
            // Real bounded snapshot: capture EVERY partition's high-watermark ONCE, up front, via
            // this one real probe consumer, BEFORE any partition's own real, independent read below.
            // A message produced after this call is invisible to this scan, by design -- re-run the
            // query to see it, same "re-run to see new data" contract every other connector has.
            endOffsets = probe.endOffsets(partitions);
        }
        List<Object[]> rows = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            rows.addAll(readPartition(partition, endOffsets.get(partition)));
        }
        return Linq4j.asEnumerable(rows);
    }

    /** Reads exactly one real Kafka partition, on its own real consumer, from its own beginning up
     * to its own already-captured watermark. */
    private List<Object[]> readPartition(TopicPartition partition, long endOffset) {
        List<Object[]> rows = new ArrayList<>();
        if (endOffset <= 0) {
            return rows; // a real, empty partition -- nothing to read, no point opening a consumer for it
        }
        try (Consumer<byte[], byte[]> consumer = KafkaSchemaFactory.newConsumer(bootstrapServers)) {
            List<TopicPartition> onlyThisPartition = List.of(partition);
            consumer.assign(onlyThisPartition);
            consumer.seekToBeginning(onlyThisPartition);
            while (consumer.position(partition) < endOffset) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(5));
                if (batch.isEmpty()) {
                    break; // no more records within this partition's own watermark range -- stop rather than loop forever
                }
                for (ConsumerRecord<byte[], byte[]> record : batch.records(partition)) {
                    if (record.offset() >= endOffset) {
                        continue; // produced after this scan's own watermark capture -- excluded, keeps this a real bounded snapshot
                    }
                    rows.add(decode(record));
                }
            }
        }
        return rows;
    }

    /** Package-private (not private) so {@code KafkaTableMappingTest} can exercise real message
     * decoding without a real broker -- same "expose the pure mapping logic for a real Kafka
     * client's own record type" posture {@code DynamoTable#toRow}/{@code #stringify} already
     * establish for DynamoDB. */
    Object[] decode(ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null) {
            return "string".equals(format) ? new Object[] {null} : new Object[fields.size()];
        }
        String value = new String(record.value(), StandardCharsets.UTF_8);
        if ("string".equals(format)) {
            return new Object[] {value};
        }
        Object[] row = new Object[fields.size()];
        try {
            JsonObject json = JsonParser.parseString(value).getAsJsonObject();
            for (int i = 0; i < fields.size(); i++) {
                JsonElement element = json.get(fields.get(i));
                row[i] = element == null || element.isJsonNull() ? null
                        : element.isJsonPrimitive() ? element.getAsString() : element.toString();
            }
        } catch (RuntimeException e) {
            // Malformed/non-JSON message on a "format: json" topic -- every declared field comes
            // back null for this one row rather than failing the whole scan.
        }
        return row;
    }
}
