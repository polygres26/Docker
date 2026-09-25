package com.sayonora.wire.core.connector.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Real Kafka access as a federated Warp schema -- a port of the sibling ThinkingSense project's
 * {@code com.omnigate.calcite.kafka.KafkaSchemaFactory} (real, tested there), so a Kafka topic's
 * bounded snapshot can be JOINed against a Postgres/Oracle/etc. table in one statement through
 * {@code SchemaFederationStage}. Operand shape (built from a {@code kafka://} {@code WARP_BACKENDS}
 * entry by {@link com.sayonora.wire.core.connector.ConnectorOperands}, see its javadoc for the text
 * grammar):
 * <pre>{@code
 * {"bootstrapServers": "host:9092", "tables": {
 *   "orders": {"topic": "orders-topic", "format": "json", "fields": ["orderId","customer","amount"]}
 * }}}
 * </pre>
 * {@code fields} is required and explicit for {@code format: "json"} -- a JSON message's fields have
 * no static schema this factory could introspect, same "explicit table map" posture {@code
 * mongo.MongoSchemaFactory}'s own javadoc already establishes. {@code format} is {@code "json"}
 * (default) or {@code "string"} (the raw message value as a single unnamed column).
 *
 * <p>Real, already-solved bounded-snapshot design -- see {@link KafkaTable}'s own javadoc: each
 * partition's high-watermark is captured ONCE, up front, via one probe consumer, then every
 * partition is read from its own beginning up to its own captured watermark. Bounded, deterministic;
 * no new bounding mechanism was needed to make this correct.
 *
 * <p>Read-only, matching every other connector in this codebase. Implements Calcite's {@link
 * SchemaFactory} for fidelity with the source, but Warp calls {@link #createSchema} directly from
 * {@code SchemaFederationStage} -- no model-file/reflection loading.
 */
public final class KafkaSchemaFactory implements SchemaFactory {

    public static final KafkaSchemaFactory INSTANCE = new KafkaSchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    @SuppressWarnings("unchecked")
    public static KafkaSchema createSchema(Map<String, Object> operand) {
        Object tablesObj = operand.get("tables");
        if (!(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("KafkaSchemaFactory requires at least one table -- declare its "
                    + "'topic' (and 'fields' for format=json) on the kafka:// backend URL");
        }
        String bootstrapServers = requireString(operand, "bootstrapServers");
        return new KafkaSchema(bootstrapServers, (Map<String, Map<String, Object>>) tablesObj);
    }

    /** A fresh, uniquely-grouped consumer per call -- deliberately not a single shared, long-lived
     * consumer. Kafka consumer-group semantics mean two concurrent scans sharing one consumer would
     * fight over partition assignment; a fresh random {@code group.id} means this consumer always
     * starts from {@code seekToBeginning} (see {@link KafkaTable#scan}), never resumes another
     * scan's committed offset. */
    static Consumer<byte[], byte[]> newConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "warp-scan-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(20).toMillis());
        return new KafkaConsumer<>(props);
    }

    private static String requireString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof String s) || s.isBlank()) {
            throw new IllegalArgumentException("KafkaSchemaFactory requires string field '" + key + "'");
        }
        return s;
    }
}
