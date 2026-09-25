package com.sayonora.wire.core.connector.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/** Pure row/type-mapping coverage for {@link KafkaTable}'s message decoding -- no real broker
 * needed, a real {@link ConsumerRecord} is constructed directly (the client's own record type, not
 * a hand-rolled stand-in). */
class KafkaTableMappingTest {

    @Test
    void jsonFormatExtractsDeclaredFieldsAsStrings() {
        KafkaTable table = new KafkaTable("localhost:9092", "orders", "json", List.of("orderId", "amount", "missing"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("orders", 0, 0L, null,
                "{\"orderId\":\"o1\",\"amount\":42}".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(new Object[] {"o1", "42", null}, table.decode(record));
    }

    @Test
    void malformedJsonDegradesToAllNullsRatherThanFailingTheWholeScan() {
        KafkaTable table = new KafkaTable("localhost:9092", "orders", "json", List.of("orderId"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("orders", 0, 0L, null,
                "not json".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(new Object[] {null}, table.decode(record));
    }

    @Test
    void stringFormatReturnsRawMessageAsSingleColumn() {
        KafkaTable table = new KafkaTable("localhost:9092", "raw", "string", List.of());
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("raw", 0, 0L, null,
                "hello".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(new Object[] {"hello"}, table.decode(record));
    }

    @Test
    void nullValueDecodesToNullsNotAnException() {
        KafkaTable jsonTable = new KafkaTable("localhost:9092", "orders", "json", List.of("a", "b"));
        ConsumerRecord<byte[], byte[]> tombstone = new ConsumerRecord<>("orders", 0, 0L, null, (byte[]) null);
        assertArrayEquals(new Object[] {null, null}, jsonTable.decode(tombstone));

        KafkaTable stringTable = new KafkaTable("localhost:9092", "raw", "string", List.of());
        assertArrayEquals(new Object[] {null}, stringTable.decode(tombstone));
    }

    @Test
    void rowTypeIsValueColumnForStringFormatAndFieldListForJson() {
        KafkaTable jsonTable = new KafkaTable("localhost:9092", "orders", "json", List.of("a", "b"));
        RelDataType jsonRowType = jsonTable.getRowType(new JavaTypeFactoryImpl());
        assertEquals(List.of("a", "b"), jsonRowType.getFieldNames());
        jsonRowType.getFieldList().forEach(f -> assertEquals(SqlTypeName.VARCHAR, f.getType().getSqlTypeName()));

        KafkaTable stringTable = new KafkaTable("localhost:9092", "raw", "string", List.of());
        assertEquals(List.of("value"), stringTable.getRowType(new JavaTypeFactoryImpl()).getFieldNames());
    }

    @Test
    void schemaRejectsJsonTableWithoutDeclaredFields() {
        Map<String, Map<String, Object>> tableDefs = Map.of("orders", Map.of("topic", "orders", "format", "json"));
        KafkaSchema schema = KafkaSchemaFactory.createSchema(
                Map.of("bootstrapServers", "localhost:9092", "tables", tableDefs));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> schema.tables().get("orders"));
        assertTrue(e.getMessage().contains("fields"), e.getMessage());
    }

    @Test
    void factoryRejectsMissingTables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                KafkaSchemaFactory.createSchema(Map.of("bootstrapServers", "localhost:9092", "tables", Map.of())));
        assertTrue(e.getMessage().contains("at least one table"), e.getMessage());
    }
}
