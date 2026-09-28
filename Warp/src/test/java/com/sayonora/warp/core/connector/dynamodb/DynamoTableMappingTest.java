package com.sayonora.warp.core.connector.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Pure row/type-mapping coverage for {@link DynamoTable} -- no container needed. */
class DynamoTableMappingTest {

    @Test
    void stringifiesEveryScalarAttributeValueKind() {
        assertEquals("abc", DynamoTable.stringify(AttributeValue.fromS("abc")));
        assertEquals("12.500", DynamoTable.stringify(AttributeValue.fromN("12.500")), "N keeps DynamoDB's exact decimal text");
        assertEquals("true", DynamoTable.stringify(AttributeValue.fromBool(true)));
        assertNull(DynamoTable.stringify(AttributeValue.fromNul(true)));
        assertNull(DynamoTable.stringify(null), "missing attribute -> SQL NULL");
    }

    @Test
    void nonScalarAttributeDegradesToReadableTextNotACrash() {
        String list = DynamoTable.stringify(AttributeValue.fromL(List.of(AttributeValue.fromS("x"))));
        assertTrue(list.contains("L="), list);
    }

    @Test
    void rowFollowsDeclaredFieldOrderWithNullsForMissingAttributes() {
        Map<String, AttributeValue> item = Map.of(
                "id", AttributeValue.fromN("7"), "name", AttributeValue.fromS("seven"));
        assertArrayEquals(new Object[] {"seven", "7", null},
                DynamoTable.toRow(item, List.of("name", "id", "absent")));
    }

    @Test
    void everyDeclaredColumnIsNullableVarchar() {
        DynamoTable table = new DynamoTable(null, "T", List.of("a", "b"));
        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
        assertEquals(List.of("a", "b"), rowType.getFieldNames());
        rowType.getFieldList().forEach(f -> {
            assertEquals(SqlTypeName.VARCHAR, f.getType().getSqlTypeName());
            assertTrue(f.getType().isNullable());
        });
    }

    @Test
    void factoryRejectsMissingTables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DynamoSchemaFactory.createSchema(Map.of("region", "us-east-1", "tables", Map.of())));
        assertTrue(e.getMessage().contains("table.<sqlName>"), e.getMessage());
    }
}
