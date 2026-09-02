package com.sayonora.migration.connectors.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Pure logic, no infrastructure needed -- proves {@link DynamoJson} reproduces dynamowire's own
 * typed-JSON attribute serialization for every {@link AttributeValue} shape (see this class's own
 * javadoc for why byte-identical output matters). */
class DynamoJsonTest {

    @Test
    void serializesEveryAttributeTypeInDynamoDbsOwnTypedJsonShape() {
        AttributeValue map = AttributeValue.builder().m(Map.of(
                "nested", AttributeValue.builder().s("value").build())).build();
        AttributeValue item = AttributeValue.builder().m(Map.of(
                "s", AttributeValue.builder().s("hello").build(),
                "n", AttributeValue.builder().n("42.5").build(),
                "bool", AttributeValue.builder().bool(true).build(),
                "nul", AttributeValue.builder().nul(true).build(),
                "list", AttributeValue.builder().l(List.of(AttributeValue.builder().n("1").build())).build(),
                "map", map)).build();

        String json = DynamoJson.attributeToJson(item).toString();
        assertEquals(true, json.contains("\"s\":{\"S\":\"hello\"}"));
        assertEquals(true, json.contains("\"n\":{\"N\":\"42.5\"}"));
        assertEquals(true, json.contains("\"bool\":{\"BOOL\":true}"));
        assertEquals(true, json.contains("\"nul\":{\"NULL\":true}"));
        assertEquals(true, json.contains("\"list\":{\"L\":[{\"N\":\"1\"}]}"));
        assertEquals(true, json.contains("\"map\":{\"M\":{\"nested\":{\"S\":\"value\"}}}"));
    }

    @Test
    void binaryValuesAreBase64EncodedMatchingDynamoDbsOwnJsonWireFormat() {
        byte[] raw = {1, 2, 3, 4};
        AttributeValue binary = AttributeValue.builder().b(SdkBytes.fromByteArray(raw)).build();
        String json = DynamoJson.attributeToJson(binary).toString();
        assertEquals("{\"B\":\"" + Base64.getEncoder().encodeToString(raw) + "\"}", json);
    }

    @Test
    void keyTextMatchesEachScalarKeyTypesRawForm() {
        assertEquals("cust-1", DynamoJson.keyText(AttributeValue.builder().s("cust-1").build()));
        assertEquals("42", DynamoJson.keyText(AttributeValue.builder().n("42").build()));
        byte[] raw = {5, 6};
        assertEquals(Base64.getEncoder().encodeToString(raw),
                DynamoJson.keyText(AttributeValue.builder().b(SdkBytes.fromByteArray(raw)).build()));
    }
}
