package com.sayonora.warp.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonParser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AttributeValueAndValidationTest {

    private static String msg(Runnable r) {
        return assertThrows(DynamoException.class, r::run).getMessage();
    }

    private static AttributeValue json(String j) {
        return AttributeValue.fromJson(JsonParser.parseString(j));
    }

    @Test
    void numbersAreNormalisedAndBounded() {
        assertEquals("100", AttributeValue.normalizeNumber("1e2"));
        assertEquals("100.5", AttributeValue.normalizeNumber("100.50"));
        assertEquals("0.0000001", AttributeValue.normalizeNumber("1E-7"));
        assertEquals("0", AttributeValue.normalizeNumber("-0.0"));
        assertEquals("12", AttributeValue.normalizeNumber("+00012"));
        assertEquals("12345678901234567890123456789012345678", AttributeValue.normalizeNumber("12345678901234567890123456789012345678"));
        assertEquals("DynamoDB only supports precision up to 38 digits", msg(() -> AttributeValue.normalizeNumber("123456789012345678901234567890123456789")));
        assertTrue(msg(() -> AttributeValue.normalizeNumber("1E+126")).startsWith("Number overflow"));
        assertTrue(msg(() -> AttributeValue.normalizeNumber("1E-131")).startsWith("Number underflow"));
        assertEquals("A value provided cannot be converted into a number", msg(() -> AttributeValue.normalizeNumber("abc")));
        assertEquals("A value provided cannot be converted into a number", msg(() -> AttributeValue.normalizeNumber("")));
    }

    @Test
    void valuesParseValidateAndCompare() {
        assertEquals(0, json("{\"N\":\"1.0\"}").compareTo(json("{\"N\":\"1\"}")));
        assertTrue(json("{\"N\":\"9\"}").compareTo(json("{\"N\":\"10\"}")) < 0, "numeric, not lexicographic");
        assertTrue(json("{\"S\":\"a\"}").compareTo(json("{\"S\":\"b\"}")) < 0);
        assertTrue(json("{\"B\":\"AQ==\"}").compareTo(json("{\"B\":\"/w==\"}")) < 0, "unsigned byte order");
        assertTrue(json("{\"S\":\"1\"}").compareOrNull(json("{\"N\":\"1\"}")) == null);
        assertTrue(json("{\"NS\":[\"1\",\"2.50\"]}").deepEquals(json("{\"NS\":[\"2.5\",\"1.0\"]}")));
        assertTrue(msg(() -> json("{\"SS\":[]}")).contains("may not be empty"));
        assertTrue(msg(() -> json("{\"SS\":[\"a\",\"a\"]}")).contains("contains duplicates"));
        assertTrue(msg(() -> json("{\"S\":\"a\",\"N\":\"1\"}")).contains("more than one datatypes"));
        assertTrue(msg(() -> json("{}")).contains("empty"));
        assertTrue(msg(() -> json("{\"NULL\":false}")).contains("Null attribute value types must have the value of true"));
        assertEquals("SerializationException", assertThrows(DynamoException.class, () -> json("{\"B\":\"a\"}")).dynamoErrorType);
        StringBuilder deep = new StringBuilder();
        for (int i = 0; i < 40; i++) deep.append("{\"L\":[");
        deep.append("{\"S\":\"x\"}");
        for (int i = 0; i < 40; i++) deep.append("]}");
        assertTrue(msg(() -> json(deep.toString())).contains("Nesting Levels have exceeded supported limits"));
    }

    @Test
    void itemSizeFollowsTheDeveloperGuideRules() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.ofS("abc"));                     // 2 + 3 = 5
        item.put("n", AttributeValue.ofN("12345"));                    // 1 + ((5+1)/2 + 1) = 5
        item.put("flag", AttributeValue.ofBool(true));                 // 4 + 1 = 5
        item.put("l", AttributeValue.ofL(List.of(AttributeValue.ofS("xy")))); // 1 + (3 + (2+1)) = 7
        assertEquals(22, AttributeValue.itemSize(item));
    }

    private static TableSchema table() {
        TableSchema.Meta m = TableSchema.Meta.defaults("pk", "S", "sk", "N");
        TableSchema.IndexDef gsi = new TableSchema.IndexDef("g", false, List.of(new TableSchema.KeyAttr("gk", "S")), List.of(),
                "ALL", List.of(), "ACTIVE", "dxi_x", 0, 0);
        Map<String, String> at = new LinkedHashMap<>(m.attributeTypes());
        at.put("gk", "S");
        return new TableSchema("t1x", "pk", "S", "sk", "N", "ACTIVE", 0, m.withAttributeTypes(at).withIndexes(List.of(gsi)));
    }

    @Test
    void itemAndKeyValidation() {
        TableSchema t = table();
        Map<String, AttributeValue> ok = new LinkedHashMap<>(Map.of("pk", AttributeValue.ofS("a"), "sk", AttributeValue.ofN("1")));
        ItemValidator.validateItem(t, ok, false);
        assertEquals("One of the required keys was not given a value", msg(() -> ItemValidator.validateItem(t, Map.of("pk", AttributeValue.ofS("a")), false)));
        assertTrue(msg(() -> ItemValidator.validateItem(t, Map.of("pk", AttributeValue.ofS("a"), "sk", AttributeValue.ofS("1")), false)).contains("Type mismatch for key sk"));
        assertTrue(msg(() -> ItemValidator.validateItem(t, Map.of("pk", AttributeValue.ofS(""), "sk", AttributeValue.ofN("1")), false)).contains("cannot contain an empty string value. Key: pk"));
        Map<String, AttributeValue> badIdx = new LinkedHashMap<>(ok);
        badIdx.put("gk", AttributeValue.ofN("5"));
        assertTrue(msg(() -> ItemValidator.validateItem(t, badIdx, false)).contains("Type mismatch for Index Key gk Expected: S Actual: N IndexName: g"));
        Map<String, AttributeValue> emptyIdx = new LinkedHashMap<>(ok);
        emptyIdx.put("gk", AttributeValue.ofS(""));
        assertTrue(msg(() -> ItemValidator.validateItem(t, emptyIdx, false)).contains("IndexName: g, IndexKey: gk"));
        Map<String, AttributeValue> big = new LinkedHashMap<>(ok);
        big.put("blob", AttributeValue.ofS("x".repeat(400 * 1024)));
        assertEquals("Item size has exceeded the maximum allowed size", msg(() -> ItemValidator.validateItem(t, big, false)));
        assertEquals("Item size to update has exceeded the maximum allowed size", msg(() -> ItemValidator.validateItem(t, big, true)));
        Map<String, AttributeValue> longPk = new LinkedHashMap<>(ok);
        longPk.put("pk", AttributeValue.ofS("k".repeat(2049)));
        assertTrue(msg(() -> ItemValidator.validateItem(t, longPk, false)).contains("Size of hashkey has exceeded the maximum size limit"));
        assertEquals("The provided key element does not match the schema", msg(() -> ItemValidator.validateKey(t, Map.of("pk", AttributeValue.ofS("a")))));
        ItemValidator.validateKey(t, ok);
    }

    @Test
    void tableNames() {
        ItemValidator.validateTableName("abc");
        ItemValidator.validateTableName("My.Table-name_1");
        for (String bad : new String[] {"ab", "", "a b c", "x".repeat(256), "tab$le"}) {
            assertTrue(msg(() -> ItemValidator.validateTableName(bad)).startsWith("Invalid table/index name."), bad);
        }
    }

    @Test
    void reservedWords() {
        assertTrue(ReservedWords.isReserved("status"));
        assertTrue(ReservedWords.isReserved("NAME"));
        assertTrue(ReservedWords.isReserved("total"));
        assertTrue(!ReservedWords.isReserved("pk"));
        assertTrue(!ReservedWords.isReserved("customer"));
    }
}
