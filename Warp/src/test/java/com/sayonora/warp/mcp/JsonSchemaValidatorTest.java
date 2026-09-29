package com.sayonora.warp.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link JsonSchemaValidator} -- exercised against real schema shapes this
 * codebase's own generators (PgTypeToJsonSchema, ToolSchemas, RegisteredFunctionTool) actually
 * produce, not synthetic/invented ones. */
class JsonSchemaValidatorTest {

    private static JsonObject objectSchema(List<String> required, Object... nameTypePairs) {
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "object");
        JsonObject properties = new JsonObject();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            JsonObject prop = new JsonObject();
            prop.addProperty("type", (String) nameTypePairs[i + 1]);
            properties.add((String) nameTypePairs[i], prop);
        }
        schema.add("properties", properties);
        JsonArray req = new JsonArray();
        required.forEach(req::add);
        schema.add("required", req);
        return schema;
    }

    private static JsonObject args(Object... nameValuePairs) {
        JsonObject o = new JsonObject();
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            String name = (String) nameValuePairs[i];
            Object v = nameValuePairs[i + 1];
            if (v instanceof String s) {
                o.addProperty(name, s);
            } else if (v instanceof Number n) {
                o.addProperty(name, n);
            } else if (v instanceof Boolean b) {
                o.addProperty(name, b);
            }
        }
        return o;
    }

    @Test
    void aNullSchemaAlwaysValidates() {
        assertTrue(JsonSchemaValidator.validate(new JsonObject(), null).isEmpty());
    }

    @Test
    void theExecuteSqlSchemaAcceptsARealSqlArgument() {
        // The real, literal shape objectSchema(Map.of("sql", stringSchema(...)), List.of("sql"))
        // produces for execute_sql.
        JsonObject schema = objectSchema(List.of("sql"), "sql", "string");
        assertTrue(JsonSchemaValidator.validate(args("sql", "select 1"), schema).isEmpty());
    }

    @Test
    void aMissingRequiredArgumentIsRejectedWithAClearMessage() {
        JsonObject schema = objectSchema(List.of("sql"), "sql", "string");
        List<String> errors = JsonSchemaValidator.validate(new JsonObject(), schema);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("missing required argument: sql"));
    }

    @Test
    void aWrongTypeIsRejected() {
        // A bare JSON object/array where a string is expected is a genuine, unrecoverable
        // mismatch (Gson's getAsString() only ever works on a JsonPrimitive) -- unlike a number
        // or boolean value, which this validator deliberately accepts for a string-typed argument
        // (see numberOrBooleanIsAcceptedWhereAStringIsExpected below).
        JsonObject schema = objectSchema(List.of("sql"), "sql", "string");
        JsonObject a = new JsonObject();
        a.add("sql", new JsonObject());
        List<String> errors = JsonSchemaValidator.validate(a, schema);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("must be of type string"), errors.get(0));
    }

    @Test
    void numberOrBooleanIsAcceptedWhereAStringIsExpected() {
        // Real, deliberate leniency matching this codebase's own existing argument handling:
        // Gson's JsonPrimitive#getAsString() never fails on a number/boolean primitive, and
        // ToolSchemas.requireString/etc. already rely on that -- rejecting it here would newly
        // break tool calls that worked before this validator existed.
        JsonObject schema = objectSchema(List.of(), "value", "string");
        assertTrue(JsonSchemaValidator.validate(args("value", 42), schema).isEmpty());
        assertTrue(JsonSchemaValidator.validate(args("value", true), schema).isEmpty());
    }

    @Test
    void aStringEncodedNumberIsAcceptedWhereAnIntegerIsExpected() {
        // Real, deliberate leniency: ToolSchemas.optLong/optInt call Gson's getAsInt()/getAsLong()
        // directly, which already parses a numeric JSON string with no error -- an agent sending
        // "limit": "50" instead of "limit": 50 already worked before this validator existed.
        JsonObject schema = objectSchema(List.of("limit"), "limit", "integer");
        assertTrue(JsonSchemaValidator.validate(args("limit", "50"), schema).isEmpty());
        List<String> errors = JsonSchemaValidator.validate(args("limit", "not-a-number"), schema);
        assertEquals(1, errors.size());
    }

    @Test
    void aStringEncodedBooleanIsAcceptedWhereABooleanIsExpected() {
        // Real, deliberate leniency: WarpMcpServer's own explain_query handling already checks
        // "true".equalsIgnoreCase(arg.getAsString()) for a boolean-shaped argument.
        JsonObject schema = objectSchema(List.of(), "analyze", "boolean");
        assertTrue(JsonSchemaValidator.validate(args("analyze", "true"), schema).isEmpty());
        assertTrue(JsonSchemaValidator.validate(args("analyze", "FALSE"), schema).isEmpty());
        List<String> errors = JsonSchemaValidator.validate(args("analyze", "maybe"), schema);
        assertEquals(1, errors.size());
    }

    @Test
    void anUnknownArgumentIsAllowed() {
        // No schema this codebase generates ever sets additionalProperties: false.
        JsonObject schema = objectSchema(List.of(), "sql", "string");
        JsonObject a = args("sql", "select 1");
        a.addProperty("extra", "whatever");
        assertTrue(JsonSchemaValidator.validate(a, schema).isEmpty());
    }

    @Test
    void integerTypeAcceptsARealIntegerButRejectsAFraction() {
        JsonObject schema = objectSchema(List.of("id"), "id", "integer");
        assertTrue(JsonSchemaValidator.validate(args("id", 5), schema).isEmpty());
        assertTrue(JsonSchemaValidator.validate(args("id", 5.0), schema).isEmpty(), "5.0 is integral");
        List<String> errors = JsonSchemaValidator.validate(args("id", 5.5), schema);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("must be of type integer"));
    }

    @Test
    void numberTypeAcceptsBothIntegersAndFractions() {
        JsonObject schema = objectSchema(List.of("amount"), "amount", "number");
        assertTrue(JsonSchemaValidator.validate(args("amount", 5), schema).isEmpty());
        assertTrue(JsonSchemaValidator.validate(args("amount", 5.5), schema).isEmpty());
    }

    @Test
    void booleanTypeRejectsAStringThatDoesNotLookLikeABoolean() {
        JsonObject schema = objectSchema(List.of(), "flag", "boolean");
        assertTrue(JsonSchemaValidator.validate(args("flag", true), schema).isEmpty());
        List<String> errors = JsonSchemaValidator.validate(args("flag", "not-a-boolean"), schema);
        assertEquals(1, errors.size());
    }

    @Test
    void arrayTypeValidatesEachStringItemsElement() {
        // The real ToolSchemas.strings() shape: {type: array, items: {type: string}}.
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "object");
        JsonObject properties = new JsonObject();
        JsonObject tagsSchema = new JsonObject();
        tagsSchema.addProperty("type", "array");
        JsonObject items = new JsonObject();
        items.addProperty("type", "string");
        tagsSchema.add("items", items);
        properties.add("tags", tagsSchema);
        schema.add("properties", properties);

        JsonObject validArgs = new JsonObject();
        JsonArray validTags = new JsonArray();
        validTags.add("a");
        validTags.add("b");
        validArgs.add("tags", validTags);
        assertTrue(JsonSchemaValidator.validate(validArgs, schema).isEmpty());

        JsonObject invalidArgs = new JsonObject();
        JsonArray invalidTags = new JsonArray();
        invalidTags.add("a");
        invalidTags.add(new JsonObject()); // an object can never satisfy getAsString() -- a real mismatch
        invalidArgs.add("tags", invalidTags);
        List<String> errors = JsonSchemaValidator.validate(invalidArgs, schema);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("tags[1]"), errors.get(0));
    }

    @Test
    void aBareObjectOrArrayTypeWithNoNestedDetailAcceptsAnyShape() {
        // The real PgTypeToJsonSchema gap: a jsonb/array Postgres param gets a bare {"type":
        // "object"}/{"type":"array"} with no properties/items at all -- this must not reject
        // anything of the right coarse kind, since there's no finer detail to check against.
        JsonObject schema = objectSchema(List.of(), "meta", "object");
        JsonObject metaValue = new JsonObject();
        metaValue.addProperty("anything", "goes");
        JsonObject a = new JsonObject();
        a.add("meta", metaValue);
        assertTrue(JsonSchemaValidator.validate(a, schema).isEmpty());
    }

    @Test
    void aNullArgumentValueForAnOptionalPropertyIsIgnoredNotRejected() {
        JsonObject schema = objectSchema(List.of(), "note", "string");
        JsonObject a = new JsonObject();
        a.add("note", com.google.gson.JsonNull.INSTANCE);
        assertTrue(JsonSchemaValidator.validate(a, schema).isEmpty());
    }

    @Test
    void multipleViolationsAreAllReportedTogether() {
        JsonObject schema = objectSchema(List.of("sql", "limit"), "sql", "string", "limit", "integer");
        List<String> errors = JsonSchemaValidator.validate(args("limit", "not-a-number"), schema);
        assertEquals(2, errors.size(), errors.toString());
    }
}
