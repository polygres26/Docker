package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates incoming MCP {@code tools/call} arguments against the tool's own advertised JSON
 * Schema -- the same {@code inputSchema} JsonObject already generated for {@code tools/list},
 * which was previously never actually checked against (argument handling was ad hoc presence
 * checks like {@code ToolSchemas.requireString}/{@code requireString}, real but narrower).
 *
 * <p>Deliberately scoped to exactly the JSON Schema SHAPE this codebase's own schema generators
 * ({@code PgTypeToJsonSchema}, {@code ToolSchemas}, {@code RegisteredFunctionTool}, and each
 * {@code BackendToolProvider}'s own {@code tools()}) ever actually produce -- confirmed by reading
 * every one of them before writing this: always a flat {@code {type: object, properties: {name:
 * {type: ...}}, required: [...]}} shape, with property schemas never recursively nested beyond one
 * level (the only nesting anywhere is {@code ToolSchemas.strings()}'s {@code {type: array, items:
 * {type: string}}}). This is NOT a general-purpose JSON Schema (draft-07 etc.) implementation --
 * no {@code $ref}, no {@code oneOf}/{@code anyOf}, no {@code pattern}/{@code format}/{@code
 * minimum}/{@code maximum}. That is a real, disclosed scope limit, not an oversight: adding a
 * general JSON Schema library is a separate, larger decision, and every schema this codebase
 * actually emits today fits the narrower shape validated here.
 *
 * <p>{@code additionalProperties} is never set by any generator in this codebase (confirmed by
 * grep), so an argument not named in {@code properties} is never rejected here either -- matching
 * the schemas' own real, advertised semantics, not inventing a stricter contract than what {@code
 * tools/list} actually promised a caller.
 */
final class JsonSchemaValidator {

    private JsonSchemaValidator() {
    }

    /** Returns one human-readable message per violation found; an empty list means the arguments
     * are valid. {@code schema} is trusted (Warp's own generated schema, never attacker-supplied);
     * {@code arguments} is the untrusted, caller-supplied {@code tools/call} arguments object.
     * {@code schema == null} (no schema registered for this tool, e.g. a tool this pass doesn't
     * yet cover) always validates successfully -- absence of a schema is not a violation. */
    static List<String> validate(JsonObject arguments, JsonObject schema) {
        List<String> errors = new ArrayList<>();
        if (schema == null) {
            return errors;
        }
        JsonObject properties = schema.has("properties") && schema.get("properties").isJsonObject()
                ? schema.getAsJsonObject("properties") : new JsonObject();
        if (schema.has("required") && schema.get("required").isJsonArray()) {
            for (JsonElement req : schema.getAsJsonArray("required")) {
                if (!req.isJsonPrimitive()) {
                    continue;
                }
                String name = req.getAsString();
                if (!arguments.has(name) || arguments.get(name).isJsonNull()) {
                    errors.add("missing required argument: " + name);
                }
            }
        }
        for (var entry : arguments.entrySet()) {
            String name = entry.getKey();
            JsonElement value = entry.getValue();
            if (value.isJsonNull() || !properties.has(name)) {
                continue; // unknown/null arguments allowed -- no schema here sets additionalProperties: false
            }
            JsonElement propertySchemaEl = properties.get(name);
            if (!propertySchemaEl.isJsonObject()) {
                continue;
            }
            JsonObject propertySchema = propertySchemaEl.getAsJsonObject();
            String expectedType = propertySchema.has("type") && propertySchema.get("type").isJsonPrimitive()
                    ? propertySchema.get("type").getAsString() : null;
            if (expectedType == null) {
                continue;
            }
            String mismatch = typeMismatch(name, value, expectedType);
            if (mismatch != null) {
                errors.add(mismatch);
                continue;
            }
            if ("array".equals(expectedType) && value.isJsonArray() && propertySchema.has("items")
                    && propertySchema.get("items").isJsonObject()) {
                validateArrayItems(name, value.getAsJsonArray(), propertySchema.getAsJsonObject("items"), errors);
            }
        }
        return errors;
    }

    private static void validateArrayItems(String name, JsonArray items, JsonObject itemSchema, List<String> errors) {
        String itemType = itemSchema.has("type") && itemSchema.get("type").isJsonPrimitive()
                ? itemSchema.get("type").getAsString() : null;
        if (itemType == null) {
            return;
        }
        for (int i = 0; i < items.size(); i++) {
            String itemMismatch = typeMismatch(name + "[" + i + "]", items.get(i), itemType);
            if (itemMismatch != null) {
                errors.add(itemMismatch);
            }
        }
    }

    /**
     * Real, deliberate leniency: an MCP {@code tools/call} argument arriving as a JSON STRING that
     * still represents the expected primitive type (a number, or {@code "true"}/{@code "false"})
     * is accepted, matching how this codebase's OWN existing argument handling already works --
     * {@code ToolSchemas.optLong}/{@code optInt} call Gson's {@code JsonPrimitive#getAsInt()}
     * directly (which itself parses a numeric string with no error), and {@code WarpMcpServer}'s
     * own {@code explain_query} handling checks {@code "true".equalsIgnoreCase(arg.getAsString())}
     * for a boolean-shaped argument. A stricter validator that rejected these would create real,
     * newly-broken tool calls that worked before this class existed -- not a real correctness win.
     */
    private static String typeMismatch(String name, JsonElement value, String expectedType) {
        boolean ok = switch (expectedType) {
            // A number or boolean value's string form is always acceptable where a string is
            // expected too -- Gson's own getAsString() never fails on any primitive.
            case "string" -> value.isJsonPrimitive();
            case "boolean" -> value.isJsonPrimitive() && (value.getAsJsonPrimitive().isBoolean() || isBooleanString(value));
            case "number" -> value.isJsonPrimitive() && (value.getAsJsonPrimitive().isNumber() || isNumericString(value));
            case "integer" -> value.isJsonPrimitive() && isIntegral(value);
            case "array" -> value.isJsonArray();
            case "object" -> value.isJsonObject();
            // An unrecognized type keyword is never enforced -- fail open (accept), not closed, since
            // every real type keyword this codebase's own generators emit is already handled above.
            default -> true;
        };
        return ok ? null : "argument \"" + name + "\" must be of type " + expectedType + ", got " + describeActualType(value);
    }

    private static boolean isBooleanString(JsonElement value) {
        var p = value.getAsJsonPrimitive();
        if (!p.isString()) {
            return false;
        }
        String s = p.getAsString();
        return "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s);
    }

    private static boolean isNumericString(JsonElement value) {
        var p = value.getAsJsonPrimitive();
        if (!p.isString()) {
            return false;
        }
        try {
            Double.parseDouble(p.getAsString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isIntegral(JsonElement value) {
        var p = value.getAsJsonPrimitive();
        if (p.isNumber()) {
            double d = p.getAsDouble();
            return !Double.isInfinite(d) && !Double.isNaN(d) && d == Math.rint(d);
        }
        if (p.isString()) {
            try {
                Long.parseLong(p.getAsString().trim());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    private static String describeActualType(JsonElement value) {
        if (value.isJsonArray()) {
            return "array";
        }
        if (value.isJsonObject()) {
            return "object";
        }
        if (value.isJsonNull()) {
            return "null";
        }
        var primitive = value.getAsJsonPrimitive();
        if (primitive.isBoolean()) {
            return "boolean";
        }
        if (primitive.isNumber()) {
            return "number";
        }
        return "string";
    }
}
