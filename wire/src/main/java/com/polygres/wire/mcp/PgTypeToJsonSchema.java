package com.polygres.wire.mcp;

import com.google.gson.JsonObject;
import java.util.Locale;
import java.util.Set;

/**
 * Maps an {@code information_schema.parameters.data_type} value to a JSON Schema type for a
 * registered function tool's auto-generated input schema. Narrow, best-effort -- the standard
 * scalar types map cleanly; anything else (enums, composite types, arrays) falls back to
 * {@code string}, the same "don't guess at a confident-but-wrong shape" posture as this project's
 * SQL dialect translation rules (a caller can still pass a string value for an unmapped type;
 * Postgres's own cast/parse on the backend is the real source of truth either way).
 */
final class PgTypeToJsonSchema {

    private static final Set<String> INTEGER_TYPES = Set.of("smallint", "integer", "bigint");
    private static final Set<String> NUMBER_TYPES = Set.of("numeric", "real", "double precision", "decimal");

    static JsonObject map(String pgDataType) {
        JsonObject schema = new JsonObject();
        String type = pgDataType == null ? "" : pgDataType.toLowerCase(Locale.ROOT);
        if (INTEGER_TYPES.contains(type)) {
            schema.addProperty("type", "integer");
        } else if (NUMBER_TYPES.contains(type)) {
            schema.addProperty("type", "number");
        } else if ("boolean".equals(type)) {
            schema.addProperty("type", "boolean");
        } else if ("json".equals(type) || "jsonb".equals(type)) {
            schema.addProperty("type", "object");
        } else if ("array".equals(type)) {
            schema.addProperty("type", "array");
        } else {
            // text, character varying, uuid, date, timestamp[tz], USER-DEFINED (enums/composites), ...
            schema.addProperty("type", "string");
        }
        return schema;
    }

    private PgTypeToJsonSchema() {
    }
}
