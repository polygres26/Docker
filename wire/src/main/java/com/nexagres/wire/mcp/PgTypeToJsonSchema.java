package com.nexagres.wire.mcp;

import com.google.gson.JsonObject;
import java.util.Locale;
import java.util.Set;

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
            
            schema.addProperty("type", "string");
        }
        return schema;
    }

    private PgTypeToJsonSchema() {
    }
}
