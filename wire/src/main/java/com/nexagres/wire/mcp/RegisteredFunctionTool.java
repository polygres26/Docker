package com.nexagres.wire.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

record RegisteredFunctionTool(String toolName, String description, PgFunctionIntrospector.FunctionSignature signature,
        JsonObject inputSchema) {

    static RegisteredFunctionTool introspect(Connection conn, String entry) throws SQLException {
        int eq = entry.indexOf('=');
        if (eq <= 0) {
            throw new IllegalArgumentException(
                    "malformed POLYWIRE_MCP_TOOLS entry (expected toolName=schema.function|description): " + entry);
        }
        String toolName = entry.substring(0, eq).trim();
        String rest = entry.substring(eq + 1);
        int pipe = rest.indexOf('|');
        String qualifiedFunction = (pipe < 0 ? rest : rest.substring(0, pipe)).trim();
        String description = pipe < 0 ? ("Calls " + qualifiedFunction) : rest.substring(pipe + 1).trim();

        String schema = "public";
        String functionName = qualifiedFunction;
        if (qualifiedFunction.contains(".")) {
            String[] parts = qualifiedFunction.split("\\.", 2);
            schema = parts[0];
            functionName = parts[1];
        }

        PgFunctionIntrospector.FunctionSignature signature = PgFunctionIntrospector.introspect(conn, schema, functionName);
        return new RegisteredFunctionTool(toolName, description, signature, buildInputSchema(signature));
    }

    private static JsonObject buildInputSchema(PgFunctionIntrospector.FunctionSignature signature) {
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "object");
        JsonObject properties = new JsonObject();
        JsonArray required = new JsonArray();
        for (PgFunctionIntrospector.ParamDef param : signature.params()) {
            
            if ("OUT".equalsIgnoreCase(param.mode())) {
                continue;
            }
            properties.add(param.name(), PgTypeToJsonSchema.map(param.pgType()));
            required.add(param.name());
        }
        schema.add("properties", properties);
        if (!required.isEmpty()) {
            schema.add("required", required);
        }
        return schema;
    }
}
