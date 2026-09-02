package com.sayonora.wire.mcp;

import com.sayonora.wire.core.ErrorCatalog;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class PgFunctionIntrospector {

    record ParamDef(String name, int position, String pgType, String mode) {
    }

    record FunctionSignature(String schema, String name, List<ParamDef> params, boolean returnsSet, boolean isProcedure) {
    }

    static FunctionSignature introspect(Connection conn, String schema, String functionName) throws SQLException {
        String specificName;
        boolean isProcedure;
        boolean returnsSet;
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT specific_name, routine_type, type_udt_name "
                        + "FROM information_schema.routines "
                        + "WHERE routine_schema = ? AND routine_name = ? "
                        + "ORDER BY specific_name LIMIT 1")) {
            stmt.setString(1, schema);
            stmt.setString(2, functionName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw ErrorCatalog.sqlExceptionWithState("ERR_FUNCTION_NOT_FOUND", "42883", schema, functionName);
                }
                specificName = rs.getString("specific_name");
                isProcedure = "PROCEDURE".equalsIgnoreCase(rs.getString("routine_type"));
            }
        }
        
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT p.proretset FROM pg_proc p "
                        + "JOIN pg_namespace n ON p.pronamespace = n.oid "
                        + "WHERE n.nspname = ? AND p.proname = ? LIMIT 1")) {
            stmt.setString(1, schema);
            stmt.setString(2, functionName);
            try (ResultSet rs = stmt.executeQuery()) {
                returnsSet = rs.next() && rs.getBoolean("proretset");
            }
        }

        List<ParamDef> params = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT parameter_name, ordinal_position, data_type, parameter_mode "
                        + "FROM information_schema.parameters "
                        + "WHERE specific_schema = ? AND specific_name = ? "
                        + "ORDER BY ordinal_position")) {
            stmt.setString(1, schema);
            stmt.setString(2, specificName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("parameter_name");
                    if (name == null || name.isBlank()) {
                        name = "arg" + rs.getInt("ordinal_position");
                    }
                    params.add(new ParamDef(name, rs.getInt("ordinal_position"), rs.getString("data_type"),
                            rs.getString("parameter_mode")));
                }
            }
        }
        return new FunctionSignature(schema, functionName, params, returnsSet, isProcedure);
    }

    private PgFunctionIntrospector() {
    }
}
