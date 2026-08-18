package com.polygres.wire.mcp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds an MCP tool's JSON Schema from a real Postgres function/procedure's parameters --
 * the PostgREST-precedent mechanic {@link PolyWireMcpServer}'s javadoc describes: introspect the
 * catalog for a callable object's real signature, auto-generate the caller-facing shape from it,
 * rather than an operator hand-writing a schema that could drift from the actual function.
 *
 * <p>Reads {@code information_schema.parameters} (joined to {@code routines} by
 * {@code specific_name}) rather than {@code pg_proc} directly -- the SQL-standard information
 * schema already gives parameter name/position/data-type as plain, portable text, avoiding
 * {@code pg_proc.proargtypes}' OID-array parsing entirely.
 *
 * <p><b>Narrow-slice limit, stated plainly</b>: assumes no overloading (one function per
 * {@code (schema, name)} pair) -- looks up by name, not full signature, so a genuinely overloaded
 * function resolves to whichever the catalog returns first. A deployment relying on overloads
 * needs distinctly-named functions registered instead; this is a real, documented gap, not
 * silently wrong behavior (the function still gets introspected and called correctly, just
 * ambiguously chosen if more than one overload exists).
 */
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
                    throw new SQLException("no such function/procedure: " + schema + "." + functionName, "42883");
                }
                specificName = rs.getString("specific_name");
                isProcedure = "PROCEDURE".equalsIgnoreCase(rs.getString("routine_type"));
            }
        }
        // A SETOF-returning function's data_type is literally "USER-DEFINED"/"record"/a real
        // table-row type rather than a scalar -- information_schema doesn't expose "is this
        // SETOF" as a plain boolean column, so this checks pg_proc.proretset directly instead
        // (the one place this class reaches past information_schema, for exactly the one piece
        // it can't express).
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
                        name = "arg" + rs.getInt("ordinal_position"); // unnamed positional parameter
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
