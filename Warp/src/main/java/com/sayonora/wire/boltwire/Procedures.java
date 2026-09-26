package com.sayonora.wire.boltwire;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The built-in procedures Warp answers (db.* and dbms.* introspection). Unknown names fail like Neo4j's ProcedureNotFound. */
final class Procedures {

    private Procedures() {
    }

    record Out(List<String> columns, List<List<Object>> rows) {
    }

    static final List<String> NAMES = List.of("db.labels", "db.relationshipTypes", "db.propertyKeys", "db.ping",
            "db.info", "db.schema.nodeTypeProperties", "db.schema.relTypeProperties", "db.schema.visualization",
            "dbms.components", "dbms.showCurrentUser", "dbms.listConfig", "dbms.queryJmx", "db.awaitIndexes",
            "db.awaitIndex", "db.indexes", "db.constraints");

    static Out call(Executor ex, String name, List<Object> args, boolean implicit) {
        String n = name;
        Exec x = ex.x;
        try {
            switch (n) {
                case "db.labels" -> {
                    expectArgs(name, args, 0);
                    return column("label", strings(x, "SELECT DISTINCT unnest(labels) AS v FROM warp_graph_nodes ORDER BY 1"));
                }
                case "db.relationshipTypes" -> {
                    expectArgs(name, args, 0);
                    return column("relationshipType", strings(x, "SELECT DISTINCT type AS v FROM warp_graph_edges ORDER BY 1"));
                }
                case "db.propertyKeys" -> {
                    expectArgs(name, args, 0);
                    return column("propertyKey", strings(x, "SELECT k AS v FROM (SELECT jsonb_object_keys(properties) AS k "
                            + "FROM warp_graph_nodes UNION SELECT jsonb_object_keys(properties) FROM warp_graph_edges) t ORDER BY 1"));
                }
                case "db.ping" -> {
                    return new Out(List.of("success"), List.of(List.of(true)));
                }
                case "db.info" -> {
                    return new Out(List.of("id", "name", "creationDate"),
                            List.of(List.of("warp-graph", "neo4j", "1970-01-01T00:00:00Z")));
                }
                case "dbms.components" -> {
                    return new Out(List.of("name", "versions", "edition"),
                            List.of(List.of("Neo4j Kernel", List.of(BoltWireSessionHandler.NEO4J_VERSION), "community")));
                }
                case "dbms.showCurrentUser" -> {
                    return new Out(List.of("username", "roles", "flags"), List.of(List.of("neo4j", List.of("PUBLIC"), List.of())));
                }
                case "db.awaitIndexes", "db.awaitIndex" -> {
                    return new Out(List.of(), List.of(List.of()));
                }
                default -> {
                }
            }
        } catch (SQLException e) {
            throw x.sql(e);
        }
        throw new CypherException("Neo.ClientError.Procedure.ProcedureNotFound",
                "There is no procedure with the name `" + name + "` registered for this database instance. Please ensure "
                        + "you've spelled the procedure name correctly and that the procedure is properly deployed.");
    }

    private static void expectArgs(String name, List<Object> args, int n) {
        if (args.size() != n) {
            throw new CypherException(CypherException.SYNTAX, "Procedure call does not provide the required number of arguments: got "
                    + args.size() + " expected " + n + ".");
        }
    }

    private static Out column(String col, List<String> values) {
        List<List<Object>> rows = new ArrayList<>();
        for (String v : values) {
            rows.add(List.of(v));
        }
        return new Out(List.of(col), rows);
    }

    private static List<String> strings(Exec x, String sql) throws SQLException {
        x.flush();
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = x.conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    /** Columns a procedure returns (for static YIELD checking); null when unknown. */
    static List<String> outputs(String name) {
        return switch (name) {
            case "db.labels" -> List.of("label");
            case "db.relationshipTypes" -> List.of("relationshipType");
            case "db.propertyKeys" -> List.of("propertyKey");
            case "db.ping" -> List.of("success");
            case "db.info" -> List.of("id", "name", "creationDate");
            case "dbms.components" -> List.of("name", "versions", "edition");
            case "dbms.showCurrentUser" -> List.of("username", "roles", "flags");
            case "db.awaitIndexes", "db.awaitIndex" -> List.of();
            default -> null;
        };
    }

    static Map<String, Object> unused() {
        return Map.of();
    }
}
