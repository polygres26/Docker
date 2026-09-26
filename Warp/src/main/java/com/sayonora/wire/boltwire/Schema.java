package com.sayonora.wire.boltwire;

import com.sayonora.wire.boltwire.Cy.SchemaCmd;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Constraint / index DDL and SHOW commands. Constraints and indexes are real Postgres (unique) partial expression
 * indexes on the graph tables; the catalog row in {@code warp_graph_schema} carries the Neo4j-facing description. */
final class Schema {

    private final Exec x;

    Schema(Exec x) {
        this.x = x;
    }

    private record Row(String name, String kind, String entity, String labelOrType, List<String> props, String constraintType,
            String indexType, String pgIndex) {
    }

    Executor.Result run(SchemaCmd sc, Executor ex) {
        try {
            x.flush();
            switch (sc.kind()) {
                case "CREATE_CONSTRAINT" -> createConstraint(sc);
                case "CREATE_INDEX" -> createIndex(sc);
                case "DROP_CONSTRAINT" -> drop(sc, true);
                case "DROP_INDEX" -> drop(sc, false);
                case "SHOW" -> {
                    return show(sc, ex);
                }
                default -> throw CypherException.syntax("unsupported schema command " + sc.kind());
            }
            return new Executor.Result(List.of(), List.of());
        } catch (SQLException e) {
            throw x.sql(e);
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

    private List<Row> all() throws SQLException {
        List<Row> out = new ArrayList<>();
        try (PreparedStatement ps = x.conn.prepareStatement(
                "SELECT name, kind, entity, label_or_type, props, constraint_type, index_type, pg_index "
                        + "FROM warp_graph_schema ORDER BY created_at, name");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new Row(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        List.of((String[]) rs.getArray(5).getArray()), rs.getString(6), rs.getString(7), rs.getString(8)));
            }
        }
        return out;
    }

    private static String hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String quoteLit(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private String indexDdl(String pgName, boolean unique, String entity, String labelOrType, List<String> props) {
        StringBuilder cols = new StringBuilder();
        for (String p : props) {
            if (cols.length() > 0) {
                cols.append(", ");
            }
            cols.append("(properties->").append(quoteLit(p)).append(")");
        }
        String table = entity.equals("NODE") ? "warp_graph_nodes" : "warp_graph_edges";
        String where = entity.equals("NODE") ? " WHERE labels @> ARRAY[" + quoteLit(labelOrType) + "]::text[]"
                : " WHERE type = " + quoteLit(labelOrType);
        return "CREATE " + (unique ? "UNIQUE " : "") + "INDEX " + pgName + " ON " + table + " (" + cols + ")" + where;
    }

    private static String schemaText(String entity, String label, List<String> props) {
        String p = String.join(", ", props);
        return entity.equals("NODE") ? "(:" + label + " {" + p + "})" : "()-[:" + label + " {" + p + "}]-()";
    }

    // ------------------------------------------------------------------------------------------ create / drop

    private void createConstraint(SchemaCmd sc) throws SQLException {
        String ctype = sc.constraintType();
        String label = sc.label();
        if (label == null || sc.properties().isEmpty()) {
            throw CypherException.syntax("Invalid constraint: a label and at least one property are required");
        }
        String entity = sc.entity() == null ? "NODE" : sc.entity();
        if (!"UNIQUENESS".equals(ctype)) {
            String what = switch (ctype == null ? "" : ctype) {
                case "EXISTENCE" -> "Property existence constraint";
                case "NODE_KEY" -> "Node key constraint";
                case "REL_KEY" -> "Relationship key constraint";
                default -> "Property type constraint";
            };
            throw new CypherException("Neo.DatabaseError.Schema.ConstraintCreationFailed",
                    "Unable to create Constraint( type='" + ctype + "', schema=" + schemaText(entity, label, sc.properties())
                            + " ):\n" + what + " requires Neo4j Enterprise Edition. It is not available in Warp.");
        }
        List<Row> rows = all();
        String name = sc.name() != null ? sc.name() : "constraint_" + hash(entity + label + sc.properties()).substring(0, 8);
        for (Row r : rows) {
            boolean sameName = r.name().equals(name);
            boolean sameSchema = r.entity().equals(entity) && r.labelOrType().equals(label) && r.props().equals(sc.properties());
            if (sameName || (sameSchema && r.kind().equals("CONSTRAINT") && "UNIQUENESS".equals(r.constraintType()))) {
                if (sc.ifNotExists()) {
                    return;
                }
                if (sameName && !sameSchema) {
                    throw new CypherException("Neo.ClientError.Schema.ConstraintWithNameAlreadyExists",
                            "There already exists a constraint called '" + name + "'.");
                }
                throw new CypherException("Neo.ClientError.Schema.ConstraintAlreadyExists",
                        "An equivalent constraint already exists, '" + describe(r) + "'.");
            }
            if (sameSchema && r.kind().equals("INDEX")) {
                if (sc.ifNotExists()) {
                    return;
                }
                throw new CypherException("Neo.ClientError.Schema.IndexAlreadyExists",
                        "There already exists an index " + schemaText(entity, label, sc.properties())
                                + ". A constraint cannot be created until the index has been dropped.");
            }
        }
        String pg = "wgc_" + hash(name).substring(0, 24);
        try (Statement st = x.conn.createStatement()) {
            st.execute(indexDdl(pg, true, entity, label, sc.properties()));
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState())) {
                throw new CypherException("Neo.DatabaseError.Schema.ConstraintCreationFailed",
                        "Unable to create Constraint( name='" + name + "', type='UNIQUENESS', schema="
                                + schemaText(entity, label, sc.properties()) + " ): existing data violates the constraint");
            }
            throw e;
        }
        insertCatalog(name, "CONSTRAINT", entity, label, sc.properties(), "UNIQUENESS", "RANGE", pg);
        x.stats.constraintsAdded++;
    }

    private void createIndex(SchemaCmd sc) throws SQLException {
        String itype = sc.indexType() == null ? "RANGE" : sc.indexType();
        String entity = sc.entity() == null ? "NODE" : sc.entity();
        if (itype.equals("FULLTEXT") || itype.equals("VECTOR")) {
            throw new CypherException("Neo.DatabaseError.Schema.IndexCreationFailed",
                    itype.toLowerCase(Locale.ROOT) + " indexes are not supported by Warp (use a RANGE or TEXT index)");
        }
        if (itype.equals("LOOKUP")) {
            return;
        }
        if (sc.label() == null || sc.properties().isEmpty()) {
            throw CypherException.syntax("Invalid index: a label and at least one property are required");
        }
        String label = sc.label();
        List<Row> rows = all();
        String name = sc.name() != null ? sc.name() : "index_" + hash(entity + label + sc.properties() + itype).substring(0, 8);
        for (Row r : rows) {
            boolean sameName = r.name().equals(name);
            boolean sameSchema = r.entity().equals(entity) && r.labelOrType().equals(label) && r.props().equals(sc.properties());
            if (sameName || (sameSchema && (r.kind().equals("INDEX") ? r.indexType().equals(itype) : itype.equals("RANGE")))) {
                if (sc.ifNotExists()) {
                    return;
                }
                if (sameSchema && r.kind().equals("CONSTRAINT")) {
                    throw new CypherException("Neo.ClientError.Schema.ConstraintAlreadyExists",
                            "There is a uniqueness constraint on " + schemaText(entity, label, sc.properties())
                                    + ", so an index is already created that matches this.");
                }
                if (sameName && !sameSchema) {
                    throw new CypherException("Neo.ClientError.Schema.IndexWithNameAlreadyExists",
                            "There already exists an index called '" + name + "'.");
                }
                throw new CypherException("Neo.ClientError.Schema.IndexAlreadyExists",
                        "An equivalent index already exists, '" + describe(r) + "'.");
            }
        }
        String pg = "wgi_" + hash(name).substring(0, 24);
        try (Statement st = x.conn.createStatement()) {
            st.execute(indexDdl(pg, false, entity, label, sc.properties()));
        }
        insertCatalog(name, "INDEX", entity, label, sc.properties(), null, itype, pg);
        x.stats.indexesAdded++;
    }

    private void insertCatalog(String name, String kind, String entity, String label, List<String> props, String ctype,
            String itype, String pg) throws SQLException {
        try (PreparedStatement ps = x.conn.prepareStatement(
                "INSERT INTO warp_graph_schema (name, kind, entity, label_or_type, props, constraint_type, index_type, pg_index) "
                        + "VALUES (?, ?, ?, ?, ?::text[], ?, ?, ?)")) {
            ps.setString(1, name);
            ps.setString(2, kind);
            ps.setString(3, entity);
            ps.setString(4, label);
            ps.setString(5, PgGraphStore.pgTextArray(props));
            ps.setString(6, ctype);
            ps.setString(7, itype);
            ps.setString(8, pg);
            ps.executeUpdate();
        }
    }

    private void drop(SchemaCmd sc, boolean constraint) throws SQLException {
        for (Row r : all()) {
            if (r.name().equals(sc.name())) {
                if (constraint != r.kind().equals("CONSTRAINT")) {
                    if (constraint) {
                        break;
                    }
                    throw new CypherException("Neo.DatabaseError.Schema.IndexDropFailed",
                            "Unable to drop index called `" + sc.name() + "`. There is a uniqueness constraint "
                                    + "with that name.");
                }
                try (Statement st = x.conn.createStatement()) {
                    st.execute("DROP INDEX IF EXISTS " + r.pgIndex());
                }
                try (PreparedStatement ps = x.conn.prepareStatement("DELETE FROM warp_graph_schema WHERE name = ?")) {
                    ps.setString(1, sc.name());
                    ps.executeUpdate();
                }
                if (constraint) {
                    x.stats.constraintsRemoved++;
                } else {
                    x.stats.indexesRemoved++;
                }
                return;
            }
        }
        if (sc.ifExists()) {
            return;
        }
        if (constraint) {
            throw new CypherException("Neo.DatabaseError.Schema.ConstraintDropFailed",
                    "Unable to drop constraint `" + sc.name() + "`: No such constraint " + sc.name() + ".");
        }
        throw new CypherException("Neo.DatabaseError.Schema.IndexDropFailed",
                "Unable to drop index called `" + sc.name() + "`. No such index.");
    }

    private String describe(Row r) {
        return (r.kind().equals("CONSTRAINT") ? "Constraint" : "Index") + "( name='" + r.name() + "', type='"
                + (r.kind().equals("CONSTRAINT") ? r.constraintType() : r.indexType()) + "', schema="
                + schemaText(r.entity(), r.labelOrType(), r.props()) + " )";
    }

    /** Describes a unique-index violation the way Neo4j words it (looked up on a side connection: the failed statement
     * left the transaction connection aborted). */
    static String describeUnique(com.sayonora.wire.core.BackendTarget target, String pgIndex, String pgDetail)
            throws SQLException {
        try (java.sql.Connection c = target.open();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT entity, label_or_type, props FROM warp_graph_schema WHERE pg_index = ?")) {
            ps.setString(1, pgIndex);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String label = rs.getString(2);
                String[] props = (String[]) rs.getArray(3).getArray();
                // pgDetail: "Key ((properties -> 'email'))=("a") already exists."
                String value = null;
                if (pgDetail != null) {
                    int i = pgDetail.indexOf(")=(");
                    int j = pgDetail.lastIndexOf(") already exists");
                    if (i >= 0 && j > i) {
                        value = pgDetail.substring(i + 3, j);
                    }
                }
                String shown = value == null ? "?" : value.startsWith("\"") && value.endsWith("\"")
                        ? "'" + value.substring(1, value.length() - 1) + "'" : value;
                String existing = "?";
                try (PreparedStatement q = c.prepareStatement(
                        "SELECT id FROM warp_graph_nodes WHERE labels @> ARRAY[?]::text[] ORDER BY id LIMIT 1")) {
                    q.setString(1, label);
                    try (ResultSet r2 = q.executeQuery()) {
                        if (r2.next()) {
                            existing = String.valueOf(r2.getLong(1));
                        }
                    }
                }
                return "Node(" + existing + ") already exists with label `" + label + "` and property `" + props[0]
                        + "` = " + shown;
            }
        }
    }

    // ------------------------------------------------------------------------------------------ SHOW

    private Executor.Result show(SchemaCmd sc, Executor ex) throws SQLException {
        String what = sc.showWhat();
        List<String> cols;
        List<Map<String, Object>> rows = new ArrayList<>();
        if (what.startsWith("INDEX") || what.startsWith("ALL INDEX") || what.startsWith("RANGE INDEX") || what.startsWith("TEXT INDEX")
                || what.startsWith("POINT INDEX") || what.startsWith("LOOKUP INDEX") || what.startsWith("FULLTEXT INDEX")) {
            cols = List.of("id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties",
                    "indexProvider", "owningConstraint", "lastRead", "readCount");
            long id = 1;
            rows.add(indexRow(id++, "index_" + hash("lookup-node").substring(0, 8), "LOOKUP", "NODE", null, null,
                    "token-lookup-1.0", null));
            rows.add(indexRow(id++, "index_" + hash("lookup-rel").substring(0, 8), "LOOKUP", "RELATIONSHIP", null, null,
                    "token-lookup-1.0", null));
            for (Row r : all()) {
                if (r.kind().equals("INDEX") || r.constraintType() != null) {
                    rows.add(indexRow(id++, r.name(), r.indexType(), r.entity().equals("NODE") ? "NODE" : "RELATIONSHIP",
                            List.of(r.labelOrType()), r.props(), "range-1.0", r.kind().equals("CONSTRAINT") ? r.name() : null));
                }
            }
        } else if (what.startsWith("CONSTRAINT") || what.startsWith("UNIQUE CONSTRAINT") || what.startsWith("ALL CONSTRAINT")) {
            cols = List.of("id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType");
            long id = 1;
            for (Row r : all()) {
                if (r.kind().equals("CONSTRAINT")) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", id++);
                    m.put("name", r.name());
                    m.put("type", r.constraintType());
                    m.put("entityType", r.entity().equals("NODE") ? "NODE" : "RELATIONSHIP");
                    m.put("labelsOrTypes", new ArrayList<Object>(List.of(r.labelOrType())));
                    m.put("properties", new ArrayList<Object>(r.props()));
                    m.put("ownedIndex", r.name());
                    m.put("propertyType", null);
                    rows.add(m);
                }
            }
        } else if (what.startsWith("PROCEDURE")) {
            cols = List.of("name", "description", "mode", "worksOnSystem", "signature", "argumentDescription",
                    "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy",
                    "option");
            for (String p : Procedures.NAMES) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", p);
                m.put("description", "");
                m.put("mode", "READ");
                m.put("worksOnSystem", false);
                m.put("signature", p + "()");
                m.put("argumentDescription", new ArrayList<Object>());
                m.put("returnDescription", new ArrayList<Object>());
                m.put("admin", false);
                m.put("rolesExecution", null);
                m.put("rolesBoostedExecution", null);
                m.put("isDeprecated", false);
                m.put("deprecatedBy", null);
                m.put("option", new LinkedHashMap<String, Object>(Map.of("deprecated", false)));
                rows.add(m);
            }
        } else if (what.startsWith("FUNCTION") || what.startsWith("BUILT IN FUNCTION") || what.startsWith("USER DEFINED FUNCTION")) {
            cols = List.of("name", "category", "description", "signature", "isBuiltIn", "argumentDescription",
                    "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated",
                    "deprecatedBy");
            if (!what.startsWith("USER DEFINED")) {
                for (String f : Funcs.names()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", f);
                    m.put("category", "");
                    m.put("description", "");
                    m.put("signature", f + "(...)");
                    m.put("isBuiltIn", true);
                    m.put("argumentDescription", new ArrayList<Object>());
                    m.put("returnDescription", "ANY");
                    m.put("aggregating", Funcs.isAggregate(f));
                    m.put("rolesExecution", null);
                    m.put("rolesBoostedExecution", null);
                    m.put("isDeprecated", false);
                    m.put("deprecatedBy", null);
                    rows.add(m);
                }
            }
        } else if (what.startsWith("DATABASE") || what.startsWith("DEFAULT DATABASE") || what.startsWith("HOME DATABASE")) {
            cols = List.of("name", "type", "aliases", "access", "address", "role", "writer", "requestedStatus", "currentStatus",
                    "statusMessage", "default", "home", "constituents");
            for (String[] d : new String[][] {{"neo4j", "standard"}, {"system", "system"}}) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", d[0]);
                m.put("type", d[1]);
                m.put("aliases", new ArrayList<Object>());
                m.put("access", "read-write");
                m.put("address", "localhost:7687");
                m.put("role", "primary");
                m.put("writer", true);
                m.put("requestedStatus", "online");
                m.put("currentStatus", "online");
                m.put("statusMessage", "");
                m.put("default", d[0].equals("neo4j"));
                m.put("home", d[0].equals("neo4j"));
                m.put("constituents", new ArrayList<Object>());
                rows.add(m);
            }
        } else if (what.startsWith("TRANSACTION")) {
            cols = List.of("database", "transactionId", "currentQueryId", "connectionId", "clientAddress", "username",
                    "currentQuery", "startTime", "status", "elapsedTime");
        } else {
            throw new CypherException("Neo.ClientError.Statement.SyntaxError", "SHOW " + what + " is not supported by Warp");
        }
        // YIELD / WHERE / RETURN tail: run it through the ordinary pipeline
        boolean hasYield = sc.properties() != null;
        List<String> yields = hasYield ? sc.properties() : cols;
        String tail = sc.options().isEmpty() ? "" : sc.options().get(0);
        if (!hasYield && tail.isEmpty()) {
            List<List<Object>> out = new ArrayList<>();
            for (Map<String, Object> r : rows) {
                List<Object> v = new ArrayList<>();
                for (String c : cols) {
                    v.add(r.get(c));
                }
                out.add(v);
            }
            return new Executor.Result(cols, out);
        }
        StringBuilder q = new StringBuilder("UNWIND $__rows AS __r WITH ");
        List<String> outCols = new ArrayList<>();
        for (int i = 0; i < yields.size(); i++) {
            String y = yields.get(i);
            String col = y, alias = y;
            int as = y.toUpperCase(Locale.ROOT).indexOf(" AS ");
            if (as > 0) {
                col = y.substring(0, as);
                alias = y.substring(as + 4);
            }
            if (!cols.contains(col)) {
                throw CypherException.syntax("Variable `" + col + "` not defined");
            }
            q.append(i > 0 ? ", " : "").append("__r.`").append(col).append("` AS `").append(alias).append('`');
            outCols.add(alias);
        }
        boolean tailHasReturn = tail.toUpperCase(Locale.ROOT).contains("RETURN");
        q.append(' ').append(tail);
        if (!tailHasReturn) {
            q.append(" RETURN ");
            for (int i = 0; i < outCols.size(); i++) {
                q.append(i > 0 ? ", " : "").append('`').append(outCols.get(i)).append('`');
            }
        }
        Cy.Query parsed = CypherParser.parse(q.toString());
        Analyzer.Info pinfo = Analyzer.analyze(parsed);
        Map<String, Object> params = new HashMap<>(x.params);
        List<Object> rowObjs = new ArrayList<>(rows);
        Exec sub = new Exec(x.store, x.target, x.conn, Map.of("__rows", rowObjs));
        return new Executor(sub, pinfo).runQuery(parsed, List.of(new HashMap<>()));
    }

    private Map<String, Object> indexRow(long id, String name, String type, String entity, List<String> labels,
            List<String> props, String provider, String owning) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        m.put("state", "ONLINE");
        m.put("populationPercent", 100.0);
        m.put("type", type);
        m.put("entityType", entity);
        m.put("labelsOrTypes", labels == null ? null : new ArrayList<Object>(labels));
        m.put("properties", props == null ? null : new ArrayList<Object>(props));
        m.put("indexProvider", provider);
        m.put("owningConstraint", owning);
        m.put("lastRead", null);
        m.put("readCount", null);
        return m;
    }
}
