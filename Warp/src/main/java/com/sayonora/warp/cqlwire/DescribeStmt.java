package com.sayonora.warp.cqlwire;

import com.sayonora.warp.cqlwire.Ast.Describe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** The server side DESCRIBE statement (cqlsh 6 relies on it): CREATE statements rendered exactly like Cassandra's schema element printer. */
final class DescribeStmt {

    private final Engine e;

    DescribeStmt(Engine e) {
        this.e = e;
    }

    private static final List<Result.ColSpec> SHORT = List.of(spec("keyspace_name"), spec("type"), spec("name"));
    private static final List<Result.ColSpec> LONG = List.of(spec("keyspace_name"), spec("type"), spec("name"), spec("create_statement"));

    private static Result.ColSpec spec(String n) {
        return new Result.ColSpec("system", "describe", n, CqlType.TEXT);
    }

    private static byte[] b(String s) {
        return CqlType.TEXT.serialize(s);
    }

    private static Result rows(List<Result.ColSpec> cols, List<String[]> rows) {
        List<byte[][]> out = new ArrayList<>();
        for (String[] r : rows) {
            byte[][] x = new byte[r.length][];
            for (int i = 0; i < r.length; i++) {
                x[i] = b(r[i]);
            }
            out.add(x);
        }
        return Result.rows(cols, out, null);
    }

    // ------------------------------------------------------------------ statement renderers

    static String quote(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    static String ident(String n) {
        return n.matches("[a-z][a-z0-9_]*") && !RESERVED_LIKE.contains(n) ? n : "\"" + n.replace("\"", "\"\"") + "\"";
    }

    private static final java.util.Set<String> RESERVED_LIKE = java.util.Set.of("select", "from", "where", "table", "keyspace", "index", "primary",
            "key", "order", "by", "and", "or", "not", "in", "to", "update", "insert", "delete", "into", "set", "with", "using", "limit", "token");

    private static String mapText(Map<?, ?> m) {
        StringBuilder b = new StringBuilder("{");
        boolean first = true;
        for (var en : new TreeMap<String, Object>(toStringKeys(m)).entrySet()) {
            b.append(first ? "" : ", ").append(quote(en.getKey())).append(": ").append(quote(String.valueOf(en.getValue())));
            first = false;
        }
        return b.append('}').toString();
    }

    private static Map<String, Object> toStringKeys(Map<?, ?> m) {
        Map<String, Object> o = new java.util.LinkedHashMap<>();
        m.forEach((k, v) -> o.put(String.valueOf(k), v));
        return o;
    }

    static String keyspaceStatement(Schema.Keyspace k) {
        Map<String, String> repl = new TreeMap<>();
        k.replication.forEach((a, b) -> repl.put(a, a.equals("class") ? b.substring(b.lastIndexOf('.') + 1) : b));
        return "CREATE KEYSPACE " + ident(k.name) + " WITH replication = " + mapText(repl) + "  AND durable_writes = " + k.durableWrites + ";";
    }

    private static String typeText(CqlType t, String ks) {
        String s = t.cql();
        return s;
    }

    static String typeStatement(Schema.Udt u) {
        StringBuilder b = new StringBuilder("CREATE TYPE " + ident(u.ks) + "." + ident(u.name) + " (");
        for (int i = 0; i < u.fields.size(); i++) {
            b.append(i > 0 ? "," : "").append("\n    ").append(ident(u.fields.get(i))).append(' ').append(u.types.get(i).cql());
        }
        return b.append("\n);").toString();
    }

    static String tableStatement(Schema.Table t) {
        Map<String, Object> o = SysTables.effective(t);
        StringBuilder b = new StringBuilder("CREATE TABLE " + ident(t.ks) + "." + ident(t.name) + " (");
        boolean inlinePk = t.partition.size() == 1 && t.clustering.isEmpty();
        for (Schema.Column c : t.selectOrder) {
            b.append("\n    ").append(ident(c.name)).append(' ').append(c.type.cql());
            if (c.kind == Schema.Kind.STATIC) {
                b.append(" static");
            }
            if (inlinePk && c.kind == Schema.Kind.PARTITION_KEY) {
                b.append(" PRIMARY KEY");
            }
            b.append(',');
        }
        if (!inlinePk) {
            b.append("\n    PRIMARY KEY (");
            if (t.partition.size() > 1) {
                b.append('(').append(String.join(", ", t.partition.stream().map(c -> ident(c.name)).toList())).append(')');
            } else {
                b.append(ident(t.partition.get(0).name));
            }
            for (Schema.Column c : t.clustering) {
                b.append(", ").append(ident(c.name));
            }
            b.append(')');
        } else {
            b.setLength(b.length() - 1);
        }
        b.append("\n) WITH ");
        boolean first = true;
        if (!t.clustering.isEmpty()) {
            b.append("CLUSTERING ORDER BY (");
            b.append(String.join(", ", t.clustering.stream().map(c -> ident(c.name) + (c.desc ? " DESC" : " ASC")).toList())).append(')');
            first = false;
        }
        String[] order = {"additional_write_policy", "allow_auto_snapshot", "bloom_filter_fp_chance", "caching", "cdc", "comment", "compaction",
            "compression", "memtable", "crc_check_chance", "default_time_to_live", "extensions", "gc_grace_seconds", "incremental_backups",
            "max_index_interval", "memtable_flush_period_in_ms", "min_index_interval", "read_repair", "speculative_retry"};
        for (String k : order) {
            Object v = o.get(k);
            b.append(first ? "" : "\n    AND ").append(k).append(" = ");
            first = false;
            if (v instanceof Map<?, ?> m) {
                b.append(mapText(m));
            } else if (v instanceof String s) {
                b.append(quote(s));
            } else {
                b.append(v);
            }
        }
        return b.append(';').toString();
    }

    static String indexStatement(Schema.Table t, Schema.Index i) {
        Schema.Column c = t.col(i.column);
        String target = i.kind.equals("values") && !(c != null && c.type.isMultiCell()) ? ident(i.column) : i.kind + "(" + ident(i.column) + ")";
        if (i.custom != null) {
            return "CREATE CUSTOM INDEX " + ident(i.name) + " ON " + ident(t.ks) + "." + ident(t.name) + " (" + target + ") USING " + quote(i.custom) + ";";
        }
        return "CREATE INDEX " + ident(i.name) + " ON " + ident(t.ks) + "." + ident(t.name) + " (" + target + ");";
    }

    // ------------------------------------------------------------------ statement

    private void keyspaceRows(Schema.Keyspace k, List<String[]> out, boolean only) {
        out.add(new String[] {k.name, "keyspace", k.name, keyspaceStatement(k)});
        if (only) {
            return;
        }
        for (Schema.Udt u : orderedTypes(k)) {
            out.add(new String[] {k.name, "type", u.name, typeStatement(u)});
        }
        for (Schema.Table t : sortedTables(k)) {
            tableRows(t, out);
        }
    }

    private static List<Schema.Table> sortedTables(Schema.Keyspace k) {
        List<Schema.Table> l = new ArrayList<>(k.tables.values());
        l.sort((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.name, b.name));
        return l;
    }

    /** Types in dependency order (a type after the types it uses). */
    private static List<Schema.Udt> orderedTypes(Schema.Keyspace k) {
        List<Schema.Udt> out = new ArrayList<>();
        java.util.Set<String> done = new java.util.HashSet<>();
        List<Schema.Udt> pending = new ArrayList<>(k.types.values());
        while (!pending.isEmpty()) {
            boolean progressed = false;
            for (var it = pending.iterator(); it.hasNext();) {
                Schema.Udt u = it.next();
                boolean ready = true;
                for (CqlType t : u.types) {
                    String q = t.qualified();
                    for (Schema.Udt o : pending) {
                        if (o != u && q.contains("\"" + k.name + "\".\"" + o.name + "\"")) {
                            ready = false;
                        }
                    }
                }
                if (ready) {
                    out.add(u);
                    it.remove();
                    progressed = true;
                }
            }
            if (!progressed) {
                out.addAll(pending);
                break;
            }
        }
        return out;
    }

    private void tableRows(Schema.Table t, List<String[]> out) {
        out.add(new String[] {t.ks, "table", ident(t.name), tableStatement(t)});
        for (Schema.Index i : t.indexes) {
            out.add(new String[] {t.ks, "index", ident(i.name), indexStatement(t, i)});
        }
    }

    Result describe(Describe d, ClientState cs) {
        Schema s = e.catalog.schema();
        String cur = cs.keyspace;
        switch (d.what()) {
            case "cluster" -> {
                if (cur == null || s.ks(cur) == null) {
                    return rows(List.of(spec("cluster"), spec("partitioner"), spec("snitch")),
                            List.<String[]>of(new String[] {CqlWireServer.clusterName(), "Murmur3Partitioner", "DynamicEndpointSnitch"}));
                }
                CqlType ringType = CqlType.map(CqlType.TEXT, CqlType.list(CqlType.TEXT, false), false);
                TreeMap<Object, Object> ring = new TreeMap<>(CqlType.TEXT);
                for (String tok : SysTables.tokens()) {
                    ring.put(tok, new ArrayList<Object>(List.of("/" + cs.localAddress + ":7000")));
                }
                List<Result.ColSpec> cols = List.of(spec("cluster"), spec("partitioner"), spec("snitch"),
                        new Result.ColSpec("system", "describe", "range_ownership", ringType));
                return Result.rows(cols, java.util.Collections.singletonList(new byte[][] {b(CqlWireServer.clusterName()), b("Murmur3Partitioner"),
                    b("DynamicEndpointSnitch"), ringType.serialize(ring)}), null);
            }
            case "keyspaces" -> {
                List<String[]> r = new ArrayList<>();
                for (Schema.Keyspace k : s.keyspaces.values()) {
                    r.add(new String[] {k.name, "keyspace", k.name});
                }
                return rows(SHORT, r);
            }
            case "schema" -> {
                List<String[]> r = new ArrayList<>();
                for (Schema.Keyspace k : s.keyspaces.values()) {
                    if (k.virtual && !d.full()) {
                        continue;
                    }
                    keyspaceRows(k, r, false);
                }
                return rows(LONG, r);
            }
            case "tables", "types", "functions", "aggregates" -> {
                List<String[]> r = new ArrayList<>();
                for (Schema.Keyspace k : s.keyspaces.values()) {
                    if (cur != null && !k.name.equals(cur)) {
                        continue;
                    }
                    if (d.what().equals("tables")) {
                        sortedTables(k).forEach(t -> r.add(new String[] {k.name, "table", ident(t.name)}));
                    } else if (d.what().equals("types")) {
                        orderedTypes(k).forEach(u -> r.add(new String[] {k.name, "type", u.name}));
                    }
                }
                return rows(SHORT, r);
            }
            case "keyspace" -> {
                String name = d.name() != null ? d.name() : cur;
                if (name == null) {
                    throw CqlError.invalid("No keyspace specified and no current keyspace");
                }
                Schema.Keyspace k = s.ks(name);
                if (k == null) {
                    throw CqlError.invalid("'" + name + "' not found in keyspaces");
                }
                List<String[]> r = new ArrayList<>();
                keyspaceRows(k, r, d.only());
                return rows(LONG, r);
            }
            case "element" -> {
                if (d.ks() == null) {
                    Schema.Keyspace k = s.ks(d.name());
                    if (k != null) {
                        List<String[]> r = new ArrayList<>();
                        keyspaceRows(k, r, false);
                        return rows(LONG, r);
                    }
                    if (cur == null) {
                        throw CqlError.invalid("'" + d.name() + "' not found in keyspaces");
                    }
                }
                String ks = d.ks() != null ? d.ks() : cur;
                Schema.Keyspace k = s.ks(ks);
                if (k == null) {
                    throw CqlError.invalid("'" + ks + "' not found in keyspaces");
                }
                Schema.Table t = k.tables.get(d.name());
                if (t != null) {
                    List<String[]> r = new ArrayList<>();
                    tableRows(t, r);
                    return rows(LONG, r);
                }
                for (Schema.Table x : k.tables.values()) {
                    for (Schema.Index i : x.indexes) {
                        if (i.name.equals(d.name())) {
                            return rows(LONG, List.<String[]>of(new String[] {ks, "index", i.name, indexStatement(x, i)}));
                        }
                    }
                }
                throw CqlError.invalid("'" + d.name() + "' not found in keyspace '" + ks + "'");
            }
            default -> {
                String ks = d.ks() != null ? d.ks() : cur;
                if (ks == null) {
                    throw CqlError.invalid("No keyspace specified and no current keyspace");
                }
                Schema.Keyspace k = s.ks(ks);
                if (k == null) {
                    throw CqlError.invalid("'" + ks + "' not found in keyspaces");
                }
                switch (d.what()) {
                    case "table" -> {
                        Schema.Table t = k.tables.get(d.name());
                        if (t == null) {
                            throw CqlError.invalid("Table '" + d.name() + "' not found in keyspace '" + ks + "'");
                        }
                        List<String[]> r = new ArrayList<>();
                        tableRows(t, r);
                        return rows(LONG, r);
                    }
                    case "type" -> {
                        Schema.Udt u = k.types.get(d.name());
                        if (u == null) {
                            throw CqlError.invalid("User defined type '" + d.name() + "' not found in '" + ks + "'");
                        }
                        return rows(LONG, List.<String[]>of(new String[] {ks, "type", u.name, typeStatement(u)}));
                    }
                    case "index" -> {
                        for (Schema.Table x : k.tables.values()) {
                            for (Schema.Index i : x.indexes) {
                                if (i.name.equals(d.name())) {
                                    return rows(LONG, List.<String[]>of(new String[] {ks, "index", i.name, indexStatement(x, i)}));
                                }
                            }
                        }
                        throw CqlError.invalid("Table for existing index '" + d.name() + "' not found in '" + ks + "'");
                    }
                    case "view" -> throw CqlError.invalid("Materialized view '" + d.name() + "' not found in '" + ks + "'");
                    case "function" -> throw CqlError.invalid("User defined function '" + d.name() + "' not found in '" + ks + "'");
                    default -> throw CqlError.invalid("User defined aggregate '" + d.name() + "' not found in '" + ks + "'");
                }
            }
        }
    }
}
