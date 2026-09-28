package com.sayonora.warp.cqlwire;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;

/** Immutable snapshot of the CQL catalog: keyspaces, tables, user defined types and secondary indexes. */
final class Schema {

    enum Kind { PARTITION_KEY, CLUSTERING, REGULAR, STATIC }

    static final class Column {
        final String name;
        final CqlType type;
        final Kind kind;
        final int position;
        final boolean desc;
        /** Index in the table's column list. */
        int ordinal;

        Column(String name, CqlType type, Kind kind, int position, boolean desc) {
            this.name = name;
            this.type = type;
            this.kind = kind;
            this.position = position;
            this.desc = desc;
        }

        boolean isKey() {
            return kind == Kind.PARTITION_KEY || kind == Kind.CLUSTERING;
        }
    }

    static final class Index {
        final String name;
        final String column;
        final String kind;
        final String custom;
        final Map<String, Object> options;

        Index(String name, String column, String kind, String custom, Map<String, Object> options) {
            this.name = name;
            this.column = column;
            this.kind = kind;
            this.custom = custom;
            this.options = options;
        }
    }

    static final class Table {
        final String ks;
        final String name;
        final UUID id;
        final List<Column> columns;
        final List<Column> partition = new ArrayList<>();
        final List<Column> clustering = new ArrayList<>();
        final Map<String, Column> byName = new LinkedHashMap<>();
        final Map<String, Object> options;
        final List<Index> indexes;
        final boolean counter;
        /** Non-null for virtual (system) tables: produces the rows. */
        final Supplier<List<Map<String, Object>>> virtual;
        final boolean hasStatic;
        /** Columns in SELECT * order: partition key, clustering, then static and regular columns by name. */
        final List<Column> selectOrder = new ArrayList<>();

        Table(String ks, String name, UUID id, List<Column> columns, Map<String, Object> options, List<Index> indexes,
                Supplier<List<Map<String, Object>>> virtual) {
            this.ks = ks;
            this.name = name;
            this.id = id;
            this.columns = columns;
            this.options = options;
            this.indexes = indexes;
            this.virtual = virtual;
            boolean ctr = false, st = false;
            int i = 0;
            for (Column c : columns) {
                c.ordinal = i++;
                byName.put(c.name, c);
                if (c.kind == Kind.PARTITION_KEY) {
                    partition.add(c);
                } else if (c.kind == Kind.CLUSTERING) {
                    clustering.add(c);
                }
                ctr |= c.type.isCounter();
                st |= c.kind == Kind.STATIC;
            }
            partition.sort((a, b) -> Integer.compare(a.position, b.position));
            clustering.sort((a, b) -> Integer.compare(a.position, b.position));
            this.counter = ctr;
            this.hasStatic = st;
            selectOrder.addAll(partition);
            selectOrder.addAll(clustering);
            List<Column> rest = new ArrayList<>();
            for (Column c : columns) {
                if (!c.isKey()) {
                    rest.add(c);
                }
            }
            rest.sort((a, b) -> a.kind == b.kind ? a.name.compareTo(b.name) : a.kind == Kind.STATIC ? -1 : 1);
            selectOrder.addAll(rest);
        }

        Column col(String n) {
            return byName.get(n);
        }

        boolean indexed(String col) {
            for (Index x : indexes) {
                if (x.column.equals(col)) {
                    return true;
                }
            }
            return false;
        }

        /** Serializes a composite partition key from its column values in the wire form Cassandra hashes. */
        byte[] partitionBytes(Object[] values) {
            if (partition.size() == 1) {
                return partition.get(0).type.serialize(values[0]);
            }
            Wire.Out o = new Wire.Out();
            for (int i = 0; i < partition.size(); i++) {
                byte[] b = partition.get(i).type.serialize(values[i]);
                o.u16(b.length).raw(b).u8(0);
            }
            return o.toBytes();
        }

        Object[] partitionValues(byte[] pk) {
            Object[] v = new Object[partition.size()];
            if (partition.size() == 1) {
                v[0] = partition.get(0).type.deserialize(pk);
                return v;
            }
            Wire.In in = new Wire.In(pk);
            for (int i = 0; i < v.length; i++) {
                byte[] b = in.raw(in.u16());
                in.u8();
                v[i] = partition.get(i).type.deserialize(b);
            }
            return v;
        }

        /** Order-preserving encoding of clustering values (a prefix is allowed). */
        byte[] clusteringBytes(Object[] values, int n) {
            java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
            for (int i = 0; i < n; i++) {
                Column c = clustering.get(i);
                o.writeBytes(c.type.keyEncode(values[i], c.desc));
            }
            return o.toByteArray();
        }

        /** Serialized clustering values, [int len][bytes] each. */
        byte[] clusteringValues(Object[] values) {
            Wire.Out o = new Wire.Out();
            for (int i = 0; i < clustering.size(); i++) {
                o.bytes(clustering.get(i).type.serialize(values[i]));
            }
            return o.toBytes();
        }

        Object[] clusteringFromValues(byte[] ckv) {
            Object[] v = new Object[clustering.size()];
            Wire.In in = new Wire.In(ckv);
            for (int i = 0; i < v.length; i++) {
                v[i] = clustering.get(i).type.deserialize(in.bytes());
            }
            return v;
        }
    }

    static final class Udt {
        final String ks;
        final String name;
        final List<String> fields;
        final List<CqlType> types;

        Udt(String ks, String name, List<String> fields, List<CqlType> types) {
            this.ks = ks;
            this.name = name;
            this.fields = fields;
            this.types = types;
        }

        CqlType type() {
            return CqlType.udt(ks, name, fields, types, false);
        }
    }

    static final class Keyspace {
        final String name;
        final Map<String, String> replication;
        final boolean durableWrites;
        final Map<String, Table> tables = new TreeMap<>();
        final Map<String, Udt> types = new TreeMap<>();
        final boolean virtual;

        Keyspace(String name, Map<String, String> replication, boolean durableWrites, boolean virtual) {
            this.name = name;
            this.replication = replication;
            this.durableWrites = durableWrites;
            this.virtual = virtual;
        }
    }

    final Map<String, Keyspace> keyspaces = new TreeMap<>();
    final long version;

    Schema(long version) {
        this.version = version;
    }

    Keyspace ks(String n) {
        return keyspaces.get(n);
    }

    Table table(String ks, String n) {
        Keyspace k = keyspaces.get(ks);
        return k == null ? null : k.tables.get(n);
    }

    /** The type resolver for parsing type texts inside keyspace {@code ks}. */
    CqlType udt(String ks, String name) {
        Keyspace k = keyspaces.get(ks);
        Udt u = k == null ? null : k.types.get(name);
        return u == null ? null : u.type();
    }

    // ------------------------------------------------------------------ persistence (JSON specs)

    private static final Gson GSON = new Gson();

    static String keyspaceSpec(String name, Map<String, String> replication, boolean durable) {
        JsonObject o = new JsonObject();
        o.add("replication", GSON.toJsonTree(replication));
        o.addProperty("durable_writes", durable);
        return o.toString();
    }

    static String udtSpec(List<String> fields, List<String> types) {
        JsonObject o = new JsonObject();
        o.add("fields", GSON.toJsonTree(fields));
        o.add("types", GSON.toJsonTree(types));
        return o.toString();
    }

    static String tableSpec(Table t) {
        JsonObject o = new JsonObject();
        o.addProperty("id", t.id.toString());
        JsonArray cols = new JsonArray();
        for (Column c : t.columns) {
            JsonObject j = new JsonObject();
            j.addProperty("name", c.name);
            j.addProperty("type", c.type.qualified());
            j.addProperty("kind", c.kind.name());
            j.addProperty("pos", c.position);
            j.addProperty("desc", c.desc);
            cols.add(j);
        }
        o.add("columns", cols);
        o.add("options", GSON.toJsonTree(t.options));
        return o.toString();
    }

    static String indexSpec(String table, Index i) {
        JsonObject o = new JsonObject();
        o.addProperty("table", table);
        o.addProperty("column", i.column);
        o.addProperty("kind", i.kind);
        if (i.custom != null) {
            o.addProperty("custom", i.custom);
        }
        o.add("options", GSON.toJsonTree(i.options));
        return o.toString();
    }

    /** Rows of warp_cql_schema: {kind, ks, name, spec}. Keyspaces and types first so tables can reference UDTs. */
    static Schema load(long version, List<String[]> rows) {
        Schema s = new Schema(version);
        for (String[] r : rows) {
            if (r[0].equals("keyspace")) {
                JsonObject o = JsonParser.parseString(r[3]).getAsJsonObject();
                Map<String, String> repl = new LinkedHashMap<>();
                o.getAsJsonObject("replication").entrySet().forEach(e -> repl.put(e.getKey(), e.getValue().getAsString()));
                s.keyspaces.put(r[2], new Keyspace(r[2], repl, o.get("durable_writes").getAsBoolean(), false));
            }
        }
        // user types may reference each other: resolve in creation order (rows are ordered by seq)
        for (String[] r : rows) {
            if (r[0].equals("type") && s.keyspaces.containsKey(r[1])) {
                JsonObject o = JsonParser.parseString(r[3]).getAsJsonObject();
                List<String> f = new ArrayList<>(), tt = new ArrayList<>();
                o.getAsJsonArray("fields").forEach(e -> f.add(e.getAsString()));
                List<CqlType> types = new ArrayList<>();
                for (JsonElement e : o.getAsJsonArray("types")) {
                    types.add(Parser.parseType(e.getAsString(), r[1], s::udt));
                }
                s.keyspaces.get(r[1]).types.put(r[2], new Udt(r[1], r[2], f, types));
            }
        }
        Map<String, List<Index>> idx = new LinkedHashMap<>();
        for (String[] r : rows) {
            if (r[0].equals("index")) {
                JsonObject o = JsonParser.parseString(r[3]).getAsJsonObject();
                @SuppressWarnings("unchecked")
                Map<String, Object> opts = GSON.fromJson(o.get("options"), Map.class);
                idx.computeIfAbsent(r[1] + "." + o.get("table").getAsString(), k -> new ArrayList<>()).add(new Index(r[2],
                        o.get("column").getAsString(), o.get("kind").getAsString(),
                        o.has("custom") ? o.get("custom").getAsString() : null, opts == null ? Map.of() : opts));
            }
        }
        for (String[] r : rows) {
            if (r[0].equals("table") && s.keyspaces.containsKey(r[1])) {
                JsonObject o = JsonParser.parseString(r[3]).getAsJsonObject();
                List<Column> cols = new ArrayList<>();
                for (JsonElement e : o.getAsJsonArray("columns")) {
                    JsonObject j = e.getAsJsonObject();
                    cols.add(new Column(j.get("name").getAsString(), Parser.parseType(j.get("type").getAsString(), r[1], s::udt),
                            Kind.valueOf(j.get("kind").getAsString()), j.get("pos").getAsInt(), j.get("desc").getAsBoolean()));
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> opts = GSON.fromJson(o.get("options"), Map.class);
                s.keyspaces.get(r[1]).tables.put(r[2], new Table(r[1], r[2], UUID.fromString(o.get("id").getAsString()), cols,
                        opts == null ? new LinkedHashMap<>() : opts, idx.getOrDefault(r[1] + "." + r[2], List.of()), null));
            }
        }
        return s;
    }

    static List<Index> noIndexes() {
        return Collections.emptyList();
    }
}
