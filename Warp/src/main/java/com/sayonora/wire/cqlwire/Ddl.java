package com.sayonora.wire.cqlwire;

import com.sayonora.wire.cqlwire.Ast.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Schema statements: keyspaces, tables, types, indexes. The catalog is written on the home host, data cleanup runs on every host. */
final class Ddl {

    private final Engine e;
    private final Catalog catalog;
    private final CqlStore store;

    Ddl(Engine e) {
        this.e = e;
        this.catalog = e.catalog;
        this.store = e.store;
    }

    private static final java.util.regex.Pattern NAME = java.util.regex.Pattern.compile("[a-zA-Z0-9_]{1,48}");

    private Result done(String change, String target, String ks, String name) {
        catalog.invalidate();
        Result.SchemaEvent ev = new Result.SchemaEvent(change, target, ks, name);
        e.publish(ev);
        return Result.schema(ev);
    }

    private String ksOf(String ks, ClientState cs) {
        if (ks != null) {
            return ks;
        }
        if (cs.keyspace == null) {
            throw CqlError.invalid("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        }
        return cs.keyspace;
    }

    private Schema.Keyspace requireKs(Schema s, String ks) {
        Schema.Keyspace k = s.ks(ks);
        if (k == null) {
            throw CqlError.invalid("Keyspace '" + ks + "' doesn't exist");
        }
        return k;
    }

    private void userKs(String ks) {
        if (catalog.isSystem(ks)) {
            throw CqlError.unauthorized("system keyspace is not user-modifiable.");
        }
    }

    static String qual(String ks, String table) {
        return ks + "." + (table.matches("[a-z][a-z0-9_]*") ? table : "\"" + table + "\"");
    }

    // ------------------------------------------------------------------ keyspaces

    private static Map<String, String> replication(Map<String, Object> opts) {
        Object r = opts.get("replication");
        if (!(r instanceof Map<?, ?> m)) {
            throw CqlError.invalid("Missing mandatory option 'replication'");
        }
        Object cls = m.get("class");
        if (cls == null) {
            throw CqlError.invalid("Missing mandatory replication strategy class");
        }
        String c = String.valueOf(cls);
        String simple = c.substring(c.lastIndexOf('.') + 1);
        Map<String, String> out = new LinkedHashMap<>();
        switch (simple) {
            case "SimpleStrategy" -> {
                Object rf = m.get("replication_factor");
                if (rf == null) {
                    rf = "1";
                }
                checkRf(String.valueOf(rf));
                out.put("class", "org.apache.cassandra.locator.SimpleStrategy");
                out.put("replication_factor", String.valueOf(rf));
            }
            case "NetworkTopologyStrategy" -> {
                out.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
                m.forEach((k, v) -> {
                    if (!k.equals("class")) {
                        checkRf(String.valueOf(v));
                        out.put(String.valueOf(k), String.valueOf(v));
                    }
                });
            }
            case "LocalStrategy" -> throw CqlError.config("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
            default -> throw CqlError.config("Unable to find replication strategy class '" + c + "'");
        }
        return out;
    }

    private static void checkRf(String rf) {
        try {
            if (Integer.parseInt(rf) < 0) {
                throw CqlError.config("Replication factor must be non-negative");
            }
        } catch (NumberFormatException ex) {
            throw CqlError.config("Replication factor must be numeric; found " + rf);
        }
    }

    Result createKeyspace(CreateKeyspace st) {
        Schema s = catalog.schema();
        if (!NAME.matcher(st.name()).matches()) {
            throw CqlError.invalid("Keyspace name must not be empty and must contain alphanumeric or underscore characters only (got \"" + st.name()
                    + "\")");
        }
        if (s.ks(st.name()) != null) {
            if (st.ine()) {
                return Result.VOID;
            }
            throw CqlError.exists(st.name(), "");
        }
        Map<String, String> repl = replication(st.options());
        boolean durable = !(st.options().get("durable_writes") instanceof Boolean b) || b;
        if (!store.insertSchema(store.shards().home(), "keyspace", st.name(), st.name(), Schema.keyspaceSpec(st.name(), repl, durable))) {
            if (st.ine()) {
                return Result.VOID;
            }
            throw CqlError.exists(st.name(), "");
        }
        return done("CREATED", "KEYSPACE", st.name(), "");
    }

    Result alterKeyspace(AlterKeyspace st) {
        Schema s = catalog.schema();
        Schema.Keyspace k = requireKs(s, st.name());
        userKs(st.name());
        Map<String, String> repl = st.options().containsKey("replication") ? replication(st.options()) : k.replication;
        boolean durable = st.options().get("durable_writes") instanceof Boolean b ? b : k.durableWrites;
        store.putSchema(store.shards().home(), "keyspace", st.name(), st.name(), Schema.keyspaceSpec(st.name(), repl, durable));
        return done("UPDATED", "KEYSPACE", st.name(), "");
    }

    Result dropKeyspace(DropKeyspace st) {
        Schema s = catalog.schema();
        Schema.Keyspace k = s.ks(st.name());
        if (k == null) {
            if (st.ifExists()) {
                return Result.VOID;
            }
            throw CqlError.invalid("Keyspace '" + st.name() + "' doesn't exist");
        }
        userKs(st.name());
        for (Schema.Table t : k.tables.values()) {
            dropData(t.id);
        }
        store.deleteKeyspaceSchema(store.shards().home(), st.name());
        return done("DROPPED", "KEYSPACE", st.name(), "");
    }

    private void dropData(UUID tid) {
        for (String h : store.shards().allHosts()) {
            store.dropTable(h, tid);
        }
    }

    // ------------------------------------------------------------------ tables

    private static void checkTableName(String n) {
        if (!NAME.matcher(n).matches()) {
            throw CqlError.invalid("Table names may be at most 48 characters long and contain only alphanumeric characters and underscores (got \"" + n
                    + "\")");
        }
    }

    Result createTable(CreateTable st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        requireKs(s, ks);
        userKs(ks);
        checkTableName(st.name());
        if (s.table(ks, st.name()) != null) {
            if (st.ine()) {
                return Result.VOID;
            }
            throw CqlError.exists(ks, st.name());
        }
        if (st.partition().isEmpty()) {
            throw CqlError.invalid("No PRIMARY KEY specifed for table '" + ks + "." + st.name() + "' (exactly one required)");
        }
        Map<String, ColDef> defs = new LinkedHashMap<>();
        for (ColDef d : st.cols()) {
            if (defs.put(d.name(), d) != null) {
                throw CqlError.invalid("Duplicate column '" + d.name() + "' declaration for table '" + ks + "." + st.name() + "'");
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String p : st.partition()) {
            if (!defs.containsKey(p)) {
                throw CqlError.invalid("Unknown column '" + p + "' referenced in PRIMARY KEY for table '" + st.name() + "'");
            }
            if (!seen.add(p)) {
                throw CqlError.invalid("Duplicate column '" + p + "' in PRIMARY KEY clause for table '" + st.name() + "'");
            }
        }
        for (String p : st.clustering()) {
            if (!defs.containsKey(p)) {
                throw CqlError.invalid("Unknown column '" + p + "' referenced in PRIMARY KEY for table '" + st.name() + "'");
            }
            if (!seen.add(p)) {
                throw CqlError.invalid("Duplicate column '" + p + "' in PRIMARY KEY clause for table '" + st.name() + "'");
            }
        }
        List<Schema.Column> cols = new ArrayList<>();
        boolean anyCounter = false, anyNonCounter = false;
        for (ColDef d : st.cols()) {
            CqlType t = Parser.parseType(d.type(), ks, s::udt);
            boolean isPk = st.partition().contains(d.name()), isCk = st.clustering().contains(d.name());
            if ((isPk || isCk)) {
                if (t.isMultiCell()) {
                    throw CqlError.invalid("Invalid non-frozen collection type " + t.cql() + " for PRIMARY KEY column '" + d.name() + "'");
                }
                if (t.k == CqlType.K.COUNTER) {
                    throw CqlError.invalid("counter type is not supported for PRIMARY KEY column '" + d.name() + "'");
                }
                if (t.k == CqlType.K.DURATION) {
                    throw CqlError.invalid("duration type is not supported for PRIMARY KEY column '" + d.name() + "'");
                }
                if (d.isStatic()) {
                    throw CqlError.invalid("Static column " + d.name() + " cannot be part of the PRIMARY KEY");
                }
            }
            if (d.isStatic() && st.clustering().isEmpty()) {
                throw CqlError.invalid("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
            }
            if (d.isStatic() && t.k == CqlType.K.COUNTER) {
                // allowed in Cassandra (static counters); Warp stores them like any counter
                anyCounter = true;
            }
            if (!isPk && !isCk) {
                if (t.k == CqlType.K.COUNTER) {
                    anyCounter = true;
                } else {
                    anyNonCounter = true;
                }
            }
            Schema.Kind kind = isPk ? Schema.Kind.PARTITION_KEY : isCk ? Schema.Kind.CLUSTERING : d.isStatic() ? Schema.Kind.STATIC : Schema.Kind.REGULAR;
            int pos = isPk ? st.partition().indexOf(d.name()) : isCk ? st.clustering().indexOf(d.name()) : -1;
            cols.add(new Schema.Column(d.name(), t, kind, pos, isCk && st.clusteringDesc().get(pos)));
        }
        if (st.clusteringDesc().size() != st.clustering().size()) {
            throw CqlError.invalid("Internal clustering order mismatch");
        }
        if (anyCounter && anyNonCounter) {
            throw CqlError.invalid("Cannot mix counter and non counter columns in the same table");
        }
        Map<String, Object> options = new LinkedHashMap<>();
        for (var o : st.options().entrySet()) {
            if (!o.getKey().equals("compact storage")) {
                options.put(o.getKey(), o.getValue());
            } else {
                throw CqlError.invalid("COMPACT STORAGE tables are not supported by Warp's cqlwire");
            }
        }
        validateOptions(options);
        UUID id = UUID.randomUUID();
        Schema.Table t = new Schema.Table(ks, st.name(), id, cols, options, List.of(), null);
        if (!store.insertSchema(store.shards().home(), "table", ks, st.name(), Schema.tableSpec(t))) {
            if (st.ine()) {
                return Result.VOID;
            }
            throw CqlError.exists(ks, st.name());
        }
        return done("CREATED", "TABLE", ks, st.name());
    }

    private static final Set<String> KNOWN_OPTIONS = Set.of("comment", "gc_grace_seconds", "default_time_to_live", "bloom_filter_fp_chance",
            "caching", "compaction", "compression", "crc_check_chance", "read_repair", "read_repair_chance", "dclocal_read_repair_chance",
            "speculative_retry", "additional_write_policy", "min_index_interval", "max_index_interval", "memtable_flush_period_in_ms",
            "cdc", "memtable", "id", "extensions", "allow_auto_snapshot", "incremental_backups");

    private static void validateOptions(Map<String, Object> o) {
        for (String k : o.keySet()) {
            if (!KNOWN_OPTIONS.contains(k.toLowerCase(Locale.ROOT))) {
                throw CqlError.syntax("Unknown property '" + k + "'");
            }
        }
        Object ttl = o.get("default_time_to_live");
        if (ttl instanceof Number n && (n.longValue() < 0 || n.longValue() > 630720000)) {
            throw CqlError.invalid("default_time_to_live must be between 0 and 630720000 (got " + n + ")");
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> dropped(Map<String, Object> options) {
        return (Map<String, Object>) options.computeIfAbsent("_dropped", k -> new LinkedHashMap<String, Object>());
    }

    Result alterTable(AlterTable st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Table t = s.table(ks, st.name());
        if (t == null) {
            if (st.ifExists()) {
                return Result.VOID;
            }
            throw CqlError.invalid("Table '" + ks + "." + st.name() + "' doesn't exist");
        }
        userKs(ks);
        List<Schema.Column> cols = new ArrayList<>(t.columns);
        Map<String, Object> options = new LinkedHashMap<>(t.options);
        if (!st.alterType().isEmpty()) {
            throw CqlError.invalid("Altering column types is no longer supported");
        }
        for (ColDef d : st.add()) {
            if (t.col(d.name()) != null) {
                if (st.addIfNotExists()) {
                    continue;
                }
                throw CqlError.invalid("Column with name '" + d.name() + "' already exists");
            }
            CqlType ty = Parser.parseType(d.type(), ks, s::udt);
            Object dropped = dropped(options).get(d.name());
            if (dropped != null && !dropped.equals(ty.qualified())) {
                throw CqlError.invalid("Cannot add a column '" + d.name() + "' of type " + ty.cql() + ", incompatible with previously dropped column '"
                        + d.name() + "' of type " + Parser.parseType(String.valueOf(dropped), ks, s::udt).cql());
            }
            dropped(options).remove(d.name());
            if (d.isStatic() && t.clustering.isEmpty()) {
                throw CqlError.invalid("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
            }
            if (t.counter && ty.k != CqlType.K.COUNTER) {
                throw CqlError.config(ks + "." + st.name() + ": Cannot have a non counter column (\"" + d.name() + "\") in a counter table");
            }
            if (!t.counter && ty.k == CqlType.K.COUNTER && cols.stream().anyMatch(c -> !c.isKey())) {
                throw CqlError.config(ks + "." + st.name() + ": Cannot have a counter column (\"" + d.name() + "\") in a non counter table");
            }
            cols.add(new Schema.Column(d.name(), ty, d.isStatic() ? Schema.Kind.STATIC : Schema.Kind.REGULAR, -1, false));
        }
        for (String d : st.drop()) {
            Schema.Column c = t.col(d);
            if (c == null) {
                if (st.dropIfExists()) {
                    continue;
                }
                throw CqlError.invalid("Column " + d + " was not found in table '" + ks + "." + st.name() + "'");
            }
            if (c.isKey()) {
                throw CqlError.invalid("Cannot drop PRIMARY KEY column " + d);
            }
            cols.removeIf(x -> x.name.equals(d));
            dropped(options).put(d, c.type.qualified());
            for (String h : store.shards().allHosts()) {
                store.dropColumn(h, t.id, d);
            }
        }
        for (var r : st.rename().entrySet()) {
            Schema.Column c = t.col(r.getKey());
            if (c == null) {
                throw CqlError.invalid("Cannot rename unknown column " + r.getKey() + " in table " + st.name());
            }
            if (!c.isKey()) {
                throw CqlError.invalid("Cannot rename non PRIMARY KEY column " + r.getKey());
            }
            if (t.col(r.getValue()) != null) {
                throw CqlError.invalid("Cannot rename column " + r.getKey() + " to " + r.getValue() + " in table " + st.name()
                        + "; another column of that name already exist");
            }
            int i = cols.indexOf(c);
            cols.set(i, new Schema.Column(r.getValue(), c.type, c.kind, c.position, c.desc));
        }
        if (!st.options().isEmpty()) {
            validateOptions(st.options());
            st.options().forEach((k, v) -> {
                if (!k.equals("compact storage") && !k.equals("clustering order")) {
                    options.put(k, v);
                }
            });
        }
        Schema.Table nt = new Schema.Table(ks, st.name(), t.id, cols, options, t.indexes, null);
        store.putSchema(store.shards().home(), "table", ks, st.name(), Schema.tableSpec(nt));
        return done("UPDATED", "TABLE", ks, st.name());
    }

    Result dropTable(DropTable st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Table t = s.table(ks, st.name());
        if (t == null) {
            if (st.ifExists()) {
                return Result.VOID;
            }
            throw CqlError.invalid("Table '" + ks + "." + st.name() + "' doesn't exist");
        }
        userKs(ks);
        dropData(t.id);
        for (Schema.Index i : t.indexes) {
            store.deleteSchema(store.shards().home(), "index", ks, i.name);
        }
        store.deleteSchema(store.shards().home(), "table", ks, st.name());
        return done("DROPPED", "TABLE", ks, st.name());
    }

    Result truncate(Truncate st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema.Table t = catalog.schema().table(ks, st.name());
        if (t == null) {
            throw CqlError.invalid("table " + st.name() + " does not exist");
        }
        userKs(ks);
        dropData(t.id);
        return Result.VOID;
    }

    // ------------------------------------------------------------------ indexes

    Result createIndex(CreateIndex st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Table t = s.table(ks, st.table());
        if (t == null) {
            throw CqlError.invalid("Table '" + ks + "." + st.table() + "' doesn't exist");
        }
        userKs(ks);
        Schema.Column c = t.col(st.target());
        if (c == null) {
            throw CqlError.invalid("Undefined column name " + st.target() + " in table " + ks + "." + st.table());
        }
        if (c.type.isCounter()) {
            throw CqlError.invalid("Secondary indexes are not supported on counter columns");
        }
        if (c.kind == Schema.Kind.PARTITION_KEY && t.partition.size() == 1) {
            throw CqlError.invalid("Cannot create secondary index on the only partition key column " + c.name);
        }
        if (c.type.isMultiCell() && st.kind().equals("values") && c.type.k != CqlType.K.MAP) {
            // non-frozen list/set indexes index the values
        }
        String kind = st.kind();
        if (c.type.frozen && c.type.isCollection() && kind.equals("values")) {
            throw CqlError.invalid("Cannot create values() index on frozen column " + c.name + ". Frozen collections are immutable and must be fully "
                    + "indexed by using the 'full(" + c.name + ")' modifier");
        }
        if ((kind.equals("keys") || kind.equals("entries")) && !c.type.isCollection()) {
            throw CqlError.invalid("Cannot create " + kind + "() index on " + c.name + ". Non-collection columns only support simple indexes");
        }
        if ((st.kind().equals("keys") || st.kind().equals("entries")) && c.type.k != CqlType.K.MAP) {
            throw CqlError.invalid("Cannot create index on " + st.kind().toUpperCase(Locale.ROOT) + "(" + c.name + "): the column is not a map");
        }
        if (st.kind().equals("full") && !c.type.frozen) {
            throw CqlError.invalid("full() indexes can only be created on frozen collections");
        }
        String name = st.name() != null ? st.name() : st.table() + "_" + c.name + "_idx";
        for (Schema.Keyspace k : List.of(s.ks(ks))) {
            for (Schema.Table x : k.tables.values()) {
                for (Schema.Index i : x.indexes) {
                    if (i.name.equals(name)) {
                        if (st.ine()) {
                            return Result.VOID;
                        }
                        throw CqlError.invalid("Index '" + name + "' already exists");
                    }
                }
            }
        }
        Schema.Index idx = new Schema.Index(name, c.name, kind, st.custom(), st.options());
        store.putSchema(store.shards().home(), "index", ks, name, Schema.indexSpec(st.table(), idx));
        return done("UPDATED", "TABLE", ks, st.table());
    }

    Result dropIndex(DropIndex st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Keyspace k = s.ks(ks);
        if (k != null) {
            for (Schema.Table t : k.tables.values()) {
                for (Schema.Index i : t.indexes) {
                    if (i.name.equals(st.name())) {
                        userKs(ks);
                        store.deleteSchema(store.shards().home(), "index", ks, i.name);
                        return done("UPDATED", "TABLE", ks, t.name);
                    }
                }
            }
        }
        if (st.ifExists()) {
            return Result.VOID;
        }
        throw CqlError.invalid("Index '" + ks + "." + st.name() + "' doesn't exist");
    }

    // ------------------------------------------------------------------ types

    Result createType(CreateType st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        requireKs(s, ks);
        userKs(ks);
        Schema.Keyspace k = s.ks(ks);
        if (k.types.containsKey(st.name())) {
            if (st.ine()) {
                return Result.VOID;
            }
            throw CqlError.invalid("A user type with name '" + st.name() + "' already exists");
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String f : st.fields()) {
            if (!seen.add(f)) {
                throw CqlError.invalid("Duplicate field name '" + f + "' in type '" + st.name() + "'");
            }
        }
        List<String> quals = new ArrayList<>();
        for (String ty : st.types()) {
            CqlType t = Parser.parseType(ty, ks, s::udt);
            if (t.k == CqlType.K.COUNTER) {
                throw CqlError.invalid("A user type cannot contain counters");
            }
            quals.add(t.qualified());
        }
        store.putSchema(store.shards().home(), "type", ks, st.name(), Schema.udtSpec(st.fields(), quals));
        return done("CREATED", "TYPE", ks, st.name());
    }

    Result alterType(AlterType st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Keyspace k = requireKs(s, ks);
        Schema.Udt u = k.types.get(st.name());
        if (u == null) {
            throw CqlError.invalid("Unknown type " + ks + "." + st.name());
        }
        userKs(ks);
        List<String> fields = new ArrayList<>(u.fields);
        List<String> types = new ArrayList<>();
        u.types.forEach(t -> types.add(t.qualified()));
        if (st.addField() != null) {
            if (fields.contains(st.addField())) {
                throw CqlError.invalid("Cannot add field " + st.addField() + " to type " + ks + "." + st.name() + ": a field with name " + st.addField()
                        + " already exists");
            }
            fields.add(st.addField());
            types.add(Parser.parseType(st.addType(), ks, s::udt).qualified());
        } else {
            for (var r : st.rename().entrySet()) {
                int i = fields.indexOf(r.getKey());
                if (i < 0) {
                    throw CqlError.invalid("Unknown field " + r.getKey() + " in type " + st.name());
                }
                fields.set(i, r.getValue());
            }
        }
        store.putSchema(store.shards().home(), "type", ks, st.name(), Schema.udtSpec(fields, types));
        return done("UPDATED", "TYPE", ks, st.name());
    }

    Result dropType(DropType st, ClientState cs) {
        String ks = ksOf(st.ks(), cs);
        Schema s = catalog.schema();
        Schema.Keyspace k = requireKs(s, ks);
        if (!k.types.containsKey(st.name())) {
            if (st.ifExists()) {
                return Result.VOID;
            }
            throw CqlError.invalid("Type '" + ks + "." + st.name() + "' doesn't exist");
        }
        userKs(ks);
        for (Schema.Udt u : k.types.values()) {
            if (u != k.types.get(st.name()) && u.types.stream().anyMatch(t -> t.qualified().contains("\"" + ks + "\".\"" + st.name() + "\""))) {
                throw CqlError.invalid("Cannot drop user type '" + ks + "." + st.name() + "' as it is still used by user types " + u.name);
            }
        }
        for (Schema.Table t : k.tables.values()) {
            for (Schema.Column c : t.columns) {
                if (c.type.qualified().contains("\"" + ks + "\".\"" + st.name() + "\"")) {
                    throw CqlError.invalid("Cannot drop user type '" + ks + "." + st.name() + "' as it is still used by table " + ks + "." + t.name);
                }
            }
        }
        store.deleteSchema(store.shards().home(), "type", ks, st.name());
        return done("DROPPED", "TYPE", ks, st.name());
    }
}
