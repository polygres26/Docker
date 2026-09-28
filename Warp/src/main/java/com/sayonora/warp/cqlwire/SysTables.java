package com.sayonora.warp.cqlwire;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;

/** The virtual system keyspaces (system, system_schema, ...) served from the catalog so drivers can connect and discover the schema. */
final class SysTables {

    private SysTables() {
    }

    static final ThreadLocal<String> LOCAL_ADDRESS = ThreadLocal.withInitial(() -> "127.0.0.1");

    static final String[] SYSTEM_KEYSPACES = {"system", "system_schema", "system_auth", "system_distributed", "system_traces",
        "system_views", "system_virtual_schema"};

    private static final String[] TOKENS = {"-2570060250155372869", "-3548205702671486944", "-5167228998368530241", "-6757233550882428879",
        "-7724597793960964378", "-791425949331785740", "-8968117799641556352", "1395913061595364807", "2240289709086526564",
        "304615950903152548", "3680557938989109802", "4752058555329142615", "5769909960071569630", "6587581892683920425",
        "7663337773909499964", "8399253466620393605"};

    static List<String> tokens() {
        return List.of(TOKENS);
    }

    private static final UUID HOST_ID = UUID.nameUUIDFromBytes(("warp-cqlwire-" + System.getenv("WARP_CQLWIRE_HOST_ID")).getBytes(StandardCharsets.UTF_8));

    /** A definition line "name type [pk|ck|ckdesc]". */
    private static Schema.Table table(String ks, String name, Supplier<List<Map<String, Object>>> rows, String... defs) {
        List<Schema.Column> cols = new ArrayList<>();
        int pk = 0, ck = 0;
        for (String d : defs) {
            String[] p = d.split(" ", 3);
            String flag = p.length > 2 ? p[2] : "";
            CqlType t = Parser.parseType(p[1].replace('_', ' ').replace("~", ", "), ks, (a, b) -> null);
            Schema.Kind kind = flag.equals("pk") ? Schema.Kind.PARTITION_KEY : flag.startsWith("ck") ? Schema.Kind.CLUSTERING : Schema.Kind.REGULAR;
            cols.add(new Schema.Column(p[0], t, kind, kind == Schema.Kind.PARTITION_KEY ? pk++ : kind == Schema.Kind.CLUSTERING ? ck++ : -1,
                    flag.equals("ckdesc")));
        }
        return new Schema.Table(ks, name, UUID.nameUUIDFromBytes((ks + "." + name).getBytes(StandardCharsets.UTF_8)), cols, new LinkedHashMap<>(),
                List.of(), rows);
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static TreeMap<Object, Object> smap(String... kv) {
        TreeMap<Object, Object> m = new TreeMap<>(CqlType.TEXT);
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static InetAddress addr(String a) {
        try {
            return InetAddress.getByName(a);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    static UUID schemaVersion(Schema real) {
        StringBuilder sb = new StringBuilder();
        for (var k : real.keyspaces.values()) {
            sb.append(k.name).append(k.replication);
            for (var t : k.tables.values()) {
                sb.append(t.name).append(Schema.tableSpec(t));
                t.indexes.forEach(i -> sb.append(i.name));
            }
            k.types.values().forEach(u -> sb.append(u.name).append(u.fields).append(u.types));
        }
        return UUID.nameUUIDFromBytes(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    static Map<String, Object> tableDefaults() {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("comment", "");
        o.put("gc_grace_seconds", 864000L);
        o.put("default_time_to_live", 0L);
        return o;
    }

    private static TreeMap<Object, Object> merged(Object user, Map<String, String> defaults, String classPrefix) {
        TreeMap<Object, Object> r = new TreeMap<>(CqlType.TEXT);
        r.putAll(defaults);
        if (user instanceof Map<?, ?> m) {
            m.forEach((a, b) -> {
                String v = b instanceof Double d && d == Math.rint(d) ? String.valueOf(d.longValue()) : String.valueOf(b);
                if (String.valueOf(a).equals("class") && classPrefix != null && !v.contains(".")) {
                    v = classPrefix + v;
                }
                r.put(String.valueOf(a), v);
            });
        }
        return r;
    }

    /** Every table option with Cassandra's defaults filled in, values typed as system_schema.tables stores them. */
    static Map<String, Object> effective(Schema.Table t) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("additional_write_policy", opt(t, "additional_write_policy", "99p"));
        o.put("allow_auto_snapshot", opt(t, "allow_auto_snapshot", Boolean.TRUE));
        TreeMap<Object, Object> compaction = merged(t.options.get("compaction"), Map.of("class",
                "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold", "32", "min_threshold", "4"),
                "org.apache.cassandra.db.compaction.");
        boolean lcs = String.valueOf(compaction.get("class")).endsWith("LeveledCompactionStrategy");
        o.put("bloom_filter_fp_chance", dblOpt(t, "bloom_filter_fp_chance", lcs ? 0.1 : 0.01));
        o.put("caching", merged(t.options.get("caching"), Map.of("keys", "ALL", "rows_per_partition", "NONE"), null));
        o.put("cdc", opt(t, "cdc", Boolean.FALSE));
        o.put("comment", opt(t, "comment", ""));
        o.put("compaction", compaction);
        o.put("compression", merged(t.options.get("compression"), Map.of("chunk_length_in_kb", "16", "class",
                "org.apache.cassandra.io.compress.LZ4Compressor"), "org.apache.cassandra.io.compress."));
        o.put("memtable", opt(t, "memtable", "default"));
        o.put("crc_check_chance", dblOpt(t, "crc_check_chance", 1.0));
        o.put("default_time_to_live", intOpt(t, "default_time_to_live", 0));
        o.put("extensions", new TreeMap<Object, Object>(CqlType.TEXT));
        o.put("gc_grace_seconds", intOpt(t, "gc_grace_seconds", 864000));
        o.put("incremental_backups", opt(t, "incremental_backups", Boolean.TRUE));
        o.put("max_index_interval", intOpt(t, "max_index_interval", 2048));
        o.put("memtable_flush_period_in_ms", intOpt(t, "memtable_flush_period_in_ms", 0));
        o.put("min_index_interval", intOpt(t, "min_index_interval", 128));
        o.put("read_repair", opt(t, "read_repair", "BLOCKING"));
        o.put("speculative_retry", opt(t, "speculative_retry", "99p"));
        return o;
    }

    private static Object opt(Schema.Table t, String k, Object def) {
        Object v = t.options.get(k);
        return v == null ? def : v;
    }

    private static int intOpt(Schema.Table t, String k, int def) {
        Object v = t.options.get(k);
        return v instanceof Number n ? n.intValue() : def;
    }

    private static double dblOpt(Schema.Table t, String k, double def) {
        Object v = t.options.get(k);
        return v instanceof Number n ? n.doubleValue() : def;
    }

    private static TreeMap<Object, Object> mapOpt(Schema.Table t, String k, TreeMap<Object, Object> def) {
        Object v = t.options.get(k);
        if (v instanceof Map<?, ?> m && !m.isEmpty()) {
            TreeMap<Object, Object> r = new TreeMap<>(CqlType.TEXT);
            m.forEach((a, b) -> r.put(String.valueOf(a), b instanceof Double d && d == Math.rint(d) ? String.valueOf(d.longValue()) : String.valueOf(b)));
            return r;
        }
        return def;
    }

    /** Adds the system keyspaces to a copy of the real schema. */
    static Schema withSystem(Schema real) {
        Schema s = new Schema(real.version);
        s.keyspaces.putAll(real.keyspaces);
        for (String k : SYSTEM_KEYSPACES) {
            boolean local = k.equals("system") || k.equals("system_schema") || k.startsWith("system_v");
            Map<String, String> repl = new LinkedHashMap<>();
            repl.put("class", "org.apache.cassandra.locator." + (local ? "LocalStrategy" : "SimpleStrategy"));
            if (!local) {
                repl.put("replication_factor", k.equals("system_distributed") ? "3" : k.equals("system_traces") ? "2" : "1");
            }
            s.keyspaces.put(k, new Schema.Keyspace(k, repl, true, true));
        }
        Schema.Keyspace sys = s.keyspaces.get("system"), sch = s.keyspaces.get("system_schema"), vs = s.keyspaces.get("system_virtual_schema");
        UUID schemaVersion = schemaVersion(real);

        sys.tables.put("local", table("system", "local", () -> {
            String a = LOCAL_ADDRESS.get();
            TreeSet<Object> tok = new TreeSet<>(CqlType.TEXT);
            tok.addAll(List.of(TOKENS));
            return List.of(row("key", "local", "bootstrapped", "COMPLETED", "broadcast_address", addr(a), "broadcast_port", 7000,
                    "cluster_name", CqlWireServer.clusterName(), "cql_version", "3.4.7", "data_center", "datacenter1",
                    "gossip_generation", (int) (System.currentTimeMillis() / 1000), "host_id", HOST_ID, "listen_address", addr(a),
                    "listen_port", 7000, "native_protocol_version", "4", "partitioner", "org.apache.cassandra.dht.Murmur3Partitioner",
                    "rack", "rack1", "release_version", CqlWireServer.releaseVersion(), "rpc_address", addr(a), "rpc_port", 9042,
                    "schema_version", schemaVersion, "tokens", tok, "truncated_at", new TreeMap<Object, Object>(CqlType.UUID_T)));
        }, "key text pk", "bootstrapped text", "broadcast_address inet", "broadcast_port int", "cluster_name text", "cql_version text",
                "data_center text", "gossip_generation int", "host_id uuid", "listen_address inet", "listen_port int",
                "native_protocol_version text", "partitioner text", "rack text", "release_version text", "rpc_address inet", "rpc_port int",
                "schema_version uuid", "tokens set<text>", "truncated_at map<uuid~blob>"));
        sys.tables.put("peers", table("system", "peers", List::of, "peer inet pk", "data_center text", "host_id uuid", "preferred_ip inet",
                "rack text", "release_version text", "rpc_address inet", "schema_version uuid", "tokens set<text>"));
        sys.tables.put("peers_v2", table("system", "peers_v2", List::of, "peer inet pk", "peer_port int ck", "data_center text",
                "host_id uuid", "native_address inet", "native_port int", "preferred_ip inet", "preferred_port int", "rack text",
                "release_version text", "schema_version uuid", "tokens set<text>"));
        sys.tables.put("available_ranges", table("system", "available_ranges", List::of, "keyspace_name text pk", "ranges set<blob>"));
        sys.tables.put("size_estimates", table("system", "size_estimates", List::of, "keyspace_name text pk", "table_name text ck",
                "range_start text ck", "range_end text ck", "mean_partition_size bigint", "partitions_count bigint"));

        sch.tables.put("keyspaces", table("system_schema", "keyspaces", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                TreeMap<Object, Object> m = new TreeMap<>(CqlType.TEXT);
                m.putAll(k.replication);
                r.add(row("keyspace_name", k.name, "durable_writes", k.durableWrites, "replication", m));
            }
            return r;
        }, "keyspace_name text pk", "durable_writes boolean", "replication frozen<map<text~text>>"));

        sch.tables.put("tables", table("system_schema", "tables", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                for (var t : k.tables.values()) {
                    TreeSet<Object> flags = new TreeSet<>(CqlType.TEXT);
                    flags.add(t.clustering.isEmpty() && t.columns.stream().noneMatch(c -> c.kind == Schema.Kind.REGULAR) ? "compound" : "compound");
                    if (t.counter) {
                        flags.add("counter");
                    }
                    Map<String, Object> eff = effective(t);
                    Map<String, Object> m = row("keyspace_name", k.name, "table_name", t.name,
                            "additional_write_policy", eff.get("additional_write_policy"), "allow_auto_snapshot", eff.get("allow_auto_snapshot"),
                            "bloom_filter_fp_chance", eff.get("bloom_filter_fp_chance"), "caching", eff.get("caching"), "cdc", null,
                            "comment", eff.get("comment"), "compaction", eff.get("compaction"), "compression", eff.get("compression"),
                            "crc_check_chance", eff.get("crc_check_chance"), "dclocal_read_repair_chance", 0.0,
                            "default_time_to_live", eff.get("default_time_to_live"), "extensions", eff.get("extensions"), "flags", flags,
                            "gc_grace_seconds", eff.get("gc_grace_seconds"), "id", t.id, "incremental_backups", eff.get("incremental_backups"),
                            "max_index_interval", eff.get("max_index_interval"), "memtable", null,
                            "memtable_flush_period_in_ms", eff.get("memtable_flush_period_in_ms"),
                            "min_index_interval", eff.get("min_index_interval"), "read_repair", eff.get("read_repair"),
                            "read_repair_chance", 0.0, "speculative_retry", eff.get("speculative_retry"));
                    r.add(m);
                }
            }
            return r;
        }, "keyspace_name text pk", "table_name text ck", "additional_write_policy text", "allow_auto_snapshot boolean",
                "bloom_filter_fp_chance double", "caching frozen<map<text~text>>", "cdc boolean", "comment text",
                "compaction frozen<map<text~text>>", "compression frozen<map<text~text>>", "crc_check_chance double",
                "dclocal_read_repair_chance double", "default_time_to_live int", "extensions frozen<map<text~blob>>",
                "flags frozen<set<text>>", "gc_grace_seconds int", "id uuid", "incremental_backups boolean", "max_index_interval int",
                "memtable text", "memtable_flush_period_in_ms int", "min_index_interval int", "read_repair text",
                "read_repair_chance double", "speculative_retry text"));

        sch.tables.put("columns", table("system_schema", "columns", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                for (var t : k.tables.values()) {
                    for (var c : t.columns) {
                        r.add(row("keyspace_name", k.name, "table_name", t.name, "column_name", c.name,
                                "clustering_order", c.kind == Schema.Kind.CLUSTERING ? (c.desc ? "desc" : "asc") : "none",
                                "column_name_bytes", c.name.getBytes(StandardCharsets.UTF_8),
                                "kind", switch (c.kind) {
                                    case PARTITION_KEY -> "partition_key";
                                    case CLUSTERING -> "clustering";
                                    case STATIC -> "static";
                                    default -> "regular";
                                }, "position", c.isKey() ? c.position : -1, "type", c.type.cql()));
                    }
                }
            }
            return r;
        }, "keyspace_name text pk", "table_name text ck", "column_name text ck", "clustering_order text", "column_name_bytes blob",
                "kind text", "position int", "type text"));

        sch.tables.put("indexes", table("system_schema", "indexes", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                for (var t : k.tables.values()) {
                    for (var i : t.indexes) {
                        TreeMap<Object, Object> o = new TreeMap<>(CqlType.TEXT);
                        Schema.Column ic = t.col(i.column);
                        boolean coll = ic != null && ic.type.isMultiCell();
                        String target = i.kind.equals("values") && !coll ? i.column : i.kind + "(" + i.column + ")";
                        o.put("target", target);
                        if (i.custom != null) {
                            o.put("class_name", i.custom);
                        }
                        i.options.forEach((a, b) -> o.put(a, String.valueOf(b)));
                        r.add(row("keyspace_name", k.name, "table_name", t.name, "index_name", i.name,
                                "kind", i.custom != null ? "CUSTOM" : "COMPOSITES", "options", o));
                    }
                }
            }
            return r;
        }, "keyspace_name text pk", "table_name text ck", "index_name text ck", "kind text", "options frozen<map<text~text>>"));

        sch.tables.put("types", table("system_schema", "types", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                for (var u : k.types.values()) {
                    List<Object> types = new ArrayList<>();
                    u.types.forEach(t -> types.add(t.cql()));
                    r.add(row("keyspace_name", k.name, "type_name", u.name, "field_names", new ArrayList<Object>(u.fields), "field_types", types));
                }
            }
            return r;
        }, "keyspace_name text pk", "type_name text ck", "field_names frozen<list<text>>", "field_types frozen<list<text>>"));

        sch.tables.put("views", table("system_schema", "views", List::of, "keyspace_name text pk", "view_name text ck",
                "additional_write_policy text", "allow_auto_snapshot boolean", "base_table_id uuid", "base_table_name text",
                "bloom_filter_fp_chance double", "caching frozen<map<text~text>>", "cdc boolean", "comment text",
                "compaction frozen<map<text~text>>", "compression frozen<map<text~text>>", "crc_check_chance double",
                "dclocal_read_repair_chance double", "default_time_to_live int", "extensions frozen<map<text~blob>>",
                "gc_grace_seconds int", "id uuid", "include_all_columns boolean", "incremental_backups boolean",
                "max_index_interval int", "memtable text", "memtable_flush_period_in_ms int", "min_index_interval int",
                "read_repair text", "read_repair_chance double", "speculative_retry text", "where_clause text"));
        sch.tables.put("functions", table("system_schema", "functions", List::of, "keyspace_name text pk", "function_name text ck",
                "argument_types frozen<list<text>> ck", "argument_names frozen<list<text>>", "body text", "called_on_null_input boolean",
                "language text", "return_type text"));
        sch.tables.put("aggregates", table("system_schema", "aggregates", List::of, "keyspace_name text pk", "aggregate_name text ck",
                "argument_types frozen<list<text>> ck", "final_func text", "initcond text", "return_type text", "state_func text",
                "state_type text"));
        sch.tables.put("triggers", table("system_schema", "triggers", List::of, "keyspace_name text pk", "table_name text ck",
                "trigger_name text ck", "options frozen<map<text~text>>"));
        sch.tables.put("dropped_columns", table("system_schema", "dropped_columns", () -> {
            List<Map<String, Object>> r = new ArrayList<>();
            for (var k : s.keyspaces.values()) {
                for (var t : k.tables.values()) {
                    if (t.options.get("_dropped") instanceof Map<?, ?> dm) {
                        for (var e : dm.entrySet()) {
                            r.add(row("keyspace_name", k.name, "table_name", t.name, "column_name", String.valueOf(e.getKey()), "dropped_time",
                                    System.currentTimeMillis(), "kind", "regular", "type",
                                    Parser.parseType(String.valueOf(e.getValue()), k.name, s::udt).cql()));
                        }
                    }
                }
            }
            return r;
        }, "keyspace_name text pk", "table_name text ck",
                "column_name text ck", "dropped_time timestamp", "kind text", "type text"));
        sch.tables.put("column_masks", table("system_schema", "column_masks", List::of, "keyspace_name text pk", "table_name text ck",
                "column_name text ck", "function_argument_nulls frozen<list<boolean>>", "function_argument_types frozen<list<text>>",
                "function_argument_values frozen<list<text>>", "function_keyspace text", "function_name text"));

        vs.tables.put("keyspaces", table("system_virtual_schema", "keyspaces", () -> List.of(
                row("keyspace_name", "system_views"), row("keyspace_name", "system_virtual_schema")), "keyspace_name text pk"));
        vs.tables.put("tables", table("system_virtual_schema", "tables", List::of, "keyspace_name text pk", "table_name text ck",
                "comment text"));
        vs.tables.put("columns", table("system_virtual_schema", "columns", List::of, "keyspace_name text pk", "table_name text ck",
                "column_name text ck", "clustering_order text", "column_name_bytes blob", "kind text", "position int", "type text"));
        s.keyspaces.get("system_auth").tables.put("roles", table("system_auth", "roles", () -> List.of(row("role", "cassandra", "can_login",
                true, "is_superuser", true)), "role text pk", "can_login boolean", "is_superuser boolean"));
        return s;
    }
}
