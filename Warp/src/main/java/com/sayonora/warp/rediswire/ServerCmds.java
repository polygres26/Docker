package com.sayonora.warp.rediswire;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Server-level commands: INFO, CONFIG, COMMAND, CLUSTER, SLOWLOG, LATENCY, MEMORY, ACL, TIME, scripting stubs. */
final class ServerCmds {

    private static final SecureRandom RNG = new SecureRandom();
    private static final String NO_SCRIPTING = "ERR scripting is not supported by Warp's Redis frontend (no Lua engine); "
            + "EVAL, EVALSHA, SCRIPT and FUNCTION are unavailable";

    private ServerCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("info", -1, "loading stale", 0, 0, 0, "@slow @dangerous", ServerCmds::info);
        r.add("config", -2, "admin noscript loading stale", 0, 0, 0, "@slow @dangerous", ServerCmds::config);
        r.add("command", -1, "loading stale", 0, 0, 0, "@slow @connection", ServerCmds::command);
        r.add("time", 1, "loading stale fast", 0, 0, 0, "@fast", (x, a) -> {
            long ms = System.currentTimeMillis();
            long micros = (System.nanoTime() / 1000) % 1000;
            return new ArrayList<Object>(List.of(Long.toString(ms / 1000), Long.toString((ms % 1000) * 1000 + micros)));
        });
        r.add("slowlog", -2, "admin loading stale", 0, 0, 0, "@admin @slow @dangerous", ServerCmds::slowlog);
        r.add("latency", -2, "admin noscript loading stale", 0, 0, 0, "@admin @slow @dangerous", ServerCmds::latency);
        r.add("memory", -2, "readonly", 2, 2, 1, "@slow", ServerCmds::memory);
        r.add("acl", -2, "admin noscript loading stale", 0, 0, 0, "@admin @slow @dangerous", ServerCmds::acl);
        r.add("cluster", -2, "loading stale", 0, 0, 0, "@slow", ServerCmds::cluster);
        r.add("wait", 3, "noscript", 0, 0, 0, "@slow @connection", (x, a) -> {
            if (Num.parseLong(a[1]) == null || Num.parseLong(a[2]) == null) {
                throw RedisError.notInt();
            }
            return 0L;
        });
        r.add("waitaof", 4, "noscript", 0, 0, 0, "@slow @connection", (x, a) -> new ArrayList<Object>(List.of(0L, 0L)));
        r.add("lolwut", -1, "readonly fast", 0, 0, 0, "@read @fast", (x, a) -> new Resp.Verbatim("Redis ver. " + ConnCmds.VERSION + "\n"));
        r.add("save", 1, "admin noscript no_async_loading", 0, 0, 0, "@admin @slow @dangerous", (x, a) -> Resp.OK);
        r.add("bgsave", -1, "admin noscript no_async_loading", 0, 0, 0, "@admin @slow @dangerous", (x, a) -> new Resp.Simple("Background saving started"));
        r.add("bgrewriteaof", 1, "admin noscript no_async_loading", 0, 0, 0, "@admin @slow @dangerous",
                (x, a) -> new Resp.Simple("Background append only file rewriting started"));
        r.add("lastsave", 1, "loading stale fast", 0, 0, 0, "@admin @fast @dangerous", (x, a) -> System.currentTimeMillis() / 1000);
        r.add("role", 1, "noscript loading stale fast", 0, 0, 0, "@admin @fast @dangerous", (x, a) -> new ArrayList<Object>(List.of("master", 0L, new ArrayList<>())));
        r.add("shutdown", -1, "admin noscript loading stale", 0, 0, 0, "@admin @slow @dangerous", (x, a) -> {
            throw RedisError.err("SHUTDOWN is not supported: this server is a frontend of Warp");
        });
        for (String c : new String[] {"replicaof", "slaveof", "failover", "sync", "psync", "replconf", "monitor", "debug", "migrate", "module"}) {
            r.add(c, -1, "admin", 0, 0, 0, "@admin @slow @dangerous", (x, a) -> unsupported(a));
        }
        for (String c : new String[] {"eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro"}) {
            r.add(c, -3, "noscript skip_monitor may_replicate no_mandatory_keys stale movablekeys", 0, 0, 0, "@slow @scripting",
                    (x, a) -> {
                        String n = Num.str(a[0]).toLowerCase(Locale.ROOT);
                        if (n.startsWith("evalsha")) {
                            throw new RedisError("NOSCRIPT No matching script. Please use EVAL.");
                        }
                        throw new RedisError(NO_SCRIPTING);
                    }).keys(a -> ListCmds.numKeys(a, 2));
        }
        r.add("script", -2, "noscript", 0, 0, 0, "@slow @scripting", ServerCmds::script);
        r.add("function", -2, "noscript", 0, 0, 0, "@slow @scripting", ServerCmds::function);
    }

    private static Object unsupported(byte[][] a) {
        String n = Num.str(a[0]).toUpperCase(Locale.ROOT);
        if (n.equals("DEBUG")) {
            throw RedisError.err("DEBUG command not allowed. This server does not implement DEBUG");
        }
        if (n.equals("MODULE")) {
            if (a.length > 1 && Num.eq(a[1], "LIST")) {
                return new ArrayList<>();
            }
            throw RedisError.err("MODULE is not supported by this server");
        }
        throw RedisError.err(n + " is not supported by this server (data lives in Postgres; replication and monitoring commands are unavailable)");
    }

    private static Object script(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "FLUSH":
                return Resp.OK;
            case "EXISTS": {
                List<Object> out = new ArrayList<>();
                for (int i = 2; i < a.length; i++) {
                    out.add(0L);
                }
                return out;
            }
            case "KILL":
                throw new RedisError("NOTBUSY No scripts in execution right now.");
            case "HELP":
                return Resp.help("SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "EXISTS <sha1> [<sha1> ...]", "FLUSH [ASYNC|SYNC]", "HELP");
            default:
                throw new RedisError(NO_SCRIPTING);
        }
    }

    private static Object function(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "FLUSH":
                return Resp.OK;
            case "LIST":
                return new ArrayList<>();
            case "STATS": {
                List<Object> f = new ArrayList<>();
                f.add("running_script");
                f.add(null);
                f.add("engines");
                f.add(new Resp.RMap(new ArrayList<>()));
                return new Resp.RMap(f);
            }
            default:
                throw new RedisError(NO_SCRIPTING);
        }
    }

    // ------------------------------------------------------------------------------------------
    // INFO
    // ------------------------------------------------------------------------------------------

    private static String kv(String k, Object v) {
        return k + ":" + v + "\r\n";
    }

    private static String section(Ctx x, String name) throws Exception {
        RedisStore st = x.store;
        StringBuilder sb = new StringBuilder();
        long now = System.currentTimeMillis();
        long uptime = (now - st.startMillis) / 1000;
        Runtime rt = Runtime.getRuntime();
        switch (name) {
            case "server" -> {
                sb.append("# Server\r\n").append(kv("redis_version", ConnCmds.VERSION)).append(kv("redis_git_sha1", "00000000"))
                        .append(kv("redis_git_dirty", 0)).append(kv("redis_build_id", "warp-rediswire")).append(kv("redis_mode", "standalone"))
                        .append(kv("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch")))
                        .append(kv("arch_bits", 64)).append(kv("monotonic_clock", "POSIX clock_gettime")).append(kv("multiplexing_api", "epoll"))
                        .append(kv("atomicvar_api", "c11-builtin")).append(kv("gcc_version", "0.0.0"))
                        .append(kv("process_id", ProcessHandle.current().pid())).append(kv("process_supervised", "no")).append(kv("run_id", st.runId))
                        .append(kv("tcp_port", st.options().port())).append(kv("server_time_usec", now * 1000)).append(kv("uptime_in_seconds", uptime))
                        .append(kv("uptime_in_days", uptime / 86400)).append(kv("hz", st.config.get("hz"))).append(kv("configured_hz", st.config.get("hz")))
                        .append(kv("lru_clock", (now / 1000) & 0xFFFFFF)).append(kv("executable", "")).append(kv("config_file", ""))
                        .append(kv("io_threads_active", 0)).append(kv("backing_store", "postgres"));
            }
            case "clients" -> {
                long blocked = st.clients.values().stream().filter(c -> c.blocked).count();
                sb.append("# Clients\r\n").append(kv("connected_clients", st.clients.size())).append(kv("cluster_connections", 0))
                        .append(kv("maxclients", st.config.get("maxclients"))).append(kv("client_recent_max_input_buffer", 0))
                        .append(kv("client_recent_max_output_buffer", 0)).append(kv("blocked_clients", blocked))
                        .append(kv("tracking_clients", 0)).append(kv("pubsub_clients", st.clients.values().stream().filter(Session::subscribedMode).count()))
                        .append(kv("watching_clients", st.clients.values().stream().filter(c -> !c.watches.isEmpty()).count()))
                        .append(kv("clients_in_timeout_table", 0)).append(kv("total_watched_keys", st.clients.values().stream().mapToInt(c -> c.watches.size()).sum()))
                        .append(kv("total_blocking_keys", blocked)).append(kv("total_blocking_keys_on_nokey", 0));
            }
            case "memory" -> {
                long used = rt.totalMemory() - rt.freeMemory();
                sb.append("# Memory\r\n").append(kv("used_memory", used)).append(kv("used_memory_human", human(used))).append(kv("used_memory_rss", rt.totalMemory()))
                        .append(kv("used_memory_rss_human", human(rt.totalMemory()))).append(kv("used_memory_peak", rt.totalMemory()))
                        .append(kv("used_memory_peak_human", human(rt.totalMemory()))).append(kv("used_memory_overhead", 0)).append(kv("used_memory_dataset", 0))
                        .append(kv("total_system_memory", rt.maxMemory())).append(kv("total_system_memory_human", human(rt.maxMemory())))
                        .append(kv("maxmemory", st.config.get("maxmemory"))).append(kv("maxmemory_human", "0B"))
                        .append(kv("maxmemory_policy", st.config.get("maxmemory-policy"))).append(kv("mem_fragmentation_ratio", "1.00"))
                        .append(kv("mem_allocator", "jvm")).append(kv("lazyfree_pending_objects", 0)).append(kv("lazyfreed_objects", 0));
            }
            case "persistence" -> sb.append("# Persistence\r\n").append(kv("loading", 0)).append(kv("async_loading", 0)).append(kv("current_cow_peak", 0))
                    .append(kv("rdb_changes_since_last_save", 0)).append(kv("rdb_bgsave_in_progress", 0)).append(kv("rdb_last_save_time", now / 1000))
                    .append(kv("rdb_last_bgsave_status", "ok")).append(kv("aof_enabled", 0)).append(kv("aof_rewrite_in_progress", 0))
                    .append(kv("aof_last_bgrewrite_status", "ok")).append(kv("aof_last_write_status", "ok"));
            case "stats" -> sb.append("# Stats\r\n").append(kv("total_connections_received", st.totalConnections.get()))
                    .append(kv("total_commands_processed", st.totalCommands.get())).append(kv("instantaneous_ops_per_sec", 0))
                    .append(kv("total_net_input_bytes", 0)).append(kv("total_net_output_bytes", 0)).append(kv("rejected_connections", st.rejectedConnections.get()))
                    .append(kv("sync_full", 0)).append(kv("sync_partial_ok", 0)).append(kv("sync_partial_err", 0))
                    .append(kv("expired_keys", st.expiredKeys.get())).append(kv("evicted_keys", 0)).append(kv("keyspace_hits", 0)).append(kv("keyspace_misses", 0))
                    .append(kv("pubsub_channels", st.hub().channels.values().stream().mapToInt(Map::size).sum()))
                    .append(kv("pubsub_patterns", st.hub().patterns.values().stream().mapToInt(Map::size).sum()))
                    .append(kv("pubsubshard_channels", st.hub().shardChannels.values().stream().mapToInt(Map::size).sum()))
                    .append(kv("total_error_replies", 0)).append(kv("acl_access_denied_auth", 0));
            case "replication" -> sb.append("# Replication\r\n").append(kv("role", "master")).append(kv("connected_slaves", 0))
                    .append(kv("master_failover_state", "no-failover")).append(kv("master_replid", st.runId.substring(0, 40)))
                    .append(kv("master_replid2", "0000000000000000000000000000000000000000")).append(kv("master_repl_offset", 0))
                    .append(kv("second_repl_offset", -1)).append(kv("repl_backlog_active", 0));
            case "cpu" -> {
                double cpu = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
                sb.append("# CPU\r\n").append(kv("used_cpu_sys", "0.000000")).append(kv("used_cpu_user", "0.000000"))
                        .append(kv("used_cpu_sys_children", "0.000000")).append(kv("used_cpu_user_children", "0.000000"));
            }
            case "modules" -> sb.append("# Modules\r\n");
            case "errorstats" -> sb.append("# Errorstats\r\n");
            case "cluster" -> sb.append("# Cluster\r\n").append(kv("cluster_enabled", 0));
            case "keyspace" -> {
                sb.append("# Keyspace\r\n");
                Map<Integer, long[]> dbs = new TreeMap<>();
                for (int s = 0; s < x.shardCount(); s++) {
                    x.useShard(s);
                    for (long[] row : x.list("SELECT db, count(*), count(exp) FROM warp_redis_keys WHERE (exp IS NULL OR exp > ?) GROUP BY db",
                            rs -> new long[] {rs.getLong(1), rs.getLong(2), rs.getLong(3)}, x.now)) {
                        long[] cur = dbs.computeIfAbsent((int) row[0], k -> new long[2]);
                        cur[0] += row[1];
                        cur[1] += row[2];
                    }
                }
                for (Map.Entry<Integer, long[]> e : dbs.entrySet()) {
                    sb.append("db").append(e.getKey()).append(":keys=").append(e.getValue()[0]).append(",expires=").append(e.getValue()[1])
                            .append(",avg_ttl=0,subexpiry=0\r\n");
                }
            }
            case "commandstats" -> sb.append("# Commandstats\r\n");
            case "latencystats" -> sb.append("# Latencystats\r\n");
            default -> {
            }
        }
        return sb.toString();
    }

    private static String human(long b) {
        if (b < 1024) {
            return b + "B";
        }
        double k = b / 1024.0;
        if (k < 1024) {
            return String.format(Locale.ROOT, "%.2fK", k);
        }
        double m = k / 1024;
        return m < 1024 ? String.format(Locale.ROOT, "%.2fM", m) : String.format(Locale.ROOT, "%.2fG", m / 1024);
    }

    private static final List<String> DEFAULT_SECTIONS = List.of("server", "clients", "memory", "persistence", "stats", "replication", "cpu",
            "modules", "errorstats", "cluster", "keyspace");
    private static final List<String> ALL_SECTIONS = List.of("server", "clients", "memory", "persistence", "stats", "replication", "cpu",
            "modules", "errorstats", "cluster", "keyspace", "commandstats", "latencystats");

    private static Object info(Ctx x, byte[][] a) throws Exception {
        List<String> want = new ArrayList<>();
        if (a.length == 1) {
            want.addAll(DEFAULT_SECTIONS);
        }
        for (int i = 1; i < a.length; i++) {
            String s = Num.str(a[i]).toLowerCase(Locale.ROOT);
            if (s.equals("default")) {
                want.addAll(DEFAULT_SECTIONS);
            } else if (s.equals("all") || s.equals("everything")) {
                want.addAll(ALL_SECTIONS);
            } else if (!want.contains(s)) {
                want.add(s);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String s : want) {
            String body = section(x, s);
            if (!body.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append("\r\n");
                }
                sb.append(body);
            }
        }
        return new Resp.Verbatim(sb.toString());
    }

    // ------------------------------------------------------------------------------------------
    // CONFIG
    // ------------------------------------------------------------------------------------------

    private static Object config(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        Map<String, String> cfg = x.store.config;
        switch (sub) {
            case "GET": {
                if (a.length < 3) {
                    throw RedisError.arity("config|get");
                }
                Map<String, String> out = new TreeMap<>();
                for (int i = 2; i < a.length; i++) {
                    for (Map.Entry<String, String> e : cfg.entrySet()) {
                        if (Glob.matches(Num.str(a[i]).toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8), e.getKey().getBytes(StandardCharsets.UTF_8))) {
                            out.put(e.getKey(), e.getValue());
                        }
                    }
                }
                List<Object> flat = new ArrayList<>();
                for (Map.Entry<String, String> e : out.entrySet()) {
                    flat.add(e.getKey());
                    flat.add(e.getValue());
                }
                return new Resp.RMap(flat);
            }
            case "SET": {
                if (a.length < 4 || a.length % 2 != 0) {
                    throw RedisError.arity("config|set");
                }
                Map<String, String> updates = new java.util.LinkedHashMap<>();
                for (int i = 2; i < a.length; i += 2) {
                    String k = Num.str(a[i]).toLowerCase(Locale.ROOT);
                    if (!cfg.containsKey(k)) {
                        throw RedisError.err("Unknown option or number of arguments for CONFIG SET - '" + Num.str(a[i]) + "'");
                    }
                    if (updates.containsKey(k)) {
                        throw RedisError.err("CONFIG SET failed (possibly related to argument '" + k + "') - duplicate parameter");
                    }
                    String v = Num.str(a[i + 1]);
                    if (List.of("maxmemory", "hz", "timeout", "slowlog-log-slower-than", "slowlog-max-len", "tcp-keepalive", "maxclients", "databases")
                            .contains(k) && Num.parseLong(v) == null) {
                        throw RedisError.err("CONFIG SET failed (possibly related to argument '" + k + "') - argument couldn't be parsed into an integer");
                    }
                    updates.put(k, v);
                }
                cfg.putAll(updates);
                return Resp.OK;
            }
            case "RESETSTAT":
                x.store.slowLog.reset();
                x.store.expiredKeys.set(0);
                x.store.totalCommands.set(0);
                x.store.totalConnections.set(0);
                return Resp.OK;
            case "REWRITE":
                throw RedisError.err("The server is running without a config file");
            case "HELP":
                return Resp.help("CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "GET <pattern>", "SET <directive> <value>",
                        "RESETSTAT", "REWRITE", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try CONFIG HELP.");
        }
    }

    // ------------------------------------------------------------------------------------------
    // COMMAND
    // ------------------------------------------------------------------------------------------

    private static Object cmdInfo(Cmd c) {
        List<Object> flags = new ArrayList<>();
        for (String f : c.flags.split(" ")) {
            if (!f.isEmpty()) {
                flags.add(new Resp.Simple(f));
            }
        }
        List<Object> cats = new ArrayList<>();
        for (String f : c.categories.split(" ")) {
            if (!f.isEmpty()) {
                cats.add(new Resp.Simple(f));
            }
        }
        List<Object> info = new ArrayList<>();
        info.add(c.name);
        info.add((long) c.arity);
        info.add(flags);
        info.add((long) c.first);
        info.add((long) c.last);
        info.add((long) c.step);
        info.add(cats);
        info.add(new ArrayList<>());
        info.add(new ArrayList<>());
        info.add(new ArrayList<>());
        return info;
    }

    private static Object command(Ctx x, byte[][] a) {
        Cmd.Registry reg = x.session.registry;
        if (a.length == 1) {
            List<Object> out = new ArrayList<>();
            for (Cmd c : reg.all()) {
                out.add(cmdInfo(c));
            }
            return out;
        }
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "COUNT":
                return (long) reg.size();
            case "INFO": {
                List<Object> out = new ArrayList<>();
                if (a.length == 2) {
                    for (Cmd c : reg.all()) {
                        out.add(cmdInfo(c));
                    }
                    return out;
                }
                for (int i = 2; i < a.length; i++) {
                    Cmd c = reg.get(Num.str(a[i]).toLowerCase(Locale.ROOT));
                    out.add(c == null ? null : cmdInfo(c));
                }
                return out;
            }
            case "LIST": {
                List<Object> out = new ArrayList<>();
                String filterKind = null;
                String filterArg = null;
                if (a.length > 2) {
                    if (a.length != 5 || !Num.eq(a[2], "FILTERBY")) {
                        throw RedisError.syntax();
                    }
                    filterKind = Num.upper(a[3]);
                    filterArg = Num.str(a[4]);
                }
                for (Cmd c : reg.all()) {
                    if (filterKind != null) {
                        boolean ok = switch (filterKind) {
                            case "PATTERN" -> Glob.matches(filterArg.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8), c.name.getBytes(StandardCharsets.UTF_8));
                            case "ACLCAT" -> c.categories.contains("@" + filterArg.toLowerCase(Locale.ROOT));
                            case "MODULE" -> false;
                            default -> throw RedisError.syntax();
                        };
                        if (!ok) {
                            continue;
                        }
                    }
                    out.add(c.name);
                }
                return out;
            }
            case "DOCS": {
                List<Object> flat = new ArrayList<>();
                List<Cmd> cs = new ArrayList<>();
                if (a.length == 2) {
                    cs.addAll(reg.all());
                } else {
                    for (int i = 2; i < a.length; i++) {
                        Cmd c = reg.get(Num.str(a[i]).toLowerCase(Locale.ROOT));
                        if (c != null) {
                            cs.add(c);
                        }
                    }
                }
                for (Cmd c : cs) {
                    List<Object> d = new ArrayList<>();
                    d.add("summary");
                    d.add("The " + c.name.toUpperCase(Locale.ROOT) + " command.");
                    d.add("since");
                    d.add("1.0.0");
                    d.add("group");
                    d.add(c.categories.contains("@") ? c.categories.split(" ")[c.categories.startsWith("@write") || c.categories.startsWith("@read") ? 1 : 0].substring(1) : "generic");
                    d.add("complexity");
                    d.add("O(1)");
                    flat.add(c.name);
                    flat.add(new Resp.RMap(d));
                }
                return new Resp.RMap(flat);
            }
            case "GETKEYS":
            case "GETKEYSANDFLAGS": {
                if (a.length < 3) {
                    throw RedisError.arity("command|" + sub.toLowerCase(Locale.ROOT));
                }
                byte[][] inner = Arrays.copyOfRange(a, 2, a.length);
                Cmd c = reg.get(Num.str(inner[0]).toLowerCase(Locale.ROOT));
                if (c == null) {
                    throw RedisError.err("Invalid command specified");
                }
                if ((c.arity > 0 && inner.length != c.arity) || inner.length < -c.arity) {
                    throw RedisError.err("Invalid number of arguments specified for command");
                }
                int[] kp = c.keyPositions(inner);
                if (kp.length == 0) {
                    throw RedisError.err("The command has no key arguments");
                }
                List<Object> out = new ArrayList<>();
                for (int p : kp) {
                    if (sub.equals("GETKEYS")) {
                        out.add(inner[p]);
                    } else {
                        out.add(new ArrayList<Object>(List.of(inner[p], new ArrayList<Object>(List.of(new Resp.Simple(c.isWrite() ? "RW" : "RO"))))));
                    }
                }
                return out;
            }
            case "HELP":
                return Resp.help("COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "(no subcommand)", "COUNT", "DOCS [<command-name> ...]",
                        "GETKEYS <full-command>", "INFO [<command-name> ...]", "LIST [FILTERBY (MODULE <module-name>|ACLCAT <category>|PATTERN <pattern>)]", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try COMMAND HELP.");
        }
    }

    // ------------------------------------------------------------------------------------------
    // SLOWLOG / LATENCY / MEMORY / ACL
    // ------------------------------------------------------------------------------------------

    private static Object slowlog(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        RedisStore.SlowLog log = x.store.slowLog;
        switch (sub) {
            case "GET": {
                long count = 10;
                if (a.length > 2) {
                    Long c = Num.parseLong(a[2]);
                    if (c == null || c < -1) {
                        throw RedisError.err("count should be greater than or equal to -1");
                    }
                    count = c == -1 ? Integer.MAX_VALUE : c;
                }
                List<Object> out = new ArrayList<>();
                for (Object[] e : log.get((int) Math.min(count, Integer.MAX_VALUE))) {
                    List<Object> args = new ArrayList<>();
                    for (byte[] b : (byte[][]) e[3]) {
                        args.add(b);
                    }
                    out.add(new ArrayList<Object>(Arrays.asList(e[0], e[1], e[2], args, e[4] == null ? "" : e[4], e[5] == null ? "" : e[5])));
                }
                return out;
            }
            case "LEN":
                return (long) log.len();
            case "RESET":
                log.reset();
                return Resp.OK;
            case "HELP":
                return Resp.help("SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "GET [<count>]", "LEN", "RESET", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try SLOWLOG HELP.");
        }
    }

    private static Object latency(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "LATEST":
                return new ArrayList<>();
            case "HISTORY":
                if (a.length != 3) {
                    throw RedisError.arity("latency|history");
                }
                return new ArrayList<>();
            case "RESET":
                return 0L;
            case "DOCTOR":
                return new Resp.Verbatim("Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sit down calmly, take a stress pill, and think things over.\n");
            case "GRAPH":
                throw RedisError.err("No samples available for event '" + (a.length > 2 ? Num.str(a[2]) : "") + "'");
            case "HISTOGRAM":
                return new Resp.RMap(new ArrayList<>());
            case "HELP":
                return Resp.help("LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "DOCTOR", "GRAPH <event>", "HISTORY <event>", "LATEST", "RESET [<event> ...]", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try LATENCY HELP.");
        }
    }

    private static Object memory(Ctx x, byte[][] a) throws Exception {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "USAGE": {
                if (a.length < 3) {
                    throw RedisError.arity("memory|usage");
                }
                Ctx.Meta m = x.get(a[2]);
                if (m == null) {
                    return null;
                }
                String q = switch (m.type()) {
                    case Ctx.T_STRING -> "SELECT coalesce(octet_length(sv), 0) + 16 FROM warp_redis_keys WHERE db = ? AND k = ?";
                    case Ctx.T_HASH -> "SELECT coalesce(sum(octet_length(f) + octet_length(v) + 24), 0) + 56 FROM warp_redis_hashes WHERE db = ? AND k = ?";
                    case Ctx.T_LIST -> "SELECT coalesce(sum(octet_length(v) + 16), 0) + 40 FROM warp_redis_lists WHERE db = ? AND k = ?";
                    case Ctx.T_SET -> "SELECT coalesce(sum(octet_length(m) + 16), 0) + 56 FROM warp_redis_sets WHERE db = ? AND k = ?";
                    case Ctx.T_ZSET -> "SELECT coalesce(sum(octet_length(m) + 24), 0) + 64 FROM warp_redis_zsets WHERE db = ? AND k = ?";
                    default -> "SELECT coalesce(sum(octet_length(f) + 32), 0) + 96 FROM warp_redis_stream_entries WHERE db = ? AND k = ?";
                };
                return x.scalar(q, x.db, a[2]) + a[2].length + 48;
            }
            case "STATS": {
                Runtime rt = Runtime.getRuntime();
                List<Object> f = new ArrayList<>();
                f.add("peak.allocated");
                f.add(rt.totalMemory());
                f.add("total.allocated");
                f.add(rt.totalMemory() - rt.freeMemory());
                f.add("keys.count");
                f.add(0L);
                return new Resp.RMap(f);
            }
            case "DOCTOR":
                return new Resp.Verbatim("Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base.\n");
            case "PURGE":
                return Resp.OK;
            case "MALLOC-STATS":
                return new Resp.Verbatim("Stats not supported for the current allocator\n");
            case "HELP":
                return Resp.help("MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "DOCTOR", "MALLOC-STATS", "PURGE", "STATS", "USAGE <key> [SAMPLES <count>]", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try MEMORY HELP.");
        }
    }

    private static Object acl(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "WHOAMI":
                return x.session.user;
            case "LIST":
                return new ArrayList<Object>(List.of("user default on " + (x.store.password() == null ? "nopass" : "#" + "*") + " ~* &* +@all"));
            case "USERS":
                return new ArrayList<Object>(List.of("default"));
            case "GETUSER": {
                if (a.length != 3) {
                    throw RedisError.arity("acl|getuser");
                }
                if (!Num.str(a[2]).equals("default")) {
                    return null;
                }
                List<Object> f = new ArrayList<>();
                f.add("flags");
                f.add(new Resp.RSet(new ArrayList<Object>(List.of(x.store.password() == null ? "nopass" : "on", "on"))));
                f.add("passwords");
                f.add(new ArrayList<>());
                f.add("commands");
                f.add("+@all");
                f.add("keys");
                f.add("~*");
                f.add("channels");
                f.add("&*");
                f.add("selectors");
                f.add(new ArrayList<>());
                return new Resp.RMap(f);
            }
            case "CAT": {
                List<String> cats = List.of("keyspace", "read", "write", "set", "sortedset", "list", "hash", "string", "bitmap", "hyperloglog", "geo",
                        "stream", "pubsub", "admin", "fast", "slow", "blocking", "dangerous", "connection", "transaction", "scripting");
                if (a.length == 2) {
                    return new ArrayList<Object>(cats);
                }
                List<Object> out = new ArrayList<>();
                String cat = "@" + Num.str(a[2]).toLowerCase(Locale.ROOT);
                if (!cats.contains(cat.substring(1))) {
                    throw RedisError.err("Unknown category '" + Num.str(a[2]) + "'");
                }
                for (Cmd c : x.session.registry.all()) {
                    if (c.categories.contains(cat)) {
                        out.add(c.name);
                    }
                }
                return out;
            }
            case "GENPASS": {
                long bits = 256;
                if (a.length > 2) {
                    Long b = Num.parseLong(a[2]);
                    if (b == null || b < 1 || b > 4096) {
                        throw RedisError.err("ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096");
                    }
                    bits = b;
                }
                int chars = (int) ((bits + 3) / 4);
                byte[] raw = new byte[(chars + 1) / 2];
                RNG.nextBytes(raw);
                StringBuilder sb = new StringBuilder();
                for (byte b : raw) {
                    sb.append(String.format("%02x", b));
                }
                return sb.substring(0, chars);
            }
            case "LOG":
                return new ArrayList<>();
            case "SETUSER":
            case "DELUSER":
            case "LOAD":
            case "SAVE":
            case "DRYRUN":
                throw RedisError.err("ACL " + sub + " is not supported: the server has one user; authentication is the WARP_REDISWIRE_PASSWORD/requirepass password and the AUTH username selects a connect-time route");
            case "HELP":
                return Resp.help("ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "CAT [<category>]", "GENPASS [<bits>]", "GETUSER <username>",
                        "LIST", "LOG [<count> | RESET]", "USERS", "WHOAMI", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try ACL HELP.");
        }
    }

    // ------------------------------------------------------------------------------------------
    // CLUSTER
    // ------------------------------------------------------------------------------------------

    private static String advertisedHost(Ctx x) {
        String h = x.store.options().advertiseHost();
        if (h != null) {
            return h;
        }
        if (x.session.socket.getLocalSocketAddress() instanceof InetSocketAddress isa && isa.getAddress() != null) {
            return isa.getAddress().getHostAddress();
        }
        return "127.0.0.1";
    }

    private static int advertisedPort(Ctx x) {
        return x.session.socket.getLocalPort();
    }

    private static Object cluster(Ctx x, byte[][] a) throws Exception {
        String sub = Num.upper(a[1]);
        RedisStore st = x.store;
        String host = advertisedHost(x);
        int port = advertisedPort(x);
        String id = st.clusterNodeId;
        switch (sub) {
            case "SLOTS": {
                List<Object> node = new ArrayList<>();
                node.add(host);
                node.add((long) port);
                node.add(id);
                node.add(new ArrayList<>());
                List<Object> range = new ArrayList<>();
                range.add(0L);
                range.add((long) (Slot.SLOTS - 1));
                range.add(node);
                return new ArrayList<Object>(List.of(range));
            }
            case "SHARDS": {
                List<Object> n = new ArrayList<>();
                n.add("id");
                n.add(id);
                n.add("port");
                n.add((long) port);
                n.add("ip");
                n.add(host);
                n.add("endpoint");
                n.add(host);
                n.add("role");
                n.add("master");
                n.add("replication-offset");
                n.add(0L);
                n.add("health");
                n.add("online");
                List<Object> s = new ArrayList<>();
                s.add("slots");
                s.add(new ArrayList<Object>(List.of(0L, (long) (Slot.SLOTS - 1))));
                s.add("nodes");
                s.add(new ArrayList<Object>(List.of(new Resp.RMap(n))));
                return new ArrayList<Object>(List.of(new Resp.RMap(s)));
            }
            case "NODES":
                return new Resp.Verbatim(id + " " + host + ":" + port + "@" + (port + 10000) + " myself,master - 0 0 1 connected 0-" + (Slot.SLOTS - 1) + "\n");
            case "INFO":
                return new Resp.Verbatim("cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
                        + "cluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\n"
                        + "cluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n");
            case "KEYSLOT":
                if (a.length != 3) {
                    throw RedisError.arity("cluster|keyslot");
                }
                return (long) Slot.of(a[2]);
            case "MYID":
                return id;
            case "MYSHARDID":
                return id;
            case "COUNTKEYSINSLOT": {
                Long slot = a.length == 3 ? Num.parseLong(a[2]) : null;
                if (slot == null || slot < 0 || slot >= Slot.SLOTS) {
                    throw RedisError.err("Invalid slot");
                }
                x.requireFanOut();
                long n = 0;
                int shard = Slot.shardOf(slot.intValue(), x.shardCount());
                x.useShard(shard);
                n += x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND slot = ? AND (exp IS NULL OR exp > ?)", x.db, slot.intValue(), x.now);
                return n;
            }
            case "GETKEYSINSLOT": {
                Long slot = a.length == 4 ? Num.parseLong(a[2]) : null;
                Long count = a.length == 4 ? Num.parseLong(a[3]) : null;
                if (slot == null || slot < 0 || slot >= Slot.SLOTS) {
                    throw RedisError.err("Invalid slot");
                }
                if (count == null || count < 0) {
                    throw RedisError.err("Invalid number of keys");
                }
                x.useShard(Slot.shardOf(slot.intValue(), x.shardCount()));
                return new ArrayList<Object>(x.list("SELECT k FROM warp_redis_keys WHERE db = ? AND slot = ? AND (exp IS NULL OR exp > ?) ORDER BY id LIMIT ?",
                        rs -> rs.getBytes(1), x.db, slot.intValue(), x.now, count));
            }
            case "COUNT-FAILURE-REPORTS":
                return 0L;
            case "LINKS":
                return new ArrayList<>();
            case "REPLICAS":
            case "SLAVES":
                return new ArrayList<>();
            case "HELP":
                return Resp.help("CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "COUNTKEYSINSLOT <slot>", "GETKEYSINSLOT <slot> <count>", "INFO",
                        "KEYSLOT <key>", "MYID", "NODES", "SHARDS", "SLOTS", "HELP");
            default:
                throw RedisError.err("This instance's cluster topology is managed by Warp; CLUSTER " + sub + " is not supported");
        }
    }
}
