package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.rediswire.RedisEmbedded;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis vocabulary. Tool names follow the official Redis MCP server (redis/mcp-redis: set, get, delete, type, expire,
 * rename, scan_keys, scan_all_keys, hset/hget/hgetall/hdel/hexists, lpush/rpush/lpop/rpop/lrange/llen, sadd/srem/smembers,
 * zadd/zrange/zrem, xadd/xrange/xdel, publish, dbsize, info), prefixed {@code redis_} so they cannot collide with another
 * store's tools. Every tool runs the real Redis command through rediswire's own command engine (same sharding by hash slot,
 * same Postgres tables), so data written by a RESP client is visible here and vice versa. Blocking commands, subscribe and
 * scripting are not offered. Replies are bounded (collections cut at {@link #MAX_ITEMS}, text at 256 KiB).
 */
final class RedisToolProvider extends StoreToolProvider {

    private static final int MAX_ITEMS = 1000;

    RedisToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.REDIS, describer, stores);
    }

    private RedisEmbedded redis() {
        return stores.engine("redis", reg -> new RedisEmbedded(reg, stores.sqlMetrics()));
    }

    private static Tool t(String name, String desc, boolean write, List<String> req, Object... props) {
        Object[] all = new Object[props.length + 2];
        System.arraycopy(props, 0, all, 0, props.length);
        all[props.length] = "db";
        all[props.length + 1] = num("Logical database number (default 0)");
        return new Tool(name, desc, schema(req, all), write);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject key = str("Key name");
        return List.of(
                t("redis_get", "Get the string value of a key.", false, List.of("key"), "key", key),
                t("redis_set", "Set a string value, optionally with an expiry (ex seconds / px milliseconds) or only if the key does (xx) or does not (nx) exist.",
                        true, List.of("key", "value"), "key", key, "value", str("String value"), "ex", num("Expire after N seconds"),
                        "px", num("Expire after N milliseconds"), "nx", bool("Only set if the key does not exist"),
                        "xx", bool("Only set if the key exists")),
                t("redis_delete", "Delete one or more keys; returns how many existed.", true, List.of("keys"),
                        "keys", strings("Keys to delete (all on one hash slot when the store is sharded)")),
                t("redis_type", "The type of a key: string, hash, list, set, zset, stream or none.", false, List.of("key"), "key", key),
                t("redis_expire", "Set a key's time to live in seconds.", true, List.of("key", "seconds"), "key", key,
                        "seconds", num("Seconds until the key expires")),
                t("redis_ttl", "A key's remaining time to live in seconds (-1 no expiry, -2 no such key).", false, List.of("key"), "key", key),
                t("redis_rename", "Rename a key.", true, List.of("key", "newKey"), "key", key, "newKey", str("New key name")),
                t("redis_incr", "Increment the integer value of a key by 1 or by an amount.", true, List.of("key"), "key", key,
                        "by", num("Increment (default 1; may be negative)")),
                t("redis_scan_keys", "One SCAN page of key names matching a glob pattern; continue with the returned cursor until it is \"0\".",
                        false, List.of(), "pattern", str("Glob pattern (default *)"), "cursor", str("Cursor (default 0)"),
                        "count", num("Page size hint (default 100, max 1000)")),
                t("redis_scan_all_keys", "All key names matching a glob pattern, up to a limit (iterates SCAN).", false, List.of(),
                        "pattern", str("Glob pattern (default *)"), "limit", num("Max keys (default 1000, max 1000)")),
                t("redis_hset", "Set hash fields: a single field/value or a mapping of several.", true, List.of("key"), "key", key,
                        "field", str("Field name"), "value", str("Field value"), "mapping", obj("Field name -> value pairs")),
                t("redis_hget", "Get one hash field.", false, List.of("key", "field"), "key", key, "field", str("Field name")),
                t("redis_hgetall", "All fields and values of a hash (up to 1000 fields).", false, List.of("key"), "key", key),
                t("redis_hdel", "Delete hash fields.", true, List.of("key", "fields"), "key", key, "fields", strings("Field names")),
                t("redis_hexists", "Whether a hash field exists.", false, List.of("key", "field"), "key", key, "field", str("Field name")),
                t("redis_lpush", "Push values onto the head of a list.", true, List.of("key", "values"), "key", key, "values", strings("Values")),
                t("redis_rpush", "Push values onto the tail of a list.", true, List.of("key", "values"), "key", key, "values", strings("Values")),
                t("redis_lpop", "Pop from the head of a list.", true, List.of("key"), "key", key, "count", num("How many (default 1)")),
                t("redis_rpop", "Pop from the tail of a list.", true, List.of("key"), "key", key, "count", num("How many (default 1)")),
                t("redis_lrange", "A range of list elements (default 0..99, at most 1000).", false, List.of("key"), "key", key,
                        "start", num("Start index (default 0)"), "stop", num("Stop index inclusive (default start+99)")),
                t("redis_llen", "Length of a list.", false, List.of("key"), "key", key),
                t("redis_sadd", "Add members to a set.", true, List.of("key", "members"), "key", key, "members", strings("Members")),
                t("redis_srem", "Remove members from a set.", true, List.of("key", "members"), "key", key, "members", strings("Members")),
                t("redis_smembers", "Members of a set (up to 1000).", false, List.of("key"), "key", key),
                t("redis_zadd", "Add members with scores to a sorted set.", true, List.of("key", "members"), "key", key,
                        "members", obj("Member -> score pairs")),
                t("redis_zrange", "A range of a sorted set by rank (default 0..99, at most 1000), optionally with scores.", false,
                        List.of("key"), "key", key, "start", num("Start rank (default 0)"), "stop", num("Stop rank inclusive"),
                        "withScores", bool("Include scores"), "reverse", bool("Highest score first")),
                t("redis_zrem", "Remove members from a sorted set.", true, List.of("key", "members"), "key", key, "members", strings("Members")),
                t("redis_xadd", "Append an entry to a stream.", true, List.of("key", "fields"), "key", key,
                        "fields", obj("Field -> value pairs of the entry"), "id", str("Entry id (default *)"),
                        "maxlen", num("Trim the stream to about this length")),
                t("redis_xrange", "Entries of a stream between two ids (default - .. +, at most 1000).", false, List.of("key"), "key", key,
                        "start", str("Start id (default -)"), "end", str("End id (default +)"), "count", num("Max entries (default 100)")),
                t("redis_xdel", "Delete stream entries by id.", true, List.of("key", "ids"), "key", key, "ids", strings("Entry ids")),
                t("redis_publish", "Publish a message to a pub/sub channel; returns the number of receivers.", true,
                        List.of("channel", "message"), "channel", str("Channel"), "message", str("Message")),
                t("redis_dbsize", "Number of keys in the database.", false, List.of()),
                t("redis_info", "Server information (INFO), optionally one section.", false, List.of(), "section", str("Section name")));
    }

    private JsonElement cmd(int db, String... args) {
        return redis().command(db, List.of(args));
    }

    private JsonElement cmd(int db, List<String> args) {
        return redis().command(db, args);
    }

    private static List<String> words(String... first) {
        return new ArrayList<>(List.of(first));
    }

    private static List<String> strs(JsonObject a, String k) {
        List<String> out = new ArrayList<>();
        if (!a.has(k) || !a.get(k).isJsonArray() || a.getAsJsonArray(k).isEmpty()) {
            throw new IllegalArgumentException(k + " must be a non-empty array of strings");
        }
        a.getAsJsonArray(k).forEach(e -> out.add(e.getAsString()));
        return out;
    }

    private static JsonObject one(String k, JsonElement v) {
        JsonObject o = new JsonObject();
        o.add(k, v);
        return o;
    }

    /** Cuts a flat array to {@code max} items, adding truncated/count. */
    private static JsonObject bounded(String name, JsonElement arr, int max) {
        JsonArray a = arr.isJsonArray() ? arr.getAsJsonArray() : new JsonArray();
        JsonArray cut = new JsonArray();
        for (int i = 0; i < a.size() && i < max; i++) {
            cut.add(a.get(i));
        }
        JsonObject o = new JsonObject();
        o.add(name, cut);
        o.addProperty("count", cut.size());
        o.addProperty("truncated", a.size() > max);
        return o;
    }

    private JsonObject boundedText(JsonObject o, String key, JsonElement v) {
        if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString() && v.getAsString().length() > MAX_TEXT) {
            o.addProperty(key, v.getAsString().substring(0, MAX_TEXT));
            o.addProperty("truncated", true);
            o.addProperty("length", v.getAsString().length());
        } else {
            o.add(key, v);
            o.addProperty("truncated", false);
        }
        return o;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        Integer dbArg = optInt(a, "db");
        int db = dbArg == null ? 0 : dbArg;
        if (db < 0 || db >= redis().databases()) {
            throw new IllegalArgumentException("db must be between 0 and " + (redis().databases() - 1));
        }
        String k = a.has("key") ? optString(a, "key") : null;
        switch (tool) {
            case "redis_get": {
                JsonElement v = cmd(db, "GET", requireString(a, "key"));
                JsonObject o = new JsonObject();
                o.addProperty("key", k);
                o.addProperty("exists", !v.isJsonNull());
                return json(boundedText(o, "value", v));
            }
            case "redis_set": {
                List<String> c = words("SET", requireString(a, "key"), requireString(a, "value"));
                if (optLong(a, "ex") != null) {
                    c.addAll(List.of("EX", String.valueOf(optLong(a, "ex"))));
                }
                if (optLong(a, "px") != null) {
                    c.addAll(List.of("PX", String.valueOf(optLong(a, "px"))));
                }
                if (optBool(a, "nx", false)) {
                    c.add("NX");
                }
                if (optBool(a, "xx", false)) {
                    c.add("XX");
                }
                JsonElement r = cmd(db, c);
                return json(one("ok", new com.google.gson.JsonPrimitive(!r.isJsonNull())));
            }
            case "redis_delete": {
                List<String> c = words("DEL");
                c.addAll(strs(a, "keys"));
                return json(one("deleted", cmd(db, c)));
            }
            case "redis_type":
                return json(one("type", cmd(db, "TYPE", requireString(a, "key"))));
            case "redis_expire":
                return json(one("applied", new com.google.gson.JsonPrimitive(cmd(db, "EXPIRE", requireString(a, "key"),
                        String.valueOf(a.get("seconds").getAsLong())).getAsLong() == 1)));
            case "redis_ttl":
                return json(one("ttl", cmd(db, "TTL", requireString(a, "key"))));
            case "redis_rename":
                cmd(db, "RENAME", requireString(a, "key"), requireString(a, "newKey"));
                return json(one("ok", new com.google.gson.JsonPrimitive(true)));
            case "redis_incr": {
                Long by = optLong(a, "by");
                return json(one("value", cmd(db, "INCRBY", requireString(a, "key"), String.valueOf(by == null ? 1 : by))));
            }
            case "redis_scan_keys": {
                JsonElement r = cmd(db, "SCAN", optString(a, "cursor") == null ? "0" : optString(a, "cursor"), "MATCH",
                        optString(a, "pattern") == null ? "*" : optString(a, "pattern"), "COUNT",
                        String.valueOf(limit(a, "count", 100, MAX_ITEMS)));
                JsonObject o = new JsonObject();
                o.add("cursor", r.getAsJsonArray().get(0));
                o.add("keys", r.getAsJsonArray().get(1));
                return json(o);
            }
            case "redis_scan_all_keys": {
                int max = limit(a, "limit", MAX_ITEMS, MAX_ITEMS);
                String pattern = optString(a, "pattern") == null ? "*" : optString(a, "pattern");
                JsonArray keys = new JsonArray();
                String cursor = "0";
                boolean more = false;
                do {
                    JsonElement r = cmd(db, "SCAN", cursor, "MATCH", pattern, "COUNT", "500");
                    cursor = r.getAsJsonArray().get(0).getAsString();
                    for (JsonElement e : r.getAsJsonArray().get(1).getAsJsonArray()) {
                        if (keys.size() < max) {
                            keys.add(e);
                        } else {
                            more = true;
                        }
                    }
                } while (!cursor.equals("0") && !more);
                JsonObject o = new JsonObject();
                o.add("keys", keys);
                o.addProperty("count", keys.size());
                o.addProperty("truncated", more);
                return json(o);
            }
            case "redis_hset": {
                List<String> c = words("HSET", requireString(a, "key"));
                if (a.has("mapping") && a.get("mapping").isJsonObject()) {
                    for (Map.Entry<String, JsonElement> e : a.getAsJsonObject("mapping").entrySet()) {
                        c.add(e.getKey());
                        c.add(e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : e.getValue().toString());
                    }
                }
                if (optString(a, "field") != null) {
                    c.add(optString(a, "field"));
                    c.add(requireString(a, "value"));
                }
                if (c.size() < 4) {
                    throw new IllegalArgumentException("give field and value, or a mapping");
                }
                return json(one("added", cmd(db, c)));
            }
            case "redis_hget": {
                JsonElement v = cmd(db, "HGET", requireString(a, "key"), requireString(a, "field"));
                JsonObject o = new JsonObject();
                o.addProperty("exists", !v.isJsonNull());
                return json(boundedText(o, "value", v));
            }
            case "redis_hgetall": {
                JsonObject fields = new JsonObject();
                String cursor = "0";
                boolean more = false;
                do {
                    JsonElement r = cmd(db, "HSCAN", requireString(a, "key"), cursor, "COUNT", "500");
                    cursor = r.getAsJsonArray().get(0).getAsString();
                    JsonArray flat = r.getAsJsonArray().get(1).getAsJsonArray();
                    for (int i = 0; i + 1 < flat.size(); i += 2) {
                        if (fields.size() < MAX_ITEMS) {
                            fields.add(flat.get(i).getAsString(), flat.get(i + 1));
                        } else {
                            more = true;
                        }
                    }
                } while (!cursor.equals("0") && !more);
                JsonObject o = new JsonObject();
                o.add("fields", fields);
                o.addProperty("count", fields.size());
                o.addProperty("truncated", more);
                return json(o);
            }
            case "redis_hdel": {
                List<String> c = words("HDEL", requireString(a, "key"));
                c.addAll(strs(a, "fields"));
                return json(one("deleted", cmd(db, c)));
            }
            case "redis_hexists":
                return json(one("exists", new com.google.gson.JsonPrimitive(
                        cmd(db, "HEXISTS", requireString(a, "key"), requireString(a, "field")).getAsLong() == 1)));
            case "redis_lpush", "redis_rpush": {
                List<String> c = words(tool.equals("redis_lpush") ? "LPUSH" : "RPUSH", requireString(a, "key"));
                c.addAll(strs(a, "values"));
                return json(one("length", cmd(db, c)));
            }
            case "redis_lpop", "redis_rpop": {
                String op = tool.equals("redis_lpop") ? "LPOP" : "RPOP";
                Integer count = optInt(a, "count");
                JsonElement r = count == null ? cmd(db, op, requireString(a, "key"))
                        : cmd(db, op, requireString(a, "key"), String.valueOf(Math.min(count, MAX_ITEMS)));
                return json(one(count == null ? "value" : "values", r));
            }
            case "redis_lrange": {
                int start = optInt(a, "start") == null ? 0 : optInt(a, "start");
                int stop = optInt(a, "stop") == null ? start + 99 : optInt(a, "stop");
                boolean clamped = start >= 0 && stop >= 0 && stop - start + 1 > MAX_ITEMS;
                if (clamped) {
                    stop = start + MAX_ITEMS - 1;
                }
                JsonObject out = bounded("values", cmd(db, "LRANGE", requireString(a, "key"), String.valueOf(start), String.valueOf(stop)), MAX_ITEMS);
                if (clamped) {
                    out.addProperty("truncated", true);   // the asked range was longer than the 1000-item cap (use start to page on)
                }
                return json(out);
            }
            case "redis_llen":
                return json(one("length", cmd(db, "LLEN", requireString(a, "key"))));
            case "redis_sadd", "redis_srem": {
                List<String> c = words(tool.equals("redis_sadd") ? "SADD" : "SREM", requireString(a, "key"));
                c.addAll(strs(a, "members"));
                return json(one(tool.equals("redis_sadd") ? "added" : "removed", cmd(db, c)));
            }
            case "redis_smembers": {
                JsonArray members = new JsonArray();
                String cursor = "0";
                boolean more = false;
                do {
                    JsonElement r = cmd(db, "SSCAN", requireString(a, "key"), cursor, "COUNT", "500");
                    cursor = r.getAsJsonArray().get(0).getAsString();
                    for (JsonElement e : r.getAsJsonArray().get(1).getAsJsonArray()) {
                        if (members.size() < MAX_ITEMS) {
                            members.add(e);
                        } else {
                            more = true;
                        }
                    }
                } while (!cursor.equals("0") && !more);
                JsonObject o = new JsonObject();
                o.add("members", members);
                o.addProperty("count", members.size());
                o.addProperty("truncated", more);
                return json(o);
            }
            case "redis_zadd": {
                List<String> c = words("ZADD", requireString(a, "key"));
                JsonObject m = a.has("members") && a.get("members").isJsonObject() ? a.getAsJsonObject("members") : null;
                if (m == null || m.size() == 0) {
                    throw new IllegalArgumentException("members must be an object of member -> score");
                }
                for (Map.Entry<String, JsonElement> e : m.entrySet()) {
                    c.add(e.getValue().getAsString());
                    c.add(e.getKey());
                }
                return json(one("added", cmd(db, c)));
            }
            case "redis_zrange": {
                int start = optInt(a, "start") == null ? 0 : optInt(a, "start");
                int stop = optInt(a, "stop") == null ? start + 99 : optInt(a, "stop");
                boolean clamped = start >= 0 && stop >= 0 && stop - start + 1 > MAX_ITEMS;
                if (clamped) {
                    stop = start + MAX_ITEMS - 1;
                }
                boolean scores = optBool(a, "withScores", false);
                List<String> c = words(optBool(a, "reverse", false) ? "ZREVRANGE" : "ZRANGE", requireString(a, "key"),
                        String.valueOf(start), String.valueOf(stop));
                if (scores) {
                    c.add("WITHSCORES");
                }
                JsonArray flat = cmd(db, c).getAsJsonArray();
                if (!scores) {
                    JsonObject out = bounded("members", flat, MAX_ITEMS);
                    if (clamped) {
                        out.addProperty("truncated", true);
                    }
                    return json(out);
                }
                JsonArray pairs = new JsonArray();
                for (int i = 0; i + 1 < flat.size(); i += 2) {
                    JsonObject p = new JsonObject();
                    p.add("member", flat.get(i));
                    p.addProperty("score", Double.parseDouble(flat.get(i + 1).getAsString()));
                    pairs.add(p);
                }
                JsonObject out = bounded("members", pairs, MAX_ITEMS);
                if (clamped) {
                    out.addProperty("truncated", true);
                }
                return json(out);
            }
            case "redis_zrem": {
                List<String> c = words("ZREM", requireString(a, "key"));
                c.addAll(strs(a, "members"));
                return json(one("removed", cmd(db, c)));
            }
            case "redis_xadd": {
                List<String> c = words("XADD", requireString(a, "key"));
                if (optLong(a, "maxlen") != null) {
                    c.addAll(List.of("MAXLEN", "~", String.valueOf(optLong(a, "maxlen"))));
                }
                c.add(optString(a, "id") == null ? "*" : optString(a, "id"));
                JsonObject f = a.has("fields") && a.get("fields").isJsonObject() ? a.getAsJsonObject("fields") : null;
                if (f == null || f.size() == 0) {
                    throw new IllegalArgumentException("fields must be a non-empty object");
                }
                for (Map.Entry<String, JsonElement> e : f.entrySet()) {
                    c.add(e.getKey());
                    c.add(e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : e.getValue().toString());
                }
                return json(one("id", cmd(db, c)));
            }
            case "redis_xrange": {
                int count = limit(a, "count", 100, MAX_ITEMS);
                JsonArray raw = cmd(db, "XRANGE", requireString(a, "key"), optString(a, "start") == null ? "-" : optString(a, "start"),
                        optString(a, "end") == null ? "+" : optString(a, "end"), "COUNT", String.valueOf(count)).getAsJsonArray();
                JsonArray entries = new JsonArray();
                for (JsonElement e : raw) {
                    JsonObject entry = new JsonObject();
                    entry.add("id", e.getAsJsonArray().get(0));
                    JsonObject fields = new JsonObject();
                    JsonArray flat = e.getAsJsonArray().get(1).getAsJsonArray();
                    for (int i = 0; i + 1 < flat.size(); i += 2) {
                        fields.add(flat.get(i).getAsString(), flat.get(i + 1));
                    }
                    entry.add("fields", fields);
                    entries.add(entry);
                }
                return json(bounded("entries", entries, MAX_ITEMS));
            }
            case "redis_xdel": {
                List<String> c = words("XDEL", requireString(a, "key"));
                c.addAll(strs(a, "ids"));
                return json(one("deleted", cmd(db, c)));
            }
            case "redis_publish":
                return json(one("receivers", cmd(db, "PUBLISH", requireString(a, "channel"), requireString(a, "message"))));
            case "redis_dbsize":
                return json(one("keys", cmd(db, "DBSIZE")));
            case "redis_info": {
                JsonElement r = optString(a, "section") == null ? cmd(db, "INFO") : cmd(db, "INFO", optString(a, "section"));
                return json(boundedText(new JsonObject(), "info", r));
            }
            default:
                return Outcome.error("unknown redis tool: " + tool);
        }
    }
}
