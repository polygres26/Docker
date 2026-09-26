package com.sayonora.wire.rediswire;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Pub/sub. Messages travel through Postgres NOTIFY (payloads over ~6 KB through a side table), one dedicated LISTEN
 * connection per Warp process and host delivers them to the local subscribers of every node. A subscribed connection
 * holds no pooled connection: it just receives pushes.
 */
final class PubSubCmds {

    private PubSubCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("subscribe", -2, "pubsub noscript loading stale", 0, 0, 0, "@pubsub @slow", (x, a) -> sub(x, a, 0));
        r.add("unsubscribe", -1, "pubsub noscript loading stale", 0, 0, 0, "@pubsub @slow", (x, a) -> unsub(x, a, 0));
        r.add("psubscribe", -2, "pubsub noscript loading stale", 0, 0, 0, "@pubsub @slow", (x, a) -> sub(x, a, 1));
        r.add("punsubscribe", -1, "pubsub noscript loading stale", 0, 0, 0, "@pubsub @slow", (x, a) -> unsub(x, a, 1));
        r.add("ssubscribe", -2, "pubsub noscript loading stale", 1, -1, 1, "@pubsub @slow", (x, a) -> sub(x, a, 2));
        r.add("sunsubscribe", -1, "pubsub noscript loading stale", 1, -1, 1, "@pubsub @slow", (x, a) -> unsub(x, a, 2));
        r.add("publish", 3, "pubsub loading stale fast may_replicate", 0, 0, 0, "@pubsub @fast", (x, a) -> publish(x, a, false));
        r.add("spublish", 3, "pubsub loading stale fast may_replicate", 1, 1, 1, "@pubsub @fast", (x, a) -> publish(x, a, true));
        r.add("pubsub", -2, "pubsub loading stale", 0, 0, 0, "@pubsub @slow", PubSubCmds::pubsub);
    }

    private static Map<Hub.BK, String> reg(Session s, int kind) {
        return kind == 0 ? s.channels : kind == 1 ? s.patterns : s.shardChannels;
    }

    private static int count(Session s, int kind) {
        return kind == 2 ? s.shardChannels.size() : s.channels.size() + s.patterns.size();
    }

    private static final String[] SUB = {"subscribe", "psubscribe", "ssubscribe"};
    private static final String[] UNSUB = {"unsubscribe", "punsubscribe", "sunsubscribe"};

    private static Object sub(Ctx x, byte[][] a, int kind) {
        Session s = x.session;
        List<Object> replies = new ArrayList<>();
        for (int i = 1; i < a.length; i++) {
            Hub.BK k = new Hub.BK(a[i]);
            String host = kind == 2 ? s.shardHost(a[i]) : s.home();
            if (!reg(s, kind).containsKey(k)) {
                x.store.hub().subscribe(host, a[i], s, kind);
                reg(s, kind).put(k, host);
            }
            replies.add(new Resp.Push(new ArrayList<Object>(List.of(SUB[kind], a[i], (long) count(s, kind)))));
        }
        return new Resp.Multi(replies);
    }

    private static Object unsub(Ctx x, byte[][] a, int kind) {
        Session s = x.session;
        List<byte[]> names = new ArrayList<>();
        if (a.length == 1) {
            for (Hub.BK k : reg(s, kind).keySet()) {
                names.add(k.b);
            }
        } else {
            names.addAll(Arrays.asList(a).subList(1, a.length));
        }
        List<Object> replies = new ArrayList<>();
        if (names.isEmpty()) {
            replies.add(new Resp.Push(new ArrayList<Object>(Arrays.asList(UNSUB[kind], null, (long) count(s, kind)))));
        }
        for (byte[] n : names) {
            String host = reg(s, kind).remove(new Hub.BK(n));
            if (host != null) {
                x.store.hub().unsubscribe(host, n, s, kind);
            }
            replies.add(new Resp.Push(new ArrayList<Object>(List.of(UNSUB[kind], n, (long) count(s, kind)))));
        }
        return new Resp.Multi(replies);
    }

    private static Object publish(Ctx x, byte[][] a, boolean sharded) throws Exception {
        String host = sharded ? x.session.shardHost(a[1]) : x.session.home();
        int shard = x.hosts().indexOf(host);
        x.useShard(Math.max(0, shard));
        String inline = Hub.encodeInline(sharded, a[1], a[2]);
        if (inline != null) {
            try (PreparedStatement ps = x.c().prepareStatement("SELECT pg_notify('" + Hub.CH_PUBSUB + "', ?)")) {
                ps.setString(1, inline);
                ps.execute();
            }
        } else {
            x.atomic(() -> {
                Long id = x.one("INSERT INTO warp_redis_pubsub (ch, msg) VALUES (?, ?) RETURNING id", rs -> rs.getLong(1), a[1], a[2]);
                try (PreparedStatement ps = x.c().prepareStatement("SELECT pg_notify('" + Hub.CH_PUBSUB + "', ?)")) {
                    ps.setString(1, (sharded ? "S" : "P") + id);
                    ps.execute();
                }
                return null;
            });
        }
        return (long) x.store.hub().localReceivers(host, a[1], sharded);
    }

    private static Object pubsub(Ctx x, byte[][] a) {
        String sub = Num.upper(a[1]);
        if (((sub.equals("CHANNELS") || sub.equals("SHARDCHANNELS")) && a.length > 3) || (sub.equals("NUMPAT") && a.length != 2)) {
            throw new RedisError("ERR unknown subcommand or wrong number of arguments for '" + Num.str(a[1]) + "'. Try PUBSUB HELP.");
        }
        Hub hub = x.store.hub();
        String host = x.session.home();
        switch (sub) {
            case "CHANNELS":
            case "SHARDCHANNELS": {
                boolean sharded = sub.equals("SHARDCHANNELS");
                Map<String, Map<Hub.BK, java.util.Set<Session>>> m = sharded ? hub.shardChannels : hub.channels;
                List<Object> out = new ArrayList<>();
                for (Map.Entry<String, Map<Hub.BK, java.util.Set<Session>>> he : m.entrySet()) {
                    if (!sharded && !he.getKey().equals(host)) {
                        continue;
                    }
                    for (Hub.BK k : he.getValue().keySet()) {
                        if (a.length < 3 || Glob.matches(a[2], k.b)) {
                            out.add(k.b);
                        }
                    }
                }
                return out;
            }
            case "NUMSUB":
            case "SHARDNUMSUB": {
                boolean sharded = sub.equals("SHARDNUMSUB");
                List<Object> out = new ArrayList<>();
                for (int i = 2; i < a.length; i++) {
                    long n = 0;
                    Map<Hub.BK, java.util.Set<Session>> r = (sharded ? hub.shardChannels : hub.channels).get(sharded ? x.session.shardHost(a[i]) : host);
                    if (r != null && r.get(new Hub.BK(a[i])) != null) {
                        n = r.get(new Hub.BK(a[i])).size();
                    }
                    out.add(a[i]);
                    out.add(n);
                }
                return out;
            }
            case "NUMPAT": {
                Map<Hub.BK, java.util.Set<Session>> p = hub.patterns.get(host);
                return p == null ? 0L : (long) p.size();
            }
            case "HELP":
                return Resp.help("PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "CHANNELS [<pattern>]", "NUMPAT",
                        "NUMSUB [<channel> ...]", "SHARDCHANNELS [<pattern>]", "SHARDNUMSUB [<shardchannel> ...]", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try PUBSUB HELP.");
        }
    }
}
