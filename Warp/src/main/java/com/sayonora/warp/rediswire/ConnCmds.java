package com.sayonora.warp.rediswire;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/** Connection-level commands: HELLO, AUTH, PING, ECHO, SELECT, QUIT, RESET, CLIENT, MULTI/EXEC/DISCARD, WATCH. */
final class ConnCmds {

    static final String VERSION = "7.2.5";

    private ConnCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("hello", -1, "noscript loading stale fast no_auth sentinel", 0, 0, 0, "@fast @connection", ConnCmds::hello).noAuth();
        r.add("auth", -2, "noscript loading stale fast no_auth sentinel allow_busy", 0, 0, 0, "@fast @connection", ConnCmds::auth).noAuth();
        r.add("ping", -1, "fast sentinel", 0, 0, 0, "@fast @connection", ConnCmds::ping);
        r.add("echo", 2, "fast", 0, 0, 0, "@fast @connection", (x, a) -> a[1]);
        r.add("quit", -1, "allow_busy no_auth", 0, 0, 0, "@fast @connection", (x, a) -> {
            x.session.closeAfterReply = true;
            return Resp.OK;
        }).noAuth();
        r.add("reset", 1, "noscript loading stale fast no_auth allow_busy", 0, 0, 0, "@fast @connection", ConnCmds::reset).noAuth();
        r.add("select", 2, "loading stale fast", 0, 0, 0, "@fast @connection", ConnCmds::select);
        r.add("client", -2, "admin noscript loading stale sentinel", 0, 0, 0, "@slow @connection", ConnCmds::client);
        r.add("multi", 1, "noscript loading stale fast allow_busy", 0, 0, 0, "@fast @transaction", (x, a) -> {
            if (x.session.inMulti) {
                throw RedisError.err("MULTI calls can not be nested");
            }
            x.session.inMulti = true;
            x.session.multiDirty = false;
            return Resp.OK;
        });
        r.add("exec", 1, "noscript loading stale skip_slowlog", 0, 0, 0, "@slow @transaction", (x, a) -> {
            throw RedisError.err("EXEC without MULTI");
        });
        r.add("discard", 1, "noscript loading stale fast allow_busy", 0, 0, 0, "@fast @transaction", (x, a) -> {
            Session s = x.session;
            if (!s.inMulti) {
                throw RedisError.err("DISCARD without MULTI");
            }
            s.inMulti = false;
            s.multiDirty = false;
            s.queue.clear();
            s.unwatchAll();
            return Resp.OK;
        });
        r.add("watch", -2, "noscript loading stale fast allow_busy", 1, -1, 1, "@fast @transaction", ConnCmds::watch);
        r.add("unwatch", 1, "noscript loading stale fast allow_busy", 0, 0, 0, "@fast @transaction", (x, a) -> {
            x.session.unwatchAll();
            return Resp.OK;
        });
        r.add("readonly", 1, "loading stale fast", 0, 0, 0, "@fast @connection", (x, a) -> Resp.OK);
        r.add("readwrite", 1, "loading stale fast", 0, 0, 0, "@fast @connection", (x, a) -> Resp.OK);
    }

    // ------------------------------------------------------------------------------------------

    private static Object ping(Ctx x, byte[][] a) {
        if (a.length > 2) {
            throw RedisError.arity("ping");
        }
        Session s = x.session;
        if (!s.resp3 && s.subscribedMode()) {
            List<Object> l = new ArrayList<>();
            l.add("pong");
            l.add(a.length == 2 ? a[1] : new byte[0]);
            return l;
        }
        return a.length == 2 ? a[1] : Resp.PONG;
    }

    static boolean checkPassword(RedisStore st, byte[] given) {
        String pw = st.password();
        if (pw == null) {
            return true;
        }
        return MessageDigest.isEqual(pw.getBytes(StandardCharsets.UTF_8), given);
    }

    /** Authenticates {@code user}/{@code pass}; throws the Redis error otherwise. */
    private static void authenticate(Ctx x, byte[] user, byte[] pass, boolean twoArg) {
        RedisStore st = x.store;
        Session s = x.session;
        String u = user == null ? "default" : new String(user, StandardCharsets.UTF_8);
        if (st.password() == null) {
            if (!twoArg) {
                throw RedisError.err("AUTH <password> called without any password configured for the default user. "
                        + "Are you sure your configuration is correct?");
            }
            // no password configured: the username is only a routing hint (connect-time routing), accept it
        } else if (!checkPassword(st, pass)) {
            throw new RedisError("WRONGPASS invalid username-password pair or user is disabled.");
        }
        String previous = s.user;
        s.user = u;
        try {
            s.reroute();
        } catch (RedisError e) {
            s.user = previous;
            throw e;
        }
        s.authed = true;
    }

    private static Object auth(Ctx x, byte[][] a) {
        if (a.length > 3) {
            throw RedisError.syntax();
        }
        if (a.length == 2) {
            authenticate(x, null, a[1], false);
        } else {
            authenticate(x, a[1], a[2], true);
        }
        return Resp.OK;
    }

    private static Object hello(Ctx x, byte[][] a) {
        Session s = x.session;
        int proto = s.resp3 ? 3 : 2;
        int i = 1;
        if (a.length > 1) {
            Long v = Num.parseLong(a[1]);
            if (v == null) {
                throw RedisError.err("Protocol version is not an integer or out of range");
            }
            if (v < 2 || v > 3) {
                throw new RedisError("NOPROTO unsupported protocol version");
            }
            proto = v.intValue();
            i = 2;
        }
        byte[] authUser = null;
        byte[] authPass = null;
        byte[] setName = null;
        while (i < a.length) {
            int left = a.length - i - 1;
            if (Num.eq(a[i], "AUTH") && left >= 2) {
                authUser = a[i + 1];
                authPass = a[i + 2];
                i += 3;
            } else if (Num.eq(a[i], "SETNAME") && left >= 1) {
                setName = a[i + 1];
                i += 2;
            } else {
                throw new RedisError("ERR Syntax error in HELLO option '" + Num.str(a[i]) + "'");
            }
        }
        if (setName != null) {
            validateName(setName);
        }
        if (authUser != null) {
            authenticate(x, authUser, authPass, true);
        } else if (!s.authed) {
            throw new RedisError("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
        }
        if (setName != null) {
            s.name = Num.str(setName);
        }
        s.resp3 = proto == 3;
        List<Object> f = new ArrayList<>();
        f.add("server");
        f.add("redis");
        f.add("version");
        f.add(VERSION);
        f.add("proto");
        f.add((long) proto);
        f.add("id");
        f.add(s.id);
        f.add("mode");
        f.add("standalone");
        f.add("role");
        f.add("master");
        f.add("modules");
        f.add(new ArrayList<>());
        return new Resp.RMap(f);
    }

    private static void validateName(byte[] n) {
        for (byte b : n) {
            if (b < '!' || b > '~') {
                throw RedisError.err("Client names cannot contain spaces, newlines or special characters.");
            }
        }
    }

    private static Object reset(Ctx x, byte[][] a) {
        Session s = x.session;
        s.inMulti = false;
        s.multiDirty = false;
        s.queue.clear();
        s.unwatchAll();
        s.unsubscribeAll();
        s.resp3 = false;
        s.name = "";
        s.db = 0;
        s.replyMode = "on";
        s.user = "default";
        s.authed = x.store.password() == null;
        s.reroute();
        return new Resp.Simple("RESET");
    }

    private static Object select(Ctx x, byte[][] a) {
        Long v = Num.parseLong(a[1]);
        if (v == null) {
            throw RedisError.notInt();
        }
        if (v < 0 || v >= x.store.options().databases()) {
            throw RedisError.err("DB index is out of range");
        }
        Session s = x.session;
        int old = s.db;
        s.db = v.intValue();
        try {
            s.reroute();
        } catch (RedisError e) {
            s.db = old;
            throw e;
        }
        x.db = s.db;
        return Resp.OK;
    }

    private static Object watch(Ctx x, byte[][] a) throws Exception {
        Session s = x.session;
        if (s.inMulti) {
            throw RedisError.err("WATCH inside MULTI is not allowed");
        }
        for (int i = 1; i < a.length; i++) {
            Ctx.Meta m = x.get(a[i]);
            s.watches.add(new Session.Watch(x.db, a[i], m == null ? -1 : m.ver(), x.shardOfKey(a[i])));
        }
        return Resp.OK;
    }

    // ------------------------------------------------------------------------------------------
    // CLIENT
    // ------------------------------------------------------------------------------------------

    static String clientLine(Session c) {
        long now = System.currentTimeMillis();
        String laddr = c.socket.getLocalSocketAddress() == null ? "" : c.socket.getLocalSocketAddress().toString().replace("/", "");
        return "id=" + c.id + " addr=" + c.socketAddr() + " laddr=" + laddr + " fd=" + (c.id + 7) + " name=" + c.name
                + " age=" + (now - c.createdAt) / 1000 + " idle=" + (now - c.lastCmdAt) / 1000
                + " flags=" + (c.subscribedMode() ? "P" : c.blocked ? "b" : c.inMulti ? "x" : "N") + " db=" + c.db
                + " sub=" + c.channels.size() + " psub=" + c.patterns.size() + " ssub=" + c.shardChannels.size()
                + " multi=" + (c.inMulti ? c.queue.size() : -1) + " watch=" + c.watches.size()
                + " qbuf=0 qbuf-free=20474 argv-mem=0 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=22298 events=r"
                + " cmd=" + c.lastCmd + " user=" + c.user + " redir=-1 resp=" + (c.resp3 ? 3 : 2)
                + " lib-name=" + c.libName + " lib-ver=" + c.libVer;
    }

    private static Object client(Ctx x, byte[][] a) {
        Session s = x.session;
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "ID":
                return s.id;
            case "SETNAME":
                if (a.length != 3) {
                    throw RedisError.arity("client|setname");
                }
                validateName(a[2]);
                s.name = Num.str(a[2]);
                return Resp.OK;
            case "GETNAME":
                return s.name.isEmpty() ? null : s.name;
            case "SETINFO": {
                if (a.length != 4) {
                    throw RedisError.arity("client|setinfo");
                }
                String attr = Num.upper(a[2]);
                validateName(a[3]);
                if (attr.equals("LIB-NAME")) {
                    s.libName = Num.str(a[3]);
                } else if (attr.equals("LIB-VER")) {
                    s.libVer = Num.str(a[3]);
                } else {
                    throw RedisError.err("Unrecognized option '" + Num.str(a[2]) + "'");
                }
                return Resp.OK;
            }
            case "INFO":
                return new Resp.Verbatim(clientLine(s) + "\n");
            case "LIST": {
                StringBuilder sb = new StringBuilder();
                List<Long> want = null;
                int i = 2;
                String type = null;
                while (i < a.length) {
                    if (Num.eq(a[i], "TYPE") && i + 1 < a.length) {
                        type = Num.upper(a[i + 1]);
                        if (!List.of("NORMAL", "MASTER", "REPLICA", "SLAVE", "PUBSUB").contains(type)) {
                            throw RedisError.err("Unknown client type '" + Num.str(a[i + 1]) + "'");
                        }
                        i += 2;
                    } else if (Num.eq(a[i], "ID") && i + 1 < a.length) {
                        want = new ArrayList<>();
                        for (i = i + 1; i < a.length; i++) {
                            Long id = Num.parseLong(a[i]);
                            if (id == null) {
                                throw RedisError.err("Invalid client ID");
                            }
                            want.add(id);
                        }
                    } else {
                        throw RedisError.syntax();
                    }
                }
                List<Session> all = new ArrayList<>(x.store.clients.values());
                all.sort((p, q) -> Long.compare(p.id, q.id));
                for (Session c : all) {
                    if (want != null && !want.contains(c.id)) {
                        continue;
                    }
                    if (type != null) {
                        boolean pub = c.subscribedMode();
                        if ((type.equals("PUBSUB") && !pub) || (type.equals("NORMAL") && pub)
                                || type.equals("MASTER") || type.equals("REPLICA") || type.equals("SLAVE")) {
                            continue;
                        }
                    }
                    sb.append(clientLine(c)).append('\n');
                }
                return new Resp.Verbatim(sb.toString());
            }
            case "KILL":
                return clientKill(x, a);
            case "NO-EVICT":
            case "NO-TOUCH":
                if (a.length != 3 || !(Num.eq(a[2], "ON") || Num.eq(a[2], "OFF"))) {
                    throw RedisError.syntax();
                }
                return Resp.OK;
            case "REPLY": {
                if (a.length != 3) {
                    throw RedisError.arity("client|reply");
                }
                if (Num.eq(a[2], "ON")) {
                    s.replyMode = "on";
                    return Resp.OK;
                }
                if (Num.eq(a[2], "OFF")) {
                    s.replyMode = "off";
                    return Session.NO_REPLY;
                }
                if (Num.eq(a[2], "SKIP")) {
                    s.replyMode = "skip";
                    return Session.NO_REPLY;
                }
                throw RedisError.syntax();
            }
            case "PAUSE":
                if (a.length < 3 || Num.parseLong(a[2]) == null) {
                    throw RedisError.err("timeout is not an integer or out of range");
                }
                return Resp.OK;
            case "UNPAUSE":
                return Resp.OK;
            case "CACHING":
                throw RedisError.err("CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled");
            case "GETREDIR":
                return -1L;
            case "TRACKING":
                throw RedisError.err("client tracking is not supported by this server");
            case "TRACKINGINFO": {
                List<Object> f = new ArrayList<>();
                f.add("flags");
                f.add(new Resp.RSet(List.of("off")));
                f.add("redirect");
                f.add(-1L);
                f.add("prefixes");
                f.add(new ArrayList<>());
                return new Resp.RMap(f);
            }
            case "UNBLOCK": {
                if (a.length < 3 || a.length > 4) {
                    throw RedisError.arity("client|unblock");
                }
                Long id = Num.parseLong(a[2]);
                if (id == null) {
                    throw RedisError.err("value is not an integer or out of range");
                }
                String mode = a.length == 4 ? Num.upper(a[3]) : "TIMEOUT";
                if (!mode.equals("TIMEOUT") && !mode.equals("ERROR")) {
                    throw RedisError.err("CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                }
                Session c = x.store.clients.get(id);
                if (c == null || !c.blocked) {
                    return 0L;
                }
                c.unblockMode = mode.toLowerCase();
                Hub.Waiter w = c.blockWaiter;
                if (w != null) {
                    w.signal();
                }
                return 1L;
            }
            case "HELP":
                return Resp.help("CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "ID", "SETNAME <name>", "GETNAME",
                        "INFO", "LIST [TYPE ...]", "KILL ...", "SETINFO ...", "UNBLOCK <id>", "HELP");
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try CLIENT HELP.");
        }
    }

    private static Object clientKill(Ctx x, byte[][] a) {
        Session me = x.session;
        if (a.length == 3) {
            String addr = Num.str(a[2]);
            for (Session c : x.store.clients.values()) {
                if (c.socketAddr().equals(addr)) {
                    if (c == me) {
                        me.closeAfterReply = true;
                    } else {
                        c.kill();
                    }
                    return Resp.OK;
                }
            }
            throw RedisError.err("No such client");
        }
        Long id = null;
        String addr = null;
        String laddr = null;
        String user = null;
        String type = null;
        boolean skipMe = true;
        if ((a.length - 2) % 2 != 0) {
            throw RedisError.syntax();
        }
        for (int i = 2; i + 1 < a.length; i += 2) {
            String k = Num.upper(a[i]);
            String v = Num.str(a[i + 1]);
            switch (k) {
                case "ID" -> {
                    id = Num.parseLong(a[i + 1]);
                    if (id == null || id <= 0) {
                        throw RedisError.err("client-id should be greater than 0");
                    }
                }
                case "ADDR" -> addr = v;
                case "LADDR" -> laddr = v;
                case "USER" -> user = v;
                case "TYPE" -> type = v.toUpperCase();
                case "SKIPME" -> {
                    if (v.equalsIgnoreCase("yes")) {
                        skipMe = true;
                    } else if (v.equalsIgnoreCase("no")) {
                        skipMe = false;
                    } else {
                        throw RedisError.syntax();
                    }
                }
                default -> throw RedisError.syntax();
            }
        }
        long killed = 0;
        for (Session c : new ArrayList<>(x.store.clients.values())) {
            if (id != null && c.id != id) {
                continue;
            }
            if (addr != null && !c.socketAddr().equals(addr)) {
                continue;
            }
            if (laddr != null && !String.valueOf(c.socket.getLocalSocketAddress()).replace("/", "").equals(laddr)) {
                continue;
            }
            if (user != null && !c.user.equals(user)) {
                continue;
            }
            if (type != null && ((type.equals("PUBSUB")) != c.subscribedMode())) {
                continue;
            }
            if (c == me) {
                if (skipMe) {
                    continue;
                }
                me.closeAfterReply = true;
            } else {
                c.kill();
            }
            killed++;
        }
        return killed;
    }
}
