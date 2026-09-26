package com.sayonora.warp.rediswire;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** One registered command: its Redis metadata (as reported by COMMAND INFO) and its handler. */
final class Cmd {

    interface Handler {
        Object run(Ctx x, byte[][] a) throws Exception;
    }

    /** Positions (indexes into the argument array) of the keys of one invocation. */
    interface KeyFn {
        int[] keys(byte[][] a);
    }

    /** For blocking commands: timeout in ms (0 = forever), or -1 when this invocation does not block. */
    interface BlockFn {
        long timeoutMs(byte[][] a);
    }

    /** Returned by a blocking command's attempt when nothing is ready yet. */
    static final Object NOT_READY = new Object();

    final String name;
    final int arity;
    final String flags;
    final int first;
    final int last;
    final int step;
    final String categories;
    final Handler handler;
    KeyFn keyFn;
    BlockFn blockFn;
    Object timeoutReply;
    boolean timeoutReplySet;
    boolean noAuth;
    boolean movable;

    Cmd(String name, int arity, String flags, int first, int last, int step, String categories, Handler handler) {
        this.name = name;
        this.arity = arity;
        this.flags = flags;
        this.first = first;
        this.last = last;
        this.step = step;
        this.categories = categories;
        this.handler = handler;
    }

    boolean isWrite() {
        return flags.contains("write");
    }

    Cmd keys(KeyFn f) {
        this.keyFn = f;
        this.movable = true;
        return this;
    }

    Cmd blocking(BlockFn f, Object timeoutReply) {
        this.blockFn = f;
        this.timeoutReply = timeoutReply;
        this.timeoutReplySet = true;
        return this;
    }

    Cmd noAuth() {
        this.noAuth = true;
        return this;
    }

    /** Key positions per the command table (first/last/step), or the custom extractor. */
    int[] keyPositions(byte[][] a) {
        if (keyFn != null) {
            return keyFn.keys(a);
        }
        if (first == 0) {
            return new int[0];
        }
        int lastIdx = last >= 0 ? last : a.length + last;
        List<Integer> out = new ArrayList<>();
        for (int i = first; i <= lastIdx && i < a.length; i += step) {
            out.add(i);
        }
        int[] r = new int[out.size()];
        for (int i = 0; i < r.length; i++) {
            r[i] = out.get(i);
        }
        return r;
    }

    // ------------------------------------------------------------------------------------------

    /** The command table. */
    static final class Registry {
        private final Map<String, Cmd> byName = new TreeMap<>();

        Cmd add(String name, int arity, String flags, int first, int last, int step, String cats, Handler h) {
            Cmd c = new Cmd(name.toLowerCase(Locale.ROOT), arity, flags, first, last, step, cats, h);
            byName.put(c.name, c);
            return c;
        }

        Cmd get(String lowerName) {
            return byName.get(lowerName);
        }

        java.util.Collection<Cmd> all() {
            return byName.values();
        }

        int size() {
            return byName.size();
        }
    }

    static Registry buildRegistry() {
        Registry r = new Registry();
        ConnCmds.register(r);
        ServerCmds.register(r);
        KeyCmds.register(r);
        StringCmds.register(r);
        HashCmds.register(r);
        ListCmds.register(r);
        SetCmds.register(r);
        ZSetCmds.register(r);
        StreamCmds.register(r);
        BitCmds.register(r);
        HllCmds.register(r);
        GeoCmds.register(r);
        PubSubCmds.register(r);
        return r;
    }
}
