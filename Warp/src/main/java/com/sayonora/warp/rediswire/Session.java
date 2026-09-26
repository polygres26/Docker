package com.sayonora.warp.rediswire;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One client connection: protocol state, command dispatch, MULTI/WATCH, blocking waits and pub/sub pushes. */
final class Session implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Session.class);

    record Watch(int db, byte[] key, long ver, int shard) {
    }

    final RedisStore store;
    final Cmd.Registry registry;
    final Socket socket;
    final long id;
    final long createdAt = System.currentTimeMillis();
    private final Executor pushExecutor;

    String name = "";
    String libName = "";
    String libVer = "";
    String user = "default";
    boolean authed;
    boolean resp3;
    int db;
    volatile long lastCmdAt = System.currentTimeMillis();
    String lastCmd = "NULL";
    boolean closeAfterReply;
    String replyMode = "on";

    // MULTI / WATCH
    boolean inMulti;
    boolean multiDirty;
    final List<byte[][]> queue = new ArrayList<>();
    final List<Watch> watches = new ArrayList<>();

    // pub/sub
    /** subscription name -> shard host it was registered on (pub/sub is per routed host group). */
    final java.util.Map<Hub.BK, String> channels = new java.util.LinkedHashMap<>();
    final java.util.Map<Hub.BK, String> patterns = new java.util.LinkedHashMap<>();
    final java.util.Map<Hub.BK, String> shardChannels = new java.util.LinkedHashMap<>();

    // blocking
    volatile boolean blocked;
    volatile String unblockMode;
    volatile Hub.Waiter blockWaiter;
    /** per-command scratch that survives blocking retries (e.g. XREAD's resolved "$" ids). */
    java.util.Map<String, Object> callState = new java.util.HashMap<>();

    private volatile List<String> hosts;
    private Resp.Reader reader;
    private Resp.Writer writer;
    private final Object writeLock = new Object();
    private final ConcurrentLinkedQueue<Object> pushQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean pushing = new AtomicBoolean();
    private volatile boolean closed;
    private final Runnable onClose;

    Session(RedisStore store, Cmd.Registry registry, Socket socket, Executor pushExecutor, Runnable onClose) {
        this.store = store;
        this.registry = registry;
        this.socket = socket;
        this.pushExecutor = pushExecutor;
        this.onClose = onClose;
        this.id = store.clientIds.incrementAndGet();
        this.authed = store.password() == null;
        this.hosts = store.backends().defaultHosts();
    }

    List<String> hosts() {
        return hosts;
    }

    /** Recomputes the shard hosts from connect-time routes (username + selected db). */
    void reroute() {
        List<String> r = store.backends().routeHosts(user, db);
        hosts = r != null ? r : store.backends().defaultHosts();
    }

    String home() {
        return hosts.get(0);
    }

    int subscriptionCount() {
        return channels.size() + patterns.size();
    }

    boolean subscribedMode() {
        return !channels.isEmpty() || !patterns.isEmpty() || !shardChannels.isEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // I/O loop
    // ------------------------------------------------------------------------------------------

    @Override
    public void run() {
        store.clients.put(id, this);
        store.totalConnections.incrementAndGet();
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            writer = new Resp.Writer(socket.getOutputStream());
            reader = new Resp.Reader(socket.getInputStream(), this::flushQuietly);
            while (!closed) {
                byte[][] a = reader.readCommand();
                if (a == null) {
                    break;
                }
                long start = System.nanoTime();
                Object reply = handle(a);
                synchronized (writeLock) {
                    if (!"off".equals(replyMode) && !(reply == NO_REPLY)) {
                        writer.write(reply, resp3);
                    }
                    if ("skip".equals(replyMode)) {
                        replyMode = "on";
                    }
                    writer.maybeFlush();
                }
                if (closeAfterReply) {
                    break;
                }
                lastRttNanos = System.nanoTime() - start;
            }
        } catch (Resp.ProtocolError pe) {
            try {
                synchronized (writeLock) {
                    writer.write(new Resp.Err("ERR Protocol error: " + pe.getMessage()), resp3);
                }
            } catch (RuntimeException ignored) {
                // closing anyway
            }
        } catch (IOException e) {
            // client went away
        } catch (RuntimeException e) {
            log.warn("rediswire: session {} failed: {}", id, e.toString(), e);
        } finally {
            close();
        }
    }

    long lastRttNanos;

    static final Object NO_REPLY = new Object();

    private void flushQuietly() {
        try {
            synchronized (writeLock) {
                if (writer != null && writer.buffered() > 0) {
                    writer.flush();
                }
            }
        } catch (IOException e) {
            closed = true;
            try {
                socket.close();
            } catch (IOException ignored) {
                // gone
            }
        }
    }

    private final AtomicBoolean closing = new AtomicBoolean();

    void close() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        closed = true;
        store.clients.remove(id);
        unsubscribeAll();
        try {
            flushQuietly();
            socket.close();
        } catch (IOException ignored) {
            // already closed
        }
        onClose.run();
    }

    /** Terminates the connection from another thread (CLIENT KILL). */
    void kill() {
        closed = true;
        Hub.Waiter w = blockWaiter;
        if (w != null) {
            w.signal();
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // gone
        }
    }

    boolean isClosed() {
        return closed;
    }

    // ------------------------------------------------------------------------------------------
    // pub/sub pushes (called from other threads)
    // ------------------------------------------------------------------------------------------

    void pushMessage(String kind, byte[] pattern, byte[] channel, byte[] message) {
        List<Object> items = new ArrayList<>(4);
        items.add(kind);
        if (pattern != null) {
            items.add(pattern);
        }
        items.add(channel);
        items.add(message);
        pushQueue.add(new Resp.Push(items));
        if (pushing.compareAndSet(false, true)) {
            pushExecutor.execute(this::drainPushes);
        }
    }

    private void drainPushes() {
        try {
            Object o;
            while ((o = pushQueue.poll()) != null && !closed) {
                synchronized (writeLock) {
                    writer.write(o, resp3);
                    writer.flush();
                }
            }
        } catch (IOException | RuntimeException e) {
            kill();
        } finally {
            pushing.set(false);
            if (!pushQueue.isEmpty() && pushing.compareAndSet(false, true)) {
                pushExecutor.execute(this::drainPushes);
            }
        }
    }

    void unsubscribeAll() {
        for (java.util.Map.Entry<Hub.BK, String> e : channels.entrySet()) {
            store.hub().unsubscribe(e.getValue(), e.getKey().b, this, 0);
        }
        for (java.util.Map.Entry<Hub.BK, String> e : patterns.entrySet()) {
            store.hub().unsubscribe(e.getValue(), e.getKey().b, this, 1);
        }
        for (java.util.Map.Entry<Hub.BK, String> e : shardChannels.entrySet()) {
            store.hub().unsubscribe(e.getValue(), e.getKey().b, this, 2);
        }
        channels.clear();
        patterns.clear();
        shardChannels.clear();
    }

    String shardHost(byte[] channel) {
        return hosts.get(Slot.shardOf(Slot.of(channel), hosts.size()));
    }

    // ------------------------------------------------------------------------------------------
    // dispatch
    // ------------------------------------------------------------------------------------------

    private static String lower(byte[] b) {
        return new String(b, StandardCharsets.ISO_8859_1).toLowerCase(Locale.ROOT);
    }

    Object handle(byte[][] a) {
        long start = System.nanoTime();
        store.totalCommands.incrementAndGet();
        lastCmdAt = System.currentTimeMillis();
        String name = lower(a[0]);
        Cmd cmd = registry.get(name);
        String label = cmd == null ? "unknown" : name;
        lastCmd = name;
        boolean write = cmd != null && cmd.isWrite();
        String host = hosts.isEmpty() ? "default" : hosts.get(0);
        try {
            return dispatch(cmd, name, a);
        } finally {
            long nanos = System.nanoTime() - start - blockedNanos;
            blockedNanos = 0;
            if (nanos < 0) {
                nanos = 0;
            }
            store.backends().record(host, write, label, nanos);
            long micros = nanos / 1000;
            int threshold = parseIntConfig("slowlog-log-slower-than", 10000);
            if (threshold >= 0 && micros >= threshold && cmd != null) {
                store.slowLog.add(micros, System.currentTimeMillis() / 1000, a, socketAddr(), this.name,
                        parseIntConfig("slowlog-max-len", 128));
            }
        }
    }

    long blockedNanos;

    private int parseIntConfig(String k, int dflt) {
        try {
            return Integer.parseInt(store.config.getOrDefault(k, String.valueOf(dflt)));
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    String socketAddr() {
        return socket == null || socket.getRemoteSocketAddress() == null ? "?" : socket.getRemoteSocketAddress().toString().replace("/", "");
    }

    private Object dispatch(Cmd cmd, String name, byte[][] a) {
        if (cmd == null) {
            if (inMulti) {
                multiDirty = true;
            }
            return unknownCommand(a);
        }
        if ((cmd.arity > 0 && a.length != cmd.arity) || a.length < -cmd.arity) {
            String msg = RedisError.arity(name).getMessage();
            if (inMulti && name.equals("exec")) {
                // EXEC itself is malformed: Redis discards the whole transaction
                inMulti = false;
                multiDirty = false;
                queue.clear();
                unwatchAll();
                return new Resp.Err("EXECABORT Transaction discarded because of: " + msg.substring(4));
            }
            if (inMulti) {
                multiDirty = true;
            }
            return new Resp.Err(msg);
        }
        if (!authed && !cmd.noAuth) {
            if (inMulti) {
                multiDirty = true;
            }
            return new Resp.Err("NOAUTH Authentication required.");
        }
        if (!resp3 && subscribedMode() && !SUBSCRIBED_OK.contains(name)) {
            return new Resp.Err("ERR Can't execute '" + name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context");
        }
        if (inMulti && !MULTI_PASS.contains(name)) {
            queue.add(a);
            return Resp.QUEUED;
        }
        if (name.equals("exec")) {
            return exec();
        }
        return execute(cmd, a, false);
    }

    private static final Set<String> SUBSCRIBED_OK = Set.of("subscribe", "unsubscribe", "psubscribe", "punsubscribe",
            "ssubscribe", "sunsubscribe", "ping", "quit", "reset");
    private static final Set<String> MULTI_PASS = Set.of("exec", "discard", "multi", "watch", "quit", "reset", "hello",
            "auth");

    private Object unknownCommand(byte[][] a) {
        StringBuilder sb = new StringBuilder("ERR unknown command '");
        sb.append(printable(a[0], 128)).append("', with args beginning with: ");
        int used = 0;
        for (int i = 1; i < a.length && used < 128; i++) {
            String p = printable(a[i], 128 - used);
            sb.append('\'').append(p).append("' ");
            used += p.length();
        }
        return new Resp.Err(sb.toString());
    }

    private static String printable(byte[] b, int max) {
        String s = new String(b, 0, Math.min(b.length, max), StandardCharsets.ISO_8859_1);
        return s.replace("\r", " ").replace("\n", " ");
    }

    /** Runs one command (not queued): shard selection, connection, retries, error mapping. */
    Object execute(Cmd cmd, byte[][] a, boolean unused) {
        try {
            int[] kp = cmd.keyPositions(a);
            if (cmd.blockFn != null) {
                callState = new java.util.HashMap<>();
                long t = cmd.blockFn.timeoutMs(a);
                if (t >= 0) {
                    return executeBlocking(cmd, a, kp, t);
                }
            }
            for (int attempt = 0;; attempt++) {
                try (Ctx x = new Ctx(store, this)) {
                    selectShard(x, a, kp);
                    Object r = cmd.handler.run(x, a);
                    if (r == Cmd.NOT_READY) {
                        return timeoutReplyOf(cmd);
                    }
                    return r;
                } catch (SQLException e) {
                    if (retryable(e) && attempt < 6) {
                        Thread.sleep(2L << attempt);
                        continue;
                    }
                    throw e;
                }
            }
        } catch (RedisError e) {
            return new Resp.Err(e.getMessage());
        } catch (SQLException e) {
            return sqlError(e);
        } catch (Resp.ProtocolError e) {
            return new Resp.Err("ERR Protocol error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Resp.Err("ERR interrupted");
        } catch (RuntimeException e) {
            if (e.getClass().getSimpleName().equals("BackendPoolExhaustedException")) {
                return new Resp.Err("BUSY backend connection pool exhausted: " + e.getMessage());
            }
            log.warn("rediswire: command {} failed: {}", cmd.name, e.toString(), e);
            return new Resp.Err("ERR internal error: " + e);
        } catch (Exception e) {
            log.warn("rediswire: command {} failed: {}", cmd.name, e.toString(), e);
            return new Resp.Err("ERR internal error: " + e);
        }
    }

    private static Object timeoutReplyOf(Cmd cmd) {
        return cmd.timeoutReplySet ? cmd.timeoutReply : Resp.NIL_ARRAY;
    }

    static boolean retryable(SQLException e) {
        String st = e.getSQLState();
        return "40001".equals(st) || "40P01".equals(st);
    }

    private Object sqlError(SQLException e) {
        String st = e.getSQLState();
        if (st != null && st.startsWith("08")) {
            return new Resp.Err("LOADING backend connection failed: " + e.getMessage());
        }
        if ("54000".equals(st) || (e.getMessage() != null && e.getMessage().contains("out of memory"))) {
            return new Resp.Err("OOM command not allowed when used memory > 'maxmemory'. " + e.getMessage());
        }
        log.debug("rediswire: postgres error {}: {}", st, e.getMessage());
        return new Resp.Err("ERR Postgres error: " + e.getMessage());
    }

    /** Selects the shard from the command's keys; CROSSSLOT when they live on different shards. */
    void selectShard(Ctx x, byte[][] a, int[] kp) {
        if (kp.length == 0 || hosts.size() == 1) {
            return;
        }
        int shard = x.shardOfKey(a[kp[0]]);
        for (int i = 1; i < kp.length; i++) {
            if (x.shardOfKey(a[kp[i]]) != shard) {
                throw new RedisError(RedisError.CROSSSLOT);
            }
        }
        x.useShard(shard);
    }

    // ------------------------------------------------------------------------------------------
    // blocking
    // ------------------------------------------------------------------------------------------

    private Object executeBlocking(Cmd cmd, byte[][] a, int[] kp, long timeoutMs) throws Exception {
        long deadline = timeoutMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
        callState = new java.util.HashMap<>();
        String host;
        List<String> ids = new ArrayList<>();
        try (Ctx x0 = new Ctx(store, this)) {
            selectShard(x0, a, kp);
            host = x0.host();
        }
        for (int p : kp) {
            ids.add(RedisStore.wakeId(db, a[p]));
        }
        Hub.Waiter w = store.hub().register(host, ids);
        blockWaiter = w;
        unblockMode = null;
        try {
            boolean first = true;
            while (true) {
                if (closed) {
                    return NO_REPLY;
                }
                // first come, first served: after the first attempt only the longest-blocked client of a key may take from it
                if (first || store.hub().mayProceed(w)) {
                    for (int attempt = 0;; attempt++) {
                        try (Ctx x = new Ctx(store, this)) {
                            selectShard(x, a, kp);
                            Object r = cmd.handler.run(x, a);
                            if (r != Cmd.NOT_READY) {
                                return r;
                            }
                            break;
                        } catch (SQLException e) {
                            if (retryable(e) && attempt < 6) {
                                Thread.sleep(2L << attempt);
                                continue;
                            }
                            throw e;
                        }
                    }
                }
                first = false;
                long left = deadline == Long.MAX_VALUE ? 2000 : deadline - System.currentTimeMillis();
                if (left <= 0) {
                    return timeoutReplyOf(cmd);
                }
                blocked = true;
                long ws = System.nanoTime();
                try {
                    w.await(Math.min(left, 2000));
                } finally {
                    blockedNanos += System.nanoTime() - ws;
                    blocked = false;
                }
                String um = unblockMode;
                if (um != null) {
                    unblockMode = null;
                    return "error".equals(um) ? new Resp.Err("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                            : timeoutReplyOf(cmd);
                }
            }
        } finally {
            blockWaiter = null;
            store.hub().unregister(w, ids);
        }
    }

    // ------------------------------------------------------------------------------------------
    // MULTI / EXEC
    // ------------------------------------------------------------------------------------------

    void unwatchAll() {
        watches.clear();
    }

    private Object exec() {
        if (!inMulti) {
            return new Resp.Err("ERR EXEC without MULTI");
        }
        List<byte[][]> q = new ArrayList<>(queue);
        boolean dirty = multiDirty;
        inMulti = false;
        multiDirty = false;
        queue.clear();
        List<Watch> ws = new ArrayList<>(watches);
        watches.clear();
        if (dirty) {
            return new Resp.Err("EXECABORT Transaction discarded because of previous errors.");
        }
        try {
            for (int attempt = 0;; attempt++) {
                try (Ctx x = new Ctx(store, this)) {
                    return runExec(x, q, ws);
                } catch (SQLException e) {
                    if (retryable(e) && attempt < 6) {
                        Thread.sleep(2L << attempt);
                        continue;
                    }
                    throw e;
                }
            }
        } catch (RedisError e) {
            return new Resp.Err(e.getMessage());
        } catch (SQLException e) {
            return sqlError(e);
        } catch (Exception e) {
            log.warn("rediswire: EXEC failed: {}", e.toString(), e);
            return new Resp.Err("ERR internal error: " + e);
        }
    }

    private Object runExec(Ctx x, List<byte[][]> q, List<Watch> ws) throws Exception {
        // one Postgres transaction => every key must live on the same shard
        int shard = -1;
        for (byte[][] a : q) {
            Cmd cmd = registry.get(lower(a[0]));
            for (int p : cmd.keyPositions(a)) {
                shard = mergeShard(shard, x.shardOfKey(a[p]));
            }
        }
        for (Watch w : ws) {
            shard = mergeShard(shard, w.shard());
        }
        if (shard >= 0) {
            x.useShard(shard);
        }
        x.begin();
        try {
            if (!ws.isEmpty()) {
                for (Watch w : ws) {
                    Long ver = x.one("SELECT ver FROM warp_redis_keys WHERE db = ? AND k = ? AND (exp IS NULL OR exp > ?) FOR UPDATE",
                            rs -> rs.getLong(1), w.db(), w.key(), x.now);
                    if ((ver == null ? -1L : ver) != w.ver()) {
                        x.rollback();
                        return Resp.NIL_ARRAY;
                    }
                }
            }
            List<Object> results = new ArrayList<>(q.size());
            for (byte[][] a : q) {
                Cmd cmd = registry.get(lower(a[0]));
                x.db = db;
                x.resp3 = resp3;
                Object r;
                try {
                    r = cmd.handler.run(x, a);
                    if (r == Cmd.NOT_READY) {
                        r = timeoutReplyOf(cmd);
                    }
                } catch (RedisError e) {
                    r = new Resp.Err(e.getMessage());
                }
                results.add(r);
            }
            x.commit();
            return results;
        } catch (Throwable t) {
            x.rollback();
            throw t;
        }
    }

    private static int mergeShard(int cur, int next) {
        if (cur >= 0 && cur != next) {
            throw new RedisError(RedisError.CROSSSLOT);
        }
        return next;
    }
}
