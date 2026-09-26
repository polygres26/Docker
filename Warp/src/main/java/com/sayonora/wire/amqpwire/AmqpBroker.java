package com.sayonora.wire.amqpwire;

import com.sayonora.wire.amqpwire.AmqpStore.BindingDef;
import com.sayonora.wire.amqpwire.AmqpStore.ExchangeDef;
import com.sayonora.wire.amqpwire.AmqpStore.Ins;
import com.sayonora.wire.amqpwire.AmqpStore.MsgRow;
import com.sayonora.wire.amqpwire.AmqpStore.NewMsg;
import com.sayonora.wire.amqpwire.AmqpStore.QueueDef;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP 0-9-1 broker core shared by every connection: the topology cache (exchanges and bindings live on the home host and are
 * re-read when they change or after a short TTL), routing, publish (one transaction per host, an outbox when a publish spans
 * hosts), dead-lettering, queue/exchange operations with RabbitMQ's error texts, consumer dispatch (FOR UPDATE SKIP LOCKED leases,
 * in-process wakeups plus a poll for other instances) and the maintenance sweeper. No database connection is held while a consumer
 * is idle.
 */
final class AmqpBroker {

    private static final Logger log = LoggerFactory.getLogger(AmqpBroker.class);
    private static final SecureRandom RND = new SecureRandom();

    record QKey(String vhost, String name) {
    }

    record PubResult(int routed, boolean rejected, String rejectedQueue) {
    }

    /** One consumer of a channel (basic.consume). */
    static final class Consumer {
        final DeliveryTarget ch;
        final String tag;
        final QKey key;
        final boolean noAck;
        final boolean exclusive;
        final int prefetch;
        final int priority;
        final java.util.concurrent.atomic.AtomicInteger unacked = new java.util.concurrent.atomic.AtomicInteger();
        volatile boolean cancelled;
        volatile boolean active;

        Consumer(DeliveryTarget ch, String tag, QKey key, boolean noAck, boolean exclusive, int prefetch, int priority) {
            this.ch = ch;
            this.tag = tag;
            this.key = key;
            this.noAck = noAck;
            this.exclusive = exclusive;
            this.prefetch = prefetch;
            this.priority = priority;
        }
    }

    private static final class Topo {
        long gen;
        long loadedAt;
        Map<String, ExchangeDef> ex = new LinkedHashMap<>();
        Map<String, List<BindingDef>> bySource = new LinkedHashMap<>();
    }

    private static final class QMeta {
        final QueueDef def;
        final long at;

        QMeta(QueueDef def, long at) {
            this.def = def;
            this.at = at;
        }
    }

    private static final class Disp {
        final AtomicBoolean running = new AtomicBoolean();
        final AtomicBoolean pending = new AtomicBoolean();
        int rr;
    }

    final AmqpConfig cfg;
    final AmqpShards shards;
    final AmqpStore store;
    final String nodeId = UUID.randomUUID().toString();
    private final AtomicLong topoGen = new AtomicLong();
    private final Map<String, Topo> topos = new ConcurrentHashMap<>();
    private final Set<String> knownVhosts = ConcurrentHashMap.newKeySet();
    private final Map<QKey, QMeta> qmeta = new ConcurrentHashMap<>();
    final Map<QKey, List<Consumer>> consumers = new ConcurrentHashMap<>();
    private final Map<QKey, Disp> disps = new ConcurrentHashMap<>();
    private final Set<QKey> hadConsumer = ConcurrentHashMap.newKeySet();
    private final Map<String, AmqpChannel> replyTo = new ConcurrentHashMap<>();
    private final ExecutorService dispatchPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "amqpwire-dispatch");
        t.setDaemon(true);
        return t;
    });
    /** Heartbeats and the consumer poll: never behind a slow maintenance pass. */
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "amqpwire-timer");
        t.setDaemon(true);
        return t;
    });
    /** Sweeper and node heartbeat (database work). */
    private final ScheduledExecutorService maint = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "amqpwire-maint");
        t.setDaemon(true);
        return t;
    });
    private final AtomicLong connSeq = new AtomicLong();
    private volatile boolean stopped;

    AmqpBroker(AmqpShards shards, AmqpConfig cfg) {
        this.shards = shards;
        this.cfg = cfg;
        this.store = new AmqpStore(shards);
    }

    ScheduledExecutorService timers() {
        return timers;
    }

    String newConnId() {
        return nodeId + "/" + connSeq.incrementAndGet();
    }

    void start() {
        timers.scheduleWithFixedDelay(this::pollTick, cfg.pollMs, cfg.pollMs, TimeUnit.MILLISECONDS);
        maint.scheduleWithFixedDelay(this::safeSweep, cfg.sweepMs, cfg.sweepMs, TimeUnit.MILLISECONDS);
        maint.scheduleWithFixedDelay(this::safeBeat, 0, 5, TimeUnit.SECONDS);
    }

    void stop() {
        stopped = true;
        timers.shutdownNow();
        maint.shutdownNow();
        dispatchPool.shutdownNow();
        try {
            if (shards.available()) {
                store.bye(nodeId);
            }
        } catch (RuntimeException ignored) {
            // shutting down
        }
    }

    private void safeBeat() {
        try {
            if (shards.available()) {
                store.beat(nodeId);
            }
        } catch (RuntimeException e) {
            log.debug("amqpwire heartbeat failed: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------ topology

    private static List<ExchangeDef> defaultExchanges(String vhost) {
        Map<String, Object> none = new LinkedHashMap<>();
        return List.of(new ExchangeDef(vhost, "amq.direct", "direct", true, false, false, none),
                new ExchangeDef(vhost, "amq.fanout", "fanout", true, false, false, none),
                new ExchangeDef(vhost, "amq.topic", "topic", true, false, false, none),
                new ExchangeDef(vhost, "amq.headers", "headers", true, false, false, none),
                new ExchangeDef(vhost, "amq.match", "headers", true, false, false, none),
                new ExchangeDef(vhost, "amq.rabbitmq.trace", "topic", true, false, true, none));
    }

    void ensureVhost(String vhost) {
        if (knownVhosts.contains(vhost)) {
            return;
        }
        store.ensureVhost(vhost, defaultExchanges(vhost));
        knownVhosts.add(vhost);
    }

    void invalidateTopology() {
        topoGen.incrementAndGet();
    }

    private Topo topo(String vhost) {
        Topo t = topos.get(vhost);
        long g = topoGen.get();
        long now = System.currentTimeMillis();
        if (t == null || t.gen != g || now - t.loadedAt > cfg.topologyTtlMs) {
            ensureVhost(vhost);
            Topo n = new Topo();
            n.gen = g;
            n.loadedAt = now;
            for (ExchangeDef e : store.exchanges(vhost)) {
                n.ex.put(e.name(), e);
            }
            for (BindingDef b : store.bindings(vhost)) {
                n.bySource.computeIfAbsent(b.source(), k -> new ArrayList<>()).add(b);
            }
            topos.put(vhost, n);
            t = n;
        }
        return t;
    }

    ExchangeDef exchange(String vhost, String name) {
        ensureVhost(vhost);
        return store.exchange(vhost, name);
    }

    private void routeInto(Topo t, String exName, String rk, Map<String, Object> headers, Set<String> visited, Set<String> out) {
        if (exName.isEmpty()) {
            out.add(rk);
            return;
        }
        ExchangeDef ex = t.ex.get(exName);
        if (ex == null || !visited.add(exName)) {
            return;
        }
        Set<String> matched = new LinkedHashSet<>();
        for (BindingDef b : t.bySource.getOrDefault(exName, List.of())) {
            if (AmqpRouting.matches(ex.type(), b, rk, headers)) {
                if (b.destType() == 'q') {
                    matched.add(b.dest());
                } else {
                    routeInto(t, b.dest(), rk, headers, visited, matched);
                }
            }
        }
        if (matched.isEmpty() && ex.args().get("alternate-exchange") instanceof String ae) {
            routeInto(t, ae, rk, headers, visited, matched);
        }
        out.addAll(matched);
    }

    Set<String> route(String vhost, String exName, String rk, Map<String, Object> headers) {
        Topo t = topo(vhost);
        Set<String> out = new LinkedHashSet<>();
        routeInto(t, exName, rk, headers, new HashSet<>(), out);
        return out;
    }

    // ------------------------------------------------------------------------------------------ queue metadata cache

    QueueDef qdef(String vhost, String name) {
        QKey k = new QKey(vhost, name);
        QMeta m = qmeta.get(k);
        long now = System.currentTimeMillis();
        if (m != null && now - m.at < 1000) {
            return m.def;
        }
        QueueDef d = store.queue(vhost, name);
        if (d == null) {
            qmeta.remove(k);
        } else {
            if (qmeta.size() > 20000) {
                qmeta.clear();
            }
            qmeta.put(k, new QMeta(d, now));
        }
        return d;
    }

    // ------------------------------------------------------------------------------------------ publish

    /** Routes and stores one message. Throws {@link AmqpException} 404 when the exchange does not exist. */
    PubResult publish(String vhost, String exName, String rk, byte[] propsRaw, AmqpProps props, byte[] body, String baseMsgId) {
        return publish(vhost, exName, rk, propsRaw, props, body, baseMsgId, false, null, null);
    }

    PubResult publish(String vhost, String exName, String rk, byte[] propsRaw, AmqpProps props, byte[] body, String baseMsgId, boolean redelivered,
            byte[] deaths, byte[] raw10) {
        ensureVhost(vhost);
        Topo t = topo(vhost);
        if (!exName.isEmpty() && !t.ex.containsKey(exName)) {
            ExchangeDef fresh = store.exchange(vhost, exName);
            if (fresh == null) {
                throw AmqpException.notFound("no exchange '" + exName + "' in vhost '" + vhost + "'");
            }
            invalidateTopology();
            t = topo(vhost);
        }
        if (!exName.isEmpty() && t.ex.get(exName) != null && t.ex.get(exName).internal() && baseMsgId == null) {
            throw AmqpException.accessRefused("cannot publish to internal exchange '" + exName + "' in vhost '" + vhost + "'");
        }
        Long ttl = props.expirationMs();
        if (ttl != null && ttl < 0) {
            String why = "no_integer";
            try {
                long v = Long.parseLong(props.expiration.trim());
                why = "{value_negative," + v + "}";
            } catch (NumberFormatException e) {
                // not an integer
            }
            throw AmqpException.precondition("invalid expiration '" + props.expiration + "': " + why);
        }
        List<String> keys = new ArrayList<>();
        keys.add(rk);
        boolean hadBcc = false;
        if (props.headers != null) {
            for (String h : new String[] {"CC", "BCC"}) {
                if (props.headers.get(h) instanceof List<?> l) {
                    for (Object o : l) {
                        if (o instanceof String s) {
                            keys.add(s);
                        }
                    }
                }
            }
            if (props.headers.containsKey("BCC")) {
                hadBcc = true;
                props.headers.remove("BCC");
                propsRaw = props.toBytes();
            }
        }
        Set<String> queues = new LinkedHashSet<>();
        for (String key : keys) {
            routeInto(t, exName, key, props.headersOrEmpty(), new HashSet<>(), queues);
        }
        if (queues.isEmpty()) {
            return new PubResult(0, false, null);
        }
        int prio = props.priority == null ? 0 : props.priority;
        boolean persistent = props.deliveryMode != null && props.deliveryMode == 2;
        String msgId = baseMsgId != null ? baseMsgId : UUID.randomUUID().toString();
        NewMsg m = new NewMsg(msgId, exName, rk, propsRaw, body, ttl, prio, persistent, redelivered, deaths, raw10);
        Map<String, Ins> res = deliverToHosts(vhost, queues, m, true);
        int routed = 0;
        boolean rejected = false;
        String rejectedQueue = null;
        for (Map.Entry<String, Ins> e : res.entrySet()) {
            if (e.getValue() == Ins.INSERTED) {
                routed++;
                signal(vhost, e.getKey());
                afterInsert(vhost, e.getKey());
            } else if (e.getValue() == Ins.REJECTED) {
                rejected = true;
                rejectedQueue = rejectedQueue == null ? e.getKey() : rejectedQueue;
                QueueDef qd = qdef(vhost, e.getKey());
                if (qd != null && "reject-publish-dlx".equals(qd.overflow()) && qd.dlx() != null) {
                    // the refused message is dead-lettered with reason maxlen instead of being dropped silently
                    MsgRow refused = new MsgRow(0, vhost, e.getKey(), msgId, exName, rk, propsRaw, body, prio, persistent, false, System.currentTimeMillis(), null, null, 0);
                    deadLetter(qd, List.of(refused), "maxlen");
                }
            }
        }
        return new PubResult(routed, rejected && routed == 0, rejectedQueue);
    }

    private Map<String, Ins> deliverToHosts(String vhost, Collection<String> queues, NewMsg m, boolean allowOutbox) {
        Map<String, List<String>> byHost = new LinkedHashMap<>();
        for (String q : queues) {
            byHost.computeIfAbsent(shards.owner(vhost, q), k -> new ArrayList<>()).add(q);
        }
        Map<String, Ins> out = new LinkedHashMap<>();
        if (byHost.size() == 1) {
            var e = byHost.entrySet().iterator().next();
            out.putAll(store.insertMessages(e.getKey(), vhost, e.getValue(), m));
            return out;
        }
        long outbox = -1;
        if (allowOutbox) {
            outbox = store.outboxInsert(encodeOutbox(vhost, queues, m));
        }
        boolean ok = true;
        for (var e : byHost.entrySet()) {
            try {
                out.putAll(store.insertMessages(e.getKey(), vhost, e.getValue(), m));
            } catch (RuntimeException ex) {
                ok = false;
                if (outbox < 0) {
                    throw ex;
                }
                log.warn("amqpwire: delivery to host {} failed, the outbox will retry it: {}", e.getKey(), ex.toString());
                for (String q : e.getValue()) {
                    out.put(q, Ins.INSERTED); // durable in the outbox: the publish is accepted
                }
            }
        }
        if (ok && outbox >= 0) {
            store.outboxDelete(outbox);
        }
        return out;
    }

    private static byte[] encodeOutbox(String vhost, Collection<String> queues, NewMsg m) {
        AmqpCodec.Writer w = new AmqpCodec.Writer();
        w.u8(1).longStr(vhost).longStr(m.msgId()).longStr(m.exchange()).longStr(m.rkey()).longStr(m.props()).longStr(m.body());
        w.i64(m.msgTtlMs() == null ? -1 : m.msgTtlMs()).u32(m.prio()).u8(m.persistent() ? 1 : 0).u8(m.redelivered() ? 1 : 0)
                .longStr(m.deaths() == null ? new byte[0] : m.deaths()).longStr(m.raw10() == null ? new byte[0] : m.raw10()).u32(queues.size());
        queues.forEach(w::longStr);
        return w.toBytes();
    }

    private void replayOutbox(byte[] payload) {
        AmqpCodec.Reader r = new AmqpCodec.Reader(payload);
        r.u8();
        String vhost = r.longStr();
        String id = r.longStr();
        String ex = r.longStr();
        String rk = r.longStr();
        byte[] props = r.longStrBytes();
        byte[] body = r.longStrBytes();
        long ttl = r.i64();
        int prio = (int) r.u32();
        boolean pers = r.u8() == 1;
        boolean redel = r.u8() == 1;
        byte[] deaths = r.longStrBytes();
        byte[] raw10 = r.longStrBytes();
        int n = (int) r.u32();
        List<String> qs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            qs.add(r.longStr());
        }
        NewMsg m = new NewMsg(id, ex, rk, props, body, ttl < 0 ? null : ttl, prio, pers, redel, deaths.length == 0 ? null : deaths, raw10.length == 0 ? null : raw10);
        for (var e : deliverToHosts(vhost, qs, m, false).entrySet()) {
            if (e.getValue() == Ins.INSERTED) {
                signal(vhost, e.getKey());
            }
        }
    }

    private void afterInsert(String vhost, String queue) {
        QueueDef q = qdef(vhost, queue);
        if (q != null && "drop-head".equals(q.overflow()) && (q.maxLen() != null || q.maxBytes() != null)) {
            enforceLimits(q);
        }
    }

    void enforceLimits(QueueDef q) {
        shards.tx(shards.owner(q.vhost(), q.name()), c -> {
            List<MsgRow> over = new ArrayList<>(store.lockOverflow(c, q));
            for (MsgRow r : store.lockByteOverflow(c, q)) {
                if (over.stream().noneMatch(o -> o.seq() == r.seq())) {
                    over.add(r);
                }
            }
            if (!over.isEmpty()) {
                deadLetter(q, over, "maxlen");
                store.deleteRows(c, over);
            }
            return null;
        });
    }

    // ------------------------------------------------------------------------------------------ dead lettering

    @SuppressWarnings("unchecked")
    void deadLetter(QueueDef src, List<MsgRow> rows, String reason) {
        deadLetter(src, rows, reason, null);
    }

    /** {@code redelivered}: the flag the dead letter gets (null = that of the source message); RabbitMQ sets it for basic.reject but not for basic.nack. */
    void deadLetter(QueueDef src, List<MsgRow> rows, String reason, Boolean redelivered) {
        if (src == null || src.dlx() == null) {
            return;
        }
        for (MsgRow row : rows) {
            try {
                AmqpProps p = AmqpProps.parse(row.props());
                Map<String, Object> h = p.headers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(p.headers);
                // the broker's own record of the message's deaths (a client-supplied x-death header is opaque and gets replaced)
                List<Object> deaths = new ArrayList<>();
                boolean firstDeath = row.deaths() == null;
                if (!firstDeath) {
                    Object o = new AmqpCodec.Reader(row.deaths()).value();
                    if (o instanceof List<?> l) {
                        deaths.addAll((List<Object>) l);
                    }
                }
                // RabbitMQ drops a message that returns to a queue it already died in when no death of the cycle was a rejection
                boolean anyRejected = "rejected".equals(reason);
                boolean seenHere = false;
                for (Object d : deaths) {
                    if (d instanceof Map<?, ?> dm) {
                        anyRejected |= "rejected".equals(dm.get("reason"));
                        seenHere |= src.name().equals(dm.get("queue"));
                    }
                }
                if (seenHere && !anyRejected) {
                    log.debug("amqpwire: dead-letter cycle through {} dropped", src.name());
                    continue;
                }
                Map<String, Object> entry = null;
                for (int i = 0; i < deaths.size(); i++) {
                    if (deaths.get(i) instanceof Map<?, ?> dm && src.name().equals(dm.get("queue")) && reason.equals(dm.get("reason"))
                            && row.exchange().equals(dm.get("exchange"))) {
                        entry = new LinkedHashMap<>((Map<String, Object>) dm);
                        deaths.remove(i);
                        Long cnt = AmqpCodec.asLong(entry.get("count"));
                        entry.put("count", (cnt == null ? 0L : cnt) + 1);
                        break;
                    }
                }
                if (entry == null) {
                    entry = new LinkedHashMap<>();
                    entry.put("count", 1L);
                    entry.put("reason", reason);
                    entry.put("queue", src.name());
                    entry.put("time", new AmqpCodec.Ts(System.currentTimeMillis() / 1000));
                    entry.put("exchange", row.exchange());
                    entry.put("routing-keys", new ArrayList<Object>(List.of(row.rkey())));
                    if (p.expiration != null) {
                        entry.put("original-expiration", p.expiration);
                    }
                }
                deaths.add(0, entry);
                h.put("x-death", new ArrayList<Object>(deaths));
                if (firstDeath) {
                    h.put("x-first-death-reason", reason);
                    h.put("x-first-death-queue", src.name());
                    h.put("x-first-death-exchange", row.exchange());
                }
                h.put("x-last-death-reason", reason);
                h.put("x-last-death-queue", src.name());
                h.put("x-last-death-exchange", row.exchange());
                p.headers = h;
                p.expiration = null;
                String rk = src.dlxRkey() != null ? src.dlxRkey() : row.rkey();
                AmqpCodec.Writer dw = new AmqpCodec.Writer();
                dw.value(deaths);
                publish(src.vhost(), src.dlx(), rk, p.toBytes(), p, row.body(),
                        "dl:" + row.msgId() + ":" + src.name() + ":" + reason + ":" + entryCount(entry), redelivered != null ? redelivered : row.redelivered(),
                        dw.toBytes(), row.raw10());
            } catch (AmqpException e) {
                log.debug("amqpwire: dead-letter of a message from {} dropped: {}", src.name(), e.getMessage());
            }
        }
    }

    private static long entryCount(Map<String, Object> e) {
        Long c = AmqpCodec.asLong(e.get("count"));
        return c == null ? 1 : c;
    }

    // ------------------------------------------------------------------------------------------ exchanges

    private static String q(String s) {
        return "'" + s + "'";
    }

    static String diffText(String what, String kind, String name, String vhost, Object received, Object current) {
        return "inequivalent arg '" + what + "' for " + kind + " '" + name + "' in vhost '" + vhost + "': received '" + received
                + "' but current is '" + current + "'";
    }

    /** RabbitMQ's name of a field table value's AMQP type. */
    static String typeName(Object v) {
        if (v == null) {
            return "void";
        } else if (v instanceof Integer) {
            return "signedint";
        } else if (v instanceof Long) {
            return "long";
        } else if (v instanceof Short) {
            return "short";
        } else if (v instanceof Byte) {
            return "byte";
        } else if (v instanceof String) {
            return "longstr";
        } else if (v instanceof Boolean) {
            return "bool";
        } else if (v instanceof Double) {
            return "double";
        } else if (v instanceof Float) {
            return "float";
        } else if (v instanceof java.math.BigDecimal) {
            return "decimal";
        } else if (v instanceof AmqpCodec.Ts) {
            return "timestamp";
        } else if (v instanceof List<?>) {
            return "array";
        } else if (v instanceof Map<?, ?>) {
            return "table";
        }
        return "binary";
    }

    private static String valueText(Object v) {
        return v instanceof AmqpCodec.Ts t ? String.valueOf(t.seconds()) : String.valueOf(v);
    }

    /** RabbitMQ's argument equivalence check on a re-declare: only the listed keys are compared, with its three wordings. */
    static void assertArgsEquivalent(String kind, String name, String vhost, Map<String, Object> cur, Map<String, Object> want, List<String> keys) {
        for (String k : keys) {
            boolean hc = cur.containsKey(k);
            boolean hw = want.containsKey(k);
            if (!hc && !hw) {
                continue;
            }
            String base = "inequivalent arg '" + k + "' for " + kind + " '" + name + "' in vhost '" + vhost + "': ";
            if (hc && !hw) {
                throw AmqpException.precondition(base + "received none but current is the value '" + valueText(cur.get(k)) + "' of type '"
                        + typeName(cur.get(k)) + "'");
            }
            if (!hc) {
                throw AmqpException.precondition(base + "received the value '" + valueText(want.get(k)) + "' of type '" + typeName(want.get(k))
                        + "' but current is none");
            }
            if (!AmqpCodec.canonical(cur.get(k)).equals(AmqpCodec.canonical(want.get(k)))) {
                throw AmqpException.precondition(base + "received '" + valueText(want.get(k)) + "' but current is '" + valueText(cur.get(k)) + "'");
            }
        }
    }

    static final List<String> QUEUE_EQUIV_KEYS = List.of("x-expires", "x-message-ttl", "x-dead-letter-exchange", "x-dead-letter-routing-key",
            "x-max-length", "x-max-length-bytes", "x-max-priority", "x-overflow", "x-queue-type");

    void declareExchange(String vhost, String name, String type, boolean passive, boolean durable, boolean autoDelete, boolean internal,
            Map<String, Object> args) {
        ensureVhost(vhost);
        if (name.isEmpty()) {
            throw AmqpException.accessRefused("operation not permitted on the default exchange");
        }
        ExchangeDef cur = store.exchange(vhost, name);
        if (passive) {
            if (cur == null) {
                throw AmqpException.notFound("no exchange " + q(name) + " in vhost " + q(vhost));
            }
            return;
        }
        if (name.startsWith("amq.") && cur == null) {
            throw AmqpException.accessRefused("exchange name '" + name + "' contains reserved prefix 'amq.*'");
        }
        if (type.isEmpty()) {
            throw AmqpException.precondition("invalid exchange type ''");
        }
        if (!AmqpRouting.validType(type)) {
            throw AmqpException.precondition("unknown exchange type '" + type + "'");
        }
        ExchangeDef d = new ExchangeDef(vhost, name, type, durable, autoDelete, internal, args);
        if (cur == null) {
            if (store.insertExchange(d)) {
                invalidateTopology();
                return;
            }
            cur = store.exchange(vhost, name);
        }
        if (!cur.type().equals(type)) {
            throw AmqpException.precondition(diffText("type", "exchange", name, vhost, type, cur.type()));
        }
        if (cur.durable() != durable) {
            throw AmqpException.precondition(diffText("durable", "exchange", name, vhost, durable, cur.durable()));
        }
        if (cur.autoDelete() != autoDelete) {
            throw AmqpException.precondition(diffText("auto_delete", "exchange", name, vhost, autoDelete, cur.autoDelete()));
        }
        if (cur.internal() != internal) {
            throw AmqpException.precondition(diffText("internal", "exchange", name, vhost, internal, cur.internal()));
        }
        assertArgsEquivalent("exchange", name, vhost, cur.args(), args, List.of("alternate-exchange"));
    }

    void deleteExchange(String vhost, String name, boolean ifUnused) {
        ensureVhost(vhost);
        if (name.isEmpty()) {
            throw AmqpException.accessRefused("operation not permitted on the default exchange");
        }
        if (name.startsWith("amq.")) {
            throw AmqpException.accessRefused("deletion of system exchange '" + name + "' in vhost '" + vhost + "' not allowed");
        }
        ExchangeDef cur = store.exchange(vhost, name);
        if (cur == null) {
            return;
        }
        if (ifUnused && !store.bindings(vhost).stream().noneMatch(b -> b.source().equals(name))) {
            throw AmqpException.precondition("exchange " + q(name) + " in vhost " + q(vhost) + " in use");
        }
        Set<String> affected = new HashSet<>();
        store.bindings(vhost).stream().filter(b -> b.dest().equals(name) && b.destType() == 'e').forEach(b -> affected.add(b.source()));
        store.deleteExchange(vhost, name);
        invalidateTopology();
        affected.forEach(s -> autoDeleteExchange(vhost, s));
    }

    private void autoDeleteExchange(String vhost, String ex) {
        if (!ex.isEmpty() && store.autoDeletable(vhost, ex)) {
            store.deleteExchange(vhost, ex);
            invalidateTopology();
            store.bindings(vhost).stream().filter(b -> b.dest().equals(ex) && b.destType() == 'e').findAny(); // removed with the exchange
        }
    }

    // ------------------------------------------------------------------------------------------ bindings

    private void checkBindEndpoints(String vhost, String source, String dest, char destType, String owner) {
        if (source.isEmpty()) {
            throw AmqpException.accessRefused("operation not permitted on the default exchange");
        }
        if (destType == 'q') {
            QueueDef qd = store.queue(vhost, dest);
            if (qd == null) {
                throw AmqpException.notFound("no queue " + q(dest) + " in vhost " + q(vhost));
            }
            checkExclusive(qd, owner);
        } else if (dest.isEmpty()) {
            throw AmqpException.accessRefused("operation not permitted on the default exchange");
        } else if (store.exchange(vhost, dest) == null) {
            throw AmqpException.notFound("no exchange " + q(dest) + " in vhost " + q(vhost));
        }
        if (store.exchange(vhost, source) == null) {
            throw AmqpException.notFound("no exchange " + q(source) + " in vhost " + q(vhost));
        }
    }

    void bind(String vhost, String source, String dest, char destType, String rkey, Map<String, Object> args, String owner) {
        ensureVhost(vhost);
        checkBindEndpoints(vhost, source, dest, destType, owner);
        ExchangeDef src = store.exchange(vhost, source);
        if (src != null && src.type().equals("headers") && args.containsKey("x-match")) {
            Object xm = args.get("x-match");
            if (!(xm instanceof String)) {
                throw AmqpException.precondition("Invalid x-match field type " + typeName(xm) + " (value " + valueText(xm) + "); expected longstr");
            }
            if (!List.of("all", "any", "all-with-x", "any-with-x").contains(xm)) {
                throw AmqpException.precondition("Invalid x-match field value <<\"" + xm + "\">>; expected all, any, all-with-x, or any-with-x");
            }
        }
        store.addBinding(new BindingDef(vhost, source, dest, destType, rkey, args));
        invalidateTopology();
    }

    void unbind(String vhost, String source, String dest, char destType, String rkey, Map<String, Object> args, String owner) {
        ensureVhost(vhost);
        if (source.isEmpty()) {
            throw AmqpException.accessRefused("operation not permitted on the default exchange");
        }
        if (destType == 'q') {
            QueueDef qd = store.queue(vhost, dest);
            if (qd != null) {
                checkExclusive(qd, owner);
            }
        }
        store.removeBinding(new BindingDef(vhost, source, dest, destType, rkey, args));
        invalidateTopology();
        autoDeleteExchange(vhost, source);
    }

    // ------------------------------------------------------------------------------------------ queues

    static void checkExclusive(QueueDef qd, String owner) {
        if (qd.exclOwner() != null && !qd.exclOwner().equals(owner)) {
            throw AmqpException.locked("cannot obtain exclusive access to locked queue '" + qd.name() + "' in vhost '" + qd.vhost()
                    + "'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.");
        }
    }

    static String generatedName() {
        byte[] b = new byte[16];
        RND.nextBytes(b);
        return "amq.gen-" + Base64.getUrlEncoder().withoutPadding().encodeToString(b).substring(0, 22);
    }

    private static AmqpException invalidArg(String key, String qname, String vhost, String why) {
        return AmqpException.precondition("invalid arg '" + key + "' for queue '" + qname + "' in vhost '" + vhost + "': " + why);
    }

    private static Long intArg(Map<String, Object> args, String key, String qname, String vhost, boolean zeroOk, long max) {
        if (!args.containsKey(key)) {
            return null;
        }
        Object v = args.get(key);
        Long n = AmqpCodec.asLong(v);
        if (n == null) {
            throw invalidArg(key, qname, vhost, "\"expected integer, got " + typeName(v) + "\"");
        }
        if (n < 0) {
            throw invalidArg(key, qname, vhost, "{value_negative," + n + "}");
        }
        if (n == 0 && !zeroOk) {
            throw invalidArg(key, qname, vhost, "{value_zero,0}");
        }
        if (n > max) {
            throw invalidArg(key, qname, vhost, "{max_value_exceeded," + n + "}");
        }
        return n;
    }

    static QueueDef buildQueue(String vhost, String name, boolean durable, String exclOwner, boolean autoDelete, Map<String, Object> args) {
        Long ttl = intArg(args, "x-message-ttl", name, vhost, true, 4294967295L);
        Long expires = intArg(args, "x-expires", name, vhost, false, 4294967295L);
        Long maxLen = intArg(args, "x-max-length", name, vhost, true, Long.MAX_VALUE);
        Long maxBytes = intArg(args, "x-max-length-bytes", name, vhost, true, Long.MAX_VALUE);
        Long prio = intArg(args, "x-max-priority", name, vhost, true, 255);
        String overflow = "drop-head";
        if (args.containsKey("x-overflow")) {
            Object o = args.get("x-overflow");
            if (!(o instanceof String s) || !List.of("drop-head", "reject-publish", "reject-publish-dlx").contains(s)) {
                throw invalidArg("x-overflow", name, vhost, "invalid_overflow");
            }
            overflow = s;
        }
        String dlx = null;
        if (args.containsKey("x-dead-letter-exchange")) {
            if (!(args.get("x-dead-letter-exchange") instanceof String s)) {
                throw AmqpException.precondition("invalid type '" + typeName(args.get("x-dead-letter-exchange")) + "' for arg 'x-dead-letter-exchange' in queue '"
                        + name + "' in vhost '" + vhost + "'");
            }
            dlx = s;
        }
        String dlk = null;
        if (args.containsKey("x-dead-letter-routing-key")) {
            if (!(args.get("x-dead-letter-routing-key") instanceof String s)) {
                throw invalidArg("x-dead-letter-routing-key", name, vhost, "{unacceptable_type," + typeName(args.get("x-dead-letter-routing-key")) + "}");
            }
            dlk = s;
        }
        if (args.containsKey("x-queue-type")) {
            Object o = args.get("x-queue-type");
            if (!(o instanceof String t) || !(t.equals("classic") || t.equals("quorum"))) {
                throw invalidArg("x-queue-type", name, vhost, "\"unsupported queue type '" + o + "'\"");
            }
            if (t.equals("quorum") && (!durable || exclOwner != null || autoDelete)) {
                throw AmqpException.precondition("invalid property '" + (!durable ? "non-durable" : exclOwner != null ? "exclusive-owner" : "auto-delete")
                        + "' for queue '" + name + "' in vhost '" + vhost + "'");
            }
        }
        return new QueueDef(vhost, name, durable, exclOwner, autoDelete, args, prio == null ? 0 : prio.intValue(), ttl, maxLen, maxBytes, overflow, dlx,
                dlk, expires);
    }

    QueueDef declareQueue(String vhost, String name, boolean passive, boolean durable, boolean exclusive, boolean autoDelete,
            Map<String, Object> args, String owner) {
        return declareQueue(vhost, name, passive, durable, exclusive, autoDelete, args, owner, false);
    }

    QueueDef declareQueue(String vhost, String name, boolean passive, boolean durable, boolean exclusive, boolean autoDelete,
            Map<String, Object> args, String owner, boolean serverNamed) {
        ensureVhost(vhost);
        if (passive) {
            QueueDef d = store.queue(vhost, name);
            if (d == null) {
                throw AmqpException.notFound("no queue " + q(name) + " in vhost " + q(vhost));
            }
            checkExclusive(d, owner);
            return d;
        }
        if (!serverNamed && name.startsWith("amq.")) {
            throw AmqpException.accessRefused("queue name '" + name + "' contains reserved prefix 'amq.*'");
        }
        QueueDef cur = store.queue(vhost, name);
        if (cur == null) {
            // arguments are validated when a queue is created, not when an existing one is re-declared
            QueueDef want = buildQueue(vhost, name, durable, exclusive ? owner : null, autoDelete, args);
            if (store.insertQueue(want)) {
                qmeta.remove(new QKey(vhost, name));
                return want;
            }
            cur = store.queue(vhost, name);
            if (cur == null) {
                return want;
            }
        }
        // RabbitMQ order: exclusive owner, durable, auto_delete, then the known arguments
        if (!java.util.Objects.equals(cur.exclOwner(), exclusive ? owner : null)) {
            throw AmqpException.locked("cannot obtain exclusive access to locked queue '" + name + "' in vhost '" + vhost
                    + "'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.");
        }
        if (cur.durable() != durable) {
            throw AmqpException.precondition(diffText("durable", "queue", name, vhost, durable, cur.durable()));
        }
        if (cur.autoDelete() != autoDelete) {
            throw AmqpException.precondition(diffText("auto_delete", "queue", name, vhost, autoDelete, cur.autoDelete()));
        }
        Map<String, Object> ca = new LinkedHashMap<>(cur.args());
        Map<String, Object> wa = new LinkedHashMap<>(args);
        if ("classic".equals(ca.get("x-queue-type")) && !wa.containsKey("x-queue-type")) {
            wa.put("x-queue-type", "classic");
        }
        if ("classic".equals(wa.get("x-queue-type")) && !ca.containsKey("x-queue-type")) {
            ca.put("x-queue-type", "classic");
        }
        assertArgsEquivalent("queue", name, vhost, ca, wa, QUEUE_EQUIV_KEYS);
        store.touch(vhost, name);
        return cur;
    }

    int consumerCount(String vhost, String name) {
        List<Consumer> l = consumers.get(new QKey(vhost, name));
        return l == null ? 0 : l.size();
    }

    long deleteQueue(String vhost, String name, boolean ifUnused, boolean ifEmpty, String owner) {
        ensureVhost(vhost);
        QueueDef d = store.queue(vhost, name);
        if (d == null) {
            return 0;
        }
        checkExclusive(d, owner);
        if (ifUnused && consumerCount(vhost, name) > 0) {
            throw AmqpException.precondition("queue " + q(name) + " in vhost " + q(vhost) + " in use");
        }
        if (ifEmpty && store.ready(vhost, name) > 0) {
            throw AmqpException.precondition("queue " + q(name) + " in vhost " + q(vhost) + " not empty");
        }
        return removeQueue(vhost, name);
    }

    /** Deletes a queue unconditionally: cancels its consumers, drops its bindings and messages. */
    long removeQueue(String vhost, String name) {
        QKey k = new QKey(vhost, name);
        List<Consumer> cs = consumers.remove(k);
        if (cs != null) {
            for (Consumer c : cs) {
                c.cancelled = true;
                c.ch.serverCancel(c);
            }
        }
        long n = store.deleteQueue(vhost, name);
        qmeta.remove(k);
        hadConsumer.remove(k);
        Set<String> sources = store.deleteQueueBindings(vhost, name);
        invalidateTopology();
        sources.forEach(s -> autoDeleteExchange(vhost, s));
        return Math.max(n, 0);
    }

    long purge(String vhost, String name, String owner) {
        QueueDef d = store.queue(vhost, name);
        if (d == null) {
            throw AmqpException.notFound("no queue " + q(name) + " in vhost " + q(vhost));
        }
        checkExclusive(d, owner);
        return store.purge(vhost, name);
    }

    // ------------------------------------------------------------------------------------------ consumers and dispatch

    Consumer addConsumer(DeliveryTarget ch, String vhost, String queue, String tag, boolean noAck, boolean exclusive, int prefetch,
            Map<String, Object> args, String owner) {
        QueueDef d = store.queue(vhost, queue);
        if (d == null) {
            throw AmqpException.notFound("no queue " + q(queue) + " in vhost " + q(vhost));
        }
        checkExclusive(d, owner);
        QKey k = new QKey(vhost, queue);
        Long pr = args == null ? null : AmqpCodec.asLong(args.get("x-priority"));
        Consumer c = new Consumer(ch, tag, k, noAck, exclusive, prefetch, pr == null ? 0 : pr.intValue());
        synchronized (consumers) {
            List<Consumer> l = consumers.computeIfAbsent(k, x -> new CopyOnWriteArrayList<>());
            if (!l.isEmpty() && (exclusive || l.get(0).exclusive)) {
                throw AmqpException.accessRefused("queue " + q(queue) + " in vhost " + q(vhost) + " in exclusive use");
            }
            l.add(c);
            hadConsumer.add(k);
        }
        store.touch(vhost, queue);
        return c;
    }

    /** Removes a consumer; deletes the queue when it is auto-delete and this was its last consumer. */
    void removeConsumer(Consumer c) {
        c.cancelled = true;
        boolean empty;
        synchronized (consumers) {
            List<Consumer> l = consumers.get(c.key);
            if (l == null) {
                return;
            }
            l.remove(c);
            empty = l.isEmpty();
            if (empty) {
                consumers.remove(c.key);
                disps.remove(c.key);
            }
        }
        if (empty && hadConsumer.contains(c.key)) {
            try {
                QueueDef d = store.queue(c.key.vhost(), c.key.name());
                if (d != null && d.autoDelete()) {
                    removeQueue(c.key.vhost(), c.key.name());
                }
            } catch (RuntimeException e) {
                log.debug("amqpwire: auto-delete of {} failed: {}", c.key, e.toString());
            }
        }
    }

    void signal(String vhost, String queue) {
        QKey k = new QKey(vhost, queue);
        if (!consumers.containsKey(k) || stopped) {
            return;
        }
        Disp d = disps.computeIfAbsent(k, x -> new Disp());
        d.pending.set(true);
        if (d.running.compareAndSet(false, true)) {
            try {
                dispatchPool.execute(() -> runDispatch(k, d));
            } catch (RuntimeException e) {
                d.running.set(false);
            }
        }
    }

    private void runDispatch(QKey k, Disp d) {
        try {
            do {
                d.pending.set(false);
                try {
                    deliverRound(k, d);
                } catch (RuntimeException e) {
                    log.warn("amqpwire: dispatch of {} failed: {}", k, e.toString());
                }
            } while (d.pending.get());
        } finally {
            d.running.set(false);
            if (d.pending.get() && consumers.containsKey(k)) {
                signal(k.vhost(), k.name());
            }
        }
    }

    private void deliverRound(QKey k, Disp d) {
        while (true) {
            List<Consumer> cs = consumers.get(k);
            if (cs == null || cs.isEmpty()) {
                return;
            }
            int cap = 0;
            for (Consumer c : cs) {
                if (!c.cancelled) {
                    cap = (int) Math.min(1000, (long) cap + Math.min(1000, c.ch.available(c)));
                }
            }
            if (cap <= 0) {
                return;
            }
            int n = Math.min(cap, 200);
            List<MsgRow> rows = store.claim(k.vhost(), k.name(), nodeId + "/d", n);
            if (rows.isEmpty()) {
                return;
            }
            List<Long> unplaced = new ArrayList<>();
            List<Long> autoAck = new ArrayList<>();
            for (MsgRow row : rows) {
                Consumer target = pick(cs, d);
                if (target == null || !target.ch.deliver(target, row)) {
                    unplaced.add(row.seq());
                } else if (target.noAck) {
                    autoAck.add(row.seq());
                }
            }
            store.ack(k.vhost(), k.name(), autoAck);
            if (!unplaced.isEmpty()) {
                store.requeue(k.vhost(), k.name(), unplaced);
                return;
            }
            if (rows.size() < n) {
                return;
            }
        }
    }

    /** Round robin over the consumers of the highest priority that still have prefetch capacity. */
    private Consumer pick(List<Consumer> cs, Disp d) {
        int best = Integer.MIN_VALUE;
        for (Consumer c : cs) {
            if (!c.cancelled && c.ch.available(c) > 0) {
                best = Math.max(best, c.priority);
            }
        }
        if (best == Integer.MIN_VALUE) {
            return null;
        }
        int n = cs.size();
        for (int i = 0; i < n; i++) {
            Consumer c = cs.get(Math.floorMod(d.rr + i, n));
            if (!c.cancelled && c.priority == best && c.ch.available(c) > 0) {
                d.rr = Math.floorMod(d.rr + i + 1, n);
                return c;
            }
        }
        return null;
    }

    /** Signals the queue's dispatcher and waits (bounded) until it has nothing left to do; used to finish an AMQP 1.0 drain. */
    void dispatchAndWait(String vhost, String queue, long maxWaitMs) {
        QKey k = new QKey(vhost, queue);
        signal(vhost, queue);
        long end = System.currentTimeMillis() + maxWaitMs;
        Disp d = disps.get(k);
        while (d != null && (d.running.get() || d.pending.get()) && System.currentTimeMillis() < end) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void pollTick() {
        try {
            for (QKey k : consumers.keySet()) {
                signal(k.vhost(), k.name());
            }
        } catch (RuntimeException e) {
            log.debug("amqpwire poll failed: {}", e.toString());
        }
    }

    // ------------------------------------------------------------------------------------------ direct reply-to

    String registerReplyTo(AmqpChannel ch) {
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes());
        replyTo.put(token, ch);
        return token;
    }

    void unregisterReplyTo(String token) {
        replyTo.remove(token);
    }

    AmqpChannel replyChannel(String token) {
        return replyTo.get(token);
    }

    private static byte[] uuidBytes() {
        byte[] b = new byte[16];
        RND.nextBytes(b);
        return b;
    }

    // ------------------------------------------------------------------------------------------ maintenance

    private void safeSweep() {
        if (stopped || !shards.available()) {
            return;
        }
        try {
            sweep();
        } catch (RuntimeException e) {
            log.debug("amqpwire sweep failed: {}", e.toString());
        }
    }

    /** Dead-letters or drops the expired head messages of one queue (or of every queue of {@code host}) right now. */
    void expire(String host, String vhost, String queue) {
        shards.tx(host, c -> {
            var exp = store.lockExpired(c, 200, vhost, queue);
            if (exp.isEmpty()) {
                return null;
            }
            Map<String, List<MsgRow>> perQueue = new LinkedHashMap<>();
            Map<String, QueueDef> defs = new LinkedHashMap<>();
            for (var e : exp) {
                String key = e.msg().vhost() + "\u0000" + e.msg().queue();
                perQueue.computeIfAbsent(key, x -> new ArrayList<>()).add(e.msg());
                defs.put(key, e.queue());
            }
            for (var e : perQueue.entrySet()) {
                deadLetter(defs.get(e.getKey()), e.getValue(), "expired");
            }
            List<MsgRow> all = new ArrayList<>();
            exp.forEach(e -> all.add(e.msg()));
            store.deleteRows(c, all);
            return null;
        });
    }

    void expireQueue(String vhost, String queue) {
        expire(shards.owner(vhost, queue), vhost, queue);
    }

    void sweep() {
        Set<String> live = store.liveNodes(30);
        live.add(nodeId);
        // keep the queues that have local consumers alive for x-expires
        for (QKey k : consumers.keySet()) {
            try {
                store.touch(k.vhost(), k.name());
            } catch (RuntimeException ignored) {
                // next tick
            }
        }
        for (String h : shards.allHosts()) {
            try {
                store.releaseDead(h, live);
                for (String vq : store.deadExclusive(h, live)) {
                    String[] p = vq.split("\u0001", 2);
                    removeQueue(p[0], p[1]);
                }
                for (QueueDef q : store.expiredQueues(h)) {
                    if (consumerCount(q.vhost(), q.name()) == 0) {
                        removeQueue(q.vhost(), q.name());
                    }
                }
                expire(h, null, null);
            } catch (RuntimeException e) {
                log.debug("amqpwire sweep of {} failed: {}", h, e.toString());
            }
        }
        for (AmqpStore.OutboxRow o : store.outboxStale(20, 5)) {
            try {
                replayOutbox(o.payload());
                store.outboxDelete(o.id());
            } catch (RuntimeException e) {
                log.debug("amqpwire outbox replay failed: {}", e.toString());
            }
        }
    }
}
