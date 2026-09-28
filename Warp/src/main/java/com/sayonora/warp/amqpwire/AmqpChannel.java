package com.sayonora.warp.amqpwire;

import com.sayonora.warp.amqpwire.AmqpBroker.Consumer;
import com.sayonora.warp.amqpwire.AmqpStore.MsgRow;
import com.sayonora.warp.amqpwire.AmqpStore.QueueDef;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One AMQP channel: publish assembly, confirms, transactions, consumers, delivery tags and acknowledgements. */
final class AmqpChannel implements DeliveryTarget {

    static final String REPLY_TO = "amq.rabbitmq.reply-to";

    private static final class Pending {
        String exchange;
        String rkey;
        boolean mandatory;
        byte[] props;
        AmqpProps parsed;
        long size = -1;
        ByteArrayOutputStream body = new ByteArrayOutputStream();
    }

    private record Unack(long seq, String vhost, String queue, Consumer consumer, MsgRow row) {
    }

    private record TxAck(long tag, boolean multiple, boolean nack, boolean requeue, boolean reject) {
    }

    final AmqpConnection conn;
    final int no;
    private final AmqpBroker br;
    private final Object lock = new Object();
    /** Serializes tag assignment + the socket write of a delivery so tags reach the client in order; acks only need {@link #lock}. */
    private final Object deliverLock = new Object();
    private Pending cur;
    private boolean confirm;
    private boolean tx;
    private long publishSeq;
    private final List<Pending> txPublishes = new ArrayList<>();
    private final List<TxAck> txAcks = new ArrayList<>();
    private long nextTag = 1;
    private final LinkedHashMap<Long, Unack> unacked = new LinkedHashMap<>();
    private final Map<String, Consumer> consumers = new LinkedHashMap<>();
    private int prefetchPerConsumer;
    private int prefetchGlobal;
    private volatile boolean flowActive = true;
    private String lastQueue;
    private String replyToken;
    private String replyTag;
    volatile boolean closed;
    /** After a channel error the channel ignores everything but channel.close-ok. */
    volatile boolean errored;
    private int ctagSeq;

    AmqpChannel(AmqpConnection conn, int no, AmqpBroker br) {
        this.conn = conn;
        this.no = no;
        this.br = br;
    }

    // ------------------------------------------------------------------------------------------ frames

    boolean expectingContent() {
        return cur != null;
    }

    void onHeader(int classId, long bodySize, byte[] propsRaw) {
        Pending p = cur;
        if (p == null || p.size >= 0) {
            throw AmqpException.conn(505, "UNEXPECTED_FRAME - expected method frame, got non method frame instead").at(0, 0);
        }
        if (classId != 60) {
            throw AmqpException.conn(505, "UNEXPECTED_FRAME - expected content header for class 60, got one for class " + classId + " instead").at(60, 40);
        }
        if (bodySize > br.cfg.maxMessageBytes) {
            cur = null;
            throw AmqpException.conn(541, "INTERNAL_ERROR - message size " + bodySize + " is larger than configured max size " + br.cfg.maxMessageBytes);
        }
        p.size = bodySize;
        p.props = propsRaw;
        p.parsed = AmqpProps.parse(propsRaw);
        if (bodySize == 0) {
            cur = null;
            completePublish(p);
        }
    }

    void onBody(byte[] data) {
        Pending p = cur;
        if (p == null || p.size < 0) {
            throw AmqpException.conn(505, "UNEXPECTED_FRAME - expected method frame, got non method frame instead").at(0, 0);
        }
        p.body.write(data, 0, data.length);
        if (p.body.size() > p.size) {
            cur = null;
            throw AmqpException.conn(501, "FRAME_ERROR - content body larger than the size in the content header");
        }
        if (p.body.size() == p.size) {
            cur = null;
            completePublish(p);
        }
    }

    // ------------------------------------------------------------------------------------------ methods

    /** Handles one method frame of this channel. */
    void onMethod(int cls, int mth, AmqpCodec.Reader r) {
        if (cur != null) {
            throw AmqpException.conn(505, "UNEXPECTED_FRAME - expected content header for class 60, got non content header frame instead").at(60, 40);
        }
        switch (cls * 1000 + mth) {
            case 20020 -> flow(r);
            case 20021 -> { /* flow-ok */ }
            case 40010 -> exchangeDeclare(r);
            case 40020 -> exchangeDelete(r);
            case 40030 -> exchangeBind(r, true);
            case 40040 -> exchangeBind(r, false);
            case 50010 -> queueDeclare(r);
            case 50020 -> queueBind(r, true);
            case 50050 -> queueBind(r, false);
            case 50030 -> queuePurge(r);
            case 50040 -> queueDelete(r);
            case 60010 -> basicQos(r);
            case 60020 -> basicConsume(r);
            case 60030 -> basicCancel(r);
            case 60040 -> basicPublish(r);
            case 60070 -> basicGet(r);
            case 60080 -> basicAck(r);
            case 60090 -> basicReject(r);
            case 60100 -> recover(r, false);
            case 60110 -> recover(r, true);
            case 60120 -> basicNack(r);
            case 85010 -> confirmSelect(r);
            case 90010 -> txSelect();
            case 90020 -> txCommit();
            case 90030 -> txRollback();
            default -> throw AmqpException.silentClose();
        }
    }

    private void reply(int cls, int mth, AmqpCodec.Writer w) {
        conn.sendMethod(no, cls, mth, w);
    }

    private static AmqpCodec.Writer w() {
        return new AmqpCodec.Writer();
    }

    private String vhost() {
        return conn.vhost;
    }

    private void flow(AmqpCodec.Reader r) {
        boolean active = (r.u8() & 1) != 0;
        if (!active) {
            throw AmqpException.notImplemented("active=false");
        }
        reply(20, 21, w().u8(1));
    }

    // ---- exchange

    private void exchangeDeclare(AmqpCodec.Reader r) {
        r.u16();
        String name = r.shortStr();
        String type = r.shortStr();
        int bits = r.u8();
        Map<String, Object> args = r.table();
        boolean noWait = (bits & 16) != 0;
        br.declareExchange(vhost(), name, type, (bits & 1) != 0, (bits & 2) != 0, (bits & 4) != 0, (bits & 8) != 0, args);
        if (!noWait) {
            reply(40, 11, w());
        }
    }

    private void exchangeDelete(AmqpCodec.Reader r) {
        r.u16();
        String name = r.shortStr();
        int bits = r.u8();
        br.deleteExchange(vhost(), name, (bits & 1) != 0);
        if ((bits & 2) == 0) {
            reply(40, 21, w());
        }
    }

    private void exchangeBind(AmqpCodec.Reader r, boolean bind) {
        r.u16();
        String dest = r.shortStr();
        String source = r.shortStr();
        String rk = r.shortStr();
        boolean noWait = (r.u8() & 1) != 0;
        Map<String, Object> args = r.table();
        if (bind) {
            br.bind(vhost(), source, dest, 'e', rk, args, conn.connId);
        } else {
            br.unbind(vhost(), source, dest, 'e', rk, args, conn.connId);
        }
        if (!noWait) {
            reply(40, bind ? 31 : 51, w());
        }
    }

    // ---- queue

    private String queueName(String n) {
        if (n.isEmpty() && lastQueue != null) {
            return lastQueue;
        }
        return n;
    }

    private void queueDeclare(AmqpCodec.Reader r) {
        r.u16();
        String name = r.shortStr();
        int bits = r.u8();
        Map<String, Object> args = r.table();
        boolean passive = (bits & 1) != 0;
        boolean durable = (bits & 2) != 0;
        boolean exclusive = (bits & 4) != 0;
        boolean autoDelete = (bits & 8) != 0;
        boolean noWait = (bits & 16) != 0;
        boolean serverNamed = false;
        if (name.isEmpty()) {
            if (!passive) {
                name = AmqpBroker.generatedName();
                serverNamed = true;
            }
        }
        QueueDef d = br.declareQueue(vhost(), name, passive, durable, exclusive, autoDelete, args, conn.connId, serverNamed);
        if (!passive) {
            lastQueue = name;
        }
        if (d.exclOwner() != null && d.exclOwner().equals(conn.connId)) {
            conn.exclusiveQueues.add(new AmqpBroker.QKey(vhost(), name));
        }
        if (!noWait) {
            long ready = br.store.ready(vhost(), name);
            reply(50, 11, w().shortStr(name).u32(ready).u32(br.consumerCount(vhost(), name)));
        }
    }

    private void queueBind(AmqpCodec.Reader r, boolean bind) {
        r.u16();
        String queue = queueName(r.shortStr());
        String ex = r.shortStr();
        String rk = r.shortStr();
        boolean noWait = false;
        if (bind) {
            noWait = (r.u8() & 1) != 0;
        }
        Map<String, Object> args = r.table();
        if (bind) {
            br.bind(vhost(), ex, queue, 'q', rk, args, conn.connId);
        } else {
            br.unbind(vhost(), ex, queue, 'q', rk, args, conn.connId);
        }
        if (!noWait) {
            reply(50, bind ? 21 : 51, w());
        }
    }

    private void queuePurge(AmqpCodec.Reader r) {
        r.u16();
        String queue = queueName(r.shortStr());
        boolean noWait = (r.u8() & 1) != 0;
        long n = br.purge(vhost(), queue, conn.connId);
        if (!noWait) {
            reply(50, 31, w().u32(n));
        }
    }

    private void queueDelete(AmqpCodec.Reader r) {
        r.u16();
        String queue = queueName(r.shortStr());
        int bits = r.u8();
        long n = br.deleteQueue(vhost(), queue, (bits & 1) != 0, (bits & 2) != 0, conn.connId);
        conn.exclusiveQueues.remove(new AmqpBroker.QKey(vhost(), queue));
        if ((bits & 4) == 0) {
            reply(50, 41, w().u32(n));
        }
    }

    // ---- basic

    private void basicQos(AmqpCodec.Reader r) {
        long size = r.u32();
        int count = r.u16();
        boolean global = (r.u8() & 1) != 0;
        if (size != 0) {
            throw AmqpException.notImplemented("prefetch_size!=0 (" + size + ")");
        }
        if (global) {
            prefetchGlobal = count;
        } else {
            prefetchPerConsumer = count;
        }
        reply(60, 11, w());
        signalAll();
    }

    private void signalAll() {
        Set<AmqpBroker.QKey> ks = new HashSet<>();
        synchronized (lock) {
            consumers.values().forEach(c -> ks.add(c.key));
        }
        ks.forEach(k -> br.signal(k.vhost(), k.name()));
    }

    private void basicConsume(AmqpCodec.Reader r) {
        r.u16();
        String queue = r.shortStr();
        String tag = r.shortStr();
        int bits = r.u8();
        Map<String, Object> args = r.table();
        boolean noAck = (bits & 2) != 0;
        boolean exclusive = (bits & 4) != 0;
        boolean noWait = (bits & 8) != 0;
        if (tag.isEmpty()) {
            tag = "amq.ctag-" + AmqpBroker.generatedName().substring(8);
        }
        synchronized (lock) {
            if (consumers.containsKey(tag) || tag.equals(replyTag)) {
                throw AmqpException.notAllowed("attempt to reuse consumer tag '" + tag + "'");
            }
        }
        if (queue.equals(REPLY_TO)) {
            if (!noAck) {
                throw AmqpException.precondition("reply consumer cannot acknowledge");
            }
            if (replyToken != null) {
                throw AmqpException.precondition("reply consumer already set");
            }
            replyToken = br.registerReplyTo(this);
            replyTag = tag;
            if (!noWait) {
                reply(60, 21, w().shortStr(tag));
            }
            return;
        }
        queue = queueName(queue);
        Consumer c = br.addConsumer(this, vhost(), queue, tag, noAck, exclusive, prefetchPerConsumer, args, conn.connId);
        synchronized (lock) {
            consumers.put(tag, c);
        }
        if (!noWait) {
            reply(60, 21, w().shortStr(tag));
        }
        c.active = true;
        br.signal(vhost(), queue);
    }

    private void basicCancel(AmqpCodec.Reader r) {
        String tag = r.shortStr();
        boolean noWait = (r.u8() & 1) != 0;
        Consumer c;
        synchronized (lock) {
            c = consumers.remove(tag);
            if (c != null) {
                c.cancelled = true;
            }
        }
        if (c != null) {
            br.removeConsumer(c);
        } else if (tag.equals(replyTag)) {
            br.unregisterReplyTo(replyToken);
            replyToken = null;
            replyTag = null;
        }
        if (!noWait) {
            reply(60, 31, w().shortStr(tag));
        }
    }

    private void basicGet(AmqpCodec.Reader r) {
        r.u16();
        String queue = queueName(r.shortStr());
        boolean noAck = (r.u8() & 1) != 0;
        QueueDef d = br.store.queue(vhost(), queue);
        if (d == null) {
            throw AmqpException.notFound("no queue '" + queue + "' in vhost '" + vhost() + "'");
        }
        AmqpBroker.checkExclusive(d, conn.connId);
        List<MsgRow> rows = br.store.claim(vhost(), queue, br.nodeId + "/g", 1);
        if (rows.isEmpty()) {
            reply(60, 72, w().shortStr(""));
            return;
        }
        MsgRow m = rows.get(0);
        br.expireQueue(vhost(), queue);          // whatever expired behind the message just taken is gone, like in RabbitMQ
        long left = br.store.ready(vhost(), queue);
        synchronized (deliverLock) {
            long tag;
            synchronized (lock) {
                tag = nextTag++;
                if (!noAck) {
                    unacked.put(tag, new Unack(m.seq(), m.vhost(), m.queue(), null, m));
                }
            }
            AmqpCodec.Writer args = w().i64(tag).u8(m.redelivered() ? 1 : 0).shortStr(m.exchange()).shortStr(m.rkey()).u32(left);
            conn.sendContent(no, 60, 71, args, m.props(), m.body());
        }
        if (noAck) {
            br.store.ack(vhost(), queue, List.of(m.seq()));
        }
    }

    /** Called by the dispatcher: records the delivery and writes it. */
    @Override
    public boolean deliver(Consumer c, MsgRow m) {
        synchronized (deliverLock) {
            long tag;
            synchronized (lock) {
                if (c.cancelled || closed || errored) {
                    return false;
                }
                tag = nextTag++;
                if (!c.noAck) {
                    unacked.put(tag, new Unack(m.seq(), m.vhost(), m.queue(), c, m));
                    c.unacked.incrementAndGet();
                }
            }
            AmqpCodec.Writer args = w().shortStr(c.tag).i64(tag).u8(m.redelivered() ? 1 : 0).shortStr(m.exchange()).shortStr(m.rkey());
            conn.sendContent(no, 60, 60, args, m.props(), m.body());
            return true;
        }
    }

    @Override
    public int available(Consumer c) {
        if (!c.active || c.cancelled || closed || errored || !flowActive) {
            return 0;
        }
        if (c.noAck) {
            return 1000;
        }
        int a = Integer.MAX_VALUE;
        if (c.prefetch > 0) {
            a = c.prefetch - c.unacked.get();
        }
        if (prefetchGlobal > 0) {
            // RabbitMQ (classic queues) applies the channel-wide limit to the unacknowledged messages of each queue
            synchronized (lock) {
                int n = 0;
                for (Unack u : unacked.values()) {
                    if (u.queue().equals(c.key.name()) && u.vhost().equals(c.key.vhost())) {
                        n++;
                    }
                }
                a = Math.min(a, prefetchGlobal - n);
            }
        }
        return Math.max(0, Math.min(a, 1000));
    }

    @Override
    public void serverCancel(Consumer c) {
        synchronized (lock) {
            consumers.remove(c.tag);
        }
        if (conn.clientCancelNotify) {
            conn.sendMethod(no, 60, 30, w().shortStr(c.tag).u8(1));
        }
    }

    private void basicAck(AmqpCodec.Reader r) {
        long tag = r.i64();
        boolean multiple = (r.u8() & 1) != 0;
        if (tx) {
            txAcks.add(new TxAck(tag, multiple, false, false, false));
            return;
        }
        settle(tag, multiple, false, false, false);
    }

    private void basicReject(AmqpCodec.Reader r) {
        long tag = r.i64();
        boolean requeue = (r.u8() & 1) != 0;
        if (tx) {
            txAcks.add(new TxAck(tag, false, true, requeue, true));
            return;
        }
        settle(tag, false, true, requeue, true);
    }

    private void basicNack(AmqpCodec.Reader r) {
        long tag = r.i64();
        int bits = r.u8();
        if (tx) {
            txAcks.add(new TxAck(tag, (bits & 1) != 0, true, (bits & 2) != 0, false));
            return;
        }
        settle(tag, (bits & 1) != 0, true, (bits & 2) != 0, false);
    }

    private void settle(long tag, boolean multiple, boolean negative, boolean requeue, boolean reject) {
        List<Unack> done = new ArrayList<>();
        synchronized (lock) {
            if (multiple) {
                if (tag != 0 && tag >= nextTag) {
                    throw AmqpException.precondition("unknown delivery tag " + tag);
                }
                var it = unacked.entrySet().iterator();
                while (it.hasNext()) {
                    var e = it.next();
                    if (tag == 0 || e.getKey() <= tag) {
                        done.add(e.getValue());
                        it.remove();
                    }
                }
            } else {
                Unack u = unacked.remove(tag);
                if (u == null) {
                    throw AmqpException.precondition("unknown delivery tag " + tag);
                }
                done.add(u);
            }
            for (Unack u : done) {
                if (u.consumer() != null) {
                    u.consumer().unacked.decrementAndGet();
                }
            }
        }
        finish(done, negative, requeue, reject);
    }

    private void finish(List<Unack> done, boolean negative, boolean requeue, boolean reject) {
        finishQuiet(done, negative, requeue, reject).forEach(k -> br.signal(k.vhost(), k.name()));
    }

    /** Applies the acknowledgement to the store and returns the queues to wake up (the caller signals them once its reply is written). */
    private Set<AmqpBroker.QKey> finishQuiet(List<Unack> done, boolean negative, boolean requeue, boolean reject) {
        Map<AmqpBroker.QKey, List<Unack>> byQ = new LinkedHashMap<>();
        for (Unack u : done) {
            byQ.computeIfAbsent(new AmqpBroker.QKey(u.vhost(), u.queue()), k -> new ArrayList<>()).add(u);
        }
        for (var e : byQ.entrySet()) {
            AmqpBroker.QKey k = e.getKey();
            List<Long> seqs = new ArrayList<>();
            e.getValue().forEach(u -> seqs.add(u.seq()));
            if (negative && requeue) {
                br.store.requeue(k.vhost(), k.name(), seqs);
            } else {
                if (negative) {
                    QueueDef d = br.qdef(k.vhost(), k.name());
                    if (d != null && d.dlx() != null) {
                        List<MsgRow> rows = new ArrayList<>();
                        e.getValue().forEach(u -> rows.add(u.row()));
                        br.deadLetter(d, rows, "rejected", reject);
                    }
                }
                br.store.ack(k.vhost(), k.name(), seqs);
            }
        }
        return byQ.keySet();
    }

    private void recover(AmqpCodec.Reader r, boolean withReply) {
        if ((r.u8() & 1) == 0) {
            throw AmqpException.notImplemented("requeue=false");
        }
        List<Unack> all;
        synchronized (lock) {
            all = new ArrayList<>(unacked.values());
            unacked.clear();
            for (Unack u : all) {
                if (u.consumer() != null) {
                    u.consumer().unacked.decrementAndGet();
                }
            }
        }
        Set<AmqpBroker.QKey> wake = finishQuiet(all, true, true, false);
        if (withReply) {
            reply(60, 111, w());
        }
        wake.forEach(k -> br.signal(k.vhost(), k.name()));
    }

    // ---- publish

    private void basicPublish(AmqpCodec.Reader r) {
        r.u16();
        Pending p = new Pending();
        p.exchange = r.shortStr();
        p.rkey = r.shortStr();
        int bits = r.u8();
        p.mandatory = (bits & 1) != 0;
        if ((bits & 2) != 0) {
            throw AmqpException.notImplemented("immediate=true");
        }
        cur = p;
    }

    private void completePublish(Pending p) {
        if (tx) {
            txPublishes.add(p);
            return;
        }
        doPublish(p);
    }

    private void doPublish(Pending p) {
        long seqNo = confirm ? ++publishSeq : 0;
        byte[] body = p.body.toByteArray();
        AmqpProps props = p.parsed;
        byte[] propsRaw = p.props;
        if (props.userId != null && !props.userId.equals(conn.user)) {
            throw AmqpException.precondition("user_id property set to '" + props.userId + "' but authenticated user was '" + conn.user + "'");
        }
        if (AmqpChannel.REPLY_TO.equals(props.replyTo)) {
            if (replyToken == null) {
                throw AmqpException.precondition("fast reply consumer does not exist");
            }
            props.replyTo = REPLY_TO + "." + replyToken;
            propsRaw = props.toBytes();
        }
        if (p.exchange.isEmpty() && p.rkey.startsWith(REPLY_TO + ".")) {
            AmqpChannel target = br.replyChannel(p.rkey.substring(REPLY_TO.length() + 1));
            if (target != null && target.replyTag != null && !target.closed) {
                target.deliverDirect(p.rkey, propsRaw, body);
                if (confirm) {
                    reply(60, 80, w().i64(seqNo).u8(0));
                }
                return;
            }
        }
        AmqpBroker.PubResult res = br.publish(vhost(), p.exchange, p.rkey, propsRaw, props, body, null);
        if (res.routed() == 0 && !res.rejected() && p.mandatory) {
            AmqpCodec.Writer a = w().u16(312).shortStr("NO_ROUTE").shortStr(p.exchange).shortStr(p.rkey);
            conn.sendContent(no, 60, 50, a, propsRaw, body);
        }
        if (confirm) {
            if (res.rejected()) {
                reply(60, 120, w().i64(seqNo).u8(0));
            } else {
                reply(60, 80, w().i64(seqNo).u8(0));
            }
        }
    }

    private void deliverDirect(String rkey, byte[] props, byte[] body) {
        synchronized (deliverLock) {
            long tag;
            synchronized (lock) {
                tag = nextTag++;
            }
            AmqpCodec.Writer a = w().shortStr(replyTag).i64(tag).u8(0).shortStr("").shortStr(rkey);
            conn.sendContent(no, 60, 60, a, props, body);
        }
    }

    // ---- confirm / tx

    private void confirmSelect(AmqpCodec.Reader r) {
        boolean noWait = (r.u8() & 1) != 0;
        if (tx) {
            throw AmqpException.precondition("cannot switch from tx to confirm mode");
        }
        confirm = true;
        if (!noWait) {
            reply(85, 11, w());
        }
    }

    private void txSelect() {
        if (confirm) {
            throw AmqpException.precondition("cannot switch from confirm to tx mode");
        }
        tx = true;
        reply(90, 11, w());
    }

    private void txCommit() {
        if (!tx) {
            throw AmqpException.precondition("channel is not transactional");
        }
        List<Pending> ps = new ArrayList<>(txPublishes);
        List<TxAck> as = new ArrayList<>(txAcks);
        txPublishes.clear();
        txAcks.clear();
        for (Pending p : ps) {
            doPublish(p);
        }
        for (TxAck a : as) {
            settle(a.tag(), a.multiple(), a.nack(), a.requeue(), a.reject());
        }
        reply(90, 21, w());
    }

    private void txRollback() {
        if (!tx) {
            throw AmqpException.precondition("channel is not transactional");
        }
        txPublishes.clear();
        txAcks.clear();
        reply(90, 31, w());
    }

    // ------------------------------------------------------------------------------------------ lifecycle

    /** Cancels the consumers and requeues everything unacknowledged (channel close, channel error, connection loss). */
    void cleanup() {
        if (closed) {
            return;
        }
        List<Consumer> cs;
        List<Unack> rest;
        synchronized (lock) {
            closed = true;
            cs = new ArrayList<>(consumers.values());
            consumers.clear();
            rest = new ArrayList<>(unacked.values());
            unacked.clear();
        }
        for (Consumer c : cs) {
            c.cancelled = true;
            br.removeConsumer(c);
        }
        if (replyToken != null) {
            br.unregisterReplyTo(replyToken);
            replyToken = null;
        }
        if (!rest.isEmpty()) {
            finish(rest, true, true, false);
        }
    }
}
