package com.sayonora.warp.pubsubwire;

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingPull. A stream never owns a database connection or a thread: one {@link Pump} per subscription that has open
 * streams wakes up on publish (or every 200 ms while idle), leases messages with one short query for all the streams that have
 * flow-control room, and writes them to the streams. Acks and deadline changes from the client are handled in the gRPC callback
 * with one short statement each. Flow control is the client's {@code max_outstanding_messages}/{@code max_outstanding_bytes}
 * against what this stream has been sent and not yet acknowledged or seen expire.
 */
final class PsStreams {

    private static final Logger log = LoggerFactory.getLogger(PsStreams.class);
    private static final int MAX_RESPONSE_BYTES = 3_500_000;

    private final PsService svc;
    private final ScheduledExecutorService pool = Executors.newScheduledThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()), r -> {
                Thread t = new Thread(r, "pubsubwire-stream");
                t.setDaemon(true);
                return t;
            });
    private final Map<String, Pump> pumps = new ConcurrentHashMap<>();

    PsStreams(PsService svc) {
        this.svc = svc;
    }

    void stop() {
        pool.shutdownNow();
    }

    int openStreams() {
        int n = 0;
        for (Pump p : pumps.values()) {
            n += p.streams.size();
        }
        return n;
    }

    StreamObserver<StreamingPullRequest> open(StreamObserver<StreamingPullResponse> out) {
        return new Stream((ServerCallStreamObserver<StreamingPullResponse>) out);
    }

    // ------------------------------------------------------------------------------------------ pump

    private final class Pump {
        final String name;
        final List<Stream> streams = new CopyOnWriteArrayList<>();
        final AtomicBoolean pending = new AtomicBoolean();
        final AtomicBoolean idle = new AtomicBoolean();
        final Runnable waker = this::kick;
        volatile PsService.Sub sub;

        Pump(String name) {
            this.name = name;
            svc.wakeups.listen(name, waker);
        }

        void kick() {
            if (pending.compareAndSet(false, true)) {
                try {
                    pool.execute(this::run);
                } catch (RuntimeException e) {
                    pending.set(false);
                }
            }
        }

        private void scheduleIdle() {
            if (idle.compareAndSet(false, true)) {
                try {
                    pool.schedule(() -> {
                        idle.set(false);
                        kick();
                    }, 200, TimeUnit.MILLISECONDS);
                } catch (RuntimeException e) {
                    idle.set(false);
                }
            }
        }

        void run() {
            pending.set(false);
            int delivered = 0;
            try {
                if (streams.isEmpty()) {
                    return;
                }
                delivered = pumpOnce();
            } catch (PsException e) {
                for (Stream s : streams) {
                    s.fail(e);
                }
            } catch (RuntimeException e) {
                log.debug("pubsubwire pump {} failed: {}", name, e.toString());
            } finally {
                if (streams.isEmpty()) {
                    synchronized (PsStreams.this) {
                        if (streams.isEmpty()) {
                            pumps.remove(name, this);
                            svc.wakeups.unlisten(name, waker);
                        }
                    }
                } else if (delivered > 0) {
                    kick();
                } else {
                    scheduleIdle();
                }
            }
        }

        private int pumpOnce() {
            PsService.Sub s = svc.sub(name);
            sub = s;
            Map<Integer, List<Stream>> byDeadline = new LinkedHashMap<>();
            for (Stream st : streams) {
                if (st.sub != null && st.capacity() > 0) {
                    byDeadline.computeIfAbsent(st.ackDeadlineSeconds, k -> new ArrayList<>()).add(st);
                }
            }
            int total = 0;
            for (var e : byDeadline.entrySet()) {
                List<Stream> group = e.getValue();
                int cap = 0;
                for (Stream st : group) {
                    cap += st.capacity();
                }
                List<PsStore.Delivered> got = svc.lease(s, Math.min(cap, 1000), e.getKey() * 1000L);
                if (got.isEmpty()) {
                    continue;
                }
                total += got.size();
                Map<Stream, List<PsStore.Delivered>> plan = new HashMap<>();
                int gi = 0;
                for (PsStore.Delivered d : got) {
                    // round robin over the streams that still have room
                    Stream target = null;
                    for (int tries = 0; tries < group.size(); tries++) {
                        Stream cand = group.get(gi++ % group.size());
                        int planned = plan.getOrDefault(cand, List.of()).size();
                        if (cand.capacity() - planned > 0) {
                            target = cand;
                            break;
                        }
                    }
                    if (target == null) {
                        target = group.get(0);
                    }
                    plan.computeIfAbsent(target, k -> new ArrayList<>()).add(d);
                }
                for (var pe : plan.entrySet()) {
                    pe.getKey().deliver(pe.getValue(), s);
                }
            }
            return total;
        }
    }

    // ------------------------------------------------------------------------------------------ stream

    private final class Stream implements StreamObserver<StreamingPullRequest> {
        final ServerCallStreamObserver<StreamingPullResponse> out;
        volatile PsService.Sub sub;
        volatile int ackDeadlineSeconds = 10;
        volatile long maxMessages = 1000;
        volatile long maxBytes;
        Pump pump;
        final Map<String, long[]> outstanding = new HashMap<>(); // ack id -> {expiryMs, bytes}
        long outstandingBytes;
        volatile boolean closed;

        Stream(ServerCallStreamObserver<StreamingPullResponse> out) {
            this.out = out;
            out.setOnCancelHandler(this::detach);
            out.setOnReadyHandler(() -> {
                if (pump != null) {
                    pump.kick();
                }
            });
        }

        synchronized int capacity() {
            if (closed || !out.isReady()) {
                return 0;
            }
            long now = System.currentTimeMillis();
            var it = outstanding.entrySet().iterator();
            while (it.hasNext()) {
                var e = it.next();
                if (e.getValue()[0] < now) {
                    outstandingBytes -= e.getValue()[1];
                    it.remove();
                }
            }
            long room = maxMessages <= 0 ? 1000 : maxMessages - outstanding.size();
            if (maxBytes > 0 && outstandingBytes >= maxBytes) {
                room = 0;
            }
            return (int) Math.max(0, Math.min(room, 1000));
        }

        void deliver(List<PsStore.Delivered> ds, PsService.Sub s) {
            List<ReceivedMessage> batch = new ArrayList<>();
            int size = 0;
            long expiry = System.currentTimeMillis() + ackDeadlineSeconds * 1000L;
            for (PsStore.Delivered d : ds) {
                ReceivedMessage rm = PsService.received(d, s);
                int sz = rm.getSerializedSize();
                if (!batch.isEmpty() && size + sz > MAX_RESPONSE_BYTES) {
                    send(StreamingPullResponse.newBuilder().addAllReceivedMessages(batch).build());
                    batch.clear();
                    size = 0;
                }
                batch.add(rm);
                size += sz;
                synchronized (this) {
                    outstanding.put(rm.getAckId(), new long[] {expiry, sz});
                    outstandingBytes += sz;
                }
            }
            if (!batch.isEmpty()) {
                send(StreamingPullResponse.newBuilder().addAllReceivedMessages(batch).build());
            }
        }

        void send(StreamingPullResponse r) {
            synchronized (out) {
                if (!closed) {
                    try {
                        out.onNext(r);
                    } catch (RuntimeException e) {
                        closed = true;
                    }
                }
            }
        }

        void fail(PsException e) {
            synchronized (out) {
                if (!closed) {
                    closed = true;
                    try {
                        out.onError(e.toGrpc());
                    } catch (RuntimeException ignored) {
                        // already cancelled
                    }
                }
            }
            detach();
        }

        void detach() {
            closed = true;
            Pump p = pump;
            if (p != null) {
                p.streams.remove(this);
                if (p.streams.isEmpty()) {
                    p.kick();
                }
            }
        }

        @Override
        public void onNext(StreamingPullRequest r) {
            try {
                if (sub == null) {
                    open(r);
                } else if (!r.getSubscription().isEmpty() && !r.getSubscription().equals(sub.name.full())) {
                    throw PsException.invalid("The subscription cannot change on an open stream.");
                }
                handle(r);
            } catch (PsException e) {
                fail(e);
            } catch (RuntimeException e) {
                log.warn("pubsubwire streaming pull failed", e);
                fail(PsException.internal("Internal error"));
            }
        }

        private void open(StreamingPullRequest r) {
            PsService.Sub s = svc.sub(r.getSubscription());
            if (s.row.detached()) {
                throw PsException.precondition("Subscription " + s.name.full() + " is detached.");
            }
            PsService.requirePullable(s);
            int d = r.getStreamAckDeadlineSeconds();
            if (d != 0 && (d < 10 || d > 600)) {
                throw PsException.invalid("Invalid stream_ack_deadline_seconds: " + d + ". It must be between 10 and 600 seconds.");
            }
            ackDeadlineSeconds = d == 0 ? s.ackDeadline : d;
            maxMessages = r.getMaxOutstandingMessages();
            maxBytes = r.getMaxOutstandingBytes();
            if (openStreams() >= svc.cfg.maxStreamsPerSubscription * 100) {
                throw new PsException(io.grpc.Status.Code.RESOURCE_EXHAUSTED, "Too many open streams.");
            }
            send(StreamingPullResponse.newBuilder().setSubscriptionProperties(
                    StreamingPullResponse.SubscriptionProperties.newBuilder()
                            .setExactlyOnceDeliveryEnabled(s.exactlyOnce).setMessageOrderingEnabled(s.ordered)).build());
            synchronized (PsStreams.this) {
                Pump p = pumps.computeIfAbsent(s.name.full(), Pump::new);
                pump = p;
                sub = s;
                p.streams.add(this);
            }
            pump.kick();
        }

        private void handle(StreamingPullRequest r) {
            PsService.Sub s = sub;
            if (r.getStreamAckDeadlineSeconds() != 0 && r.getStreamAckDeadlineSeconds() >= 10 && r.getStreamAckDeadlineSeconds() <= 600) {
                ackDeadlineSeconds = r.getStreamAckDeadlineSeconds();
            }
            StreamingPullResponse.Builder resp = StreamingPullResponse.newBuilder();
            boolean confirm = false;
            if (r.getAckIdsCount() > 0) {
                StreamingPullResponse.AcknowledgeConfirmation.Builder c = StreamingPullResponse.AcknowledgeConfirmation.newBuilder();
                try {
                    Map<String, PsStore.AckResult> res = svc.ackIds(s, r.getAckIdsList());
                    res.forEach((id, v) -> {
                        if (v == PsStore.AckResult.OK) {
                            c.addAckIds(id);
                        } else {
                            c.addInvalidAckIds(id);
                        }
                    });
                } catch (PsException e) {
                    r.getAckIdsList().forEach(c::addTemporaryFailedAckIds);
                }
                forget(r.getAckIdsList());
                if (s.exactlyOnce) {
                    resp.setAcknowledgeConfirmation(c);
                    confirm = true;
                }
            }
            if (r.getModifyDeadlineAckIdsCount() > 0) {
                StreamingPullResponse.ModifyAckDeadlineConfirmation.Builder c =
                        StreamingPullResponse.ModifyAckDeadlineConfirmation.newBuilder();
                Map<Integer, List<String>> bySeconds = new LinkedHashMap<>();
                for (int i = 0; i < r.getModifyDeadlineAckIdsCount(); i++) {
                    int secs = i < r.getModifyDeadlineSecondsCount() ? r.getModifyDeadlineSeconds(i) : ackDeadlineSeconds;
                    bySeconds.computeIfAbsent(Math.max(0, Math.min(600, secs)), k -> new ArrayList<>()).add(r.getModifyDeadlineAckIds(i));
                }
                for (var e : bySeconds.entrySet()) {
                    try {
                        Map<String, PsStore.AckResult> res = svc.modackIds(s, e.getValue(), e.getKey());
                        res.forEach((id, v) -> {
                            if (v == PsStore.AckResult.OK) {
                                c.addAckIds(id);
                            } else {
                                c.addInvalidAckIds(id);
                            }
                        });
                    } catch (PsException ex) {
                        e.getValue().forEach(c::addTemporaryFailedAckIds);
                    }
                    if (e.getKey() == 0) {
                        forget(e.getValue());
                    } else {
                        extend(e.getValue(), e.getKey());
                    }
                }
                if (s.exactlyOnce) {
                    resp.setModifyAckDeadlineConfirmation(c);
                    confirm = true;
                }
            }
            if (confirm) {
                send(resp.build());
            }
            if (r.getAckIdsCount() > 0 || r.getModifyDeadlineAckIdsCount() > 0) {
                pump.kick();
            }
        }

        private synchronized void forget(List<String> ids) {
            for (String id : ids) {
                long[] v = outstanding.remove(id);
                if (v != null) {
                    outstandingBytes -= v[1];
                }
            }
        }

        private synchronized void extend(List<String> ids, int seconds) {
            long exp = System.currentTimeMillis() + seconds * 1000L;
            for (String id : ids) {
                long[] v = outstanding.get(id);
                if (v != null) {
                    v[0] = exp;
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            detach();
        }

        @Override
        public void onCompleted() {
            synchronized (out) {
                if (!closed) {
                    closed = true;
                    try {
                        out.onCompleted();
                    } catch (RuntimeException ignored) {
                        // already closed
                    }
                }
            }
            detach();
        }
    }
}
