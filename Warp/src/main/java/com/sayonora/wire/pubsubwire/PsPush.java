package com.sayonora.wire.pubsubwire;

import com.google.gson.JsonObject;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Push delivery. A scanner lists the push subscriptions and starts one drain per subscription at a time; a drain leases messages
 * (like a Pull would), POSTs each as the documented JSON envelope ({@code {"message":{...},"subscription":"..."}}) or, with
 * {@code no_wrapper}, as the raw data, and acknowledges on 2xx (also 102). Anything else, a timeout or a connection error nacks:
 * with the subscription's retry policy the message is redelivered after its backoff, otherwise after an exponential
 * backoff of 1 s doubling to 60 s. No pooled database connection is held while an HTTP request is in flight.
 * OIDC token settings are stored and returned but no token is minted (there is no Google identity here).
 */
final class PsPush {

    private static final Logger log = LoggerFactory.getLogger(PsPush.class);
    private static final long DEFAULT_MIN_MS = 1000;
    private static final long DEFAULT_MAX_MS = 60_000;

    private final PsService svc;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build();
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pubsubwire-push-scan");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService workers = Executors.newFixedThreadPool(16, r -> {
        Thread t = new Thread(r, "pubsubwire-push");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, AtomicBoolean> busy = new ConcurrentHashMap<>();

    PsPush(PsService svc) {
        this.svc = svc;
    }

    void start() {
        scanner.scheduleWithFixedDelay(this::scan, 500, 250, TimeUnit.MILLISECONDS);
    }

    void stop() {
        scanner.shutdownNow();
        workers.shutdownNow();
    }

    private void scan() {
        try {
            if (!svc.store.shards.available()) {
                return;
            }
            for (PsStore.SubRow row : svc.store.pushSubs()) {
                AtomicBoolean b = busy.computeIfAbsent(row.name(), k -> new AtomicBoolean());
                if (b.compareAndSet(false, true)) {
                    workers.execute(() -> {
                        try {
                            drain(row.name());
                        } catch (RuntimeException e) {
                            log.debug("pubsubwire push drain of {} failed: {}", row.name(), e.toString());
                        } finally {
                            b.set(false);
                        }
                    });
                }
            }
        } catch (RuntimeException e) {
            log.debug("pubsubwire push scan failed: {}", e.toString());
        }
    }

    private void drain(String name) {
        for (int round = 0; round < 20; round++) {
            PsService.Sub s = svc.sub(name);
            if (!s.push) {
                return;
            }
            long leaseMs = Math.max(10, s.ackDeadline) * 1000L;
            List<PsStore.Delivered> got = svc.lease(s, 100, leaseMs);
            if (got.isEmpty()) {
                return;
            }
            List<CompletableFuture<Boolean>> fs = new ArrayList<>();
            for (PsStore.Delivered d : got) {
                fs.add(post(s, d));
            }
            List<String> acks = new ArrayList<>();
            List<String> nacks = new ArrayList<>();
            int maxAttempt = 1;
            for (int i = 0; i < got.size(); i++) {
                boolean ok;
                try {
                    ok = fs.get(i).get(Math.min(leaseMs, 30_000) + 2000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    ok = false;
                }
                String id = PsAckId.encode(got.get(i).seq(), got.get(i).attempt(), got.get(i).token());
                (ok ? acks : nacks).add(id);
                maxAttempt = Math.max(maxAttempt, got.get(i).attempt());
            }
            if (!acks.isEmpty()) {
                svc.ackIds(s, acks);
            }
            if (!nacks.isEmpty()) {
                if (s.retryMin >= 0) {
                    svc.modackIds(s, nacks, 0);
                } else {
                    for (int i = 0; i < got.size(); i++) {
                        String id = PsAckId.encode(got.get(i).seq(), got.get(i).attempt(), got.get(i).token());
                        if (nacks.contains(id)) {
                            svc.modackMillis(s, List.of(id), PsBackoff.delayMillis(DEFAULT_MIN_MS, DEFAULT_MAX_MS, got.get(i).attempt()));
                        }
                    }
                }
            }
        }
    }

    static String envelope(PsService.Sub s, PsStore.Delivered d) {
        PubsubMessage m = d.msg();
        JsonObject msg = new JsonObject();
        msg.addProperty("data", Base64.getEncoder().encodeToString(m.getData().toByteArray()));
        JsonObject attrs = new JsonObject();
        m.getAttributesMap().forEach(attrs::addProperty);
        msg.add("attributes", attrs);
        String time = DateTimeFormatter.ISO_INSTANT.format(PsStore.instant(m.getPublishTime()));
        msg.addProperty("messageId", m.getMessageId());
        msg.addProperty("message_id", m.getMessageId());
        msg.addProperty("publishTime", time);
        msg.addProperty("publish_time", time);
        if (!m.getOrderingKey().isEmpty()) {
            msg.addProperty("orderingKey", m.getOrderingKey());
        }
        JsonObject top = new JsonObject();
        top.add("message", msg);
        top.addProperty("subscription", s.name.full());
        if (s.maxAttempts > 0) {
            top.addProperty("deliveryAttempt", d.attempt());
        }
        return top.toString();
    }

    private CompletableFuture<Boolean> post(PsService.Sub s, PsStore.Delivered d) {
        try {
            PushConfig pc = s.doc.getPushConfig();
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(pc.getPushEndpoint()))
                    .timeout(java.time.Duration.ofSeconds(Math.min(Math.max(10, s.ackDeadline), 30)))
                    .header("User-Agent", "CloudPubSub-Google");
            if (pc.hasNoWrapper()) {
                b.header("Content-Type", "application/octet-stream");
                if (pc.getNoWrapper().getWriteMetadata()) {
                    b.header("X-Goog-Pubsub-Subscription-Name", s.name.full());
                    b.header("X-Goog-Pubsub-Message-Id", d.msg().getMessageId());
                    b.header("X-Goog-Pubsub-Publish-Time", DateTimeFormatter.ISO_INSTANT.format(PsStore.instant(d.msg().getPublishTime())));
                    if (!d.msg().getOrderingKey().isEmpty()) {
                        b.header("X-Goog-Pubsub-Ordering-Key", d.msg().getOrderingKey());
                    }
                    d.msg().getAttributesMap().forEach((k, v) -> {
                        try {
                            b.header(k, v);
                        } catch (IllegalArgumentException ignored) {
                            // not a legal header name/value
                        }
                    });
                }
                b.POST(HttpRequest.BodyPublishers.ofByteArray(d.msg().getData().toByteArray()));
            } else {
                b.header("Content-Type", "application/json");
                b.POST(HttpRequest.BodyPublishers.ofString(envelope(s, d)));
            }
            return http.sendAsync(b.build(), HttpResponse.BodyHandlers.discarding()).handle((r, e) -> {
                if (e != null) {
                    return false;
                }
                int c = r.statusCode();
                return c == 200 || c == 201 || c == 202 || c == 204 || c == 102;
            });
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(false);
        }
    }
}
