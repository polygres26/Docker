package com.sayonora.wire.pubsubwire;

import com.sayonora.wire.pubsubwire.iam.GetIamPolicyRequest;
import com.sayonora.wire.pubsubwire.iam.Policy;
import com.sayonora.wire.pubsubwire.iam.SetIamPolicyRequest;
import com.sayonora.wire.pubsubwire.iam.TestIamPermissionsRequest;
import com.sayonora.wire.pubsubwire.iam.TestIamPermissionsResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.pubsub.v1.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pub/Sub API semantics, independent of the transport: gRPC ({@link PsGrpc}) and REST ({@link PsRest}) both call the methods
 * here. Validation rules and error texts follow Google's service. See {@link PsStore} for storage.
 */
final class PsService {

    private static final Logger log = LoggerFactory.getLogger(PsService.class);

    static final int MAX_PUBLISH_MESSAGES = 1000;
    static final long MAX_PUBLISH_BYTES = 10_000_000L;
    static final long MAX_MESSAGE_BYTES = 10_000_000L;
    static final String DELETED_TOPIC = "_deleted-topic_";
    private static final long DEFAULT_RETENTION_S = 604_800L;
    private static final long DEFAULT_EXPIRATION_S = 2_678_400L;

    final PsStore store;
    final PsConfig cfg;
    final PsWakeups wakeups = new PsWakeups();
    private final Map<String, PsFilter> filters = new ConcurrentHashMap<>();
    private final Map<String, Sub> subCache = new ConcurrentHashMap<>();
    final AtomicLong published = new AtomicLong();

    PsService(PsStore store, PsConfig cfg) {
        this.store = store;
        this.cfg = cfg;
    }

    // ------------------------------------------------------------------------------------------ subscription view

    /** A subscription with the derived values the hot paths need. */
    static final class Sub {
        PsStore.SubRow row;
        Subscription doc;
        PsNames.Name name;
        String host;
        boolean ordered;
        boolean exactlyOnce;
        boolean retain;
        boolean push;
        long retryMin = -1;
        long retryMax = -1;
        int maxAttempts;
        String dlq;
        int ackDeadline;
        long retentionSec;
        long loadedAt;
    }

    private Sub load(PsNames.Name n, PsStore.SubRow row) {
        Sub s = new Sub();
        s.row = row;
        s.doc = row.doc();
        s.name = n;
        s.host = store.shards.owner(n.project(), n.full());
        s.ordered = s.doc.getEnableMessageOrdering();
        s.exactlyOnce = s.doc.getEnableExactlyOnceDelivery();
        s.retain = s.doc.getRetainAckedMessages();
        s.push = !s.doc.getPushConfig().getPushEndpoint().isEmpty();
        if (s.doc.hasRetryPolicy()) {
            RetryPolicy r = s.doc.getRetryPolicy();
            s.retryMin = r.hasMinimumBackoff() ? ms(r.getMinimumBackoff()) : PsBackoff.DEFAULT_MIN_MS;
            s.retryMax = r.hasMaximumBackoff() ? ms(r.getMaximumBackoff()) : PsBackoff.DEFAULT_MAX_MS;
        }
        if (s.doc.hasDeadLetterPolicy() && !s.doc.getDeadLetterPolicy().getDeadLetterTopic().isEmpty()) {
            s.dlq = s.doc.getDeadLetterPolicy().getDeadLetterTopic();
            s.maxAttempts = s.doc.getDeadLetterPolicy().getMaxDeliveryAttempts() == 0 ? 5 : s.doc.getDeadLetterPolicy().getMaxDeliveryAttempts();
        }
        s.ackDeadline = s.doc.getAckDeadlineSeconds() == 0 ? 10 : s.doc.getAckDeadlineSeconds();
        s.retentionSec = s.doc.hasMessageRetentionDuration() ? s.doc.getMessageRetentionDuration().getSeconds() : DEFAULT_RETENTION_S;
        s.loadedAt = System.currentTimeMillis();
        return s;
    }

    /** Finds a subscription (cached for one second; local changes invalidate). NOT_FOUND when absent. */
    Sub sub(String fullName) {
        PsNames.Name n = PsNames.parseExisting("subscriptions", fullName);
        Sub c = subCache.get(n.full());
        if (c != null && System.currentTimeMillis() - c.loadedAt < 1000) {
            return c;
        }
        PsStore.SubRow row = store.getSub(n.project(), n.full());
        if (row == null) {
            subCache.remove(n.full());
            throw PsException.notFound(n.id());
        }
        Sub s = load(n, row);
        subCache.put(n.full(), s);
        return s;
    }

    private static String dur(long ms) {
        return ms % 1000 == 0 ? (ms / 1000) + "s" : (ms / 1000.0) + "s";
    }

    private static long ms(Duration d) {
        return d.getSeconds() * 1000L + d.getNanos() / 1_000_000L;
    }

    static Duration secs(long s) {
        return Duration.newBuilder().setSeconds(s).build();
    }

    // ------------------------------------------------------------------------------------------ paging

    private static int pageSize(int requested) {
        if (requested < 0) {
            throw PsException.invalid("The value for page_size is too small. You passed " + requested
                    + " in the request, but the minimum value is 0.");
        }
        return requested == 0 ? 100 : Math.min(requested, 1000);
    }

    private static String after(String token) {
        if (token == null || token.isEmpty()) {
            return "";
        }
        try {
            return new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw PsException.invalid("Invalid page token.");
        }
    }

    private static String token(String lastName) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(lastName.getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------------------------------ topics

    private static final long TOPIC_MIN_RETENTION_S = 600;
    private static final long TOPIC_MAX_RETENTION_S = 31L * 86400;

    private void validateTopic(Topic t, String project) {
        if (t.hasMessageRetentionDuration()) {
            long s = t.getMessageRetentionDuration().getSeconds();
            if (s < TOPIC_MIN_RETENTION_S || s > TOPIC_MAX_RETENTION_S) {
                throw PsException.invalid("message_retention_duration must be between 10 minutes and 31 days.");
            }
        }
        if (t.hasSchemaSettings() && !t.getSchemaSettings().getSchema().isEmpty()
                && !t.getSchemaSettings().getSchema().equals("_deleted-schema_")) {
            PsNames.Name sn = PsNames.parseExisting("schemas", t.getSchemaSettings().getSchema());
            if (store.getSchema(sn.project(), sn.full()) == null) {
                throw PsException.notFound(sn.id());
            }
        }
        for (var e : t.getLabelsMap().entrySet()) {
            if (e.getKey().isEmpty() || e.getKey().length() > 63 || e.getValue().length() > 63) {
                throw PsException.invalid("Invalid label: keys and values are limited to 63 characters and keys cannot be empty.");
            }
        }
    }

    Topic createTopic(Topic in) {
        PsNames.Name n = PsNames.parse("topics", in.getName());
        Topic t = in.toBuilder().clearTags().clearState().build();
        validateTopic(t, n.project());
        try {
            store.insertTopic(n.project(), t);
        } catch (PsException e) {
            if (PsShards.isUniqueViolation(e)) {
                throw PsException.exists(n.id());
            }
            throw e;
        }
        return t;
    }

    Topic getTopic(GetTopicRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic());
        Topic t = store.getTopic(n.project(), n.full());
        if (t == null) {
            throw PsException.notFound(n.id());
        }
        return t;
    }

    private static String badMask(String rpc, String msg, String field, String type) {
        return "Invalid update_mask provided in the " + rpc + ": " + field + " is not a known " + type
                + " field. Note that field paths must be of the form 'schema_settings' rather than 'schemaSetings'.";
    }

    private static <M extends com.google.protobuf.Message> void checkMask(String rpc, FieldMask mask, M proto, java.util.Set<String> immutable) {
        if (mask == null || mask.getPathsCount() == 0) {
            throw PsException.invalid("The update_mask in the " + rpc + " must be set, and must contain a non-empty paths list.");
        }
        Descriptors.Descriptor d = proto.getDescriptorForType();
        for (String p : mask.getPathsList()) {
            String top = p.split("\\.")[0];
            if (d.findFieldByName(top) == null || immutable.contains(top)) {
                throw PsException.invalid(badMask(rpc, p, p, d.getName()));
            }
        }
    }

    Topic updateTopic(UpdateTopicRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic().getName());
        checkMask("UpdateTopicRequest", r.getUpdateMask(), r.getTopic(), java.util.Set.of("name", "state", "tags"));
        Topic cur = store.getTopic(n.project(), n.full());
        if (cur == null) {
            throw PsException.notFound(n.id());
        }
        Topic.Builder b = cur.toBuilder();
        FieldMaskUtil.merge(r.getUpdateMask(), r.getTopic(), b, new FieldMaskUtil.MergeOptions().setReplaceMessageFields(true)
                .setReplaceRepeatedFields(true));
        Topic t = b.build();
        validateTopic(t, n.project());
        store.updateTopic(n.project(), t);
        return t;
    }

    ListTopicsResponse listTopics(ListTopicsRequest r) {
        String project = PsNames.project(r.getProject());
        int size = pageSize(r.getPageSize());
        List<Topic> l = store.listTopics(project, after(r.getPageToken()), size + 1);
        ListTopicsResponse.Builder b = ListTopicsResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            b.addTopics(l.get(i));
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).getName()));
        }
        return b.build();
    }

    ListTopicSubscriptionsResponse listTopicSubscriptions(ListTopicSubscriptionsRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic());
        if (store.getTopic(n.project(), n.full()) == null) {
            throw PsException.notFound(n.id());
        }
        int size = pageSize(r.getPageSize());
        List<PsStore.SubRow> l = store.topicSubs(n.project(), n.full(), after(r.getPageToken()), size + 1, true);
        ListTopicSubscriptionsResponse.Builder b = ListTopicSubscriptionsResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            b.addSubscriptions(l.get(i).name());
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).name()));
        }
        return b.build();
    }

    ListTopicSnapshotsResponse listTopicSnapshots(ListTopicSnapshotsRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic());
        if (store.getTopic(n.project(), n.full()) == null) {
            throw PsException.notFound(n.id());
        }
        int size = pageSize(r.getPageSize());
        List<PsStore.SnapRow> l = store.listSnapshots(n.project(), n.full(), after(r.getPageToken()), size + 1);
        ListTopicSnapshotsResponse.Builder b = ListTopicSnapshotsResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            b.addSnapshots(l.get(i).name());
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).name()));
        }
        return b.build();
    }

    Empty deleteTopic(DeleteTopicRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic());
        if (!store.deleteTopic(n.project(), n.full())) {
            throw PsException.notFound(n.id());
        }
        subCache.clear();
        return Empty.getDefaultInstance();
    }

    DetachSubscriptionResponse detachSubscription(DetachSubscriptionRequest r) {
        Sub s = sub(r.getSubscription());
        Subscription d = s.doc.toBuilder().setDetached(true).build();
        store.updateSub(s.name.project(), d);
        subCache.remove(s.name.full());
        store.dropAll(s.host, s.name.full());
        return DetachSubscriptionResponse.getDefaultInstance();
    }

    // ------------------------------------------------------------------------------------------ publish

    PublishResponse publish(PublishRequest r) {
        PsNames.Name n = PsNames.parseExisting("topics", r.getTopic());
        if (r.getMessagesCount() == 0) {
            throw PsException.invalid("No messages to publish");
        }
        if (r.getMessagesCount() > MAX_PUBLISH_MESSAGES) {
            throw PsException.invalid("The value for message_count is too large. You passed " + r.getMessagesCount()
                    + " in the request, but the maximum value is " + MAX_PUBLISH_MESSAGES + ".");
        }
        long total = 0;
        for (PubsubMessage m : r.getMessagesList()) {
            if (m.getData().isEmpty() && m.getAttributesCount() == 0) {
                throw PsException.invalid("Some messages are empty");
            }
            if (m.getData().size() > MAX_MESSAGE_BYTES) {
                throw PsException.invalid("One or more messages exceed the maximum message size of " + MAX_MESSAGE_BYTES + " bytes.");
            }
            if (m.getAttributesCount() > 100) {
                throw PsException.invalid("Messages can have at most 100 attributes.");
            }
            for (var e : m.getAttributesMap().entrySet()) {
                if (e.getKey().isEmpty() || e.getKey().getBytes(StandardCharsets.UTF_8).length > 256) {
                    throw PsException.invalid("Attribute keys must be non-empty and at most 256 bytes.");
                }
                if (e.getKey().startsWith("goog")) {
                    throw PsException.invalid("Attribute keys must not start with 'goog'.");
                }
                if (e.getValue().getBytes(StandardCharsets.UTF_8).length > 1024) {
                    throw PsException.invalid("Attribute values must be at most 1024 bytes.");
                }
            }
            if (m.getOrderingKey().getBytes(StandardCharsets.UTF_8).length > 1024) {
                throw PsException.invalid("The ordering key must be at most 1024 bytes.");
            }
            total += m.getSerializedSize();
        }
        if (total > MAX_PUBLISH_BYTES) {
            throw new PsException(io.grpc.Status.Code.INVALID_ARGUMENT,
                    "Request payload size exceeds the limit: " + MAX_PUBLISH_BYTES + " bytes.");
        }
        Topic topic = store.getTopic(n.project(), n.full());
        if (topic == null) {
            throw PsException.notFound(n.id());
        }
        if (topic.hasSchemaSettings() && !topic.getSchemaSettings().getSchema().isEmpty()
                && !topic.getSchemaSettings().getSchema().equals("_deleted-schema_")) {
            PsNames.Name sn = PsNames.parseExisting("schemas", topic.getSchemaSettings().getSchema());
            Schema schema = store.getSchema(sn.project(), sn.full());
            if (schema != null) {
                for (PubsubMessage m : r.getMessagesList()) {
                    PsSchemas.validateMessage(schema, topic.getSchemaSettings().getEncoding(), m.getData());
                }
            }
        }
        Instant now = PsStore.micros(Instant.now());
        List<PubsubMessage> stamped = new ArrayList<>(r.getMessagesCount());
        PublishResponse.Builder out = PublishResponse.newBuilder();
        for (PubsubMessage m : r.getMessagesList()) {
            String id = PsIds.next();
            stamped.add(m.toBuilder().setMessageId(id).setPublishTime(PsStore.ts(now)).build());
            out.addMessageIds(id);
        }
        fanOut(n.project(), n.full(), stamped, true);
        published.addAndGet(stamped.size());
        return out.build();
    }

    private PsFilter filterOf(String text) {
        return text.isEmpty() ? null : filters.computeIfAbsent(text, PsFilter::parse);
    }

    /**
     * Copies stamped messages into the queue of every subscription of the topic. One target host: a single transaction. Several
     * hosts: the batch is first written to the outbox on the home host (durable), then inserted on each host (idempotent on
     * (subscription, message id)), then the outbox row is deleted; whatever a failure leaves is finished by the sweeper.
     */
    void fanOut(String project, String topic, List<PubsubMessage> msgs, boolean allowOutbox) {
        List<PsStore.SubRow> subs = store.topicSubs(project, topic, "", Integer.MAX_VALUE, false);
        if (subs.isEmpty()) {
            return;
        }
        Map<String, List<PsStore.Enqueue>> byHost = new LinkedHashMap<>();
        List<String> touched = new ArrayList<>();
        for (PsStore.SubRow s : subs) {
            PsFilter f = filterOf(s.filter());
            String host = store.shards.owner(project, s.name());
            for (PubsubMessage m : msgs) {
                if (f == null || f.matches(m.getAttributesMap())) {
                    byHost.computeIfAbsent(host, k -> new ArrayList<>()).add(new PsStore.Enqueue(s.name(), m));
                }
            }
            touched.add(s.name());
        }
        if (byHost.isEmpty()) {
            return;
        }
        if (byHost.size() == 1) {
            var e = byHost.entrySet().iterator().next();
            store.insertMessages(e.getKey(), e.getValue());
        } else {
            long outbox = -1;
            if (allowOutbox) {
                outbox = store.outboxInsert(project, topic,
                        PublishRequest.newBuilder().setTopic(topic).addAllMessages(msgs).build().toByteArray());
            }
            boolean ok = true;
            for (var e : byHost.entrySet()) {
                try {
                    store.insertMessages(e.getKey(), e.getValue());
                } catch (RuntimeException ex) {
                    ok = false;
                    if (outbox < 0) {
                        throw ex;
                    }
                    log.warn("pubsubwire: delivery to host {} failed, the outbox will retry it: {}", e.getKey(), ex.toString());
                }
            }
            if (ok && outbox >= 0) {
                store.outboxDelete(project, outbox);
            }
        }
        for (String s : touched) {
            wakeups.signal(s);
        }
    }

    // ------------------------------------------------------------------------------------------ subscriptions

    private static final long MIN_RETENTION_S = 600;
    private static final long MAX_RETENTION_S = 604_800;

    private void validateSub(Subscription s, boolean create) {
        int ack = s.getAckDeadlineSeconds();
        if (ack != 0 && (ack < 10 || ack > 600)) {
            throw PsException.invalid("Invalid ack_deadline_seconds: " + ack + ". It must be between 10 and 600 seconds.");
        }
        if (s.hasMessageRetentionDuration()) {
            long d = s.getMessageRetentionDuration().getSeconds();
            if (d < MIN_RETENTION_S || d > MAX_RETENTION_S || (d == MAX_RETENTION_S && s.getMessageRetentionDuration().getNanos() > 0)) {
                throw PsException.invalid("message_retention_duration must be between 10 minutes and 7 days.");
            }
        }
        if (s.hasDeadLetterPolicy()) {
            DeadLetterPolicy d = s.getDeadLetterPolicy();
            int m = d.getMaxDeliveryAttempts();
            if (m != 0 && m < 5) {
                throw new PsException(io.grpc.Status.Code.OUT_OF_RANGE, "The value for max_delivery_attempts is too small. You passed " + m
                        + " in the request, but the minimum value is 5.");
            }
            if (m > 100) {
                throw new PsException(io.grpc.Status.Code.OUT_OF_RANGE, "The value for max_delivery_attempts is too large. You passed " + m
                        + " in the request, but the maximum value is 100.");
            }
            if (d.getDeadLetterTopic().isEmpty()) {
                throw PsException.invalid("dead_letter_topic must be set in the dead_letter_policy.");
            }
            PsNames.Name dn = PsNames.parse("topics", d.getDeadLetterTopic());
            if (create && store.getTopic(dn.project(), dn.full()) == null) {
                throw PsException.notFound(dn.id());
            }
        }
        if (s.hasRetryPolicy()) {
            RetryPolicy r = s.getRetryPolicy();
            long min = r.hasMinimumBackoff() ? ms(r.getMinimumBackoff()) : 10_000;
            long max = r.hasMaximumBackoff() ? ms(r.getMaximumBackoff()) : 600_000;
            if (min < 0 || min > 600_000) {
                throw PsException.invalid("The value for minimum_backoff is out of bounds. You passed " + dur(min)
                        + " in the request, but the value must be between 0s and 600s.");
            }
            if (max < 0 || max > 600_000) {
                throw PsException.invalid("The value for maximum_backoff is out of bounds. You passed " + dur(max)
                        + " in the request, but the value must be between 0s and 600s.");
            }
            if (max < min) {
                throw PsException.invalid("The value for maximum_backoff is too small. You passed " + dur(max)
                        + " in the request, but the minimum value is " + dur(min) + ".");
            }
        }
        PushConfig pc = s.getPushConfig();
        if (!pc.getPushEndpoint().isEmpty()) {
            String e = pc.getPushEndpoint();
            if (!(e.startsWith("https://") || e.startsWith("http://")) || e.length() > 2048) {
                throw PsException.invalid("Invalid push endpoint: " + e);
            }
        }
        if (!s.getFilter().isEmpty()) {
            try {
                PsFilter.parse(s.getFilter());
            } catch (IllegalArgumentException ex) {
                throw PsException.invalid("Invalid filter: " + ex.getMessage());
            }
        }
        for (var e : s.getLabelsMap().entrySet()) {
            if (e.getKey().isEmpty() || e.getKey().length() > 63 || e.getValue().length() > 63) {
                throw PsException.invalid("Invalid label: keys and values are limited to 63 characters and keys cannot be empty.");
            }
        }
    }

    Subscription createSubscription(Subscription in) {
        PsNames.Name n = PsNames.parse("subscriptions", in.getName());
        if (!in.getTopic().equals(DELETED_TOPIC)) {
            PsNames.parse("topics", in.getTopic());
        }
        validateSub(in, true);
        PsNames.Name tn = PsNames.parseExisting("topics", in.getTopic());
        Topic topic = store.getTopic(tn.project(), tn.full());
        if (topic == null) {
            throw PsException.notFound(tn.id());
        }
        Subscription.Builder b = in.toBuilder().clearTags().clearState().clearDetached();
        if (b.getAckDeadlineSeconds() == 0) {
            b.setAckDeadlineSeconds(10);
        }
        if (!b.hasMessageRetentionDuration()) {
            b.setMessageRetentionDuration(secs(DEFAULT_RETENTION_S));
        }
        if (!b.hasExpirationPolicy()) {
            b.setExpirationPolicy(ExpirationPolicy.newBuilder().setTtl(secs(DEFAULT_EXPIRATION_S)));
        }
        if (!b.hasPushConfig()) {
            b.setPushConfig(PushConfig.getDefaultInstance());
        }
        if (b.hasDeadLetterPolicy() && b.getDeadLetterPolicy().getMaxDeliveryAttempts() == 0) {
            b.getDeadLetterPolicyBuilder().setMaxDeliveryAttempts(5);
        }
        if (topic.hasMessageRetentionDuration()) {
            b.setTopicMessageRetentionDuration(topic.getMessageRetentionDuration());
        }
        Subscription s = b.build();
        try {
            store.insertSub(n.project(), s, s.getFilter());
        } catch (PsException e) {
            if (PsShards.isUniqueViolation(e)) {
                throw PsException.exists(n.id());
            }
            throw e;
        }
        return s;
    }

    Subscription getSubscription(GetSubscriptionRequest r) {
        return sub(r.getSubscription()).doc;
    }

    Subscription updateSubscription(UpdateSubscriptionRequest r) {
        PsNames.Name n = PsNames.parseExisting("subscriptions", r.getSubscription().getName());
        checkMask("UpdateSubscriptionRequest", r.getUpdateMask(), r.getSubscription(),
                java.util.Set.of("name", "topic", "filter", "state", "detached", "tags", "topic_message_retention_duration",
                        "analytics_hub_subscription_info"));
        Sub cur = sub(n.full());
        Subscription.Builder b = cur.doc.toBuilder();
        FieldMaskUtil.merge(r.getUpdateMask(), r.getSubscription(), b, new FieldMaskUtil.MergeOptions()
                .setReplaceMessageFields(true).setReplaceRepeatedFields(true));
        Subscription s = b.build();
        validateSub(s, false);
        if (s.getAckDeadlineSeconds() == 0) {
            s = s.toBuilder().setAckDeadlineSeconds(10).build();
        }
        if (s.hasDeadLetterPolicy() && s.getDeadLetterPolicy().getMaxDeliveryAttempts() == 0) {
            s = s.toBuilder().setDeadLetterPolicy(s.getDeadLetterPolicy().toBuilder().setMaxDeliveryAttempts(5)).build();
        }
        store.updateSub(n.project(), s);
        subCache.remove(n.full());
        return s;
    }

    ListSubscriptionsResponse listSubscriptions(ListSubscriptionsRequest r) {
        String project = PsNames.project(r.getProject());
        int size = pageSize(r.getPageSize());
        List<PsStore.SubRow> l = store.listSubs(project, after(r.getPageToken()), size + 1);
        ListSubscriptionsResponse.Builder b = ListSubscriptionsResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            b.addSubscriptions(l.get(i).doc());
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).name()));
        }
        return b.build();
    }

    Empty deleteSubscription(DeleteSubscriptionRequest r) {
        PsNames.Name n = PsNames.parseExisting("subscriptions", r.getSubscription());
        if (!store.deleteSub(n.project(), n.full())) {
            throw PsException.notFound(n.id());
        }
        subCache.remove(n.full());
        return Empty.getDefaultInstance();
    }

    Empty modifyPushConfig(ModifyPushConfigRequest r) {
        Sub s = sub(r.getSubscription());
        Subscription d = s.doc.toBuilder().setPushConfig(r.getPushConfig()).build();
        validateSub(d, false);
        store.updateSub(s.name.project(), d);
        subCache.remove(s.name.full());
        return Empty.getDefaultInstance();
    }

    // ------------------------------------------------------------------------------------------ delivery

    static ReceivedMessage received(PsStore.Delivered d, Sub s) {
        ReceivedMessage.Builder b = ReceivedMessage.newBuilder().setAckId(PsAckId.encode(d.seq(), d.attempt(), d.token()))
                .setMessage(d.msg());
        if (s.maxAttempts > 0) {
            b.setDeliveryAttempt(d.attempt());
        }
        return b.build();
    }

    /** Leases up to {@code max} messages for delivery (Pull, StreamingPull and push all use this); forwards messages that used up
     * their delivery attempts to the dead-letter topic first. Never blocks. */
    List<PsStore.Delivered> lease(Sub s, int max, long leaseMs) {
        if (s.maxAttempts > 0) {
            forwardDead(s);
        }
        return store.claim(s.host, s.name.full(), max, leaseMs, s.ordered, s.maxAttempts, false, s.retryMin, s.retryMax);
    }

    private void forwardDead(Sub s) {
        List<PsStore.Delivered> dead = store.claim(s.host, s.name.full(), 100, 60_000, s.ordered, s.maxAttempts, true, -1, -1);
        if (dead.isEmpty()) {
            return;
        }
        PsNames.Name dn = PsNames.parseExisting("topics", s.dlq);
        if (store.getTopic(dn.project(), dn.full()) == null) {
            return; // stays leased for a minute, then retried
        }
        Instant now = PsStore.micros(Instant.now());
        List<PubsubMessage> out = new ArrayList<>();
        for (PsStore.Delivered d : dead) {
            PubsubMessage.Builder b = d.msg().toBuilder().setMessageId(PsIds.next()).setPublishTime(PsStore.ts(now));
            b.putAttributes("CloudPubSubDeadLetterSourceDeliveryCount", Integer.toString(d.attempt()));
            b.putAttributes("CloudPubSubDeadLetterSourceSubscription", s.name.id());
            b.putAttributes("CloudPubSubDeadLetterSourceSubscriptionProject", s.name.project());
            b.putAttributes("CloudPubSubDeadLetterSourceTopicPublishTime", java.time.format.DateTimeFormatter.ISO_INSTANT
                    .format(PsStore.instant(d.msg().getPublishTime())));
            out.add(b.build());
        }
        fanOut(dn.project(), dn.full(), out, true);
        List<Long> seqs = new ArrayList<>();
        dead.forEach(d -> seqs.add(d.seq()));
        store.dropSeqs(s.host, s.name.full(), seqs);
    }

    PullResponse pull(PullRequest r) {
        Sub s = sub(r.getSubscription());
        if (s.row.detached()) {
            throw PsException.precondition("Subscription " + s.name.full() + " is detached.");
        }
        requirePullable(s);
        if (r.getMaxMessages() <= 0) {
            throw PsException.invalid("No max_messages specified");
        }
        int max = Math.min(r.getMaxMessages(), 1000);
        long deadline = System.currentTimeMillis() + cfg.pullWaitMs;
        while (true) {
            List<PsStore.Delivered> l = lease(s, max, s.ackDeadline * 1000L);
            if (!l.isEmpty()) {
                PullResponse.Builder b = PullResponse.newBuilder();
                l.forEach(d -> b.addReceivedMessages(received(d, s)));
                return b.build();
            }
            long left = deadline - System.currentTimeMillis();
            if (r.getReturnImmediately() || left <= 0) {
                return PullResponse.getDefaultInstance();
            }
            wakeups.await(s.name.full(), Math.min(left, 200));
        }
    }

    /** BigQuery, Cloud Storage and Bigtable subscriptions are stored but their delivery is not implemented. */
    static void requirePullable(Sub s) {
        if (s.doc.hasBigqueryConfig() || s.doc.hasCloudStorageConfig() || s.doc.hasBigtableConfig()) {
            throw PsException.unimplemented("Delivery to BigQuery, Cloud Storage and Bigtable subscriptions is not implemented; "
                    + "the configuration is stored and returned but no messages are exported or pulled.");
        }
    }

    private static void requireAckIds(List<String> ids, String suffix) {
        if (ids.isEmpty()) {
            throw PsException.invalid("No ack ids specified" + suffix);
        }
    }

    Empty acknowledge(AcknowledgeRequest r) {
        Sub s = sub(r.getSubscription());
        requireAckIds(r.getAckIdsList(), ".");
        Map<String, PsStore.AckResult> res = ackIds(s, r.getAckIdsList());
        failIfInvalid(s, res);
        return Empty.getDefaultInstance();
    }

    Map<String, PsStore.AckResult> ackIds(Sub s, List<String> ids) {
        return store.ack(s.host, s.name.full(), ids, s.exactlyOnce, s.retain || store.hasHold(s.host, s.name.full()));
    }

    Map<String, PsStore.AckResult> modackMillis(Sub s, List<String> ids, long ms) {
        return store.modack(s.host, s.name.full(), ids, ms, s.exactlyOnce, -1, -1);
    }

    Map<String, PsStore.AckResult> modackIds(Sub s, List<String> ids, int seconds) {
        return store.modack(s.host, s.name.full(), ids, seconds * 1000L, s.exactlyOnce, s.retryMin, s.retryMax);
    }

    private static void failIfInvalid(Sub s, Map<String, PsStore.AckResult> res) {
        Map<String, String> bad = new LinkedHashMap<>();
        res.forEach((k, v) -> {
            if (v == PsStore.AckResult.INVALID) {
                bad.put(k, "PERMANENT_FAILURE_INVALID_ACK_ID");
            }
        });
        if (bad.isEmpty()) {
            return;
        }
        if (s.exactlyOnce) {
            throw PsException.exactlyOnce(bad);
        }
        String first = bad.keySet().iterator().next();
        throw PsException.invalid("Invalid ack id (ack_id=" + first + ")");
    }

    Empty modifyAckDeadline(ModifyAckDeadlineRequest r) {
        Sub s = sub(r.getSubscription());
        requireAckIds(r.getAckIdsList(), "");
        int d = r.getAckDeadlineSeconds();
        if (d < 0 || d > 600) {
            throw PsException.invalid("Invalid ack_deadline_seconds: " + d + ". It must be between 0 and 600 seconds.");
        }
        failIfInvalid(s, modackIds(s, r.getAckIdsList(), d));
        return Empty.getDefaultInstance();
    }

    // ------------------------------------------------------------------------------------------ snapshots & seek

    private Snapshot snapshotProto(PsStore.SnapRow r) {
        return r.doc();
    }

    Snapshot createSnapshot(CreateSnapshotRequest r) {
        PsNames.Name n = PsNames.parse("snapshots", r.getName());
        Sub s = sub(r.getSubscription());
        if (!s.name.project().equals(n.project())) {
            throw PsException.invalid("The snapshot and its subscription must be in the same project.");
        }
        if (s.row.topic().equals(DELETED_TOPIC)) {
            throw PsException.precondition("The subscription's topic has been deleted.");
        }
        if (store.getSnapshot(n.project(), n.full()) != null) {
            throw PsException.exists(n.id());
        }
        Instant now = PsStore.micros(Instant.now());
        Instant oldest = store.snapshotState(s.host, n.full(), s.name.full());
        long maxAge = 7 * 86400L;
        Instant expire = oldest == null ? now.plusSeconds(maxAge) : oldest.plusSeconds(maxAge);
        if (!expire.isAfter(now)) {
            throw PsException.precondition("The subscription's oldest unacked message is older than the snapshot retention limit.");
        }
        Snapshot snap = Snapshot.newBuilder().setName(n.full()).setTopic(s.row.topic()).setExpireTime(PsStore.ts(expire))
                .putAllLabels(r.getLabelsMap()).build();
        store.setHold(s.host, s.name.full());
        for (PsStore.SubRow o : store.topicSubs(n.project(), s.row.topic(), "", Integer.MAX_VALUE, false)) {
            store.setHold(store.shards.owner(n.project(), o.name()), o.name());
        }
        try {
            store.insertSnapshot(n.project(), new PsStore.SnapRow(n.full(), s.row.topic(), s.name.full(), snap, now, expire));
        } catch (PsException e) {
            if (PsShards.isUniqueViolation(e)) {
                throw PsException.exists(n.id());
            }
            throw e;
        }
        return snap;
    }

    private PsStore.SnapRow snap(String name) {
        PsNames.Name n = PsNames.parseExisting("snapshots", name);
        PsStore.SnapRow row = store.getSnapshot(n.project(), n.full());
        if (row == null) {
            throw PsException.notFound(n.id());
        }
        return row;
    }

    Snapshot getSnapshot(GetSnapshotRequest r) {
        return snapshotProto(snap(r.getSnapshot()));
    }

    ListSnapshotsResponse listSnapshots(ListSnapshotsRequest r) {
        String project = PsNames.project(r.getProject());
        int size = pageSize(r.getPageSize());
        List<PsStore.SnapRow> l = store.listSnapshots(project, null, after(r.getPageToken()), size + 1);
        ListSnapshotsResponse.Builder b = ListSnapshotsResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            b.addSnapshots(l.get(i).doc());
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).name()));
        }
        return b.build();
    }

    Snapshot updateSnapshot(UpdateSnapshotRequest r) {
        PsNames.Name n = PsNames.parseExisting("snapshots", r.getSnapshot().getName());
        checkMask("UpdateSnapshotRequest", r.getUpdateMask(), r.getSnapshot(), java.util.Set.of("name", "topic"));
        PsStore.SnapRow cur = snap(n.full());
        Snapshot.Builder b = cur.doc().toBuilder();
        FieldMaskUtil.merge(r.getUpdateMask(), r.getSnapshot(), b, new FieldMaskUtil.MergeOptions().setReplaceMessageFields(true));
        Snapshot s = b.build();
        Instant expire = s.hasExpireTime() ? PsStore.instant(s.getExpireTime()) : cur.expireAt();
        store.updateSnapshot(n.project(), new PsStore.SnapRow(cur.name(), cur.topic(), cur.originSub(), s, cur.createdAt(), expire));
        return s;
    }

    Empty deleteSnapshot(DeleteSnapshotRequest r) {
        PsStore.SnapRow row = snap(r.getSnapshot());
        PsNames.Name n = PsNames.parseExisting("snapshots", r.getSnapshot());
        store.deleteSnapshot(n.project(), row);
        return Empty.getDefaultInstance();
    }

    SeekResponse seek(SeekRequest r) {
        Sub s = sub(r.getSubscription());
        boolean retain = s.retain || store.hasHold(s.host, s.name.full());
        if (r.getTargetCase() == SeekRequest.TargetCase.SNAPSHOT) {
            PsStore.SnapRow row = snap(r.getSnapshot());
            if (!row.topic().equals(s.row.topic())) {
                throw PsException.invalid("The snapshot's topic does not match the subscription's topic.");
            }
            PsNames.Name on = PsNames.parseExisting("subscriptions", row.originSub());
            List<String> unacked = store.snapshotUnacked(store.shards.owner(on.project(), on.full()), row.name());
            store.seek(s.host, s.name.full(), row.createdAt(), unacked, retain);
        } else if (r.getTargetCase() == SeekRequest.TargetCase.TIME) {
            store.seekTime(s.host, s.name.full(), PsStore.instant(r.getTime()), retain);
        } else {
            throw PsException.invalid("No target was specified in the SeekRequest. Must specify either a time or a snapshot");
        }
        wakeups.signal(s.name.full());
        return SeekResponse.getDefaultInstance();
    }

    // ------------------------------------------------------------------------------------------ IAM

    private void checkResource(String resource) {
        String[] p = resource.split("/", -1);
        if (p.length != 4 || !p[0].equals("projects") || !List.of("topics", "subscriptions", "snapshots", "schemas").contains(p[2])) {
            throw PsException.invalid("Invalid resource name given (name=" + resource + ").");
        }
        boolean ok = switch (p[2]) {
            case "topics" -> store.getTopic(p[1], resource) != null;
            case "subscriptions" -> store.getSub(p[1], resource) != null;
            case "snapshots" -> store.getSnapshot(p[1], resource) != null;
            default -> store.getSchema(p[1], resource) != null;
        };
        if (!ok) {
            throw PsException.notFound(p[3]);
        }
    }

    Policy getIamPolicy(GetIamPolicyRequest r) {
        checkResource(r.getResource());
        byte[] b = store.getIam(r.getResource().split("/")[1], r.getResource());
        try {
            return b == null ? Policy.newBuilder().setEtag(ByteString.copyFrom(new byte[] {0x00, 0x01})).build() : Policy.parseFrom(b);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw PsException.internal("corrupt IAM policy");
        }
    }

    Policy setIamPolicy(SetIamPolicyRequest r) {
        checkResource(r.getResource());
        Policy p = r.getPolicy();
        Policy stored = p.toBuilder().setEtag(ByteString.copyFrom(java.nio.ByteBuffer.allocate(8).putLong(System.nanoTime()).array()))
                .build();
        store.setIam(r.getResource().split("/")[1], r.getResource(), stored.toByteArray());
        return stored;
    }

    TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest r) {
        checkResource(r.getResource());
        return TestIamPermissionsResponse.newBuilder().addAllPermissions(r.getPermissionsList()).build();
    }

    // ------------------------------------------------------------------------------------------ schemas

    Schema createSchema(CreateSchemaRequest r) {
        String project = PsNames.project(r.getParent());
        String id = r.getSchemaId();
        if (id.isEmpty()) {
            id = r.getSchema().getName().isEmpty() ? "" : r.getSchema().getName().substring(r.getSchema().getName().lastIndexOf('/') + 1);
        }
        PsNames.Name n = PsNames.parse("schemas", "projects/" + project + "/schemas/" + id);
        PsSchemas.validateDefinition(r.getSchema());
        String rev = Long.toHexString(System.nanoTime() & 0xffffffffL);
        Schema s = r.getSchema().toBuilder().setName(n.full()).setRevisionId(rev)
                .setRevisionCreateTime(PsStore.ts(PsStore.micros(Instant.now()))).build();
        try {
            store.insertSchema(project, s);
        } catch (PsException e) {
            if (PsShards.isUniqueViolation(e)) {
                throw PsException.exists(n.id());
            }
            throw e;
        }
        return s;
    }

    Schema getSchema(GetSchemaRequest r) {
        PsNames.Name n = PsNames.parseExisting("schemas", r.getName().split("@")[0]);
        Schema s = store.getSchema(n.project(), n.full());
        if (s == null) {
            throw PsException.notFound(n.id());
        }
        return r.getView() == SchemaView.BASIC ? s.toBuilder().clearDefinition().clearCompiledProtoSchema().build() : s;
    }

    ListSchemasResponse listSchemas(ListSchemasRequest r) {
        String project = PsNames.project(r.getParent());
        int size = pageSize(r.getPageSize());
        List<Schema> l = store.listSchemas(project, after(r.getPageToken()), size + 1);
        ListSchemasResponse.Builder b = ListSchemasResponse.newBuilder();
        for (int i = 0; i < Math.min(size, l.size()); i++) {
            Schema s = l.get(i);
            b.addSchemas(r.getView() == SchemaView.FULL ? s : s.toBuilder().clearDefinition().clearCompiledProtoSchema().build());
        }
        if (l.size() > size) {
            b.setNextPageToken(token(l.get(size - 1).getName()));
        }
        return b.build();
    }

    Empty deleteSchema(DeleteSchemaRequest r) {
        PsNames.Name n = PsNames.parseExisting("schemas", r.getName());
        if (!store.deleteSchema(n.project(), n.full())) {
            throw PsException.notFound(n.id());
        }
        return Empty.getDefaultInstance();
    }

    ValidateSchemaResponse validateSchema(ValidateSchemaRequest r) {
        PsNames.project(r.getParent());
        PsSchemas.validateDefinition(r.getSchema());
        return ValidateSchemaResponse.getDefaultInstance();
    }

    ValidateMessageResponse validateMessage(ValidateMessageRequest r) {
        String project = PsNames.project(r.getParent());
        Schema schema;
        if (r.getSchemaSpecCase() == ValidateMessageRequest.SchemaSpecCase.NAME) {
            PsNames.Name n = PsNames.parseExisting("schemas", r.getName());
            schema = store.getSchema(project, n.full());
            if (schema == null) {
                throw PsException.notFound(n.id());
            }
        } else if (r.getSchemaSpecCase() == ValidateMessageRequest.SchemaSpecCase.SCHEMA) {
            schema = r.getSchema();
            PsSchemas.validateDefinition(schema);
        } else {
            throw PsException.invalid("Either a schema name or a schema must be specified.");
        }
        PsSchemas.validateMessage(schema, r.getEncoding(), r.getMessage());
        return ValidateMessageResponse.getDefaultInstance();
    }

    // ------------------------------------------------------------------------------------------ maintenance

    /** Periodic maintenance: retention, snapshots, stale outbox rows, held-back dead letters. */
    void sweep() {
        List<PsStore.SubRow> subs = store.allSubs();
        for (PsStore.SubRow row : subs) {
            try {
                PsNames.Name n = PsNames.parseExisting("subscriptions", row.name());
                Sub s = load(n, row);
                store.sweepQueue(s.host, n.full(), s.retentionSec, s.retain || store.hasHold(s.host, n.full()));
                if (s.maxAttempts > 0) {
                    forwardDead(s);
                }
            } catch (RuntimeException e) {
                log.debug("pubsubwire sweep of {} failed: {}", row.name(), e.toString());
            }
        }
        store.sweepSnapshots();
        for (String h : store.shards.allHosts()) {
            try {
                for (PsStore.OutboxRow o : store.outboxStale(h, 50)) {
                    PublishRequest pr = PublishRequest.parseFrom(o.payload());
                    PsNames.Name tn = PsNames.parseExisting("topics", o.topic());
                    fanOut(tn.project(), tn.full(), pr.getMessagesList(), false);
                    store.outboxDeleteOn(h, o.id());
                }
            } catch (Exception e) {
                log.debug("pubsubwire outbox sweep failed: {}", e.toString());
            }
        }
    }
}
