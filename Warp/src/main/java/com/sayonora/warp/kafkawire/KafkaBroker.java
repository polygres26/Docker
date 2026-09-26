package com.sayonora.warp.kafkawire;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The request handlers of the Kafka protocol: one method per API key, each decoding the request body of one version and encoding the
 * matching response. The broker is stateless apart from the group coordinator and the topic cache in {@link KafkaStore}; the log
 * is in Postgres. One logical broker (id from the broker registry, 0 for the first node) per Warp node.
 */
final class KafkaBroker {

    /** Supported API versions: {apiKey, min, max}. ApiVersions advertises exactly this table. */
    static final int[][] APIS = {
        {0, 3, 9}, {1, 4, 12}, {2, 1, 7}, {3, 1, 12}, {8, 2, 8}, {9, 1, 7}, {10, 0, 3}, {11, 0, 7}, {12, 0, 4}, {13, 0, 4},
        {14, 0, 5}, {15, 0, 5}, {16, 0, 4}, {17, 0, 1}, {18, 0, 3}, {19, 2, 7}, {20, 1, 5}, {21, 0, 2}, {22, 0, 4}, {23, 2, 4},
        {32, 1, 4}, {33, 0, 2}, {36, 0, 2}, {37, 0, 3}, {42, 0, 2}, {44, 0, 1}, {60, 0, 1}};

    private static final int[] FLEX_FROM = new int[128];
    static {
        java.util.Arrays.fill(FLEX_FROM, Integer.MAX_VALUE);
        int[][] f = {{0, 9}, {1, 12}, {2, 6}, {3, 9}, {8, 8}, {9, 6}, {10, 3}, {11, 6}, {12, 4}, {13, 4}, {14, 4}, {15, 5}, {16, 3}, {18, 3}, {19, 5},
            {20, 4}, {21, 2}, {22, 2}, {23, 4}, {32, 4}, {33, 2}, {36, 2}, {37, 2}, {42, 2}, {44, 1}, {60, 0}};
        for (int[] x : f) {
            FLEX_FROM[x[0]] = x[1];
        }
    }

    static boolean flexible(int api, int ver) {
        return api >= 0 && api < FLEX_FROM.length && ver >= FLEX_FROM[api];
    }

    static boolean supported(int api, int ver) {
        for (int[] a : APIS) {
            if (a[0] == api) {
                return ver >= a[1] && ver <= a[2];
            }
        }
        return false;
    }

    private static final Pattern LEGAL_TOPIC = Pattern.compile("[a-zA-Z0-9._-]+");
    private static final byte[] NONE = new byte[0];

    /** Per-connection facts a handler may need. */
    static final class Conn {
        String clientId = "";
        String clientHost = "";
        String user;
    }

    final KafkaStore store;
    final GroupCoordinator groups;
    final String advertisedHost;
    final int advertisedPort;
    volatile int nodeId;
    final boolean autoCreate;
    final int defaultPartitions;
    final boolean saslEnabled;

    KafkaBroker(KafkaStore store, GroupCoordinator groups, String advertisedHost, int advertisedPort, boolean autoCreate, int defaultPartitions,
            boolean saslEnabled) {
        this.store = store;
        this.groups = groups;
        this.advertisedHost = advertisedHost;
        this.advertisedPort = advertisedPort;
        this.autoCreate = autoCreate;
        this.defaultPartitions = defaultPartitions;
        this.saslEnabled = saslEnabled;
    }

    static int codeOf(Throwable t) {
        return t instanceof KafkaError k ? k.code : KafkaError.UNKNOWN_SERVER_ERROR;
    }

    // ------------------------------------------------------------------ cluster view

    List<KafkaStore.Broker> brokers() {
        List<KafkaStore.Broker> b;
        try {
            b = store.liveBrokers();
        } catch (RuntimeException e) {
            b = List.of();
        }
        if (b.isEmpty()) {
            return List.of(new KafkaStore.Broker(nodeId, advertisedHost, advertisedPort));
        }
        return b;
    }

    int leaderOf(String topic, int part) {
        List<KafkaStore.Broker> b = brokers();
        return b.get(Math.floorMod((topic + "-" + part).hashCode(), b.size())).id();
    }

    KafkaStore.Broker coordinatorFor(String group) {
        List<KafkaStore.Broker> b = brokers();
        return b.get(Math.floorMod(group.hashCode(), b.size()));
    }

    boolean isCoordinator(String group) {
        return coordinatorFor(group).id() == nodeId;
    }

    // ------------------------------------------------------------------ dispatch

    /** @return the response body, or null when the request expects no response (Produce acks=0) */
    KWriter handle(int api, int ver, KReader r, Conn cn) {
        return switch (api) {
            case 0 -> produce(r, ver);
            case 1 -> fetch(r, ver);
            case 2 -> listOffsets(r, ver);
            case 3 -> metadata(r, ver);
            case 8 -> offsetCommit(r, ver);
            case 9 -> offsetFetch(r, ver);
            case 10 -> findCoordinator(r, ver);
            case 11 -> joinGroup(r, ver, cn);
            case 12 -> heartbeat(r, ver);
            case 13 -> leaveGroup(r, ver);
            case 14 -> syncGroup(r, ver);
            case 15 -> describeGroups(r, ver);
            case 16 -> listGroups(r, ver);
            case 18 -> apiVersions(ver);
            case 19 -> createTopics(r, ver);
            case 20 -> deleteTopics(r, ver);
            case 21 -> deleteRecords(r, ver);
            case 22 -> initProducerId(r, ver);
            case 23 -> offsetForLeaderEpoch(r, ver);
            case 32 -> describeConfigs(r, ver);
            case 33 -> alterConfigs(r, ver, false);
            case 44 -> alterConfigs(r, ver, true);
            case 37 -> createPartitions(r, ver);
            case 42 -> deleteGroups(r, ver);
            case 60 -> describeCluster(r, ver);
            default -> throw new KafkaError(KafkaError.UNSUPPORTED_VERSION, "unsupported api " + api);
        };
    }

    static boolean writeApi(int api) {
        return switch (api) {
            case 0, 8, 11, 13, 14, 19, 20, 21, 22, 33, 37, 42, 44 -> true;
            default -> false;
        };
    }

    static String apiName(int api) {
        return switch (api) {
            case 0 -> "Produce";
            case 1 -> "Fetch";
            case 2 -> "ListOffsets";
            case 3 -> "Metadata";
            case 8 -> "OffsetCommit";
            case 9 -> "OffsetFetch";
            case 10 -> "FindCoordinator";
            case 11 -> "JoinGroup";
            case 12 -> "Heartbeat";
            case 13 -> "LeaveGroup";
            case 14 -> "SyncGroup";
            case 15 -> "DescribeGroups";
            case 16 -> "ListGroups";
            case 17 -> "SaslHandshake";
            case 18 -> "ApiVersions";
            case 19 -> "CreateTopics";
            case 20 -> "DeleteTopics";
            case 21 -> "DeleteRecords";
            case 22 -> "InitProducerId";
            case 23 -> "OffsetForLeaderEpoch";
            case 32 -> "DescribeConfigs";
            case 33 -> "AlterConfigs";
            case 36 -> "SaslAuthenticate";
            case 37 -> "CreatePartitions";
            case 42 -> "DeleteGroups";
            case 44 -> "IncrementalAlterConfigs";
            case 60 -> "DescribeCluster";
            default -> "api" + api;
        };
    }

    // ------------------------------------------------------------------ ApiVersions

    KWriter apiVersions(int ver) {
        return apiVersions(ver, KafkaError.NONE);
    }

    KWriter apiVersions(int ver, int error) {
        KWriter w = new KWriter(ver >= 3);
        w.i16(error);
        w.arr(APIS.length);
        for (int[] a : APIS) {
            w.i16(a[0]).i16(a[1]).i16(a[2]).tagged();
        }
        if (ver >= 1) {
            w.i32(0);
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ Metadata

    private void topicMeta(KWriter w, int ver, String name, KafkaStore.Topic t, int error) {
        w.i16(error);
        w.str(name);
        if (ver >= 10) {
            w.uuid(t == null ? new UUID(0, 0) : t.id());
        }
        if (ver >= 1) {
            w.bool(false);
        }
        int np = t == null ? 0 : t.partitions();
        w.arr(np);
        for (int p = 0; p < np; p++) {
            int leader = leaderOf(name, p);
            w.i16(0).i32(p).i32(leader);
            if (ver >= 7) {
                w.i32(0);
            }
            w.arr(1).i32(leader);
            w.arr(1).i32(leader);
            if (ver >= 5) {
                w.arr(0);
            }
            w.tagged();
        }
        if (ver >= 8) {
            w.i32(Integer.MIN_VALUE);
        }
        w.tagged();
    }

    KWriter metadata(KReader r, int ver) {
        int n = r.arr();
        List<String> names = null;
        Map<UUID, String> byId = null;
        if (n >= 0) {
            names = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                UUID id = ver >= 10 ? r.uuid() : null;
                String name = r.str();
                r.tagged();
                if (name == null && id != null) {
                    if (byId == null) {
                        byId = new HashMap<>();
                        for (KafkaStore.Topic t : store.topics().values()) {
                            byId.put(t.id(), t.name());
                        }
                    }
                    name = byId.get(id);
                    names.add(name == null ? "\u0000" + id : name);
                } else {
                    names.add(name);
                }
            }
        }
        boolean allowAuto = ver >= 4 ? r.bool() : true;
        if (ver >= 8) {
            if (ver <= 10) {
                r.bool();
            }
            r.bool();
        }
        r.tagged();

        Map<String, KafkaStore.Topic> all = store.topics();
        KWriter w = new KWriter(ver >= 9);
        if (ver >= 3) {
            w.i32(0);
        }
        List<KafkaStore.Broker> bs = brokers();
        w.arr(bs.size());
        for (KafkaStore.Broker b : bs) {
            w.i32(b.id()).str(b.host()).i32(b.port());
            if (ver >= 1) {
                w.str(null);
            }
            w.tagged();
        }
        if (ver >= 2) {
            w.str(store.clusterId());
        }
        if (ver >= 1) {
            w.i32(bs.get(0).id());
        }
        List<String> list = new ArrayList<>();
        if (names == null) {
            list.addAll(all.keySet());
        } else {
            list.addAll(names);
        }
        w.arr(list.size());
        for (String name : list) {
            if (name == null) {
                topicMeta(w, ver, null, null, KafkaError.INVALID_TOPIC_EXCEPTION);
                continue;
            }
            if (name.startsWith("\u0000")) {
                topicMeta(w, ver, null, null, KafkaError.UNKNOWN_TOPIC_ID);
                continue;
            }
            KafkaStore.Topic t = all.get(name);
            if (t == null && names != null && validateTopicName(name) != null) {
                topicMeta(w, ver, name, null, KafkaError.INVALID_TOPIC_EXCEPTION);
                continue;
            }
            if (t == null && names != null && allowAuto && autoCreate) {
                try {
                    t = store.createTopic(name, defaultPartitions, new LinkedHashMap<>());
                } catch (KafkaError e) {
                    t = e.code == KafkaError.TOPIC_ALREADY_EXISTS ? store.topic(name) : null;
                } catch (RuntimeException e) {
                    t = null;
                }
            }
            if (t == null) {
                topicMeta(w, ver, name, null, KafkaError.UNKNOWN_TOPIC_OR_PARTITION);
            } else {
                topicMeta(w, ver, name, t, KafkaError.NONE);
            }
        }
        if (ver >= 8 && ver <= 10) {
            w.i32(Integer.MIN_VALUE);
        }
        w.tagged();
        return w;
    }

    static String validateTopicName(String name) {
        if (name == null || name.isEmpty()) {
            return "Topic name is invalid: the empty string is not allowed";
        }
        if (name.equals(".") || name.equals("..")) {
            return "Topic name cannot be \".\" or \"..\"";
        }
        if (name.length() > 249) {
            return "Topic name is invalid: the length of '" + name + "' is longer than the max allowed length 249";
        }
        if (!LEGAL_TOPIC.matcher(name).matches()) {
            return "Topic name is invalid: '" + name + "' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'";
        }
        return null;
    }

    // ------------------------------------------------------------------ Produce

    private record PartOut(int index, int error, long base, long logStart, String msg) {
    }

    KWriter produce(KReader r, int ver) {
        String txnId = r.str();
        int acks = r.i16();
        r.i32(); // timeout
        int nt = r.arr();
        Map<String, List<Object[]>> in = new LinkedHashMap<>();
        for (int i = 0; i < nt; i++) {
            String name = r.str();
            int np = r.arr();
            List<Object[]> ps = in.computeIfAbsent(name, k -> new ArrayList<>());
            for (int j = 0; j < np; j++) {
                int idx = r.i32();
                byte[] rec = r.bytes();
                r.tagged();
                ps.add(new Object[] {idx, rec});
            }
            r.tagged();
        }
        r.tagged();

        Map<String, List<PartOut>> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Object[]>> e : in.entrySet()) {
            KafkaStore.Topic t = null;
            try {
                t = store.topic(e.getKey());
            } catch (RuntimeException ex) {
                // reported per partition below
            }
            List<PartOut> pl = out.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            for (Object[] p : e.getValue()) {
                int idx = (Integer) p[0];
                PartOut po;
                try {
                    if (acks != 0 && acks != 1 && acks != -1) {
                        po = new PartOut(idx, KafkaError.INVALID_REQUIRED_ACKS, -1, -1, "Produce request specifies an invalid value for required acks: " + acks);
                    } else if (txnId != null) {
                        po = new PartOut(idx, KafkaError.UNSUPPORTED_VERSION, -1, -1, "transactions are not supported by this broker");
                    } else if (t == null || idx < 0 || idx >= t.partitions()) {
                        po = new PartOut(idx, KafkaError.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, "This server does not host this topic-partition.");
                    } else {
                        po = appendOne(t, idx, (byte[]) p[1]);
                    }
                } catch (KafkaError ke) {
                    po = new PartOut(idx, ke.code, -1, -1, ke.getMessage());
                } catch (RuntimeException ex) {
                    po = new PartOut(idx, KafkaError.UNKNOWN_SERVER_ERROR, -1, -1, "internal error");
                }
                pl.add(po);
            }
        }
        if (acks == 0) {
            return null;
        }
        KWriter w = new KWriter(ver >= 9);
        w.arr(out.size());
        for (Map.Entry<String, List<PartOut>> e : out.entrySet()) {
            w.str(e.getKey());
            w.arr(e.getValue().size());
            for (PartOut p : e.getValue()) {
                w.i32(p.index()).i16(p.error()).i64(p.base());
                w.i64(-1);
                if (ver >= 5) {
                    w.i64(p.logStart());
                }
                if (ver >= 8) {
                    w.arr(0);
                    w.str(p.error() == 0 ? null : p.msg());
                }
                w.tagged();
            }
            w.tagged();
        }
        w.i32(0);
        w.tagged();
        return w;
    }

    /** Validates (CRC32C, magic, record counts, size) and appends the batches of one partition. */
    private PartOut appendOne(KafkaStore.Topic t, int part, byte[] records) {
        if (records == null || records.length == 0) {
            return new PartOut(part, KafkaError.INVALID_RECORD, -1, -1, "Produce requests must contain a record batch");
        }
        long max = KafkaConfigs.longOf(t.config(), "max.message.bytes");
        List<KBatch> batches = KBatch.parse(records);
        if (batches.size() != 1) {
            return new PartOut(part, KafkaError.INVALID_RECORD, -1, -1, "Produce requests with version 3 or higher must contain exactly one record batch per partition");
        }
        for (KBatch b : batches) {
            if (b.batchLength() > max) {
                throw new KafkaError(KafkaError.MESSAGE_TOO_LARGE, "The request included a message larger than the max message size the server will accept.");
            }
            if (!b.crcOk()) {
                throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "Record batch for partition " + t.name() + "-" + part + " is corrupt: CRC does not match");
            }
            if (b.recordCount() < 1 || b.recordCount() != b.lastOffsetDelta() + 1) {
                throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "Record batch for partition " + t.name() + "-" + part + " has an inconsistent record count");
            }
            if (b.transactional() || b.control()) {
                throw new KafkaError(KafkaError.INVALID_TXN_STATE, "transactional and control batches are not supported by this broker");
            }
        }
        KafkaStore.AppendResult a = store.append(t.name(), part, batches);
        return new PartOut(part, a.error(), a.baseOffset(), a.logStart(), a.message());
    }

    // ------------------------------------------------------------------ Fetch

    private record FetchReq(String topic, int part, long offset, int maxBytes) {
    }

    KWriter fetch(KReader r, int ver) {
        r.i32(); // replica id
        int maxWait = r.i32();
        int minBytes = r.i32();
        int maxBytes = ver >= 3 ? r.i32() : Integer.MAX_VALUE;
        int isolation = ver >= 4 ? r.i8() : 0;
        if (ver >= 7) {
            r.i32();
            r.i32();
        }
        int nt = r.arr();
        List<FetchReq> reqs = new ArrayList<>();
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arr();
            for (int j = 0; j < np; j++) {
                int part = r.i32();
                if (ver >= 9) {
                    r.i32();
                }
                long off = r.i64();
                if (ver >= 12) {
                    r.i32();
                }
                if (ver >= 5) {
                    r.i64();
                }
                int pmax = r.i32();
                r.tagged();
                reqs.add(new FetchReq(topic, part, off, pmax));
            }
            r.tagged();
        }
        if (ver >= 7) {
            int nf = r.arr();
            for (int i = 0; i < nf; i++) {
                r.str();
                int np = r.arr();
                for (int j = 0; j < np; j++) {
                    r.i32();
                }
                r.tagged();
            }
        }
        if (ver >= 11) {
            r.str();
        }
        r.tagged();

        long deadline = System.currentTimeMillis() + Math.max(0, maxWait);
        Map<String, List<Object[]>> results = new LinkedHashMap<>();
        while (true) {
            long seen = store.appendVersion();
            results.clear();
            long total = 0;
            boolean anyError = false;
            int budget = maxBytes <= 0 ? Integer.MAX_VALUE : maxBytes;
            boolean first = true;
            for (FetchReq q : reqs) {
                Object[] res;
                KafkaStore.Topic t = null;
                try {
                    t = store.topic(q.topic());
                    if (t == null || q.part() < 0 || q.part() >= t.partitions()) {
                        res = new Object[] {q.part(), KafkaError.UNKNOWN_TOPIC_OR_PARTITION, -1L, -1L, NONE};
                        anyError = true;
                    } else {
                        int cap = (int) Math.min(Math.max(q.maxBytes(), 0), budget);
                        KafkaStore.FetchResult fr = store.fetch(q.topic(), q.part(), q.offset(), cap, first);
                        if (fr.error() != 0) {
                            anyError = true;
                        }
                        res = new Object[] {q.part(), fr.error(), fr.highWatermark(), fr.logStart(), fr.records()};
                        total += fr.records().length;
                        budget = (int) Math.max(0, (long) budget - fr.records().length);
                        if (fr.records().length > 0) {
                            first = false;
                        }
                    }
                } catch (KafkaError ke) {
                    res = new Object[] {q.part(), ke.code, -1L, -1L, NONE};
                    anyError = true;
                } catch (RuntimeException ex) {
                    res = new Object[] {q.part(), KafkaError.UNKNOWN_SERVER_ERROR, -1L, -1L, NONE};
                    anyError = true;
                }
                results.computeIfAbsent(q.topic(), k -> new ArrayList<>()).add(res);
            }
            long now = System.currentTimeMillis();
            if (anyError || minBytes <= 0 || total >= minBytes || now >= deadline) {
                break;
            }
            store.awaitAppend(seen, Math.min(100, deadline - now));
        }
        KWriter w = new KWriter(ver >= 12);
        if (ver >= 1) {
            w.i32(0);
        }
        if (ver >= 7) {
            w.i16(0).i32(0);
        }
        w.arr(results.size());
        for (Map.Entry<String, List<Object[]>> e : results.entrySet()) {
            w.str(e.getKey());
            w.arr(e.getValue().size());
            for (Object[] p : e.getValue()) {
                boolean failed = (Integer) p[1] != 0;
                w.i32((Integer) p[0]).i16((Integer) p[1]).i64(failed ? -1 : (Long) p[2]);
                w.i64(failed ? -1 : (Long) p[2]); // last stable offset == high watermark (no transactions)
                if (ver >= 5) {
                    w.i64(failed ? -1 : (Long) p[3]);
                }
                if (ver >= 4) {
                    if (isolation == 0) {
                        w.arr(-1);
                    } else {
                        w.arr(0);
                    }
                }
                if (ver >= 11) {
                    w.i32(-1);
                }
                w.bytes((byte[]) p[4]);
                w.tagged();
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ ListOffsets, OffsetForLeaderEpoch, DeleteRecords

    KWriter listOffsets(KReader r, int ver) {
        r.i32();
        if (ver >= 2) {
            r.i8();
        }
        int nt = r.arr();
        KWriter w = new KWriter(ver >= 6);
        if (ver >= 2) {
            w.i32(0);
        }
        w.arr(nt);
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arr();
            w.str(topic);
            w.arr(np);
            KafkaStore.Topic t = null;
            try {
                t = store.topic(topic);
            } catch (RuntimeException ignored) {
                // per partition error below
            }
            for (int j = 0; j < np; j++) {
                int part = r.i32();
                if (ver >= 4) {
                    r.i32();
                }
                long ts = r.i64();
                r.tagged();
                int err = 0;
                long off = -1;
                long time = -1;
                try {
                    if (t == null || part < 0 || part >= t.partitions()) {
                        err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    } else if (ts == -1) {
                        long[] b = store.bounds(topic, part);
                        off = b[0];
                    } else if (ts == -2) {
                        long[] b = store.bounds(topic, part);
                        off = b[1];
                    } else if (ts == -3 && ver >= 7) {
                        long[] x = store.offsetOfMaxTimestamp(topic, part);
                        off = x[0];
                        time = x[1];
                    } else if (ts < 0) {
                        err = KafkaError.UNSUPPORTED_VERSION;
                    } else {
                        long[] x = store.offsetForTimestamp(topic, part, ts);
                        off = x[0];
                        time = x[1];
                    }
                } catch (KafkaError ke) {
                    err = ke.code;
                } catch (RuntimeException ex) {
                    err = KafkaError.UNKNOWN_SERVER_ERROR;
                }
                w.i32(part).i16(err).i64(time).i64(off);
                if (ver >= 4) {
                    w.i32(err == 0 ? 0 : -1);
                }
                w.tagged();
            }
            r.tagged();
            w.tagged();
        }
        r.tagged();
        w.tagged();
        return w;
    }

    KWriter offsetForLeaderEpoch(KReader r, int ver) {
        if (ver >= 3) {
            r.i32();
        }
        int nt = r.arr();
        KWriter w = new KWriter(ver >= 4);
        w.i32(0);
        w.arr(nt);
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arr();
            w.str(topic);
            w.arr(np);
            KafkaStore.Topic t = store.topic(topic);
            for (int j = 0; j < np; j++) {
                int part = r.i32();
                r.i32(); // current leader epoch
                int epoch = r.i32();
                r.tagged();
                if (t == null || part < 0 || part >= t.partitions()) {
                    w.i16(KafkaError.UNKNOWN_TOPIC_OR_PARTITION).i32(part).i32(-1).i64(-1);
                } else if (epoch != 0) {
                    w.i16(0).i32(part).i32(-1).i64(-1); // this broker only ever had epoch 0: like Kafka, no end offset for any other epoch
                } else {
                    long[] b = store.bounds(topic, part);
                    w.i16(0).i32(part).i32(0).i64(b[0]);
                }
                w.tagged();
            }
            r.tagged();
            w.tagged();
        }
        r.tagged();
        w.tagged();
        return w;
    }

    KWriter deleteRecords(KReader r, int ver) {
        int nt = r.arr();
        KWriter w = new KWriter(ver >= 2);
        w.i32(0);
        List<Object[]> topics = new ArrayList<>();
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arr();
            List<long[]> parts = new ArrayList<>();
            for (int j = 0; j < np; j++) {
                int part = r.i32();
                long off = r.i64();
                r.tagged();
                parts.add(new long[] {part, off});
            }
            r.tagged();
            topics.add(new Object[] {topic, parts});
        }
        r.i32();
        r.tagged();
        w.arr(topics.size());
        for (Object[] tp : topics) {
            String topic = (String) tp[0];
            @SuppressWarnings("unchecked")
            List<long[]> parts = (List<long[]>) tp[1];
            w.str(topic);
            w.arr(parts.size());
            KafkaStore.Topic t = store.topic(topic);
            for (long[] p : parts) {
                int part = (int) p[0];
                long low = -1;
                int err = 0;
                try {
                    if (t == null || part < 0 || part >= t.partitions()) {
                        err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    } else {
                        low = store.deleteRecords(topic, part, p[1]);
                    }
                } catch (KafkaError ke) {
                    err = ke.code;
                } catch (RuntimeException ex) {
                    err = KafkaError.UNKNOWN_SERVER_ERROR;
                }
                w.i32(part).i64(low).i16(err).tagged();
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ InitProducerId

    KWriter initProducerId(KReader r, int ver) {
        String txnId = r.str();
        r.i32();
        long pid = -1;
        int epoch = -1;
        if (ver >= 3) {
            pid = r.i64();
            epoch = r.i16();
        }
        r.tagged();
        KWriter w = new KWriter(ver >= 2);
        w.i32(0);
        if (txnId != null) {
            w.i16(KafkaError.UNSUPPORTED_VERSION).i64(-1).i16(-1).tagged();
            return w;
        }
        try {
            // like Kafka, a non-transactional producer always gets a fresh producer id (epoch 0), whatever id/epoch it presents
            long[] res = store.newProducer();
            w.i16(0).i64(res[0]).i16((int) res[1]).tagged();
        } catch (RuntimeException e) {
            w.i16(KafkaError.UNKNOWN_SERVER_ERROR).i64(-1).i16(-1).tagged();
        }
        return w;
    }

    // ------------------------------------------------------------------ groups

    KWriter findCoordinator(KReader r, int ver) {
        String key = r.str();
        int type = ver >= 1 ? r.i8() : 0;
        r.tagged();
        KWriter w = new KWriter(ver >= 3);
        if (ver >= 1) {
            w.i32(0);
        }
        int err = 0;
        String msg = null;
        KafkaStore.Broker b = null;
        if (type == 1) {
            err = KafkaError.COORDINATOR_NOT_AVAILABLE;
            msg = "transactions are not supported by this broker";
        } else if (key == null) {
            err = KafkaError.INVALID_GROUP_ID;
            msg = "The group id is invalid";
        } else {
            try {
                b = coordinatorFor(key);
            } catch (RuntimeException e) {
                err = KafkaError.COORDINATOR_NOT_AVAILABLE;
            }
        }
        w.i16(err);
        if (ver >= 1) {
            w.str(msg);
        }
        if (err == 0) {
            w.i32(b.id()).str(b.host()).i32(b.port());
        } else {
            w.i32(-1).str("").i32(-1);
        }
        w.tagged();
        return w;
    }

    KWriter joinGroup(KReader r, int ver, Conn cn) {
        String group = r.str();
        int session = r.i32();
        int rebalance = ver >= 1 ? r.i32() : session;
        String member = r.str();
        String instance = ver >= 5 ? r.str() : null;
        String ptype = r.str();
        int np = r.arr();
        List<GroupCoordinator.Proto> protos = new ArrayList<>();
        for (int i = 0; i < np; i++) {
            String name = r.str();
            byte[] meta = r.bytes();
            r.tagged();
            protos.add(new GroupCoordinator.Proto(name, meta == null ? NONE : meta));
        }
        r.tagged();
        GroupCoordinator.JoinResult res;
        if (group != null && !group.isEmpty() && !isCoordinator(group)) {
            res = GroupCoordinator.JoinResult.error(KafkaError.NOT_COORDINATOR, member);
        } else {
            try {
                res = groups.join(group, session, rebalance, member, instance, ptype, protos, cn.clientId, cn.clientHost, ver);
            } catch (KafkaError e) {
                res = GroupCoordinator.JoinResult.error(e.code, member);
            } catch (RuntimeException e) {
                res = GroupCoordinator.JoinResult.error(KafkaError.COORDINATOR_NOT_AVAILABLE, member);
            }
        }
        KWriter w = new KWriter(ver >= 6);
        if (ver >= 2) {
            w.i32(0);
        }
        w.i16(res.error()).i32(res.generation());
        if (ver >= 7) {
            w.str(res.protocolType());
            w.str(res.protocolName());
        } else {
            w.str(res.protocolName() == null ? "" : res.protocolName());
        }
        w.str(res.leader() == null ? "" : res.leader());
        w.str(res.memberId() == null ? "" : res.memberId());
        w.arr(res.members().size());
        for (GroupCoordinator.MemberInfo m : res.members()) {
            w.str(m.memberId());
            if (ver >= 5) {
                w.str(m.instanceId());
            }
            w.bytes(m.metadata());
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter syncGroup(KReader r, int ver) {
        String group = r.str();
        int gen = r.i32();
        String member = r.str();
        if (ver >= 3) {
            r.str();
        }
        String ptype = null;
        String pname = null;
        if (ver >= 5) {
            ptype = r.str();
            pname = r.str();
        }
        int na = r.arr();
        Map<String, byte[]> assignments = new HashMap<>();
        for (int i = 0; i < na; i++) {
            String id = r.str();
            byte[] a = r.bytes();
            r.tagged();
            assignments.put(id, a == null ? NONE : a);
        }
        r.tagged();
        GroupCoordinator.SyncResult res;
        if (group != null && !group.isEmpty() && !isCoordinator(group)) {
            res = new GroupCoordinator.SyncResult(KafkaError.NOT_COORDINATOR, null, null, NONE);
        } else {
            try {
                res = groups.sync(group, gen, member == null ? "" : member, ptype, pname, assignments);
            } catch (KafkaError e) {
                res = new GroupCoordinator.SyncResult(e.code, null, null, NONE);
            } catch (RuntimeException e) {
                res = new GroupCoordinator.SyncResult(KafkaError.COORDINATOR_NOT_AVAILABLE, null, null, NONE);
            }
        }
        KWriter w = new KWriter(ver >= 4);
        if (ver >= 1) {
            w.i32(0);
        }
        w.i16(res.error());
        if (ver >= 5) {
            w.str(res.protocolType());
            w.str(res.protocolName());
        }
        w.bytes(res.assignment());
        w.tagged();
        return w;
    }

    KWriter heartbeat(KReader r, int ver) {
        String group = r.str();
        int gen = r.i32();
        String member = r.str();
        String instance = ver >= 3 ? r.str() : null;
        r.tagged();
        int err;
        if (group == null || group.isEmpty()) {
            err = KafkaError.INVALID_GROUP_ID;
        } else if (!isCoordinator(group)) {
            err = KafkaError.NOT_COORDINATOR;
        } else {
            try {
                err = groups.heartbeat(group, gen, member == null ? "" : member, instance);
            } catch (RuntimeException e) {
                err = codeOf(e) == KafkaError.UNKNOWN_SERVER_ERROR ? KafkaError.COORDINATOR_NOT_AVAILABLE : codeOf(e);
            }
        }
        KWriter w = new KWriter(ver >= 4);
        if (ver >= 1) {
            w.i32(0);
        }
        w.i16(err).tagged();
        return w;
    }

    KWriter leaveGroup(KReader r, int ver) {
        String group = r.str();
        List<String[]> members = new ArrayList<>();
        if (ver >= 3) {
            int n = r.arr();
            for (int i = 0; i < n; i++) {
                String id = r.str();
                String inst = r.str();
                if (ver >= 5) {
                    r.str();
                }
                r.tagged();
                members.add(new String[] {id, inst});
            }
        } else {
            members.add(new String[] {r.str(), null});
        }
        r.tagged();
        int top = 0;
        List<Integer> errs = new ArrayList<>();
        if (group == null || group.isEmpty()) {
            top = KafkaError.INVALID_GROUP_ID;
        } else if (!isCoordinator(group)) {
            top = KafkaError.NOT_COORDINATOR;
        } else {
            for (String[] m : members) {
                try {
                    errs.add(groups.leave(group, m[0] == null ? "" : m[0], m[1]));
                } catch (RuntimeException e) {
                    errs.add(KafkaError.COORDINATOR_NOT_AVAILABLE);
                }
            }
        }
        KWriter w = new KWriter(ver >= 4);
        if (ver >= 1) {
            w.i32(0);
        }
        if (ver >= 3) {
            w.i16(top);
            w.arr(members.size());
            for (int i = 0; i < members.size(); i++) {
                w.str(members.get(i)[0]).str(members.get(i)[1]).i16(top != 0 ? top : errs.get(i)).tagged();
            }
        } else {
            w.i16(top != 0 ? top : errs.get(0));
        }
        w.tagged();
        return w;
    }

    KWriter describeGroups(KReader r, int ver) {
        int n = r.arr();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(r.str());
        }
        if (ver >= 3) {
            r.bool();
        }
        r.tagged();
        KWriter w = new KWriter(ver >= 5);
        if (ver >= 1) {
            w.i32(0);
        }
        w.arr(ids.size());
        for (String id : ids) {
            int err = 0;
            GroupCoordinator.Group g = null;
            if (id == null) {
                err = KafkaError.INVALID_GROUP_ID;
            } else if (!isCoordinator(id)) {
                err = KafkaError.NOT_COORDINATOR;
            } else {
                try {
                    g = groups.snapshot(id);
                } catch (RuntimeException e) {
                    err = KafkaError.COORDINATOR_NOT_AVAILABLE;
                }
            }
            w.i16(err).str(id);
            if (g == null) {
                w.str(err == 0 ? GroupCoordinator.DEAD : "").str("").str("").arr(0);
            } else {
                w.str(g.state).str(g.protocolType == null ? "" : g.protocolType)
                        .str(g.protocolName == null || !g.state.equals(GroupCoordinator.STABLE) ? "" : g.protocolName);
                w.arr(g.members.size());
                for (GroupCoordinator.Member m : g.members.values()) {
                    w.str(m.id);
                    if (ver >= 4) {
                        w.str(m.instanceId);
                    }
                    w.str(m.clientId).str(m.clientHost);
                    w.bytes(m.metadataFor(g.protocolName));
                    w.bytes(m.assignment);
                    w.tagged();
                }
            }
            if (ver >= 3) {
                w.i32(Integer.MIN_VALUE);
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter listGroups(KReader r, int ver) {
        List<String> filter = new ArrayList<>();
        if (ver >= 4) {
            int n = r.arr();
            for (int i = 0; i < n; i++) {
                filter.add(r.str().toLowerCase(java.util.Locale.ROOT));
            }
        }
        r.tagged();
        int err = 0;
        Map<String, String[]> all = new java.util.TreeMap<>();
        try {
            for (String[] row : store.groupIds()) {
                all.put(row[0], new String[] {row[1], GroupCoordinator.EMPTY});
            }
            for (GroupCoordinator.Group g : groups.memoryGroups()) {
                GroupCoordinator.Group s = groups.snapshot(g.id);
                if (s != null) {
                    all.put(g.id, new String[] {s.protocolType == null ? "" : s.protocolType, s.state});
                }
            }
        } catch (RuntimeException e) {
            err = KafkaError.COORDINATOR_NOT_AVAILABLE;
        }
        List<Map.Entry<String, String[]>> rows = new ArrayList<>();
        for (Map.Entry<String, String[]> e : all.entrySet()) {
            if (!isCoordinator(e.getKey())) {
                continue;
            }
            if (!filter.isEmpty() && !filter.contains(e.getValue()[1].toLowerCase(java.util.Locale.ROOT))) {
                continue;
            }
            rows.add(e);
        }
        KWriter w = new KWriter(ver >= 3);
        if (ver >= 1) {
            w.i32(0);
        }
        w.i16(err);
        w.arr(rows.size());
        for (Map.Entry<String, String[]> e : rows) {
            w.str(e.getKey()).str(e.getValue()[0]);
            if (ver >= 4) {
                w.str(e.getValue()[1]);
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter deleteGroups(KReader r, int ver) {
        int n = r.arr();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(r.str());
        }
        r.tagged();
        KWriter w = new KWriter(ver >= 2);
        w.i32(0);
        w.arr(ids.size());
        for (String id : ids) {
            int err;
            if (id == null) {
                err = KafkaError.INVALID_GROUP_ID;
            } else if (!isCoordinator(id)) {
                err = KafkaError.NOT_COORDINATOR;
            } else {
                try {
                    err = groups.delete(id);
                } catch (RuntimeException e) {
                    err = KafkaError.COORDINATOR_NOT_AVAILABLE;
                }
            }
            w.str(id).i16(err).tagged();
        }
        w.tagged();
        return w;
    }

    KWriter offsetCommit(KReader r, int ver) {
        String group = r.str();
        int gen = r.i32();
        String member = r.str();
        String instance = ver >= 7 ? r.str() : null;
        if (ver <= 4) {
            r.i64();
        }
        int nt = r.arr();
        List<Object[]> topics = new ArrayList<>();
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arr();
            List<Object[]> parts = new ArrayList<>();
            for (int j = 0; j < np; j++) {
                int part = r.i32();
                long off = r.i64();
                int epoch = ver >= 6 ? r.i32() : -1;
                String meta = r.str();
                r.tagged();
                parts.add(new Object[] {part, off, epoch, meta});
            }
            r.tagged();
            topics.add(new Object[] {topic, parts});
        }
        r.tagged();
        int groupErr = 0;
        if (group == null) {
            groupErr = KafkaError.INVALID_GROUP_ID;
        } else if (!isCoordinator(group)) {
            groupErr = KafkaError.NOT_COORDINATOR;
        } else {
            try {
                groupErr = groups.validateCommit(group, gen, member, instance);
            } catch (RuntimeException e) {
                groupErr = KafkaError.COORDINATOR_NOT_AVAILABLE;
            }
        }
        KWriter w = new KWriter(ver >= 8);
        if (ver >= 3) {
            w.i32(0);
        }
        w.arr(topics.size());
        List<KafkaStore.Committed> toCommit = new ArrayList<>();
        long now = System.currentTimeMillis();
        List<List<Integer>> errsByTopic = new ArrayList<>();
        for (Object[] tp : topics) {
            String topic = (String) tp[0];
            @SuppressWarnings("unchecked")
            List<Object[]> parts = (List<Object[]>) tp[1];
            KafkaStore.Topic t = null;
            try {
                t = store.topic(topic);
            } catch (RuntimeException ignored) {
                // per partition below
            }
            List<Integer> errs = new ArrayList<>();
            for (Object[] p : parts) {
                int err = groupErr;
                String meta = (String) p[3];
                if (err == 0) {
                    if (t == null || (Integer) p[0] < 0 || (Integer) p[0] >= t.partitions()) {
                        err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    } else if (meta != null && meta.length() > 4096) {
                        err = KafkaError.OFFSET_METADATA_TOO_LARGE;
                    } else {
                        toCommit.add(new KafkaStore.Committed(topic, (Integer) p[0], (Long) p[1], (Integer) p[2], meta, now));
                    }
                }
                errs.add(err);
            }
            errsByTopic.add(errs);
        }
        if (!toCommit.isEmpty()) {
            try {
                store.commitOffsets(group, toCommit);
                groups.group(group, true); // a group that only has committed offsets exists (Empty)
            } catch (RuntimeException e) {
                for (List<Integer> errs : errsByTopic) {
                    for (int k = 0; k < errs.size(); k++) {
                        if (errs.get(k) == 0) {
                            errs.set(k, KafkaError.COORDINATOR_NOT_AVAILABLE);
                        }
                    }
                }
            }
        }
        for (int i = 0; i < topics.size(); i++) {
            @SuppressWarnings("unchecked")
            List<Object[]> parts = (List<Object[]>) topics.get(i)[1];
            w.str((String) topics.get(i)[0]);
            w.arr(parts.size());
            for (int k = 0; k < parts.size(); k++) {
                w.i32((Integer) parts.get(k)[0]).i16(errsByTopic.get(i).get(k)).tagged();
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter offsetFetch(KReader r, int ver) {
        String group = r.str();
        int nt = r.arr();
        Map<String, List<Integer>> want = null;
        if (nt >= 0) {
            want = new LinkedHashMap<>();
            for (int i = 0; i < nt; i++) {
                String topic = r.str();
                int np = r.arr();
                List<Integer> ps = new ArrayList<>();
                for (int j = 0; j < np; j++) {
                    ps.add(r.i32());
                }
                r.tagged();
                want.put(topic, ps);
            }
        }
        if (ver >= 7) {
            r.bool();
        }
        r.tagged();
        int err = 0;
        List<KafkaStore.Committed> have = List.of();
        if (group == null) {
            err = KafkaError.INVALID_GROUP_ID;
        } else if (!isCoordinator(group)) {
            err = KafkaError.NOT_COORDINATOR;
        } else {
            try {
                have = store.committed(group, null);
            } catch (RuntimeException e) {
                err = KafkaError.COORDINATOR_NOT_AVAILABLE;
            }
        }
        Map<String, Map<Integer, KafkaStore.Committed>> idx = new LinkedHashMap<>();
        for (KafkaStore.Committed c : have) {
            idx.computeIfAbsent(c.topic(), k -> new LinkedHashMap<>()).put(c.partition(), c);
        }
        KWriter w = new KWriter(ver >= 6);
        if (ver >= 3) {
            w.i32(0);
        }
        Map<String, List<Integer>> plan = new LinkedHashMap<>();
        if (err == 0) {
            if (want == null) {
                for (Map.Entry<String, Map<Integer, KafkaStore.Committed>> e : idx.entrySet()) {
                    plan.put(e.getKey(), new ArrayList<>(e.getValue().keySet()));
                }
            } else {
                plan.putAll(want);
            }
        }
        w.arr(plan.size());
        for (Map.Entry<String, List<Integer>> e : plan.entrySet()) {
            w.str(e.getKey());
            w.arr(e.getValue().size());
            for (int p : e.getValue()) {
                KafkaStore.Committed c = idx.getOrDefault(e.getKey(), Map.of()).get(p);
                w.i32(p);
                if (c == null) {
                    w.i64(-1);
                    if (ver >= 5) {
                        w.i32(-1);
                    }
                    w.str("");
                } else {
                    w.i64(c.offset());
                    if (ver >= 5) {
                        w.i32(c.leaderEpoch());
                    }
                    w.str(c.metadata() == null ? "" : c.metadata());
                }
                w.i16(0).tagged();
            }
            w.tagged();
        }
        if (ver >= 2) {
            w.i16(err);
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ topic administration

    private void topicConfigs(KWriter w, Map<String, String> overrides) {
        w.arr(KafkaConfigs.TOPIC.size());
        for (KafkaConfigs.Def d : KafkaConfigs.TOPIC) {
            boolean ov = overrides.containsKey(d.name());
            w.str(d.name()).str(ov ? overrides.get(d.name()) : d.def()).bool(false).i8(ov ? KafkaConfigs.SRC_TOPIC : KafkaConfigs.SRC_DEFAULT).bool(false).tagged();
        }
    }

    KWriter createTopics(KReader r, int ver) {
        int n = r.arr();
        List<Object[]> reqs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String name = r.str();
            int parts = r.i32();
            int rf = r.i16();
            int na = r.arr();
            List<List<Integer>> assign = new ArrayList<>();
            boolean sequential = true;
            for (int a = 0; a < na; a++) {
                sequential &= r.i32() == a;
                int nb = r.arr();
                List<Integer> ids = new ArrayList<>();
                for (int b = 0; b < nb; b++) {
                    ids.add(r.i32());
                }
                r.tagged();
                assign.add(ids);
            }
            int nc = r.arr();
            Map<String, String> cfg = new LinkedHashMap<>();
            for (int c = 0; c < nc; c++) {
                String k = r.str();
                String v = r.str();
                r.tagged();
                cfg.put(k, v);
            }
            r.tagged();
            reqs.add(new Object[] {name, parts, rf, assign, cfg, sequential});
        }
        r.i32();
        boolean validateOnly = ver >= 1 && r.bool();
        r.tagged();
        KWriter w = new KWriter(ver >= 5);
        w.i32(0);
        Map<String, Integer> occurrences = new HashMap<>();
        for (Object[] q : reqs) {
            occurrences.merge((String) q[0], 1, Integer::sum);
        }
        List<Object[]> unique = new ArrayList<>();
        java.util.Set<String> listed = new java.util.HashSet<>();
        for (Object[] q : reqs) {
            if (listed.add((String) q[0])) {
                unique.add(q);
            }
        }
        w.arr(unique.size());
        for (Object[] q : unique) {
            String name = (String) q[0];
            int parts = (Integer) q[1];
            int rf = (Integer) q[2];
            @SuppressWarnings("unchecked")
            List<List<Integer>> assign = (List<List<Integer>>) q[3];
            @SuppressWarnings("unchecked")
            Map<String, String> cfg = (Map<String, String>) q[4];
            int err = 0;
            String msg = null;
            KafkaStore.Topic created = null;
            try {
                String bad = validateTopicName(name);
                if (occurrences.get(name) > 1) {
                    err = KafkaError.INVALID_REQUEST;
                    msg = "Duplicate topic name.";
                } else if (bad != null) {
                    err = KafkaError.INVALID_TOPIC_EXCEPTION;
                    msg = bad;
                } else if (!assign.isEmpty() && (parts != -1 || rf != -1)) {
                    err = KafkaError.INVALID_REQUEST;
                    msg = "Both numPartitions or replicationFactor and replicasAssignments were set. Both cannot be used at the same time.";
                } else {
                    int np = assign.isEmpty() ? (parts == -1 ? defaultPartitions : parts) : assign.size();
                    int factor = !assign.isEmpty() ? assign.get(0).size() : rf == -1 ? 1 : rf;
                    if (assign.isEmpty() && parts != -1 && parts <= 0) {
                        err = KafkaError.INVALID_PARTITIONS;
                        msg = "Number of partitions was set to an invalid non-positive value.";
                    } else if (assign.isEmpty() && rf != -1 && rf <= 0) {
                        err = KafkaError.INVALID_REPLICATION_FACTOR;
                        msg = "Replication factor must be larger than 0, or -1 to use the default value.";
                    } else if (!assign.isEmpty() && !validAssignment(assign, (Boolean) q[5])) {
                        err = KafkaError.INVALID_REPLICA_ASSIGNMENT;
                        msg = "Inconsistent replica assignment: partitions must be numbered from 0, every partition needs the same number of existing brokers.";
                    } else if (factor > brokers().size()) {
                        err = KafkaError.INVALID_REPLICATION_FACTOR;
                        msg = "Replication factor: " + factor + " larger than available brokers: " + brokers().size() + ".";
                    } else {
                        for (Map.Entry<String, String> c : cfg.entrySet()) {
                            String cm = KafkaConfigs.validate(c.getKey(), c.getValue());
                            if (cm != null) {
                                err = KafkaError.INVALID_CONFIG;
                                msg = cm;
                                break;
                            }
                        }
                        if (err == 0 && store.topic(name) != null) {
                            err = KafkaError.TOPIC_ALREADY_EXISTS;
                            msg = "Topic '" + name + "' already exists.";
                        }
                        if (err == 0 && !validateOnly) {
                            created = store.createTopic(name, np, cfg);
                        } else if (err == 0) {
                            created = new KafkaStore.Topic(name, new UUID(0, 0), np, cfg, 0);
                        }
                    }
                }
            } catch (KafkaError e) {
                err = e.code;
                msg = e.getMessage();
            } catch (RuntimeException e) {
                err = KafkaError.UNKNOWN_SERVER_ERROR;
                msg = "internal error";
            }
            w.str(name);
            if (ver >= 7) {
                w.uuid(created == null ? new UUID(0, 0) : created.id());
            }
            w.i16(err);
            if (ver >= 1) {
                w.str(msg);
            }
            if (ver >= 5) {
                if (created != null) {
                    w.i32(created.partitions()).i16(1);
                    topicConfigs(w, created.config());
                } else {
                    w.i32(-1).i16(-1).arr(-1);
                }
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    private boolean validAssignment(List<List<Integer>> assign, boolean sequential) {
        if (!sequential) {
            return false;
        }
        java.util.Set<Integer> live = new java.util.HashSet<>();
        brokers().forEach(b -> live.add(b.id()));
        int size = assign.get(0).size();
        for (List<Integer> a : assign) {
            if (a.isEmpty() || a.size() != size || !live.containsAll(a) || new java.util.HashSet<>(a).size() != a.size()) {
                return false;
            }
        }
        return true;
    }

    KWriter deleteTopics(KReader r, int ver) {
        int n = r.arr();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            names.add(r.str());
        }
        r.i32();
        r.tagged();
        KWriter w = new KWriter(ver >= 4);
        if (ver >= 1) {
            w.i32(0);
        }
        w.arr(names.size());
        for (String name : names) {
            int err = 0;
            String msg = null;
            try {
                if (name == null || store.topic(name) == null) {
                    err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    msg = "This server does not host this topic-partition.";
                } else {
                    store.deleteTopic(name);
                }
            } catch (RuntimeException e) {
                err = KafkaError.UNKNOWN_SERVER_ERROR;
                msg = "internal error";
            }
            w.str(name).i16(err);
            if (ver >= 5) {
                w.str(msg);
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter createPartitions(KReader r, int ver) {
        int n = r.arr();
        List<Object[]> reqs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String name = r.str();
            int count = r.i32();
            int na = r.arr();
            for (int a = 0; a < na; a++) {
                int nb = r.arr();
                for (int b = 0; b < nb; b++) {
                    r.i32();
                }
                r.tagged();
            }
            r.tagged();
            reqs.add(new Object[] {name, count, na});
        }
        r.i32();
        boolean validateOnly = r.bool();
        r.tagged();
        KWriter w = new KWriter(ver >= 2);
        w.i32(0);
        w.arr(reqs.size());
        for (Object[] q : reqs) {
            String name = (String) q[0];
            int count = (Integer) q[1];
            int err = 0;
            String msg = null;
            try {
                KafkaStore.Topic t = store.topic(name);
                if (t == null) {
                    err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    msg = "This server does not host this topic-partition.";
                } else if (count == t.partitions()) {
                    err = KafkaError.INVALID_PARTITIONS;
                    msg = "Topic already has " + t.partitions() + " partitions.";
                } else if (count < t.partitions()) {
                    err = KafkaError.INVALID_PARTITIONS;
                    msg = "Topic currently has " + t.partitions() + " partitions, which is higher than the requested " + count + ".";
                } else if ((Integer) q[2] > 0 && (Integer) q[2] != count - t.partitions()) {
                    err = KafkaError.INVALID_REPLICA_ASSIGNMENT;
                    msg = "Replica assignment has " + q[2] + " partitions, but " + (count - t.partitions()) + " were expected";
                } else if (!validateOnly) {
                    store.addPartitions(name, count);
                }
            } catch (RuntimeException e) {
                err = KafkaError.UNKNOWN_SERVER_ERROR;
                msg = "internal error";
            }
            w.str(name).i16(err).str(msg).tagged();
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ configs

    KWriter describeConfigs(KReader r, int ver) {
        int n = r.arr();
        List<Object[]> reqs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int type = r.i8();
            String name = r.str();
            int nk = r.arr();
            List<String> keys = null;
            if (nk >= 0) {
                keys = new ArrayList<>();
                for (int k = 0; k < nk; k++) {
                    keys.add(r.str());
                }
            }
            r.tagged();
            reqs.add(new Object[] {type, name, keys});
        }
        if (ver >= 1) {
            r.bool();
        }
        if (ver >= 3) {
            r.bool();
        }
        r.tagged();
        KWriter w = new KWriter(ver >= 4);
        w.i32(0);
        w.arr(reqs.size());
        for (Object[] q : reqs) {
            int type = (Integer) q[0];
            String name = (String) q[1];
            @SuppressWarnings("unchecked")
            List<String> keys = (List<String>) q[2];
            int err = 0;
            String msg = null;
            List<Object[]> rows = new ArrayList<>();
            if (type == 2) {
                KafkaStore.Topic t = null;
                try {
                    t = store.topic(name);
                } catch (RuntimeException e) {
                    err = KafkaError.UNKNOWN_SERVER_ERROR;
                }
                if (t == null && err == 0) {
                    err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                    msg = "This server does not host this topic-partition.";
                } else if (t != null) {
                    for (KafkaConfigs.Def d : KafkaConfigs.TOPIC) {
                        if (keys != null && !keys.contains(d.name())) {
                            continue;
                        }
                        boolean ov = t.config().containsKey(d.name());
                        rows.add(new Object[] {d.name(), ov ? t.config().get(d.name()) : d.def(), ov ? KafkaConfigs.SRC_TOPIC : KafkaConfigs.SRC_DEFAULT, d.type()});
                    }
                }
            } else if (type == 4) {
                if (name != null && !name.isEmpty() && !name.equals(String.valueOf(nodeId))) {
                    err = KafkaError.INVALID_REQUEST;
                    msg = "Unexpected broker id, expected " + nodeId + " or empty string, but received " + name;
                } else if (name != null && !name.isEmpty()) {
                    for (KafkaConfigs.Def d : KafkaConfigs.BROKER) {
                        if (keys != null && !keys.contains(d.name())) {
                            continue;
                        }
                        rows.add(new Object[] {d.name(), d.def(), KafkaConfigs.SRC_DEFAULT, d.type()});
                    }
                }
            } else if (type == 8) {
                // broker loggers: nothing to list
                err = 0;
            } else {
                err = KafkaError.INVALID_REQUEST;
                msg = "Unsupported resource type: " + type;
            }
            w.i16(err).str(msg).i8(type).str(name);
            w.arr(rows.size());
            for (Object[] c : rows) {
                w.str((String) c[0]).str((String) c[1]).bool(false).i8((Integer) c[2]).bool(false);
                w.arr(0);
                if (ver >= 3) {
                    w.i8((Integer) c[3]);
                    w.str(null);
                }
                w.tagged();
            }
            w.tagged();
        }
        w.tagged();
        return w;
    }

    KWriter alterConfigs(KReader r, int ver, boolean incremental) {
        int n = r.arr();
        List<Object[]> reqs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int type = r.i8();
            String name = r.str();
            int nc = r.arr();
            List<String[]> ops = new ArrayList<>();
            for (int c = 0; c < nc; c++) {
                String k = r.str();
                String op = incremental ? String.valueOf(r.i8()) : "0";
                String v = r.str();
                r.tagged();
                ops.add(new String[] {k, op, v});
            }
            r.tagged();
            reqs.add(new Object[] {type, name, ops});
        }
        boolean validateOnly = r.bool();
        r.tagged();
        KWriter w = new KWriter(ver >= (incremental ? 1 : 2));
        w.i32(0);
        w.arr(reqs.size());
        for (Object[] q : reqs) {
            int type = (Integer) q[0];
            String name = (String) q[1];
            @SuppressWarnings("unchecked")
            List<String[]> ops = (List<String[]>) q[2];
            int err = 0;
            String msg = null;
            try {
                if (type != 2) {
                    err = type == 4 ? KafkaError.INVALID_REQUEST : KafkaError.INVALID_REQUEST;
                    msg = type == 4 ? "Broker configuration changes are not supported by this broker" : "Unsupported resource type: " + type;
                } else {
                    KafkaStore.Topic t = store.topic(name);
                    if (t == null) {
                        err = KafkaError.UNKNOWN_TOPIC_OR_PARTITION;
                        msg = "This server does not host this topic-partition.";
                    } else {
                        Map<String, String> cfg = incremental ? new LinkedHashMap<>(t.config()) : new LinkedHashMap<>();
                        for (String[] op : ops) {
                            KafkaConfigs.Def d = KafkaConfigs.def(op[0]);
                            if (d == null) {
                                err = KafkaError.INVALID_CONFIG;
                                msg = "Unknown topic config name: " + op[0];
                                break;
                            }
                            String o = op[1];
                            if (o.equals("1")) {
                                cfg.remove(op[0]);
                            } else if (o.equals("2") || o.equals("3")) {
                                if (d.type() != KafkaConfigs.LIST) {
                                    err = KafkaError.INVALID_CONFIG;
                                    msg = "Config value append is not allowed for config key: " + op[0];
                                    break;
                                }
                                List<String> cur = new ArrayList<>();
                                String base = cfg.getOrDefault(op[0], d.def());
                                for (String s : base.split(",")) {
                                    if (!s.isBlank()) {
                                        cur.add(s.trim());
                                    }
                                }
                                for (String s : op[2] == null ? new String[0] : op[2].split(",")) {
                                    if (o.equals("2") && !cur.contains(s.trim())) {
                                        cur.add(s.trim());
                                    } else if (o.equals("3")) {
                                        cur.remove(s.trim());
                                    }
                                }
                                cfg.put(op[0], String.join(",", cur));
                            } else if (op[2] == null) {
                                if (incremental) {
                                    err = KafkaError.INVALID_REQUEST;
                                    msg = "Null value not supported for : " + op[0];
                                    break;
                                }
                                cfg.remove(op[0]);
                            } else {
                                String bad = KafkaConfigs.validate(op[0], op[2]);
                                if (bad != null) {
                                    err = KafkaError.INVALID_CONFIG;
                                    msg = bad;
                                    break;
                                }
                                cfg.put(op[0], op[2]);
                            }
                        }
                        if (err == 0 && !validateOnly) {
                            store.setConfig(name, cfg);
                        }
                    }
                }
            } catch (RuntimeException e) {
                err = KafkaError.UNKNOWN_SERVER_ERROR;
                msg = "internal error";
            }
            w.i16(err).str(msg).i8(type).str(name).tagged();
        }
        w.tagged();
        return w;
    }

    // ------------------------------------------------------------------ DescribeCluster

    KWriter describeCluster(KReader r, int ver) {
        r.bool();
        int endpointType = ver >= 1 ? r.i8() : 1;
        r.tagged();
        KWriter w = new KWriter(true);
        w.i32(0).i16(0).str(null);
        if (ver >= 1) {
            w.i8(endpointType);
        }
        w.str(store.clusterId());
        List<KafkaStore.Broker> bs = brokers();
        w.i32(bs.get(0).id());
        w.arr(bs.size());
        for (KafkaStore.Broker b : bs) {
            w.i32(b.id()).str(b.host()).i32(b.port()).str(null).tagged();
        }
        w.i32(Integer.MIN_VALUE);
        w.tagged();
        return w;
    }
}
