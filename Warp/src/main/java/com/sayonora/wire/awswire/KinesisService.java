package com.sayonora.wire.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon Kinesis Data Streams on Postgres (the {@code kinesis} store), JSON 1.1 and CBOR (AWS SDK v2's default).
 *
 * <p><b>Sharding</b>: a stream (shards, records, consumers) lives wholly on the host owning hash(stream name); ListStreams
 * fans out. <b>Sequence numbers</b>: every shard has a counter row; PutRecord(s) increment it with
 * {@code UPDATE ... RETURNING} inside the same transaction that inserts the records, so concurrent producers to one shard are
 * serialized on that row and receive strictly increasing numbers in commit order (a reader never sees number N+1 before N).
 * A sequence number is a fixed 36-digit shard prefix plus the 20-digit counter, so numeric and lexical order agree.
 * <b>Partition keys</b> hash exactly like Kinesis (MD5 as a 128-bit integer) into the shard hash ranges. Records older than
 * the stream's retention period are deleted by a sweeper. Iterators are opaque, carry the stream name (routing) and expire
 * after 5 minutes like the real ones.
 */
public final class KinesisService extends AwsService {

    private static final Logger log = LoggerFactory.getLogger(KinesisService.class);
    private static final Pattern NAME = Pattern.compile("^[a-zA-Z0-9_.-]{1,128}$");
    private static final int MAX_RECORD_BYTES = 1024 * 1024;
    private static final long ITERATOR_TTL_MS = 5 * 60 * 1000L;
    private static final int MAX_SHARDS = 1000;

    private final AwsRuntime rt;
    private final AwsShards sh;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kinesiswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public KinesisService(AwsRuntime rt) {
        this.rt = rt;
        this.sh = new AwsShards(rt.registry, StoreType.KINESIS);
    }

    @Override
    public String id() {
        return "kinesis";
    }

    @Override
    public String metricsProtocol() {
        return "kinesiswire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("kinesis");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of("Kinesis_20131202.");
    }

    @Override
    public boolean supportsCbor() {
        return true;
    }

    @Override
    public Set<String> blobFields() {
        return Set.of("Data");
    }

    @Override
    public Set<String> timestampFields() {
        return Set.of("StreamCreationTimestamp", "ApproximateArrivalTimestamp", "ConsumerCreationTimestamp");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public String backendLabel(String op, JsonObject req) {
        try {
            String n = req == null ? null : streamNameOf(req);
            return n == null || !sh.available() ? "default" : sh.owner(n);
        } catch (RuntimeException e) {
            return "default";
        }
    }

    @Override
    public void start() {
        long every = 60;
        try {
            String v = System.getenv("WARP_KINESISWIRE_SWEEP_SECONDS");
            if (v != null && !v.isBlank()) {
                every = Math.max(1, Long.parseLong(v.trim()));
            }
        } catch (NumberFormatException ignored) {
            // default
        }
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (sh.available()) {
                    for (String h : sh.hosts()) {
                        long n = sh.conn(h, c -> {
                            try (var st = c.createStatement()) {
                                return (long) st.executeUpdate("DELETE FROM warp_kinesis_records r USING warp_kinesis_streams s "
                                        + "WHERE r.stream = s.name AND r.arrived < now() - make_interval(hours => s.retention_hours)");
                            }
                        });
                        if (n > 0) {
                            log.info("kinesiswire: retention sweeper removed {} expired record(s)", n);
                        }
                    }
                }
            } catch (RuntimeException e) {
                log.debug("kinesiswire sweep failed: {}", e.getMessage());
            }
        }, every, every, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        sweeper.shutdownNow();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return switch (op) {
            case "PutRecord", "PutRecords" -> SqlMetricsCollector.StatementKind.WRITE;
            case "GetRecords", "GetShardIterator", "ListShards", "ListStreams", "DescribeStream", "DescribeStreamSummary",
                    "ListTagsForStream", "DescribeLimits", "DescribeStreamConsumer", "ListStreamConsumers",
                    "ListTagsForResource", "GetResourcePolicy" -> SqlMetricsCollector.StatementKind.READ;
            default -> SqlMetricsCollector.StatementKind.WRITE;
        };
    }

    // ---------------------------------------------------------------------------------------------- helpers

    private static AwsException notFound(String stream, String account) {
        return new AwsException(400, "ResourceNotFoundException", "Stream " + stream + " under account " + account + " not found.");
    }

    private static AwsException invalid(String msg) {
        return new AwsException(400, "InvalidArgumentException", msg);
    }

    private String streamNameOf(JsonObject req) {
        String n = Args.str(req, "StreamName");
        if (n != null) {
            return n;
        }
        String arn = Args.str(req, "StreamARN");
        if (arn == null) {
            arn = Args.str(req, "ResourceARN");
        }
        if (arn == null) {
            String it = Args.str(req, "ShardIterator");
            if (it != null) {
                return decodeIterator(it)[0];
            }
            String c = Args.str(req, "ConsumerARN");
            if (c != null && c.contains("stream/")) {
                String rest = c.substring(c.indexOf("stream/") + 7);
                return rest.substring(0, rest.indexOf('/'));
            }
            return null;
        }
        int i = arn.indexOf(":stream/");
        if (i < 0) {
            throw invalid("Invalid StreamARN: " + arn);
        }
        String rest = arn.substring(i + 8);
        int slash = rest.indexOf('/');
        return slash < 0 ? rest : rest.substring(0, slash);
    }

    private String needStreamName(JsonObject req) {
        String n = streamNameOf(req);
        if (n == null || n.isEmpty()) {
            throw invalid("Either StreamName or StreamARN must be specified");
        }
        return n;
    }

    private String arnOf(String name) {
        return rt.config.arn("kinesis", "stream/" + name);
    }

    private record Stream(String name, String arn, String status, String mode, int retention, Timestamp created,
            Map<String, String> tags, String encryption, String keyId, String metrics, String policy, int nextShard) {
    }

    private record Shard(String id, int idx, BigInteger start, BigInteger end, String parent, String adjacent, long nextSeq,
            boolean closed, Long endSeq) {
    }

    private Stream stream(Connection c, String name, boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT arn, status, mode, retention_hours, created_at, tags::text, encryption_type, "
                + "key_id, metrics::text, policy, next_shard_index FROM warp_kinesis_streams WHERE name = ?" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw notFound(name, rt.config.accountId);
                }
                Map<String, String> tags = new TreeMap<>();
                JsonParser.parseString(rs.getString(6)).getAsJsonObject().entrySet().forEach(e -> tags.put(e.getKey(), e.getValue().getAsString()));
                return new Stream(name, rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getTimestamp(5), tags,
                        rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10), rs.getInt(11));
            }
        }
    }

    private static List<Shard> shards(Connection c, String stream) throws SQLException {
        List<Shard> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT shard_id, idx, hash_start, hash_end, parent, adjacent_parent, next_seq, closed, "
                + "end_seq FROM warp_kinesis_shards WHERE stream = ? ORDER BY idx")) {
            ps.setString(1, stream);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long e = rs.getLong(9);
                    out.add(new Shard(rs.getString(1), rs.getInt(2), rs.getBigDecimal(3).toBigInteger(), rs.getBigDecimal(4).toBigInteger(),
                            rs.getString(5), rs.getString(6), rs.getLong(7), rs.getBoolean(8), rs.wasNull() ? null : e));
                }
            }
        }
        return out;
    }

    private static void insertShard(Connection c, String stream, String id, int idx, BigInteger s, BigInteger e, String parent,
            String adj) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kinesis_shards (stream, shard_id, idx, hash_start, hash_end, "
                + "parent, adjacent_parent) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, stream);
            ps.setString(2, id);
            ps.setInt(3, idx);
            ps.setBigDecimal(4, new BigDecimal(s));
            ps.setBigDecimal(5, new BigDecimal(e));
            ps.setString(6, parent);
            ps.setString(7, adj);
            ps.executeUpdate();
        }
    }

    private static double epoch(Timestamp t) {
        return t.getTime() / 1000.0;
    }

    // ---------------------------------------------------------------------------------------------- dispatch

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        return switch (op) {
            case "CreateStream" -> createStream(req);
            case "DeleteStream" -> deleteStream(req);
            case "DescribeStream" -> describeStream(req);
            case "DescribeStreamSummary" -> describeSummary(req);
            case "ListStreams" -> listStreams(req);
            case "ListShards" -> listShards(req);
            case "PutRecord" -> putRecord(req);
            case "PutRecords" -> putRecords(req);
            case "GetShardIterator" -> getShardIterator(req);
            case "GetRecords" -> getRecords(req);
            case "SplitShard" -> splitShard(req);
            case "MergeShards" -> mergeShards(req);
            case "UpdateShardCount" -> updateShardCount(req);
            case "IncreaseStreamRetentionPeriod" -> retention(req, true);
            case "DecreaseStreamRetentionPeriod" -> retention(req, false);
            case "AddTagsToStream" -> addTags(req);
            case "RemoveTagsFromStream" -> removeTags(req);
            case "ListTagsForStream" -> listTags(req);
            case "TagResource" -> addTags(req);
            case "UntagResource" -> removeTags(req);
            case "ListTagsForResource" -> listTags(req);
            case "EnableEnhancedMonitoring" -> monitoring(req, true);
            case "DisableEnhancedMonitoring" -> monitoring(req, false);
            case "UpdateStreamMode" -> updateMode(req);
            case "StartStreamEncryption" -> encryption(req, true);
            case "StopStreamEncryption" -> encryption(req, false);
            case "DescribeLimits" -> describeLimits();
            case "RegisterStreamConsumer" -> registerConsumer(req);
            case "DeregisterStreamConsumer" -> deregisterConsumer(req);
            case "DescribeStreamConsumer" -> describeConsumer(req);
            case "ListStreamConsumers" -> listConsumers(req);
            case "PutResourcePolicy" -> putPolicy(req);
            case "GetResourcePolicy" -> getPolicy(req);
            case "DeleteResourcePolicy" -> deletePolicy(req);
            case "SubscribeToShard" -> throw new AwsException(400, "InvalidArgumentException",
                    "SubscribeToShard is an HTTP/2 event stream: use an HTTP/2 (h2c prior knowledge) connection, as the AWS SDK async clients do");
            default -> throw new AwsException(400, "UnknownOperationException", "Unknown operation " + op);
        };
    }

    // ---------------------------------------------------------------------------------------------- streams

    private JsonObject createStream(JsonObject req) {
        String name = Args.req(req, "StreamName");
        if (!NAME.matcher(name).matches()) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value '" + name
                    + "' at 'streamName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
        }
        JsonObject modeObj = Args.obj(req, "StreamModeDetails");
        String mode = modeObj == null ? "PROVISIONED" : Args.str(modeObj, "StreamMode");
        if (!mode.equals("PROVISIONED") && !mode.equals("ON_DEMAND")) {
            throw invalid("Invalid StreamMode " + mode);
        }
        Integer count = Args.integer(req, "ShardCount");
        if (count == null) {
            if (mode.equals("PROVISIONED")) {
                throw invalid("ShardCount must be specified when StreamMode is PROVISIONED");
            }
            count = 4;
        }
        if (count < 1) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value '" + count
                    + "' at 'shardCount' failed to satisfy constraint: Member must have value greater than or equal to 1");
        }
        if (count > MAX_SHARDS) {
            throw new AwsException(400, "LimitExceededException", "This request would exceed the shard limit for the account "
                    + rt.config.accountId + " in " + rt.config.region + ".");
        }
        Map<String, String> tags = new TreeMap<>();
        JsonObject tagObj = Args.obj(req, "Tags");
        if (tagObj != null) {
            tagObj.entrySet().forEach(e -> tags.put(e.getKey(), e.getValue().getAsString()));
        }
        int n = count;
        String md = mode;
        sh.tx(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kinesis_streams (name, arn, mode, tags, next_shard_index) "
                    + "VALUES (?, ?, ?, ?::jsonb, ?) ON CONFLICT (name) DO NOTHING")) {
                ps.setString(1, name);
                ps.setString(2, arnOf(name));
                ps.setString(3, md);
                JsonObject t = new JsonObject();
                tags.forEach(t::addProperty);
                ps.setString(4, t.toString());
                ps.setInt(5, n);
                if (ps.executeUpdate() == 0) {
                    throw new AwsException(400, "ResourceInUseException", "Stream " + name + " under account " + rt.config.accountId
                            + " already exists.");
                }
            }
            int i = 0;
            for (BigInteger[] r : KinesisHash.evenRanges(n)) {
                insertShard(c, name, KinesisHash.shardId(i), i, r[0], r[1], null, null);
                i++;
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject deleteStream(JsonObject req) {
        String name = needStreamName(req);
        sh.tx(sh.owner(name), c -> {
            stream(c, name, true);
            for (String t : new String[] {"warp_kinesis_records", "warp_kinesis_shards", "warp_kinesis_consumers"}) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE stream = ?")) {
                    ps.setString(1, name);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kinesis_streams WHERE name = ?")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject shardJson(String stream, Shard s) {
        JsonObject o = new JsonObject();
        o.addProperty("ShardId", s.id);
        if (s.parent != null) {
            o.addProperty("ParentShardId", s.parent);
        }
        if (s.adjacent != null) {
            o.addProperty("AdjacentParentShardId", s.adjacent);
        }
        JsonObject hr = new JsonObject();
        hr.addProperty("StartingHashKey", s.start.toString());
        hr.addProperty("EndingHashKey", s.end.toString());
        o.add("HashKeyRange", hr);
        JsonObject sr = new JsonObject();
        sr.addProperty("StartingSequenceNumber", KinesisHash.seqNumber(stream, s.id, 0));
        if (s.closed && s.endSeq != null) {
            sr.addProperty("EndingSequenceNumber", KinesisHash.seqNumber(stream, s.id, s.endSeq));
        }
        o.add("SequenceNumberRange", sr);
        return o;
    }

    private static JsonArray monitoringJson(String metrics) {
        JsonArray a = new JsonArray();
        JsonObject o = new JsonObject();
        o.add("ShardLevelMetrics", JsonParser.parseString(metrics));
        a.add(o);
        return a;
    }

    private JsonObject describeStream(JsonObject req) {
        String name = needStreamName(req);
        Integer limit = Args.integer(req, "Limit");
        String after = Args.str(req, "ExclusiveStartShardId");
        return sh.conn(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            List<Shard> all = shards(c, name);
            JsonArray arr = new JsonArray();
            int max = limit == null ? 100 : Math.max(1, Math.min(limit, 10000));
            boolean more = false;
            for (Shard sd : all) {
                if (after != null && sd.id.compareTo(after) <= 0) {
                    continue;
                }
                if (arr.size() >= max) {
                    more = true;
                    break;
                }
                arr.add(shardJson(name, sd));
            }
            JsonObject d = new JsonObject();
            d.addProperty("StreamName", name);
            d.addProperty("StreamARN", s.arn);
            d.addProperty("StreamStatus", s.status);
            JsonObject md = new JsonObject();
            md.addProperty("StreamMode", s.mode);
            d.add("StreamModeDetails", md);
            d.add("Shards", arr);
            d.addProperty("HasMoreShards", more);
            d.addProperty("RetentionPeriodHours", s.retention);
            d.addProperty("StreamCreationTimestamp", epoch(s.created));
            d.add("EnhancedMonitoring", monitoringJson(s.metrics));
            d.addProperty("EncryptionType", s.encryption);
            if (s.keyId != null) {
                d.addProperty("KeyId", s.keyId);
            }
            JsonObject out = new JsonObject();
            out.add("StreamDescription", d);
            return out;
        });
    }

    private JsonObject describeSummary(JsonObject req) {
        String name = needStreamName(req);
        return sh.conn(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            long open = shards(c, name).stream().filter(x -> !x.closed).count();
            long consumers;
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_kinesis_consumers WHERE stream = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    consumers = rs.getLong(1);
                }
            }
            JsonObject d = new JsonObject();
            d.addProperty("StreamName", name);
            d.addProperty("StreamARN", s.arn);
            d.addProperty("StreamStatus", s.status);
            JsonObject md = new JsonObject();
            md.addProperty("StreamMode", s.mode);
            d.add("StreamModeDetails", md);
            d.addProperty("RetentionPeriodHours", s.retention);
            d.addProperty("StreamCreationTimestamp", epoch(s.created));
            d.add("EnhancedMonitoring", monitoringJson(s.metrics));
            d.addProperty("EncryptionType", s.encryption);
            if (s.keyId != null) {
                d.addProperty("KeyId", s.keyId);
            }
            d.addProperty("OpenShardCount", open);
            d.addProperty("ConsumerCount", consumers);
            JsonObject out = new JsonObject();
            out.add("StreamDescriptionSummary", d);
            return out;
        });
    }

    private JsonObject listStreams(JsonObject req) {
        Integer limit = Args.integer(req, "Limit");
        String after = Args.str(req, "ExclusiveStartStreamName");
        String token = Args.str(req, "NextToken");
        if (token != null) {
            after = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        }
        String from = after;
        Map<String, Object[]> all = new TreeMap<>();
        for (List<Object[]> l : sh.<List<Object[]>>onAll(c -> {
            List<Object[]> r = new ArrayList<>();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT name, arn, status, mode, created_at FROM warp_kinesis_streams")) {
                while (rs.next()) {
                    r.add(new Object[] {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getTimestamp(5)});
                }
            }
            return r;
        })) {
            for (Object[] r : l) {
                all.put((String) r[0], r);
            }
        }
        int max = limit == null ? 100 : Math.max(1, Math.min(limit, 10000));
        JsonArray names = new JsonArray();
        JsonArray summaries = new JsonArray();
        boolean more = false;
        String last = null;
        for (var e : all.entrySet()) {
            if (from != null && e.getKey().compareTo(from) <= 0) {
                continue;
            }
            if (names.size() >= max) {
                more = true;
                break;
            }
            names.add(e.getKey());
            last = e.getKey();
            JsonObject s = new JsonObject();
            s.addProperty("StreamName", e.getKey());
            s.addProperty("StreamARN", (String) e.getValue()[1]);
            s.addProperty("StreamStatus", (String) e.getValue()[2]);
            JsonObject md = new JsonObject();
            md.addProperty("StreamMode", (String) e.getValue()[3]);
            s.add("StreamModeDetails", md);
            s.addProperty("StreamCreationTimestamp", epoch((Timestamp) e.getValue()[4]));
            summaries.add(s);
        }
        JsonObject out = new JsonObject();
        out.add("StreamNames", names);
        out.addProperty("HasMoreStreams", more);
        out.add("StreamSummaries", summaries);
        if (more && last != null) {
            out.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    private JsonObject listShards(JsonObject req) {
        String token = Args.str(req, "NextToken");
        String name;
        String after = Args.str(req, "ExclusiveStartShardId");
        if (token != null) {
            String[] parts = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8).split("\u0000", 2);
            name = parts[0];
            after = parts.length > 1 ? parts[1] : null;
        } else {
            name = needStreamName(req);
        }
        Integer max = Args.integer(req, "MaxResults");
        int limit = max == null ? 1000 : Math.max(1, Math.min(max, 1000));
        JsonObject filter = Args.obj(req, "ShardFilter");
        String afterId = after;
        return sh.conn(sh.owner(name), c -> {
            stream(c, name, false);
            JsonArray arr = new JsonArray();
            boolean more = false;
            String lastId = null;
            for (Shard sd : shards(c, name)) {
                if (afterId != null && sd.id.compareTo(afterId) <= 0) {
                    continue;
                }
                if (filter != null && "AT_LATEST".equals(Args.str(filter, "Type")) && sd.closed) {
                    continue;
                }
                if (arr.size() >= limit) {
                    more = true;
                    break;
                }
                arr.add(shardJson(name, sd));
                lastId = sd.id;
            }
            JsonObject out = new JsonObject();
            out.add("Shards", arr);
            if (more) {
                out.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(
                        (name + "\u0000" + lastId).getBytes(StandardCharsets.UTF_8)));
            }
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- put

    private record Put(byte[] data, String pk, BigInteger hash) {
    }

    private Put parsePut(JsonObject r) {
        String data = Args.str(r, "Data");
        String pk = Args.str(r, "PartitionKey");
        if (data == null) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value null at 'data' failed to satisfy constraint: Member must not be null");
        }
        if (pk == null || pk.isEmpty() || pk.length() > 256) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value '" + (pk == null ? "null" : pk)
                    + "' at 'partitionKey' failed to satisfy constraint: Member must have length less than or equal to 256");
        }
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(data);
        } catch (IllegalArgumentException e) {
            throw invalid("Data is not valid base64");
        }
        if (raw.length > MAX_RECORD_BYTES) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value '" + raw.length
                    + "' at 'data' failed to satisfy constraint: Member must have length less than or equal to 1048576");
        }
        BigInteger hash;
        String ehk = Args.str(r, "ExplicitHashKey");
        if (ehk != null) {
            try {
                hash = new BigInteger(ehk);
            } catch (NumberFormatException e) {
                throw invalid("Invalid ExplicitHashKey");
            }
            if (hash.signum() < 0 || hash.compareTo(KinesisHash.MAX) > 0) {
                throw invalid("Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]");
            }
        } else {
            hash = KinesisHash.hashKey(pk);
        }
        return new Put(raw, pk, hash);
    }

    private JsonObject putRecord(JsonObject req) {
        String name = needStreamName(req);
        Put p = parsePut(req);
        String[] res = putBatch(name, List.of(p)).get(0);
        JsonObject out = new JsonObject();
        out.addProperty("ShardId", res[0]);
        out.addProperty("SequenceNumber", res[1]);
        out.addProperty("EncryptionType", res[2]);
        return out;
    }

    private JsonObject putRecords(JsonObject req) {
        String name = needStreamName(req);
        List<JsonObject> recs = Args.objects(req, "Records");
        if (recs.isEmpty() || recs.size() > 500) {
            throw new AwsException(400, "ValidationException", "1 validation error detected: Value at 'records' failed to satisfy "
                    + "constraint: Member must have length less than or equal to 500");
        }
        List<Put> puts = new ArrayList<>();
        long total = 0;
        for (JsonObject r : recs) {
            Put p = parsePut(r);
            total += p.data.length + p.pk.length();
            puts.add(p);
        }
        if (total > 5L * 1024 * 1024) {
            throw invalid("Records size exceeds 5 MiB");
        }
        List<String[]> res = putBatch(name, puts);
        JsonArray arr = new JsonArray();
        for (String[] r : res) {
            JsonObject o = new JsonObject();
            o.addProperty("SequenceNumber", r[1]);
            o.addProperty("ShardId", r[0]);
            arr.add(o);
        }
        JsonObject out = new JsonObject();
        out.addProperty("FailedRecordCount", 0);
        out.add("Records", arr);
        out.addProperty("EncryptionType", res.get(0)[2]);
        return out;
    }

    private List<String[]> putBatch(String name, List<Put> puts) {
        for (int attempt = 0; ; attempt++) {
            try {
                return sh.tx(sh.owner(name), c -> {
                    Stream s = stream(c, name, false);
                    List<Shard> open = new ArrayList<>();
                    for (Shard sd : shards(c, name)) {
                        if (!sd.closed) {
                            open.add(sd);
                        }
                    }
                    // route every record to its shard; batch the counter bump per shard (sorted, so concurrent batches lock in one order)
                    Map<String, List<Integer>> byShard = new TreeMap<>();
                    String[] shardOf = new String[puts.size()];
                    for (int i = 0; i < puts.size(); i++) {
                        Shard target = null;
                        for (Shard sd : open) {
                            if (puts.get(i).hash.compareTo(sd.start) >= 0 && puts.get(i).hash.compareTo(sd.end) <= 0) {
                                target = sd;
                                break;
                            }
                        }
                        if (target == null) {
                            throw invalid("No open shard covers the hash key of record " + i);
                        }
                        shardOf[i] = target.id;
                        byShard.computeIfAbsent(target.id, k -> new ArrayList<>()).add(i);
                    }
                    long[] seq = new long[puts.size()];
                    for (var e : byShard.entrySet()) {
                        int n = e.getValue().size();
                        long last;
                        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_shards SET next_seq = next_seq + ? WHERE stream = ? "
                                + "AND shard_id = ? AND NOT closed RETURNING next_seq")) {
                            ps.setInt(1, n);
                            ps.setString(2, name);
                            ps.setString(3, e.getKey());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    throw new StaleShard();
                                }
                                last = rs.getLong(1);
                            }
                        }
                        long first = last - n + 1;
                        for (int k = 0; k < n; k++) {
                            seq[e.getValue().get(k)] = first + k;
                        }
                    }
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kinesis_records (stream, shard_id, seq, partition_key, data) "
                            + "VALUES (?, ?, ?, ?, ?)")) {
                        for (int i = 0; i < puts.size(); i++) {
                            ps.setString(1, name);
                            ps.setString(2, shardOf[i]);
                            ps.setLong(3, seq[i]);
                            ps.setString(4, puts.get(i).pk);
                            ps.setBytes(5, puts.get(i).data);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                    List<String[]> out = new ArrayList<>();
                    for (int i = 0; i < puts.size(); i++) {
                        out.add(new String[] {shardOf[i], KinesisHash.seqNumber(name, shardOf[i], seq[i]), s.encryption});
                    }
                    return out;
                });
            } catch (StaleShard e) {
                if (attempt >= 3) {
                    throw new AwsException(400, "ProvisionedThroughputExceededException", "The stream was resharded during the request; retry");
                }
            }
        }
    }

    /** A shard closed between routing and the counter bump (concurrent SplitShard/MergeShards): retry routing. */
    private static final class StaleShard extends RuntimeException {
        StaleShard() {
            super(null, null, false, false);
        }
    }

    // ---------------------------------------------------------------------------------------------- read

    private static String encodeIterator(String stream, String shardId, long position, long issuedMs) {
        String raw = "v1\u0000" + stream + "\u0000" + shardId + "\u0000" + position + "\u0000" + issuedMs;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static String[] decodeIterator(String it) {
        try {
            String[] p = new String(Base64.getUrlDecoder().decode(it), StandardCharsets.UTF_8).split("\u0000");
            if (p.length != 5 || !p[0].equals("v1")) {
                throw new IllegalArgumentException();
            }
            return new String[] {p[1], p[2], p[3], p[4]};
        } catch (IllegalArgumentException e) {
            throw invalid("Invalid ShardIterator");
        }
    }

    private JsonObject getShardIterator(JsonObject req) {
        String name = needStreamName(req);
        String shardId = Args.req(req, "ShardId");
        String type = Args.req(req, "ShardIteratorType");
        String startSeq = Args.str(req, "StartingSequenceNumber");
        Double tsArg = Args.dbl(req, "Timestamp");
        return sh.conn(sh.owner(name), c -> {
            stream(c, name, false);
            Shard shard = null;
            for (Shard s : shards(c, name)) {
                if (s.id.equals(shardId)) {
                    shard = s;
                }
            }
            if (shard == null) {
                throw new AwsException(400, "ResourceNotFoundException", "Shard " + shardId + " in stream " + name + " under account "
                        + rt.config.accountId + " does not exist");
            }
            long pos;
            switch (type) {
                case "TRIM_HORIZON" -> pos = 1;
                case "LATEST" -> pos = shard.nextSeq + 1;
                case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER" -> {
                    if (startSeq == null) {
                        throw invalid("StartingSequenceNumber is required for " + type);
                    }
                    long k = KinesisHash.counterOf(name, shardId, startSeq);
                    if (k < 0) {
                        throw invalid("StartingSequenceNumber " + startSeq + " used in GetShardIterator on shard " + shardId
                                + " in stream " + name + " under account " + rt.config.accountId + " is invalid.");
                    }
                    pos = type.startsWith("AT") ? k : k + 1;
                }
                case "AT_TIMESTAMP" -> {
                    if (tsArg == null) {
                        throw invalid("Timestamp is required for AT_TIMESTAMP");
                    }
                    try (PreparedStatement ps = c.prepareStatement("SELECT min(seq) FROM warp_kinesis_records WHERE stream = ? AND shard_id = ? "
                            + "AND arrived >= ?")) {
                        ps.setString(1, name);
                        ps.setString(2, shardId);
                        ps.setTimestamp(3, new Timestamp((long) (tsArg * 1000)));
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            long m = rs.getLong(1);
                            pos = rs.wasNull() ? shard.nextSeq + 1 : m;
                        }
                    }
                }
                default -> throw invalid("Invalid ShardIteratorType " + type);
            }
            JsonObject out = new JsonObject();
            out.addProperty("ShardIterator", encodeIterator(name, shardId, pos, System.currentTimeMillis()));
            return out;
        });
    }

    private JsonObject getRecords(JsonObject req) {
        String it = Args.req(req, "ShardIterator");
        String[] p = decodeIterator(it);
        String name = p[0], shardId = p[1];
        long pos = Long.parseLong(p[2]);
        long issued = Long.parseLong(p[3]);
        long now = System.currentTimeMillis();
        if (now - issued > ITERATOR_TTL_MS) {
            throw new AwsException(400, "ExpiredIteratorException", "Iterator expired. The iterator was created at time "
                    + java.time.Instant.ofEpochMilli(issued) + " while right now it is " + java.time.Instant.ofEpochMilli(now)
                    + " which is further in the future than the tolerated delay of 300000 milliseconds.");
        }
        Integer limitArg = Args.integer(req, "Limit");
        int limit = limitArg == null ? 10000 : limitArg;
        if (limit < 1 || limit > 10000) {
            throw new AwsException(400, "InvalidArgumentException", "Limit " + limit + " must be between 1 and 10000");
        }
        return sh.conn(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            Shard shard = null;
            for (Shard sd : shards(c, name)) {
                if (sd.id.equals(shardId)) {
                    shard = sd;
                }
            }
            if (shard == null) {
                throw new AwsException(400, "ResourceNotFoundException", "Shard " + shardId + " does not exist");
            }
            JsonArray recs = new JsonArray();
            long next = pos;
            long lastArrived = 0;
            long bytes = 0;
            try (PreparedStatement ps = c.prepareStatement("SELECT seq, partition_key, data, arrived FROM warp_kinesis_records WHERE stream = ? "
                    + "AND shard_id = ? AND seq >= ? ORDER BY seq LIMIT ?")) {
                ps.setString(1, name);
                ps.setString(2, shardId);
                ps.setLong(3, pos);
                ps.setInt(4, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        byte[] data = rs.getBytes(3);
                        if (bytes + data.length > 10 * 1024 * 1024 && recs.size() > 0) {
                            break;
                        }
                        bytes += data.length;
                        JsonObject r = new JsonObject();
                        r.addProperty("SequenceNumber", KinesisHash.seqNumber(name, shardId, rs.getLong(1)));
                        r.addProperty("ApproximateArrivalTimestamp", epoch(rs.getTimestamp(4)));
                        r.addProperty("Data", Base64.getEncoder().encodeToString(data));
                        r.addProperty("PartitionKey", rs.getString(2));
                        r.addProperty("EncryptionType", s.encryption);
                        recs.add(r);
                        next = rs.getLong(1) + 1;
                        lastArrived = rs.getTimestamp(4).getTime();
                    }
                }
            }
            long behind = 0;
            if (recs.size() > 0) {
                try (PreparedStatement ps = c.prepareStatement("SELECT max(arrived) FROM warp_kinesis_records WHERE stream = ? AND shard_id = ?")) {
                    ps.setString(1, name);
                    ps.setString(2, shardId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next() && rs.getTimestamp(1) != null) {
                            behind = Math.max(0, rs.getTimestamp(1).getTime() - lastArrived);
                        }
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("Records", recs);
            boolean ended = shard.closed && shard.endSeq != null && next > shard.endSeq;
            if (ended) {
                JsonArray children = new JsonArray();
                for (Shard sd : shards(c, name)) {
                    if (shardId.equals(sd.parent) || shardId.equals(sd.adjacent)) {
                        JsonObject ch = new JsonObject();
                        ch.addProperty("ShardId", sd.id);
                        JsonArray parents = new JsonArray();
                        if (sd.parent != null) {
                            parents.add(sd.parent);
                        }
                        if (sd.adjacent != null) {
                            parents.add(sd.adjacent);
                        }
                        ch.add("ParentShards", parents);
                        JsonObject hr = new JsonObject();
                        hr.addProperty("StartingHashKey", sd.start.toString());
                        hr.addProperty("EndingHashKey", sd.end.toString());
                        ch.add("HashKeyRange", hr);
                        children.add(ch);
                    }
                }
                out.add("ChildShards", children);
            } else {
                out.addProperty("NextShardIterator", encodeIterator(name, shardId, next, System.currentTimeMillis()));
            }
            out.addProperty("MillisBehindLatest", behind);
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- enhanced fan-out

    private static final java.util.concurrent.Semaphore SUBSCRIPTIONS = new java.util.concurrent.Semaphore(200);

    /**
     * SubscribeToShard, an event stream over HTTP/2 or chunked HTTP/1.1 (initial-response, then a SubscribeToShardEvent per batch of records, in
     * shard order from the requested position). Real Kinesis holds a subscription for 5 minutes; here it delivers what is there
     * and what arrives within {@code WARP_KINESISWIRE_EFO_LINGER_MS} (default 2000) and then ends the stream, so a consumer that
     * resubscribes with the last ContinuationSequenceNumber (the KCL does) sees the same records. Set the linger to 300000 for
     * real-Kinesis timing. Each open subscription occupies one worker thread (at most 200 at a time).
     */
    public void subscribeToShard(JsonObject req, StreamSink ex) {
        java.util.function.BiConsumer<Integer, JsonObject> fail = (status, err) -> {
            ex.head(status, java.util.Map.of("content-type", "application/x-amz-json-1.1", "x-amzn-errortype",
                    Args.str(err, "__type")));
            ex.data(err.toString().getBytes(StandardCharsets.UTF_8), true);
        };
        String consumerArn = Args.str(req, "ConsumerARN");
        String shardId = Args.str(req, "ShardId");
        JsonObject start = Args.obj(req, "StartingPosition");
        String name;
        JsonObject first;
        try {
            if (consumerArn == null || shardId == null || start == null) {
                throw invalid("ConsumerARN, ShardId and StartingPosition are required");
            }
            JsonObject probe = new JsonObject();
            probe.addProperty("ConsumerARN", consumerArn);
            name = streamNameOf(probe);
            if (name == null) {
                throw invalid("Invalid ConsumerARN");
            }
            describeConsumer(probe);
            JsonObject it = new JsonObject();
            it.addProperty("StreamName", name);
            it.addProperty("ShardId", shardId);
            it.addProperty("ShardIteratorType", Args.req(start, "Type"));
            if (Args.str(start, "SequenceNumber") != null) {
                it.addProperty("StartingSequenceNumber", Args.str(start, "SequenceNumber"));
            }
            if (Args.str(start, "Timestamp") != null) {
                it.addProperty("Timestamp", Args.str(start, "Timestamp"));
            }
            first = getShardIterator(it);
        } catch (AwsException e) {
            JsonObject err = new JsonObject();
            err.addProperty("__type", e.code);
            err.addProperty("message", e.getMessage());
            fail.accept(e.status, err);
            return;
        }
        if (!SUBSCRIPTIONS.tryAcquire()) {
            JsonObject err = new JsonObject();
            err.addProperty("__type", "LimitExceededException");
            err.addProperty("message", "Too many concurrent SubscribeToShard subscriptions");
            fail.accept(400, err);
            return;
        }
        long linger = 2000;
        try {
            String v = System.getenv("WARP_KINESISWIRE_EFO_LINGER_MS");
            if (v != null && !v.isBlank()) {
                linger = Long.parseLong(v.trim());
            }
        } catch (NumberFormatException ignored) {
            // default
        }
        try {
            ex.head(200, java.util.Map.of("content-type", "application/vnd.amazon.eventstream", "x-amzn-requestid",
                    java.util.UUID.randomUUID().toString()));
            ex.data(EventStream.message("event", "initial-response", "application/json", "{}".getBytes(StandardCharsets.UTF_8)), false);
            String iterator = Args.str(first, "ShardIterator");
            long deadline = System.currentTimeMillis() + 5 * 60 * 1000L;
            long lastActivity = System.currentTimeMillis();
            while (!ex.cancelled() && System.currentTimeMillis() < deadline) {
                JsonObject in = new JsonObject();
                in.addProperty("ShardIterator", iterator);
                in.addProperty("Limit", 1000);
                JsonObject page = getRecords(in);
                JsonArray recs = page.getAsJsonArray("Records");
                boolean ended = !page.has("NextShardIterator");
                if (recs.size() > 0 || ended) {
                    JsonObject ev = new JsonObject();
                    ev.add("Records", recs);
                    String cont = recs.size() > 0 ? recs.get(recs.size() - 1).getAsJsonObject().get("SequenceNumber").getAsString()
                            : KinesisHash.seqNumber(name, shardId, 0);
                    if (!ended) {
                        ev.addProperty("ContinuationSequenceNumber", cont);
                    }
                    ev.addProperty("MillisBehindLatest", page.get("MillisBehindLatest").getAsLong());
                    if (page.has("ChildShards")) {
                        ev.add("ChildShards", page.get("ChildShards"));
                    }
                    ex.data(EventStream.event("SubscribeToShardEvent", ev.toString()), false);
                    lastActivity = System.currentTimeMillis();
                }
                if (ended) {
                    break;
                }
                iterator = Args.str(page, "NextShardIterator");
                if (recs.size() == 0) {
                    if (System.currentTimeMillis() - lastActivity > linger) {
                        break;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            ex.data(new byte[0], true);
        } finally {
            SUBSCRIPTIONS.release();
        }
    }

    // ---------------------------------------------------------------------------------------------- resharding

    private JsonObject splitShard(JsonObject req) {
        String name = needStreamName(req);
        String target = Args.req(req, "ShardToSplit");
        BigInteger at;
        try {
            at = new BigInteger(Args.req(req, "NewStartingHashKey"));
        } catch (NumberFormatException e) {
            throw invalid("Invalid NewStartingHashKey");
        }
        sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            Shard parent = null;
            for (Shard sd : shards(c, name)) {
                if (sd.id.equals(target)) {
                    parent = sd;
                }
            }
            if (parent == null || parent.closed) {
                throw new AwsException(400, "ResourceNotFoundException", "Shard " + target + " in stream " + name + " under account "
                        + rt.config.accountId + " does not exist");
            }
            if (at.compareTo(parent.start) <= 0 || at.compareTo(parent.end) > 0) {
                throw invalid("NewStartingHashKey " + at + " used in SplitShard() on shard " + target + " in stream " + name
                        + " under account " + rt.config.accountId + " is not both greater than one plus the shard's StartingHashKey "
                        + parent.start + " and less than or equal to the shard's EndingHashKey " + parent.end + ".");
            }
            closeShard(c, name, target);
            int i = s.nextShard;
            insertShard(c, name, KinesisHash.shardId(i), i, parent.start, at.subtract(BigInteger.ONE), target, null);
            insertShard(c, name, KinesisHash.shardId(i + 1), i + 1, at, parent.end, target, null);
            bumpShardIndex(c, name, i + 2);
            return null;
        });
        return new JsonObject();
    }

    private static void closeShard(Connection c, String stream, String shard) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_shards SET closed = true, end_seq = next_seq WHERE stream = ? AND shard_id = ?")) {
            ps.setString(1, stream);
            ps.setString(2, shard);
            ps.executeUpdate();
        }
    }

    private static void bumpShardIndex(Connection c, String stream, int next) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET next_shard_index = ? WHERE name = ?")) {
            ps.setInt(1, next);
            ps.setString(2, stream);
            ps.executeUpdate();
        }
    }

    private JsonObject mergeShards(JsonObject req) {
        String name = needStreamName(req);
        String a = Args.req(req, "ShardToMerge");
        String b = Args.req(req, "AdjacentShardToMerge");
        sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            Shard sa = null, sb = null;
            for (Shard sd : shards(c, name)) {
                if (sd.id.equals(a)) {
                    sa = sd;
                }
                if (sd.id.equals(b)) {
                    sb = sd;
                }
            }
            if (sa == null || sb == null || sa.closed || sb.closed) {
                throw new AwsException(400, "ResourceNotFoundException", "Shard " + (sa == null || sa.closed ? a : b)
                        + " in stream " + name + " under account " + rt.config.accountId + " does not exist");
            }
            Shard lo = sa.start.compareTo(sb.start) < 0 ? sa : sb;
            Shard hi = lo == sa ? sb : sa;
            if (!lo.end.add(BigInteger.ONE).equals(hi.start)) {
                throw invalid("Shards " + a + " and " + b + " in stream " + name + " under account " + rt.config.accountId
                        + " are not an adjacent pair of shards eligible for merging");
            }
            closeShard(c, name, a);
            closeShard(c, name, b);
            int i = s.nextShard;
            insertShard(c, name, KinesisHash.shardId(i), i, lo.start, hi.end, a, b);
            bumpShardIndex(c, name, i + 1);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject updateShardCount(JsonObject req) {
        String name = needStreamName(req);
        int target = Args.integer(req, "TargetShardCount") == null ? 0 : Args.integer(req, "TargetShardCount");
        if (target < 1 || target > MAX_SHARDS) {
            throw invalid("TargetShardCount must be between 1 and " + MAX_SHARDS);
        }
        return sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            List<Shard> open = new ArrayList<>();
            for (Shard sd : shards(c, name)) {
                if (!sd.closed) {
                    open.add(sd);
                }
            }
            int i = s.nextShard;
            for (Shard sd : open) {
                closeShard(c, name, sd.id);
            }
            for (BigInteger[] r : KinesisHash.evenRanges(target)) {
                String parent = null;
                String adj = null;
                for (Shard sd : open) {
                    if (sd.start.compareTo(r[1]) <= 0 && sd.end.compareTo(r[0]) >= 0) {
                        if (parent == null) {
                            parent = sd.id;
                        } else if (adj == null) {
                            adj = sd.id;
                        }
                    }
                }
                insertShard(c, name, KinesisHash.shardId(i), i, r[0], r[1], parent, adj);
                i++;
            }
            bumpShardIndex(c, name, i);
            JsonObject out = new JsonObject();
            out.addProperty("StreamName", name);
            out.addProperty("CurrentShardCount", open.size());
            out.addProperty("TargetShardCount", target);
            out.addProperty("StreamARN", s.arn);
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- metadata

    private JsonObject retention(JsonObject req, boolean increase) {
        String name = needStreamName(req);
        Integer hours = Args.integer(req, "RetentionPeriodHours");
        if (hours == null) {
            throw invalid("RetentionPeriodHours is required");
        }
        sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            if (increase) {
                if (hours > 8760) {
                    throw invalid("Maximum retention period is 8760 hours");
                }
                if (hours <= s.retention) {
                    throw invalid("Requested retention period (" + hours + " hours) for stream " + name + " under account "
                            + rt.config.accountId + " must be more than the current retention period (" + s.retention + " hours).");
                }
            } else {
                if (hours < 24) {
                    throw invalid("Minimum retention period is 24 hours");
                }
                if (hours >= s.retention) {
                    throw invalid("Requested retention period (" + hours + " hours) for stream " + name + " under account "
                            + rt.config.accountId + " must be less than the current retention period (" + s.retention + " hours).");
                }
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET retention_hours = ? WHERE name = ?")) {
                ps.setInt(1, hours);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private void mutateTags(String name, java.util.function.Consumer<Map<String, String>> f) {
        sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            Map<String, String> tags = new TreeMap<>(s.tags);
            f.accept(tags);
            if (tags.size() > 50) {
                throw invalid("A tag limit of 50 tags per stream was exceeded");
            }
            JsonObject t = new JsonObject();
            tags.forEach(t::addProperty);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET tags = ?::jsonb WHERE name = ?")) {
                ps.setString(1, t.toString());
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private JsonObject addTags(JsonObject req) {
        String name = needStreamName(req);
        Map<String, String> add = new LinkedHashMap<>();
        JsonObject o = Args.obj(req, "Tags");
        if (o != null) {
            o.entrySet().forEach(e -> add.put(e.getKey(), e.getValue().getAsString()));
        } else {
            for (JsonObject t : Args.objects(req, "Tags")) {
                add.put(Args.str(t, "Key"), Args.str(t, "Value"));
            }
        }
        if (add.isEmpty()) {
            throw invalid("Tags must not be empty");
        }
        mutateTags(name, m -> m.putAll(add));
        return new JsonObject();
    }

    private JsonObject removeTags(JsonObject req) {
        String name = needStreamName(req);
        List<String> keys = Args.strings(req, "TagKeys");
        mutateTags(name, m -> keys.forEach(m::remove));
        return new JsonObject();
    }

    private JsonObject listTags(JsonObject req) {
        String name = needStreamName(req);
        String after = Args.str(req, "ExclusiveStartTagKey");
        Integer lim = Args.integer(req, "Limit");
        int limit = lim == null ? 50 : lim;
        return sh.conn(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            JsonArray arr = new JsonArray();
            boolean more = false;
            for (var e : s.tags.entrySet()) {
                if (after != null && e.getKey().compareTo(after) <= 0) {
                    continue;
                }
                if (arr.size() >= limit) {
                    more = true;
                    break;
                }
                JsonObject t = new JsonObject();
                t.addProperty("Key", e.getKey());
                t.addProperty("Value", e.getValue());
                arr.add(t);
            }
            JsonObject out = new JsonObject();
            out.add("Tags", arr);
            out.addProperty("HasMoreTags", more);
            return out;
        });
    }

    private JsonObject monitoring(JsonObject req, boolean enable) {
        String name = needStreamName(req);
        List<String> metrics = Args.strings(req, "ShardLevelMetrics");
        if (metrics.isEmpty()) {
            throw invalid("ShardLevelMetrics must not be empty");
        }
        return sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, true);
            List<String> cur = new ArrayList<>();
            JsonParser.parseString(s.metrics).getAsJsonArray().forEach(e -> cur.add(e.getAsString()));
            List<String> before = new ArrayList<>(cur);
            List<String> all = List.of("IncomingBytes", "IncomingRecords", "OutgoingBytes", "OutgoingRecords",
                    "WriteProvisionedThroughputExceeded", "ReadProvisionedThroughputExceeded", "IteratorAgeMilliseconds");
            for (String m : metrics.contains("ALL") ? all : metrics) {
                if (enable) {
                    if (!cur.contains(m)) {
                        cur.add(m);
                    }
                } else {
                    cur.remove(m);
                }
            }
            JsonArray arr = new JsonArray();
            cur.forEach(arr::add);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET metrics = ?::jsonb WHERE name = ?")) {
                ps.setString(1, arr.toString());
                ps.setString(2, name);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("StreamName", name);
            JsonArray b = new JsonArray();
            before.forEach(b::add);
            out.add("CurrentShardLevelMetrics", b);
            out.add("DesiredShardLevelMetrics", arr);
            out.addProperty("StreamARN", s.arn);
            return out;
        });
    }

    private JsonObject updateMode(JsonObject req) {
        String name = needStreamName(req);
        JsonObject md = Args.obj(req, "StreamModeDetails");
        String mode = md == null ? null : Args.str(md, "StreamMode");
        if (mode == null || !(mode.equals("PROVISIONED") || mode.equals("ON_DEMAND"))) {
            throw invalid("Invalid StreamModeDetails");
        }
        sh.tx(sh.owner(name), c -> {
            stream(c, name, true);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET mode = ? WHERE name = ?")) {
                ps.setString(1, mode);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject encryption(JsonObject req, boolean start) {
        String name = needStreamName(req);
        String keyId = Args.str(req, "KeyId");
        if (start && (keyId == null || !"KMS".equals(Args.str(req, "EncryptionType")))) {
            throw invalid("EncryptionType KMS and KeyId are required");
        }
        sh.tx(sh.owner(name), c -> {
            stream(c, name, true);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET encryption_type = ?, key_id = ? WHERE name = ?")) {
                ps.setString(1, start ? "KMS" : "NONE");
                ps.setString(2, start ? keyId : null);
                ps.setString(3, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject describeLimits() {
        long open = 0, streams = 0;
        for (long[] v : sh.<long[]>onAll(c -> {
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT (SELECT count(*) FROM warp_kinesis_shards "
                    + "WHERE NOT closed), (SELECT count(*) FROM warp_kinesis_streams WHERE mode = 'ON_DEMAND')")) {
                rs.next();
                return new long[] {rs.getLong(1), rs.getLong(2)};
            }
        })) {
            open += v[0];
            streams += v[1];
        }
        JsonObject out = new JsonObject();
        out.addProperty("ShardLimit", 500);
        out.addProperty("OpenShardCount", open);
        out.addProperty("OnDemandStreamCount", streams);
        out.addProperty("OnDemandStreamCountLimit", 50);
        return out;
    }

    // ---------------------------------------------------------------------------------------------- consumers

    private JsonObject consumerJson(String name, String arn, Timestamp created) {
        JsonObject o = new JsonObject();
        o.addProperty("ConsumerName", name);
        o.addProperty("ConsumerARN", arn);
        o.addProperty("ConsumerStatus", "ACTIVE");
        o.addProperty("ConsumerCreationTimestamp", epoch(created));
        return o;
    }

    private JsonObject registerConsumer(JsonObject req) {
        String name = streamNameOf(req);
        String consumer = Args.req(req, "ConsumerName");
        if (name == null) {
            throw invalid("StreamARN is required");
        }
        return sh.tx(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            String arn = s.arn + "/consumer/" + consumer + ":" + (System.currentTimeMillis() / 1000);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kinesis_consumers (stream, name, arn) VALUES (?, ?, ?) "
                    + "ON CONFLICT DO NOTHING RETURNING created_at")) {
                ps.setString(1, name);
                ps.setString(2, consumer);
                ps.setString(3, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new AwsException(400, "ResourceInUseException", "Consumer " + consumer + " under stream " + name
                                + " already exists.");
                    }
                    JsonObject out = new JsonObject();
                    out.add("Consumer", consumerJson(consumer, arn, rs.getTimestamp(1)));
                    return out;
                }
            }
        });
    }

    private JsonObject deregisterConsumer(JsonObject req) {
        String name = streamNameOf(req);
        String consumer = Args.str(req, "ConsumerName");
        String carn = Args.str(req, "ConsumerARN");
        sh.tx(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kinesis_consumers WHERE stream = ? AND (name = ? OR arn = ?)")) {
                ps.setString(1, name);
                ps.setString(2, consumer == null ? "" : consumer);
                ps.setString(3, carn == null ? "" : carn);
                if (ps.executeUpdate() == 0) {
                    throw new AwsException(400, "ResourceNotFoundException", "Consumer not found");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject describeConsumer(JsonObject req) {
        String name = streamNameOf(req);
        String consumer = Args.str(req, "ConsumerName");
        String carn = Args.str(req, "ConsumerARN");
        if (name == null) {
            throw invalid("StreamARN or ConsumerARN is required");
        }
        return sh.conn(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name, arn, created_at FROM warp_kinesis_consumers WHERE stream = ? "
                    + "AND (name = ? OR arn = ?)")) {
                ps.setString(1, name);
                ps.setString(2, consumer == null ? "" : consumer);
                ps.setString(3, carn == null ? "" : carn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new AwsException(400, "ResourceNotFoundException", "Consumer not found");
                    }
                    JsonObject d = consumerJson(rs.getString(1), rs.getString(2), rs.getTimestamp(3));
                    d.addProperty("StreamARN", arnOf(name));
                    JsonObject out = new JsonObject();
                    out.add("ConsumerDescription", d);
                    return out;
                }
            }
        });
    }

    private JsonObject listConsumers(JsonObject req) {
        String name = needStreamName(req);
        return sh.conn(sh.owner(name), c -> {
            stream(c, name, false);
            JsonArray a = new JsonArray();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, arn, created_at FROM warp_kinesis_consumers WHERE stream = ? ORDER BY name")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        a.add(consumerJson(rs.getString(1), rs.getString(2), rs.getTimestamp(3)));
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("Consumers", a);
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- policy

    private JsonObject putPolicy(JsonObject req) {
        String name = needStreamName(req);
        String policy = Args.req(req, "Policy");
        sh.tx(sh.owner(name), c -> {
            stream(c, name, true);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET policy = ? WHERE name = ?")) {
                ps.setString(1, policy);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject getPolicy(JsonObject req) {
        String name = needStreamName(req);
        return sh.conn(sh.owner(name), c -> {
            Stream s = stream(c, name, false);
            JsonObject out = new JsonObject();
            out.addProperty("Policy", s.policy == null ? "{}" : s.policy);
            return out;
        });
    }

    private JsonObject deletePolicy(JsonObject req) {
        String name = needStreamName(req);
        sh.tx(sh.owner(name), c -> {
            stream(c, name, true);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kinesis_streams SET policy = NULL WHERE name = ?")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }
}
