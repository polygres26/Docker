package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Base64;
import java.util.List;

/**
 * Amazon Kinesis Data Streams vocabulary (kinesis_* tools: list and describe streams and shards, create/delete streams, put
 * one or several records, read records from a shard). Each tool invokes the same {@code KinesisService} operation awswire runs
 * for a Kinesis SDK client (PutRecord, GetShardIterator + GetRecords, ...): partition-key hashing, sequence numbers and
 * shard iterators are the wire protocol's. {@code kinesis_get_records} folds GetShardIterator and GetRecords into one call.
 */
final class KinesisToolProvider extends StoreToolProvider {

    private static final int MAX_RECORDS = 1000;

    KinesisToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.KINESIS, describer, stores);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject stream = str("Stream name");
        return List.of(
                new Tool("kinesis_list_streams", "List streams.", schema(List.of(), "limit", num("Max streams (default 100)"),
                        "exclusiveStartStreamName", str("Start after this stream")), false),
                new Tool("kinesis_describe_stream_summary", "Stream summary: status, shard count, retention, mode.",
                        schema(List.of("streamName"), "streamName", stream), false),
                new Tool("kinesis_list_shards", "List a stream's shards with hash key ranges.", schema(List.of("streamName"), "streamName", stream), false),
                new Tool("kinesis_create_stream", "Create a stream.", schema(List.of("streamName"), "streamName", stream,
                        "shardCount", num("Shards (provisioned mode, default 1)"), "onDemand", bool("On-demand capacity mode")), true),
                new Tool("kinesis_delete_stream", "Delete a stream and its records.", schema(List.of("streamName"), "streamName", stream), true),
                new Tool("kinesis_put_record", "Write one record (text via data, or base64 via dataBase64).",
                        schema(List.of("streamName", "partitionKey"), "streamName", stream, "partitionKey", str("Partition key"),
                                "data", str("UTF-8 text payload"), "dataBase64", str("Base64 payload")), true),
                new Tool("kinesis_put_records", "Write several records.", schema(List.of("streamName", "records"), "streamName", stream,
                        "records", arr("Records: [{partitionKey, data | dataBase64}]")), true),
                new Tool("kinesis_get_records", "Read records from one shard (starts at the oldest record by default).",
                        schema(List.of("streamName", "shardId"), "streamName", stream, "shardId", str("Shard id, e.g. shardId-000000000000"),
                                "iteratorType", str("TRIM_HORIZON (default), LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP"),
                                "startingSequenceNumber", str("For AT/AFTER_SEQUENCE_NUMBER"), "timestamp", num("Epoch seconds, for AT_TIMESTAMP"),
                                "limit", num("Max records (default 25, max 1000)")), false));
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        JsonObject req = new JsonObject();
        switch (tool) {
            case "kinesis_list_streams": {
                req.addProperty("Limit", limit(a, "limit", 100, 10000));
                SnsToolProvider.put(req, "ExclusiveStartStreamName", optString(a, "exclusiveStartStreamName"));
                return json(AwsToolSupport.invoke(stores, "kinesis", "ListStreams", req));
            }
            case "kinesis_describe_stream_summary":
                req.addProperty("StreamName", requireString(a, "streamName"));
                return json(AwsToolSupport.invoke(stores, "kinesis", "DescribeStreamSummary", req));
            case "kinesis_list_shards":
                req.addProperty("StreamName", requireString(a, "streamName"));
                return json(AwsToolSupport.invoke(stores, "kinesis", "ListShards", req));
            case "kinesis_create_stream": {
                req.addProperty("StreamName", requireString(a, "streamName"));
                if (optBool(a, "onDemand", false)) {
                    JsonObject mode = new JsonObject();
                    mode.addProperty("StreamMode", "ON_DEMAND");
                    req.add("StreamModeDetails", mode);
                } else {
                    req.addProperty("ShardCount", optInt(a, "shardCount") == null ? 1 : optInt(a, "shardCount"));
                }
                return json(AwsToolSupport.invoke(stores, "kinesis", "CreateStream", req));
            }
            case "kinesis_delete_stream":
                req.addProperty("StreamName", requireString(a, "streamName"));
                return json(AwsToolSupport.invoke(stores, "kinesis", "DeleteStream", req));
            case "kinesis_put_record": {
                req.addProperty("StreamName", requireString(a, "streamName"));
                req.addProperty("PartitionKey", requireString(a, "partitionKey"));
                req.addProperty("Data", Base64.getEncoder().encodeToString(bodyArg(a, "data", "dataBase64")));
                return json(AwsToolSupport.invoke(stores, "kinesis", "PutRecord", req));
            }
            case "kinesis_put_records": {
                req.addProperty("StreamName", requireString(a, "streamName"));
                JsonArray records = new JsonArray();
                if (!a.has("records") || !a.get("records").isJsonArray()) {
                    throw new IllegalArgumentException("records must be an array");
                }
                for (JsonElement e : a.getAsJsonArray("records")) {
                    JsonObject r = e.getAsJsonObject();
                    JsonObject rec = new JsonObject();
                    rec.addProperty("PartitionKey", requireString(r, "partitionKey"));
                    rec.addProperty("Data", Base64.getEncoder().encodeToString(bodyArg(r, "data", "dataBase64")));
                    records.add(rec);
                }
                req.add("Records", records);
                return json(AwsToolSupport.invoke(stores, "kinesis", "PutRecords", req));
            }
            case "kinesis_get_records": {
                req.addProperty("StreamName", requireString(a, "streamName"));
                req.addProperty("ShardId", requireString(a, "shardId"));
                req.addProperty("ShardIteratorType", optString(a, "iteratorType") == null ? "TRIM_HORIZON" : optString(a, "iteratorType"));
                SnsToolProvider.put(req, "StartingSequenceNumber", optString(a, "startingSequenceNumber"));
                if (optLong(a, "timestamp") != null) {
                    req.addProperty("Timestamp", optLong(a, "timestamp"));
                }
                String iterator = AwsToolSupport.invoke(stores, "kinesis", "GetShardIterator", req).get("ShardIterator").getAsString();
                JsonObject get = new JsonObject();
                get.addProperty("ShardIterator", iterator);
                get.addProperty("Limit", limit(a, "limit", 25, MAX_RECORDS));
                JsonObject res = AwsToolSupport.invoke(stores, "kinesis", "GetRecords", get);
                JsonObject out = new JsonObject();
                JsonArray recs = new JsonArray();
                for (JsonElement e : res.getAsJsonArray("Records")) {
                    JsonObject r = e.getAsJsonObject();
                    JsonObject o = new JsonObject();
                    o.add("sequenceNumber", r.get("SequenceNumber"));
                    o.add("partitionKey", r.get("PartitionKey"));
                    o.add("approximateArrivalTimestamp", r.get("ApproximateArrivalTimestamp"));
                    byte[] data = Base64.getDecoder().decode(r.get("Data").getAsString());
                    byte[] cut = data.length > MAX_TEXT ? java.util.Arrays.copyOf(data, MAX_TEXT) : data;
                    putBody(o, cut, data.length);
                    recs.add(o);
                }
                out.add("records", recs);
                out.add("millisBehindLatest", res.get("MillisBehindLatest"));
                out.add("nextShardIterator", res.get("NextShardIterator"));
                return json(out);
            }
            default:
                return Outcome.error("unknown kinesis tool: " + tool);
        }
    }
}
