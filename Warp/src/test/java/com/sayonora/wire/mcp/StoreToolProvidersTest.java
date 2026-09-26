package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Declarations of the store tool providers (no database needed): naming, schemas, write flags, typed-value codecs. */
class StoreToolProvidersTest {

    private static List<StoreToolProvider> providers() {
        EmulatedStores stores = new EmulatedStores(null);
        List<StoreToolProvider> out = new ArrayList<>();
        out.add(new RedisToolProvider(null, stores));
        out.add(new AzBlobToolProvider(null, stores));
        out.add(new AzQueueToolProvider(null, stores));
        out.add(new AzTableToolProvider(null, stores));
        out.add(new GcsToolProvider(null, stores));
        out.add(new SnsToolProvider(null, stores));
        out.add(new KinesisToolProvider(null, stores));
        out.add(new AwsParamsToolProvider(null, stores));
        out.add(new PubsubToolProvider(null, stores));
        out.add(new FirestoreToolProvider(null, stores));
        out.add(new DatastoreToolProvider(null, stores));
        out.add(new BigtableToolProvider(null, stores));
        out.add(new KafkaToolProvider(null, stores));
        out.add(new CosmosToolProvider(null, stores));
        return out;
    }

    @Test
    void everyToolIsPrefixedUniqueDescribedAndHasARequiredListInsideItsProperties() {
        Set<String> seen = new HashSet<>();
        for (StoreToolProvider p : providers()) {
            assertFalse(p.tools().isEmpty(), p.kind().id());
            for (BackendToolProvider.Tool t : p.tools()) {
                assertTrue(seen.add(t.name()), "duplicate tool name " + t.name());
                assertFalse(t.description().isBlank(), t.name());
                assertEquals("object", t.inputSchema().get("type").getAsString());
                JsonObject props = t.inputSchema().getAsJsonObject("properties");
                if (t.inputSchema().has("required")) {
                    for (var r : t.inputSchema().getAsJsonArray("required")) {
                        assertTrue(props.has(r.getAsString()), t.name() + " requires unknown property " + r.getAsString());
                    }
                }
            }
        }
        // names carry their store: redis_*, azblob_*, ... (awsparams tools use the AWS service prefix instead)
        for (StoreToolProvider p : providers()) {
            String kind = p.kind().id();
            Set<String> prefixes = kind.equals("awsparams") ? Set.of("secrets_", "ssm_", "kms_", "sts_") : kind.equals("kafkastore") ? Set.of("kafka_") : kind.equals("cosmosstore") ? Set.of("cosmos_") : Set.of(kind + "_");
            for (BackendToolProvider.Tool t : p.tools()) {
                assertTrue(prefixes.stream().anyMatch(t.name()::startsWith), t.name());
            }
        }
    }

    @Test
    void readOnlyToolsNeverMutateAndTheRightToolsAreWriteTools() {
        Set<String> writes = new HashSet<>();
        for (StoreToolProvider p : providers()) {
            p.tools().stream().filter(BackendToolProvider.Tool::write).forEach(t -> writes.add(t.name()));
        }
        for (String w : List.of("redis_set", "redis_delete", "gcs_put_object", "azblob_upload_blob", "azqueue_receive_messages",
                "sns_publish", "kinesis_put_record", "secrets_put_secret_value", "ssm_put_parameter", "kms_create_key", "pubsub_publish",
                "pubsub_pull", "pubsub_ack", "firestore_add_document", "datastore_upsert_entity", "bigtable_mutate_row",
                "bigtable_drop_row_range", "kafka_create_topic", "kafka_delete_topic", "kafka_produce", "cosmos_upsert_item", "cosmos_delete_item", "cosmos_create_database", "cosmos_create_container")) {
            assertTrue(writes.contains(w), w + " must be a write tool");
        }
        for (String r : List.of("redis_get", "gcs_get_object", "azblob_get_blob", "azqueue_peek_messages", "sns_list_topics",
                "kinesis_get_records", "secrets_get_secret_value", "ssm_get_parameter", "kms_decrypt", "kms_encrypt", "pubsub_get_topic",
                "firestore_query_collection", "datastore_run_query", "bigtable_read_rows", "kafka_fetch", "kafka_list_topics", "kafka_group_lag")) {
            assertFalse(writes.contains(r), r + " must not be a write tool");
        }
    }

    @Test
    void noKmsToolExportsKeyMaterial() {
        AwsParamsToolProvider p = new AwsParamsToolProvider(null, new EmulatedStores(null));
        for (BackendToolProvider.Tool t : p.tools()) {
            String n = t.name();
            assertFalse(n.contains("data_key") || n.contains("export") || n.contains("public_key") || n.contains("import")
                    || n.contains("key_material"), n);
        }
    }

    @Test
    void unknownToolNamesAreNotExecutedAndAreNotUnsupportedOperationProbes() {
        RedisToolProvider p = new RedisToolProvider(null, new EmulatedStores(null));
        BackendToolProvider.Outcome o = p.call("execute_sql", new JsonObject(), null);
        assertTrue(o.isError());
        assertFalse(o.texts().get(0).startsWith("UnsupportedOperation"));
    }

    @Test
    void typedValuesRoundTripPlainJson() {
        String plain = "{\"s\":\"x\",\"i\":3,\"d\":1.5,\"b\":true,\"n\":null,\"a\":[1,\"two\"],\"m\":{\"k\":{\"z\":9}},"
                + "\"t\":{\"$timestamp\":\"2024-01-02T03:04:05Z\"}}";
        for (TypedValues tv : new TypedValues[] {TypedValues.FIRESTORE, TypedValues.DATASTORE}) {
            JsonObject typed = tv.fields(JsonParser.parseString(plain).getAsJsonObject());
            assertEquals("x", typed.getAsJsonObject("s").get("stringValue").getAsString());
            assertEquals("3", typed.getAsJsonObject("i").get("integerValue").getAsString());
            assertEquals(1.5, typed.getAsJsonObject("d").get("doubleValue").getAsDouble());
            assertEquals("NULL_VALUE", typed.getAsJsonObject("n").get("nullValue").getAsString());
            assertEquals("2024-01-02T03:04:05Z", typed.getAsJsonObject("t").get("timestampValue").getAsString());
            assertTrue(typed.getAsJsonObject("m").has(tv == TypedValues.FIRESTORE ? "mapValue" : "entityValue"));
            assertEquals(JsonParser.parseString(plain), tv.plain(typed));
        }
    }

    @Test
    void aTypedObjectIsPassedThroughUnchanged() {
        JsonObject raw = JsonParser.parseString("{\"integerValue\":\"7\"}").getAsJsonObject();
        assertEquals(raw, TypedValues.FIRESTORE.value(raw));
        JsonArray arr = TypedValues.DATASTORE.value(JsonParser.parseString("[1,2]")).getAsJsonObject("arrayValue").getAsJsonArray("values");
        assertEquals(2, arr.size());
    }

    @Test
    void bodiesAreTextWhenUtf8AndBase64Otherwise() {
        JsonObject o = new JsonObject();
        StoreToolProvider.putBody(o, "héllo".getBytes(java.nio.charset.StandardCharsets.UTF_8), 5);
        assertEquals("utf-8", o.get("encoding").getAsString());
        assertFalse(o.get("truncated").getAsBoolean());
        JsonObject bin = new JsonObject();
        StoreToolProvider.putBody(bin, new byte[] {(byte) 0xff, 1}, 10);
        assertEquals("base64", bin.get("encoding").getAsString());
        assertTrue(bin.get("truncated").getAsBoolean());
        JsonObject cut = new JsonObject();       // a cut inside a multi-byte character still yields text
        byte[] snowman = "a☃".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        StoreToolProvider.putBody(cut, java.util.Arrays.copyOf(snowman, 3), snowman.length);
        assertEquals("utf-8", cut.get("encoding").getAsString());
        assertEquals("a", cut.get("body").getAsString());
    }

    @Test
    void pathsAreEncodedSegmentWise() {
        assertEquals("d/a%20b/c%2Bd", StoreToolProvider.encPath("d/a b/c+d"));
    }
}
