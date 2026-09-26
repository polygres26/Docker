package com.sayonora.warp.mcp;

import com.mongodb.client.MongoClient;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.connector.ConnectorOperands;
import com.sayonora.warp.core.connector.dynamodb.DynamoSchemaFactory;
import com.sayonora.warp.core.connector.mongo.MongoSchemaFactory;
import com.sayonora.warp.core.connector.s3.S3SchemaFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Clients for REAL external backends registered in {@code WARP_BACKENDS} (mongodb://, dynamodb://,
 * s3://), built by the same connector factories the federated mounts use -- so URL grammar,
 * credentials and {@code vault:}/{@code cyberark:} password resolution are identical. One client
 * per backend, rebuilt (and the old one closed) when the backend's definition changes on a config
 * reload; closed on shutdown.
 */
final class ExternalClients implements AutoCloseable {

    private record Entry(String fingerprint, AutoCloseable client) {
    }

    private final Map<String, Entry> cache = new ConcurrentHashMap<>();

    MongoClient mongo(BackendTarget t) {
        return get("mongo:", t, target -> MongoSchemaFactory.openClient(operand(target), 8000), MongoClient.class);
    }

    DynamoDbClient dynamo(BackendTarget t) {
        return get("dynamo:", t, target -> DynamoSchemaFactory.buildClient(operand(target)), DynamoDbClient.class);
    }

    S3Client s3(BackendTarget t) {
        return get("s3:", t, target -> S3SchemaFactory.openClient(operand(target)), S3Client.class);
    }

    /** The connector operand map of {@code t} (parsed from its pseudo-URL). */
    static Map<String, Object> operand(BackendTarget t) {
        return t.connectorOperand() != null ? t.connectorOperand()
                : ConnectorOperands.parse(t.jdbcUrl(), t.user(), t.password());
    }

    private synchronized <C extends AutoCloseable> C get(String kind, BackendTarget t,
            Function<BackendTarget, C> factory, Class<C> type) {
        String key = kind + t.name();
        String fingerprint = t.jdbcUrl() + "|" + t.user() + "|" + t.password();
        Entry e = cache.get(key);
        if (e != null && e.fingerprint().equals(fingerprint)) {
            return type.cast(e.client());
        }
        if (e != null) {
            closeQuietly(e.client());
        }
        C client = factory.apply(t);
        cache.put(key, new Entry(fingerprint, client));
        return client;
    }

    @Override
    public synchronized void close() {
        cache.values().forEach(e -> closeQuietly(e.client()));
        cache.clear();
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // best effort
        }
    }
}
