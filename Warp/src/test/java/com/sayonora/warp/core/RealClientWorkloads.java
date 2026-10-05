package com.sayonora.warp.core;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.sayonora.warp.testsupport.BrownoutHarness.Client;
import com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig;
import com.sayonora.warp.testsupport.BrownoutHarness.Workload;
import com.sayonora.warp.testsupport.WarpProcess;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * The same brownout workloads as the raw clients, but through the vendors' own client libraries left at their DEFAULT retry,
 * timeout and reconnect behaviour, the way an application would use them: the Azure Blob SDK, the AWS Secrets Manager SDK and
 * Lettuce for Redis. (DynamoDB and Kafka have "-sdk-defaults" variants in {@link StoreWorkloads}.) The question is what an
 * application sees once its client has retried, not what one raw request sees.
 */
final class RealClientWorkloads {

    private RealClientWorkloads() {
    }

    static List<Workload> all() {
        return List.of(StoreWorkloads.dynamodb(true), StoreWorkloads.kafka(true), secrets(), azblob(), redis(),
                defaults(StoreWorkloads.sqs()), defaults(StoreWorkloads.mongodb()), defaults(StoreWorkloads.s3()),
                defaults(StoreWorkloads.cql()), defaults(StoreWorkloads.bolt()), defaults(StoreWorkloads.influx()));
    }

    /** The same workload with its client library at default settings (see {@link StoreWorkloads#sdkDefaults}). */
    static Workload defaults(Workload w) {
        return new Workload() {
            @Override
            public String name() {
                return w.name() + "-sdk-defaults";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                StoreWorkloads.sdkDefaults = true;
                w.configure(warp, stores);
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                w.prepare(warp);
            }

            @Override
            public Client open(WarpProcess warp) throws Exception {
                return w.open(warp);
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                try {
                    return w.presentIds(warp);
                } finally {
                    StoreWorkloads.sdkDefaults = false;
                }
            }
        };
    }

    // ---- AWS Secrets Manager SDK ---------------------------------------------------------------------------

    private static SecretsManagerClient secretsClient(int port) {
        return SecretsManagerClient.builder().endpointOverride(URI.create("http://localhost:" + port)).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .build();
    }

    static Workload secrets() {
        return new Workload() {
            @Override
            public String name() {
                return "secretswire-sdk-defaults";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("secrets", "WARP_SECRETSWIRE_PORT").env("WARP_KMS_INSECURE_DEV_KEY", "true");
                stores.enable("awsparams");
            }

            @Override
            public Client open(WarpProcess warp) {
                SecretsManagerClient c = secretsClient(warp.port("secrets"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        c.createSecret(b -> b.name("bo-" + id).secretString("v"));
                    }

                    @Override
                    public void read(long id) {
                        try {
                            c.describeSecret(b -> b.secretId("bo-" + id));
                        } catch (ResourceNotFoundException expected) {
                            // not written yet
                        }
                    }

                    @Override
                    public void close() {
                        c.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                try (SecretsManagerClient c = secretsClient(warp.port("secrets"))) {
                    c.listSecretsPaginator(b -> b.maxResults(100)).stream().flatMap(r -> r.secretList().stream()).forEach(s -> {
                        if (s.name().startsWith("bo-")) {
                            ids.add(Long.parseLong(s.name().substring(3)));
                        }
                    });
                }
                return ids;
            }
        };
    }

    // ---- Azure Blob Storage SDK ----------------------------------------------------------------------------

    private static BlobServiceClient blobService(int port) {
        // the well-known development account and key
        return new BlobServiceClientBuilder().endpoint("http://localhost:" + port + "/devstoreaccount1")
                .credential(new StorageSharedKeyCredential("devstoreaccount1",
                        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="))
                .buildClient();
    }

    static Workload azblob() {
        return new Workload() {
            @Override
            public String name() {
                return "azblobwire-sdk-defaults";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("azblob", "WARP_AZBLOBWIRE_PORT").env("WARP_AZURE_DEV_ACCOUNT", "true");
                stores.enable("azblob");
            }

            @Override
            public void prepare(WarpProcess warp) {
                blobService(warp.port("azblob")).createBlobContainer("bo-container");
            }

            @Override
            public Client open(WarpProcess warp) {
                BlobContainerClient c = blobService(warp.port("azblob")).getBlobContainerClient("bo-container");
                return new Client() {
                    @Override
                    public void write(long id) {
                        c.getBlobClient(String.valueOf(id)).upload(com.azure.core.util.BinaryData.fromString("payload-" + id), true);
                    }

                    @Override
                    public void read(long id) {
                        try {
                            c.getBlobClient(String.valueOf(id)).getProperties();
                        } catch (BlobStorageException e) {
                            if (e.getStatusCode() != 404) {
                                throw e;
                            }
                        }
                    }

                    @Override
                    public void close() {
                        // the client holds no connection of its own to close
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                blobService(warp.port("azblob")).getBlobContainerClient("bo-container").listBlobs()
                        .forEach(b -> ids.add(Long.parseLong(b.getName())));
                return ids;
            }
        };
    }

    // ---- Lettuce (Redis) -----------------------------------------------------------------------------------

    private static RedisClient lettuce(int port) {
        return RedisClient.create(RedisURI.builder().withHost("localhost").withPort(port).withTimeout(Duration.ofSeconds(60)).build());
    }

    static Workload redis() {
        return new Workload() {
            @Override
            public String name() {
                return "rediswire-lettuce-defaults";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("rediswire", "WARP_REDISWIRE_PORT");
                stores.enable("redis");
            }

            @Override
            public Client open(WarpProcess warp) {
                RedisClient client = lettuce(warp.port("rediswire"));
                StatefulRedisConnection<String, String> conn = client.connect();
                return new Client() {
                    @Override
                    public void write(long id) {
                        conn.sync().set("bo:" + id, "v");
                    }

                    @Override
                    public void read(long id) {
                        conn.sync().get("bo:" + id);
                    }

                    @Override
                    public void close() {
                        try {
                            conn.close();
                        } finally {
                            client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                RedisClient client = lettuce(warp.port("rediswire"));
                try (StatefulRedisConnection<String, String> conn = client.connect()) {
                    ScanCursor cursor = ScanCursor.INITIAL;
                    do {
                        KeyScanCursor<String> page = conn.sync().scan(cursor, ScanArgs.Builder.matches("bo:*").limit(1000));
                        page.getKeys().forEach(k -> ids.add(Long.parseLong(k.substring(3))));
                        cursor = page;
                    } while (!cursor.isFinished());
                } finally {
                    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
                }
                return ids;
            }
        };
    }
}
