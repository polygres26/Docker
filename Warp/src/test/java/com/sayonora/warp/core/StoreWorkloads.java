package com.sayonora.warp.core;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.warp.testsupport.BrownoutHarness.Client;
import com.sayonora.warp.testsupport.BrownoutHarness.Workload;
import com.sayonora.warp.testsupport.WarpProcess;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * One workload per Warp protocol whose data lives in the Postgres backend (see {@code StoreType}). Every client is set up
 * so that a failed call is visible: no driver-level retries, short timeouts. Result caches are off so reads reach the store.
 */
final class StoreWorkloads {

    private StoreWorkloads() {
    }

    private static final StaticCredentialsProvider AWS_CREDS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"));

    static List<Workload> all() {
        return List.of(dynamodb(), sqs(), mongodb());
    }

    // ---- DynamoDB ----------------------------------------------------------------------------------------

    private static DynamoDbClient dynamo(int port) {
        return DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:" + port)).region(Region.US_EAST_1)
                .credentialsProvider(AWS_CREDS)
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build())
                        .apiCallTimeout(java.time.Duration.ofSeconds(10)))
                .build();
    }

    static Workload dynamodb() {
        return new Workload() {
            @Override
            public String name() {
                return "dynamowire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("dynamowire", "WARP_DYNAMOWIRE_PORT").env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false");
                stores.enable("dynamodb");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (DynamoDbClient d = dynamo(warp.port("dynamowire"))) {
                    d.createTable(CreateTableRequest.builder().tableName("bo_items")
                            .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                            .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                            .billingMode(BillingMode.PAY_PER_REQUEST).build());
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                DynamoDbClient d = dynamo(warp.port("dynamowire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        d.putItem(b -> b.tableName("bo_items").item(Map.of("id", AttributeValue.fromS(String.valueOf(id)),
                                "n", AttributeValue.fromN("1"))));
                    }

                    @Override
                    public void read(long id) {
                        d.getItem(b -> b.tableName("bo_items").key(Map.of("id", AttributeValue.fromS(String.valueOf(id)))));
                    }

                    @Override
                    public void close() {
                        d.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                try (DynamoDbClient d = dynamo(warp.port("dynamowire"))) {
                    d.scanPaginator(b -> b.tableName("bo_items")).items().forEach(i -> ids.add(Long.parseLong(i.get("id").s())));
                }
                return ids;
            }
        };
    }

    // ---- SQS ---------------------------------------------------------------------------------------------

    private static SqsClient sqsClient(int port) {
        return SqsClient.builder().endpointOverride(URI.create("http://localhost:" + port)).region(Region.US_EAST_1)
                .credentialsProvider(AWS_CREDS)
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build())
                        .apiCallTimeout(java.time.Duration.ofSeconds(10)))
                .build();
    }

    static Workload sqs() {
        return new Workload() {
            private volatile String queueUrl;

            @Override
            public String name() {
                return "sqswire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("sqswire", "WARP_SQSWIRE_PORT");
                stores.enable("sqs");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (SqsClient c = sqsClient(warp.port("sqswire"))) {
                    queueUrl = c.createQueue(b -> b.queueName("bo")).queueUrl();
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                SqsClient c = sqsClient(warp.port("sqswire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        c.sendMessage(b -> b.queueUrl(queueUrl).messageBody(String.valueOf(id)));
                    }

                    @Override
                    public void read(long id) {
                        c.getQueueAttributes(b -> b.queueUrl(queueUrl).attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
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
                try (SqsClient c = sqsClient(warp.port("sqswire"))) {
                    int empty = 0;
                    while (empty < 3) {
                        var msgs = c.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10).visibilityTimeout(600)).messages();
                        if (msgs.isEmpty()) {
                            empty++;
                        }
                        msgs.forEach(m -> ids.add(Long.parseLong(m.body())));
                    }
                }
                return ids;
            }
        };
    }

    // ---- MongoDB -----------------------------------------------------------------------------------------

    private static MongoClient mongo(int port) {
        return MongoClients.create("mongodb://localhost:" + port
                + "/?directConnection=true&retryWrites=false&retryReads=false&serverSelectionTimeoutMS=5000&socketTimeoutMS=10000&connectTimeoutMS=5000");
    }

    static Workload mongodb() {
        return new Workload() {
            @Override
            public String name() {
                return "mongowire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("mongowire", "WARP_MONGOWIRE_PORT").env("WARP_MONGOWIRE_CACHE_ENABLED", "false");
                stores.enable("mongodb");
            }

            @Override
            public Client open(WarpProcess warp) {
                MongoClient m = mongo(warp.port("mongowire"));
                MongoCollection<Document> col = m.getDatabase("bo").getCollection("c");
                return new Client() {
                    @Override
                    public void write(long id) {
                        col.insertOne(new Document("_id", id).append("n", 1));
                    }

                    @Override
                    public void read(long id) {
                        col.find(new Document("_id", id)).first();
                    }

                    @Override
                    public void close() {
                        m.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                try (MongoClient m = mongo(warp.port("mongowire"))) {
                    for (Document d : m.getDatabase("bo").getCollection("c").find()) {
                        ids.add(((Number) d.get("_id")).longValue());
                    }
                }
                return ids;
            }
        };
    }
}
