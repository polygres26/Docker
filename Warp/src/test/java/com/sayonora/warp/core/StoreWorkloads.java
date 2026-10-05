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
        return List.of(dynamodb(), sqs(), mongodb(), s3(), kafka(), cql(), bolt(), influx(), opensearch());
    }

    // ---- DynamoDB ----------------------------------------------------------------------------------------

    private static DynamoDbClient dynamo(int port) {
        return dynamo(port, false);
    }

    /** {@code sdkDefaults}: leave the SDK's own retry policy and timeouts alone, as an application normally would. */
    private static DynamoDbClient dynamo(int port, boolean sdkDefaults) {
        if (sdkDefaults) {
            return DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:" + port)).region(Region.US_EAST_1)
                    .credentialsProvider(AWS_CREDS).build();
        }
        return DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:" + port)).region(Region.US_EAST_1)
                .credentialsProvider(AWS_CREDS)
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build())
                        .apiCallTimeout(java.time.Duration.ofSeconds(10)))
                .build();
    }

    static Workload dynamodb() {
        return dynamodb(false);
    }

    static Workload dynamodb(boolean sdkDefaults) {
        return new Workload() {
            @Override
            public String name() {
                return sdkDefaults ? "dynamowire-sdk-defaults" : "dynamowire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("dynamowire", "WARP_DYNAMOWIRE_PORT").env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false");
                stores.enable("dynamodb");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (DynamoDbClient d = dynamo(warp.port("dynamowire"), sdkDefaults)) {
                    d.createTable(CreateTableRequest.builder().tableName("bo_items")
                            .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                            .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                            .billingMode(BillingMode.PAY_PER_REQUEST).build());
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                DynamoDbClient d = dynamo(warp.port("dynamowire"), sdkDefaults);
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
                try (DynamoDbClient d = dynamo(warp.port("dynamowire"), sdkDefaults)) {
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

    // ---- S3 ----------------------------------------------------------------------------------------------

    private static software.amazon.awssdk.services.s3.S3Client s3Client(int port) {
        return software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1).credentialsProvider(AWS_CREDS).forcePathStyle(true)
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build())
                        .apiCallTimeout(java.time.Duration.ofSeconds(10)))
                .build();
    }

    static Workload s3() {
        return new Workload() {
            @Override
            public String name() {
                return "s3wire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                // s3wire refuses to start without credentials, by design (no unauthenticated S3)
                warp.frontend("s3wire", "WARP_S3WIRE_PORT").env("WARP_S3WIRE_CREDENTIALS", "test-access-key=test-secret-key");
                stores.enable("s3");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (var c = s3Client(warp.port("s3wire"))) {
                    c.createBucket(b -> b.bucket("bo-bucket"));
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                var c = s3Client(warp.port("s3wire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        c.putObject(b -> b.bucket("bo-bucket").key(String.valueOf(id)),
                                software.amazon.awssdk.core.sync.RequestBody.fromString("payload-" + id));
                    }

                    @Override
                    public void read(long id) {
                        try {
                            c.headObject(b -> b.bucket("bo-bucket").key(String.valueOf(id)));
                        } catch (software.amazon.awssdk.services.s3.model.NoSuchKeyException absent) {
                            // a read of a key that is not there is a successful read
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
                try (var c = s3Client(warp.port("s3wire"))) {
                    c.listObjectsV2Paginator(b -> b.bucket("bo-bucket")).contents().forEach(o -> ids.add(Long.parseLong(o.key())));
                }
                return ids;
            }
        };
    }

    // ---- Kafka -------------------------------------------------------------------------------------------

    private static java.util.Properties kafkaProps(int port) {
        java.util.Properties p = new java.util.Properties();
        p.put("bootstrap.servers", "localhost:" + port);
        p.put("request.timeout.ms", "10000");
        p.put("default.api.timeout.ms", "10000");
        p.put("socket.connection.setup.timeout.ms", "5000");
        return p;
    }

    private static org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer(int port, boolean sdkDefaults) {
        java.util.Properties p = new java.util.Properties();
        p.put("bootstrap.servers", "localhost:" + port);
        if (!sdkDefaults) {
            p = kafkaProps(port);
            p.put("acks", "all");
            p.put("retries", "0");
            p.put("max.block.ms", "5000");
            p.put("delivery.timeout.ms", "15000");
            p.put("enable.idempotence", "false");
        }
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new org.apache.kafka.clients.producer.KafkaProducer<>(p);
    }

    static Workload kafka() {
        return kafka(false);
    }

    /** {@code sdkDefaults}: a producer with only the bootstrap address set (retries, idempotence and the 120 s delivery
     * timeout are the client's own defaults), and writes that wait for the send to finish. */
    static Workload kafka(boolean sdkDefaults) {
        return new Workload() {
            @Override
            public String name() {
                return sdkDefaults ? "kafkawire-sdk-defaults" : "kafkawire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("kafkawire", "WARP_KAFKAWIRE_PORT");
                stores.enable("kafka");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                try (var admin = org.apache.kafka.clients.admin.AdminClient.create(kafkaProps(warp.port("kafkawire")))) {
                    admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic("bo-topic", 1, (short) 1))).all().get();
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                var producer = kafkaProducer(warp.port("kafkawire"), sdkDefaults);
                var admin = org.apache.kafka.clients.admin.AdminClient.create(kafkaProps(warp.port("kafkawire")));
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("bo-topic", String.valueOf(id), "v"))
                                .get(sdkDefaults ? 130 : 15, java.util.concurrent.TimeUnit.SECONDS);
                    }

                    @Override
                    public void read(long id) throws Exception {
                        admin.describeTopics(List.of("bo-topic")).allTopicNames().get(10, java.util.concurrent.TimeUnit.SECONDS);
                    }

                    @Override
                    public void close() {
                        producer.close(java.time.Duration.ofSeconds(1));
                        admin.close(java.time.Duration.ofSeconds(1));
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                java.util.Properties p = kafkaProps(warp.port("kafkawire"));
                p.put("group.id", "bo-verify");
                p.put("enable.auto.commit", "false");
                p.put("auto.offset.reset", "earliest");
                p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(p)) {
                    var tp = new org.apache.kafka.common.TopicPartition("bo-topic", 0);
                    consumer.assign(List.of(tp));
                    consumer.seekToBeginning(List.of(tp));
                    int empty = 0;
                    while (empty < 3) {
                        var recs = consumer.poll(java.time.Duration.ofSeconds(2));
                        if (recs.isEmpty()) {
                            empty++;
                        } else {
                            empty = 0;
                        }
                        recs.forEach(r -> ids.add(Long.parseLong(r.key())));
                    }
                }
                return ids;
            }
        };
    }

    // ---- Cassandra (CQL) ---------------------------------------------------------------------------------

    private static com.datastax.oss.driver.api.core.CqlSession cqlSession(int port) {
        return com.datastax.oss.driver.api.core.CqlSession.builder()
                .addContactPoint(new java.net.InetSocketAddress("localhost", port)).withLocalDatacenter("datacenter1")
                .withConfigLoader(com.datastax.oss.driver.api.core.config.DriverConfigLoader.programmaticBuilder()
                        .withDuration(com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(10))
                        .withDuration(com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, java.time.Duration.ofSeconds(5))
                        .build())
                .build();
    }

    static Workload cql() {
        return new Workload() {
            @Override
            public String name() {
                return "cqlwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("cqlwire", "WARP_CQLWIRE_PORT");
                stores.enable("cql");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (var s = cqlSession(warp.port("cqlwire"))) {
                    s.execute("CREATE KEYSPACE IF NOT EXISTS bo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                    s.execute("CREATE TABLE IF NOT EXISTS bo.items (id bigint PRIMARY KEY, n int)");
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                var s = cqlSession(warp.port("cqlwire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        s.execute(com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(
                                "INSERT INTO bo.items (id, n) VALUES (?, ?)", id, 1));
                    }

                    @Override
                    public void read(long id) {
                        s.execute(com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance("SELECT n FROM bo.items WHERE id = ?", id));
                    }

                    @Override
                    public void close() {
                        s.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                try (var s = cqlSession(warp.port("cqlwire"))) {
                    s.execute("SELECT id FROM bo.items").forEach(r -> ids.add(r.getLong(0)));
                }
                return ids;
            }
        };
    }

    // ---- Neo4j (Bolt) ------------------------------------------------------------------------------------

    private static org.neo4j.driver.Driver boltDriver(int port) {
        return org.neo4j.driver.GraphDatabase.driver("bolt://localhost:" + port, org.neo4j.driver.AuthTokens.basic("warp", "secret"),
                org.neo4j.driver.Config.builder().withConnectionTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                        .withMaxTransactionRetryTime(0, java.util.concurrent.TimeUnit.SECONDS).build());
    }

    static Workload bolt() {
        return new Workload() {
            @Override
            public String name() {
                return "boltwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("boltwire", "WARP_BOLTWIRE_PORT");
                stores.enable("neo4j");
            }

            @Override
            public Client open(WarpProcess warp) {
                var d = boltDriver(warp.port("boltwire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        try (var s = d.session()) {
                            s.run("CREATE (:Item {id: $id, n: 1})", Map.of("id", id)).consume();
                        }
                    }

                    @Override
                    public void read(long id) {
                        try (var s = d.session()) {
                            s.run("MATCH (n:Item {id: $id}) RETURN n.id", Map.of("id", id)).list();
                        }
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
                try (var d = boltDriver(warp.port("boltwire")); var s = d.session()) {
                    s.run("MATCH (n:Item) RETURN n.id AS id").list().forEach(r -> ids.add(r.get("id").asLong()));
                }
                return ids;
            }
        };
    }

    // ---- InfluxDB ----------------------------------------------------------------------------------------

    private static org.influxdb.InfluxDB influx(int port) {
        var http = new okhttp3.OkHttpClient.Builder().connectTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                .readTimeout(10, java.util.concurrent.TimeUnit.SECONDS).retryOnConnectionFailure(false);
        var db = org.influxdb.InfluxDBFactory.connect("http://localhost:" + port, http);
        db.setDatabase("bo");
        return db;
    }

    static Workload influx() {
        return new Workload() {
            @Override
            public String name() {
                return "influxwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("influxwire", "WARP_INFLUXWIRE_PORT");
                stores.enable("influxdb");
            }

            @Override
            public void prepare(WarpProcess warp) {
                try (var db = influx(warp.port("influxwire"))) {
                    db.query(new org.influxdb.dto.Query("CREATE DATABASE bo"));
                }
            }

            @Override
            public Client open(WarpProcess warp) {
                var db = influx(warp.port("influxwire"));
                return new Client() {
                    @Override
                    public void write(long id) {
                        db.write(org.influxdb.dto.Point.measurement("items").tag("k", String.valueOf(id)).addField("idf", id)
                                .time(System.currentTimeMillis(), java.util.concurrent.TimeUnit.MILLISECONDS).build());
                    }

                    @Override
                    public void read(long id) {
                        var r = db.query(new org.influxdb.dto.Query("SELECT idf FROM items LIMIT 1", "bo"));
                        if (r.hasError()) {
                            throw new IllegalStateException(r.getError());
                        }
                    }

                    @Override
                    public void close() {
                        db.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) {
                Set<Long> ids = new HashSet<>();
                try (var db = influx(warp.port("influxwire"))) {
                    var r = db.query(new org.influxdb.dto.Query("SELECT idf FROM items", "bo"));
                    if (r.hasError()) {
                        throw new IllegalStateException(r.getError());
                    }
                    for (var res : r.getResults()) {
                        if (res.getSeries() != null) {
                            for (var series : res.getSeries()) {
                                series.getValues().forEach(v -> ids.add(((Number) v.get(1)).longValue()));
                            }
                        }
                    }
                }
                return ids;
            }
        };
    }

    // ---- OpenSearch --------------------------------------------------------------------------------------

    private static org.opensearch.client.opensearch.OpenSearchClient opensearch(int port) {
        var transport = org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
                .builder(new org.apache.hc.core5.http.HttpHost("http", "localhost", port)).build();
        return new org.opensearch.client.opensearch.OpenSearchClient(transport);
    }

    @SuppressWarnings("unchecked")
    static Workload opensearch() {
        return new Workload() {
            @Override
            public String name() {
                return "oswire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig stores) {
                warp.frontend("oswire", "WARP_OSWIRE_PORT");
                stores.enable("opensearch");
            }

            @Override
            public Client open(WarpProcess warp) {
                var c = opensearch(warp.port("oswire"));
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        c.index(i -> i.index("bo").id(String.valueOf(id)).document(Map.of("n", 1)));
                    }

                    @Override
                    public void read(long id) throws Exception {
                        c.get(g -> g.index("bo").id(String.valueOf(id)), Map.class);
                    }

                    @Override
                    public void close() {
                        try {
                            c._transport().close();
                        } catch (Exception ignored) {
                            // done
                        }
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Set<Long> ids = new HashSet<>();
                var c = opensearch(warp.port("oswire"));
                try {
                    c.indices().refresh(r -> r.index("bo"));
                    var res = c.search(s -> s.index("bo").size(10000), Map.class);
                    res.hits().hits().forEach(h -> ids.add(Long.parseLong(h.id())));
                } finally {
                    c._transport().close();
                }
                return ids;
            }
        };
    }
}
