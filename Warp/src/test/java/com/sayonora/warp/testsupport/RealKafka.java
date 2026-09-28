package com.sayonora.warp.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A real, disposable single-broker KRaft-mode Kafka container ({@code apache/kafka:latest}),
 * managed via the plain {@code docker} CLI -- same discipline as every other {@code Real*} test
 * helper in this project (see {@link RealPostgres}'s own javadoc for why Testcontainers is
 * deliberately avoided here).
 *
 * <p>The {@code apache/kafka} image bakes a fixed {@code advertised.listeners=PLAINTEXT://localhost:9092}
 * into its default config when NO {@code KAFKA_*} env var is supplied -- confirmed live that setting
 * even one such env var switches the entrypoint into "regenerate the whole server.properties" mode,
 * which then fails fast with {@code Missing required configuration "process.roles"} unless every
 * required KRaft property is supplied explicitly. So, to get a real, host-reachable, DYNAMIC port
 * (this project's convention, to allow parallel test runs), this helper supplies the full minimal
 * single-node KRaft property set explicitly (process.roles, node.id, controller.quorum.voters,
 * listeners, advertised.listeners, inter.broker.listener.name, controller.listener.names) rather
 * than relying on the image's own baked-in defaults.
 */
public final class RealKafka implements AutoCloseable {

    private static final String IMAGE = "apache/kafka:latest";

    private final String containerName;
    private final int port;

    private RealKafka(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealKafka start() throws IOException, InterruptedException {
        String containerName = "warp-test-kafka-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":" + port,
                "-e", "KAFKA_PROCESS_ROLES=broker,controller",
                "-e", "KAFKA_NODE_ID=1",
                "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e", "KAFKA_LISTENERS=PLAINTEXT://:" + port + ",CONTROLLER://:9093",
                "-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + port,
                "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                IMAGE);
        RealKafka kafka = new RealKafka(containerName, port);
        kafka.waitUntilReady(Duration.ofSeconds(60));
        return kafka;
    }

    public String bootstrapServers() {
        return "localhost:" + port;
    }

    /** Publishes {@code value} (a JSON string, typically) to {@code topic} via a real
     * {@link KafkaProducer}, blocking until the broker acknowledges the write -- so a caller that
     * proceeds to query immediately after this returns sees the message in a real broker scan. */
    public void produce(String topic, String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            try {
                producer.send(new ProducerRecord<>(topic, key, value)).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("failed to produce to real Kafka topic " + topic, e);
            }
        }
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        RuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            try (Admin admin = Admin.create(props)) {
                admin.listTopics().names().get(5, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                lastFailure = new IllegalStateException(e);
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("Kafka container " + containerName + " did not become ready within "
                + timeout, lastFailure);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("command failed: " + String.join(" ", command) + "\n" + output);
        }
    }

    @Override
    public void close() {
        try {
            run("docker", "rm", "-f", "-v", containerName);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
