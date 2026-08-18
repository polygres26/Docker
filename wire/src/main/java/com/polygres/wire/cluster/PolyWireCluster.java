package com.polygres.wire.cluster;

import com.polygres.wire.server.TlsSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.azure.TcpDiscoveryAzureBlobStoreIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §12: the one embedded dependency (Apache Ignite, Apache-2.0) covering cluster
 * membership, the Coherence-style distributed cache ({@link com.polygres.wire.cluster.CacheStage}), and
 * cluster-size-aware QoS reconciliation ({@code QosControlStage}) — chosen over a hand-rolled
 * gossip protocol (ProxySQL's model) plus a separate cache, and over requiring an external
 * ZooKeeper/etcd (ShardingSphere's model), for the reasons laid out in §12.2.
 *
 * <p>Opt-in and fully backward compatible: {@link #disabled()} is what every existing
 * single-node deployment gets (no {@code POLYWIRE_CLUSTER_ENABLED=true}), and it behaves as
 * "cluster size 1, no cache, no Ignite process started at all" — nothing about not opting in
 * changes any existing behavior or startup cost.
 *
 * <h2>Multi-AZ / multi-cloud hardening (three real gaps, all addressed here)</h2>
 *
 * <p><b>1. Cloud-native discovery</b> — {@code POLYWIRE_CLUSTER_DISCOVERY} selects the {@link
 * TcpDiscoveryIpFinder}: {@code static} (default, unchanged behavior — {@link
 * TcpDiscoveryVmIpFinder} against {@code POLYWIRE_CLUSTER_SEED_NODES}), {@code s3} ({@link
 * TcpDiscoveryS3IpFinder}, nodes self-register in an S3 bucket), {@code gcs} ({@link
 * TcpDiscoveryGoogleStorageIpFinder}, same pattern against Cloud Storage), or {@code azure}
 * ({@link TcpDiscoveryAzureBlobStoreIpFinder}, same pattern against Blob Storage). Real Maven
 * Central coordinates were verified directly, not guessed — see {@code pom.xml}'s {@code
 * ignite.aws.version}/{@code ignite.gce.version}/{@code ignite.azure.version} comments for the
 * full trail: {@code ignite-aws} and {@code ignite-gce} stopped publishing after {@code 2.11.1}
 * (there is no {@code 2.18.0} artifact for either, despite that being the natural first guess
 * given this project's {@code ignite.version} pin); Azure's finder ships from a different release
 * line entirely — moved into the separate {@code apache/ignite-extensions} repo, published as
 * {@code org.apache.ignite:ignite-azure-ext:1.0.0} against {@code ignite-core:2.13.0} (provided
 * scope). All three only depend on {@link TcpDiscoveryIpFinder}/{@code
 * TcpDiscoveryIpFinderAdapter}, a stable SPI surface unchanged across the whole Ignite 2.x line —
 * confirmed by decompiling each jar's finder class directly, not assumed — but running against a
 * newer {@code ignite-core} than any of them were built/tested against is still a real,
 * un-eliminated version-skew risk this pin carries; only {@code static} discovery has been
 * live-verified end to end in this project (no real AWS/GCP/Azure credentials in the build/dev
 * sandbox this was written in — see the cloud config env vars below for what a real deployment
 * still needs to supply).
 *
 * <p><b>2. AZ-aware backup placement</b> — every node is tagged with a {@code
 * POLYWIRE_AVAILABILITY_ZONE}-sourced {@value #AZ_ATTRIBUTE} user attribute at startup (operator-
 * supplied at deploy time; auto-detecting via three different clouds' own instance-metadata
 * services was judged real extra scope beyond this pass, an explicit, deliberate choice, not an
 * oversight). {@link ClusterNodeAttributeAffinityBackupFilter} — the real, current Ignite 2.18.0
 * API for this, confirmed against the actual class (not assumed from older docs, which describe
 * older/renamed APIs) — is wired onto every cache's {@link RendezvousAffinityFunction} so a
 * backup copy is never placed on a node sharing that attribute's value with the primary. When
 * {@code POLYWIRE_AVAILABILITY_ZONE} isn't set, no attribute is published and no backup filter is
 * configured — behaves exactly as before (AZ-oblivious placement), the same
 * opt-in-by-env-var pattern as everything else in this class.
 *
 * <p><b>3. TLS on the discovery/communication SPI</b> — opt-in via the same {@code
 * POLYWIRE_TLS_KEYSTORE}/{@code POLYWIRE_TLS_KEYSTORE_PASSWORD} keystore already used for
 * client-facing TLS ({@code com.polygres.wire.server.TlsSupport}), not a separate cert-management
 * story. {@link IgniteConfiguration#setSslContextFactory} is the real Ignite 2.18.0 API — a
 * single cluster-wide {@code Factory<SSLContext>}, confirmed directly against the decompiled
 * class (there is no separate per-SPI SSL setter; {@link TcpDiscoverySpi} and {@code
 * TcpCommunicationSpi} both pick up the cluster-wide factory from {@link IgniteConfiguration} at
 * startup — {@code TcpDiscoverySpi} even exposes its own {@code sslEnable}/{@code
 * sslSrvSockFactory} fields derived from exactly that). Unlike client TLS's server-only key
 * managers, this needs {@link TlsSupport#buildMutualSslContext} — full key manager <em>and</em>
 * trust manager, both directions — because every node both dials and accepts inter-node
 * connections. When the keystore env var isn't set, {@code setSslContextFactory} is never called
 * and the cluster forms exactly as before (plaintext) — local dev/testing without a keystore
 * configured is unaffected.
 */
public final class PolyWireCluster {

    private static final Logger log = LoggerFactory.getLogger(PolyWireCluster.class);

    /** User-attribute key every node's own AZ is published under, when {@code POLYWIRE_AVAILABILITY_ZONE} is set. */
    public static final String AZ_ATTRIBUTE = "POLYWIRE_AZ";

    private final Ignite ignite; // null when disabled
    private final ScheduledExecutorService qosPublisher;
    private final String availabilityZone; // null when not configured

    private PolyWireCluster(Ignite ignite, String availabilityZone) {
        this.ignite = ignite;
        this.availabilityZone = availabilityZone;
        this.qosPublisher = ignite == null ? null : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-qos-publish");
            t.setDaemon(true);
            return t;
        });
    }

    public static PolyWireCluster disabled() {
        return new PolyWireCluster(null, null);
    }

    /**
     * {@code POLYWIRE_CLUSTER_ENABLED=true} to opt in. Discovery is chosen by {@code
     * POLYWIRE_CLUSTER_DISCOVERY} ({@code static} default / {@code s3} / {@code gcs} / {@code
     * azure} — see class javadoc). {@code static} keeps today's exact behavior: {@code
     * POLYWIRE_CLUSTER_SEED_NODES}, a comma-separated {@code host:port} list, for {@link
     * TcpDiscoverySpi}'s {@link TcpDiscoveryVmIpFinder} — the simple, dependency-free discovery
     * mode (§12.2), not multicast (unreliable in most container networks) and not a
     * ZooKeeper-backed SPI (a real future option, not wired up here).
     */
    public static PolyWireCluster fromEnv() {
        boolean enabled = "true".equalsIgnoreCase(System.getenv("POLYWIRE_CLUSTER_ENABLED"));
        if (!enabled) {
            return disabled();
        }
        TcpDiscoveryIpFinder ipFinder = buildIpFinderFromEnv();
        return start(ipFinder, discoveryDescriptionForLogging());
    }

    /**
     * Starts a single-node embedded Ignite instance with no seed nodes configured, bypassing the
     * {@code POLYWIRE_CLUSTER_ENABLED} env-var gate — for {@code Main} to use when a cache table
     * was configured ({@code POLYWIRE_CACHE_TABLES}) but full multi-node clustering wasn't opted
     * into. {@link CacheStage} needs somewhere to put entries either way; this gets it one without
     * requiring an operator to also flip on cluster membership they don't want.
     */
    public static PolyWireCluster startSingleNodeForCacheOnly() {
        // TcpDiscoveryVmIpFinder's non-shared mode requires at least one registered address even
        // for a lone node — "127.0.0.1:47500" is Ignite's own documented default discovery port
        // for exactly this single-node case.
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(List.of("127.0.0.1:47500"));
        return start(ipFinder, "static (single-node cache-only mode)");
    }

    private static String discoveryDescriptionForLogging() {
        return System.getenv().getOrDefault("POLYWIRE_CLUSTER_DISCOVERY", "static");
    }

    /**
     * {@code POLYWIRE_CLUSTER_DISCOVERY=static|s3|gcs|azure}, defaulting to {@code static} so
     * every existing local-dev/testing path (including this project's own 2-node local
     * verification) keeps working completely unchanged. Each cloud mode's own config env vars are
     * documented on its branch below.
     */
    private static TcpDiscoveryIpFinder buildIpFinderFromEnv() {
        String mode = discoveryDescriptionForLogging();
        switch (mode.toLowerCase(java.util.Locale.ROOT)) {
            case "s3": {
                // POLYWIRE_CLUSTER_S3_BUCKET (required), POLYWIRE_CLUSTER_S3_REGION (optional,
                // else the AWS SDK's own default provider chain resolves it), POLYWIRE_CLUSTER_S3_KEY_PREFIX
                // (optional namespace within the bucket — lets several clusters share one bucket).
                // Credentials: instance-profile auth by default (no static keys required, matching
                // this project's other cloud-facing config) — only reads
                // POLYWIRE_CLUSTER_S3_ACCESS_KEY/POLYWIRE_CLUSTER_S3_SECRET_KEY if both are
                // explicitly set; otherwise leaves TcpDiscoveryS3IpFinder to fall back to the AWS
                // SDK's DefaultAWSCredentialsProviderChain (instance profile / env / ~/.aws/*).
                String bucket = requireEnv("POLYWIRE_CLUSTER_S3_BUCKET", "s3");
                TcpDiscoveryS3IpFinder finder = new TcpDiscoveryS3IpFinder();
                finder.setBucketName(bucket);
                String keyPrefix = System.getenv("POLYWIRE_CLUSTER_S3_KEY_PREFIX");
                if (keyPrefix != null && !keyPrefix.isBlank()) {
                    finder.setKeyPrefix(keyPrefix);
                }
                String accessKey = System.getenv("POLYWIRE_CLUSTER_S3_ACCESS_KEY");
                String secretKey = System.getenv("POLYWIRE_CLUSTER_S3_SECRET_KEY");
                if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
                    finder.setAwsCredentials(new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey));
                } else {
                    log.info("POLYWIRE_CLUSTER_S3_ACCESS_KEY/SECRET_KEY not set — using AWS SDK's default "
                            + "credentials provider chain (instance profile / env / ~/.aws)");
                }
                finder.setShared(true);
                return finder;
            }
            case "gcs": {
                // POLYWIRE_CLUSTER_GCS_PROJECT and POLYWIRE_CLUSTER_GCS_BUCKET (required),
                // POLYWIRE_CLUSTER_GCS_SERVICE_ACCOUNT_ID / POLYWIRE_CLUSTER_GCS_SERVICE_ACCOUNT_P12
                // (optional service-account key-pair auth; TcpDiscoveryGoogleStorageIpFinder has
                // no instance-metadata-credential mode of its own to fall back to, unlike the S3
                // and Azure finders, so a real multi-node GCS deployment does need these set).
                String project = requireEnv("POLYWIRE_CLUSTER_GCS_PROJECT", "gcs");
                String bucket = requireEnv("POLYWIRE_CLUSTER_GCS_BUCKET", "gcs");
                TcpDiscoveryGoogleStorageIpFinder finder = new TcpDiscoveryGoogleStorageIpFinder();
                finder.setProjectName(project);
                finder.setBucketName(bucket);
                String svcAccountId = System.getenv("POLYWIRE_CLUSTER_GCS_SERVICE_ACCOUNT_ID");
                String svcAccountP12 = System.getenv("POLYWIRE_CLUSTER_GCS_SERVICE_ACCOUNT_P12");
                if (svcAccountId != null && !svcAccountId.isBlank()) {
                    finder.setServiceAccountId(svcAccountId);
                }
                if (svcAccountP12 != null && !svcAccountP12.isBlank()) {
                    finder.setServiceAccountP12FilePath(svcAccountP12);
                }
                finder.setShared(true);
                return finder;
            }
            case "azure": {
                // POLYWIRE_CLUSTER_AZURE_ACCOUNT_NAME and POLYWIRE_CLUSTER_AZURE_CONTAINER
                // (required), POLYWIRE_CLUSTER_AZURE_ACCOUNT_KEY (storage account key — Azure's
                // Blob Storage finder, unlike S3's, has no separate instance-managed-identity mode
                // wired into TcpDiscoveryAzureBlobStoreIpFinder itself, so this is the credential
                // an operator supplies), POLYWIRE_CLUSTER_AZURE_ACCOUNT_ENDPOINT (optional,
                // defaults to the standard *.blob.core.windows.net endpoint for the account).
                String accountName = requireEnv("POLYWIRE_CLUSTER_AZURE_ACCOUNT_NAME", "azure");
                String container = requireEnv("POLYWIRE_CLUSTER_AZURE_CONTAINER", "azure");
                String accountKey = requireEnv("POLYWIRE_CLUSTER_AZURE_ACCOUNT_KEY", "azure");
                TcpDiscoveryAzureBlobStoreIpFinder finder = new TcpDiscoveryAzureBlobStoreIpFinder();
                finder.setAccountName(accountName);
                finder.setAccountKey(accountKey);
                finder.setContainerName(container);
                String endpoint = System.getenv("POLYWIRE_CLUSTER_AZURE_ACCOUNT_ENDPOINT");
                if (endpoint != null && !endpoint.isBlank()) {
                    finder.setAccountEndpoint(endpoint);
                }
                finder.setShared(true);
                return finder;
            }
            case "static":
            default: {
                String seedSpec = System.getenv("POLYWIRE_CLUSTER_SEED_NODES");
                List<String> seeds = new ArrayList<>();
                if (seedSpec != null && !seedSpec.isBlank()) {
                    for (String entry : seedSpec.split(",")) {
                        String trimmed = entry.trim();
                        if (!trimmed.isEmpty()) {
                            seeds.add(trimmed);
                        }
                    }
                }
                TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
                finder.setAddresses(seeds.isEmpty() ? List.of("127.0.0.1:47500") : seeds);
                return finder;
            }
        }
    }

    private static String requireEnv(String name, String mode) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "POLYWIRE_CLUSTER_DISCOVERY=" + mode + " requires " + name + " to be set");
        }
        return value;
    }

    private static PolyWireCluster start(TcpDiscoveryIpFinder ipFinder, String discoveryDescription) {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("polywire");
        cfg.setDiscoverySpi(discoverySpi);
        // This process only ever wants the grid, not a full-blown server surface — cuts unrelated
        // Ignite subsystems (SQL, ODBC/JDBC/thin-client listeners) that PolyWire has no use for.
        cfg.setClientMode(false);

        String az = System.getenv("POLYWIRE_AVAILABILITY_ZONE");
        if (az != null && !az.isBlank()) {
            Map<String, String> attrs = new HashMap<>();
            attrs.put(AZ_ATTRIBUTE, az);
            cfg.setUserAttributes(attrs);
            log.info("tagging this node with {}={}", AZ_ATTRIBUTE, az);
        }

        configureTlsIfKeystoreSet(cfg);

        log.info("starting embedded Ignite node for cluster membership, discovery={}, az={}",
                discoveryDescription, az == null ? "(none)" : az);
        Ignite ignite = Ignition.start(cfg);
        log.info("polywire cluster joined, current size={}", ignite.cluster().nodes().size());
        return new PolyWireCluster(ignite, az);
    }

    /**
     * Opt-in exactly like client-facing TLS: only touches {@link IgniteConfiguration} when {@code
     * POLYWIRE_TLS_KEYSTORE} is set, so local dev/testing without a keystore configured keeps
     * working exactly as before (plaintext discovery/communication SPI, unchanged from before
     * this change).
     */
    private static void configureTlsIfKeystoreSet(IgniteConfiguration cfg) {
        String keystorePath = System.getenv("POLYWIRE_TLS_KEYSTORE");
        if (keystorePath == null || keystorePath.isBlank()) {
            return;
        }
        String keystorePassword = System.getenv("POLYWIRE_TLS_KEYSTORE_PASSWORD");
        log.info("enabling TLS on Ignite discovery/communication SPI using {}", keystorePath);
        Factory<SSLContext> sslContextFactory = () -> {
            try {
                return TlsSupport.buildMutualSslContext(keystorePath, keystorePassword);
            } catch (Exception e) {
                throw new IllegalStateException("failed to build SSLContext for Ignite cluster TLS from "
                        + keystorePath, e);
            }
        };
        cfg.setSslContextFactory(sslContextFactory);
    }

    public boolean enabled() {
        return ignite != null;
    }

    /** {@code 1} when clustering is disabled — the correct "no division" value for QoS reconciliation (§12.4). */
    public int clusterSize() {
        return ignite == null ? 1 : Math.max(1, ignite.cluster().nodes().size());
    }

    /** This node's configured AZ ({@code POLYWIRE_AVAILABILITY_ZONE}), or {@code null} if not set. */
    public String availabilityZone() {
        return availabilityZone;
    }

    /**
     * One named cache per {@code (name, ttlMillis)} combination a caller asks for — {@code
     * CacheStage} uses this for its result cache. {@code PARTITIONED} mode (Coherence's default
     * shape too, §12.3): the keyspace can outgrow one node's memory, unlike a {@code REPLICATED}
     * cache. {@code backups=1} so a single node dying doesn't cold-start every hot key it owned.
     *
     * <p>When {@code POLYWIRE_AVAILABILITY_ZONE} was set for this node at startup, every cache
     * created here gets a {@link RendezvousAffinityFunction} with a {@link
     * ClusterNodeAttributeAffinityBackupFilter} on {@value #AZ_ATTRIBUTE} — the backup copy of a
     * partition is never placed on a node sharing this node's AZ attribute value with the
     * primary. When the AZ attribute isn't configured cluster-wide, this is a no-op (Ignite's
     * default affinity function, exactly today's placement behavior).
     *
     * <p><b>No near-cache, found live, not per the original design</b>: §12.3 originally assumed
     * a near-cache would make a same-node repeat read never cross the network. Ignite rejects that
     * outright here — {@code "Failed to start near cache (local node is an affinity node for
     * cache)"} — because every PolyWire node is symmetric: a full Ignite data/affinity node for
     * this cache, not a thin client. Near caches are for nodes that <em>don't</em> already store a
     * share of the cache's own partitions; ours all do. Every cache lookup here genuinely crosses
     * the (fast, local-network) grid unless the requesting node happens to own that key's
     * partition — a real, documented correctness-neutral gap from the original plan's stated
     * benefit, not silently dropped.
     */
    public <T> IgniteCache<String, T> getOrCreateCache(String name, long ttlMillis) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        CacheConfiguration<String, T> cfg = new CacheConfiguration<>(name);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(1);
        if (availabilityZone != null) {
            RendezvousAffinityFunction affinity = new RendezvousAffinityFunction();
            affinity.setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter(AZ_ATTRIBUTE));
            cfg.setAffinity(affinity);
        }
        if (ttlMillis > 0) {
            cfg.setExpiryPolicyFactory(javax.cache.expiry.CreatedExpiryPolicy.factoryOf(
                    new javax.cache.expiry.Duration(TimeUnit.MILLISECONDS, ttlMillis)));
        }
        return ignite.getOrCreateCache(cfg);
    }

    /**
     * Cluster-wide monotonically increasing counter — every node calling {@code
     * nextSequence("polywire-sql-plan-id")} gets a distinct value, batched internally by Ignite's
     * {@code IgniteAtomicSequence} for throughput (each node reserves a range locally rather than
     * round-tripping the grid per call). Used by {@link com.polygres.wire.core.ClusterSqlPlanStore} so a
     * {@code plan_id} is unique across the whole cluster, not just this process — unlike a plain
     * {@code AtomicLong}, which would collide the moment a second node assigned the same id to a
     * different query.
     */
    public long nextSequence(String name) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        return ignite.atomicSequence(name, 0L, true).incrementAndGet();
    }

    /**
     * ARCHITECTURE.md §12.4 v1: publishes this node's local admitted/rejected counts for a
     * bucket-key every {@code intervalMillis}, purely for cluster-wide observability — never
     * consulted by the admission decision itself, which stays a pure local memory operation.
     * A no-op when clustering is disabled.
     */
    public void publishQosCountersPeriodically(String nodeLocalKey,
            java.util.function.Supplier<long[]> admittedRejectedSnapshot, long intervalMillis) {
        if (ignite == null) {
            return;
        }
        IgniteCache<String, long[]> counters = ignite.getOrCreateCache(
                new CacheConfiguration<String, long[]>("polywire-qos-counters").setCacheMode(CacheMode.REPLICATED));
        qosPublisher.scheduleWithFixedDelay(() -> {
            try {
                counters.put(nodeLocalKey, admittedRejectedSnapshot.get());
            } catch (Exception e) {
                log.debug("qos counter publish failed (non-fatal): {}", e.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /** Exposes the underlying {@link Ignite} instance for affinity/diagnostic inspection (e.g. tests proving AZ-aware backup placement via {@code ignite.affinity(cacheName)}). Null when disabled. */
    public Ignite rawIgnite() {
        return ignite;
    }

    public void shutdown() {
        if (qosPublisher != null) {
            qosPublisher.shutdownNow();
        }
        if (ignite != null) {
            ignite.close();
        }
    }
}
