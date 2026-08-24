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

public final class PolyWireCluster {

    private static final Logger log = LoggerFactory.getLogger(PolyWireCluster.class);

    public static final String AZ_ATTRIBUTE = "POLYWIRE_AZ";

    private final Ignite ignite;
    private final ScheduledExecutorService qosPublisher;
    private final String availabilityZone;

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

    public static PolyWireCluster fromEnv() {
        boolean enabled = "true".equalsIgnoreCase(System.getenv("POLYWIRE_CLUSTER_ENABLED"));
        if (!enabled) {
            return disabled();
        }
        TcpDiscoveryIpFinder ipFinder = buildIpFinderFromEnv();
        return start(ipFinder, discoveryDescriptionForLogging());
    }

    public static PolyWireCluster startSingleNodeForCacheOnly() {
        
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(List.of("127.0.0.1:47500"));
        return start(ipFinder, "static (single-node cache-only mode)");
    }

    private static String discoveryDescriptionForLogging() {
        return System.getenv().getOrDefault("POLYWIRE_CLUSTER_DISCOVERY", "static");
    }

    private static TcpDiscoveryIpFinder buildIpFinderFromEnv() {
        String mode = discoveryDescriptionForLogging();
        switch (mode.toLowerCase(java.util.Locale.ROOT)) {
            case "s3": {
                
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

    public int clusterSize() {
        return ignite == null ? 1 : Math.max(1, ignite.cluster().nodes().size());
    }

    public String availabilityZone() {
        return availabilityZone;
    }

    public <T> IgniteCache<String, T> getOrCreateCache(String name, long ttlMillis) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        CacheConfiguration<String, T> cfg = new CacheConfiguration<>(name);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(backupCountFromEnv());
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

    // POLYWIRE_CLUSTER_CACHE_BACKUPS: how many backup copies Ignite keeps of each cache entry,
    // in addition to the primary -- 1 (the prior hardcoded default) means every entry survives
    // exactly one node/AZ loss; raise it for a larger multi-AZ deployment that wants to survive
    // more than one simultaneous zone loss. Combined with the AZ-aware affinity backup filter
    // above, each backup still lands on a node in a different AZ than the primary (and, with
    // more than one backup, different from each other where the topology allows it), not just a
    // different node in the same AZ.
    private static int backupCountFromEnv() {
        String value = System.getenv("POLYWIRE_CLUSTER_CACHE_BACKUPS");
        if (value == null || value.isBlank()) {
            return 1;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < 0) {
                throw new NumberFormatException("negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            log.warn("POLYWIRE_CLUSTER_CACHE_BACKUPS={} is not a valid non-negative integer -- using default 1",
                    value);
            return 1;
        }
    }

    public long nextSequence(String name) {
        if (ignite == null) {
            throw new IllegalStateException("cluster is disabled — POLYWIRE_CLUSTER_ENABLED not set");
        }
        return ignite.atomicSequence(name, 0L, true).incrementAndGet();
    }

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
