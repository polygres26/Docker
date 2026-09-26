package com.sayonora.wire.awswire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.sqswire.SqsOperations;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything the new AWS frontends share inside one Warp process: the backend registry, identity/auth config, the metrics
 * collector, and the service instances themselves so they can call each other (SNS delivers into SQS queues through the
 * in-process {@link SqsOperations}; Secrets Manager and SSM encrypt through {@link KmsService}; the unified endpoint and
 * SigV4 validation resolve STS temporary credentials through {@link StsService}).
 */
public final class AwsRuntime {

    public final BackendRegistry registry;
    public final AwsConfig config;
    public final ConnectionGate gate;
    public final SqlMetricsCollector metrics;
    private final Map<String, AwsService> services = new LinkedHashMap<>();
    private volatile SqsOperations sqs;

    public AwsRuntime(BackendRegistry registry, AwsConfig config, ConnectionGate gate, SqlMetricsCollector metrics) {
        this.registry = registry;
        this.config = config;
        this.gate = gate;
        this.metrics = metrics;
        KmsService kms = new KmsService(this);
        register(new SnsService(this));
        register(new KinesisService(this));
        register(new SecretsService(this, kms));
        register(new SsmService(this, kms));
        register(kms);
        register(new StsService(this));
        register(new IamService(this));
    }

    private void register(AwsService s) {
        services.put(s.id(), s);
    }

    public AwsService service(String id) {
        return services.get(id);
    }

    public Map<String, AwsService> services() {
        return services;
    }

    public void attachSqs(SqsOperations ops) {
        this.sqs = ops;
    }

    /** The in-process SQS operations, or null when sqswire is not running in this process. */
    public SqsOperations sqs() {
        return sqs;
    }

    /** Secret (and session token) behind an access key: a configured static pair, or an STS session. */
    public AwsSigV4.Secret secretFor(String accessKey) {
        String s = config.credentials.secretFor(accessKey);
        if (s != null) {
            return new AwsSigV4.Secret(s, null);
        }
        if (accessKey != null && accessKey.startsWith("ASIA")) {
            return ((StsService) services.get("sts")).session(accessKey);
        }
        return null;
    }

    public void startAll() {
        services.values().forEach(AwsService::start);
    }

    public void stopAll() {
        services.values().forEach(AwsService::stop);
    }
}
