package com.sayonora.warp.mcp;

import com.google.gson.JsonObject;
import com.sayonora.warp.awswire.AwsConfig;
import com.sayonora.warp.awswire.AwsException;
import com.sayonora.warp.awswire.AwsRuntime;
import com.sayonora.warp.awswire.AwsService;
import com.sayonora.warp.awswire.AwsWireBootstrap;

/**
 * Access to awswire's service classes (SNS, Kinesis, Secrets Manager, SSM, KMS, STS) for the MCP tools. The runtime of the
 * running AWS frontends is reused when there is one (same service instances, SNS-to-SQS delivery included); otherwise a
 * private runtime over the same registry is built once. An operation is invoked exactly as {@code AwsHttp} does after
 * decoding a request: {@code service.invoke(operation, requestJson, call)}.
 */
final class AwsToolSupport {

    private AwsToolSupport() {
    }

    static AwsRuntime runtime(EmulatedStores stores) {
        return stores.engine("aws", reg -> {
            AwsRuntime running = AwsWireBootstrap.runtime();
            if (running != null) {
                return running;
            }
            AwsRuntime rt = new AwsRuntime(reg, AwsConfig.fromEnv(), null, stores.sqlMetrics());
            if (AwsWireBootstrap.sqsOperations() != null) {
                rt.attachSqs(AwsWireBootstrap.sqsOperations());
            }
            rt.startAll();
            return rt;
        });
    }

    /** Runs one AWS operation; an AWS error becomes an exception whose message is {@code Code: message}. */
    static JsonObject invoke(EmulatedStores stores, String service, String op, JsonObject req) throws Exception {
        AwsService svc = runtime(stores).service(service);
        if (svc == null) {
            throw new IllegalStateException("AWS service " + service + " is not available");
        }
        try {
            return svc.invoke(op, req, new AwsService.Call("http://localhost", "mcp", "json", null));
        } catch (AwsException e) {
            throw new IllegalStateException(e.code + ": " + e.getMessage());
        }
    }

    /**
     * Whether tools may return secret values / decrypted data: always on a read-write endpoint; on a read-only endpoint
     * (WARP_MCP_READ_ONLY=true) only when the operator explicitly allows it with WARP_MCP_ALLOW_SECRET_READS=true.
     */
    static boolean secretsReadable() {
        return !"true".equalsIgnoreCase(System.getenv("WARP_MCP_READ_ONLY"))
                || "true".equalsIgnoreCase(System.getenv("WARP_MCP_ALLOW_SECRET_READS"));
    }

    static StoreToolProvider.Outcome secretsRefused(String what) {
        return StoreToolProvider.Outcome.error("ERROR [42501]: " + what + " is not returned on a read-only MCP endpoint; use a read-write "
                + "endpoint, or have the operator set WARP_MCP_ALLOW_SECRET_READS=true to allow secret reads while writes stay blocked");
    }
}
