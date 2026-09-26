package com.sayonora.warp.awswire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.sqswire.SqsOperations;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.jetty.server.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts the new AWS frontends and the unified endpoint from {@code Main}. Main registers the request handlers of the
 * services it already runs ({@link #register}, {@link #registerSqs}) as it starts them, then calls {@link #start} once.
 *
 * <p>Environment: {@code WARP_AWSWIRE_PORT} (or {@code WARP_AWSWIRE_ENABLED=true} for the default 4566) starts the unified
 * endpoint; {@code WARP_SNSWIRE_PORT}, {@code WARP_KINESISWIRE_PORT}, {@code WARP_SECRETSWIRE_PORT}, {@code WARP_SSMWIRE_PORT},
 * {@code WARP_KMSWIRE_PORT} and {@code WARP_STSWIRE_PORT} start a service on its own port. All are off unless configured.
 */
public final class AwsWireBootstrap {

    private static final Logger log = LoggerFactory.getLogger(AwsWireBootstrap.class);
    private static final Map<String, Handler> HANDLERS = new LinkedHashMap<>();
    private static volatile SqsOperations sqsOps;
    private static volatile AwsRuntime running;

    private AwsWireBootstrap() {
    }

    public static synchronized void register(String service, Handler handler) {
        HANDLERS.put(service, handler);
    }

    public static synchronized void registerSqs(Handler handler, SqsOperations ops) {
        HANDLERS.put("sqs", handler);
        sqsOps = ops;
    }

    /** The in-process SQS operations registered by Main, or null when sqswire is not running. */
    public static SqsOperations sqsOperations() {
        return sqsOps;
    }

    /** The runtime of the AWS frontends started by {@link #start}, or null when none was configured. */
    public static AwsRuntime runtime() {
        return running;
    }

    private static String env(String n) {
        String v = System.getenv(n);
        return v == null || v.isBlank() ? null : v.trim();
    }

    public static synchronized void start(BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        String[][] singles = {{"sns", "WARP_SNSWIRE_PORT"}, {"kinesis", "WARP_KINESISWIRE_PORT"}, {"secretsmanager", "WARP_SECRETSWIRE_PORT"},
            {"ssm", "WARP_SSMWIRE_PORT"}, {"kms", "WARP_KMSWIRE_PORT"}, {"sts", "WARP_STSWIRE_PORT"}};
        boolean unified = env("WARP_AWSWIRE_PORT") != null || "true".equalsIgnoreCase(env("WARP_AWSWIRE_ENABLED"));
        boolean any = unified;
        for (String[] s : singles) {
            any |= env(s[1]) != null;
        }
        if (!any) {
            return;
        }
        AwsRuntime rt;
        try {
            rt = new AwsRuntime(registry, AwsConfig.fromEnv(), gate, metrics);
        } catch (RuntimeException e) {
            log.error("awswire failed to initialise -- every other wire protocol is still up.", e);
            return;
        }
        if (sqsOps != null) {
            rt.attachSqs(sqsOps);
        }
        rt.startAll();
        running = rt;
        for (String[] s : singles) {
            String p = env(s[1]);
            if (p == null) {
                continue;
            }
            try {
                new AwsServiceServer(Integer.parseInt(p), rt, rt.service(s[0])).start();
            } catch (Exception e) {
                log.error("{} failed to start on port {} -- every other wire protocol is still up.", s[0], p, e);
            }
        }
        if (unified) {
            try {
                String p = env("WARP_AWSWIRE_PORT");
                new AwsWireServer(p == null ? 4566 : Integer.parseInt(p), rt, new LinkedHashMap<>(HANDLERS)).start();
            } catch (Exception e) {
                log.error("awswire failed to start -- every other wire protocol is still up.", e);
            }
        }
    }
}
