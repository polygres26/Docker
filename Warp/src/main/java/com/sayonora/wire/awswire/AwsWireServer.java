package com.sayonora.wire.awswire;

import com.google.gson.JsonObject;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The unified AWS endpoint ({@code awswire}): one HTTP listener (default 4566, like LocalStack and Floci) that takes any AWS
 * request and hands it to the right service, so one SDK endpoint URL serves everything.
 *
 * <p><b>Dispatch</b>, in order: the {@code X-Amz-Target} prefix (DynamoDB, SQS and every new JSON service); the service name
 * in the SigV4 credential scope ({@code Authorization} header or {@code X-Amz-Credential} query parameter); the Query
 * {@code Action} of an unsigned request (SNS, STS, SQS); otherwise S3 (path-style and virtual-hosted requests carry no
 * target). <b>No proxy hop</b>: DynamoDB, SQS and S3 are dispatched in process, by calling the same Jetty handler object the
 * service's own listener uses ({@code dynamowire}, {@code sqswire} and {@code s3wire} keep working on their ports) -- a
 * reverse proxy would double the request latency and the socket count for no gain, and could not share the SQS operations
 * SNS delivers into. The new services run through {@link AwsHttp} directly. Metrics: requests to the new services are
 * recorded under protocol {@code awswire}; the delegated three record under their own protocols as before.
 */
public final class AwsWireServer {

    private static final Logger log = LoggerFactory.getLogger(AwsWireServer.class);
    private static final Set<String> SQS_ACTIONS = Set.of("CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
            "SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch",
            "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags",
            "AddPermission", "RemovePermission", "ListDeadLetterSourceQueues", "StartMessageMoveTask", "CancelMessageMoveTask",
            "ListMessageMoveTasks");

    private final Server server;
    private final AwsRuntime rt;
    private final Map<String, Handler> delegated = new LinkedHashMap<>();
    private final AwsHttp http;

    /** @param delegated handlers of the services already running in this process: keys {@code dynamodb}, {@code sqs}, {@code s3} */
    public AwsWireServer(int port, AwsRuntime rt, Map<String, Handler> delegated) {
        this.rt = rt;
        this.delegated.putAll(delegated);
        this.http = new AwsHttp(rt);
        this.server = new Server(new QueuedThreadPool(AwsServiceServer.threads()));
        ServerConnector c = H2cConnectionFactory.connector(server, port, this::dispatchH2, server.getThreadPool());
        server.addConnector(c);
        allowAmbiguousKeyPaths(c);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response) throws IOException {
                base.setHandled(true);
                dispatch(target, base, request, response);
            }
        });
    }

    /** S3 keys may contain empty/dot segments and encoded slashes, which Jetty otherwise rejects as ambiguous. */
    private static void allowAmbiguousKeyPaths(ServerConnector c) {
        org.eclipse.jetty.http.UriCompliance lax = org.eclipse.jetty.http.UriCompliance.from(java.util.EnumSet.of(
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_SEGMENT,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING));
        org.eclipse.jetty.server.HttpConnectionFactory f = c.getConnectionFactory(org.eclipse.jetty.server.HttpConnectionFactory.class);
        if (f != null) {
            f.getHttpConfiguration().setUriCompliance(lax);
            f.getHttpConfiguration().setRequestHeaderSize(32 * 1024);
        }
    }

    /** A request whose body was read for routing, replayable to the handler that gets it. */
    private static final class CachedBody extends HttpServletRequestWrapper {
        private final byte[] body;

        CachedBody(HttpServletRequest r, byte[] body) {
            super(r);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream in = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public int read() {
                    return in.read();
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    return in.read(b, off, len);
                }

                @Override
                public boolean isFinished() {
                    return in.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(jakarta.servlet.ReadListener l) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(body), StandardCharsets.UTF_8));
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }
    }

    private String routeByTarget(String target) {
        if (target == null) {
            return null;
        }
        if (target.startsWith("DynamoDB_") || target.startsWith("DynamoDBStreams_")) {
            return "dynamodb";
        }
        if (target.startsWith("AmazonSQS.")) {
            return "sqs";
        }
        for (AwsService s : rt.services().values()) {
            for (String p : s.targetPrefixes()) {
                if (target.startsWith(p)) {
                    return s.id();
                }
            }
        }
        return null;
    }

    private String routeBySigningName(String name) {
        if (name == null) {
            return null;
        }
        switch (name) {
            case "dynamodb":
                return "dynamodb";
            case "sqs":
                return "sqs";
            case "s3":
            case "s3-object-lambda":
                return "s3";
            default:
                for (AwsService s : rt.services().values()) {
                    if (s.signingNames().contains(name)) {
                        return s.id();
                    }
                }
                return null;
        }
    }

    private String routeByAction(String action) {
        if (action == null) {
            return null;
        }
        for (AwsService s : rt.services().values()) {
            if (s.queryActions().contains(action)) {
                return s.id();
            }
        }
        return SQS_ACTIONS.contains(action) ? "sqs" : null;
    }

    /** HTTP/2 (prior knowledge): only the new services are served over it (DynamoDB, SQS and S3 SDKs use HTTP/1.1). */
    private void dispatchH2(H2Exchange ex) {
        try {
            HttpServletRequest request = H2Dispatch.request(ex);
            String svc = routeByTarget(ex.header("x-amz-target"));
            if (svc == null) {
                svc = routeBySigningName(AwsSigV4.scopeService(request));
            }
            AwsService service = svc == null ? null : rt.service(svc);
            if (service == null) {
                ex.head(400, java.util.Map.of("content-type", "application/x-amz-json-1.0"));
                ex.data("{\"__type\":\"UnknownServiceException\",\"message\":\"HTTP/2 is served for sns, kinesis, secretsmanager, ssm, kms, sts only\"}"
                        .getBytes(StandardCharsets.UTF_8), true);
                return;
            }
            H2Dispatch.serve(rt, http, service, ex, "awswire");
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private void dispatch(String target, Request base, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ("/_warp/health".equals(request.getRequestURI()) && "GET".equals(request.getMethod())) {
            JsonObject o = new JsonObject();
            o.addProperty("edition", "warp-awswire");
            JsonObject services = new JsonObject();
            delegated.keySet().forEach(k -> services.addProperty(k, "running"));
            rt.services().values().forEach(s -> services.addProperty(s.id(), s.available() ? "running" : "store-not-enabled"));
            o.add("services", services);
            response.setContentType("application/json");
            response.getWriter().write(o.toString());
            return;
        }
        String amzTarget = request.getHeader("X-Amz-Target");
        String svc = routeByTarget(amzTarget);
        String scope = AwsSigV4.scopeService(request);
        if (svc == null) {
            svc = routeBySigningName(scope);
        }
        if (svc == null && (amzTarget != null || scope != null)) {
            // a request for an AWS service Warp does not emulate: say so instead of handing it to S3
            response.setStatus(400);
            response.setContentType("application/x-amz-json-1.0");
            response.setHeader("x-amzn-ErrorType", "UnknownServiceException");
            response.getWriter().write("{\"__type\":\"UnknownServiceException\",\"message\":\"Warp's unified AWS endpoint does not emulate "
                    + (scope != null ? "the service " + scope : "X-Amz-Target " + amzTarget) + "; supported services: dynamodb, sqs, s3, "
                    + "sns, kinesis, secretsmanager, ssm, kms, sts\"}");
            return;
        }
        HttpServletRequest req = request;
        if (svc == null) {
            String ct = request.getContentType() == null ? "" : request.getContentType().toLowerCase();
            String q = request.getQueryString();
            String action = null;
            if (q != null && q.contains("Action=")) {
                action = QueryCodec.parseForm(q).get("Action");
            }
            if (action == null && "POST".equals(request.getMethod()) && ct.contains("x-www-form-urlencoded")) {
                byte[] body = request.getInputStream().readAllBytes();
                req = new CachedBody(request, body);
                action = QueryCodec.parseForm(new String(body, StandardCharsets.UTF_8)).get("Action");
            }
            svc = routeByAction(action);
        }
        if (svc == null) {
            svc = "s3";
        }
        Handler h = delegated.get(svc);
        if (h != null && svc.equals("sqs") && rt.config.credentials.isEnabled()) {
            // sqswire has no auth of its own; with WARP_AWS_IAM_CREDENTIALS set the unified endpoint validates SigV4 for it
            byte[] body = req.getInputStream().readAllBytes();
            req = new CachedBody(req, body);
            AwsSigV4.Result r = AwsSigV4.verify(req, body, rt::secretFor, java.time.Instant.now());
            if (!r.valid()) {
                response.setStatus(r.status());
                response.setContentType("application/x-amz-json-1.0");
                response.getWriter().write("{\"__type\":\"" + r.code() + "\",\"message\":\"" + r.message() + "\"}");
                return;
            }
        }
        if (h != null) {
            try {
                h.handle(target, base, req, response);
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            return;
        }
        AwsService service = rt.service(svc);
        if (service == null) {
            response.setStatus(400);
            response.setContentType("application/x-amz-json-1.0");
            response.getWriter().write("{\"__type\":\"UnknownOperationException\",\"message\":\"The " + svc
                    + " service is not running in this Warp process (enable its listener or store)\"}");
            return;
        }
        http.serve(service, req, response, "awswire");
    }

    public void start() throws Exception {
        server.start();
        log.info("warp awswire (unified AWS endpoint: {} delegated + {}) listening on port {}", delegated.keySet(),
                rt.services().keySet(), ((ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
    }
}
