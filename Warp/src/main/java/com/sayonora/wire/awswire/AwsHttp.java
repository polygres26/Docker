package com.sayonora.wire.awswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP glue for one {@link AwsService}: SigV4 (when configured), protocol decoding (JSON 1.0/1.1, CBOR, Query),
 * dispatch, error mapping and metrics. Used both by a service's own listener and by the unified endpoint.
 */
public final class AwsHttp {

    private static final Logger log = LoggerFactory.getLogger(AwsHttp.class);

    private final AwsRuntime rt;

    public AwsHttp(AwsRuntime rt) {
        this.rt = rt;
    }

    public static String baseUrl(HttpServletRequest request) {
        String host = request.getHeader("Host");
        if (host == null || host.isBlank()) {
            host = request.getLocalName() + ":" + request.getLocalPort();
        }
        return request.getScheme() + "://" + host;
    }

    /** @param metricsProtocol the protocol label operations are recorded under ({@code snswire}, or {@code awswire}) */
    public void serve(AwsService svc, HttpServletRequest request, HttpServletResponse response, String metricsProtocol)
            throws IOException {
        String requestId = UUID.randomUUID().toString();
        response.setHeader("x-amzn-RequestId", requestId);
        response.setCharacterEncoding("UTF-8");
        String target = request.getHeader("X-Amz-Target");
        String ctype = request.getContentType() == null ? "" : request.getContentType().toLowerCase();
        boolean cbor = svc.supportsCbor() && ctype.contains("cbor");
        boolean jsonProto = target != null && !svc.targetPrefixes().isEmpty();
        boolean query = !jsonProto;
        if (!rt.gate.acceptHttp(request)) {
            error(svc, response, requestId, query, cbor, new AwsException(403, "AccessDeniedException", "forbidden"));
            return;
        }
        byte[] body = request.getInputStream().readAllBytes();
        if (rt.config.credentials.isEnabled()) {
            AwsSigV4.Result r = AwsSigV4.verify(request, body, rt::secretFor, Instant.now());
            if (!r.valid()) {
                error(svc, response, requestId, query, cbor, new AwsException(r.status(), r.code(), r.message()));
                return;
            }
        }
        String op = null;
        JsonObject req = null;
        long start = System.nanoTime();
        try {
            if (jsonProto) {
                String prefix = null;
                for (String p : svc.targetPrefixes()) {
                    if (target.startsWith(p)) {
                        prefix = p;
                    }
                }
                if (prefix == null) {
                    throw AwsException.bad("UnknownOperationException", "Unrecognized X-Amz-Target: " + target);
                }
                op = target.substring(prefix.length());
                try {
                    JsonElement parsed = cbor ? Cbor.decode(body)
                            : body.length == 0 ? new JsonObject()
                            : JsonParser.parseString(new String(body, StandardCharsets.UTF_8));
                    req = parsed.isJsonObject() ? parsed.getAsJsonObject() : new JsonObject();
                } catch (RuntimeException e) {
                    throw AwsException.bad("SerializationException", "Could not parse request body: " + e.getMessage());
                }
            } else {
                if (!svc.supportsQuery()) {
                    throw AwsException.bad("UnknownOperationException", "Missing or unrecognized X-Amz-Target header");
                }
                Map<String, String> params = QueryCodec.parseForm(request.getQueryString(), new String(body, StandardCharsets.UTF_8));
                op = params.get("Action");
                if (op == null) {
                    throw AwsException.bad("MissingAction", "Missing Action parameter");
                }
                req = QueryCodec.toJson(params);
            }
            if (svc instanceof KinesisService kin && op.equals("SubscribeToShard") && jsonProto) {
                kin.subscribeToShard(req, new ServletSink(response));
                return;
            }
            AwsService.Call call = new AwsService.Call(baseUrl(request), AwsSigV4.accessKey(request),
                    jsonProto ? (cbor ? "cbor" : "json") : "query", request);
            JsonObject result = svc.invoke(op, req, call);
            if (result == null) {
                result = new JsonObject();
            }
            response.setStatus(200);
            if (jsonProto) {
                writeJson(svc, response, cbor, result);
            } else {
                response.setContentType("text/xml;charset=UTF-8");
                response.getWriter().write(QueryCodec.renderResponse(svc, op, result, requestId));
            }
        } catch (AwsException e) {
            error(svc, response, requestId, query, cbor, e);
        } catch (SQLException e) {
            log.warn("{}: Postgres error servicing {}: {}", svc.id(), op, e.getMessage());
            error(svc, response, requestId, query, cbor, new AwsException(500, "InternalFailure", "Postgres error: " + e.getMessage()));
        } catch (RuntimeException e) {
            log.error("{} operation {} failed", svc.id(), op, e);
            error(svc, response, requestId, query, cbor, new AwsException(500, "InternalFailure", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("{} operation {} failed", svc.id(), op, e);
            error(svc, response, requestId, query, cbor, new AwsException(500, "InternalFailure", String.valueOf(e.getMessage())));
        } finally {
            if (rt.metrics != null && op != null) {
                long nanos = Math.max(0, System.nanoTime() - start);
                String backend = "default";
                try {
                    backend = svc.available() ? svc.backendLabel(op, req) : "default";
                } catch (RuntimeException ignored) {
                    // label only
                }
                rt.metrics.recordOperation(metricsProtocol, backend, svc.kindOf(op), op, nanos, nanos);
            }
        }
    }

    /** Streams an event-stream response over a servlet response (chunked HTTP/1.1). */
    private static final class ServletSink implements StreamSink {
        private final HttpServletResponse response;
        private volatile boolean cancelled;

        ServletSink(HttpServletResponse response) {
            this.response = response;
        }

        @Override
        public void head(int status, Map<String, String> headers) {
            response.setStatus(status);
            headers.forEach((k, v) -> {
                if (k.equalsIgnoreCase("content-type")) {
                    response.setContentType(v);
                } else {
                    response.setHeader(k, v);
                }
            });
            try {
                response.flushBuffer();
            } catch (IOException e) {
                cancelled = true;
            }
        }

        @Override
        public void data(byte[] bytes, boolean end) {
            if (cancelled) {
                return;
            }
            try {
                if (bytes.length > 0) {
                    response.getOutputStream().write(bytes);
                }
                response.flushBuffer();
            } catch (IOException e) {
                cancelled = true;
            }
        }

        @Override
        public boolean cancelled() {
            return cancelled;
        }
    }

    private void writeJson(AwsService svc, HttpServletResponse response, boolean cbor, JsonObject result) throws IOException {
        if (cbor) {
            response.setContentType("application/x-amz-cbor-1.1");
            response.getOutputStream().write(Cbor.encode(result, svc.blobFields(), svc.timestampFields()));
        } else {
            response.setContentType("application/x-amz-json-" + svc.jsonVersion());
            response.getWriter().write(result.toString());
        }
    }

    private void error(AwsService svc, HttpServletResponse response, String requestId, boolean query, boolean cbor, AwsException e)
            throws IOException {
        response.setStatus(e.status);
        if (query) {
            response.setContentType("text/xml;charset=UTF-8");
            response.getWriter().write(QueryCodec.renderError(svc, e.code, String.valueOf(e.getMessage()), requestId, e.senderFault));
            return;
        }
        JsonObject err = new JsonObject();
        err.addProperty("__type", svc.jsonErrorType(e.code));
        err.addProperty("message", String.valueOf(e.getMessage()));
        response.setHeader("x-amzn-ErrorType", svc.jsonErrorType(e.code));
        if (svc.supportsQuery()) {
            response.setHeader("x-amzn-query-error", e.code + (e.senderFault ? ";Sender" : ";Receiver"));
        }
        if (cbor) {
            response.setContentType("application/x-amz-cbor-1.1");
            response.getOutputStream().write(Cbor.encode(err, java.util.Set.of(), java.util.Set.of()));
        } else {
            response.setContentType("application/x-amz-json-" + svc.jsonVersion());
            response.getWriter().write(err.toString());
        }
    }
}
