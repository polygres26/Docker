package com.sayonora.warp.telemetry;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A real "test this OTLP destination" probe for {@code POST /api/observability/test-connection} --
 * NOT a bare TCP/socket reachability check. Confirmed empirically before writing this class: an
 * OTLP exporter's {@code export()} call issues a genuine network request even with an EMPTY metric
 * list (no early-return optimization in either the gRPC or HTTP OTLP exporter implementation), so
 * a throwaway exporter built from the candidate protocol/endpoint/headers and exercised with
 * {@code export(List.of())} produces a real connection attempt, a real TLS handshake where
 * applicable, and -- for a SaaS endpoint that validates the header on every request (New Relic,
 * Datadog) -- a real authentication result, all inside one measured round trip. This is exactly
 * what the product review asked for ("only add this after the backend can perform an actual test
 * export"), not the simpler, more misleading "can I open a socket" check it explicitly warned
 * against.
 *
 * <p>The throwaway exporter is discarded after one call (built, used once, shut down) -- this
 * never touches the live {@link WarpTelemetry} instance or its real export-health counters, so
 * testing a candidate endpoint from the config generator can never be confused with (or pollute)
 * the metrics of whatever endpoint is actually configured and running.
 */
public final class OtlpConnectivityTest {

    private OtlpConnectivityTest() {
    }

    public record Result(boolean success, long tookMs, String protocol, String endpoint, String errorMessage) {
    }

    /** {@code timeout} bounds both the network call itself (via the exporter's own connect/read
     * timeout) and how long this method waits for the result -- a test against an unreachable
     * endpoint returns within roughly this bound, not indefinitely. */
    public static Result test(String protocol, String endpoint, Map<String, String> headers, Duration timeout) {
        long start = System.nanoTime();
        String normalizedProtocol = "http".equalsIgnoreCase(protocol) ? "http" : "grpc";
        MetricExporter exporter = null;
        try {
            exporter = buildTestExporter(normalizedProtocol, endpoint, headers, timeout);
            CompletableResultCode code = exporter.export(Collections.emptyList());
            code.join(timeout.toMillis() + 1_000, TimeUnit.MILLISECONDS);
            long tookMs = elapsedMs(start);
            if (code.isSuccess()) {
                return new Result(true, tookMs, normalizedProtocol, endpoint, null);
            }
            Throwable failure = code.getFailureThrowable();
            return new Result(false, tookMs, normalizedProtocol, endpoint,
                    failure != null ? describeFailure(failure) : "export did not complete successfully (no exception reported)");
        } catch (RuntimeException e) {
            return new Result(false, elapsedMs(start), normalizedProtocol, endpoint, describeFailure(e));
        } finally {
            if (exporter != null) {
                exporter.shutdown();
            }
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /** Unwraps to the real root cause's message -- OTel's exporters wrap the actual network/HTTP
     * exception in their own {@code FailedExportException}, and the wrapper's own message is
     * usually just "The request could not be executed", which tells an operator nothing. */
    private static String describeFailure(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return (message == null || message.isBlank()) ? cause.getClass().getSimpleName() : message;
    }

    private static MetricExporter buildTestExporter(String protocol, String endpoint, Map<String, String> headers, Duration timeout) {
        if ("http".equals(protocol)) {
            var builder = OtlpHttpMetricExporter.builder().setEndpoint(endpoint).setTimeout(timeout);
            headers.forEach(builder::addHeader);
            return builder.build();
        }
        var builder = OtlpGrpcMetricExporter.builder().setEndpoint(endpoint).setTimeout(timeout);
        headers.forEach(builder::addHeader);
        return builder.build();
    }
}
