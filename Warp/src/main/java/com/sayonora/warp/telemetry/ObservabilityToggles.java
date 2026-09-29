package com.sayonora.warp.telemetry;

/**
 * Process-wide, admin-settable overrides for the two observability destinations -- the live
 * counterpart to {@code WarpConfig.otlpExportOverride}/{@code prometheusScrapeOverride}, applied
 * by {@code ObservabilityApi}'s PATCH handler and re-applied at startup from whatever {@code
 * warp_config} version {@code Main} loads. {@code null} means "no admin override" (defer to the
 * env-var-derived default each caller already has); {@code true}/{@code false} is an explicit
 * admin choice that wins over env.
 *
 * <p>Scope, stated plainly: the Prometheus override is a real, unconditional live toggle -- {@code
 * GET /metrics} either renders or returns 404, nothing more is needed since that endpoint has no
 * network client to construct. The OTLP override is narrower -- it can only PAUSE or RESUME an
 * exporter that was already constructed from {@code WARP_OTEL_ENDPOINT} at process startup (see
 * {@code WarpTelemetry.ExportHealthTrackingExporter}); it cannot conjure a live exporter into
 * existence if {@code WARP_OTEL_ENDPOINT=disabled} at boot, since that needs a real endpoint/
 * protocol/headers to build a network client from. That gap is surfaced honestly in {@code
 * ObservabilitySummary}'s JSON ({@code otlp.adminOverrideHasEffect}) rather than implied away.
 */
public final class ObservabilityToggles {

    private static volatile Boolean otlpOverride;
    private static volatile Boolean prometheusOverride;

    private ObservabilityToggles() {
    }

    /** Applies both overrides at once, e.g. from a freshly-loaded {@code WarpConfig}. Either spec
     * may be {@code null}/blank/unrecognized, in which case that override is cleared (falls back
     * to the env-derived default) -- never throws, matching this codebase's other config-apply
     * methods (see {@code BackendRegistry.applyStoreFrontendSets}). */
    public static void apply(String otlpSpec, String prometheusSpec) {
        otlpOverride = parseBool(otlpSpec);
        prometheusOverride = parseBool(prometheusSpec);
    }

    private static Boolean parseBool(String spec) {
        if (spec == null || spec.isBlank()) {
            return null;
        }
        if ("true".equalsIgnoreCase(spec.trim())) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(spec.trim())) {
            return Boolean.FALSE;
        }
        return null;
    }

    /** The raw admin override for OTLP export, or {@code null} if none is set. */
    public static Boolean otlpOverride() {
        return otlpOverride;
    }

    /** The raw admin override for the Prometheus scrape endpoint, or {@code null} if none is set. */
    public static Boolean prometheusOverride() {
        return prometheusOverride;
    }

    /** Whether {@code GET /metrics} should currently render -- {@code true} unless an admin has
     * explicitly disabled it (today's long-standing default is "always on"). */
    public static boolean prometheusEnabled() {
        Boolean override = prometheusOverride;
        return override == null || override;
    }
}
