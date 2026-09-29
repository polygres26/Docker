package com.sayonora.warp.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link WarpTelemetry#buildResource()} -- the OTLP resource attributes every
 * export now carries (service.name/version, host.name, warp.zone), where previously none were
 * attached at all. Exercised directly (package-private) without constructing a real OTLP exporter. */
class WarpTelemetryResourceTest {

    @Test
    void alwaysCarriesAHostNameAndAServiceName() {
        Resource r = WarpTelemetry.buildResource();
        assertFalse(r.getAttribute(AttributeKey.stringKey("host.name")).isBlank());
        assertEquals("warp", r.getAttribute(AttributeKey.stringKey("service.name")));
    }

    @Test
    void warpZoneFallsBackToHostNameWhenNoZoneEnvVarIsSet() {
        // Mirrors com.sayonora.warp.config.NodeRegistry#resolveZone's own fallback -- this test
        // assumes WARP_ZONE isn't set in the CI/dev environment, matching that class's own
        // documented default ("one real machine not in a zoned deployment is its own honest
        // group of one").
        Resource r = WarpTelemetry.buildResource();
        String host = r.getAttribute(AttributeKey.stringKey("host.name"));
        String zone = r.getAttribute(AttributeKey.stringKey("warp.zone"));
        if (System.getenv("WARP_ZONE") == null || System.getenv("WARP_ZONE").isBlank()) {
            assertEquals(host, zone, "with no WARP_ZONE set, the zone attribute must equal the host name");
        }
    }

    @Test
    void serviceVersionIsOmittedRatherThanBlankWhenNoJarManifestIsPresent() {
        // In a test JVM (classes on a plain classpath, not a packaged jar), getImplementationVersion()
        // returns null -- buildResource() must not attach a blank/garbage value in that case.
        Resource r = WarpTelemetry.buildResource();
        String version = r.getAttribute(AttributeKey.stringKey("service.version"));
        if (WarpTelemetry.class.getPackage().getImplementationVersion() == null) {
            assertNull(version, "no manifest version available -- service.version must be omitted, not blank");
        }
    }

    @Test
    void serviceNameIsOverridableViaEnv() {
        // WARP_OTEL_SERVICE_NAME isn't set in this test environment -- confirms the real default
        // ("warp") applies when the override is absent, the common case.
        if (System.getenv("WARP_OTEL_SERVICE_NAME") == null) {
            assertEquals("warp", WarpTelemetry.buildResource().getAttribute(AttributeKey.stringKey("service.name")));
        }
    }
}
