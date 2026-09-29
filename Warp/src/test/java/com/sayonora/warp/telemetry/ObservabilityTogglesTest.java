package com.sayonora.warp.telemetry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link ObservabilityToggles} -- process-wide static state, reset after every
 * test so this class's tests can't leak into others in the same JVM. */
class ObservabilityTogglesTest {

    @AfterEach
    void reset() {
        ObservabilityToggles.apply(null, null);
    }

    @Test
    void withNoOverridesPrometheusDefaultsToEnabled() {
        assertNull(ObservabilityToggles.otlpOverride());
        assertNull(ObservabilityToggles.prometheusOverride());
        assertTrue(ObservabilityToggles.prometheusEnabled());
    }

    @Test
    void explicitFalseDisablesPrometheus() {
        ObservabilityToggles.apply(null, "false");
        assertFalse(ObservabilityToggles.prometheusEnabled());
        assertFalse(ObservabilityToggles.prometheusOverride());
    }

    @Test
    void explicitTrueIsIndistinguishableFromNoOverrideForPrometheus() {
        ObservabilityToggles.apply(null, "true");
        assertTrue(ObservabilityToggles.prometheusEnabled());
        assertTrue(ObservabilityToggles.prometheusOverride());
    }

    @Test
    void malformedOrBlankSpecsClearTheOverride() {
        ObservabilityToggles.apply("garbage", "");
        assertNull(ObservabilityToggles.otlpOverride());
        assertNull(ObservabilityToggles.prometheusOverride());
        assertTrue(ObservabilityToggles.prometheusEnabled(), "a cleared override falls back to enabled");
    }

    @Test
    void parsingIsCaseInsensitive() {
        ObservabilityToggles.apply("FALSE", "TrUe");
        assertFalse(ObservabilityToggles.otlpOverride());
        assertTrue(ObservabilityToggles.prometheusOverride());
    }

    @Test
    void applyingAgainReplacesThePreviousOverride() {
        ObservabilityToggles.apply(null, "false");
        assertFalse(ObservabilityToggles.prometheusEnabled());
        ObservabilityToggles.apply(null, "true");
        assertTrue(ObservabilityToggles.prometheusEnabled());
    }
}
