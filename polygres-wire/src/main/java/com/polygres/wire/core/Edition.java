package com.polygres.wire.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * ARCHITECTURE.md §9: which distribution this process is running as —
 * {@code FREE} (capped: {@link #maxConnections()} concurrent client
 * connections, {@link #maxBackends()} named {@link BackendRegistry}
 * entries) or {@code COMMERCIAL} (no caps, today's full feature set).
 *
 * <p>Resolution order, checked once and cached for the process lifetime:
 * <ol>
 *   <li>A marker file baked into the image at build time ({@code
 *   POLYWIRE_EDITION_FILE}, default {@code /opt/polywire/EDITION}) —
 *   this is what the free Docker image ships (see {@code
 *   docker/Dockerfile.free}), specifically so the cap can't be lifted by a
 *   {@code docker run -e POLYWIRE_EDITION=commercial} override the way an
 *   env var alone could be.</li>
 *   <li>Otherwise, the {@code POLYWIRE_EDITION} env var ({@code free} or
 *   {@code commercial}) — for running from source/CI without a baked
 *   image.</li>
 *   <li>Otherwise, {@code COMMERCIAL} — unset defaults to today's
 *   unrestricted behavior, so nothing already deployed changes shape.</li>
 * </ol>
 *
 * <p><b>Honest limit</b>: this is a self-hosted, source-available process —
 * a determined operator running the free image can still edit its
 * filesystem or rebuild from source with the caps removed. This is the same
 * honor-system-plus-a-real-default-obstacle model used by most self-hosted
 * open-core products (a build-time marker, not runtime DRM); it is not
 * tamper-proof, and shouldn't be described as such.
 */
public enum Edition {
    FREE(100, 2),
    COMMERCIAL(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final int maxConnections;
    private final int maxBackends;

    Edition(int maxConnections, int maxBackends) {
        this.maxConnections = maxConnections;
        this.maxBackends = maxBackends;
    }

    /** Max total concurrent client connections across all four frontends combined. */
    public int maxConnections() {
        return maxConnections;
    }

    /** Max named entries {@link BackendRegistry} will keep from {@code POLYWIRE_BACKENDS}. */
    public int maxBackends() {
        return maxBackends;
    }

    private static volatile Edition cached;

    public static Edition current() {
        Edition local = cached;
        if (local == null) {
            synchronized (Edition.class) {
                local = cached;
                if (local == null) {
                    cached = local = resolve();
                }
            }
        }
        return local;
    }

    /** Test/harness hook — production code never calls this. */
    static void resetCacheForTests() {
        cached = null;
    }

    private static Edition resolve() {
        String markerPath = System.getenv().getOrDefault("POLYWIRE_EDITION_FILE", "/opt/polywire/EDITION");
        String markerContent = null;
        try {
            markerContent = Files.readString(Path.of(markerPath));
        } catch (IOException ignoredMissingOrUnreadable) {
            // no baked marker in this image — resolveGiven falls back to the env var
        }
        return resolveGiven(markerContent, System.getenv("POLYWIRE_EDITION"));
    }

    /** Pure resolution logic, split out from filesystem/env access so it's directly unit-testable. */
    static Edition resolveGiven(String markerContent, String envValue) {
        if (markerContent != null && !markerContent.isBlank()) {
            return parse(markerContent);
        }
        return envValue == null || envValue.isBlank() ? COMMERCIAL : parse(envValue);
    }

    private static Edition parse(String value) {
        return "free".equalsIgnoreCase(value.trim()) ? FREE : COMMERCIAL;
    }
}
