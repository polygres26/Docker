package com.nexagres.wire.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public enum Edition {
    FREE(100, 2),
    COMMERCIAL(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final int maxConnections;
    private final int maxBackends;

    Edition(int maxConnections, int maxBackends) {
        this.maxConnections = maxConnections;
        this.maxBackends = maxBackends;
    }

    public int maxConnections() {
        return maxConnections;
    }

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

    static void resetCacheForTests() {
        cached = null;
    }

    private static Edition resolve() {
        String markerPath = System.getenv().getOrDefault("WARP_EDITION_FILE", "/opt/warp/EDITION");
        String markerContent = null;
        try {
            markerContent = Files.readString(Path.of(markerPath));
        } catch (IOException ignoredMissingOrUnreadable) {
            
        }
        return resolveGiven(markerContent, System.getenv("WARP_EDITION"));
    }

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
