package com.sayonora.wire.ab;

import java.util.Locale;

/** The two places a request can be served: Warp's own emulation or the real cloud service. */
public enum AbSide {
    LOCAL, CLOUD;

    public static AbSide parse(String s) {
        try {
            return valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("side must be local or cloud, got: " + s);
        }
    }

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public AbSide other() {
        return this == LOCAL ? CLOUD : LOCAL;
    }
}
