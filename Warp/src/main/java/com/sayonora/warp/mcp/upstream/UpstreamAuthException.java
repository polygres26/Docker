package com.sayonora.warp.mcp.upstream;

import java.io.IOException;

/** The upstream rejected our credentials (HTTP 401/403). Distinguished from a generic transport
 * failure so the gateway can surface "auth" as the health/last-error reason distinctly from
 * "unreachable"/"timeout". */
public final class UpstreamAuthException extends IOException {
    public UpstreamAuthException(String message) {
        super(message);
    }
}
