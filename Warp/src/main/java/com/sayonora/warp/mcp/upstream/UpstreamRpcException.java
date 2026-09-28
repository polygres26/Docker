package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonElement;

/** Carries an upstream's own JSON-RPC error verbatim, for translation into Warp's own JSON-RPC
 * error response by the caller (see McpUpstreamGateway#callNamespacedTool). */
public final class UpstreamRpcException extends RuntimeException {
    private final int code;
    private final transient JsonElement data;

    public UpstreamRpcException(int code, String message, JsonElement data) {
        super(message);
        this.code = code;
        this.data = data;
    }

    public int code() {
        return code;
    }

    public JsonElement data() {
        return data;
    }
}
