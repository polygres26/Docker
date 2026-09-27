package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;

/**
 * Extension point for how Warp talks to an upstream MCP server. Only HTTP variants are implemented
 * for this task ({@link UpstreamHttpClient}, Streamable HTTP + legacy HTTP+SSE); a stdio/local-process
 * transport (spawning and speaking newline-delimited JSON-RPC over the child's stdio) is a clean,
 * deliberately unimplemented extension of this interface -- NOT built here (see task decisions).
 */
public interface UpstreamTransport {

    JsonObject initialize() throws IOException, InterruptedException;

    JsonArray listTools() throws IOException, InterruptedException;

    JsonObject callTool(String name, JsonObject arguments) throws IOException, InterruptedException;
}
