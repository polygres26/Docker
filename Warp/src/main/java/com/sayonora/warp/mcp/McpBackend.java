package com.sayonora.warp.mcp;

import com.sayonora.warp.core.BackendTarget;

/**
 * One backend as an MCP endpoint sees it: a registered {@code WARP_BACKENDS} entry, or a
 * Warp-emulated store (dynamowire/mongowire/influxwire data living in Postgres tables) exposed as a
 * logical store of its host backend and named {@code <host>.<kind>} (e.g. {@code default.dynamodb}).
 *
 * @param type   specific type id ({@code postgres}, {@code mongodb}, {@code dynamodb}, ...)
 * @param kind   tool family the type belongs to
 * @param host   for an emulated store, the registered backend that physically holds its data
 * @param target the registered backend (for an emulated store: its host); may be null only for a
 *               synthetic host that is not registered
 */
public record McpBackend(String name, String type, BackendKind kind, boolean emulated, String host,
        BackendTarget target) {

    public static final String EMULATED_SEPARATOR = ".";

    public String engine() {
        return emulated ? "warp-emulated (" + type + " wire protocol over Postgres tables on \"" + host + "\")"
                : "real";
    }
}
