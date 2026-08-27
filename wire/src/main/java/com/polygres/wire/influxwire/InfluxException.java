package com.polygres.wire.influxwire;

/** A domain-level influxwire failure (bad line protocol, unsupported query) -- distinct from a
 * {@link java.sql.SQLException}, the same split {@code OpenSearchException}/{@code DynamoException}
 * draw for their own protocols, so {@code InfluxWireServer} can tell "the request itself was bad"
 * (400) apart from "a real Postgres error happened" (mapped via {@link InfluxErrorMapper}). */
public final class InfluxException extends RuntimeException {
    public InfluxException(String message) {
        super(message);
    }
}
