package com.sayonora.wire.boltwire;

/** A domain-level boltwire failure (unparseable/unsupported Cypher) -- the boltwire counterpart to
 * {@code InfluxException}, distinct from a {@link java.sql.SQLException} so
 * {@code BoltWireSessionHandler} can tell "the query itself wasn't understood" (a real Bolt
 * {@code Neo.ClientError.Statement.SyntaxError}) apart from "a real Postgres error happened". */
final class CypherException extends RuntimeException {
    CypherException(String message) {
        super(message);
    }
}
