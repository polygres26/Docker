package com.sayonora.wire.rediswire;

/** A Redis error reply raised by a command handler; {@code getMessage()} is the whole reply text, code word first. */
final class RedisError extends RuntimeException {

    static final String SYNTAX = "ERR syntax error";
    static final String WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    static final String NOT_INT = "ERR value is not an integer or out of range";
    static final String NOT_FLOAT = "ERR value is not a valid float";
    static final String CROSSSLOT = "CROSSSLOT Keys in request don't hash to the same slot";
    static final String NO_KEY = "ERR no such key";
    static final String OVERFLOW = "ERR increment or decrement would overflow";

    RedisError(String message) {
        super(message, null, false, false);
    }

    static RedisError syntax() {
        return new RedisError(SYNTAX);
    }

    static RedisError wrongType() {
        return new RedisError(WRONGTYPE);
    }

    static RedisError notInt() {
        return new RedisError(NOT_INT);
    }

    static RedisError notFloat() {
        return new RedisError(NOT_FLOAT);
    }

    static RedisError arity(String cmd) {
        return new RedisError("ERR wrong number of arguments for '" + cmd.toLowerCase(java.util.Locale.ROOT) + "' command");
    }

    static RedisError err(String text) {
        return new RedisError("ERR " + text);
    }
}
