package com.sayonora.wire.cqlwire;

/** A CQL native protocol error: code (spec section 9) and message, plus the code-specific tail of the ERROR body. */
final class CqlError extends RuntimeException {

    static final int SERVER = 0x0000, PROTOCOL = 0x000A, BAD_CREDENTIALS = 0x0100, UNAVAILABLE = 0x1000, OVERLOADED = 0x1001,
            IS_BOOTSTRAPPING = 0x1002, TRUNCATE = 0x1003, WRITE_TIMEOUT = 0x1100, READ_TIMEOUT = 0x1200, FUNCTION_FAILURE = 0x1400,
            SYNTAX = 0x2000, UNAUTHORIZED = 0x2100, INVALID = 0x2200, CONFIG = 0x2300, ALREADY_EXISTS = 0x2400, UNPREPARED = 0x2500;

    final int code;
    /** ALREADY_EXISTS: keyspace and table ("" table = keyspace); UNPREPARED: the statement id in {@code id}. */
    String keyspace = "";
    String table = "";
    byte[] id;
    /** FUNCTION_FAILURE: keyspace in {@link #keyspace}, function name here. */
    String function = "";

    CqlError(int code, String message) {
        super(message, null, false, false);
        this.code = code;
    }

    static CqlError invalid(String m) {
        return new CqlError(INVALID, m);
    }

    static CqlError syntax(String m) {
        return new CqlError(SYNTAX, m);
    }

    static CqlError config(String m) {
        return new CqlError(CONFIG, m);
    }

    static CqlError server(String m) {
        return new CqlError(SERVER, m);
    }

    static CqlError functionFailure(String function, String msg) {
        CqlError e = new CqlError(FUNCTION_FAILURE, msg);
        e.keyspace = "system";
        e.function = function;
        return e;
    }

    static CqlError unauthorized(String m) {
        return new CqlError(UNAUTHORIZED, m);
    }

    static CqlError exists(String ks, String table) {
        CqlError e = new CqlError(ALREADY_EXISTS, table.isEmpty() ? "Cannot add existing keyspace \"" + ks + "\""
                : "Cannot add already existing table \"" + table + "\" to keyspace \"" + ks + "\"");
        e.keyspace = ks;
        e.table = table;
        return e;
    }
}
