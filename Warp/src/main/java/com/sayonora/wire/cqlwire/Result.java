package com.sayonora.wire.cqlwire;

import java.util.List;

/** Outcome of executing a statement. */
final class Result {

    enum Kind { VOID, ROWS, SET_KEYSPACE, SCHEMA_CHANGE }

    record ColSpec(String ks, String table, String name, CqlType type) {
    }

    /** A schema change: type CREATED/UPDATED/DROPPED, target KEYSPACE/TABLE/TYPE, plus keyspace and object name. */
    record SchemaEvent(String change, String target, String ks, String name) {
    }

    final Kind kind;
    List<ColSpec> cols = List.of();
    List<byte[][]> rows = List.of();
    byte[] pagingState;
    String keyspace;
    SchemaEvent event;

    Result(Kind kind) {
        this.kind = kind;
    }

    static final Result VOID = new Result(Kind.VOID);

    static Result rows(List<ColSpec> cols, List<byte[][]> rows, byte[] paging) {
        Result r = new Result(Kind.ROWS);
        r.cols = cols;
        r.rows = rows;
        r.pagingState = paging;
        return r;
    }

    static Result schema(SchemaEvent e) {
        Result r = new Result(Kind.SCHEMA_CHANGE);
        r.event = e;
        return r;
    }

    static Result setKeyspace(String ks) {
        Result r = new Result(Kind.SET_KEYSPACE);
        r.keyspace = ks;
        return r;
    }
}
