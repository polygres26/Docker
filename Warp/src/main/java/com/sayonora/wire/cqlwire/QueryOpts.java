package com.sayonora.wire.cqlwire;

/** The parameters of a QUERY / EXECUTE / BATCH request that influence execution. */
final class QueryOpts {

    int consistency = 1;
    /** Requested page size, or -1 when the client did not page. */
    int pageSize = -1;
    byte[] pagingState;
    /** Client supplied default write timestamp (microseconds), or Long.MIN_VALUE. */
    long timestamp = Long.MIN_VALUE;
    boolean skipMetadata;
    /** The enclosing BATCH carries an explicit timestamp. */
    boolean batchTsExplicit;

    static final QueryOpts DEFAULT = new QueryOpts();
}
