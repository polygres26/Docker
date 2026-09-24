package com.sayonora.wire.mongowire;

import com.sayonora.wire.cluster.RowCache;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import org.bson.BsonDocument;

/**
 * Public, in-process entry point to mongowire's command dispatcher for the MCP gateway's
 * MongoDB-kind tools: the same {@code find/insert/update/delete/aggregate/count/listCollections}
 * translation, the same physical tables ({@code "<db>"."<collection>"(id text, doc jsonb)}) and the
 * same row cache as a real MongoDB client connection.
 */
public final class MongoWireEmbedded {

    private final MongoCommandDispatcher dispatcher;

    public MongoWireEmbedded(BackendRegistry registry, RowCache cache, SqlMetricsCollector metrics) {
        this.dispatcher = new MongoCommandDispatcher(new PostgresDocumentStore(registry), cache, metrics);
    }

    /** Runs one command document (first key = command name, {@code $db} = database) and returns
     * the server's reply document ({@code ok: 1} or {@code ok: 0, errmsg}). */
    public synchronized BsonDocument run(BsonDocument command) {
        return dispatcher.dispatch(command);
    }

    /** Physical schema-qualified table for a database/collection pair (quoted). */
    public static String physicalTable(String db, String collection) {
        return PostgresDocumentStore.qualifiedTable(db, collection);
    }
}
