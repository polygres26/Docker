package com.polygres.wire.mongowire;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MongoWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MongoWireSessionHandler.class);

    private final Socket clientSocket;
    private final MongoCommandDispatcher dispatcher;

    public MongoWireSessionHandler(Socket clientSocket, String pgUrl, String pgUser, String pgPassword) {
        this(clientSocket, pgUrl, pgUser, pgPassword, null, null);
    }

    public MongoWireSessionHandler(Socket clientSocket, String pgUrl, String pgUser, String pgPassword, MongoCache cache) {
        this(clientSocket, pgUrl, pgUser, pgPassword, cache, null);
    }

    public MongoWireSessionHandler(Socket clientSocket, String pgUrl, String pgUser, String pgPassword,
            MongoCache cache, com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.clientSocket = clientSocket;
        PostgresDocumentStore store = new PostgresDocumentStore(() -> openConnection(pgUrl, pgUser, pgPassword));
        this.dispatcher = new MongoCommandDispatcher(store, cache, sqlMetrics);
    }

    /**
     * Sharded mode -- documents route by hashing {@code _id} across
     * {@code backendRegistry.shardGroup()}, the same shard group dynamowire and SQL sharding
     * already use. Empty shard group behaves like the single-backend constructor above, pointed
     * at the registry's default target -- see {@link PostgresDocumentStore}'s javadoc.
     */
    public MongoWireSessionHandler(Socket clientSocket, com.polygres.wire.core.BackendRegistry backendRegistry,
            MongoCache cache, com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.clientSocket = clientSocket;
        PostgresDocumentStore store = new PostgresDocumentStore(backendRegistry);
        this.dispatcher = new MongoCommandDispatcher(store, cache, sqlMetrics);
    }

    private static Connection openConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            while (true) {
                OpMsgFrame frame = OpMsgFrame.read(in);
                BsonDocument reply = dispatcher.dispatch(frame.body);
                OpMsgFrame.writeReply(out, frame.requestId, reply, frame.legacyQuery);
            }
        } catch (EOFException e) {
            
        } catch (Exception e) {
            log.warn("mongowire session terminated: {}", e.getMessage(), e);
        }
    }
}
